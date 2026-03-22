#include "writer.hpp"
#include "meta.hpp"
#include "utils.hpp"
#include <nlohmann/json.hpp>
#include <cassert>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>

using Json = nlohmann::json;

// ── Construction ──────────────────────────────────────────────────────────────

Writer::Writer(FILE* fd, bam_hdr_t* header, const std::string& codec_map_path)
    : fd_(fd), header_(header)
{
    // Reserve 1000 bytes for the file header (back-filled on close).
    if (fseek(fd_, 1000, SEEK_SET) != 0)
        throw std::runtime_error("Failed to seek past header reservation");

    load_codec_map(codec_map_path);
    init_columns();
    cur_chunk_rec_num_.fill(0);
}

Writer::~Writer() {
    if (!closed_) {
        try { close(); } catch (...) {}
    }
}

void Writer::init_columns() {
    // Wire up variable-length columns to their index columns.
    auto ci = [](ColumnType t) { return static_cast<int>(t); };
    columns_[ci(ColumnType::read_name)].index_column = &columns_[ci(ColumnType::index_read_name)];
    columns_[ci(ColumnType::cigar)].index_column     = &columns_[ci(ColumnType::index_cigar)];
    columns_[ci(ColumnType::seq)].index_column       = &columns_[ci(ColumnType::index_seq)];
    columns_[ci(ColumnType::qual)].index_column      = &columns_[ci(ColumnType::index_qual)];
    columns_[ci(ColumnType::tags)].index_column      = &columns_[ci(ColumnType::index_tags)];
}

void Writer::load_codec_map(const std::string& path) {
    std::ifstream f(path);
    if (!f) {
        // Default all columns to "none" if file is absent.
        for (int i = 0; i < COLUMNTYPE_SIZE; ++i)
            codecs_[i] = codec_from_name("none");
        return;
    }
    Json j;
    f >> j;
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) {
        std::string codec_name = "none";
        if (j.contains(ColumnTypeNames[i]))
            codec_name = j[ColumnTypeNames[i]].get<std::string>();
        codecs_[i] = codec_from_name(codec_name);
    }
}

// ── Writing ───────────────────────────────────────────────────────────────────

void Writer::write_record(const bam1_t* aln) {
    auto ci = [](ColumnType t) { return static_cast<int>(t); };

    // Fixed-size fields
    {
        auto& c = columns_[ci(ColumnType::refID)];
        c.ensure_capacity(4);
        write_le<int32_t>(&c.data[c.cur_ptr], aln->core.tid);
        c.cur_ptr += 4;
    }
    {
        auto& c = columns_[ci(ColumnType::pos)];
        c.ensure_capacity(4);
        write_le<int32_t>(&c.data[c.cur_ptr], aln->core.pos);
        c.cur_ptr += 4;
    }
    {
        auto& c = columns_[ci(ColumnType::mapq)];
        c.ensure_capacity(1);
        c.data[c.cur_ptr++] = aln->core.qual;
    }
    {
        auto& c = columns_[ci(ColumnType::bin)];
        c.ensure_capacity(2);
        write_le<int16_t>(&c.data[c.cur_ptr], static_cast<int16_t>(aln->core.bin));
        c.cur_ptr += 2;
    }
    {
        auto& c = columns_[ci(ColumnType::flag)];
        c.ensure_capacity(2);
        write_le<int16_t>(&c.data[c.cur_ptr], static_cast<int16_t>(aln->core.flag));
        c.cur_ptr += 2;
    }
    {
        auto& c = columns_[ci(ColumnType::next_refID)];
        c.ensure_capacity(4);
        write_le<int32_t>(&c.data[c.cur_ptr], aln->core.mtid);
        c.cur_ptr += 4;
    }
    {
        auto& c = columns_[ci(ColumnType::next_pos)];
        c.ensure_capacity(4);
        write_le<int32_t>(&c.data[c.cur_ptr], aln->core.mpos);
        c.cur_ptr += 4;
    }
    {
        auto& c = columns_[ci(ColumnType::tlen)];
        c.ensure_capacity(4);
        write_le<int32_t>(&c.data[c.cur_ptr], aln->core.isize);
        c.cur_ptr += 4;
    }

    // Variable-length fields
    auto append_var = [&](ColumnType ct, const uint8_t* src, int64_t sz) {
        auto& c = columns_[static_cast<int>(ct)];
        c.ensure_capacity(sz);
        std::memcpy(&c.data[c.cur_ptr], src, sz);
        c.cur_ptr += sz;
        c.write_index();
    };

    append_var(ColumnType::read_name,
               reinterpret_cast<const uint8_t*>(bam_get_qname(aln)),
               aln->core.l_qname - aln->core.l_extranul);
    append_var(ColumnType::cigar,
               reinterpret_cast<const uint8_t*>(bam_get_cigar(aln)),
               static_cast<int64_t>(aln->core.n_cigar) << 2);
    {
        int64_t seq_sz = (aln->core.l_qseq + 1) >> 1;
        append_var(ColumnType::seq, bam_get_seq(aln), seq_sz);
    }
    append_var(ColumnType::qual, bam_get_qual(aln), aln->core.l_qseq);
    append_var(ColumnType::tags, bam_get_aux(aln), bam_get_l_aux(aln));

    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) cur_chunk_rec_num_[i]++;

    dump_columns(/*force=*/false);
}

// ── Flushing ──────────────────────────────────────────────────────────────────

void Writer::dump_column(int i) {
    Column& col = columns_[i];
    if (col.cur_ptr == 0) return;

    ColumnChunkMeta meta;
    meta.file_offset      = ftell(fd_);
    meta.uncompressed_size = col.cur_ptr;
    meta.codec            = codecs_[i]->name();

    auto compressed = codecs_[i]->compress(col.data.data(), col.cur_ptr);
    meta.compressed_size = static_cast<int64_t>(compressed.size());
    fwrite(compressed.data(), 1, compressed.size(), fd_);

    meta.rec_num = cur_chunk_rec_num_[i];
    cur_chunk_rec_num_[i] = 0;
    col.reset();

    metadatas_[i].push_back(meta);
}

void Writer::dump_columns(bool force) {
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) {
        if (force || columns_[i].cur_ptr >= MAX_COLUMN_CHUNK_SIZE)
            dump_column(i);
    }
}

// ── Closing ───────────────────────────────────────────────────────────────────

void Writer::close() {
    if (closed_) return;
    closed_ = true;

    dump_columns(/*force=*/true);

    int64_t meta_start = ftell(fd_);
    write_meta(fd_, metadatas_);
    int64_t meta_size = ftell(fd_) - meta_start;

    int32_t header_len = sam_hdr_length(header_);
    fwrite(&header_len, sizeof(int32_t), 1, fd_);
    fwrite(sam_hdr_str(header_), 1, header_len, fd_);

    // Back-fill the reserved header at byte 0.
    fseek(fd_, 0, SEEK_SET);
    write_header(fd_, meta_start, meta_size, header_len);
}
