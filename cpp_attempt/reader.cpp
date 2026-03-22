#include "reader.hpp"
#include "meta.hpp"
#include "utils.hpp"
#include <cassert>
#include <cstring>
#include <stdexcept>
#include <htslib/hts.h>

// ── Helpers ───────────────────────────────────────────────────────────────────

static void bam_cigar2rqlens(int n_cigar, const uint32_t* cigar,
                              hts_pos_t* rlen, hts_pos_t* qlen)
{
    *rlen = *qlen = 0;
    for (int k = 0; k < n_cigar; ++k) {
        int type = bam_cigar_type(bam_cigar_op(cigar[k]));
        int len  = bam_cigar_oplen(cigar[k]);
        if (type & 1) *qlen += len;
        if (type & 2) *rlen += len;
    }
}

// ── Construction ──────────────────────────────────────────────────────────────

Reader::Reader(const char* mmaped_file) : mmaped_file_(mmaped_file) {
    int64_t seekpos, meta_size, header_len;
    parse_file_header(mmaped_file, seekpos, meta_size, header_len);
    assert(seekpos >= 0 && meta_size >= 0 && header_len > 0);

    metadatas_ = parse_meta_from_json_string(mmaped_file + seekpos);

    // Parse SAM header (stored after the metadata JSON + null byte)
    const int32_t* hlen_ptr = reinterpret_cast<const int32_t*>(mmaped_file + seekpos + meta_size);
    assert(*hlen_ptr == static_cast<int32_t>(header_len));
    header_ = sam_hdr_parse(*hlen_ptr, reinterpret_cast<const char*>(hlen_ptr + 1));

    // Build cumulative record-count arrays and determine total rec_num.
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) {
        const auto& chunks = metadatas_[i];
        record_counts_per_chunk_[i].resize(chunks.size());
        int64_t accum = 0;
        for (size_t j = 0; j < chunks.size(); ++j) {
            accum += chunks[j].rec_num;
            record_counts_per_chunk_[i][j] = accum;
        }
        rec_num_ = accum;  // all columns have the same total
    }

    currently_loaded_chunk_.fill(-1);
    loaded_up_to_rec_num_.fill(-1);
    loaded_since_rec_num_.fill(-1);
    m_chunk_memory_.fill(0);
}

Reader::~Reader() {
    if (header_) bam_hdr_destroy(header_);
}

// ── Lazy loading ──────────────────────────────────────────────────────────────

void Reader::fetch_field(int64_t rec_num, int col) {
    // Already loaded?
    if (loaded_since_rec_num_[col] <= rec_num &&
        rec_num <= loaded_up_to_rec_num_[col]) return;

    // Binary search: find smallest chunk index whose cumulative rec_num >= rec_num.
    const auto& counts = record_counts_per_chunk_[col];
    int l = -1, r = static_cast<int>(counts.size()) - 1;
    while ((r - l) > 1) {
        int m = (l + r) / 2;
        if (counts[m] <= rec_num) l = m;
        else                       r = m;
    }
    // r is the chunk index
    if (r == currently_loaded_chunk_[col]) return;

    currently_loaded_chunk_[col] = r;
    const ColumnChunkMeta& meta = metadatas_[col][r];

    // Grow column buffer if needed.
    if (m_chunk_memory_[col] < meta.uncompressed_size) {
        columns_[col].data.resize(meta.uncompressed_size);
        m_chunk_memory_[col] = meta.uncompressed_size;
    }

    const uint8_t* src = reinterpret_cast<const uint8_t*>(mmaped_file_ + meta.file_offset);
    auto codec = codec_from_name(meta.codec);
    codec->decompress(src, meta.compressed_size,
                      columns_[col].data.data(), meta.uncompressed_size);

    loaded_since_rec_num_[col] = (r == 0) ? 0 : counts[r - 1];
    loaded_up_to_rec_num_[col] = counts[r] - 1;
}

// ── Record reading ────────────────────────────────────────────────────────────

void Reader::read_record(int64_t rec_num, bam1_t* aln) {
    // Pre-load all columns for this record.
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) fetch_field(rec_num, i);

    auto adjusted = [&](int col) -> int64_t {
        return rec_num - loaded_since_rec_num_[col];
    };

    auto ci = [](ColumnType t) { return static_cast<int>(t); };

    // Fixed-size fields
    int32_t refID    = read_le<int32_t>(&columns_[ci(ColumnType::refID)].data[adjusted(ci(ColumnType::refID)) * 4]);
    int32_t pos      = read_le<int32_t>(&columns_[ci(ColumnType::pos)].data[adjusted(ci(ColumnType::pos)) * 4]);
    int8_t  mapq     = static_cast<int8_t>(columns_[ci(ColumnType::mapq)].data[adjusted(ci(ColumnType::mapq))]);
    int16_t bin      = read_le<int16_t>(&columns_[ci(ColumnType::bin)].data[adjusted(ci(ColumnType::bin)) * 2]);
    int16_t flag     = read_le<int16_t>(&columns_[ci(ColumnType::flag)].data[adjusted(ci(ColumnType::flag)) * 2]);
    int32_t next_pos = read_le<int32_t>(&columns_[ci(ColumnType::next_pos)].data[adjusted(ci(ColumnType::next_pos)) * 4]);
    int32_t next_ref = read_le<int32_t>(&columns_[ci(ColumnType::next_refID)].data[adjusted(ci(ColumnType::next_refID)) * 4]);
    int32_t tlen     = read_le<int32_t>(&columns_[ci(ColumnType::tlen)].data[adjusted(ci(ColumnType::tlen)) * 4]);

    // Variable-length field boundaries via index columns
    auto var_range = [&](ColumnType data_col, ColumnType idx_col,
                         int64_t& beg, int64_t& end)
    {
        end = read_le<int32_t>(&columns_[ci(idx_col)].data[adjusted(ci(idx_col)) * 4]);
        beg = 0;
        if (rec_num != 0) {
            fetch_field(rec_num - 1, ci(idx_col));
            int64_t prev_adj = (rec_num - 1) - loaded_since_rec_num_[ci(idx_col)];
            beg = read_le<int32_t>(&columns_[ci(idx_col)].data[prev_adj * 4]);
        }
    };

    int64_t qname_beg, qname_end;
    int64_t cigar_beg, cigar_end;
    int64_t seq_beg,   seq_end;
    int64_t qual_beg,  qual_end;
    int64_t tags_beg,  tags_end;

    var_range(ColumnType::read_name, ColumnType::index_read_name, qname_beg, qname_end);
    var_range(ColumnType::cigar,     ColumnType::index_cigar,     cigar_beg, cigar_end);
    var_range(ColumnType::seq,       ColumnType::index_seq,       seq_beg,   seq_end);
    var_range(ColumnType::qual,      ColumnType::index_qual,      qual_beg,  qual_end);
    var_range(ColumnType::tags,      ColumnType::index_tags,      tags_beg,  tags_end);

    int64_t l_qname = qname_end - qname_beg;
    int64_t l_cigar = cigar_end - cigar_beg;
    int64_t l_seq   = seq_end   - seq_beg;
    int64_t l_qual  = qual_end  - qual_beg;
    int64_t l_tags  = tags_end  - tags_beg;
    assert(l_seq == ((l_qual + 1) >> 1));

    bool default_name = (l_qname == 0);
    if (default_name) l_qname = 2;  // '*' + null

    uint64_t bytes_needed = l_qname + l_cigar + l_seq + l_qual + l_tags;
    if (aln->m_data < bytes_needed) {
        free(aln->data);
        aln->data   = static_cast<uint8_t*>(malloc(bytes_needed));
        aln->m_data = bytes_needed;
    }
    aln->l_data = bytes_needed;

    uint8_t* p = aln->data;
    if (default_name) {
        p[0] = '*'; p[1] = '\0';
    } else {
        std::memcpy(p, &columns_[ci(ColumnType::read_name)].data[qname_beg], l_qname);
    }
    p += l_qname;
    std::memcpy(p, &columns_[ci(ColumnType::cigar)].data[cigar_beg], l_cigar); p += l_cigar;
    std::memcpy(p, &columns_[ci(ColumnType::seq)].data[seq_beg],     l_seq);   p += l_seq;
    std::memcpy(p, &columns_[ci(ColumnType::qual)].data[qual_beg],   l_qual);  p += l_qual;
    std::memcpy(p, &columns_[ci(ColumnType::tags)].data[tags_beg],   l_tags);

    hts_pos_t rlen = 1, qlen = 0;
    if (!(flag & BAM_FUNMAP)) {
        bam_cigar2rqlens(static_cast<int>(l_cigar >> 2),
                         reinterpret_cast<const uint32_t*>(&columns_[ci(ColumnType::cigar)].data[cigar_beg]),
                         &rlen, &qlen);
    }
    if (rlen == 0) rlen = 1;

    aln->core.pos    = pos;
    aln->core.tid    = refID;
    aln->core.bin    = hts_reg2bin(pos, pos + rlen, 14, 5);
    aln->core.qual   = static_cast<uint8_t>(mapq);
    aln->core.l_extranul = 0;
    aln->core.flag   = static_cast<uint16_t>(flag);
    aln->core.l_qname = static_cast<uint16_t>(l_qname);
    aln->core.n_cigar = static_cast<uint32_t>(l_cigar >> 2);
    aln->core.l_qseq  = static_cast<int32_t>(l_qual);
    aln->core.mtid    = next_ref;
    aln->core.mpos    = next_pos;
    aln->core.isize   = tlen;
}
