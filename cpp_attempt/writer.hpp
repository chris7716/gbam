#pragma once

#include "codec.hpp"
#include "defs.hpp"
#include <array>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
#include <htslib/sam.h>

class Writer {
public:
    Writer(FILE* fd, bam_hdr_t* header, const std::string& codec_map_path = "codec_map.json");
    ~Writer();

    // Write one BAM alignment record into the columnar buffers.
    void write_record(const bam1_t* aln);

    // Flush remaining data and write metadata + SAM header. Called by destructor.
    void close();

private:
    void init_columns();
    void load_codec_map(const std::string& path);
    void dump_columns(bool force);
    void dump_column(int i);

    FILE* fd_;
    bam_hdr_t* header_;
    bool closed_ = false;

    std::array<Column, COLUMNTYPE_SIZE>                    columns_;
    std::array<std::vector<ColumnChunkMeta>, COLUMNTYPE_SIZE> metadatas_;
    std::array<int64_t, COLUMNTYPE_SIZE>                   cur_chunk_rec_num_{};
    std::array<std::unique_ptr<Codec>, COLUMNTYPE_SIZE>    codecs_;
};
