#pragma once

#include "codec.hpp"
#include "defs.hpp"
#include <array>
#include <cstdint>
#include <memory>
#include <vector>
#include <htslib/sam.h>

class Reader {
public:
    // Takes ownership of the mmap'd buffer pointer (caller munmaps it).
    explicit Reader(const char* mmaped_file);
    ~Reader();

    int64_t rec_num() const { return rec_num_; }
    bam_hdr_t* header() const { return header_; }

    void read_record(int64_t rec_num, bam1_t* aln);

private:
    void fetch_field(int64_t rec_num, int col);

    const char* mmaped_file_;
    int64_t rec_num_ = 0;
    bam_hdr_t* header_ = nullptr;

    std::array<Column, COLUMNTYPE_SIZE>                       columns_;
    std::array<std::vector<ColumnChunkMeta>, COLUMNTYPE_SIZE> metadatas_;

    // Per-column chunk bookkeeping
    std::array<std::vector<int64_t>, COLUMNTYPE_SIZE> record_counts_per_chunk_;
    std::array<int64_t, COLUMNTYPE_SIZE> currently_loaded_chunk_{};
    std::array<int64_t, COLUMNTYPE_SIZE> m_chunk_memory_{};
    std::array<int64_t, COLUMNTYPE_SIZE> loaded_since_rec_num_{};
    std::array<int64_t, COLUMNTYPE_SIZE> loaded_up_to_rec_num_{};
};
