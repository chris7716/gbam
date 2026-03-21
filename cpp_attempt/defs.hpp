#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <vector>
#include <htslib/sam.h>

static constexpr int64_t MAX_COLUMN_CHUNK_SIZE = 1024 * 1024 * 6; // 6 MB

static constexpr const char* GBAM_MAGIC = "geeBAM20";

enum class ColumnType : int {
    refID = 0,
    pos,
    mapq,
    bin,
    flag,
    next_refID,
    next_pos,
    tlen,
    read_name,
    cigar,
    seq,
    qual,
    tags,
    // Index columns for variable-length fields
    index_read_name,
    index_cigar,
    index_seq,
    index_tags,
    index_qual,
    SIZE
};

static constexpr int COLUMNTYPE_SIZE = static_cast<int>(ColumnType::SIZE);

static constexpr const char* ColumnTypeNames[] = {
    "COLUMNTYPE_refID",
    "COLUMNTYPE_pos",
    "COLUMNTYPE_mapq",
    "COLUMNTYPE_bin",
    "COLUMNTYPE_flag",
    "COLUMNTYPE_next_refID",
    "COLUMNTYPE_next_pos",
    "COLUMNTYPE_tlen",
    "COLUMNTYPE_read_name",
    "COLUMNTYPE_cigar",
    "COLUMNTYPE_seq",
    "COLUMNTYPE_qual",
    "COLUMNTYPE_tags",
    "COLUMNTYPE_index_read_name",
    "COLUMNTYPE_index_cigar",
    "COLUMNTYPE_index_seq",
    "COLUMNTYPE_index_tags",
    "COLUMNTYPE_index_qual",
};

struct ColumnChunkMeta {
    int64_t rec_num          = 0;
    int64_t file_offset      = 0;
    int64_t uncompressed_size = 0;
    int64_t compressed_size  = 0;
    std::string codec;
};

struct Column {
    std::vector<uint8_t> data;
    int64_t cur_ptr      = 0;
    Column* index_column = nullptr;

    Column() { data.resize(MAX_COLUMN_CHUNK_SIZE); }

    void ensure_capacity(int64_t needed_size) {
        if (cur_ptr + needed_size <= static_cast<int64_t>(data.size())) return;
        int64_t new_cap = data.size();
        while (cur_ptr + needed_size > new_cap) new_cap *= 2;
        data.resize(new_cap);
    }

    void write_index() {
        // Append cur_ptr of this column as a little-endian int32 into index_column
        if (!index_column) return;
        index_column->ensure_capacity(sizeof(int32_t));
        int32_t v = static_cast<int32_t>(cur_ptr);
        index_column->data[index_column->cur_ptr + 0] =  v        & 0xFF;
        index_column->data[index_column->cur_ptr + 1] = (v >>  8) & 0xFF;
        index_column->data[index_column->cur_ptr + 2] = (v >> 16) & 0xFF;
        index_column->data[index_column->cur_ptr + 3] = (v >> 24) & 0xFF;
        index_column->cur_ptr += sizeof(int32_t);
    }

    void reset() { cur_ptr = 0; }
};
