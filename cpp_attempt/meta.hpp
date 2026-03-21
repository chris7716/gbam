#pragma once

#include "defs.hpp"
#include <nlohmann/json.hpp>
#include <array>
#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

using Json = nlohmann::json;

// ── Serialise ─────────────────────────────────────────────────────────────────

inline Json chunk_to_json(const ColumnChunkMeta& m) {
    return Json{
        {"rec_num",           m.rec_num},
        {"file_offset",       m.file_offset},
        {"uncompressed_size", m.uncompressed_size},
        {"compressed_size",   m.compressed_size},
        {"codec",             m.codec},
    };
}

// Write the per-column chunk metadata JSON block to fp, then a null byte.
// Also writes the leading file-offset JSON header at byte 0.
inline void write_meta(FILE* fp,
    const std::array<std::vector<ColumnChunkMeta>, COLUMNTYPE_SIZE>& metadatas)
{
    Json root;
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) {
        Json arr = Json::array();
        for (const auto& chunk : metadatas[i])
            arr.push_back(chunk_to_json(chunk));
        root[ColumnTypeNames[i]] = arr;
    }
    std::string s = root.dump(2);
    fwrite(s.c_str(), 1, s.size(), fp);
    fputc('\0', fp);
}

inline void write_header(FILE* fp, int64_t seekpos, int64_t meta_size, int64_t header_len) {
    Json j{
        {"seekpos",    seekpos},
        {"meta_size",  meta_size},
        {"header_len", header_len},
    };
    std::string s = j.dump();
    fwrite(s.c_str(), 1, s.size(), fp);
    fputc('\0', fp);
}

// ── Deserialise ───────────────────────────────────────────────────────────────

inline std::array<std::vector<ColumnChunkMeta>, COLUMNTYPE_SIZE>
parse_meta_from_json_string(const char* json_str)
{
    std::array<std::vector<ColumnChunkMeta>, COLUMNTYPE_SIZE> result;
    Json root = Json::parse(json_str);
    for (int i = 0; i < COLUMNTYPE_SIZE; ++i) {
        const std::string key = ColumnTypeNames[i];
        if (!root.contains(key)) continue;
        for (const auto& node : root[key]) {
            ColumnChunkMeta m;
            m.rec_num           = node["rec_num"].get<int64_t>();
            m.file_offset       = node["file_offset"].get<int64_t>();
            m.uncompressed_size = node["uncompressed_size"].get<int64_t>();
            m.compressed_size   = node["compressed_size"].get<int64_t>();
            m.codec             = node["codec"].get<std::string>();
            result[i].push_back(m);
        }
    }
    return result;
}

// Parse top-level header JSON (first bytes of the file).
// Returns seekpos, meta_size, header_len.
inline void parse_file_header(const char* data,
                               int64_t& seekpos,
                               int64_t& meta_size,
                               int64_t& header_len)
{
    Json j = Json::parse(data);
    seekpos    = j["seekpos"].get<int64_t>();
    meta_size  = j["meta_size"].get<int64_t>();
    header_len = j["header_len"].get<int64_t>();
}
