#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

struct Codec {
    virtual ~Codec() = default;
    virtual std::vector<uint8_t> compress(const uint8_t* data, size_t size) const = 0;
    virtual void decompress(const uint8_t* src, size_t src_size,
                            uint8_t* dst, size_t dst_size) const = 0;
    virtual std::string name() const = 0;
};

std::unique_ptr<Codec> codec_from_name(const std::string& name);
