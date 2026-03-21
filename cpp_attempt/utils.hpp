#pragma once

#include <cstdint>

template<typename T>
inline void write_le(uint8_t* buf, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        buf[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xFF);
    }
}

template<typename T>
inline T read_le(const uint8_t* buf) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(buf[i]) << (8 * i);
    }
    return value;
}
