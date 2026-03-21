#include "codec.hpp"

#include <brotli/decode.h>
#include <brotli/encode.h>
#include <lz4.h>
#include <stdexcept>
#include <zlib.h>
#include <zstd.h>

// ── None ─────────────────────────────────────────────────────────────────────

struct NoneCodec : Codec {
    std::string name() const override { return "none"; }

    std::vector<uint8_t> compress(const uint8_t* data, size_t size) const override {
        return std::vector<uint8_t>(data, data + size);
    }

    void decompress(const uint8_t* src, size_t src_size,
                    uint8_t* dst, size_t /*dst_size*/) const override {
        std::copy(src, src + src_size, dst);
    }
};

// ── Zlib ─────────────────────────────────────────────────────────────────────

struct ZlibCodec : Codec {
    std::string name() const override { return "zlib"; }

    std::vector<uint8_t> compress(const uint8_t* data, size_t size) const override {
        uLongf bound = compressBound(size);
        std::vector<uint8_t> out(bound);
        if (::compress(out.data(), &bound,
                       reinterpret_cast<const Bytef*>(data), size) != Z_OK) {
            throw std::runtime_error("zlib compress failed");
        }
        out.resize(bound);
        return out;
    }

    void decompress(const uint8_t* src, size_t src_size,
                    uint8_t* dst, size_t dst_size) const override {
        uLongf out_size = dst_size;
        if (uncompress(reinterpret_cast<Bytef*>(dst), &out_size,
                       reinterpret_cast<const Bytef*>(src), src_size) != Z_OK) {
            throw std::runtime_error("zlib decompress failed");
        }
    }
};

// ── LZ4 ──────────────────────────────────────────────────────────────────────

struct Lz4Codec : Codec {
    std::string name() const override { return "lz4"; }

    std::vector<uint8_t> compress(const uint8_t* data, size_t size) const override {
        int bound = LZ4_compressBound(static_cast<int>(size));
        std::vector<uint8_t> out(bound);
        int n = LZ4_compress_default(reinterpret_cast<const char*>(data),
                                     reinterpret_cast<char*>(out.data()),
                                     static_cast<int>(size), bound);
        if (n <= 0) throw std::runtime_error("lz4 compress failed");
        out.resize(n);
        return out;
    }

    void decompress(const uint8_t* src, size_t src_size,
                    uint8_t* dst, size_t dst_size) const override {
        int n = LZ4_decompress_safe(reinterpret_cast<const char*>(src),
                                    reinterpret_cast<char*>(dst),
                                    static_cast<int>(src_size),
                                    static_cast<int>(dst_size));
        if (n < 0) throw std::runtime_error("lz4 decompress failed");
    }
};

// ── Brotli ────────────────────────────────────────────────────────────────────

struct BrotliCodec : Codec {
    std::string name() const override { return "brotli"; }

    std::vector<uint8_t> compress(const uint8_t* data, size_t size) const override {
        size_t bound = BrotliEncoderMaxCompressedSize(size);
        std::vector<uint8_t> out(bound);
        size_t out_size = bound;
        if (!BrotliEncoderCompress(8, BROTLI_DEFAULT_WINDOW, BROTLI_MODE_GENERIC,
                                   size, data, &out_size, out.data())) {
            throw std::runtime_error("brotli compress failed");
        }
        out.resize(out_size);
        return out;
    }

    void decompress(const uint8_t* src, size_t src_size,
                    uint8_t* dst, size_t dst_size) const override {
        size_t out_size = dst_size;
        if (BrotliDecoderDecompress(src_size, src, &out_size, dst)
                != BROTLI_DECODER_RESULT_SUCCESS || out_size != dst_size) {
            throw std::runtime_error("brotli decompress failed");
        }
    }
};

// ── Zstd ─────────────────────────────────────────────────────────────────────

struct ZstdCodec : Codec {
    std::string name() const override { return "zstd"; }

    std::vector<uint8_t> compress(const uint8_t* data, size_t size) const override {
        size_t bound = ZSTD_compressBound(size);
        std::vector<uint8_t> out(bound);
        size_t n = ZSTD_compress(out.data(), bound, data, size, 3);
        if (ZSTD_isError(n))
            throw std::runtime_error(std::string("zstd compress: ") + ZSTD_getErrorName(n));
        out.resize(n);
        return out;
    }

    void decompress(const uint8_t* src, size_t src_size,
                    uint8_t* dst, size_t dst_size) const override {
        size_t n = ZSTD_decompress(dst, dst_size, src, src_size);
        if (ZSTD_isError(n))
            throw std::runtime_error(std::string("zstd decompress: ") + ZSTD_getErrorName(n));
        if (n != dst_size)
            throw std::runtime_error("zstd decompress: unexpected output size");
    }
};

// ── Factory ───────────────────────────────────────────────────────────────────

std::unique_ptr<Codec> codec_from_name(const std::string& name) {
    if (name == "zlib")   return std::make_unique<ZlibCodec>();
    if (name == "lz4")    return std::make_unique<Lz4Codec>();
    if (name == "brotli") return std::make_unique<BrotliCodec>();
    if (name == "zstd")   return std::make_unique<ZstdCodec>();
    return std::make_unique<NoneCodec>();
}
