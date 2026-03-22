#include "reader.hpp"
#include "writer.hpp"
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <htslib/sam.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// ── RAII wrappers for htslib handles ─────────────────────────────────────────

struct SamFileGuard {
    samFile* p = nullptr;
    ~SamFileGuard() { if (p) sam_close(p); }
};

struct BamAlnGuard {
    bam1_t* p = bam_init1();
    ~BamAlnGuard() { if (p) bam_destroy1(p); }
};

struct BamHdrGuard {
    bam_hdr_t* p = nullptr;
    ~BamHdrGuard() { if (p) bam_hdr_destroy(p); }
};

// ── Progress / ETA ────────────────────────────────────────────────────────────

static void print_progress_eta(int count, time_t start_time) {
    if (count % 1000 != 0) return;
    time_t now = time(nullptr);
    double elapsed = difftime(now, start_time);
    if (elapsed < 1.0) elapsed = 1.0;
    double rps = count / elapsed;
    int est_total     = static_cast<int>(elapsed > 5 ? count * 1.3 : count * 2.0);
    int eta_seconds   = static_cast<int>((est_total - count) / rps);
    std::fprintf(stderr, "\rProcessed: %d | Speed: %.1f rec/s | ETA: %02d:%02d",
                 count, rps, eta_seconds / 60, eta_seconds % 60);
    std::fflush(stderr);
}

// ── Write mode (stdin → .gbam) ────────────────────────────────────────────────

static int write_gbam(const char* file_path) {
    FILE* fp = std::fopen(file_path, "wb");
    if (!fp) { std::perror("Failed to open file for writing"); return 1; }

    SamFileGuard in; in.p = sam_open("-", "r");
    if (!in.p) { std::cerr << "Failed to open stdin\n"; std::fclose(fp); return 1; }
    if (hts_set_threads(in.p, 8) != 0) { std::fclose(fp); return 1; }

    BamHdrGuard hdr; hdr.p = sam_hdr_read(in.p);
    if (!hdr.p) { std::cerr << "Failed to read header\n"; std::fclose(fp); return 1; }

    BamAlnGuard aln;
    if (!aln.p) { std::cerr << "Failed to init alignment\n"; std::fclose(fp); return 1; }

    try {
        Writer writer(fp, hdr.p, "codec_map.json");
        time_t start = time(nullptr);
        int count = 0;
        while (sam_read1(in.p, hdr.p, aln.p) >= 0) {
            writer.write_record(aln.p);
            print_progress_eta(++count, start);
        }
        std::fprintf(stderr, "\nCompleted writing %d records.\n", count);
        // writer.close() is called by destructor
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        std::fclose(fp);
        return 1;
    }

    std::fclose(fp);
    return 0;
}

// ── Read mode (.gbam → stdout SAM) ───────────────────────────────────────────

static int read_gbam(const char* file_path) {
    int fd = open(file_path, O_RDONLY);
    if (fd == -1) { std::perror("Error opening file"); return 1; }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* mapped = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0));
    if (mapped == MAP_FAILED) { std::perror("Error mapping file"); close(fd); return 1; }

    try {
        Reader reader(mapped);
        BamAlnGuard aln;

        // Write SAM header directly — avoids htslib format-detection picking BAM
        std::fwrite(sam_hdr_str(reader.header()), 1,
                    sam_hdr_length(reader.header()), stdout);

        kstring_t str = {0, 0, nullptr};
        for (int64_t i = 0; i < reader.rec_num(); ++i) {
            reader.read_record(i, aln.p);
            sam_format1(reader.header(), aln.p, &str);
            std::fwrite(str.s, 1, str.l, stdout);
            std::fputc('\n', stdout);
            str.l = 0;
        }
        free(str.s);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
        munmap(mapped, file_size);
        close(fd);
        return 1;
    }

    munmap(mapped, file_size);
    close(fd);
    return 0;
}

// ── Entry point ───────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::fprintf(stderr,
            "Usage: %s <file>.gbam\n"
            "Pipe SAM/BAM into the tool to convert to GBAM, "
            "or pass a .gbam file to convert back to SAM.\n", argv[0]);
        return 1;
    }
    return isatty(fileno(stdin)) ? read_gbam(argv[1]) : write_gbam(argv[1]);
}
