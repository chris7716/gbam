# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GBAM is a bioinformatics tool implementing a **column-oriented binary file format** for genomic alignment data. Unlike row-oriented BAM files, GBAM stores data by column (field), enabling faster operations and better compression when only a subset of fields are needed.

## Build Commands

```bash
# Build the release binary
cargo build --release
# Output: ./target/release/gbam_binary

# Build debug
cargo build
```

## Running Tests

Tests are Python-based using pytest:

```bash
# Run all tests
pytest

# Run a specific test
pytest -k test_bam_to_gbam_to_bam

# Run with a specific BAM file
pytest --bam-file-path=/path/to/test.bam

# Enable depth tests
pytest --depth_test
```

Default test data: `./test_data/little.bam`. Test files are in `./tests/`.

## Rust Tooling

```bash
cargo fmt        # Format code
cargo clippy     # Lint
cargo test       # Unit tests
```

## Architecture

The project is a Cargo workspace with three crates:

### `bam_tools` — BAM format primitives
- `record/fields.rs` — Defines the `Fields` enum (18 fields: RefID, Pos, Mapq, Flags, Cigar, Sequence, Quality, Tags, etc., plus index fields like LName, NCigar)
- `record/bamrawrecord.rs` — Raw BAM record parsing from bytes
- `sorting/` — Record sorting with configurable comparators (RAM/LZ4/File temp modes)
- `reader.rs` / `gz.rs` — BAM block-level I/O and gzip block handling

### `gbam_tools` — GBAM format operations
- `meta.rs` — File metadata, `Codecs` enum (Gzip, Lz4, NoCompression), format constants (magic: `"geeBAM10"`)
- `writer.rs` — Column-oriented writer; aggregates records per field into blocks, then compresses via `Compressor`
- `compressor.rs` — Parallel block compression using `rayon`
- `reader/` — Column-selective reader; uses `ParsingTemplate` to load only requested fields
- `bam/bam_to_gbam.rs` / `bam/gbam_to_bam.rs` — Bidirectional BAM↔GBAM converters
- `query/` — Analysis operations: `depth.rs`, `flagstat.rs`, `cigar.rs`, `markdup.rs`

### `gbam_binary` — CLI entry point
- `main.rs` — `structopt`-based CLI with subcommands: `--convert-to-gbam`, `--convert-to-bam`, `--sort`, `--flagstat`, `--depth`, `--query`, `--view`, `--header`, `--patch-gbam-with-dups`

### Data flow

```
BAM file → bam_tools::Reader → BAMRawRecord
                                    ↓
                            gbam_tools::Writer → Column buffers → Compressor → GBAM file

GBAM file → gbam_tools::Reader (ParsingTemplate selects columns) → GbamRecord
```

### Key design choices
- **Column-oriented storage**: Each BAM field stored as a separate compressed column, so operations like `flagstat` only decompress the Flags column.
- **Block compression**: Data grouped into fixed-size blocks (except last); each block compressed independently (gzip or lz4).
- **Variable-size fields** (Cigar, Sequence, Quality, Tags, ReadName) have a separate index column storing byte offsets.
- **Parallel compression**: `rayon`-based `Compressor` compresses blocks concurrently.
- File integrity via CRC32; metadata stored as JSON within the file.

## C++ / C Alternatives

There are experimental alternative implementations:
- `cpp_attempt/` — C++ version (CMake build)
- `c_attempt/` — C version (Makefile)

These are secondary to the main Rust implementation.
