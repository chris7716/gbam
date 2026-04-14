use crate::graph::gaf::parse_gaf;
use crate::graph::gfa::VariationGraph;
use crate::MEGA_BYTE_SIZE;
use crate::{Codecs, Writer};
use bam_tools::parse_reference_sequences;
use bam_tools::record::bamrawrecord::BAMRawRecord;
use bam_tools::record::fields::{Fields, FIELDS_NUM};
use bam_tools::sorting::sort;
use bam_tools::sorting::sort::TempFilesMode;
use bam_tools::Reader;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tempdir::TempDir;

const MEM_LIMIT: usize = 2000 * MEGA_BYTE_SIZE;

/// Converts BAM file to GBAM file. This uses the `bam_parallel` reader.
pub fn bam_to_gbam(in_path: &str, out_path: &str, codec: Codecs, full_command: String, codec_map_required: bool) {
    let (mut bam_reader, mut writer) =
        get_bam_reader_gbam_writer(in_path, out_path, codec, full_command, false);

    let mut records = bam_reader.records();
    while let Some(Ok(rec)) = records.next_rec() {
        let wrapper = BAMRawRecord(Cow::Borrowed(rec));
        writer.push_record(&wrapper, codec_map_required);
    }

    writer.finish(codec_map_required).unwrap();
}

/// Converts a BAM file to a graph-path encoded GBAM file.
///
/// Each read's sequence is encoded as a list of graph node IDs plus a sparse
/// edit list, rather than raw 4-bit packed bases. The pangenome graph and the
/// per-read path assignments are supplied as separate files:
///
/// - `gfa_path` — GFA v1 file describing the variation graph (nodes only need
///   to be present; edges are optional).
/// - `gaf_path` — GAF file produced by a graph aligner (`vg giraffe`,
///   `GraphAligner`, etc.) aligning the reads in `in_path` to the same graph.
/// - `graph_uri` — URI or relative path stored in the GBAM header so that
///   downstream tools can locate the correct graph at decode time.
///
/// Reads absent from the GAF file (e.g. unmapped reads) fall back to
/// `encode_without_path`, storing all bases as edits. This is lossless but
/// larger than graph-encoded reads.
pub fn bam_to_gbam_with_graph(
    in_path: &str,
    out_path: &str,
    codec: Codecs,
    gfa_path: &str,
    gaf_path: &str,
    graph_uri: &str,
    full_command: String,
) {
    // 1. Parse the variation graph (node sequences)
    let gfa_file = File::open(gfa_path)
        .unwrap_or_else(|e| panic!("Cannot open GFA file '{}': {}", gfa_path, e));
    let graph = Arc::new(
        VariationGraph::from_gfa(gfa_file)
            .unwrap_or_else(|e| panic!("Failed to parse GFA '{}': {}", gfa_path, e)),
    );

    // 2. Parse the GAF alignment file (read name → node ID path)
    let gaf_file = File::open(gaf_path)
        .unwrap_or_else(|e| panic!("Cannot open GAF file '{}': {}", gaf_path, e));
    let path_map = parse_gaf(gaf_file)
        .unwrap_or_else(|e| panic!("Failed to parse GAF '{}': {}", gaf_path, e));

    // 3. Open the BAM file and build the GBAM writer
    let fin = File::open(in_path).expect("Cannot open input BAM file");
    let fout = File::create(out_path).expect("Cannot create output GBAM file");

    let file_size = fin.metadata().unwrap().len();
    let buf_reader = BufReader::new(fin);
    let buf_writer = BufWriter::new(fout);

    let mut bgzf_reader = Reader::new(buf_reader, 4, Some(file_size));
    let (sam_header, ref_seqs, _) = read_sam_header_and_ref_seqs(&mut bgzf_reader);

    let mut writer = Writer::new_with_graph(
        buf_writer,
        codec,
        8,
        ref_seqs,
        sam_header,
        full_command,
        false,
        graph_uri.to_string(),
        graph,
        path_map,
    );

    // 4. Stream BAM records into the graph-path writer
    let mut records = bgzf_reader.records();
    while let Some(Ok(rec)) = records.next_rec() {
        let wrapper = BAMRawRecord(Cow::Borrowed(rec));
        writer.push_record(&wrapper, false);
    }

    writer.finish(false).unwrap();
}

/// Converts BAM file to GBAM file. Sorts BAM file in process. This uses the `bam_parallel` reader.
pub fn bam_sort_to_gbam(
    in_path: &str,
    out_path: &str,
    codec: Codecs,
    mut sort_temp_mode: Option<String>,
    temp_dir: Option<PathBuf>,
    full_command: String,
    index_sort: bool,
    codec_map_required: bool
) {
    let fin_for_ref_seqs = File::open(in_path).expect("failed");

    let mut reader_for_header_only = Reader::new(fin_for_ref_seqs, 1, None);
    let (sam_header, ref_seqs, _) = read_sam_header_and_ref_seqs(&mut reader_for_header_only);

    let fin = File::open(in_path).expect("failed");
    let fout = File::create(out_path).expect("failed");

    let file_size = fin.metadata().unwrap().len();

    let buf_reader = BufReader::new(fin);
    let buf_writer = BufWriter::new(fout);

    let mut writer = Writer::new(
        buf_writer,
        vec![codec; FIELDS_NUM],
        8,
        vec![Fields::RefID],
        ref_seqs,
        sam_header,
        full_command,
        true,
        codec_map_required
    );

    let tmp_dir_path = temp_dir.map_or(std::env::temp_dir(), |path| path);
    if sort_temp_mode.is_none() {
        sort_temp_mode = Some(String::from_str("file").unwrap());
    }
    let tmp_medium_mode = match sort_temp_mode.unwrap().as_str() {
        "file" => TempFilesMode::RegularFiles,
        "lz4_file" => TempFilesMode::LZ4CompressedFiles,
        "ram" => TempFilesMode::InMemoryBlocks,
        "lz4_ram" => TempFilesMode::InMemoryBlocksLZ4,
        _ => panic!("Unknown sort_temp_mode mode."),
    };

    let index_file = if index_sort {
        Some(BufWriter::with_capacity(
            33_554_432,
            File::create(out_path.to_owned() + ".gbai").unwrap(),
        ))
    } else {
        None
    };

    let dir = TempDir::new_in(tmp_dir_path, "BAM sort temporary directory.").unwrap();

    sort::sort_bam(
        MEM_LIMIT,
        buf_reader,
        &mut writer,
        &dir,
        0,
        8,
        tmp_medium_mode,
        index_file,
        sort::SortBy::CoordinatesAndStrand,
        Some(file_size),
    )
    .unwrap();

    writer.finish(false).unwrap();
}

/// Consumes SAM header from input BAM reader.
///
///
/// # Returns
/// Tuple of 3 elements.
///
/// **tuple.0** -> all bytes of BAM header including reference sequences.
///
/// **tuple.1** -> parsed reference sequences from BAM header.
///
/// **tuple.2** -> offset to reference sequences in tuple.0. It's before n_ref uint32_t.
fn read_sam_header_and_ref_seqs(reader: &mut Reader) -> (Vec<u8>, Vec<(String, u32)>, usize) {
    let (bytes_of_header, ref_sequences_offset) = reader.read_header().unwrap();
    let sequences = parse_reference_sequences(&bytes_of_header[ref_sequences_offset..]).unwrap();
    (bytes_of_header, sequences, ref_sequences_offset)
}

fn get_bam_reader_gbam_writer(
    in_path: &str,
    out_path: &str,
    codec: Codecs,
    full_command: String,
    codec_map_required: bool
) -> (Reader, Writer<BufWriter<File>>) {
    let fin = File::open(in_path).expect("failed");
    let fout = File::create(out_path).expect("failed");

    let file_size = fin.metadata().unwrap().len();

    let buf_reader = BufReader::new(fin);
    let buf_writer = BufWriter::new(fout);

    let mut bgzf_reader = Reader::new(buf_reader, 4, Some(file_size));

    let (sam_header, ref_seqs, _) = read_sam_header_and_ref_seqs(&mut bgzf_reader);

    let writer = Writer::new(
        buf_writer,
        vec![codec; FIELDS_NUM],
        8,
        vec![Fields::RefID],
        ref_seqs,
        sam_header,
        full_command,
        false,
        codec_map_required
    );

    (bgzf_reader, writer)
}
