use super::meta::{BlockMeta, Codecs, FileInfo, FileMeta, Stat, FILE_INFO_SIZE};
use crate::compressor::{CompressTask, Compressor, OrderingKey};
use crate::graph::gaf::PathInfo;
use crate::graph::gfa::VariationGraph;
use crate::graph::path_codec::{compute_edits, encode_without_path, GraphPathEntry};
use crate::{SIZE_LIMIT, U32_SIZE};
use bam_tools::record::bamrawrecord::{decode_seq, BAMRawRecord};
use bam_tools::record::fields::{
    field_type, is_data_field, var_size_field_to_index, FieldType, Fields,
};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;
use once_cell::sync::Lazy;
use std::fs;
use serde_json::Value;
use std::str::FromStr;

pub(crate) struct BlockInfo {
    pub numitems: u32,
    pub uncompr_size: usize,
    pub field: Fields,
    pub stats: Option<Stat>,
    pub codec: Codecs,
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            numitems: 0,
            uncompr_size: 0,
            field: Fields::RefID,
            stats: None,
            codec: Codecs::Brotli,
        }
    }
}

pub static FIELD_CODEC_MAP: Lazy<HashMap<Fields, Codecs>> = Lazy::new(|| {
    let path = std::env::var("CODEC_MAP_PATH").unwrap_or_else(|_| "codec_map.json".to_string());
    let json_str = fs::read_to_string(path).expect("Failed to read codec_map.json");

    let parsed: Value = serde_json::from_str(&json_str).expect("Invalid JSON format");

    let mut map = HashMap::new();
    for (field_str, codec_str) in parsed.as_object().unwrap() {
        let field = Fields::from_str(field_str)
            .unwrap_or_else(|_| panic!("Unknown field in JSON: {}", field_str));
        let codec = match codec_str.as_str().unwrap() {
            "Brotli" => Codecs::Brotli,
            "Zstd" => Codecs::Zstd,
            "Lz4" => Codecs::Lz4,
            "Gzip" => Codecs::Gzip,
            "NoCompression" => Codecs::NoCompression,
            other => panic!("Unsupported codec: {}", other),
        };
        map.insert(field, codec);
    }
    map
});

pub struct Writer<WS>
where
    WS: Write + Seek,
{
    file_info: FileInfo,
    file_meta: FileMeta,
    columns: Vec<Box<dyn Column>>,
    /// Present only for graph-path encoded files.
    graph_bundle: Option<GraphPathBundle>,
    compressor: Compressor,
    inner: WS,
}

impl<WS> Writer<WS>
where
    WS: Write + Seek,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut inner: WS,
        codecs: Vec<Codecs>,
        thread_num: usize,
        collect_stats_for: Vec<Fields>,
        ref_seqs: Vec<(String, u32)>,
        sam_header: Vec<u8>,
        full_command: String,
        is_sorted: bool,
        codec_map_required: bool
    ) -> Self {
        inner
            .seek(SeekFrom::Start((FILE_INFO_SIZE) as u64))
            .unwrap();

        let columns = build_standard_columns(&collect_stats_for, false);

        Self {
            file_meta: FileMeta::new(codecs[0], ref_seqs, sam_header, codec_map_required),
            inner,
            compressor: Compressor::new(thread_num),
            columns,
            graph_bundle: None,
            file_info: FileInfo::new([1, 0], 0, 0, full_command, is_sorted),
        }
    }

    pub fn new_no_stats(
        inner: WS,
        codecs: Vec<Codecs>,
        thread_num: usize,
        ref_seqs: Vec<(String, u32)>,
        sam_header: Vec<u8>,
        full_command: String,
        is_sorted: bool,
    ) -> Self {
        Self::new(
            inner,
            codecs,
            thread_num,
            Vec::new(),
            ref_seqs,
            sam_header,
            full_command,
            is_sorted,
            false
        )
    }

    /// Construct a writer that stores read sequences in dedicated graph-path
    /// columns (PathNodeIds, EditOffsets, EditBases, etc.) instead of RawSequence.
    pub fn new_with_graph(
        mut inner: WS,
        codec: Codecs,
        thread_num: usize,
        ref_seqs: Vec<(String, u32)>,
        sam_header: Vec<u8>,
        full_command: String,
        is_sorted: bool,
        pangenome_graph_uri: String,
        graph: Arc<VariationGraph>,
        path_map: HashMap<String, PathInfo>,
    ) -> Self {
        inner
            .seek(SeekFrom::Start(FILE_INFO_SIZE as u64))
            .unwrap();

        // Build standard columns but skip RawSequence (handled by graph bundle).
        let columns = build_standard_columns_except(&[], true);

        Self {
            file_meta: FileMeta::new_with_graph(codec, ref_seqs, sam_header, pangenome_graph_uri),
            inner,
            compressor: Compressor::new(thread_num),
            columns,
            graph_bundle: Some(GraphPathBundle::new(graph, path_map)),
            file_info: FileInfo::new([1, 0], 0, 0, full_command, is_sorted),
        }
    }

    pub fn push_record(&mut self, record: &BAMRawRecord, codec_map_required: bool) {
        for col in self.columns.iter_mut() {
            while let WriteStatus::Full(inner) = col.write_record_field(record) {
                flush_field_buffer(
                    &mut self.inner,
                    &mut self.file_meta,
                    &mut self.compressor,
                    inner,
                    codec_map_required
                );
            }
        }
        if self.graph_bundle.is_some() {
            push_bundle_record(
                self.graph_bundle.as_mut().unwrap(),
                record,
                &mut self.inner,
                &mut self.file_meta,
                &mut self.compressor,
                codec_map_required,
            );
        }
    }

    pub fn finish(&mut self, codec_map_required: bool) -> std::io::Result<u64> {
        // Flush standard columns.
        let mut columns: Vec<Box<dyn Column>> = self.columns.drain(..).collect();
        for (inner, idx) in columns.iter_mut().map(|col| col.get_inners()) {
            let writer = &mut self.inner;
            let meta = &mut self.file_meta;
            let compress = &mut self.compressor;

            flush_field_buffer(writer, meta, compress, inner, codec_map_required);
            if let Some(idx_inner) = idx {
                flush_field_buffer(writer, meta, compress, idx_inner, codec_map_required);
            }
        }

        // Flush graph bundle columns.
        if let Some(ref mut bundle) = self.graph_bundle {
            bundle.flush_all(
                &mut self.inner,
                &mut self.file_meta,
                &mut self.compressor,
                codec_map_required,
            );
        }

        for mut task in self.compressor.finish() {
            if let OrderingKey::Key(key) = task.ordering_key {
                write_data_and_update_meta(&mut self.inner, &mut self.file_meta, key, &mut task);
            }
        }

        let meta_start_pos = self.inner.stream_position()?;
        let main_meta = serde_json::to_string(&self.file_meta).unwrap();
        let main_meta_bytes = main_meta.as_bytes();
        let crc32 = calc_crc_for_meta_bytes(main_meta_bytes);
        self.inner.write_all(main_meta_bytes)?;

        let total_bytes_written = self.inner.stream_position()?;
        self.inner.seek(SeekFrom::Start(0)).unwrap();
        self.inner.write_all(&[0; FILE_INFO_SIZE]).unwrap();
        self.inner.seek(SeekFrom::Start(0)).unwrap();
        let file_info = &mut self.file_info;
        file_info.seekpos = meta_start_pos;
        file_info.crc32 = crc32;
        let file_info_bytes = serde_json::to_string(&file_info).unwrap();
        self.inner.write_all(file_info_bytes.as_bytes())?;
        Ok(total_bytes_written)
    }
}

/// Build the standard per-BAM-field column list, excluding graph-path fields.
/// `skip_raw_sequence`: when true, omit RawSequence + its RawSeqLen index
/// (used for graph-encoded writers where the bundle handles sequencing).
fn build_standard_columns(
    collect_stats_for: &[Fields],
    skip_raw_sequence: bool,
) -> Vec<Box<dyn Column>> {
    let mut columns = Vec::new();
    for field in Fields::iterator().filter(|f| is_data_field(f)) {
        if skip_raw_sequence && *field == Fields::RawSequence {
            continue;
        }
        let stat_collector = collect_stats_for
            .iter()
            .find(|f| *f == field)
            .and(Some(Stat::default()));
        let col = match field_type(field) {
            FieldType::FixedSized => {
                Box::new(FixedColumn::new(*field, stat_collector)) as Box<dyn Column>
            }
            FieldType::VariableSized => {
                Box::new(VariableColumn::new(*field, stat_collector)) as Box<dyn Column>
            }
        };
        columns.push(col);
    }
    columns
}

fn build_standard_columns_except(
    _extra_skip: &[Fields],
    skip_raw_sequence: bool,
) -> Vec<Box<dyn Column>> {
    build_standard_columns(&[], skip_raw_sequence)
}

// ---------------------------------------------------------------------------
// Graph-path bundle: manages PathNodeIds, PathStart, EditOffsets, EditBases
// ---------------------------------------------------------------------------

struct GraphPathBundle {
    path_start_inner: Inner,

    node_ids_inner: Inner,
    node_ids_idx_inner: Inner,

    edit_offsets_inner: Inner,
    edit_offsets_idx_inner: Inner,

    edit_bases_inner: Inner,
    // EditBases index is derived from EditCounts (divided by 4), no separate index needed

    graph: Arc<VariationGraph>,
    path_map: HashMap<String, PathInfo>,
}

impl GraphPathBundle {
    fn new(graph: Arc<VariationGraph>, path_map: HashMap<String, PathInfo>) -> Self {
        Self {
            path_start_inner: Inner::new(Fields::PathStart, None),
            node_ids_inner: Inner::new(Fields::PathNodeIds, None),
            node_ids_idx_inner: Inner::new(Fields::NodeCounts, None),
            edit_offsets_inner: Inner::new(Fields::EditOffsets, None),
            edit_offsets_idx_inner: Inner::new(Fields::EditCounts, None),
            edit_bases_inner: Inner::new(Fields::EditBases, None),
            graph,
            path_map,
        }
    }

    fn make_entry(&self, rec: &BAMRawRecord) -> GraphPathEntry {
        let raw_seq_bytes = rec.get_bytes(&Fields::RawSequence);
        let mut read_seq_str = String::new();
        decode_seq(raw_seq_bytes, &mut read_seq_str);
        let read_seq = read_seq_str.as_bytes();

        let name_bytes = rec.get_bytes(&Fields::ReadName);
        let name = std::str::from_utf8(
            name_bytes.strip_suffix(b"\0").unwrap_or(name_bytes)
        ).unwrap_or("");

        let flag_bytes = rec.get_bytes(&Fields::Flags);
        let flag = u16::from_le_bytes([flag_bytes[0], flag_bytes[1]]);
        let suffix = if flag & 0x01 != 0 {
            if flag & 0x40 != 0 { "/1" } else { "/2" }
        } else {
            ""
        };

        let path_info = if suffix.is_empty() {
            self.path_map.get(name)
        } else {
            let suffixed = format!("{}{}", name, suffix);
            self.path_map.get(suffixed.as_str()).or_else(|| self.path_map.get(name))
        };

        match path_info {
            Some(info) => {
                use crate::graph::gaf::REVERSE_BIT;
                use crate::graph::path_codec::rev_comp;
                let full_path_seq: Vec<u8> = info.node_ids
                    .iter()
                    .flat_map(|&encoded_id| {
                        let is_reverse = encoded_id & REVERSE_BIT != 0;
                        let node_id = encoded_id & !REVERSE_BIT;
                        let seq = self.graph.node_seq(node_id).unwrap_or(&[]);
                        if is_reverse { rev_comp(seq) } else { seq.to_vec() }
                    })
                    .collect();
                let path_start = info.path_start as usize;
                let aligned = full_path_seq.get(path_start..).unwrap_or(&[]);
                let edits = compute_edits(aligned, read_seq);
                GraphPathEntry::new(info.path_start, info.node_ids.clone(), edits)
            }
            None => encode_without_path(read_seq),
        }
    }

    fn flush_all<WS: Write + Seek>(
        &mut self,
        writer: &mut WS,
        file_meta: &mut FileMeta,
        compressor: &mut Compressor,
        codec_map_required: bool,
    ) {
        flush_field_buffer(writer, file_meta, compressor, &mut self.path_start_inner, codec_map_required);
        flush_field_buffer(writer, file_meta, compressor, &mut self.node_ids_inner, codec_map_required);
        flush_field_buffer(writer, file_meta, compressor, &mut self.node_ids_idx_inner, codec_map_required);
        flush_field_buffer(writer, file_meta, compressor, &mut self.edit_offsets_inner, codec_map_required);
        flush_field_buffer(writer, file_meta, compressor, &mut self.edit_offsets_idx_inner, codec_map_required);
        flush_field_buffer(writer, file_meta, compressor, &mut self.edit_bases_inner, codec_map_required);
    }
}

fn push_bundle_record<WS: Write + Seek>(
    bundle: &mut GraphPathBundle,
    rec: &BAMRawRecord,
    writer: &mut WS,
    file_meta: &mut FileMeta,
    compressor: &mut Compressor,
    codec_map_required: bool,
) {
    let entry = bundle.make_entry(rec);
    let is_raw_fallback = entry.node_ids.is_empty();

    // PathStart (fixed u32)
    let ps_bytes = entry.path_start.to_le_bytes();
    if bundle.path_start_inner.flush_required(&ps_bytes) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.path_start_inner, codec_map_required);
    }
    bundle.path_start_inner.write_data(&ps_bytes);

    // PathNodeIds (variable) + NodeCounts index
    let node_ids_bytes: Vec<u8> = if is_raw_fallback {
        Vec::new()
    } else {
        entry.node_ids.iter().flat_map(|&id| id.to_le_bytes()).collect()
    };
    let mut idx_buf = [0u8; U32_SIZE];
    if bundle.node_ids_idx_inner.flush_required(&idx_buf) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.node_ids_idx_inner, codec_map_required);
    }
    if bundle.node_ids_inner.flush_required(&node_ids_bytes) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.node_ids_inner, codec_map_required);
    }
    bundle.node_ids_inner.write_data(&node_ids_bytes);
    (&mut idx_buf[..]).write_u32::<LittleEndian>(bundle.node_ids_inner.offset as u32).unwrap();
    bundle.node_ids_idx_inner.write_data(&idx_buf);

    // EditOffsets (variable) + EditCounts index
    // Raw fallback: no offsets stored (positions are implicit 0..seq_len).
    let edit_offsets_bytes: Vec<u8> = if is_raw_fallback {
        Vec::new()
    } else {
        entry.edits.iter().flat_map(|e| e.read_offset.to_le_bytes()).collect()
    };
    if bundle.edit_offsets_idx_inner.flush_required(&idx_buf) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.edit_offsets_idx_inner, codec_map_required);
    }
    if bundle.edit_offsets_inner.flush_required(&edit_offsets_bytes) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.edit_offsets_inner, codec_map_required);
    }
    bundle.edit_offsets_inner.write_data(&edit_offsets_bytes);
    (&mut idx_buf[..]).write_u32::<LittleEndian>(bundle.edit_offsets_inner.offset as u32).unwrap();
    bundle.edit_offsets_idx_inner.write_data(&idx_buf);

    // EditBases (variable) - index derived from EditCounts / 4
    // For graph-path reads: sparse edit bases.
    // For raw fallback: all bases stored here directly.
    let edit_bases_bytes: Vec<u8> = entry.edits.iter().map(|e| e.read_base).collect();
    if bundle.edit_bases_inner.flush_required(&edit_bases_bytes) {
        flush_field_buffer(writer, file_meta, compressor, &mut bundle.edit_bases_inner, codec_map_required);
    }
    bundle.edit_bases_inner.write_data(&edit_bases_bytes);
}

// ---------------------------------------------------------------------------
// Shared infrastructure
// ---------------------------------------------------------------------------

pub(crate) fn flush_field_buffer<WS: Write + Seek>(
    writer: &mut WS,
    file_meta: &mut FileMeta,
    compressor: &mut Compressor,
    inner: &mut Inner,
    codec_map_required: bool
) {
    let data = std::mem::take(&mut inner.buffer);
    let field = &inner.field;
    let codec = *file_meta.get_field_codec(field);

    compressor.compress_block(
        OrderingKey::Key(inner.block_num),
        inner.generate_block_info(codec_map_required, codec),
        data,
    );

    let mut completed_task = compressor.get_compr_block();

    if let OrderingKey::Key(key) = completed_task.ordering_key {
        write_data_and_update_meta(writer, file_meta, key, &mut completed_task);
    }

    inner.buffer = completed_task.buf;
    inner.reset_for_new_block();
}

fn write_data_and_update_meta<WS: Write + Seek>(
    writer: &mut WS,
    file_meta: &mut FileMeta,
    key: u64,
    task: &mut CompressTask,
) {
    let compressed_size = task.buf.len();
    let meta = generate_meta(
        writer,
        &mut task.block_info,
        compressed_size.try_into().unwrap(),
    );

    writer.write_all(&task.buf).unwrap();

    let field_meta = file_meta.get_blocks(&task.block_info.field);
    if field_meta.len() <= key as usize {
        field_meta.resize(key as usize + 1, BlockMeta::default());
    }

    field_meta[key as usize] = meta;
}

fn generate_meta<S: Seek>(
    writer: &mut S,
    block_info: &mut BlockInfo,
    block_size: u32,
) -> BlockMeta {
    let seekpos = writer.stream_position().unwrap();
    BlockMeta {
        seekpos,
        numitems: block_info.numitems,
        block_size,
        uncompressed_size: block_info.uncompr_size as u64,
        stats: block_info.stats.take(),
    }
}

enum WriteStatus<'a> {
    Written,
    Full(&'a mut Inner),
}

pub(crate) struct Inner {
    stats_collector: Option<Stat>,
    pub(crate) buffer: Vec<u8>,
    pub(crate) offset: usize,
    pub(crate) field: Fields,
    rec_count: u32,
    block_num: u64,
}

impl Inner {
    pub fn new(field: Fields, stats_collector: Option<Stat>) -> Self {
        Self {
            stats_collector,
            buffer: Vec::new(),
            offset: 0,
            field,
            rec_count: 0,
            block_num: 0,
        }
    }
    pub fn write_data(&mut self, data: &[u8]) -> WriteStatus {
        debug_assert!(!self.flush_required(data));

        let limit = std::cmp::max(data.len(), SIZE_LIMIT);
        if self.buffer.len() < limit {
            self.buffer.resize(limit, 0);
        }

        self.buffer[self.offset..self.offset + data.len()].clone_from_slice(data);
        self.offset += data.len();
        self.rec_count += 1;

        WriteStatus::Written
    }

    pub fn flush_required(&self, data: &[u8]) -> bool {
        self.offset > 0 && self.offset + data.len() > SIZE_LIMIT
    }

    pub fn reset_for_new_block(&mut self) {
        self.offset = 0;
        self.rec_count = 0;
        self.block_num += 1;
    }

    pub fn generate_block_info(&mut self, codec_map_required: bool, mut codec: Codecs) -> BlockInfo {
        let stat = if self.stats_collector.is_some() {
            self.stats_collector.replace(Stat::default())
        } else {
            None
        };
        if codec_map_required {
            codec = FIELD_CODEC_MAP.get(&self.field).copied().unwrap_or(codec);
        }
        BlockInfo {
            numitems: self.rec_count,
            uncompr_size: self.offset,
            field: self.field,
            stats: stat,
            codec,
        }
    }
}

trait Column {
    fn write_record_field(&mut self, rec: &BAMRawRecord) -> WriteStatus;
    fn get_inners(&mut self) -> (&mut Inner, Option<&mut Inner>);
}

struct FixedColumn(Inner);

impl FixedColumn {
    pub fn new(field: Fields, comparator: Option<Stat>) -> Self {
        if comparator.is_some() && field != Fields::RefID && field != Fields::Pos {
            panic!("Stats collection is only supported for RefID and POS fields.");
        }
        Self(Inner::new(field, comparator))
    }
}

impl Column for FixedColumn {
    fn write_record_field(&mut self, rec: &BAMRawRecord) -> WriteStatus {
        let inner = &mut self.0;
        let data = rec.get_bytes(&inner.field);

        if inner.flush_required(data) {
            return WriteStatus::Full(inner);
        }

        if let Some(ref mut stats) = inner.stats_collector {
            stats.update((&data[..]).read_i32::<LittleEndian>().unwrap());
        }

        inner.write_data(data)
    }

    fn get_inners(&mut self) -> (&mut Inner, Option<&mut Inner>) {
        (&mut self.0, None)
    }
}

struct VariableColumn {
    inner: Inner,
    index: FixedColumn,
}

impl VariableColumn {
    pub fn new(field: Fields, comparator: Option<Stat>) -> Self {
        if comparator.is_some() {
            panic!("Stats collection is not supported for variable length fields.");
        }
        Self {
            inner: Inner::new(field, comparator),
            index: FixedColumn::new(var_size_field_to_index(&field), None),
        }
    }
}

impl Column for VariableColumn {
    fn write_record_field(&mut self, rec: &BAMRawRecord) -> WriteStatus {
        let inner = &mut self.inner;
        let index_inner = &mut self.index.0;

        let data = rec.get_bytes(&inner.field);
        let mut idx_buf: [u8; U32_SIZE] = [0; U32_SIZE];

        if index_inner.flush_required(&idx_buf) {
            return WriteStatus::Full(index_inner);
        }

        if inner.flush_required(data) {
            return WriteStatus::Full(inner);
        }

        assert!(inner.stats_collector.is_none());

        inner.write_data(data);
        (&mut idx_buf[..])
            .write_u32::<LittleEndian>(u32::try_from(inner.offset).unwrap())
            .unwrap();
        index_inner.write_data(&idx_buf)
    }

    fn get_inners(&mut self) -> (&mut Inner, Option<&mut Inner>) {
        (&mut self.inner, Some(&mut self.index.0))
    }
}

impl<W> Write for Writer<W>
where
    W: Write + Seek,
{
    /// WARNING: ENSURE THAT BUF CONTAINS A ONE FULL RECORD.
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        assert!(!buf.is_empty());
        let wrapper = BAMRawRecord(Cow::Borrowed(buf));
        self.push_record(&wrapper, false);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn calc_crc_for_meta_bytes(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}
