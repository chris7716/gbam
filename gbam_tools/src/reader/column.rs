use std::{collections::BTreeMap, io::Result, sync::Arc};

use super::reader::generate_block_treemap;
use super::record::GbamRecord;
use crate::graph::gfa::VariationGraph;
use crate::graph::path_codec::GraphPathEntry;
use crate::SIZE_LIMIT;
use bam_tools::record::fields::Fields;
use byteorder::{LittleEndian, ReadBytesExt};
use flate2::write::GzDecoder;
use lzzzz::lz4;
use memmap2::Mmap;
use std::convert::{TryFrom, TryInto};
use std::io::Read;
use xz2::read::XzDecoder;

use crate::{meta::FileMeta, Codecs};

pub struct Inner {
    pub(crate) meta: Arc<FileMeta>,
    range_begin: usize,
    range_end: usize,
    pub(crate) field: Fields,
    pub(crate) buffer: Vec<u8>,
    reader: Arc<Mmap>,
}

impl Inner {
    pub(crate) fn new(meta: Arc<FileMeta>, field: Fields, reader: Arc<Mmap>) -> Self {
        Inner {
            meta,
            range_begin: 0,
            range_end: 0,
            field,
            buffer: Vec::<u8>::with_capacity(SIZE_LIMIT * 2),
            reader,
        }
    }
}

pub trait Column {
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord);
}

pub struct FixedColumn(pub(crate) Inner, pub(crate) usize);

impl Column for FixedColumn {
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord) {
        rec.parse_from_bytes(&self.0.field.clone(), self.get_item(item_num));
    }
}

impl FixedColumn {
    pub fn new(inner: Inner, field_size: usize) -> Self {
        Self(inner, field_size)
    }

    pub(crate) fn get_item(&mut self, item_num: usize) -> &[u8] {
        if let Some(block_num) = self.find_block(item_num) {
            Self::update_buffer(&mut self.0, block_num);
        }
        let rec_num_in_block = item_num - self.0.range_begin;
        let item_size = self.1;
        let offset = rec_num_in_block * item_size;
        &self.0.buffer[offset..offset + item_size]
    }

    fn find_block(&self, item_num: usize) -> Option<usize> {
        if item_num >= self.0.range_begin && item_num < self.0.range_end {
            return None;
        }
        let block_len = self.0.meta.view_blocks(&self.0.field)[0].numitems;
        Some(item_num / block_len as usize)
    }

    fn update_buffer(inner: &mut Inner, block_num: usize) {
        fetch_block(inner, block_num).unwrap();
        let block_len = inner.meta.view_blocks(&inner.field)[0].numitems as usize;
        let cur_block_len = inner.meta.view_blocks(&inner.field)[block_num].numitems as usize;
        inner.range_begin = block_num * block_len;
        inner.range_end = inner.range_begin + cur_block_len;
    }
}

pub struct VariableColumn {
    pub(crate) inner: Inner,
    pub(crate) index: FixedColumn,
    blocks: BTreeMap<usize, usize>,
}

impl Column for VariableColumn {
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord) {
        rec.parse_from_bytes(&self.inner.field.clone(), self.get_item(item_num));
    }
}

impl VariableColumn {
    pub fn new(inner: Inner, index: FixedColumn) -> Self {
        Self {
            blocks: generate_block_treemap(&inner.meta, &inner.field),
            inner,
            index,
        }
    }

    pub(crate) fn get_item(&mut self, item_num: usize) -> &[u8] {
        if let Some((range_begin, block_num)) = self.find_block(item_num) {
            Self::update_buffer(&mut self.inner, block_num, range_begin);
        }
        let rec_num_in_block = item_num - self.inner.range_begin;
        let mut read_offset =
            |n| self.index.get_item(n).read_u32::<LittleEndian>().unwrap() as usize;
        let start = match rec_num_in_block {
            0 => 0,
            _ => read_offset(item_num - 1),
        };
        let end = read_offset(item_num);
        &self.inner.buffer[start..end]
    }

    fn find_block(&self, item_num: usize) -> Option<(usize, usize)> {
        if item_num >= self.inner.range_begin && item_num < self.inner.range_end {
            return None;
        }
        Some(
            self.blocks
                .range(..=item_num)
                .next_back()
                .map_or((0, 0), |(&range_begin, &block_num)| {
                    (range_begin, block_num)
                }),
        )
    }

    fn update_buffer(inner: &mut Inner, block_num: usize, range_begin: usize) {
        fetch_block(inner, block_num).unwrap();
        let block_len = inner.meta.view_blocks(&inner.field)[block_num].numitems as usize;
        inner.range_begin = range_begin;
        inner.range_end = inner.range_begin + block_len;
    }
}

fn fetch_block(inner_column: &mut Inner, block_num: usize) -> Result<()> {
    let field = &inner_column.field;
    let block_meta = inner_column.meta.view_blocks(field).get(block_num).unwrap();
    let reader = &inner_column.reader;
    let block_size = block_meta.block_size;
    let uncompressed_size = block_meta.uncompressed_size;

    let data = &reader[usize::try_from(block_meta.seekpos).unwrap()
        ..usize::try_from(block_meta.seekpos + block_size as u64).unwrap()];
    inner_column.buffer.resize(uncompressed_size as usize, 0);
    let codec = inner_column.meta.get_field_codec(field);

    if uncompressed_size > 0 {
        decompress_block(data, &mut inner_column.buffer, codec).expect("Decompression failed.");
    }

    Ok(())
}

pub fn decompress_block(source: &[u8], dest: &mut Vec<u8>, codec: &Codecs) -> std::io::Result<()> {
    use std::io::Write;
    match codec {
        Codecs::Gzip => {
            let mut decoder = GzDecoder::new(dest);
            decoder.write_all(source).unwrap();
            decoder.try_finish().unwrap();
        }
        Codecs::Lz4 => {
            lz4::decompress(source, dest).unwrap();
        }
        Codecs::Brotli => {
            dest.clear();
            let mut decompressor = brotli::Decompressor::new(source, 4096);
            decompressor.read_to_end(dest)?;
        }
        Codecs::Zstd => {
            dest.clear();
            let mut decoder = zstd::stream::Decoder::new(source)?;
            decoder.read_to_end(dest)?;
        }
        Codecs::Xz => {
            let mut decoder = XzDecoder::new(source);
            decoder.read_to_end(dest)?;
        }
        Codecs::NoCompression => {
            dest.clear();
            dest.extend_from_slice(source);
        }
    };
    Ok(())
}

// ---------------------------------------------------------------------------
// Graph-path sequence column
// ---------------------------------------------------------------------------

/// Reads the dedicated graph-path columns (PathNodeIds, PathStart, EditOffsets,
/// EditBases) and reconstructs the ASCII read sequence for `RawSequence` requests.
pub struct GraphPathSequenceColumn {
    path_start_col: FixedColumn,
    node_ids_col: VariableColumn,
    edit_offsets_col: VariableColumn,
    edit_bases_col: VariableColumn,
    seq_len_col: FixedColumn,
    graph: Arc<VariationGraph>,
}

impl GraphPathSequenceColumn {
    pub fn new(
        path_start_col: FixedColumn,
        node_ids_col: VariableColumn,
        edit_offsets_col: VariableColumn,
        edit_bases_col: VariableColumn,
        seq_len_col: FixedColumn,
        graph: Arc<VariationGraph>,
    ) -> Self {
        Self {
            path_start_col,
            node_ids_col,
            edit_offsets_col,
            edit_bases_col,
            seq_len_col,
            graph,
        }
    }
}

impl Column for GraphPathSequenceColumn {
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord) {
        // SequenceLength is the cumulative byte offset index for RawQual.
        // Diff of consecutive entries gives l_seq (1 qual byte per base).
        let seq_len = {
            let cur = {
                let bytes = self.seq_len_col.get_item(item_num);
                u32::from_le_bytes(bytes.try_into().unwrap()) as usize
            };
            if item_num == 0 {
                cur
            } else {
                let prev = {
                    let bytes = self.seq_len_col.get_item(item_num - 1);
                    u32::from_le_bytes(bytes.try_into().unwrap()) as usize
                };
                cur - prev
            }
        };

        let path_start = {
            let bytes = self.path_start_col.get_item(item_num);
            u32::from_le_bytes(bytes.try_into().unwrap())
        };

        let node_ids_bytes = self.node_ids_col.get_item(item_num).to_vec();

        let seq = if node_ids_bytes.is_empty() {
            // Raw fallback: EditBases holds the full sequence directly.
            self.edit_bases_col.get_item(item_num).to_vec()
        } else {
            let node_ids: Vec<u32> = node_ids_bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();

            let edit_offsets_bytes = self.edit_offsets_col.get_item(item_num).to_vec();
            let edit_bases = self.edit_bases_col.get_item(item_num).to_vec();

            let edits: Vec<_> = edit_bases
                .iter()
                .enumerate()
                .map(|(i, &base)| {
                    let offset = u32::from_le_bytes(
                        edit_offsets_bytes[i * 4..i * 4 + 4].try_into().unwrap()
                    );
                    crate::graph::path_codec::Edit { read_offset: offset, read_base: base }
                })
                .collect();

            let entry = GraphPathEntry::new(path_start, node_ids, edits);
            entry.reconstruct_sequence(&self.graph, seq_len)
        };

        rec.seq = Some(String::from_utf8(seq).unwrap_or_default());
    }
}
