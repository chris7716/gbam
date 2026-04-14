//! Encode and decode read sequences as (graph path, edit list) pairs.
//!
//! # Binary format (per read, stored in the RawSequence variable column)
//!
//! ```text
//! [seq_len    : u32 LE]       ← actual read sequence length in bases
//! [path_start : u32 LE]       ← byte offset into path where alignment begins
//! [n_nodes    : u32 LE]
//! [node_id    : u32 LE] × n   ← encoded node IDs (high bit = reverse strand)
//! [n_edits    : u32 LE]
//! [offset     : u32 LE]       ┐ one record per mismatch between
//! [base       : u8     ]      ┘ read and path_seq[path_start..]
//! ```
//!
//! Reconstruction: concatenate node sequences (applying rev-comp where
//! indicated), skip `path_start` bytes, truncate/pad to `seq_len`, apply edits.
//!
//! Reads with no path entry use `n_nodes = 0` (raw fallback):
//!   [seq_len: u32][path_start=0: u32][n_nodes=0: u32][n_raw: u32][base: u8 × n_raw]

use super::gaf::REVERSE_BIT;
use super::gfa::VariationGraph;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

/// Return the reverse complement of an ASCII uppercase DNA sequence.
pub fn rev_comp(seq: &[u8]) -> Vec<u8> {
    seq.iter().rev().map(|&b| match b {
        b'A' => b'T', b'T' => b'A',
        b'C' => b'G', b'G' => b'C',
        b'N' => b'N', _ => b'N',
    }).collect()
}

/// A single base difference between the read and the graph path sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    /// 0-based offset within the read sequence.
    pub read_offset: u32,
    /// The base actually present in the read (ASCII uppercase).
    pub read_base: u8,
}

/// Graph-encoded representation of one read's sequence.
#[derive(Debug, Clone)]
pub struct GraphPathEntry {
    /// Actual read length in bases.
    pub seq_len: u32,
    /// Byte offset into the concatenated path where the read alignment begins.
    pub path_start: u32,
    /// Encoded node IDs (high bit = REVERSE_BIT means reverse complement).
    pub node_ids: Vec<u32>,
    /// Sparse mismatches against path_seq[path_start..path_start+seq_len].
    pub edits: Vec<Edit>,
}

impl GraphPathEntry {
    pub fn new(seq_len: u32, path_start: u32, node_ids: Vec<u32>, edits: Vec<Edit>) -> Self {
        Self { seq_len, path_start, node_ids, edits }
    }

    /// Serialize to bytes.
    ///
    /// When `n_nodes == 0` (raw-fallback) bases are stored 1 byte each.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.write_u32::<LittleEndian>(self.seq_len).unwrap();
        buf.write_u32::<LittleEndian>(self.path_start).unwrap();
        buf.write_u32::<LittleEndian>(self.node_ids.len() as u32).unwrap();

        if self.node_ids.is_empty() {
            // Raw-sequence fallback: store bases as 1 byte each.
            buf.write_u32::<LittleEndian>(self.edits.len() as u32).unwrap();
            for edit in &self.edits {
                buf.write_u8(edit.read_base).unwrap();
            }
        } else {
            for &id in &self.node_ids {
                buf.write_u32::<LittleEndian>(id).unwrap();
            }
            buf.write_u32::<LittleEndian>(self.edits.len() as u32).unwrap();
            for edit in &self.edits {
                buf.write_u32::<LittleEndian>(edit.read_offset).unwrap();
                buf.write_u8(edit.read_base).unwrap();
            }
        }

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> std::io::Result<Self> {
        let mut cur = Cursor::new(data);

        let seq_len = cur.read_u32::<LittleEndian>()?;
        let path_start = cur.read_u32::<LittleEndian>()?;
        let n_nodes = cur.read_u32::<LittleEndian>()? as usize;

        if n_nodes == 0 {
            // Raw-sequence fallback: bases stored as 1 byte each.
            let n_raw = cur.read_u32::<LittleEndian>()? as usize;
            let mut edits = Vec::with_capacity(n_raw);
            for i in 0..n_raw {
                let base = cur.read_u8()?;
                edits.push(Edit { read_offset: i as u32, read_base: base });
            }
            return Ok(GraphPathEntry { seq_len, path_start: 0, node_ids: vec![], edits });
        }

        let mut node_ids = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            node_ids.push(cur.read_u32::<LittleEndian>()?);
        }
        let n_edits = cur.read_u32::<LittleEndian>()? as usize;
        let mut edits = Vec::with_capacity(n_edits);
        for _ in 0..n_edits {
            let read_offset = cur.read_u32::<LittleEndian>()?;
            let read_base = cur.read_u8()?;
            edits.push(Edit { read_offset, read_base });
        }

        Ok(GraphPathEntry { seq_len, path_start, node_ids, edits })
    }

    /// Reconstruct the ASCII read sequence from graph nodes and edits.
    ///
    /// 1. Concatenate node sequences (rev-comp where REVERSE_BIT is set).
    /// 2. Skip `path_start` bytes (the portion before the read alignment).
    /// 3. Truncate/pad to `seq_len`.
    /// 4. Apply sparse edits.
    pub fn reconstruct_sequence(&self, graph: &VariationGraph) -> Vec<u8> {
        let len = self.seq_len as usize;

        if self.node_ids.is_empty() {
            // Raw fallback: edits hold the actual bases.
            return self.edits.iter().map(|e| e.read_base).collect();
        }

        // Build full path sequence.
        let mut path_seq: Vec<u8> = Vec::new();
        for &encoded_id in &self.node_ids {
            let is_reverse = encoded_id & REVERSE_BIT != 0;
            let node_id = encoded_id & !REVERSE_BIT;
            if let Some(node_seq) = graph.node_seq(node_id) {
                if is_reverse {
                    path_seq.extend(rev_comp(node_seq));
                } else {
                    path_seq.extend_from_slice(node_seq);
                }
            }
        }

        // Skip path_start bytes, then take seq_len bases.
        let start = self.path_start as usize;
        let mut seq: Vec<u8> = path_seq
            .get(start..start + len)
            .map(|s| s.to_vec())
            .unwrap_or_else(|| {
                let available = path_seq.get(start..).unwrap_or(&[]);
                let mut v = available.to_vec();
                v.resize(len, b'N');
                v
            });

        // Apply sparse edits.
        for edit in &self.edits {
            let off = edit.read_offset as usize;
            if off < seq.len() {
                seq[off] = edit.read_base;
            }
        }

        seq
    }
}

/// Compute edits between `path_seq[path_start..]` and `read_seq`.
///
/// `N` bases in the read are treated as matching.
pub fn compute_edits(path_seq: &[u8], read_seq: &[u8]) -> Vec<Edit> {
    let len = read_seq.len();
    let mut edits = Vec::new();

    for i in 0..len {
        let r = read_seq[i].to_ascii_uppercase();
        if r == b'N' {
            continue;
        }
        let p = if i < path_seq.len() {
            path_seq[i].to_ascii_uppercase()
        } else {
            b'N'
        };
        if r != p {
            edits.push(Edit { read_offset: i as u32, read_base: r });
        }
    }

    edits
}

/// Build a `GraphPathEntry` for a read with no graph path (compact raw fallback).
pub fn encode_without_path(read_seq_ascii: &[u8]) -> GraphPathEntry {
    let edits = read_seq_ascii
        .iter()
        .enumerate()
        .map(|(i, &b)| Edit { read_offset: i as u32, read_base: b.to_ascii_uppercase() })
        .collect();
    GraphPathEntry { seq_len: read_seq_ascii.len() as u32, path_start: 0, node_ids: vec![], edits }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::gfa::VariationGraph;

    fn make_graph(nodes: &[(u32, &str)]) -> VariationGraph {
        let mut g = VariationGraph::new();
        for &(id, seq) in nodes {
            g.nodes.insert(id, seq.as_bytes().to_vec());
        }
        g
    }

    #[test]
    fn round_trip_no_edits() {
        let graph = make_graph(&[(1, "AACGT"), (3, "C"), (4, "GCAAT")]);
        let entry = GraphPathEntry::new(11, 0, vec![1, 3, 4], vec![]);
        let bytes = entry.to_bytes();
        let decoded = GraphPathEntry::from_bytes(&bytes).unwrap();
        let seq = decoded.reconstruct_sequence(&graph);
        assert_eq!(seq, b"AACGTCGCAAT");
    }

    #[test]
    fn round_trip_with_path_start() {
        // path = node1(AACGT) + node3(C) + node4(GCAAT) = "AACGTCGCAAT" (11bp)
        // path_start = 3 → aligned portion = "GTCGCAAT" (8bp)
        let graph = make_graph(&[(1, "AACGT"), (3, "C"), (4, "GCAAT")]);
        let full_path = b"AACGTCGCAAT";
        let path_start = 3usize;
        let read_seq = &full_path[path_start..path_start + 8]; // "GTCGCAAT"
        let edits = compute_edits(&full_path[path_start..], read_seq);
        let entry = GraphPathEntry::new(8, path_start as u32, vec![1, 3, 4], edits);
        let bytes = entry.to_bytes();
        let decoded = GraphPathEntry::from_bytes(&bytes).unwrap();
        let seq = decoded.reconstruct_sequence(&graph);
        assert_eq!(seq, read_seq);
    }

    #[test]
    fn encode_without_path_roundtrip() {
        let graph = VariationGraph::new();
        let read = b"ACGTACGT";
        let entry = encode_without_path(read);
        let bytes = entry.to_bytes();
        let decoded = GraphPathEntry::from_bytes(&bytes).unwrap();
        let seq = decoded.reconstruct_sequence(&graph);
        assert_eq!(&seq, read);
    }

    #[test]
    fn compute_edits_basic() {
        let path = b"AACGT";
        let read = b"AATGT";
        let edits = compute_edits(path, read);
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0].read_offset, 2);
        assert_eq!(edits[0].read_base, b'T');
    }
}
