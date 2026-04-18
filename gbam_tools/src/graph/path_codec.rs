//! Encode and decode read sequences as (graph path, edit list) pairs.
//!
//! Each read is represented by:
//! - `node_ids`: ordered list of pangenome graph nodes traversed (high bit = reverse strand)
//! - `path_start`: byte offset into the concatenated node sequences where the read begins
//! - `edits`: sparse list of positions where the read differs from the graph path
//!
//! For reads with no graph path (unmapped), `node_ids` is empty and `edits` holds
//! all bases directly (raw fallback).
//!
//! Storage is split across dedicated GBAM columns for optimal compression:
//! - `PathNodeIds`  — flat stream of node IDs
//! - `PathStart`    — one u32 per read
//! - `EditOffsets`  — flat stream of per-edit read positions
//! - `EditBases`    — flat stream of per-edit base values
//! - Index columns `NodeCounts`, `EditCounts` delimit per-read slices (EditBases uses EditCounts/4).

use super::gaf::REVERSE_BIT;
use super::gfa::VariationGraph;

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
    /// Byte offset into the concatenated path where the read alignment begins.
    pub path_start: u32,
    /// Encoded node IDs (high bit = REVERSE_BIT means reverse complement).
    /// Empty for raw-fallback reads (no graph path available).
    pub node_ids: Vec<u32>,
    /// For graph-path reads: sparse mismatches against path_seq[path_start..].
    /// For raw-fallback reads: all bases stored as sequential edits.
    pub edits: Vec<Edit>,
}

impl GraphPathEntry {
    pub fn new(path_start: u32, node_ids: Vec<u32>, edits: Vec<Edit>) -> Self {
        Self { path_start, node_ids, edits }
    }

    /// Reconstruct the ASCII read sequence from graph nodes and edits.
    ///
    /// For raw-fallback reads (`node_ids` is empty), returns `edits[*].read_base` directly.
    pub fn reconstruct_sequence(&self, graph: &VariationGraph, seq_len: usize) -> Vec<u8> {
        if self.node_ids.is_empty() {
            // Raw fallback: edits hold the actual bases.
            return self.edits.iter().map(|e| e.read_base).collect();
        }

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

        let start = self.path_start as usize;
        let mut seq: Vec<u8> = path_seq
            .get(start..start + seq_len)
            .map(|s| s.to_vec())
            .unwrap_or_else(|| {
                let available = path_seq.get(start..).unwrap_or(&[]);
                let mut v = available.to_vec();
                v.resize(seq_len, b'N');
                v
            });

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

/// Build a `GraphPathEntry` for a read with no graph path (raw fallback).
pub fn encode_without_path(read_seq_ascii: &[u8]) -> GraphPathEntry {
    let edits = read_seq_ascii
        .iter()
        .enumerate()
        .map(|(i, &b)| Edit { read_offset: i as u32, read_base: b.to_ascii_uppercase() })
        .collect();
    GraphPathEntry { path_start: 0, node_ids: vec![], edits }
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
    fn reconstruct_no_edits() {
        let graph = make_graph(&[(1, "AACGT"), (3, "C"), (4, "GCAAT")]);
        let entry = GraphPathEntry::new(0, vec![1, 3, 4], vec![]);
        let seq = entry.reconstruct_sequence(&graph, 11);
        assert_eq!(seq, b"AACGTCGCAAT");
    }

    #[test]
    fn reconstruct_with_path_start() {
        let graph = make_graph(&[(1, "AACGT"), (3, "C"), (4, "GCAAT")]);
        let full_path = b"AACGTCGCAAT";
        let path_start = 3usize;
        let read_seq = &full_path[path_start..path_start + 8];
        let edits = compute_edits(&full_path[path_start..], read_seq);
        let entry = GraphPathEntry::new(path_start as u32, vec![1, 3, 4], edits);
        let seq = entry.reconstruct_sequence(&graph, 8);
        assert_eq!(seq, read_seq);
    }

    #[test]
    fn raw_fallback_roundtrip() {
        let graph = VariationGraph::new();
        let read = b"ACGTACGT";
        let entry = encode_without_path(read);
        let seq = entry.reconstruct_sequence(&graph, read.len());
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
