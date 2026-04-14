//! Encode and decode read sequences as (graph path, edit list) pairs.
//!
//! # Binary format (per read, stored in the RawSequence variable column)
//!
//! ```text
//! [seq_len : u32 LE]          ← actual read sequence length in bases
//! [n_nodes : u32 LE]
//! [node_id : u32 LE] × n      ← ordered node IDs traversed by this read
//! [n_edits : u32 LE]
//! [offset  : u32 LE]          ┐ one record per mismatch between
//! [base    : u8     ]         ┘ read and concatenated node sequences
//! ```
//!
//! Reconstruction: concatenate node sequences in order, truncate / pad to
//! `seq_len`, then overlay each edit at its offset.
//!
//! Reads with no path entry (unmapped or absent from the GAF) use `n_nodes = 0`
//! and store every base as an edit, preserving lossless round-trip at the cost
//! of slightly larger storage than 4-bit packed encoding.

use super::gfa::VariationGraph;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

/// A single base difference between the read and the graph path sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Edit {
    /// 0-based offset within the reconstructed path sequence.
    pub read_offset: u32,
    /// The base actually present in the read (ASCII uppercase).
    pub read_base: u8,
}

/// Graph-encoded representation of one read's sequence.
#[derive(Debug, Clone)]
pub struct GraphPathEntry {
    /// Actual read length in bases (needed for truncation / padding).
    pub seq_len: u32,
    /// Ordered node IDs traversed by this read.
    pub node_ids: Vec<u32>,
    /// Sparse mismatches — positions where the read differs from the path.
    pub edits: Vec<Edit>,
}

impl GraphPathEntry {
    pub fn new(seq_len: u32, node_ids: Vec<u32>, edits: Vec<Edit>) -> Self {
        Self {
            seq_len,
            node_ids,
            edits,
        }
    }

    /// Serialize to bytes using the binary layout described in the module doc.
    pub fn to_bytes(&self) -> Vec<u8> {
        // Pre-size: 4 (seq_len) + 4 (n_nodes) + 4*n + 4 (n_edits) + 5*m
        let capacity = 8 + 4 * self.node_ids.len() + 4 + 5 * self.edits.len();
        let mut buf = Vec::with_capacity(capacity);

        buf.write_u32::<LittleEndian>(self.seq_len).unwrap();
        buf.write_u32::<LittleEndian>(self.node_ids.len() as u32)
            .unwrap();
        for &id in &self.node_ids {
            buf.write_u32::<LittleEndian>(id).unwrap();
        }
        buf.write_u32::<LittleEndian>(self.edits.len() as u32)
            .unwrap();
        for edit in &self.edits {
            buf.write_u32::<LittleEndian>(edit.read_offset).unwrap();
            buf.write_u8(edit.read_base).unwrap();
        }

        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> std::io::Result<Self> {
        let mut cur = Cursor::new(data);

        let seq_len = cur.read_u32::<LittleEndian>()?;
        let n_nodes = cur.read_u32::<LittleEndian>()? as usize;
        let mut node_ids = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            node_ids.push(cur.read_u32::<LittleEndian>()?);
        }
        let n_edits = cur.read_u32::<LittleEndian>()? as usize;
        let mut edits = Vec::with_capacity(n_edits);
        for _ in 0..n_edits {
            let read_offset = cur.read_u32::<LittleEndian>()?;
            let read_base = cur.read_u8()?;
            edits.push(Edit {
                read_offset,
                read_base,
            });
        }

        Ok(GraphPathEntry {
            seq_len,
            node_ids,
            edits,
        })
    }

    /// Reconstruct the ASCII read sequence using the graph node sequences plus edits.
    ///
    /// Algorithm:
    /// 1. Concatenate node sequences along the path.
    /// 2. Truncate to `seq_len` (path may span beyond read) or pad with `N`.
    /// 3. Apply each edit (mismatch / novel variant).
    pub fn reconstruct_sequence(&self, graph: &VariationGraph) -> Vec<u8> {
        let len = self.seq_len as usize;
        let mut seq = Vec::with_capacity(len);

        for &node_id in &self.node_ids {
            if seq.len() >= len {
                break;
            }
            if let Some(node_seq) = graph.node_seq(node_id) {
                let remaining = len - seq.len();
                seq.extend_from_slice(&node_seq[..remaining.min(node_seq.len())]);
            }
        }

        // Pad with N if the path was shorter than the read length
        seq.resize(len, b'N');

        // Apply sparse edits
        for edit in &self.edits {
            let off = edit.read_offset as usize;
            if off < seq.len() {
                seq[off] = edit.read_base;
            }
        }

        seq
    }
}

/// Compute edits between a path-reconstructed sequence and the actual read sequence.
///
/// Both arguments should be ASCII uppercase bases. The reconstructed path
/// sequence is typically longer than the read; it will be truncated to the
/// read length before comparison.
///
/// `N` bases in the read are treated as matching and produce no edit record.
pub fn compute_edits(path_seq: &[u8], read_seq: &[u8]) -> Vec<Edit> {
    let len = read_seq.len();
    let mut edits = Vec::new();

    for i in 0..len {
        let r = read_seq[i].to_ascii_uppercase();
        // Skip N — treat as "matches anything"
        if r == b'N' {
            continue;
        }
        let p = if i < path_seq.len() {
            path_seq[i].to_ascii_uppercase()
        } else {
            b'N' // path is shorter than read → force an edit for every extra base
        };
        if r != p {
            edits.push(Edit {
                read_offset: i as u32,
                read_base: r,
            });
        }
    }

    edits
}

/// Build a `GraphPathEntry` for a read that has no path in the graph.
///
/// Stores every base as an edit against an empty path. This is lossless but
/// larger than 4-bit packed encoding (~5 bytes/base vs ~0.5 bytes/base).
/// In practice, truly unmapped reads are a small fraction of a typical cohort.
pub fn encode_without_path(read_seq_ascii: &[u8]) -> GraphPathEntry {
    let edits = read_seq_ascii
        .iter()
        .enumerate()
        .map(|(i, &b)| Edit {
            read_offset: i as u32,
            read_base: b.to_ascii_uppercase(),
        })
        .collect();

    GraphPathEntry {
        seq_len: read_seq_ascii.len() as u32,
        node_ids: vec![],
        edits,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::gfa::VariationGraph;
    use std::collections::HashMap;

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
        let entry = GraphPathEntry::new(11, vec![1, 3, 4], vec![]);
        let bytes = entry.to_bytes();
        let decoded = GraphPathEntry::from_bytes(&bytes).unwrap();
        let seq = decoded.reconstruct_sequence(&graph);
        assert_eq!(seq, b"AACGTCGCAAT");
    }

    #[test]
    fn round_trip_with_edit() {
        let graph = make_graph(&[(1, "AACGT"), (2, "T"), (4, "GCAAT")]);
        // Read differs at position 5: has 'A' instead of 'T'
        let entry = GraphPathEntry::new(
            10,
            vec![1, 2, 4],
            vec![Edit {
                read_offset: 5,
                read_base: b'A',
            }],
        );
        let bytes = entry.to_bytes();
        let decoded = GraphPathEntry::from_bytes(&bytes).unwrap();
        let seq = decoded.reconstruct_sequence(&graph);
        // Path seq: AACGT·T·GCAAT → AACGTTGCAAT
        // After edit at 5: AACGT·A·GCAAT = AACGTAGCAAT... wait
        // node1=AACGT(5), node2=T(1), node4=GCAAT(5) → AACGTTGCAAT (11 bases)
        // seq_len=10 so truncated to AACGTTGCAA, then edit at 5: pos5='A' → AACGTAG CAA
        // Let me recheck: AACGTTGCAA with edit at offset 5 changing T→A = AACGTAG CAA
        assert_eq!(seq[5], b'A');
    }

    #[test]
    fn encode_without_path_roundtrip() {
        let graph = VariationGraph::new(); // empty graph
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
