//! GFA v1 parser. Stores node sequences needed for graph-based read encoding.
//!
//! Only S (segment) lines are parsed — edges are not required for sequence
//! reconstruction from a known path.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};

/// A variation graph that stores per-node sequences.
///
/// Constructed from a GFA v1 file. Only the sequence data is retained;
/// graph topology (edges, paths) is not stored here because the read's
/// traversal order is supplied externally via the GAF alignment.
pub struct VariationGraph {
    /// node_id → ASCII sequence bytes (uppercase A/C/G/T/N)
    pub nodes: HashMap<u32, Vec<u8>>,
}

impl VariationGraph {
    pub fn new() -> Self {
        VariationGraph {
            nodes: HashMap::new(),
        }
    }

    /// Parse a GFA v1 stream. Only `S` (segment) lines are read.
    ///
    /// Node names must be parseable as `u32`. This matches the convention used
    /// by tools like `vg`, `minigraph`, and `pggb` when emitting numeric node IDs.
    pub fn from_gfa<R: Read>(reader: R) -> std::io::Result<Self> {
        let mut graph = VariationGraph::new();
        let buf = BufReader::new(reader);

        for line in buf.lines() {
            let line = line?;
            if !line.starts_with('S') {
                continue;
            }
            // S <name> <sequence> [optional fields...]
            let mut parts = line.splitn(4, '\t');
            parts.next(); // record type 'S'
            let name = parts.next().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "GFA S line: missing name")
            })?;
            let seq = parts.next().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "GFA S line: missing sequence",
                )
            })?;

            let node_id: u32 = name.parse().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("GFA node name is not a u32: '{}'", name),
                )
            })?;

            // '*' means sequence is not stored in the GFA
            if seq != "*" {
                graph
                    .nodes
                    .insert(node_id, seq.to_ascii_uppercase().into_bytes());
            }
        }

        Ok(graph)
    }

    /// Return the sequence for a node, or `None` if unknown.
    #[inline]
    pub fn node_seq(&self, node_id: u32) -> Option<&[u8]> {
        self.nodes.get(&node_id).map(|v| v.as_slice())
    }

    /// Total number of nodes loaded.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

impl Default for VariationGraph {
    fn default() -> Self {
        Self::new()
    }
}
