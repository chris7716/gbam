//! GAF (Graph Alignment Format) v1 parser.
//!
//! Extracts per-read path node IDs from a GAF file produced by a graph aligner
//! such as `vg giraffe` or `GraphAligner`.
//!
//! GAF is tab-separated. The fields used here are:
//!
//! | Col | Field      | Example         |
//! |-----|------------|-----------------|
//! |  0  | query name | `read1`         |
//! |  5  | path       | `>1>3>4>7`      |
//!
//! Strand indicators (`>` forward, `<` reverse) are parsed but currently
//! discarded — node IDs are returned in traversal order regardless of strand.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};

/// Parse a GAF stream and return `read_name → [node_id, ...]`.
///
/// Lines beginning with `#` and empty lines are skipped.
/// Lines with fewer than 6 tab-separated columns are silently skipped.
/// Node names that are not parseable as `u32` are silently skipped.
pub fn parse_gaf<R: Read>(reader: R) -> std::io::Result<HashMap<String, Vec<u32>>> {
    let buf = BufReader::new(reader);
    let mut map = HashMap::new();

    for line in buf.lines() {
        let line = line?;
        // Skip empty lines, comments, and SAM header lines that vg giraffe
        // may emit at the start of GAF output.
        if line.is_empty() || line.starts_with('#') || line.starts_with('@') {
            continue;
        }
        let mut cols = line.splitn(7, '\t');
        let read_name = match cols.next() {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Skip columns 1-4
        for _ in 0..4 {
            if cols.next().is_none() {
                continue;
            }
        }
        let path_str = match cols.next() {
            Some(p) => p,
            None => continue,
        };

        let node_ids = parse_path_str(path_str);
        if !node_ids.is_empty() {
            map.insert(read_name, node_ids);
        }
    }

    Ok(map)
}

/// Parse a GAF path string like `>1>3>4>7` or `<5>6` into ordered node IDs.
///
/// Segments are delimited by `>` (forward) or `<` (reverse) characters.
/// Strand is ignored; only numeric IDs are extracted.
fn parse_path_str(path: &str) -> Vec<u32> {
    let mut ids = Vec::new();
    let mut cur = String::new();

    for ch in path.chars() {
        match ch {
            '>' | '<' => {
                if let Ok(id) = cur.trim().parse::<u32>() {
                    ids.push(id);
                }
                cur.clear();
            }
            c if c.is_ascii_digit() => cur.push(c),
            _ => {}
        }
    }
    // Flush the last segment
    if let Ok(id) = cur.trim().parse::<u32>() {
        ids.push(id);
    }

    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_forward_path() {
        assert_eq!(parse_path_str(">1>3>4>7"), vec![1, 3, 4, 7]);
    }

    #[test]
    fn parse_mixed_strand_path() {
        assert_eq!(parse_path_str("<5>6>7"), vec![5, 6, 7]);
    }

    #[test]
    fn parse_gaf_basic() {
        let gaf = b"read1\t150\t0\t150\t+\t>1>3>4\t200\t10\t160\t150\t150\t60\n\
                    # comment\n\
                    read2\t100\t0\t100\t+\t>2>4\t200\t5\t105\t100\t100\t60\n";
        let map = parse_gaf(&gaf[..]).unwrap();
        assert_eq!(map["read1"], vec![1, 3, 4]);
        assert_eq!(map["read2"], vec![2, 4]);
    }
}
