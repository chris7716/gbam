//! GAF (Graph Alignment Format) v1 parser.
//!
//! Extracts per-read path node IDs and alignment offsets from a GAF file
//! produced by a graph aligner such as `vg giraffe` or `GraphAligner`.
//!
//! GAF is tab-separated. The fields used here are:
//!
//! | Col | Field       | Example         |
//! |-----|-------------|-----------------|
//! |  0  | query name  | `read1`         |
//! |  5  | path        | `>1>3>4>7`      |
//! |  8  | path start  | `10`            |
//!
//! Strand indicators (`>` forward, `<` reverse) are encoded in the high bit
//! of each node ID so callers can apply reverse complement when needed.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};

/// Per-read alignment record extracted from a GAF file.
#[derive(Debug, Clone)]
pub struct PathInfo {
    /// Encoded node IDs: high bit (`REVERSE_BIT`) set = reverse complement.
    pub node_ids: Vec<u32>,
    /// 0-based offset into the concatenated path sequence where the read
    /// alignment begins.  Must be skipped when reconstructing the read.
    pub path_start: u32,
}

/// The high bit of an encoded node ID marks reverse orientation.
pub const REVERSE_BIT: u32 = 0x8000_0000;

/// Parse a GAF stream and return `read_name → PathInfo`.
///
/// Lines beginning with `#`, `@`, and empty lines are skipped.
/// Lines with fewer than 9 tab-separated columns are silently skipped.
/// Node names that are not parseable as `u32` are silently skipped.
pub fn parse_gaf<R: Read>(reader: R) -> std::io::Result<HashMap<String, PathInfo>> {
    let buf = BufReader::new(reader);
    let mut map = HashMap::new();

    for line in buf.lines() {
        let line = line?;
        // Skip empty lines, comments, and SAM header lines that vg giraffe
        // may emit at the start of GAF output.
        if line.is_empty() || line.starts_with('#') || line.starts_with('@') {
            continue;
        }
        // Need cols 0 (name), 5 (path), 8 (path_start) — split into 10 parts.
        let parts: Vec<&str> = line.splitn(10, '\t').collect();
        if parts.len() < 9 {
            continue;
        }
        let read_name = parts[0].to_string();
        let path_str = parts[5];
        let path_start: u32 = parts[8].parse().unwrap_or(0);

        let node_ids = parse_path_str(path_str);
        if !node_ids.is_empty() {
            map.insert(read_name, PathInfo { node_ids, path_start });
        }
    }

    Ok(map)
}

/// Parse a GAF path string like `>1>3>4>7` or `<5>6` into encoded node IDs.
///
/// Forward (`>`) nodes are stored as-is; reverse (`<`) nodes have the
/// `REVERSE_BIT` set so callers can apply reverse complement when needed.
fn parse_path_str(path: &str) -> Vec<u32> {
    let mut ids = Vec::new();
    let mut cur = String::new();
    let mut is_reverse = false;

    for ch in path.chars() {
        match ch {
            '>' | '<' => {
                if let Ok(id) = cur.trim().parse::<u32>() {
                    ids.push(if is_reverse { id | REVERSE_BIT } else { id });
                }
                cur.clear();
                is_reverse = ch == '<';
            }
            c if c.is_ascii_digit() => cur.push(c),
            _ => {}
        }
    }
    // Flush the last segment
    if let Ok(id) = cur.trim().parse::<u32>() {
        ids.push(if is_reverse { id | REVERSE_BIT } else { id });
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
        // Reverse bit should be set for '<' nodes
        assert_eq!(
            parse_path_str("<5>6>7"),
            vec![5 | REVERSE_BIT, 6, 7]
        );
    }

    #[test]
    fn parse_gaf_basic() {
        // path_start = 10 (col 8)
        let gaf = b"read1\t150\t0\t150\t+\t>1>3>4\t200\t10\t160\t150\t150\t60\n\
                    # comment\n\
                    read2\t100\t0\t100\t+\t>2>4\t200\t0\t100\t100\t100\t60\n";
        let map = parse_gaf(&gaf[..]).unwrap();
        assert_eq!(map["read1"].node_ids, vec![1, 3, 4]);
        assert_eq!(map["read1"].path_start, 10);
        assert_eq!(map["read2"].node_ids, vec![2, 4]);
        assert_eq!(map["read2"].path_start, 0);
    }
}
