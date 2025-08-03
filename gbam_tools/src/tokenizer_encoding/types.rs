//! Core data types for read name tokenization

use std::fmt;

/// Tokenized representation of a sequencing read name
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenizedReadName {
    pub instrument_id: u8,
    pub run_id: u32,
    pub flowcell_id: u8,
    pub lane: u8,
    pub tile: u16,
    pub x_coord: u32,
    pub y_coord: u32,
    pub umi_id: Option<u16>,
    pub read_num: u8,
    pub flags: u8,
    pub index_id: Option<u8>,
}

impl TokenizedReadName {
    /// Get the size in bytes of this tokenized read name
    pub fn size_bytes() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// Delta-encoded coordinates for space efficiency
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinateDeltas {
    pub x_delta: i16,
    pub y_delta: i16,
    pub tile_delta: i16,
}

/// Detected pattern types for read names
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadNamePattern {
    /// Illumina sequencing platform pattern
    Illumina,
    /// PacBio sequencing platform pattern
    PacBio,
    /// Custom pattern with some structure
    Custom,
    /// Unstructured read names (no tokenization benefit)
    Unstructured,
}

impl fmt::Display for ReadNamePattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadNamePattern::Illumina => write!(f, "Illumina"),
            ReadNamePattern::PacBio => write!(f, "PacBio"),
            ReadNamePattern::Custom => write!(f, "Custom"),
            ReadNamePattern::Unstructured => write!(f, "Unstructured"),
        }
    }
}

/// Statistics about the tokenization process
#[derive(Debug, Clone)]
pub struct TokenizationStats {
    pub total_reads: usize,
    pub successfully_tokenized: usize,
    pub dictionary_size: usize,
    pub compression_ratio: f64,
}

impl fmt::Display for TokenizationStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, 
            "TokenizationStats {{ total_reads: {}, tokenized: {}, dict_size: {} bytes, ratio: {:.2}x }}",
            self.total_reads, 
            self.successfully_tokenized, 
            self.dictionary_size, 
            self.compression_ratio
        )
    }
}