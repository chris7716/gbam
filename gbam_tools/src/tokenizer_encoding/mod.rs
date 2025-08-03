//! Read Name Tokenization Library for CRAM-style compression
//! 
//! This library provides efficient tokenization of sequencing read names
//! to achieve better compression ratios in bioinformatics file formats.

pub mod error;
pub mod utils;
pub mod dictionary;
pub mod tokenizer;
pub mod analyzer;
pub mod encoder;
pub mod post_compression;

// Re-export main types for convenience
pub use error::TokenizationError;
pub use dictionary::{ReadNameDictionary, TokenizedReadName};
pub use tokenizer::IlluminaTokenizer;
pub use analyzer::{ReadNameAnalyzer, ReadNamePattern};
pub use encoder::{CoordinateEncoder, CoordinateDeltas};
pub use utils::ByteUtils;
pub use post_compression::{PostTokenizationCompressor, PostTokenizationConfig};

/// Statistics about tokenization performance
#[derive(Debug, Clone)]
pub struct TokenizationStats {
    pub total_reads: usize,
    pub successfully_tokenized: usize,
    pub dictionary_size: usize,
    pub compression_ratio: f64,
}