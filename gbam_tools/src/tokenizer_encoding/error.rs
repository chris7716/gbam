//! Error types for read name tokenization

use std::fmt;

#[derive(Debug)]
pub enum TokenizationError {
    InvalidFormat(String),
    ParseError(String),
    InvalidDictionary(String),
    UnsupportedPattern(String),
}

impl fmt::Display for TokenizationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TokenizationError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            TokenizationError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            TokenizationError::InvalidDictionary(msg) => write!(f, "Dictionary error: {}", msg),
            TokenizationError::UnsupportedPattern(msg) => write!(f, "Unsupported pattern: {}", msg),
        }
    }
}

impl std::error::Error for TokenizationError {}