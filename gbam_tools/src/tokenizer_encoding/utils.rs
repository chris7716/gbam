//! Utility functions for byte manipulation and parsing

use std::collections::HashSet;

pub struct ByteUtils;

impl ByteUtils {
    /// Convert &[u8] to String, handling invalid UTF-8 gracefully
    pub fn bytes_to_string(bytes: &[u8]) -> Result<String, std::str::Utf8Error> {
        std::str::from_utf8(bytes).map(|s| s.to_string())
    }

    /// Convert &[u8] to String, replacing invalid UTF-8 with replacement chars
    pub fn bytes_to_string_lossy(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).to_string()
    }

    /// Split bytes on a delimiter
    pub fn split_bytes(bytes: &[u8], delimiter: u8) -> Vec<&[u8]> {
        let mut parts = Vec::new();
        let mut start = 0;
        
        for (i, &byte) in bytes.iter().enumerate() {
            if byte == delimiter {
                parts.push(&bytes[start..i]);
                start = i + 1;
            }
        }
        
        // Add the last part
        if start <= bytes.len() {
            parts.push(&bytes[start..]);
        }
        
        parts
    }

    /// Parse bytes to u32
    pub fn parse_u32(bytes: &[u8]) -> Result<u32, std::num::ParseIntError> {
        std::str::from_utf8(bytes)
            .map_err(|_| "0".parse::<u32>().unwrap_err())?
            .parse::<u32>()
    }

    /// Parse bytes to u16
    pub fn parse_u16(bytes: &[u8]) -> Result<u16, std::num::ParseIntError> {
        std::str::from_utf8(bytes)
            .map_err(|_| "0".parse::<u16>().unwrap_err())?
            .parse::<u16>()
    }

    /// Parse bytes to u8
    pub fn parse_u8(bytes: &[u8]) -> Result<u8, std::num::ParseIntError> {
        std::str::from_utf8(bytes)
            .map_err(|_| "0".parse::<u8>().unwrap_err())?
            .parse::<u8>()
    }

    /// Find common prefix length between two byte slices
    pub fn common_prefix_length(a: &[u8], b: &[u8]) -> usize {
        a.iter()
            .zip(b.iter())
            .take_while(|(x, y)| x == y)
            .count()
    }

    /// Count occurrences of a byte in a slice
    pub fn count_byte(haystack: &[u8], needle: u8) -> usize {
        haystack.iter().filter(|&&b| b == needle).count()
    }

    /// Calculate redundancy in a collection of byte slices
    pub fn calculate_redundancy(data: &[&[u8]]) -> f64 {
        let total_bytes: usize = data.iter().map(|s| s.len()).sum();
        if total_bytes == 0 {
            return 0.0;
        }

        let unique_bytes: HashSet<u8> = data.iter()
            .flat_map(|s| s.iter())
            .cloned()
            .collect();
        
        1.0 - (unique_bytes.len() as f64 / total_bytes as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_bytes() {
        let input = b"hello:world:test";
        let parts = ByteUtils::split_bytes(input, b':');
        
        // Convert to Vec<Vec<u8>> for easier comparison
        let parts_vec: Vec<Vec<u8>> = parts.iter().map(|&slice| slice.to_vec()).collect();
        let expected = vec![
            b"hello".to_vec(),
            b"world".to_vec(), 
            b"test".to_vec()
        ];
        
        assert_eq!(parts_vec, expected);
    }

    #[test]
    fn test_split_bytes_alternative() {
        let input = b"hello:world:test";
        let parts = ByteUtils::split_bytes(input, b':');
        
        // Compare each part individually
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0], b"hello");
        assert_eq!(parts[1], b"world");
        assert_eq!(parts[2], b"test");
    }

    #[test]
    fn test_parse_functions() {
        assert_eq!(ByteUtils::parse_u8(b"123").unwrap(), 123);
        assert_eq!(ByteUtils::parse_u16(b"1234").unwrap(), 1234);
        assert_eq!(ByteUtils::parse_u32(b"12345").unwrap(), 12345);
        
        // Test error cases
        assert!(ByteUtils::parse_u8(b"256").is_err()); // Too large for u8
        assert!(ByteUtils::parse_u8(b"abc").is_err()); // Not a number
    }

    #[test]
    fn test_bytes_to_string_lossy() {
        let valid_utf8 = b"hello world";
        assert_eq!(ByteUtils::bytes_to_string_lossy(valid_utf8), "hello world");
        
        // Test with invalid UTF-8
        let invalid_utf8 = &[0xFF, 0xFE, 0xFD];
        let result = ByteUtils::bytes_to_string_lossy(invalid_utf8);
        assert!(!result.is_empty()); // Should produce some replacement characters
    }
}