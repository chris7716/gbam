//! Illumina read name tokenizer implementation

use super::{
    error::TokenizationError,
    dictionary::{ReadNameDictionary, TokenizedReadName},
    utils::ByteUtils,
    TokenizationStats,
};

pub struct IlluminaTokenizer {
    dictionary: ReadNameDictionary,
    total_reads_processed: usize,
    successfully_tokenized: usize,
}

impl IlluminaTokenizer {
    pub fn new() -> Self {
        Self {
            dictionary: ReadNameDictionary::new(),
            total_reads_processed: 0,
            successfully_tokenized: 0,
        }
    }

    pub fn with_dictionary(dictionary: ReadNameDictionary) -> Self {
        Self { 
            dictionary,
            total_reads_processed: 0,
            successfully_tokenized: 0,
        }
    }

    pub fn tokenize_batch(&mut self, read_names: &[&[u8]]) -> Result<Vec<TokenizedReadName>, TokenizationError> {
        let mut results = Vec::with_capacity(read_names.len());
        
        for (index, name) in read_names.iter().enumerate() {
            self.total_reads_processed += 1;
            match self.tokenize_single(name) {
                Ok(tokenized) => {
                    self.successfully_tokenized += 1;
                    results.push(tokenized);
                },
                Err(e) => return Err(TokenizationError::ParseError(
                    format!("Failed to tokenize read {} ({}): {}", index, ByteUtils::bytes_to_string_lossy(name), e)
                )),
            }
        }
        
        Ok(results)
    }

    pub fn tokenize_single(&mut self, read_name: &[u8]) -> Result<TokenizedReadName, TokenizationError> {
        let name_str = std::str::from_utf8(read_name)
            .map_err(|e| TokenizationError::ParseError(e.to_string()))?;
        
        // Try modern Illumina format first
        if let Ok(tokenized) = self.parse_modern_illumina(name_str) {
            return Ok(tokenized);
        }
        
        // Try legacy Illumina format
        if let Ok(tokenized) = self.parse_legacy_illumina(name_str) {
            return Ok(tokenized);
        }
        
        Err(TokenizationError::InvalidFormat(
            format!("Unrecognized Illumina format: {}", name_str)
        ))
    }

    // Modern Illumina format: INSTRUMENT:RUN:FLOWCELL:LANE:TILE:X:Y UMI:READ:FLAGS:INDEX
    // Fix the parse_modern_illumina method around line 81:
    fn parse_modern_illumina(&mut self, name_str: &str) -> Result<TokenizedReadName, TokenizationError> {
        let parts: Vec<&str> = name_str.split([':', ' ']).collect();
        
        if parts.len() < 7 {
            return Err(TokenizationError::InvalidFormat("Not modern Illumina format".to_string()));
        }

        let instrument_id = self.dictionary.add_instrument(parts[0].as_bytes());
        let run_id: u32 = parts[1].parse::<u32>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let flowcell_id = self.dictionary.add_flowcell(parts[2].as_bytes());
        let lane: u8 = parts[3].parse::<u8>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let tile: u16 = parts[4].parse::<u16>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let x_coord: u32 = parts[5].parse::<u32>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let y_coord: u32 = parts[6].parse::<u32>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;

        // Optional fields
        let umi_id = if parts.len() > 7 && !parts[7].is_empty() {
            Some(self.dictionary.add_umi(parts[7].as_bytes()))
        } else {
            None
        };

        let read_num = if parts.len() > 8 {
            parts[8].parse().unwrap_or(1)
        } else {
            1
        };

        let flags = if parts.len() > 9 {
            self.parse_flags_from_string(parts[9])
        } else {
            0
        };

        let index_id = if parts.len() > 10 && !parts[10].is_empty() {
            Some(self.dictionary.add_index(parts[10].as_bytes()))
        } else {
            None
        };

        Ok(TokenizedReadName {
            instrument_id,
            run_id,
            flowcell_id,
            lane,
            tile,
            x_coord,
            y_coord,
            umi_id,
            read_num,
            flags,
            index_id,
        })
    }

    // Fix the parse_legacy_illumina method around line 156:
    fn parse_legacy_illumina(&mut self, name_str: &str) -> Result<TokenizedReadName, TokenizationError> {
        // Split by '#' first to separate index/UMI part
        let main_parts: Vec<&str> = name_str.split('#').collect();
        let coordinate_part = main_parts[0];
        
        // Parse the main coordinate part
        let parts: Vec<&str> = coordinate_part.split(':').collect();
        
        if parts.len() != 5 {
            return Err(TokenizationError::InvalidFormat(
                format!("Legacy format should have 5 colon-separated parts, got {}", parts.len())
            ));
        }

        // Parse instrument_run (like "HWUSI-EAS566_0007")
        let instrument_run = parts[0];
        let instrument_id = self.dictionary.add_instrument(instrument_run.as_bytes());
        
        // For legacy format, we don't have separate run ID, so use a hash of the instrument
        let run_id = self.hash_string(instrument_run);
        
        // Parse lane, tile, x, y
        let lane: u8 = parts[1].parse::<u8>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let tile: u16 = parts[2].parse::<u16>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let x_coord: u32 = parts[3].parse::<u32>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;
        let y_coord: u32 = parts[4].parse::<u32>()
            .map_err(|e: std::num::ParseIntError| TokenizationError::ParseError(e.to_string()))?;

        // Parse index and UMI from the suffix (if present)
        let (index_id, umi_id) = if main_parts.len() > 1 {
            let suffix = main_parts[1];
            if let Some(umi_pos) = suffix.find('|') {
                // Format: #INDEX|UMI
                let index_part = &suffix[..umi_pos];
                let umi_part = &suffix[umi_pos + 1..];
                
                let index_id = if !index_part.is_empty() {
                    Some(self.dictionary.add_index(index_part.as_bytes()))
                } else {
                    None
                };
                
                let umi_id = if !umi_part.is_empty() {
                    Some(self.dictionary.add_umi(umi_part.as_bytes()))
                } else {
                    None
                };
                
                (index_id, umi_id)
            } else {
                // Just index, no UMI
                let index_id = if !suffix.is_empty() {
                    Some(self.dictionary.add_index(suffix.as_bytes()))
                } else {
                    None
                };
                (index_id, None)
            }
        } else {
            (None, None)
        };

        Ok(TokenizedReadName {
            instrument_id,
            run_id,
            flowcell_id: 0, // Legacy format doesn't have separate flowcell ID
            lane,
            tile,
            x_coord,
            y_coord,
            umi_id,
            read_num: 1,
            flags: 0,
            index_id,
        })
    }

    fn parse_flags_from_string(&self, flags_str: &str) -> u8 {
        // Parse flags like "Y", "N", "0", "1", etc.
        match flags_str {
            "Y" => 1,
            "N" => 0,
            _ => flags_str.parse().unwrap_or(0),
        }
    }

    fn hash_string(&self, s: &str) -> u32 {
        let mut hash = 0u32;
        for byte in s.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        hash
    }

    pub fn detokenize(&self, tokenized: &TokenizedReadName) -> Result<Vec<u8>, TokenizationError> {
        let instrument = self.dictionary.get_instrument(tokenized.instrument_id)
            .ok_or_else(|| TokenizationError::InvalidFormat(
                format!("Instrument ID {} not found", tokenized.instrument_id)
            ))?;
        
        let mut name = Vec::new();
        
        // Build the read name - handle legacy format differently
        name.extend_from_slice(instrument);
        
        if tokenized.flowcell_id == 0 {
            // Legacy format: INSTRUMENT:LANE:TILE:X:Y#INDEX|UMI
            name.push(b':');
            name.extend_from_slice(tokenized.lane.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.tile.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.x_coord.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.y_coord.to_string().as_bytes());
            
            // Add index and UMI if present
            if tokenized.index_id.is_some() || tokenized.umi_id.is_some() {
                name.push(b'#');
                
                if let Some(index_id) = tokenized.index_id {
                    if let Some(index) = self.dictionary.get_index(index_id) {
                        name.extend_from_slice(index);
                    }
                }
                
                if let Some(umi_id) = tokenized.umi_id {
                    if let Some(umi) = self.dictionary.get_umi(umi_id) {
                        name.push(b'|');
                        name.extend_from_slice(umi);
                    }
                }
            }
        } else {
            // Modern format: INSTRUMENT:RUN:FLOWCELL:LANE:TILE:X:Y
            name.push(b':');
            name.extend_from_slice(tokenized.run_id.to_string().as_bytes());
            
            if let Some(flowcell) = self.dictionary.get_flowcell(tokenized.flowcell_id) {
                name.push(b':');
                name.extend_from_slice(flowcell);
            }
            
            name.push(b':');
            name.extend_from_slice(tokenized.lane.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.tile.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.x_coord.to_string().as_bytes());
            name.push(b':');
            name.extend_from_slice(tokenized.y_coord.to_string().as_bytes());

            // Add UMI if present
            if let Some(umi_id) = tokenized.umi_id {
                if let Some(umi) = self.dictionary.get_umi(umi_id) {
                    name.push(b':');
                    name.extend_from_slice(umi);
                }
            }

            // Add read number
            name.push(b':');
            name.extend_from_slice(tokenized.read_num.to_string().as_bytes());

            // Add flags
            name.push(b':');
            name.push(if tokenized.flags & 0x01 != 0 { b'Y' } else { b'N' });

            // Add index if present
            if let Some(index_id) = tokenized.index_id {
                if let Some(index) = self.dictionary.get_index(index_id) {
                    name.push(b':');
                    name.extend_from_slice(index);
                }
            }
        }

        Ok(name)
    }

    pub fn get_dictionary(&self) -> &ReadNameDictionary {
        &self.dictionary
    }

    pub fn get_dictionary_mut(&mut self) -> &mut ReadNameDictionary {
        &mut self.dictionary
    }

    pub fn dictionary_size(&self) -> usize {
        let (instruments, flowcells, umis, indices) = self.dictionary.entry_counts();
        instruments + flowcells + umis + indices
    }

    pub fn serialize_dictionary(&self) -> Vec<u8> {
        // Simple serialization for testing
        let mut result = Vec::new();
        
        // Serialize instruments
        result.extend_from_slice(&(self.dictionary.instruments.len() as u32).to_le_bytes());
        for instrument in &self.dictionary.instruments {
            result.extend_from_slice(&(instrument.len() as u32).to_le_bytes());
            result.extend_from_slice(instrument);
        }
        
        // Serialize flowcells
        result.extend_from_slice(&(self.dictionary.flowcells.len() as u32).to_le_bytes());
        for flowcell in &self.dictionary.flowcells {
            result.extend_from_slice(&(flowcell.len() as u32).to_le_bytes());
            result.extend_from_slice(flowcell);
        }
        
        // Serialize UMIs
        result.extend_from_slice(&(self.dictionary.umis.len() as u32).to_le_bytes());
        for umi in &self.dictionary.umis {
            result.extend_from_slice(&(umi.len() as u32).to_le_bytes());
            result.extend_from_slice(umi);
        }
        
        // Serialize indices
        result.extend_from_slice(&(self.dictionary.indices.len() as u32).to_le_bytes());
        for index in &self.dictionary.indices {
            result.extend_from_slice(&(index.len() as u32).to_le_bytes());
            result.extend_from_slice(index);
        }
        
        result
    }

    pub fn get_stats(&self) -> TokenizationStats {
        TokenizationStats {
            total_reads: self.total_reads_processed,
            successfully_tokenized: self.successfully_tokenized,
            dictionary_size: self.dictionary.total_size(),
            compression_ratio: if self.total_reads_processed > 0 {
                self.successfully_tokenized as f64 / self.total_reads_processed as f64
            } else {
                0.0
            },
        }
    }

    pub fn calculate_compression_stats(&self, original_names: &[&[u8]], tokenized: &[TokenizedReadName]) -> CompressionStats {
        let original_size: usize = original_names.iter().map(|name| name.len()).sum();
        
        // Calculate tokenized size: each TokenizedReadName is approximately 19 bytes
        // instrument_id (1) + run_id (4) + flowcell_id (1) + lane (1) + tile (2) + 
        // x_coord (4) + y_coord (4) + umi_id opt (2) + read_num (1) + flags (1) + index_id opt (1) = ~22 bytes
        let tokenized_size = tokenized.len() * 22;
        
        let dictionary_size = self.dictionary.total_size();
        let total_compressed_size = tokenized_size + dictionary_size;
        
        CompressionStats {
            total_reads: original_names.len(),
            original_size,
            tokenized_size,
            dictionary_size,
            compression_ratio: if total_compressed_size > 0 {
                original_size as f64 / total_compressed_size as f64
            } else {
                0.0
            },
        }
    }

    pub fn reset_stats(&mut self) {
        self.total_reads_processed = 0;
        self.successfully_tokenized = 0;
    }

    pub fn clear_dictionary(&mut self) {
        self.dictionary = ReadNameDictionary::new();
    }

    pub fn is_empty(&self) -> bool {
        let (instruments, flowcells, umis, indices) = self.dictionary.entry_counts();
        instruments == 0 && flowcells == 0 && umis == 0 && indices == 0
    }
}

impl Default for IlluminaTokenizer {
    fn default() -> Self {
        Self::new()
    }
}

// Support structures for the compressor integration
#[derive(Debug, Clone)]
pub struct CompressionStats {
    pub total_reads: usize,
    pub original_size: usize,
    pub tokenized_size: usize,
    pub dictionary_size: usize,
    pub compression_ratio: f64,
}

// Simplified dictionary for display purposes (for compressor compatibility)
#[derive(Debug, Clone)]
pub struct ReadNameDictionaryDisplay {
    pub instruments: Vec<Vec<u8>>,
    pub flowcells: Vec<Vec<u8>>,
}

// Helper method to convert for display
impl IlluminaTokenizer {
    pub fn get_dictionary_for_display(&self) -> ReadNameDictionaryDisplay {
        ReadNameDictionaryDisplay {
            instruments: self.dictionary.instruments.clone(),
            flowcells: self.dictionary.flowcells.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legacy_illumina_format() {
        let mut tokenizer = IlluminaTokenizer::new();
        let read_name = b"HWUSI-EAS566_0007:2:30:18804:9636#0|AGC";
        
        let result = tokenizer.tokenize_single(read_name);
        assert!(result.is_ok());
        
        let tokenized = result.unwrap();
        assert_eq!(tokenized.lane, 2);
        assert_eq!(tokenized.tile, 30);
        assert_eq!(tokenized.x_coord, 18804);
        assert_eq!(tokenized.y_coord, 9636);
        assert_eq!(tokenized.flowcell_id, 0); // Legacy format uses 0
    }

    #[test]
    fn test_modern_illumina_format() {
        let mut tokenizer = IlluminaTokenizer::new();
        let read_name = b"INSTRUMENT:123:FLOWCELL:1:1234:5678:9012";
        
        let result = tokenizer.tokenize_single(read_name);
        assert!(result.is_ok());
        
        let tokenized = result.unwrap();
        assert_eq!(tokenized.lane, 1);
        assert_eq!(tokenized.tile, 1234);
        assert_eq!(tokenized.x_coord, 5678);
        assert_eq!(tokenized.y_coord, 9012);
        assert_ne!(tokenized.flowcell_id, 0); // Modern format has flowcell ID
    }

    #[test]
    fn test_batch_tokenization() {
        let mut tokenizer = IlluminaTokenizer::new();
        let read_names = vec![
            b"HWUSI-EAS566_0007:2:30:18804:9636#0|AGC".as_slice(),
            b"HWUSI-EAS566_0007:2:30:18804:9637#0|AGC".as_slice(),
        ];
        
        let result = tokenizer.tokenize_batch(&read_names);
        assert!(result.is_ok());
        
        let tokenized = result.unwrap();
        assert_eq!(tokenized.len(), 2);
        assert_eq!(tokenized[0].y_coord, 9636);
        assert_eq!(tokenized[1].y_coord, 9637);
    }
}