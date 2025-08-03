// src/tokenizer/post_compression.rs
use crate::tokenizer_encoding::{TokenizedReadName, ReadNameDictionary, ByteUtils};
use std::collections::HashMap;
use flate2::{Compression, write::ZlibEncoder};
use std::io::Write;

#[derive(Debug, Clone)]
pub struct PostTokenizationConfig {
    pub use_rle: bool,
    pub use_huffman: bool,
    pub use_delta_encoding: bool,
    pub use_deflate: bool,
    pub rle_threshold: f64,
}

impl Default for PostTokenizationConfig {
    fn default() -> Self {
        Self {
            use_rle: true,
            use_huffman: true,
            use_delta_encoding: true,
            use_deflate: true,
            rle_threshold: 0.2,
        }
    }
}

pub struct PostTokenizationCompressor {
    config: PostTokenizationConfig,
}

impl PostTokenizationCompressor {
    pub fn new(config: PostTokenizationConfig) -> Self {
        Self { config }
    }

    pub fn compress_tokenized_data(
        &self,
        tokenized: &[TokenizedReadName],
        dictionary: &ReadNameDictionary,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        
        // Step 1: Separate into streams
        let streams = self.separate_into_streams(tokenized);
        
        // Step 2: Compress dictionary
        let compressed_dict = self.compress_dictionary(dictionary)?;
        
        // Step 3: Compress each stream
        let compressed_streams = self.compress_all_streams(&streams)?;
        
        // Step 4: Assemble final block
        Ok(self.assemble_block(compressed_dict, compressed_streams))
    }

    fn separate_into_streams(&self, tokenized: &[TokenizedReadName]) -> TokenizedStreams {
        let mut streams = TokenizedStreams::new(tokenized.len());
        
        for token in tokenized {
            streams.instrument_ids.push(token.instrument_id);
            streams.run_ids.push(token.run_id);
            streams.flowcell_ids.push(token.flowcell_id);
            streams.lanes.push(token.lane);
            streams.tiles.push(token.tile);
            streams.x_coords.push(token.x_coord);
            streams.y_coords.push(token.y_coord);
            streams.umi_ids.push(token.umi_id);
            streams.read_nums.push(token.read_num);
            streams.flags.push(token.flags);
            streams.index_ids.push(token.index_id);
        }
        
        streams
    }

    fn compress_categorical_stream(&self, data: &[u8], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut working_data = data.to_vec();
        
        // Stage 1: RLE if beneficial
        if self.config.use_rle && self.calculate_rle_benefit(&working_data) > self.config.rle_threshold {
            working_data = self.run_length_encode(&working_data);
            println!("Applied RLE to {}: {} -> {} bytes", stream_name, data.len(), working_data.len());
        }
        
        // Stage 2: Huffman encoding
        if self.config.use_huffman && self.should_use_huffman(&working_data) {
            working_data = self.huffman_encode(&working_data)?;
            println!("Applied Huffman to {}: size after Huffman: {} bytes", stream_name, working_data.len());
        }
        
        // Stage 3: Final DEFLATE compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&working_data)?;
            println!("Applied DEFLATE to {}: {} -> {} bytes", stream_name, working_data.len(), deflated.len());
            working_data = deflated;
        }
        
        Ok(working_data)
    }

    fn compress_numeric_stream(&self, data: &[u32], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Stage 1: Delta encoding + varint
        let mut compressed = if self.config.use_delta_encoding && data.len() > 1 {
            println!("Applying delta encoding to {}", stream_name);
            self.delta_encode_with_varint(data)
        } else {
            println!("Applying direct varint encoding to {}", stream_name);
            self.direct_varint_encode(data)
        };
        
        println!("{} after varint: {} bytes", stream_name, compressed.len());
        
        // Stage 2: Final compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&compressed)?;
            println!("{} after DEFLATE: {} -> {} bytes", stream_name, compressed.len(), deflated.len());
            compressed = deflated;
        }
        
        Ok(compressed)
    }

    fn compress_coordinate_streams(&self, x_coords: &[u32], y_coords: &[u32], tiles: &[u16]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        println!("Compressing coordinate streams...");
        
        // Stage 1: 2D delta encoding
        let deltas = self.encode_2d_deltas(x_coords, y_coords, tiles);
        println!("Coordinate deltas calculated: {} deltas", deltas.len());
        
        // Stage 2: Interleave and varint encode
        let mut compressed = Vec::new();
        for delta in &deltas {
            compressed.extend(self.encode_varint(delta.dx));
            compressed.extend(self.encode_varint(delta.dy));
            compressed.extend(self.encode_varint(delta.dtile as i32));
        }
        
        println!("Coordinates after varint: {} bytes", compressed.len());
        
        // Stage 3: Final compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&compressed)?;
            println!("Coordinates after DEFLATE: {} -> {} bytes", compressed.len(), deflated.len());
            compressed = deflated;
        }
        
        Ok(compressed)
    }

    fn compress_sparse_stream(&self, data: &[Option<u16>], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Stage 1: Create bitmap and values
        let mut bitmap = Vec::new();
        let mut values = Vec::new();
        let mut current_byte = 0u8;
        let mut bit_pos = 0;
        
        for opt_val in data {
            if let Some(val) = opt_val {
                current_byte |= 1 << bit_pos;
                values.extend(self.encode_varint(*val as i32));
            }
            
            bit_pos += 1;
            if bit_pos == 8 {
                bitmap.push(current_byte);
                current_byte = 0;
                bit_pos = 0;
            }
        }
        
        // Push final byte if needed
        if bit_pos > 0 {
            bitmap.push(current_byte);
        }
        
        println!("{}: bitmap {} bytes, values {} bytes", stream_name, bitmap.len(), values.len());
        
        // Stage 2: Compress bitmap and values
        let compressed_bitmap = if self.config.use_deflate {
            self.deflate_compress(&bitmap)?
        } else {
            bitmap
        };
        
        let compressed_values = if self.config.use_deflate {
            self.deflate_compress(&values)?
        } else {
            values
        };
        
        // Stage 3: Combine
        let mut result = Vec::new();
        result.extend(self.encode_varint(compressed_bitmap.len() as i32));
        result.extend(compressed_bitmap);
        result.extend(self.encode_varint(compressed_values.len() as i32));
        result.extend(compressed_values);
        
        println!("{} final size: {} bytes", stream_name, result.len());
        Ok(result)
    }

    fn compress_sparse_u8_stream(&self, data: &[Option<u8>], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let u16_data: Vec<Option<u16>> = data.iter().map(|&opt| opt.map(|v| v as u16)).collect();
        self.compress_sparse_stream(&u16_data, stream_name)
    }

    // Helper functions
    fn calculate_rle_benefit(&self, data: &[u8]) -> f64 {
        if data.len() < 10 {
            return 0.0;
        }
        
        let mut runs = 0usize;  // Explicitly specify type
        let mut total_run_length = 0usize;  // Explicitly specify type
        let mut current_run = 1usize;  // Explicitly specify type
        
        for i in 1..data.len() {
            if data[i] == data[i-1] {
                current_run += 1;
            } else {
                if current_run >= 3 {
                    runs += 1;
                    total_run_length += current_run;
                }
                current_run = 1;
            }
        }
        
        if current_run >= 3 {
            runs += 1;
            total_run_length += current_run;
        }
        
        let bytes_saved = total_run_length.saturating_sub(runs * 2);
        bytes_saved as f64 / data.len() as f64
    }

    fn run_length_encode(&self, data: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::new();
        if data.is_empty() {
            return encoded;
        }
        
        let mut current = data[0];
        let mut count = 1u32;
        
        for &byte in &data[1..] {
            if byte == current && count < u32::MAX {
                count += 1;
            } else {
                encoded.extend(self.encode_varint(count as i32));
                encoded.push(current);
                current = byte;
                count = 1;
            }
        }
        
        encoded.extend(self.encode_varint(count as i32));
        encoded.push(current);
        encoded
    }

    fn should_use_huffman(&self, data: &[u8]) -> bool {
        let mut frequencies = HashMap::new();
        for &byte in data {
            *frequencies.entry(byte).or_insert(0) += 1;
        }
        
        let unique_symbols = frequencies.len();
        unique_symbols < data.len() / 2 && unique_symbols > 1 && data.len() > 20
    }

    fn huffman_encode(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Simplified: For now, just return the data
        // A full implementation would build Huffman trees and encode
        Ok(data.to_vec())
    }

    fn delta_encode_with_varint(&self, data: &[u32]) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend(self.encode_varint(data[0] as i32));
        
        for i in 1..data.len() {
            let delta = data[i] as i32 - data[i-1] as i32;
            encoded.extend(self.encode_varint(delta));
        }
        
        encoded
    }

    fn direct_varint_encode(&self, data: &[u32]) -> Vec<u8> {
        let mut encoded = Vec::new();
        for &value in data {
            encoded.extend(self.encode_varint(value as i32));
        }
        encoded
    }

    fn encode_2d_deltas(&self, x_coords: &[u32], y_coords: &[u32], tiles: &[u16]) -> Vec<CoordinateDelta> {
        let mut deltas = Vec::with_capacity(x_coords.len());
        let mut last_x = 0u32;
        let mut last_y = 0u32;
        let mut last_tile = 0u16;
        
        for i in 0..x_coords.len() {
            let delta = CoordinateDelta {
                dx: x_coords[i] as i32 - last_x as i32,
                dy: y_coords[i] as i32 - last_y as i32,
                dtile: tiles[i] as i32 - last_tile as i32,
            };
            
            deltas.push(delta);
            last_x = x_coords[i];
            last_y = y_coords[i];
            last_tile = tiles[i];
        }
        
        deltas
    }

    fn encode_varint(&self, mut value: i32) -> Vec<u8> {
        let mut encoded = Vec::new();
        let mut uvalue = ((value << 1) ^ (value >> 31)) as u32; // ZigZag encoding
        
        while uvalue >= 0x80 {
            encoded.push((uvalue & 0x7F) as u8 | 0x80);
            uvalue >>= 7;
        }
        encoded.push(uvalue as u8);
        encoded
    }

    fn deflate_compress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data)?;
        Ok(encoder.finish()?)
    }

    fn compress_all_streams(&self, streams: &TokenizedStreams) -> Result<CompressedStreams, Box<dyn std::error::Error>> {
        println!("\n=== Compressing Individual Streams ===");
        
        Ok(CompressedStreams {
            instrument_ids: self.compress_categorical_stream(&streams.instrument_ids, "instrument_ids")?,
            run_ids: self.compress_numeric_stream(&streams.run_ids, "run_ids")?,
            flowcell_ids: self.compress_categorical_stream(&streams.flowcell_ids, "flowcell_ids")?,
            lanes: self.compress_categorical_stream(&streams.lanes, "lanes")?,
            read_nums: self.compress_categorical_stream(&streams.read_nums, "read_nums")?,
            flags: self.compress_categorical_stream(&streams.flags, "flags")?,
            coordinates: self.compress_coordinate_streams(&streams.x_coords, &streams.y_coords, &streams.tiles)?,
            umi_ids: self.compress_sparse_stream(&streams.umi_ids, "umi_ids")?,
            index_ids: self.compress_sparse_u8_stream(&streams.index_ids, "index_ids")?,
        })
    }

    fn compress_dictionary(&self, dictionary: &ReadNameDictionary) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut serialized = Vec::new();
        
        // Serialize each section
        for (section_name, section) in [
            ("instruments", &dictionary.instruments),
            ("flowcells", &dictionary.flowcells), 
            ("umis", &dictionary.umis),
            ("indices", &dictionary.indices),
        ] {
            serialized.extend(self.encode_varint(section.len() as i32));
            for item in section {
                serialized.extend(self.encode_varint(item.len() as i32));
                serialized.extend(item);
            }
        }
        
        println!("Dictionary serialized: {} bytes", serialized.len());
        
        if self.config.use_deflate {
            let compressed = self.deflate_compress(&serialized)?;
            println!("Dictionary after compression: {} -> {} bytes", serialized.len(), compressed.len());
            Ok(compressed)
        } else {
            Ok(serialized)
        }
    }

    fn assemble_block(&self, compressed_dict: Vec<u8>, compressed_streams: CompressedStreams) -> Vec<u8> {
        let mut block = Vec::new();
        
        // Block header
        block.extend(self.encode_varint(compressed_dict.len() as i32));
        block.extend(compressed_dict);
        
        // Stream data
        block.extend(self.encode_varint(compressed_streams.instrument_ids.len() as i32));
        block.extend(compressed_streams.instrument_ids);
        
        block.extend(self.encode_varint(compressed_streams.run_ids.len() as i32));
        block.extend(compressed_streams.run_ids);
        
        block.extend(self.encode_varint(compressed_streams.flowcell_ids.len() as i32));
        block.extend(compressed_streams.flowcell_ids);
        
        block.extend(self.encode_varint(compressed_streams.lanes.len() as i32));
        block.extend(compressed_streams.lanes);
        
        block.extend(self.encode_varint(compressed_streams.read_nums.len() as i32));
        block.extend(compressed_streams.read_nums);
        
        block.extend(self.encode_varint(compressed_streams.flags.len() as i32));
        block.extend(compressed_streams.flags);
        
        block.extend(self.encode_varint(compressed_streams.coordinates.len() as i32));
        block.extend(compressed_streams.coordinates);
        
        block.extend(self.encode_varint(compressed_streams.umi_ids.len() as i32));
        block.extend(compressed_streams.umi_ids);
        
        block.extend(self.encode_varint(compressed_streams.index_ids.len() as i32));
        block.extend(compressed_streams.index_ids);
        
        println!("Final assembled block size: {} bytes", block.len());
        block
    }

    // Add remaining helper methods...
}

#[derive(Debug)]
struct TokenizedStreams {
    instrument_ids: Vec<u8>,
    run_ids: Vec<u32>,
    flowcell_ids: Vec<u8>,
    lanes: Vec<u8>,
    tiles: Vec<u16>,
    x_coords: Vec<u32>,
    y_coords: Vec<u32>,
    umi_ids: Vec<Option<u16>>,
    read_nums: Vec<u8>,
    flags: Vec<u8>,
    index_ids: Vec<Option<u8>>,
}

impl TokenizedStreams {
    fn new(capacity: usize) -> Self {
        Self {
            instrument_ids: Vec::with_capacity(capacity),
            run_ids: Vec::with_capacity(capacity),
            flowcell_ids: Vec::with_capacity(capacity),
            lanes: Vec::with_capacity(capacity),
            tiles: Vec::with_capacity(capacity),
            x_coords: Vec::with_capacity(capacity),
            y_coords: Vec::with_capacity(capacity),
            umi_ids: Vec::with_capacity(capacity),
            read_nums: Vec::with_capacity(capacity),
            flags: Vec::with_capacity(capacity),
            index_ids: Vec::with_capacity(capacity),
        }
    }
}

#[derive(Debug)]
struct CoordinateDelta {
    dx: i32,
    dy: i32,
    dtile: i32,
}

#[derive(Debug)]
struct CompressedStreams {
    instrument_ids: Vec<u8>,
    run_ids: Vec<u8>,
    flowcell_ids: Vec<u8>,
    lanes: Vec<u8>,
    read_nums: Vec<u8>,
    flags: Vec<u8>,
    coordinates: Vec<u8>,
    umi_ids: Vec<u8>,
    index_ids: Vec<u8>,
}
