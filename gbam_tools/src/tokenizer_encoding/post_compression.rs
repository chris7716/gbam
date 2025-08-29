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
        }
        
        // Stage 2: Huffman encoding
        if self.config.use_huffman && self.should_use_huffman(&working_data) {
            working_data = self.huffman_encode(&working_data)?;
        }
        
        // Stage 3: Final DEFLATE compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&working_data)?;
            working_data = deflated;
        }
        
        Ok(working_data)
    }

    fn compress_numeric_stream(&self, data: &[u32], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Stage 1: Delta encoding + varint
        let mut compressed = if self.config.use_delta_encoding && data.len() > 1 {
            self.delta_encode_with_varint(data)
        } else {
            self.direct_varint_encode(data)
        };
        
        // Stage 2: Final compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&compressed)?;
            compressed = deflated;
        }
        
        Ok(compressed)
    }

    fn compress_coordinate_streams(&self, x_coords: &[u32], y_coords: &[u32], tiles: &[u16]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Stage 1: 2D delta encoding
        let deltas = self.encode_2d_deltas(x_coords, y_coords, tiles);
        
        // Stage 2: Interleave and varint encode
        let mut compressed = Vec::new();
        for delta in &deltas {
            compressed.extend(self.encode_varint(delta.dx));
            compressed.extend(self.encode_varint(delta.dy));
            compressed.extend(self.encode_varint(delta.dtile as i32));
        }
        
        // Stage 3: Final compression
        if self.config.use_deflate {
            let deflated = self.deflate_compress(&compressed)?;
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

        
        if self.config.use_deflate {
            let compressed = self.deflate_compress(&serialized)?;
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

        block
    }

    fn decode_varint(&self, data: &[u8], cursor: &mut usize) -> Result<i32, Box<dyn std::error::Error>> {
        let mut result = 0u32;
        let mut shift = 0;
        
        while *cursor < data.len() {
            let byte = data[*cursor];
            *cursor += 1;
            
            result |= ((byte & 0x7F) as u32) << shift;
            
            if (byte & 0x80) == 0 {
                break;
            }
            
            shift += 7;
            if shift >= 35 { // Allow full 5-byte varint
                return Err("Varint too long".into());
            }
        }
        
        // ZigZag decode: (n >> 1) ^ (-(n & 1))
        let zigzag_decoded = ((result >> 1) as i32) ^ (-((result & 1) as i32));
        Ok(zigzag_decoded)
    }
    
    pub fn decompress_tokenized_data(
        &self,
        compressed_data: &[u8],
        dictionary: &ReadNameDictionary,
    ) -> Result<Vec<TokenizedReadName>, Box<dyn std::error::Error>> {
        
        // Step 1: Parse the varint-encoded dictionary size
        let mut cursor = 0;
        let dict_size = self.decode_varint(compressed_data, &mut cursor)?;
        
        if dict_size < 0 || dict_size as usize > compressed_data.len() {
            return Err(format!("Invalid dictionary size: {}", dict_size).into());
        }
        
        // Step 2: Skip embedded dictionary (we use the provided one)
        let embedded_dict = &compressed_data[cursor..cursor + dict_size as usize];
        let streams_start = cursor + dict_size as usize;
        
        if streams_start > compressed_data.len() {
            return Err("Invalid compressed data: dictionary size exceeds data length".into());
        }

        let streams_data = &compressed_data[streams_start..];
        
        // Step 3: Decompress all streams
        let streams = self.decompress_all_streams(streams_data)?;
        
        // Step 4: Reconstruct tokens from streams
        Ok(self.reconstruct_tokens(streams))
    }

    fn decompress_all_streams(&self, streams_data: &[u8]) -> Result<TokenizedStreams, Box<dyn std::error::Error>> {
        let mut cursor = 0;
        
        // Stream 1: instrument_ids
        let instrument_ids_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + instrument_ids_size as usize > streams_data.len() {
            return Err(format!("instrument_ids stream exceeds data bounds").into());
        }
        let instrument_ids_data = &streams_data[cursor..cursor + instrument_ids_size as usize];
        cursor += instrument_ids_size as usize;
        let instrument_ids = self.decompress_u8_stream(instrument_ids_data, "instrument_ids")?;
        
        // Stream 2: run_ids
        let run_ids_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + run_ids_size as usize > streams_data.len() {
            return Err(format!("run_ids stream exceeds data bounds").into());
        }
        let run_ids_data = &streams_data[cursor..cursor + run_ids_size as usize];
        cursor += run_ids_size as usize;
        let run_ids = self.decompress_u32_stream(run_ids_data, "run_ids")?;
        
        // Stream 3: flowcell_ids
        let flowcell_ids_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + flowcell_ids_size as usize > streams_data.len() {
            return Err(format!("flowcell_ids stream exceeds data bounds").into());
        }
        let flowcell_ids_data = &streams_data[cursor..cursor + flowcell_ids_size as usize];
        cursor += flowcell_ids_size as usize;
        let flowcell_ids = self.decompress_u8_stream(flowcell_ids_data, "flowcell_ids")?;
        
        // Stream 4: lanes
        let lanes_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + lanes_size as usize > streams_data.len() {
            return Err(format!("lanes stream exceeds data bounds").into());
        }
        let lanes_data = &streams_data[cursor..cursor + lanes_size as usize];
        cursor += lanes_size as usize;
        let lanes = self.decompress_u8_stream(lanes_data, "lanes")?;
        
        // Stream 5: read_nums
        let read_nums_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + read_nums_size as usize > streams_data.len() {
            return Err(format!("read_nums stream exceeds data bounds").into());
        }
        let read_nums_data = &streams_data[cursor..cursor + read_nums_size as usize];
        cursor += read_nums_size as usize;
        let read_nums = self.decompress_u8_stream(read_nums_data, "read_nums")?;
        
        // Stream 6: flags
        let flags_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + flags_size as usize > streams_data.len() {
            return Err(format!("flags stream exceeds data bounds").into());
        }
        let flags_data = &streams_data[cursor..cursor + flags_size as usize];
        cursor += flags_size as usize;
        let flags = self.decompress_u8_stream(flags_data, "flags")?;
        
        // Stream 7: coordinates (combined x_coords, y_coords, tiles)
        let coordinates_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + coordinates_size as usize > streams_data.len() {
            return Err(format!("coordinates stream exceeds data bounds").into());
        }
        let coordinates_data = &streams_data[cursor..cursor + coordinates_size as usize];
        cursor += coordinates_size as usize;
        let (x_coords, y_coords, tiles) = self.decompress_coordinate_streams(coordinates_data)?;
        
        // Stream 8: umi_ids (sparse)
        let umi_ids_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + umi_ids_size as usize > streams_data.len() {
            return Err(format!("umi_ids stream exceeds data bounds").into());
        }
        let umi_ids_data = &streams_data[cursor..cursor + umi_ids_size as usize];
        cursor += umi_ids_size as usize;
        let umi_ids = self.decompress_sparse_u16_stream(umi_ids_data, "umi_ids")?;
        
        // Stream 9: index_ids (sparse)
        let index_ids_size = self.decode_varint(streams_data, &mut cursor)?;
        if cursor + index_ids_size as usize > streams_data.len() {
            return Err(format!("index_ids stream exceeds data bounds").into());
        }
        let index_ids_data = &streams_data[cursor..cursor + index_ids_size as usize];
        cursor += index_ids_size as usize;
        let index_ids = self.decompress_sparse_u8_stream(index_ids_data, "index_ids")?;
        
        Ok(TokenizedStreams {
            instrument_ids,
            run_ids,
            flowcell_ids,
            lanes,
            tiles,
            x_coords,
            y_coords,
            umi_ids,
            read_nums,
            flags,
            index_ids,
        })
    }

    fn deflate_decompress(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        use flate2::read::ZlibDecoder;
        use std::io::Read;
        
        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();
        match decoder.read_to_end(&mut decompressed) {
            Ok(bytes_read) => {
                Ok(decompressed)
            }
            Err(e) => {
                Err(e.into())
            }
        }
    }

    fn decompress_u8_stream(&self, compressed_data: &[u8], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        
        // Check if this looks like DEFLATE data
        if compressed_data.len() >= 2 {
            let header = u16::from_be_bytes([compressed_data[0], compressed_data[1]]);
            
            // DEFLATE (zlib) headers: 0x789c (default), 0x78da (best compression), etc.
            if (compressed_data[0] & 0x0F) == 0x08 && (header % 31) == 0 {
                match self.deflate_decompress(compressed_data) {
                    Ok(decompressed) => {
                        // Continue with further decoding (RLE, Huffman)
                        return self.reverse_categorical_encoding(&decompressed, stream_name);
                    }
                    Err(e) => {
                        println!("    ✗ DEFLATE failed despite good header: {}", e);
                    }
                }
            } else {
                println!("    This doesn't look like zlib data, skipping DEFLATE");
            }
        }
        
        // Not DEFLATE data, try direct decoding
        self.reverse_categorical_encoding(compressed_data, stream_name)
    }

    fn reverse_categorical_encoding(&self, data: &[u8], stream_name: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // The compression applies layers in this order:
        // 1. RLE (if beneficial)
        // 2. Huffman (if beneficial)
        // 3. DEFLATE (if enabled)
        //
        // So decompression should reverse in opposite order:
        // 1. DEFLATE (already handled)
        // 2. Huffman
        // 3. RLE
        
        // For now, since Huffman is just a passthrough, try RLE first
        match self.try_decode_rle(data) {
            Ok(rle_decoded) => {
                Ok(rle_decoded)
            }
            Err(e) => {
                // If RLE fails, maybe it wasn't RLE encoded - return raw data
                Ok(data.to_vec())
            }
        }
    }
    
    fn try_decode_huffman(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // For now, since Huffman encoding is simplified (just returns input),
        // return the data as-is
        // TODO: Implement proper Huffman decoding when we implement proper Huffman encoding
        Ok(data.to_vec())
    }
    
    fn try_decode_rle(&self, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut decoded = Vec::new();
        let mut cursor = 0;
        
        while cursor < data.len() {
            // Save cursor position for error reporting
            let start_cursor = cursor;
            
            // Try to decode varint count
            let count = match self.decode_varint(data, &mut cursor) {
                Ok(c) => c,
                Err(e) => {
                    return Err(format!("RLE varint decode failed at {}: {}", start_cursor, e).into());
                }
            };
            
            if cursor >= data.len() {
                return Err("RLE: Incomplete data, missing value byte".into());
            }
            
            let value = data[cursor];
            cursor += 1;
            
            // Sanity check
            if count < 0 {
                // Handle ZigZag decoding if needed
                return Err(format!("RLE: Negative count: {}", count).into());
            }
            
            if count > 1_000_000 {
                return Err(format!("RLE: Count too large: {}", count).into());
            }
            
            // Extend with the repeated value
            let count_u = count as usize;
            for _ in 0..count_u {
                decoded.push(value);
            }
            
            // Safety check
            if decoded.len() > 10_000_000 {
                return Err("RLE decode result too large".into());
            }
        }
        Ok(decoded)
    }

    fn decompress_u16_stream(&self, data: &[u8], _stream_name: &str) -> Result<Vec<u16>, Box<dyn std::error::Error>> {
        let bytes = self.deflate_decompress(data)?;
        if bytes.len() % 2 != 0 {
            return Err("Invalid u16 stream: odd number of bytes".into());
        }
        
        Ok(bytes.chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect())
    }

    fn decompress_u32_stream(&self, compressed_data: &[u8], stream_name: &str) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        // First decompress with DEFLATE
        let deflate_decompressed = self.deflate_decompress(compressed_data)?;
        
        // Then reverse varint/delta encoding
        let values = self.decode_u32_varint_stream(&deflate_decompressed, stream_name == "run_ids")?;
        
        Ok(values)
    }

    fn decompress_coordinate_streams(&self, compressed_data: &[u8]) -> Result<(Vec<u32>, Vec<u32>, Vec<u16>), Box<dyn std::error::Error>> {
        // First decompress with DEFLATE
        let deflate_decompressed = self.deflate_decompress(compressed_data)?;
        
        // Then decode 2D delta encoding + varints
        let (x_coords, y_coords, tiles) = self.decode_2d_coordinate_deltas(&deflate_decompressed)?;
        
        Ok((x_coords, y_coords, tiles))
    }

    fn decompress_sparse_u16_stream(&self, compressed_data: &[u8], stream_name: &str) -> Result<Vec<Option<u16>>, Box<dyn std::error::Error>> {
        
        // Sparse streams have bitmap + values format
        let (bitmap, values) = self.decode_sparse_u16_data(compressed_data)?;
        
        Ok(self.reconstruct_sparse_u16_stream(&bitmap, &values))
    }

    fn decompress_sparse_u8_stream(&self, compressed_data: &[u8], stream_name: &str) -> Result<Vec<Option<u8>>, Box<dyn std::error::Error>> {
        
        // Sparse streams have bitmap + values format  
        let (bitmap, values) = self.decode_sparse_u8_data(compressed_data)?;
        
        Ok(self.reconstruct_sparse_u8_stream(&bitmap, &values))
    }

    fn decompress_optional_u16_stream(&self, data: &[u8], stream_name: &str) -> Result<Vec<Option<u16>>, Box<dyn std::error::Error>> {
        let values = self.decompress_u16_stream(data, stream_name)?;
        Ok(values.into_iter()
            .map(|v| if v == 0xFFFF { None } else { Some(v) })
            .collect())
    }

    fn decompress_optional_u8_stream(&self, data: &[u8], stream_name: &str) -> Result<Vec<Option<u8>>, Box<dyn std::error::Error>> {
        let values = self.decompress_u8_stream(data, stream_name)?;
        Ok(values.into_iter()
            .map(|v| if v == 0xFF { None } else { Some(v) })
            .collect())
    }

    fn decode_u32_varint_stream(&self, data: &[u8], use_delta: bool) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
        let mut values = Vec::new();
        let mut cursor = 0;
        
        if use_delta {
            // First value is absolute
            if cursor < data.len() {
                let first = self.decode_varint(data, &mut cursor)?;
                values.push(first as u32);
                
                // Remaining values are deltas
                while cursor < data.len() {
                    let delta = self.decode_varint(data, &mut cursor)?;
                    let prev = *values.last().unwrap();
                    values.push((prev as i32 + delta) as u32);
                }
            }
        } else {
            // All values are absolute
            while cursor < data.len() {
                let value = self.decode_varint(data, &mut cursor)?;
                values.push(value as u32);
            }
        }
        
        Ok(values)
    }

    fn decode_2d_coordinate_deltas(&self, data: &[u8]) -> Result<(Vec<u32>, Vec<u32>, Vec<u16>), Box<dyn std::error::Error>> {
        let mut x_coords = Vec::new();
        let mut y_coords = Vec::new();
        let mut tiles = Vec::new();
        
        let mut cursor = 0;
        let mut last_x = 0i32;
        let mut last_y = 0i32;
        let mut last_tile = 0i32;
        
        while cursor < data.len() {
            // Read delta triplet: dx, dy, dtile
            let dx = self.decode_varint(data, &mut cursor)?;
            let dy = self.decode_varint(data, &mut cursor)?;
            let dtile = self.decode_varint(data, &mut cursor)?;
            
            // Apply deltas
            last_x += dx;
            last_y += dy;
            last_tile += dtile;
            
            x_coords.push(last_x as u32);
            y_coords.push(last_y as u32);
            tiles.push(last_tile as u16);
        }
        
        Ok((x_coords, y_coords, tiles))
    }

    fn decode_sparse_u16_data(&self, data: &[u8]) -> Result<(Vec<u8>, Vec<u16>), Box<dyn std::error::Error>> {
        let mut cursor = 0;
        
        // Read bitmap size and decompress bitmap
        let bitmap_size = self.decode_varint(data, &mut cursor)? as usize;
        let bitmap_data = &data[cursor..cursor + bitmap_size];
        cursor += bitmap_size;
        let bitmap = self.deflate_decompress(bitmap_data)?;
        
        // Read values size and decompress values
        let values_size = self.decode_varint(data, &mut cursor)? as usize;
        let values_data = &data[cursor..cursor + values_size];
        let values_bytes = self.deflate_decompress(values_data)?;
        
        // Decode varint values
        let mut values = Vec::new();
        let mut val_cursor = 0;
        while val_cursor < values_bytes.len() {
            let val = self.decode_varint(&values_bytes, &mut val_cursor)?;
            values.push(val as u16);
        }
        
        Ok((bitmap, values))
    }

    fn decode_sparse_u8_data(&self, data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error>> {
        let mut cursor = 0;
        
        // Read bitmap size and decompress bitmap
        let bitmap_size = self.decode_varint(data, &mut cursor)? as usize;
        let bitmap_data = &data[cursor..cursor + bitmap_size];
        cursor += bitmap_size;
        let bitmap = self.deflate_decompress(bitmap_data)?;
        
        // Read values size and decompress values
        let values_size = self.decode_varint(data, &mut cursor)? as usize;
        let values_data = &data[cursor..cursor + values_size];
        let values_bytes = self.deflate_decompress(values_data)?;
        
        // Decode varint values as u8
        let mut values = Vec::new();
        let mut val_cursor = 0;
        while val_cursor < values_bytes.len() {
            let val = self.decode_varint(&values_bytes, &mut val_cursor)?;
            values.push(val as u8);
        }
        
        Ok((bitmap, values))
    }

    fn reconstruct_sparse_u16_stream(&self, bitmap: &[u8], values: &[u16]) -> Vec<Option<u16>> {
        let mut result = Vec::new();
        let mut value_idx = 0;
        
        for &byte in bitmap {
            for bit in 0..8 {
                if (byte >> bit) & 1 == 1 {
                    // Bit is set, use next value
                    if value_idx < values.len() {
                        result.push(Some(values[value_idx]));
                        value_idx += 1;
                    } else {
                        result.push(None);
                    }
                } else {
                    // Bit is not set, use None
                    result.push(None);
                }
            }
        }
        
        result
    }

    fn reconstruct_sparse_u8_stream(&self, bitmap: &[u8], values: &[u8]) -> Vec<Option<u8>> {
        let mut result = Vec::new();
        let mut value_idx = 0;
        
        for &byte in bitmap {
            for bit in 0..8 {
                if (byte >> bit) & 1 == 1 {
                    // Bit is set, use next value
                    if value_idx < values.len() {
                        result.push(Some(values[value_idx]));
                        value_idx += 1;
                    } else {
                        result.push(None);
                    }
                } else {
                    // Bit is not set, use None
                    result.push(None);
                }
            }
        }
        
        result
    }

    

    fn decompress_bytes(&self, compressed: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Simple deflate decompression for now
        // You can enhance this to detect compression type and use appropriate decompression
        use flate2::read::DeflateDecoder;
        use std::io::Read;
        
        let mut decoder = DeflateDecoder::new(compressed);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }
    
    fn reconstruct_tokens(&self, streams: TokenizedStreams) -> Vec<TokenizedReadName> {
        let count = streams.instrument_ids.len();
        let mut tokens = Vec::with_capacity(count);
        
        for i in 0..count {
            tokens.push(TokenizedReadName {
                instrument_id: streams.instrument_ids[i],
                run_id: streams.run_ids[i],
                flowcell_id: streams.flowcell_ids[i],
                lane: streams.lanes[i],
                tile: streams.tiles[i],
                x_coord: streams.x_coords[i],
                y_coord: streams.y_coords[i],
                umi_id: streams.umi_ids[i],
                read_num: streams.read_nums[i],
                flags: streams.flags[i],
                index_id: streams.index_ids[i],
            });
        }
        
        tokens
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
