use std::{collections::BTreeMap, io::Result, sync::Arc};

use super::reader::generate_block_treemap;
use super::record::GbamRecord;
use crate::SIZE_LIMIT;
use bam_tools::record::fields::Fields;
use byteorder::{LittleEndian, ReadBytesExt};
use flate2::write::GzDecoder;
use lzzzz::lz4;
use memmap2::Mmap;
use std::convert::TryFrom;
use std::io::{Read, Write};
use xz2::read::XzDecoder;

use crate::{meta::FileMeta, Codecs};
use crate::tokenizer_encoding::{IlluminaTokenizer, TokenizedReadName};
use crate::tokenizer_encoding::dictionary::ReadNameDictionary;
use crate::meta::{TokenizationMethod, SerializedDictionary};

// Contains fields needed both for fixed sized fields and variable sized fields.
pub struct Inner {
    /// Arc is needed since this struct should work with PyO3 which sends struct between threads (Send trait is required).
    meta: Arc<FileMeta>,
    range_begin: usize,
    range_end: usize,
    field: Fields,
    buffer: Vec<u8>,
    reader: Arc<Mmap>,
}

impl Inner {
    pub(crate) fn new(meta: Arc<FileMeta>, field: Fields, reader: Arc<Mmap>) -> Self {
        Inner {
            meta,
            range_begin: 0,
            range_end: 0,
            field,
            buffer: Vec::<u8>::with_capacity(SIZE_LIMIT * 2),
            reader,
        }
    }
}

/// Defines how columns will operate. It is needed since variable sized fields
/// columns also require parsing of additional fixed sized fields columns.
pub trait Column {
    // Fills GbamRecord field with data from corresponding BAM record.
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord);
}

/// GBAM file column. Responsible for fetching data.
pub struct FixedColumn(Inner, usize);

impl Column for FixedColumn {
    /// Fetches data into provider record buffer. If item is located outside of
    /// currently loaded data block, the new block will be loaded and
    /// decompressed.
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord) {
        rec.parse_from_bytes(&self.0.field.clone(), self.get_item(item_num));
    }
}

impl FixedColumn {
    pub fn new(inner: Inner, field_size: usize) -> Self {
        Self(inner, field_size)
    }
    fn get_item(&mut self, item_num: usize) -> &[u8] {
        if let Some(block_num) = self.find_block(item_num) {
            Self::update_buffer(&mut self.0, block_num);
        }
        let rec_num_in_block = item_num - self.0.range_begin;
        let item_size = self.1;
        let offset = rec_num_in_block * item_size;
        &self.0.buffer[offset..offset + item_size]
    }
    // Finds blocks where record is located. None is returned if block is already loaded.
    fn find_block(&self, item_num: usize) -> Option<usize> {
        if item_num >= self.0.range_begin && item_num < self.0.range_end {
            return None;
        }
        // All blocks sizes are equal except maybe the last one since it's a fixed sized column and block size limit is constant.
        let block_len = self.0.meta.view_blocks(&self.0.field)[0].numitems;
        Some(item_num / block_len as usize)
    }

    fn update_buffer(inner: &mut Inner, block_num: usize) {
        fetch_block(inner, block_num).unwrap();
        let block_len = inner.meta.view_blocks(&inner.field)[0].numitems as usize;
        let cur_block_len = inner.meta.view_blocks(&inner.field)[block_num].numitems as usize;
        inner.range_begin = block_num * block_len;
        inner.range_end = inner.range_begin + cur_block_len;
    }
}

/// Column managing access to variable sized data. Utilizes another column (for fixed sized fields) to index data.
pub struct VariableColumn {
    inner: Inner,
    index: FixedColumn,
    // Used to quickly determine what block record belongs to.
    blocks: BTreeMap<usize, usize>,
}

impl Column for VariableColumn {
    fn fill_record_field(&mut self, item_num: usize, rec: &mut GbamRecord) {
        rec.parse_from_bytes(&self.inner.field.clone(), self.get_item(item_num));
    }
}

impl VariableColumn {
    pub fn new(inner: Inner, index: FixedColumn) -> Self {
        Self {
            blocks: generate_block_treemap(&inner.meta, &inner.field),
            inner,
            index,
        }
    }

    fn get_item(&mut self, item_num: usize) -> &[u8] {
        if let Some((range_begin, block_num)) = self.find_block(item_num) {
            Self::update_buffer(&mut self.inner, block_num, range_begin);
        }
        let rec_num_in_block = item_num - self.inner.range_begin;
        let mut read_offset =
            |n| self.index.get_item(n).read_u32::<LittleEndian>().unwrap() as usize;
        let start = match rec_num_in_block {
            0 => 0,
            _ => read_offset(item_num - 1),
        };
        let end = read_offset(item_num);
        &self.inner.buffer[start..end]
    }

    // Finds blocks where record is located. None is returned if block is already loaded.
    fn find_block(&self, item_num: usize) -> Option<(usize, usize)> {
        if item_num >= self.inner.range_begin && item_num < self.inner.range_end {
            return None;
        }
        // To determine what block record N is in.
        Some(
            self.blocks
                // Inclusive range.
                .range(..=item_num)
                .next_back()
                .map_or((0, 0), |(&range_begin, &block_num)| {
                    (range_begin, block_num)
                }),
        )
    }

    fn update_buffer(inner: &mut Inner, block_num: usize, range_begin: usize) {
        fetch_block(inner, block_num).unwrap();
        let block_len = inner.meta.view_blocks(&inner.field)[block_num].numitems as usize;
        inner.range_begin = range_begin;
        inner.range_end = inner.range_begin + block_len;
    }
}

/// Fetch and decompress a data block.
fn fetch_block(inner_column: &mut Inner, block_num: usize) -> Result<()> {
    let field = &inner_column.field;
    let block_meta = inner_column.meta.view_blocks(field).get(block_num).unwrap();
    let reader = &inner_column.reader;
    let block_size = block_meta.block_size;
    let uncompressed_size = block_meta.uncompressed_size;

    let data = &reader[usize::try_from(block_meta.seekpos).unwrap()
        ..usize::try_from(block_meta.seekpos + block_size as u64).unwrap()];
    
    inner_column.buffer.resize(uncompressed_size as usize, 0);
    let codec = inner_column.meta.get_field_codec(field);

    if uncompressed_size > 0 {
        // Check if this is a tokenized ReadName block
        if field == &Fields::ReadName && block_meta.is_tokenized {
            
            match decode_tokenized_readname_block(
                data, 
                &mut inner_column.buffer, 
                block_meta, 
                &inner_column.meta, 
                codec,
                block_num
            ) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Tokenized decoding failed for block {}: {}", block_num, e);
                    eprintln!("This likely indicates a metadata inconsistency - block marked as tokenized but contains standard data");
                    return Err(e);
                }
            }
        } else {
            // Standard decompression
            decompress_block(data, &mut inner_column.buffer, codec)
                .expect("Decompression failed.");
        }
    }

    Ok(())
}

pub fn deserialize_dictionary(data: &[u8]) -> std::result::Result<ReadNameDictionary, Box<dyn std::error::Error>> {
    let mut cursor = std::io::Cursor::new(data);
    let mut dictionary = ReadNameDictionary::new();
    
    // Read instruments
    let instruments_count = cursor.read_u32::<LittleEndian>()? as usize;
    for _ in 0..instruments_count {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut instrument = vec![0u8; len];
        cursor.read_exact(&mut instrument)?;
        dictionary.add_instrument(&instrument); // Add & to pass slice
    }
    
    // Read flowcells
    let flowcells_count = cursor.read_u32::<LittleEndian>()? as usize;
    for _ in 0..flowcells_count {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut flowcell = vec![0u8; len];
        cursor.read_exact(&mut flowcell)?;
        dictionary.add_flowcell(&flowcell); // Add & to pass slice
    }
    
    // Read UMIs
    let umis_count = cursor.read_u32::<LittleEndian>()? as usize;
    for _ in 0..umis_count {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut umi = vec![0u8; len];
        cursor.read_exact(&mut umi)?;
        dictionary.add_umi(&umi); // Add & to pass slice
    }
    
    // Read indices
    let indices_count = cursor.read_u32::<LittleEndian>()? as usize;
    for _ in 0..indices_count {
        let len = cursor.read_u32::<LittleEndian>()? as usize;
        let mut index = vec![0u8; len];
        cursor.read_exact(&mut index)?;
        dictionary.add_index(&index); // Add & to pass slice
    }
    
    Ok(dictionary)
}

pub fn detokenize(
    token: &TokenizedReadName, 
    dictionary: &ReadNameDictionary
) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Get instrument name
    let instrument = dictionary.get_instrument(token.instrument_id)
        .ok_or("Invalid instrument ID")?;

    // LEGACY FORMAT: flowcell_id == 0
    if token.flowcell_id == 0 {
        let mut read_name = format!(
            "{}:{}:{}:{}:{}",
            String::from_utf8_lossy(instrument),
            token.lane,
            token.tile,
            token.x_coord,
            token.y_coord
        );

        // Add index if present
        if let Some(index_id) = token.index_id {
            if let Some(index) = dictionary.get_index(index_id) {
                read_name.push_str(&format!("#{}", String::from_utf8_lossy(index)));
            }
        }

        // Add UMI if present
        if let Some(umi_id) = token.umi_id {
            if let Some(umi) = dictionary.get_umi(umi_id) {
                read_name.push_str(&format!("|{}", String::from_utf8_lossy(umi)));
            }
        }

        Ok(read_name.into_bytes())
    } else {
        // MODERN FORMAT (unchanged)
        let flowcell = dictionary.get_flowcell(token.flowcell_id - 1)
            .ok_or("Invalid flowcell ID")?;
        let mut read_name = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            String::from_utf8_lossy(instrument),
            token.run_id,
            String::from_utf8_lossy(flowcell),
            token.lane,
            token.tile,
            token.x_coord,
            token.y_coord
        );

        // Add UMI if present
        if let Some(umi_id) = token.umi_id {
            if let Some(umi) = dictionary.get_umi(umi_id) {
                read_name.push_str(&format!(":{}", String::from_utf8_lossy(umi)));
            }
        }

        // Add read number
        read_name.push_str(&format!(" {}", token.read_num));

        // Add flags
        if token.flags > 0 {
            read_name.push_str(&format!(" {}", token.flags));
        }

        // Add index if present
        if let Some(index_id) = token.index_id {
            if let Some(index) = dictionary.get_index(index_id) {
                read_name.push_str(&format!(" {}", String::from_utf8_lossy(index)));
            }
        }

        Ok(read_name.into_bytes())
    }
}

pub fn parse_tokens(data: &[u8]) -> std::result::Result<Vec<TokenizedReadName>, Box<dyn std::error::Error>> {
    let mut cursor = std::io::Cursor::new(data);
    
    // Read token count
    let token_count = cursor.read_u32::<LittleEndian>()? as usize;
    let mut tokens = Vec::with_capacity(token_count);
    
    for _ in 0..token_count {
        let instrument_id = cursor.read_u8()?;
        let run_id = cursor.read_u32::<LittleEndian>()?;
        let flowcell_id = cursor.read_u8()?;
        let lane = cursor.read_u8()?;
        let tile = cursor.read_u16::<LittleEndian>()?;
        let x_coord = cursor.read_u32::<LittleEndian>()?;
        let y_coord = cursor.read_u32::<LittleEndian>()?;
        
        let umi_raw = cursor.read_u16::<LittleEndian>()?;
        let umi_id = if umi_raw == 0xFFFF { None } else { Some(umi_raw) };
        
        let read_num = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        
        let index_raw = cursor.read_u8()?;
        let index_id = if index_raw == 0xFF { None } else { Some(index_raw) };
        
        tokens.push(TokenizedReadName {
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
        });
    }
    
    Ok(tokens)
}

fn convert_tokens_to_buffer(
    tokens: &[TokenizedReadName],
    dictionary: &ReadNameDictionary,
    dest_buffer: &mut Vec<u8>
) -> std::io::Result<()> {
    dest_buffer.clear();
    
    for token in tokens {
        let read_name = detokenize(token, dictionary)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        
        // Write read name as null-terminated string (matching original BAM format)
        dest_buffer.extend_from_slice(&read_name);
        dest_buffer.push(0); // Null terminator
    }
    
    Ok(())
}

fn decode_standard_tokenized_readnames(
    compressed_data: &[u8],
    dest_buffer: &mut Vec<u8>,
    dictionary: &ReadNameDictionary,
    codec: &Codecs,
) -> std::io::Result<()> {
    println!("Starting standard tokenized decoding (fallback format), compressed data size: {}", compressed_data.len());
    
    // First decompress using standard codec (this should work now)
    let mut decompressed_data = Vec::new();
    match decompress_block(compressed_data, &mut decompressed_data, codec) {
        Ok(_) => println!("Standard decompression successful, decompressed size: {}", decompressed_data.len()),
        Err(e) => {
            eprintln!("Standard decompression failed: {}", e);
            return Err(e);
        }
    }
    
    // Parse tokens from the decompressed data using fallback format
    let tokens = match parse_fallback_tokens(&decompressed_data) {
        Ok(tokens) => {
            println!("Successfully parsed {} tokens from fallback format", tokens.len());
            tokens
        }
        Err(e) => {
            eprintln!("Fallback token parsing failed: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
        }
    };
    
    // Convert tokens back to read names and write to buffer
    convert_tokens_to_buffer(&tokens, dictionary, dest_buffer)
}

// New function to parse the exact format used by fallback_tokenization
pub fn parse_fallback_tokens(data: &[u8]) -> std::result::Result<Vec<TokenizedReadName>, Box<dyn std::error::Error>> {
    let mut cursor = std::io::Cursor::new(data);
    
    // Read token count
    let token_count = cursor.read_u32::<LittleEndian>()? as usize;
    println!("Parsing {} tokens from fallback format", token_count);
    let mut tokens = Vec::with_capacity(token_count);
    
    for i in 0..token_count {
        // Parse exactly as written by fallback_tokenization
        let instrument_id = cursor.read_u8()?;
        let run_id = cursor.read_u32::<LittleEndian>()?;
        let flowcell_id = cursor.read_u8()?;
        let lane = cursor.read_u8()?;
        let tile = cursor.read_u16::<LittleEndian>()?;
        let x_coord = cursor.read_u32::<LittleEndian>()?;
        let y_coord = cursor.read_u32::<LittleEndian>()?;
        
        let umi_raw = cursor.read_u16::<LittleEndian>()?;
        let umi_id = if umi_raw == 0xFFFF { None } else { Some(umi_raw) };
        
        let read_num = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        
        let index_raw = cursor.read_u8()?;
        let index_id = if index_raw == 0xFF { None } else { Some(index_raw) };
        
        tokens.push(TokenizedReadName {
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
        });
        
        if i < 5 {
            println!("Token {}: instrument={}, run={}, flowcell={}, lane={}, tile={}, x={}, y={}", 
                i, instrument_id, run_id, flowcell_id, lane, tile, x_coord, y_coord);
        }
    }
    
    Ok(tokens)
}

fn decode_post_tokenization_compressed_readnames(
    compressed_data: &[u8],
    dest_buffer: &mut Vec<u8>,
    dictionary: &ReadNameDictionary,
    _codec: &Codecs,
    block_id: usize
) -> std::io::Result<()> {
    
    use crate::tokenizer_encoding::post_compression::{PostTokenizationCompressor, PostTokenizationConfig};
    
    // Decompress using PostTokenizationCompressor
    let post_compressor = PostTokenizationCompressor::new(PostTokenizationConfig::default());
    
    let tokens = match post_compressor.decompress_tokenized_data(compressed_data, dictionary) {
        Ok(tokens) => {
            tokens
        }
        Err(e) => {
            eprintln!("Post-tokenization decompression failed: {}", e);
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
        }
    };
    
    // Convert tokens back to read names and write to buffer
    match convert_tokens_to_buffer(&tokens, dictionary, dest_buffer) {
        Ok(_) => {
            Ok(())
        }
        Err(e) => {
            eprintln!("Post-tokenized token to buffer conversion failed: {}", e);
            Err(e)
        }
    }
}

pub fn decode_tokenized_readname_block(
    compressed_data: &[u8],
    dest_buffer: &mut Vec<u8>,
    block_meta: &crate::meta::BlockMeta,
    file_meta: &FileMeta,
    codec: &Codecs,
    block_id: usize
) -> std::io::Result<()> {
    
    if let Some(dict_id) = block_meta.dictionary_id {
        
        let dict_entry = file_meta.get_dictionary(dict_id).unwrap();
        let dictionary = deserialize_dictionary(&dict_entry.dictionary_data).unwrap();
        
        // Based on compression logs, ALL blocks use post-tokenization compression
        // The compression never fell back to fallback_tokenization
        decode_post_tokenization_compressed_readnames(
            compressed_data, 
            dest_buffer, 
            &dictionary,
            codec,
            block_id
        )
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Block marked as tokenized but no dictionary ID found"
        ))
    }
}

fn decode_fallback_tokenized_readnames(
    compressed_data: &[u8],
    dest_buffer: &mut Vec<u8>,
    dictionary: &ReadNameDictionary,
    codec: &Codecs,
) -> std::io::Result<()> {
    println!("Decoding fallback tokenized format, size: {} bytes", compressed_data.len());
    println!("Using codec: {:?}", codec);
    
    // Step 1: Decompress using standard codec (Brotli)
    let mut decompressed_data = Vec::new();
    println!("Attempting decompression with {:?}...", codec);
    
    match decompress_block(compressed_data, &mut decompressed_data, codec) {
        Ok(_) => {
            println!("Decompression successful: {} bytes", decompressed_data.len());
            
            // Debug the decompressed data
            if decompressed_data.len() >= 8 {
                println!("First 16 bytes of decompressed data: {:02x?}", &decompressed_data[..std::cmp::min(16, decompressed_data.len())]);
                
                let token_count = u32::from_le_bytes([
                    decompressed_data[0], decompressed_data[1], decompressed_data[2], decompressed_data[3]
                ]);
                println!("Token count from decompressed data: {}", token_count);
            }
        }
        Err(e) => {
            println!("Decompression failed: {}", e);
            return Err(e);
        }
    }
    
    // Step 2: Parse tokens from binary format
    let tokens = parse_fallback_tokens(&decompressed_data)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    println!("Parsed {} tokens", tokens.len());
    
    // Step 3: Convert tokens back to ReadName strings
    convert_tokens_to_buffer(&tokens, dictionary, dest_buffer)?;
    println!("Converted to {} ReadName bytes", dest_buffer.len());
    
    Ok(())
}

pub fn decompress_block(source: &[u8], dest: &mut Vec<u8>, codec: &Codecs) -> std::io::Result<()> {
    use std::io::Write;
    match codec {
        Codecs::Gzip => {
            let mut decoder = GzDecoder::new(dest);
            decoder.write_all(source).unwrap();
            decoder.try_finish().unwrap();
        }
        Codecs::Lz4 => {
            lz4::decompress(source, dest).unwrap();
        }
        Codecs::Brotli => {
            dest.clear();
            let mut decompressor = brotli::Decompressor::new(source, 4096);
            decompressor.read_to_end(dest)?;
        }
        Codecs::Zstd => {
            dest.clear();
            let mut decoder = zstd::stream::Decoder::new(source)?;
            decoder.read_to_end(dest)?;
        }
        Codecs::Xz => {
            let mut decoder = XzDecoder::new(source);
            decoder.read_to_end(dest)?;
        }
        Codecs::NoCompression => {
            dest.clear();
            dest.extend_from_slice(source);
        }
    };
    Ok(())
}
