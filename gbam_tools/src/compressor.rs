use super::Codecs;
use super::meta::{FileMeta, TokenizationMethod};
use crate::writer::BlockInfo;
use crate::SIZE_LIMIT;
use flume::{Receiver, Sender};
use rayon::ThreadPool;

use flate2::write::GzEncoder;
use flate2::Compression;
use brotli::CompressorWriter;
use zstd::stream::encode_all;
// use lz4::EncoderBuilder;
use std::io::Write;
use std::mem;

use std::sync::{Mutex, OnceLock};
use std::fmt::Write as FmtWrite; // For formatting into String
use bam_tools::record::fields::Fields;
use std::fs::File;

// use lz4_flex::block::{compress_into, get_maximum_output_size};
use lzzzz::lz4;
use xz2::write::XzEncoder;

use crate::tokenizer_encoding::{
    IlluminaTokenizer, ReadNameAnalyzer, ReadNamePattern,
    ByteUtils, PostTokenizationCompressor, PostTokenizationConfig, TokenizedReadName
};
use chrono::Local;


pub(crate) enum OrderingKey {
    Key(u64),
    UnusedBlock,
}

/// Accompanies compressed buffer to generate meta when written out
pub(crate) struct CompressTask {
    pub ordering_key: OrderingKey,
    pub block_info: BlockInfo,
    pub buf: Vec<u8>,
    pub dictionary_info: Option<DictionaryInfo>, 
}

#[derive(Debug)]
pub struct DictionaryInfo {
    pub action: DictionaryAction,
    pub tokenization_method: TokenizationMethod,
}

#[derive(Debug)]
pub enum DictionaryAction {
    CreateNew(Vec<u8>), // Serialized dictionary data
    UseExisting(u32),   // Dictionary ID
    None,               // No tokenization
}
pub(crate) struct Compressor {
    compr_pool: ThreadPool,
    compr_data_tx: Sender<CompressTask>,
    compr_data_rx: Receiver<CompressTask>,
    /// Buffers shared among threads
    buf_tx: Sender<Vec<u8>>,
    buf_rx: Receiver<Vec<u8>>,
    // Total number of decompression queryies
    sent: usize,
    // Processed blocks number
    received: usize,
}

impl Compressor {
    pub fn new(thread_num: usize) -> Self {
        let (compr_data_tx, compr_data_rx) = flume::unbounded();
        let (buf_tx, buf_rx) = flume::unbounded();
        for _ in 0..thread_num {
            buf_tx.send(vec![0; SIZE_LIMIT]).unwrap();
            compr_data_tx
                .send(CompressTask {
                    ordering_key: OrderingKey::UnusedBlock,
                    block_info: BlockInfo::default(),
                    buf: vec![0; SIZE_LIMIT],
                    dictionary_info: None,
                })
                .unwrap();
        }
        Compressor {
            compr_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(thread_num)
                .build()
                .unwrap(),
            compr_data_tx,
            compr_data_rx,
            buf_tx,
            buf_rx,
            sent: 0,
            received: 0,
        }
    }

    fn prepare_readname_dictionary(
        &self, 
        data: &[u8], 
        uncompr_size: usize,
        file_meta: &FileMeta
    ) -> Option<DictionaryInfo> {
        // Extract read names
        let read_name_refs = self.extract_read_names(data, uncompr_size);
        
        if read_name_refs.is_empty() || !ReadNameAnalyzer::should_tokenize(&read_name_refs) {
            return None;
        }
        
        // Check for existing compatible dictionary
        // if let Some(existing_id) = file_meta.find_compatible_dictionary() {
        //     return Some(DictionaryInfo {
        //         action: DictionaryAction::UseExisting(existing_id),
        //         tokenization_method: TokenizationMethod::IlluminaModern, // Or detect from dict
        //     });
        // }
        
        // Need to create new dictionary
        let mut tokenizer = IlluminaTokenizer::new();
        if let Ok(tokenized) = tokenizer.tokenize_batch(&read_name_refs) {
            let dict_data = tokenizer.serialize_dictionary();
            let method = Self::determine_tokenization_method(&tokenized);
            
            Some(DictionaryInfo {
                action: DictionaryAction::CreateNew(dict_data),
                tokenization_method: method,
            })
        } else {
            None
        }
    }
    
    fn extract_read_names<'a>(&self, data: &'a [u8], uncompr_size: usize) -> Vec<&'a [u8]> {
        let mut read_name_refs = Vec::new();
        let mut offset = 0;
        
        while offset < uncompr_size {
            if let Some(null_pos) = data[offset..uncompr_size].iter().position(|&b| b == 0) {
                let read_name = &data[offset..offset + null_pos];
                if !read_name.is_empty() {
                    read_name_refs.push(read_name);
                }
                offset += null_pos + 1;
            } else {
                if offset < uncompr_size {
                    read_name_refs.push(&data[offset..uncompr_size]);
                }
                break;
            }
        }
        
        read_name_refs
    }
    
    fn determine_tokenization_method(tokenized: &[TokenizedReadName]) -> TokenizationMethod {
        if tokenized.is_empty() {
            TokenizationMethod::None
        } else if tokenized[0].flowcell_id == 0 {
            TokenizationMethod::IlluminaLegacy
        } else {
            TokenizationMethod::IlluminaModern
        }
    }

    fn compress_readname_block(data: &[u8], buf: &mut Vec<u8>, block_info: &BlockInfo) -> Vec<u8> {
        // Extract read names
        let mut read_name_refs = Vec::new();
        let mut offset = 0;
        
        while offset < block_info.uncompr_size {
            if let Some(null_pos) = data[offset..block_info.uncompr_size].iter().position(|&b| b == 0) {
                let read_name = &data[offset..offset + null_pos];
                if !read_name.is_empty() {
                    read_name_refs.push(read_name);
                }
                offset += null_pos + 1;
            } else {
                if offset < block_info.uncompr_size {
                    read_name_refs.push(&data[offset..block_info.uncompr_size]);
                }
                break;
            }
        }
        
        if read_name_refs.is_empty() {
            return compress(&data[..block_info.uncompr_size], mem::take(buf), block_info.codec)
        }
        
        // Tokenize read names
        let mut tokenizer = IlluminaTokenizer::new();
        match tokenizer.tokenize_batch(&read_name_refs) {
            Ok(tokenized) => {
                
                // Apply post-tokenization compression
                let post_compressor = PostTokenizationCompressor::new(
                    PostTokenizationConfig::default()
                );
                
                match post_compressor.compress_tokenized_data(&tokenized, tokenizer.get_dictionary()) {
                    Ok(compressed_data) => {
                        let original_total_size: usize = read_name_refs.iter().map(|name| name.len()).sum();
                        
                        compressed_data
                    }
                    Err(e) => {
                        eprintln!("Post-tokenization compression failed: {}, falling back to basic tokenization", e);
                        Self::fallback_tokenization(&tokenized, buf, block_info.codec)
                    }
                }
            }
            Err(e) => {
                eprintln!("Tokenization failed: {}, using standard compression", e);
                return compress(&data[..block_info.uncompr_size], mem::take(buf), block_info.codec)
            }
        }
    }
    
    fn fallback_tokenization(tokenized: &[TokenizedReadName], buf: &mut Vec<u8>, codec: Codecs) -> Vec<u8> {
        let mut encoded_data = Vec::new();
        
        // Store only tokens (dictionary is in FileMeta)
        encoded_data.extend_from_slice(&(tokenized.len() as u32).to_le_bytes());
        
        for token in tokenized {
            encoded_data.push(token.instrument_id);
            encoded_data.extend_from_slice(&token.run_id.to_le_bytes());
            encoded_data.push(token.flowcell_id);
            encoded_data.push(token.lane);
            encoded_data.extend_from_slice(&token.tile.to_le_bytes());
            encoded_data.extend_from_slice(&token.x_coord.to_le_bytes());
            encoded_data.extend_from_slice(&token.y_coord.to_le_bytes());
            
            if let Some(umi_id) = token.umi_id {
                encoded_data.extend_from_slice(&umi_id.to_le_bytes());
            } else {
                encoded_data.extend_from_slice(&0xFFFFu16.to_le_bytes());
            }
            
            encoded_data.push(token.read_num);
            encoded_data.push(token.flags);
            
            if let Some(index_id) = token.index_id {
                encoded_data.push(index_id);
            } else {
                encoded_data.push(0xFF);
            }
        }
        
        compress(&encoded_data, std::mem::take(buf), codec)
    }
    
    pub fn compress_block(
        &mut self,
        ordering_key: OrderingKey,
        mut block_info: BlockInfo,
        data: Vec<u8>,
        codec: Codecs,
        file_meta: &FileMeta,
    ) {
        let dictionary_info = if block_info.field == Fields::ReadName {
            self.prepare_readname_dictionary(&data, block_info.uncompr_size, file_meta)
        } else {
            None
        };
        
        // Set block info based on dictionary decision
        match &dictionary_info {
            Some(dict_info) => {
                match &dict_info.action {
                    DictionaryAction::UseExisting(dict_id) => {
                        block_info.dictionary_id = Some(*dict_id);
                        block_info.is_tokenized = true;
                    }
                    DictionaryAction::CreateNew(_) => {
                        block_info.dictionary_id = Some(0); // Placeholder, will be updated
                        block_info.is_tokenized = true;
                    }
                    DictionaryAction::None => {
                        block_info.dictionary_id = None;
                        block_info.is_tokenized = false;
                    }
                }
            }
            None => {
                block_info.dictionary_id = None;
                block_info.is_tokenized = false;
            }
        }

        let buf_queue_tx = self.buf_tx.clone();
        let buf_queue_rx = self.buf_rx.clone();
        let compressed_tx = self.compr_data_tx.clone();
        self.sent += 1;
        self.compr_pool.install(|| {
            rayon::spawn(move || {
                let mut buf = buf_queue_rx.recv().unwrap();
                buf.clear();
                
                let compr_data = if block_info.field == Fields::ReadName && block_info.is_tokenized {
                    // Perform tokenization and compression
                    Self::compress_readname_block(&data, &mut buf, &block_info)
                } else {
                    // Standard compression
                    compress(&data[..block_info.uncompr_size], std::mem::take(&mut buf), block_info.codec)
                };
                
                // Send the compression result
                compressed_tx
                    .send(CompressTask {
                        ordering_key,
                        block_info,
                        buf: compr_data,
                        dictionary_info,
                    })
                    .unwrap();
                
                // **IMPORTANT**: Return the buffer to the pool for reuse
                buf_queue_tx.send(buf).unwrap();
            })
        });
    }

    /// Drain completed tasks
    pub fn get_compr_block(&mut self) -> CompressTask {
        let task = self.compr_data_rx.recv().unwrap();
        // Correct for first dummy blocks
        if let OrderingKey::Key(_) = task.ordering_key {
            self.received += 1;
        }
        task
    }

    /// Wait for all threads to finish and return leftovers
    pub fn finish(&mut self) -> Vec<CompressTask> {
        let mut leftovers = Vec::new();
        while self.received != self.sent {
            leftovers.push(self.get_compr_block());
        }
        leftovers
    }
}

pub fn compress(source: &[u8], mut dest: Vec<u8>, codec: Codecs) -> Vec<u8> {
    let compressed_bytes = match codec {
        Codecs::Gzip => {
            let mut encoder = GzEncoder::new(dest, Compression::new(9));
            encoder.write_all(source).unwrap();
            encoder.finish()
        }
        Codecs::Lz4 => {
            dest.clear();
            let res = lz4::compress_to_vec(source, &mut dest, lz4::ACC_LEVEL_DEFAULT);
            match res {
                Ok(size) => {
                    dest.resize(size, 0);
                    Ok(dest)
                }
                Err(_) => Err(std::io::Error::other(
                    "Compression error",
                )),
            }
        }
        Codecs::Brotli => {
            dest.clear();
            {
                let mut writer = CompressorWriter::new(&mut dest, 4096, 8, 22);
                writer.write_all(source).unwrap();
                writer.flush().unwrap();
            }
            Ok(dest)
        }
        Codecs::Xz => {
            let mut encoder = XzEncoder::new(Vec::new(), 6);
            encoder.write_all(source).unwrap();
            let compressed = encoder.finish().unwrap();
            Ok(compressed)
        }
        Codecs::Zstd => {
            // encode_all returns a Vec<u8>
            match encode_all(source, 14) {
                Ok(c) => Ok(c),
                Err(_) => Err(std::io::Error::other(
                    "Zstd compression error",
                )),
            }
        }
        Codecs::NoCompression => {
            dest.clear();
            dest.extend_from_slice(source);
            Ok(dest)
        }
    };
    compressed_bytes.unwrap()
}
