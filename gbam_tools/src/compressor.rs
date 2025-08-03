use super::Codecs;
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

use std::sync::{Mutex, OnceLock};
use std::fmt::Write as FmtWrite; // For formatting into String
use bam_tools::record::fields::Fields;
use std::fs::File;

// use lz4_flex::block::{compress_into, get_maximum_output_size};
use lzzzz::lz4;
use xz2::write::XzEncoder;

use crate::tokenizer_encoding::{IlluminaTokenizer, ReadNameAnalyzer, ReadNamePattern, ByteUtils, PostTokenizationCompressor, PostTokenizationConfig};


pub(crate) enum OrderingKey {
    Key(u64),
    UnusedBlock,
}

/// Accompanies compressed buffer to generate meta when written out
pub(crate) struct CompressTask {
    pub ordering_key: OrderingKey,
    pub block_info: BlockInfo,
    pub buf: Vec<u8>,
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

    pub fn compress_block(
        &mut self,
        ordering_key: OrderingKey,
        block_info: BlockInfo,
        data: Vec<u8>,
        codec: Codecs,
    ) {
        let buf_queue_tx = self.buf_tx.clone();
        let buf_queue_rx = self.buf_rx.clone();
        let compressed_tx = self.compr_data_tx.clone();
        self.sent += 1;
        self.compr_pool.install(|| {
            rayon::spawn(move || {
                let mut buf = buf_queue_rx.recv().unwrap();
                buf.clear();
                
                let compr_data = if block_info.field == Fields::ReadName {
                    // Extract read names from the block for tokenization testing
                    let mut read_name_refs = Vec::new();
                    let mut offset = 0;
                    
                    // Parse read names from the block (assuming null-terminated strings)
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
                
                    // Test tokenization if we have read names
                    if !read_name_refs.is_empty() {
                        if ReadNameAnalyzer::should_tokenize(&read_name_refs) {
                            println!("Tokenizing read names...");
                            
                            let mut tokenizer = IlluminaTokenizer::new();
                            match tokenizer.tokenize_batch(&read_name_refs) {
                                Ok(tokenized) => {
                                    println!("Tokenization successful! Applying post-tokenization compression...");
                                    
                                    // Apply post-tokenization compression pipeline
                                    let post_compressor = PostTokenizationCompressor::new(
                                        PostTokenizationConfig::default()
                                    );
                                    
                                    match post_compressor.compress_tokenized_data(&tokenized, tokenizer.get_dictionary()) {
                                        Ok(compressed_data) => {
                                            let original_total_size: usize = read_name_refs.iter().map(|name| name.len()).sum();
                                            
                                            println!("\n=== Post-Tokenization Compression Results ===");
                                            println!("Original block size: {} bytes", block_info.uncompr_size);
                                            println!("Original read names total: {} bytes", original_total_size);
                                            println!("Post-tokenization compressed size: {} bytes", compressed_data.len());
                                            println!("Final compression ratio: {:.2}x", original_total_size as f64 / compressed_data.len() as f64);
                                            println!("Space saved: {:.1}%", 
                                                     (1.0 - compressed_data.len() as f64 / original_total_size as f64) * 100.0);
                                            
                                            compressed_data
                                        }
                                        Err(e) => {
                                            eprintln!("Post-tokenization compression failed: {}, falling back", e);
                                            
                                            // Fallback to simple tokenization + basic compression
                                            let mut encoded_data = Vec::new();
                                            
                                            // Serialize dictionary
                                            let serialized_dict = tokenizer.serialize_dictionary();
                                            encoded_data.extend_from_slice(&(serialized_dict.len() as u32).to_le_bytes());
                                            encoded_data.extend_from_slice(&serialized_dict);
                                            
                                            // Serialize tokenized data
                                            encoded_data.extend_from_slice(&(tokenized.len() as u32).to_le_bytes());
                                            for token in &tokenized {
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
                                            
                                            // Apply basic compression
                                            compress(&encoded_data, buf, block_info.codec)
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Tokenization failed: {}, falling back to original compression", e);
                                    compress(&data[..block_info.uncompr_size], buf, block_info.codec)
                                }
                            }
                        } else {
                            println!("Tokenization not beneficial for this dataset");
                            compress(&data[..block_info.uncompr_size], buf, block_info.codec)
                        }
                    } else {
                        compress(&data[..block_info.uncompr_size], buf, block_info.codec)
                    }
                } else {
                    // Non-read-name fields, compress normally
                    compress(&data[..block_info.uncompr_size], buf, block_info.codec)
                };
                
                buf_queue_tx.send(data).unwrap();
    
                let field_name = format!("{:?}", block_info.field);
                let uncompressed_size = block_info.uncompr_size;
                let compressed_size = compr_data.len();
    
                let log_line = format!(
                    "Field: {}, Uncompressed: {}, Compressed: {}\n",
                    field_name, uncompressed_size, compressed_size
                );
    
                compressed_tx
                    .send(CompressTask {
                        ordering_key,
                        block_info,
                        buf: compr_data,
                    })
                    .unwrap();
            });
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
