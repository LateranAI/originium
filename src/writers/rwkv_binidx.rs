use crate::writers::Writer; // Ensure Writer trait is in scope
use bytemuck;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize; // Added Serialize import
use std::fs::{self, File}; // Removed OpenOptions
use std::io::{BufWriter, Write, BufReader}; // Removed Seek, SeekFrom as pipeline handles full flow
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use tokio::sync::mpsc::Receiver as TokioReceiver; // Renamed for clarity if multiple Receiver types are in scope
use tokio::task::JoinHandle;
use std::fmt::Debug;
use async_trait::async_trait;
use std::sync::mpsc::{self, Sender as StdSender, Receiver as StdReceiver}; // Restored std::sync::mpsc
use std::marker::PhantomData;
use futures::future::join_all; // Added for join_all
use tokio::sync::mpsc::{channel, Sender as TokioSender};

// Constants from jsonl2binidx, verify their meaning for RWKV format if possible
const MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00"; // "Magic Multi-Index IDX" ?
const VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0]; // Version 1.0
const ACTUAL_DTYPE_BYTE: [u8; 1] = [8u8]; // Explicitly add/ensure this constant for DTYPE
const TOKEN_SIZE_BYTES: usize = 2; // Assuming u16 tokens

#[derive(Debug, Clone, Serialize)]
pub struct BinidxItem {
    pub tokens: Vec<u16>,
}

// Implement Display for BinidxItem
impl std::fmt::Display for BinidxItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Provide a summary representation, as displaying all tokens might be too verbose.
        write!(f, "BinidxItem(tokens: [{}])", self.tokens.len())
    }
}

// WorkerPayload now internal to pipeline's worker setup
struct WorkerPayloadInternal {
    item: Option<BinidxItem>, // Option to signal end-of-stream
    // thread_id is implicit by worker task
}

struct WorkerResult {
    thread_id: usize,
    doc_sizes_for_thread: Vec<u64>,
    temp_bin_path: PathBuf,
    bytes_written_to_temp_bin: u64,
    tokens_processed_in_thread: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriterMode {
    Data,
    Index,
}

pub struct RwkvBinidxWriter<T: Serialize + Send + Sync + 'static + Debug + Into<BinidxItem>> {
    base_path: PathBuf,
    filename_prefix: String,
    num_threads: usize,
    _phantom: PhantomData<T>,
}

impl<T: Serialize + Send + Sync + 'static + Debug + Into<BinidxItem>> RwkvBinidxWriter<T> {
    pub fn new(base_path: String, filename_prefix: String, num_threads: usize) -> Result<Self, std::io::Error> {
        if num_threads == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "num_threads cannot be zero"));
        }
        Ok(Self {
            base_path: PathBuf::from(base_path),
            filename_prefix,
            num_threads: num_threads.max(1),
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static + Debug + Into<BinidxItem>> Writer<T> for RwkvBinidxWriter<T> {
    async fn pipeline(
        &self,
        mut rx: TokioReceiver<T>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("RwkvBinidxWriter pipeline started for {} based at {} with {} threads.", self.filename_prefix, self.base_path.display(), self.num_threads);
        let overall_start_time = Instant::now();

        let (coordinator_input_tx, mut coordinator_input_rx): (TokioSender<Option<BinidxItem>>, TokioReceiver<Option<BinidxItem>>) = channel(self.num_threads * 2);
        let (worker_result_tx, worker_result_rx): (StdSender<WorkerResult>, StdReceiver<WorkerResult>) = mpsc::channel();

        let num_threads_coord = self.num_threads;
        let filename_prefix_coord = self.filename_prefix.clone();
        let base_path_coord = self.base_path.clone();
        
        let coordinator_handle = thread::spawn(move || -> Result<u64, String> {
            let rt = tokio::runtime::Runtime::new().map_err(|e| format!("Failed to create Tokio runtime in coordinator: {}", e))?;
            let mut items_processed_by_coordinator_thread: u64 = 0;
            
            rt.block_on(async {
                let mut worker_handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_threads_coord);
                let mut worker_senders: Vec<TokioSender<Option<BinidxItem>>> = Vec::with_capacity(num_threads_coord);

                for i in 0..num_threads_coord {
                    let (worker_task_tx, mut worker_task_rx): (TokioSender<Option<BinidxItem>>, TokioReceiver<Option<BinidxItem>>) = channel(100);
                    worker_senders.push(worker_task_tx);
                    let result_sender_clone = worker_result_tx.clone();
                    let temp_file_base = base_path_coord.join(format!("{}_temp_thread_{}", filename_prefix_coord, i));

                    worker_handles.push(tokio::spawn(async move {
                        let temp_bin_file_path_str = temp_file_base.with_extension("bin.tmp");
                        let mut temp_bin_writer = BufWriter::new(File::create(&temp_bin_file_path_str).expect("Failed to create temp bin file"));
                        let mut doc_sizes_for_this_thread = Vec::new();
                        let mut bytes_written_this_thread: u64 = 0;
                        let mut tokens_count_this_thread: u64 = 0;

                        while let Some(Some(bin_item)) = worker_task_rx.recv().await {
                            doc_sizes_for_this_thread.push(bin_item.tokens.len() as u64);
                            tokens_count_this_thread += bin_item.tokens.len() as u64;
                            let bytes_slice = bytemuck::cast_slice(&bin_item.tokens);
                            temp_bin_writer.write_all(bytes_slice).expect("Failed to write to temp bin file");
                            bytes_written_this_thread += bytes_slice.len() as u64;
                        }
                        temp_bin_writer.flush().expect("Failed to flush temp bin writer");
                        
                        let worker_final_result = WorkerResult {
                            thread_id: i,
                            temp_bin_path: temp_bin_file_path_str,
                            doc_sizes_for_thread: doc_sizes_for_this_thread,
                            bytes_written_to_temp_bin: bytes_written_this_thread,
                            tokens_processed_in_thread: tokens_count_this_thread,
                        };
                        if result_sender_clone.send(worker_final_result).is_err() {
                            eprintln!("[Worker {}] Failed to send result, main channel closed.", i);
                        }
                    }));
                }

                loop {
                    tokio::select! {
                        Some(maybe_item) = coordinator_input_rx.recv() => {
                            if let Some(item) = maybe_item {
                                let worker_idx = (items_processed_by_coordinator_thread % num_threads_coord as u64) as usize;
                                if worker_senders[worker_idx].send(Some(item)).await.is_err() {
                                    eprintln!("[Coordinator] Failed to send item to worker {}. Channel closed.", worker_idx);
                                }
                                items_processed_by_coordinator_thread += 1;
                            } else { 
                                println!("[Coordinator] End signal received from main pipeline. Signaling workers to stop.");
                                for (idx, sender) in worker_senders.iter().enumerate() {
                                    if sender.send(None).await.is_err() { 
                                        eprintln!("[Coordinator] Failed to send stop signal to worker {}", idx);
                                    }
                                }
                                break; 
                            }
                        }
                        else => { 
                            println!("[Coordinator] Input channel closed unexpectedly.");
                             for (idx, sender) in worker_senders.iter().enumerate() {
                                if sender.send(None).await.is_err() {
                                    eprintln!("[Coordinator] Failed to send stop signal to worker {} on unexpected close.", idx);
                                }
                            }
                            break;
                        }
                    }
                }
                
                println!("[Coordinator] Waiting for all worker tasks to complete...");
                join_all(worker_handles).await;
                println!("[Coordinator] All worker tasks completed. Items processed by coord: {}", items_processed_by_coordinator_thread);
            });
            
            drop(worker_result_tx);
            Ok(items_processed_by_coordinator_thread)
        });

        let forward_to_coord_handle = tokio::spawn(async move {
            while let Some(item_t) = rx.recv().await {
                let bin_item: BinidxItem = item_t.into();
                if coordinator_input_tx.send(Some(bin_item)).await.is_err() {
                    eprintln!("Failed to send item to coordinator. Coordinator likely terminated.");
                    break;
                }
            }
            if coordinator_input_tx.send(None).await.is_err() {
                eprintln!("Failed to send end signal to coordinator.");
            }
        });

        forward_to_coord_handle.await.map_err(|e| format!("Item forwarding task panicked: {}", e))?;
        println!("Item forwarding to coordinator complete.");

        let mut total_items_processed_approx: u64 = 0;
        match coordinator_handle.join() {
            Ok(Ok(count)) => {
                println!("Coordinator thread finished successfully. Items distributed by coord: {}", count);
                total_items_processed_approx = count;
            },
            Ok(Err(e)) => return Err(format!("Coordinator thread failed: {}", e).into()),
            Err(_e) => return Err("Coordinator thread panicked.".into()),
        }

        let collected_worker_results: Vec<WorkerResult> = worker_result_rx.iter().collect();
        println!("Collected {} worker results.", collected_worker_results.len());

        if collected_worker_results.is_empty() && total_items_processed_approx > 0 && self.num_threads > 0 {
             println!("Warning: No worker results collected, but {} items were processed by coordinator for {} threads. Check worker logic.", total_items_processed_approx, self.num_threads);
        }

        let mut final_worker_results = collected_worker_results;
        final_worker_results.sort_by_key(|r| r.thread_id);
        
        let total_docs_from_workers: usize = final_worker_results.iter().map(|r| r.doc_sizes_for_thread.len()).sum();
        let total_tokens_from_workers: u64 = final_worker_results.iter().map(|r| r.tokens_processed_in_thread).sum();

        let (final_bin_path, total_bytes_in_final_bin) = self.merge_temp_files(&final_worker_results)?;
        let all_doc_sizes: Vec<u64> = final_worker_results.iter().flat_map(|r| r.doc_sizes_for_thread.clone()).collect();
        self.write_final_idx(&final_bin_path, &all_doc_sizes)?;
        self.cleanup_temp_files(&final_worker_results);

        println!(
            "[RwkvBinidxWriter] Pipeline finished in {:?}. Total items proxied by coord: {}. Docs from workers: {}. Tokens from workers: {}. Final .bin size: {:.2} MB.",
            overall_start_time.elapsed(),
            total_items_processed_approx,
            total_docs_from_workers,
            total_tokens_from_workers,
            total_bytes_in_final_bin as f64 / (1024.0 * 1024.0)
        );
        
        Ok(())
    }
}

impl<T: Serialize + Send + Sync + 'static + Debug + Into<BinidxItem>> RwkvBinidxWriter<T> {
    fn merge_temp_files(
        &self, 
        worker_results: &[WorkerResult]
    ) -> Result<(PathBuf, u64), Box<dyn std::error::Error + Send + Sync>> {
        let final_bin_path = self.base_path.join(format!("{}.bin", self.filename_prefix));
        let mut final_bin_writer = BufWriter::new(File::create(&final_bin_path)?);
        let mut total_bytes_written: u64 = 0;

        println!("Merging {} temporary .bin files into {}", worker_results.len(), final_bin_path.display());
        let merge_pb = ProgressBar::new(worker_results.iter().map(|r| r.bytes_written_to_temp_bin).sum());
        merge_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Merging [{bar:40.green/blue}] {bytes}/{total_bytes} ({eta})"
            ).unwrap().progress_chars("##-")
        );

        for result in worker_results {
            let mut temp_file_reader = BufReader::new(File::open(&result.temp_bin_path)?);
            let bytes_copied = std::io::copy(&mut temp_file_reader, &mut final_bin_writer)?;
            total_bytes_written += bytes_copied;
            merge_pb.inc(bytes_copied);
        }
        final_bin_writer.flush()?;
        merge_pb.finish_with_message("Merging complete.");
        Ok((final_bin_path, total_bytes_written))
    }

    fn write_final_idx(
        &self, 
        final_bin_path: &Path, 
        all_doc_sizes: &[u64]
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let final_idx_path = final_bin_path.with_extension("idx");
        let mut idx_writer = BufWriter::new(File::create(&final_idx_path)?);
        println!("Writing final .idx file to: {}", final_idx_path.display());

        idx_writer.write_all(b"BINIDX")?;
        idx_writer.write_all(&1u32.to_le_bytes())?;
        idx_writer.write_all(&(<u16>::max_value() as u32).to_le_bytes())?;
        idx_writer.write_all(&(all_doc_sizes.len() as u64).to_le_bytes())?;
        idx_writer.write_all(&(1u64).to_le_bytes())?;
        idx_writer.write_all(&(1u64).to_le_bytes())?;

        let mut current_offset: u64 = 0;
        for &size_in_tokens in all_doc_sizes {
            let size_in_bytes = size_in_tokens * std::mem::size_of::<u16>() as u64;
            idx_writer.write_all(&size_in_bytes.to_le_bytes())?;
            idx_writer.write_all(&current_offset.to_le_bytes())?;
            current_offset += size_in_bytes;
        }
        idx_writer.flush()?;
        println!("Finished writing .idx file.");
        Ok(())
    }

    fn cleanup_temp_files(&self, worker_results: &[WorkerResult]) {
        println!("Cleaning up temporary files...");
        for result in worker_results {
            if let Err(e) = fs::remove_file(&result.temp_bin_path) {
                eprintln!("Failed to remove temp file {}: {}", result.temp_bin_path.display(), e);
            }
        }
        println!("Temporary files cleanup complete.");
    }
}
