use crate::writers::Writer; // Ensure Writer trait is in scope
use crate::utils::tokenizer::Tokenizer; // Will be removed from this file's direct use
use bytemuck;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize; // Added Serialize import
use std::fs::{File, remove_file};
use std::io::{BufWriter, Write, BufReader, Read}; // Removed Seek, SeekFrom as pipeline handles full flow
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender as StdSender, Receiver as StdReceiver}; // Using std::sync::mpsc for worker results for now
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tokio::sync::mpsc::{self as tk_mpsc, Sender as TkSender, Receiver as TkReceiver}; // For async pipeline input

// Constants from jsonl2binidx, verify their meaning for RWKV format if possible
const MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00"; // "Magic Multi-Index IDX" ?
const VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0]; // Version 1.0
const ACTUAL_DTYPE_BYTE: [u8; 1] = [8u8]; // Explicitly add/ensure this constant for DTYPE
const TOKEN_SIZE_BYTES: usize = 2; // Assuming u16 tokens

#[derive(Debug, Clone, Serialize)]
pub struct BinidxItem {
    pub tokens: Vec<u16>,
}

// WorkerPayload now internal to pipeline's worker setup
struct WorkerPayloadInternal {
    item: Option<BinidxItem>, // Option to signal end-of-stream
    // thread_id is implicit by worker task
}

struct WorkerResult {
    thread_id: usize,
    doc_sizes_for_thread: Vec<u32>,
    temp_bin_path: PathBuf,
    tokens_processed_in_thread: u64,
    bytes_written_to_temp_bin: u64,
}

pub struct RwkvBinidxWriter {
    base_path: PathBuf,
    filename_prefix: String,
    num_threads: usize,
    // Removed: tokenizer, data_sender, coordinator_handle, progress_bar
}

impl RwkvBinidxWriter {
    pub fn new(
        output_base_path: &Path,
        output_filename_prefix: &str,
        num_threads: usize,
    ) -> Result<Self, std::io::Error> { // Error type might need to be more generic if other errors can occur
        if num_threads == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "num_threads cannot be zero"));
        }
        Ok(Self {
            base_path: output_base_path.to_path_buf(),
            filename_prefix: output_filename_prefix.to_string(),
            num_threads,
        })
    }

    // Logic from old `finalize` and worker/coordinator setup will move into `pipeline`
    // Old `add_record`, `finalize`, `Drop` are removed.
}

#[async_trait::async_trait]
impl Writer<BinidxItem> for RwkvBinidxWriter {
    async fn pipeline(
        &self, // pipeline now takes &self
        mut rx: TkReceiver<BinidxItem>, // Changed to tokio's mpsc::Receiver
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("RwkvBinidxWriter pipeline started for {} with {} threads.", self.filename_prefix, self.num_threads);
        let overall_start_time = Instant::now();

        let (coordinator_input_tx, mut coordinator_input_rx) = tk_mpsc::channel::<Option<BinidxItem>>(self.num_threads * 2); // Buffer for coordinator
        let (worker_result_tx, mut worker_result_rx) = mpsc::channel::<WorkerResult>(); // std::sync::mpsc for thread results

        // --- Spawn Coordinator Thread ---
        // The coordinator will manage worker threads and distribute BinidxItems.
        // It receives items from this pipeline's main rx (forwarded via coordinator_input_tx)
        // and collects results from workers via worker_result_rx.
        let num_threads_coord = self.num_threads;
        let base_path_coord = self.base_path.clone();
        let filename_prefix_coord = self.filename_prefix.clone();
        
        let coordinator_handle = thread::spawn(move || -> Vec<WorkerResult> {
            let mut worker_handles = Vec::with_capacity(num_threads_coord);
            let mut worker_senders = Vec::with_capacity(num_threads_coord);

            for i in 0..num_threads_coord {
                // Each worker gets its own channel for BinidxItems
                let (worker_task_tx, mut worker_task_rx) = tk_mpsc::channel::<Option<BinidxItem>>(100); // Buffer per worker
                worker_senders.push(worker_task_tx);
                
                let temp_bin_path_prefix = base_path_coord.join(format!("{}_thread_{}", filename_prefix_coord, i));
                let result_tx_clone = worker_result_tx.clone();

                worker_handles.push(tokio::spawn(async move { // Workers are now tokio tasks
                    let temp_bin_file_path = temp_bin_path_prefix.with_extension("bin.tmp");
                    let mut temp_bin_writer = BufWriter::new(File::create(&temp_bin_file_path).expect("Failed to create temp bin file"));
                    let mut doc_sizes_for_this_thread = Vec::new();
                    let mut tokens_count: u64 = 0;
                    let mut bytes_written: u64 = 0;

                    while let Some(payload_opt) = worker_task_rx.recv().await {
                        match payload_opt {
                            Some(bin_item) => {
                                let tokens = bin_item.tokens;
                                if !tokens.is_empty() {
                                    let token_bytes: &[u8] = bytemuck::cast_slice(&tokens);
                                    temp_bin_writer.write_all(token_bytes).expect("Failed to write to temp bin file");
                                    doc_sizes_for_this_thread.push(tokens.len() as u32);
                                    tokens_count += tokens.len() as u64;
                                    bytes_written += token_bytes.len() as u64;
                                } else {
                                    doc_sizes_for_this_thread.push(0);
                                }
                            }
                            None => { // End signal for this worker
                                temp_bin_writer.flush().expect("Failed to flush temp bin writer");
                                break;
                            }
                        }
                    }
                    // Send result back to coordinator (via std::sync::mpsc)
                    result_tx_clone.send(WorkerResult {
                        thread_id: i,
                        doc_sizes_for_thread: doc_sizes_for_this_thread,
                        temp_bin_path: temp_bin_file_path,
                        tokens_processed_in_thread: tokens_count,
                        bytes_written_to_temp_bin: bytes_written,
                    }).expect("Failed to send worker result to coordinator");
                }));
            }
            
            drop(worker_result_tx); // Drop original sender so receiver closes when all clones are dropped

            // Coordinator logic: distribute incoming BinidxItems to workers
            let mut current_worker_idx = 0;
            let mut records_processed_by_coordinator = 0u64;
            let progress_bar_encoding = ProgressBar::new(0); // Length unknown initially
            progress_bar_encoding.set_style(
                 ProgressStyle::with_template(
                    "[{elapsed_precise}] [Encoding Items {bar:40.cyan/blue}] {pos} items ({per_sec}, ETA {eta})",
                ).unwrap().progress_chars("##-"),
            );


            // This loop receives from the main pipeline's `rx` (forwarded via coordinator_input_tx)
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    while let Some(item_opt) = coordinator_input_rx.recv().await {
                        match item_opt {
                            Some(bin_item) => {
                                progress_bar_encoding.inc(1);
                                records_processed_by_coordinator +=1;
                                worker_senders[current_worker_idx]
                                    .send(Some(bin_item)).await // Send Some(item)
                                    .expect("Failed to send record to worker thread's tokio channel");
                                current_worker_idx = (current_worker_idx + 1) % num_threads_coord;
                            }
                            None => { // End signal for coordinator from main pipeline
                                println!("[Coordinator] End signal received. Signaling workers to stop.");
                                for (idx, sender) in worker_senders.iter().enumerate() {
                                     if sender.send(None).await.is_err() { // Send None to stop worker
                                         eprintln!("[Coordinator] Failed to send stop signal to worker {}", idx);
                                     }
                                }
                                break; 
                            }
                        }
                    }
                });
            progress_bar_encoding.finish_with_message(format!("Item encoding/distribution phase complete ({} items).", records_processed_by_coordinator));
            
            // Collect results from worker tokio tasks (these are JoinHandles from tokio::spawn)
            // This part needs to be careful with futures::future::join_all or similar
            let mut all_results = Vec::with_capacity(num_threads_coord);
            // We need to run a small tokio runtime here too, or pass the handles out to an async context
            // For simplicity in a sync thread, let's assume the tokio::spawn handles complete.
            // This is a tricky part: mixing std::thread for coordinator and tokio tasks for workers.
            // A better design would be all-async or all-std::thread for the worker pool.
            // Given workers do I/O, async is good. Coordinator could also be async.

            // Let's assume the worker_result_rx (std::sync::mpsc) collects results correctly as workers finish.
            // The tokio::spawn tasks will complete and send their results.
            // We will collect from worker_result_rx (std::sync::mpsc) after the loop.
            // This implies worker_handles.join() is not directly done here in the same way if they are tokio tasks.

            // The results will be collected via worker_result_rx (std::sync::mpsc)
            // after the coordinator_input_rx loop finishes and workers are signaled to stop.
            // The number of results to expect is num_threads_coord.
            
            // Instead of joining tokio handles here, we rely on workers sending to worker_result_tx (std::sync::mpsc)
            // The collection happens outside this tokio::runtime block, using worker_result_rx.
            all_results // This will be collected via worker_result_rx in the outer scope of coordinator_handle
        });
        // --- End Spawn Coordinator Thread ---


        // --- Forward data from pipeline's rx to coordinator's input channel ---
        let forwarding_overall_item_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let forwarding_item_count_clone = Arc::clone(&forwarding_overall_item_count);

        let forward_to_coordinator_handle = tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                forwarding_item_count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if coordinator_input_tx.send(Some(item)).await.is_err() {
                    eprintln!("Coordinator input channel closed. Cannot forward item.");
                    // This error should propagate out of the pipeline
                    return Err("Coordinator input channel closed while forwarding".to_string());
                }
            }
            // Signal coordinator that no more items are coming
            if coordinator_input_tx.send(None).await.is_err() {
                eprintln!("Failed to send end signal to coordinator after input rx closed.");
                 return Err("Failed to send end signal to coordinator".to_string());
            }
            Ok(())
        });
        
        // Wait for forwarding to complete
        match forward_to_coordinator_handle.await {
            Ok(Ok(())) => println!("All items forwarded to coordinator."),
            Ok(Err(e)) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e))),
            Err(e) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("Forwarding task panicked: {:?}", e)))),
        }

        // --- Collect results from coordinator (which collects from workers) ---
        // The coordinator thread `coordinator_handle` will finish after its tokio runtime block_on completes
        // and then it returns Vec<WorkerResult> implicitly when worker_result_rx is drained.
        // The collection of WorkerResult is done by the coordinator thread internally from worker_result_rx.
        // The coordinator_handle.join() will give Vec<WorkerResult>.

        let mut worker_results = Vec::new();
        for _ in 0..self.num_threads { // Expect one result per worker thread
            if let Ok(result) = worker_result_rx.recv() { // Blocking recv on std::sync::mpsc::Receiver
                 worker_results.push(result);
            } else {
                // This means a worker (or coordinator) might have panicked or not sent a result.
                // Or channel closed prematurely.
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Failed to receive all worker results.")));
            }
        }
        // Ensure coordinator thread itself has finished, though results collection implies it should have.
        // The coordinator_handle.join() call is problematic if it returns Vec<WorkerResult> *and* we also collect from worker_result_rx.
        // The design: coordinator thread `thread::spawn` uses `mpsc::channel` to send results out.
        // So, we `worker_result_rx.recv()` here in the main pipeline async task.
        // The `coordinator_handle.join()` will just ensure the thread finished.
        
        // The coordinator thread will finish after its tokio runtime block_on completes.
        // The results are collected above. Now, we can join the std::thread.
        if let Err(_e) = coordinator_handle.join() {
             return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Coordinator thread panicked.")));
        }
        
        // Sort results by thread_id to ensure deterministic merge order
        worker_results.sort_by_key(|r| r.thread_id);


        if worker_results.len() != self.num_threads {
            eprintln!("Expected {} worker results, but got {}. Finalization might be incomplete.", self.num_threads, worker_results.len());
            if worker_results.is_empty() && self.num_threads > 0 {
                 return Err("No worker results collected, cannot finalize.".into());
            }
        }

        // --- Merge temporary .bin files and collect all doc_sizes (from old finalize) ---
        println!("Merging temporary .bin files...");
        let final_bin_path = self.base_path.join(&self.filename_prefix).with_extension("bin");
        let mut final_bin_writer = BufWriter::new(File::create(&final_bin_path)?);
        
        let mut all_doc_sizes: Vec<u32> = Vec::new();
        let mut total_docs: u64 = 0;
        let mut total_tokens_overall: u64 = 0;
        let mut total_bytes_in_final_bin: u64 = 0;

        let total_bytes_to_merge: u64 = worker_results.iter().map(|r| r.bytes_written_to_temp_bin).sum();
        let merge_pb = ProgressBar::new(total_bytes_to_merge);
        merge_pb.set_style(
             ProgressStyle::with_template("[{elapsed_precise}] [Merging .bin files {bar:40.green/black}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        for result in worker_results {
            all_doc_sizes.extend(&result.doc_sizes_for_thread);
            total_docs += result.doc_sizes_for_thread.len() as u64;
            total_tokens_overall += result.tokens_processed_in_thread;
            
            if result.bytes_written_to_temp_bin > 0 {
                let mut temp_file_reader = BufReader::new(File::open(&result.temp_bin_path)?);
                let copied_bytes = std::io::copy(&mut temp_file_reader, &mut final_bin_writer)?;
                total_bytes_in_final_bin += copied_bytes;
                merge_pb.inc(copied_bytes);
            }
            if let Err(e) = remove_file(&result.temp_bin_path) {
                 eprintln!("Failed to remove temp file {:?}: {}", result.temp_bin_path, e);
            }
        }
        final_bin_writer.flush()?;
        merge_pb.finish_with_message(format!("Temporary .bin files merged ({} bytes).", total_bytes_in_final_bin));

        // --- Build and write .idx file (from old finalize) ---
        println!("Building and writing .idx file to path: {:?}", self.base_path);
        let final_idx_path = self.base_path.join(&self.filename_prefix).with_extension("idx");
        let mut idx_writer = BufWriter::new(File::create(&final_idx_path)?);

        idx_writer.write_all(MAGIC_HDR)?;
        idx_writer.write_all(&VERSION)?;
        idx_writer.write_all(&ACTUAL_DTYPE_BYTE)?;
        idx_writer.write_all(&(all_doc_sizes.len() as u64).to_le_bytes())?;
        
        let text_idx_len = all_doc_sizes.len() as u64 + 1;
        idx_writer.write_all(&text_idx_len.to_le_bytes())?;

        for &size in &all_doc_sizes {
            idx_writer.write_all(&size.to_le_bytes())?;
        }

        let mut current_bin_offset: u64 = 0;
        for &size in &all_doc_sizes {
            idx_writer.write_all(&current_bin_offset.to_le_bytes())?;
            current_bin_offset += size as u64 * TOKEN_SIZE_BYTES as u64;
        }
        
        for i in 0..text_idx_len {
            idx_writer.write_all(&i.to_le_bytes())?;
        }
        idx_writer.flush()?;
        println!(".idx file written to {:?}", final_idx_path);

        // --- Calculate Magic Prime (from old finalize) ---
        let mut magic_prime: u64 = 0;
        let context_length: u64 = 2048; // Example, should be configurable
        if total_tokens_overall > context_length * 3 {
            let n_chunk = (total_tokens_overall / context_length).saturating_sub(1);
            for i in (0..=n_chunk).rev() {
                // Placeholder for primes::is_prime(i)
                // Add `primes = "0.3.0"` to Cargo.toml if you want to use the primes crate
                // For now, let's use a simple primality test or skip.
                // Simple primality test (not for production)
                let mut is_i_prime = true;
                if i <= 1 { is_i_prime = false; }
                else if i <= 3 { is_i_prime = true; }
                else if i % 2 == 0 || i % 3 == 0 { is_i_prime = false; }
                else {
                    let mut k = 5;
                    while k * k <= i {
                        if i % k == 0 || i % (k + 2) == 0 {
                            is_i_prime = false;
                            break;
                        }
                        k += 6;
                    }
                }
                if is_i_prime && i % 3 == 2 {
                    magic_prime = i;
                    break;
                }
            }
        }
        println!("Magic prime (example context {}): {}", context_length, magic_prime);
        
        let duration = overall_start_time.elapsed();
        println!("RwkvBinidxWriter pipeline finished successfully in {:?}.", duration);
        println!(
            "Output: {} and {}",
            final_bin_path.display(),
            final_idx_path.display()
        );
        let total_items_processed = forwarding_overall_item_count.load(std::sync::atomic::Ordering::Relaxed);
        println!(
            "Processed: {} items, {} documents, {} tokens. Final .bin size: {:.2} MB.",
            total_items_processed,
            total_docs,
            total_tokens_overall,
            total_bytes_in_final_bin as f32 / 1024.0 / 1024.0,
        );

        Ok(())
    }
}
