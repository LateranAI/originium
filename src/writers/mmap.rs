use crate::custom_tasks::DataEndpoint;
use crate::errors::FrameworkError;
use crate::utils::common_type::{MmapItem, MmapTokenUnitType};
use crate::writers::Writer;
use async_trait::async_trait;
use bytemuck::{self, Pod, Zeroable};
use futures::future::join_all;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{BufWriter, Read, Write};
use std::io;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver as StdReceiver, Sender as StdSender};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver as TokioReceiver;
use tokio::sync::mpsc::{Sender as TokioSender, channel};
use tokio::task::JoinHandle;
use primes;

const LEGACY_MMAP_BINIDX_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const LEGACY_MMAP_BINIDX_VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
const LEGACY_MMAP_BINIDX_DTYPE_U16: u8 = 8;

const GENERIC_MMAP_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const GENERIC_MMAP_VERSION: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];

struct TempBinWorkerResult {
    worker_id: usize,
    logical_item_counts: Vec<u32>,
    temp_bin_file_path: PathBuf,
    bytes_written_to_temp_bin: u64,
    total_units_processed_by_worker: u64,
}

pub struct MmapWriter<T, TokenUnit>
where
    T: Serialize + Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>>,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    output_base_path: PathBuf,
    output_filename: String,
    num_processing_workers: usize,
    token_unit_type: MmapTokenUnitType,
    token_unit_len: usize,
    is_legacy_rwkv_format: bool,
    context_length: Option<usize>,
    _phantom_data: PhantomData<(T, TokenUnit)>,
}

impl<T, TokenUnit> MmapWriter<T, TokenUnit>
where
    T: Serialize + Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>>,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    pub fn new(endpoint_config: &DataEndpoint) -> Self {
        if let DataEndpoint::Mmap {
            base_path,
            filename,
            num_threads,
            token_unit_type,
            token_unit_len,
            is_legacy_rwkv_format,
            context_length,
        } = endpoint_config
        {
            if *num_threads == 0 {
                panic!(
                    "[MmapWriter::new] num_threads cannot be zero. This is a critical configuration error."
                );
            }
            if *token_unit_len == 0 {
                panic!(
                    "[MmapWriter::new] token_unit_len cannot be zero. This is a critical configuration error."
                );
            }

            if *is_legacy_rwkv_format {
                assert_eq!(
                    *token_unit_type,
                    MmapTokenUnitType::U16,
                    "Legacy RWKV format must use U16 tokens."
                );
                assert_eq!(
                    *token_unit_len, 1,
                    "Legacy RWKV format must have token_unit_len of 1."
                );
            }

            fs::create_dir_all(base_path)
                .unwrap_or_else(|e| panic!("[MmapWriter::new] Failed to create output base directory '{}': {}. This is critical.", base_path, e));

            Self {
                output_base_path: PathBuf::from(base_path),
                output_filename: filename.clone(),
                num_processing_workers: (*num_threads).max(1),
                token_unit_type: *token_unit_type,
                token_unit_len: *token_unit_len,
                is_legacy_rwkv_format: *is_legacy_rwkv_format,
                context_length: *context_length,
                _phantom_data: PhantomData,
            }
        } else {
            panic!(
                "[MmapWriter::new] Incorrect DataEndpoint variant provided. Expected DataEndpoint::Mmap."
            );
        }
    }
}

#[async_trait]
impl<T, TokenUnit> Writer<T> for MmapWriter<T, TokenUnit>
where
    T: Serialize + Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>>,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    async fn pipeline(
        &self,
        mut incoming_item_rx: TokioReceiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        mp.println(format!(
            "[MmapWriter] Pipeline started for '{}' (Format: {}, UnitType: {:?}, UnitLen: {}) based at '{}' with {} worker(s).",
            self.output_filename,
            if self.is_legacy_rwkv_format { "Legacy RWKV" } else { "Generic" },
            self.token_unit_type,
            self.token_unit_len,
            self.output_base_path.display(),
            self.num_processing_workers
        )).unwrap_or_default();
        let overall_start_time = Instant::now();

        if self.num_processing_workers == 1 {

            mp.println("[MmapWriter] Running in single-threaded mode (direct write).")
              .unwrap_or_default();

            let processing_pb = mp.add(ProgressBar::new_spinner());
            processing_pb.enable_steady_tick(Duration::from_millis(120));
            processing_pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [Writing Directly {spinner:.blue}] {pos} items processed ({per_sec})"
                ).unwrap()
            );

            let final_bin_file_path = self
                .output_base_path
                .join(format!("{}.bin", self.output_filename));
            let mut final_bin_file_writer = BufWriter::new(
                File::create(&final_bin_file_path)?
            );

            let mut logical_item_counts: Vec<u64> = Vec::new();
            let mut total_bytes_written: u64 = 0;
            let mut total_units_processed: u64 = 0;
            let mut items_processed_count: u64 = 0;

            while let Some(item_t_from_caller) = incoming_item_rx.recv().await {
                processing_pb.inc(1);
                items_processed_count += 1;

                let mmap_item: MmapItem<TokenUnit> = item_t_from_caller.into();

                if mmap_item.tokens.is_empty() {
                    mp.println("[MmapWriter Single] Warning: Received empty MmapItem, skipping.").unwrap_or_default();
                    logical_item_counts.push(0);
                    continue;
                }
                
                if mmap_item.tokens.len() % self.token_unit_len != 0 {
                    return Err(Box::new(FrameworkError::InternalError(format!(
                        "[MmapWriter Single] Received MmapItem with {} token units, which is not divisible by token_unit_len {}. Critical data error.",
                        mmap_item.tokens.len(), self.token_unit_len
                    ))));
                }
                
                let num_logical_tokens = (mmap_item.tokens.len() / self.token_unit_len) as u64;
                logical_item_counts.push(num_logical_tokens);
                total_units_processed += mmap_item.tokens.len() as u64;
                
                let token_bytes_slice = bytemuck::cast_slice(&mmap_item.tokens);
                final_bin_file_writer.write_all(token_bytes_slice)?;
                total_bytes_written += token_bytes_slice.len() as u64;
            }

            final_bin_file_writer.flush()?;
            
            processing_pb.finish_with_message(format!(
                "[Writing Directly] Finished processing items. Total: {}",
                processing_pb.position()
            ));

            self.write_final_idx_file(&final_bin_file_path, &logical_item_counts, Arc::clone(&mp))?;

            mp.println(format!(
                "[MmapWriter Single] Pipeline finished in {:?}. Items processed: {}. Token Units processed: {}. Final .bin size: {:.2} MB.",
                overall_start_time.elapsed(),
                items_processed_count,
                total_units_processed,
                total_bytes_written as f64 / (1024.0 * 1024.0)
            )).unwrap_or_default();


            let total_logical_tokens: u64 = logical_item_counts.iter().sum();
            
            let mut magic_prime: u64 = 0;
            if let Some(ctx_len) = self.context_length {
                let context_length = ctx_len as u64;
                if context_length > 0 && total_logical_tokens > context_length * 3 {
                    let n_chunk = (total_logical_tokens / context_length).saturating_sub(1);

                    for i in (0..n_chunk).rev() {
                        if i % 3 == 2 && primes::is_prime(i) {
                            magic_prime = i;
                            break;
                        }
                    }
                    mp.println(format!(
                        "[MmapWriter Single] Total logical tokens: {}. Context length: {}. Magic prime selected: {}.",
                        total_logical_tokens, context_length, magic_prime
                    )).unwrap_or_default();
                } else {
                    mp.println(format!(
                        "[MmapWriter Single] Skipping magic_prime calculation: total_tokens ({}) <= context_length ({}) * 3 or context_length is 0.",
                        total_logical_tokens, context_length
                    )).unwrap_or_default();
                }
            } else {
                mp.println("[MmapWriter Single] Skipping magic_prime calculation: context_length not provided.").unwrap_or_default();
            }


            Ok(())

        } else {

            let processing_pb = mp.add(ProgressBar::new_spinner());
            processing_pb.enable_steady_tick(Duration::from_millis(120));
            processing_pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [Writing to Temp Files {spinner:.blue}] {pos} items processed ({per_sec})"
                ).unwrap()
            );

            let (coordinator_input_tx, mut coordinator_input_rx): (
                TokioSender<Option<MmapItem<TokenUnit>>>,
                TokioReceiver<Option<MmapItem<TokenUnit>>>,
            ) = channel(self.num_processing_workers * 2);
            let (worker_result_tx, worker_result_rx): (
                StdSender<TempBinWorkerResult>,
                StdReceiver<TempBinWorkerResult>,
            ) = mpsc::channel();

            let num_workers_for_coord = self.num_processing_workers;
            let filename_for_coord = self.output_filename.clone();
            let base_path_for_coord = self.output_base_path.clone();
            let token_unit_len_for_worker = self.token_unit_len;

            let mp_for_coord = Arc::clone(&mp);

            let coordinator_handle = thread::spawn(move || -> Result<u64, String> {
                let rt = tokio::runtime::Runtime::new()
                    .map_err(|e| format!("[Coordinator] Failed to create Tokio runtime: {}", e))?;
                let mut items_distributed_by_coordinator: u64 = 0;

                rt.block_on(async {
                    let mut worker_join_handles: Vec<JoinHandle<()>> = Vec::with_capacity(num_workers_for_coord);
                    let mut worker_task_senders: Vec<TokioSender<Option<MmapItem<TokenUnit>>>> = Vec::with_capacity(num_workers_for_coord);

                    for worker_idx in 0..num_workers_for_coord {
                        let (worker_task_tx, mut worker_task_rx): (TokioSender<Option<MmapItem<TokenUnit>>>, TokioReceiver<Option<MmapItem<TokenUnit>>>) = channel(100);
                        worker_task_senders.push(worker_task_tx);
                        let result_sender_for_worker = worker_result_tx.clone();
                        let temp_file_base_for_worker = base_path_for_coord.join(format!("{}_temp_worker_{}", filename_for_coord, worker_idx));
                        
                        let current_token_unit_len = token_unit_len_for_worker;

                        worker_join_handles.push(tokio::spawn(async move {
                            let temp_bin_file_path = temp_file_base_for_worker.with_extension("bin.tmp");
                            let mut temp_bin_file_writer = BufWriter::new(
                                File::create(&temp_bin_file_path)
                                    .expect(&format!("[Worker {}] Failed to create temp bin file: {}", worker_idx, temp_bin_file_path.display()))
                            );
                            let mut logical_item_counts_for_worker: Vec<u32> = Vec::new();
                            let mut bytes_written_by_worker: u64 = 0;
                            let mut total_units_count_for_worker: u64 = 0;

                            while let Some(Some(mmap_item)) = worker_task_rx.recv().await {
                                if mmap_item.tokens.is_empty() {

                                     logical_item_counts_for_worker.push(0);
                                     continue;
                                }

                                if mmap_item.tokens.len() % current_token_unit_len != 0 {
                                    panic!(
                                        "[Worker {}] Received MmapItem with {} token units, which is not divisible by token_unit_len {}. Critical data error.",
                                        worker_idx, mmap_item.tokens.len(), current_token_unit_len
                                    );
                                }
                                

                                let num_logical_tokens_u32 = (mmap_item.tokens.len() / current_token_unit_len) as u32; 
                                if num_logical_tokens_u32 as usize * current_token_unit_len != mmap_item.tokens.len() {
                                     panic!("[Worker {}] Potential overflow calculating logical tokens as u32.", worker_idx);
                                }
                                logical_item_counts_for_worker.push(num_logical_tokens_u32); 
                                
                                total_units_count_for_worker += mmap_item.tokens.len() as u64;
                                
                                let token_bytes_slice = bytemuck::cast_slice(&mmap_item.tokens);
                                temp_bin_file_writer.write_all(token_bytes_slice)
                                    .expect(&format!("[Worker {}] Failed to write to temp bin file: {}", worker_idx, temp_bin_file_path.display()));
                                bytes_written_by_worker += token_bytes_slice.len() as u64;
                            }
                            temp_bin_file_writer.flush()
                                .expect(&format!("[Worker {}] Failed to flush temp bin file: {}", worker_idx, temp_bin_file_path.display()));

                            let worker_final_result = TempBinWorkerResult {
                                worker_id: worker_idx,
                                temp_bin_file_path: temp_bin_file_path,
                                logical_item_counts: logical_item_counts_for_worker,
                                bytes_written_to_temp_bin: bytes_written_by_worker,
                                total_units_processed_by_worker: total_units_count_for_worker,
                            };
                            if result_sender_for_worker.send(worker_final_result).is_err() {
                                eprintln!("[Worker {}] Failed to send result, coordinator channel likely closed.", worker_idx);
                            }
                        }));
                    }

                    loop {
                        tokio::select! {
                            biased;
                            Some(maybe_item_from_input) = coordinator_input_rx.recv() => {
                                if let Some(item_to_distribute) = maybe_item_from_input {
                                    let worker_idx_to_send_to = (items_distributed_by_coordinator % num_workers_for_coord as u64) as usize;
                                    if worker_task_senders[worker_idx_to_send_to].send(Some(item_to_distribute)).await.is_err() {
                                        eprintln!("[Coordinator] Failed to send item to worker {}. Channel closed.", worker_idx_to_send_to);
                                    }
                                    items_distributed_by_coordinator += 1;
                                } else { 
                                    mp_for_coord.println(format!("[Coordinator] End signal received from main pipeline. Signaling {} workers to stop.", worker_task_senders.len())).unwrap_or_default();
                                    for (idx, sender) in worker_task_senders.iter().enumerate() {
                                        if sender.send(None).await.is_err() { 
                                            eprintln!("[Coordinator] Failed to send stop signal to worker {}", idx);
                                        }
                                    }
                                    break; 
                                }
                            }
                            else => { 
                                mp_for_coord.println(format!("[Coordinator] Input channel closed unexpectedly. Signaling {} workers to stop.", worker_task_senders.len())).unwrap_or_default();
                                 for (idx, sender) in worker_task_senders.iter().enumerate() {
                                    if sender.send(None).await.is_err() {
                                        eprintln!("[Coordinator] Failed to send stop signal to worker {} on unexpected close.", idx);
                                    }
                                }
                                break;
                            }
                        }
                    }

                    mp_for_coord.println(format!("[Coordinator] Waiting for all {} worker tasks to complete...", worker_join_handles.len())).unwrap_or_default();
                    join_all(worker_join_handles).await;
                    mp_for_coord.println(format!("[Coordinator] All worker tasks completed. Items distributed by coordinator: {}", items_distributed_by_coordinator)).unwrap_or_default();
                });

                drop(worker_result_tx);
                Ok(items_distributed_by_coordinator)
            });

            let pb_clone_for_forwarder = processing_pb.clone();
            let forward_to_coord_handle = tokio::spawn(async move {
                while let Some(item_t_from_caller) = incoming_item_rx.recv().await {
                    let mmap_item: MmapItem<TokenUnit> = item_t_from_caller.into();
                    pb_clone_for_forwarder.inc(1);
                    if coordinator_input_tx.send(Some(mmap_item)).await.is_err() {
                        eprintln!(
                            "[MmapWriter] Failed to send item to coordinator. Coordinator likely terminated."
                        );
                        break;
                    }
                }
                if coordinator_input_tx.send(None).await.is_err() {
                    eprintln!("[MmapWriter] Failed to send end signal to coordinator.");
                }
                pb_clone_for_forwarder.finish_with_message(format!(
                    "[Writing to Temp Files] Finished processing items. Total: {}",
                    pb_clone_for_forwarder.position()
                ));
            });

            forward_to_coord_handle
                .await
                .expect("[MmapWriter] Item forwarding task to coordinator panicked");
            mp.println("[MmapWriter] Item forwarding to coordinator complete.".to_string())
                .unwrap_or_default();

            let approx_items_via_coord = match coordinator_handle.join() {
                Ok(Ok(count)) => {
                    mp.println(format!("[MmapWriter] Coordinator thread finished successfully. Approx items distributed: {}", count)).unwrap_or_default();
                    count
                }
                Ok(Err(e)) => {
                    return Err(Box::new(FrameworkError::InternalError(format!(
                        "[MmapWriter] Coordinator thread failed: {}",
                        e
                    ))));
                }
                Err(panic_err) => {
                    return Err(Box::new(FrameworkError::InternalError(format!(
                        "[MmapWriter] Coordinator thread panicked: {:?}",
                        panic_err
                    ))));
                }
            };

            let collected_worker_results: Vec<TempBinWorkerResult> = worker_result_rx.iter().collect();
            mp.println(format!(
                "[MmapWriter] Collected {} worker results.",
                collected_worker_results.len()
            ))
            .unwrap_or_default();

            if collected_worker_results.is_empty()
                && approx_items_via_coord > 0
                && self.num_processing_workers > 0
            {
                mp.println(format!(
                    "[MmapWriter] Warning: No worker results collected, but {} items were proxied by coordinator for {} workers. Check worker logic.", 
                    approx_items_via_coord, self.num_processing_workers
                )).unwrap_or_default();


                 let duration = overall_start_time.elapsed();
                 mp.println(format!(
                    "[MmapWriter Multi] Pipeline finished (no worker results) in {:?}. Approx items via coord: {}. Items from workers: 0. Tokens from workers: 0.",
                    duration,
                    approx_items_via_coord
                 )).unwrap_or_default();
                 return Ok(());
            }

            let mut final_worker_results = collected_worker_results;
            final_worker_results.sort_by_key(|r| r.worker_id);

            let total_docs_from_workers: usize = final_worker_results
                .iter()
                .map(|r| r.logical_item_counts.len())
                .sum();
            let total_tokens_from_workers: u64 = final_worker_results
                .iter()
                .map(|r| r.total_units_processed_by_worker)
                .sum();

            let (final_bin_file_path, total_bytes_in_final_bin) = self.merge_temp_bin_files(&final_worker_results, Arc::clone(&mp))?;
            

            let all_item_token_counts: Vec<u64> = final_worker_results
                .iter()
                .flat_map(|r| r.logical_item_counts.iter().map(|&count| count as u64))
                .collect();

            self.write_final_idx_file(
                &final_bin_file_path,
                &all_item_token_counts,
                Arc::clone(&mp),
            )?;
            self.cleanup_temp_bin_files(&final_worker_results, Arc::clone(&mp));

            mp.println(format!(
                "[MmapWriter Multi] Pipeline finished in {:?}. Approx items via coord: {}. Items from workers: {}. Token Units from workers: {}. Final .bin size: {:.2} MB.",
                overall_start_time.elapsed(),
                approx_items_via_coord,
                total_docs_from_workers,
                total_tokens_from_workers,
                total_bytes_in_final_bin as f64 / (1024.0 * 1024.0)
            )).unwrap_or_default();


            let total_logical_tokens: u64 = all_item_token_counts.iter().sum();
            
            let mut magic_prime: u64 = 0;
            if let Some(ctx_len) = self.context_length {
                let context_length = ctx_len as u64;
                if context_length > 0 && total_logical_tokens > context_length * 3 {
                     let n_chunk = (total_logical_tokens / context_length).saturating_sub(1);

                     for i in (0..n_chunk).rev() {
                         if i % 3 == 2 && primes::is_prime(i) {
                             magic_prime = i;
                             break;
                         }
                     }
                      mp.println(format!(
                         "[MmapWriter Multi] Total logical tokens: {}. Context length: {}. Magic prime selected: {}.",
                         total_logical_tokens, context_length, magic_prime
                     )).unwrap_or_default();
                } else {
                    mp.println(format!(
                        "[MmapWriter Multi] Skipping magic_prime calculation: total_tokens ({}) <= context_length ({}) * 3 or context_length is 0.",
                        total_logical_tokens, context_length
                    )).unwrap_or_default();
                }
            } else {
                mp.println("[MmapWriter Multi] Skipping magic_prime calculation: context_length not provided.").unwrap_or_default();
            }


            Ok(())
        }
    }
}

impl<T, TokenUnit> MmapWriter<T, TokenUnit>
where
    T: Serialize + Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>>,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    fn merge_temp_bin_files(
        &self,
        worker_results: &[TempBinWorkerResult],
        mp: Arc<MultiProgress>,
    ) -> Result<(PathBuf, u64), FrameworkError> {
        let final_bin_file_path = self
            .output_base_path
            .join(format!("{}.bin", self.output_filename));
        let mut final_bin_file_writer = BufWriter::new(
            File::create(&final_bin_file_path)?
        );
        let mut total_bytes_written_to_final_bin: u64 = 0;

        mp.println(format!(
            "[MmapWriter] Merging {} temporary .bin files into {}",
            worker_results.len(),
            final_bin_file_path.display()
        ))
        .unwrap_or_default();
        let merge_pb = mp.add(ProgressBar::new(
            worker_results
                .iter()
                .map(|r| r.bytes_written_to_temp_bin)
                .sum(),
        ));
        merge_pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] Merging temp bins [{bar:40.green/blue}] {bytes}/{total_bytes} ({eta})"
            ).unwrap().progress_chars("##-")
        );

        const FAST_COPY_BUFFER_SIZE: usize = 1024 * 1024;

        for result in worker_results {
            let temp_file = File::open(&result.temp_bin_file_path)?;

            let bytes_copied = fast_copy(temp_file, &mut final_bin_file_writer, FAST_COPY_BUFFER_SIZE)?;

            total_bytes_written_to_final_bin += bytes_copied;
            merge_pb.inc(bytes_copied);
        }

        final_bin_file_writer.flush()?;
        merge_pb.finish_with_message("Merging temporary .bin files complete.");
        Ok((final_bin_file_path, total_bytes_written_to_final_bin))
    }

    fn write_final_idx_file(
        &self,
        final_bin_path: &Path,
        all_item_token_counts: &[u64],
        mp: Arc<MultiProgress>,
    ) -> Result<(), FrameworkError> {
        let final_idx_path = final_bin_path.with_extension("idx");
        mp.println(format!(
            "[MmapWriter] Writing final .idx file to {}",
            final_idx_path.display()
        ))
        .unwrap_or_default();
        let mut idx_file_writer = BufWriter::new(
            File::create(&final_idx_path)?
        );

        let mut write_bytes = |bytes: &[u8]| -> Result<(), io::Error> {
             idx_file_writer.write_all(bytes)
        };

        if self.is_legacy_rwkv_format {
            write_bytes(LEGACY_MMAP_BINIDX_MAGIC_HDR)?;
            write_bytes(&LEGACY_MMAP_BINIDX_VERSION)?;
            write_bytes(&[LEGACY_MMAP_BINIDX_DTYPE_U16])?;
        } else {
            write_bytes(GENERIC_MMAP_MAGIC_HDR)?;
            write_bytes(&GENERIC_MMAP_VERSION)?;
            write_bytes(&(self.token_unit_type as u8).to_le_bytes())?;
            write_bytes(&(self.token_unit_len as u32).to_le_bytes())?;
        }

        let num_items = all_item_token_counts.len() as u64;
        write_bytes(&num_items.to_le_bytes())?;

        let doc_indices_len = num_items + 1;
        write_bytes(&doc_indices_len.to_le_bytes())?;

        for &token_count_u64 in all_item_token_counts {
            if token_count_u64 > u32::MAX as u64 {
                 return Err(FrameworkError::InternalError(format!(
                    "[MmapWriter] Token count {} for an item exceeds u32::MAX limit ({}). Cannot write to .idx file.",
                    token_count_u64,
                    u32::MAX
                )));
            }
            let token_count_u32 = token_count_u64 as u32;
             write_bytes(&token_count_u32.to_le_bytes())?;
        }

        let mut current_byte_offset: u64 = 0;
        let size_of_token_unit = std::mem::size_of::<TokenUnit>() as u64;
        for &token_count_u64 in all_item_token_counts {
            write_bytes(&current_byte_offset.to_le_bytes())?;

            let item_byte_size_result = token_count_u64
                .checked_mul(self.token_unit_len as u64)
                .and_then(|v| v.checked_mul(size_of_token_unit));

            let item_size_in_bytes = match item_byte_size_result {
                 Some(size) => size,
                 None => {
                     return Err(FrameworkError::InternalError(format!(
                         "[MmapWriter] Overflow calculating byte size ({} tokens * {} unit_len * {} bytes/unit) during .idx writing.",
                         token_count_u64, self.token_unit_len, size_of_token_unit
                     )));
                 }
            };
            
            current_byte_offset = match current_byte_offset.checked_add(item_size_in_bytes) {
                Some(offset) => offset,
                None => {
                    return Err(FrameworkError::InternalError(format!(
                       "[MmapWriter] Overflow calculating total offset at token count {} (adding {} bytes) during .idx writing. This indicates an extremely large dataset.", 
                       token_count_u64, item_size_in_bytes
                    )));
                }
            };
        }

         write_bytes(&current_byte_offset.to_le_bytes())?;




        for i in 0..=num_items {
            write_bytes(&(i as u64).to_le_bytes())?;
        }

        idx_file_writer.flush()?;
        mp.println(format!(
            "[MmapWriter] Finished writing .idx file (Format: {}).",
            if self.is_legacy_rwkv_format {
                "Legacy RWKV"
            } else {
                "Generic"
            }
        ))
        .unwrap_or_default();
        Ok(())
    }

    fn cleanup_temp_bin_files(
        &self,
        worker_results: &[TempBinWorkerResult],
        mp: Arc<MultiProgress>,
    ) {
        mp.println(format!(
            "[MmapWriter] Cleaning up {} temporary .bin files...",
            worker_results.len()
        ))
        .unwrap_or_default();
        for result in worker_results {
            if let Err(e) = fs::remove_file(&result.temp_bin_file_path) {
                eprintln!(
                    "[MmapWriter] Failed to remove temp file {}: {}",
                    result.temp_bin_file_path.display(),
                    e
                );
            }
        }
        mp.println("[MmapWriter] Temporary .bin files cleanup complete.")
        .unwrap_or_default();
    }
}


fn fast_copy(mut src: File, dst: &mut BufWriter<File>, buffer_size: usize) -> std::io::Result<u64> {
    let mut buf = vec![0u8; buffer_size];
    let mut total = 0;
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst.write_all(&buf[..n])?;
        total += n as u64;
    }
    Ok(total)
}
