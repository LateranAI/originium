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
use tokio::sync::Mutex as TokioMutex;
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
    num_devices: usize,
    threads_per_device: usize,
    token_unit_type: MmapTokenUnitType,
    token_unit_len: usize,
    is_legacy_rwkv_format: bool,
    context_length: Option<usize>,
    _phantom_t: PhantomData<T>,
    _phantom_token_unit: PhantomData<TokenUnit>,
}

#[async_trait]
impl<T, TokenUnit> Writer<T> for MmapWriter<T, TokenUnit>
where
    T: Serialize + Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>>,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    async fn pipeline(
        &self,
        incoming_item_rx: TokioReceiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        mp.println(format!(
            "[MmapWriter] Pipeline started. Output: '{}/{}_device_*'. Devices: {}, Threads/Device: {}. Format: {}, UnitType: {:?}, UnitLen: {}",
            self.output_base_path.display(), self.output_filename, self.num_devices, self.threads_per_device,
            if self.is_legacy_rwkv_format { "Legacy RWKV" } else { "Generic" }, self.token_unit_type, self.token_unit_len
        )).unwrap_or_default();
        let overall_start_time = Instant::now();

        if self.num_devices == 1 {
            mp.println("[MmapWriter] Mode: Single Device (Device 0).").unwrap_or_default();
            let (mmap_item_tx, mmap_item_rx): (TokioSender<MmapItem<TokenUnit>>, TokioReceiver<MmapItem<TokenUnit>>)= channel(self.threads_per_device * 2 + 10);
            
            let conversion_task_handle = tokio::spawn(async move {
                let mut original_rx = incoming_item_rx;
                while let Some(original_item) = original_rx.recv().await {
                    let mmap_item: MmapItem<TokenUnit> = original_item.into();
                    if mmap_item_tx.send(mmap_item).await.is_err() {
                        eprintln!("[MmapWriter SingleDevice] Failed to send MmapItem to processing channel. Receiver dropped.");
                        break;
                    }
                }
            });

            self.process_single_device_output(0, mmap_item_rx, Arc::clone(&mp)).await?;
            conversion_task_handle.await.map_err(|e| Box::new(FrameworkError::InternalError(format!("Conversion task panicked: {:?}",e))) as Box<dyn std::error::Error + Send + Sync + 'static>)?;
        
        } else {
            mp.println(format!(
                "[MmapWriter] Mode: Multi-Device ({} devices), {} Threads/Device. Using competitive pulling.", 
                self.num_devices, self.threads_per_device
            )).unwrap_or_default();

            let shared_original_item_rx = Arc::new(TokioMutex::new(incoming_item_rx));
            let mut device_task_handles = Vec::new();

            for device_id in 0..self.num_devices {
                let task_shared_rx = Arc::clone(&shared_original_item_rx);
                let task_mp = Arc::clone(&mp);
                let task_output_base_path = self.output_base_path.clone();
                let task_output_filename = self.output_filename.clone();
                let task_threads_per_device = self.threads_per_device;
                let task_token_unit_type = self.token_unit_type;
                let task_token_unit_len = self.token_unit_len;
                let task_is_legacy_format = self.is_legacy_rwkv_format;
                let task_context_length = self.context_length;

                device_task_handles.push(tokio::spawn(async move {
                    run_device_writer_task::<T, TokenUnit>(
                        device_id,
                        task_shared_rx,
                        task_mp,
                        task_output_base_path,
                        task_output_filename,
                        task_threads_per_device,
                        task_token_unit_type,
                        task_token_unit_len,
                        task_is_legacy_format,
                        task_context_length,
                    ).await
                }));
            }

            for (idx, handle) in device_task_handles.into_iter().enumerate() {
                match handle.await {
                    Ok(Ok(())) => mp.println(format!("[MmapWriter] Device {} processing completed successfully.", idx)).unwrap_or_default(),
                    Ok(Err(e)) => {
                        mp.println(format!("[MmapWriter] Device {} processing failed: {}", idx, e)).unwrap_or_default();
                        return Err(e);
                    }
                    Err(e) => {
                        mp.println(format!("[MmapWriter] Device {} task panicked: {:?}", idx, e)).unwrap_or_default();
                        return Err(Box::new(FrameworkError::InternalError(format!("Device {} task panicked: {:?}", idx, e))));
                    }
                }
            }
        }
        mp.println(format!("[MmapWriter] Pipeline finished in {:?}.", overall_start_time.elapsed())).unwrap_or_default();
        Ok(())
    }
}

async fn run_device_writer_task<T, TokenUnit>(
    device_id: usize,
    shared_original_item_rx_arc_mutex: Arc<TokioMutex<TokioReceiver<T>>>,
    mp: Arc<MultiProgress>,
    output_base_path: PathBuf,
    output_filename: String,
    threads_per_device: usize,
    token_unit_type: MmapTokenUnitType,
    token_unit_len: usize,
    is_legacy_rwkv_format: bool,
    context_length: Option<usize>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> 
where
    T: Send + Sync + 'static + Debug + Into<MmapItem<TokenUnit>> + Serialize,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    let channel_capacity = (threads_per_device * 2).max(10);
    let (device_specific_mmap_item_tx, device_specific_mmap_item_rx): (TokioSender<MmapItem<TokenUnit>>, TokioReceiver<MmapItem<TokenUnit>>) = channel(channel_capacity);

    let mp_clone_fetch = Arc::clone(&mp);
    let data_fetch_task = tokio::spawn(async move {
        loop {
            let mut locked_rx = shared_original_item_rx_arc_mutex.lock().await;
            match locked_rx.recv().await {
                Some(original_item) => {
                    let mmap_item: MmapItem<TokenUnit> = original_item.into();
                    if device_specific_mmap_item_tx.send(mmap_item).await.is_err() {
                        mp_clone_fetch.println(format!("[Device {} Fetch] Failed to send MmapItem to internal channel. Processor task likely exited.", device_id)).unwrap_or_default();
                        break;
                    }
                }
                None => {
                    mp_clone_fetch.println(format!("[Device {} Fetch] Shared original item receiver closed. Stopping fetch.", device_id)).unwrap_or_default();
                    break;
                }
            }
        }
        Result::<(), ()>::Ok(())
    });

    let temp_writer_for_processing = MmapWriter {
        output_base_path,
        output_filename,
        num_devices: 1,
        threads_per_device,
        token_unit_type,
        token_unit_len,
        is_legacy_rwkv_format,
        context_length,
        _phantom_t: PhantomData::<T>,
        _phantom_token_unit: PhantomData::<TokenUnit>,
    };

    let processing_result = temp_writer_for_processing.process_single_device_output(device_id, device_specific_mmap_item_rx, mp).await;
    
    match data_fetch_task.await {
        Ok(Ok(())) => { /* Fetch task completed cleanly */ }
        Ok(Err(_)) => { /* Fetch task had an internal error, already logged by task */ }
        Err(join_error) => {
            eprintln!("[Device {}] Data fetch task panicked: {:?}", device_id, join_error);
        }
    }
    processing_result
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
            num_devices,
            threads_per_device,
            token_unit_type,
            token_unit_len,
            is_legacy_rwkv_format,
            context_length,
        } = endpoint_config
        {
            if *num_devices == 0 {
                panic!("[MmapWriter::new] num_devices cannot be zero.");
            }
            if *threads_per_device == 0 {
                panic!("[MmapWriter::new] threads_per_device cannot be zero.");
            }
            if *token_unit_len == 0 {
                panic!("[MmapWriter::new] token_unit_len cannot be zero.");
            }
            if *is_legacy_rwkv_format {
                assert_eq!(*token_unit_type, MmapTokenUnitType::U16, "Legacy RWKV must use U16 tokens.");
                assert_eq!(*token_unit_len, 1, "Legacy RWKV format must have token_unit_len of 1.");
            }
            fs::create_dir_all(base_path).unwrap_or_else(|e| panic!("[MmapWriter::new] Failed to create output base directory '{}': {}. This is critical.", base_path, e));
            Self {
                output_base_path: PathBuf::from(base_path),
                output_filename: filename.clone(),
                num_devices: *num_devices,
                threads_per_device: *threads_per_device,
                token_unit_type: *token_unit_type,
                token_unit_len: *token_unit_len,
                is_legacy_rwkv_format: *is_legacy_rwkv_format,
                context_length: *context_length,
                _phantom_t: PhantomData,
                _phantom_token_unit: PhantomData,
            }
        } else {
            panic!("[MmapWriter::new] Incorrect DataEndpoint variant provided. Expected DataEndpoint::Mmap.");
        }
    }

    async fn process_single_device_output(
        &self,
        device_id: usize,
        mut incoming_mmap_item_rx: TokioReceiver<MmapItem<TokenUnit>>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let overall_start_time_device = Instant::now();
        let device_output_filename_base = format!("{}_device_{}", self.output_filename, device_id);
        mp.println(format!(
            "[MmapWriter Device {}] Processing started. Output: {}. Threads: {}.",
            device_id, device_output_filename_base, self.threads_per_device
        )).unwrap_or_default();

        if self.threads_per_device == 1 {
            mp.println(format!("[MmapWriter Device {}] Single-thread mode.", device_id)).unwrap_or_default();
            let processing_pb = mp.add(ProgressBar::new(0));
            processing_pb.enable_steady_tick(Duration::from_millis(120));
            let pb_template = format!("[Device {} {{elapsed_precise}}] [DirectWrite] {{wide_bar:.cyan/blue}} {{pos}} items ({{per_sec}}, ETA: {{eta}})", device_id);
            processing_pb.set_style(
                ProgressStyle::with_template(&pb_template.replace("{", "{{").replace("}", "}}"))
                    .unwrap()
                    .progress_chars("##-"),
            );
            let final_bin_file_path = self.output_base_path.join(format!("{}.bin", device_output_filename_base));
            let mut final_bin_file_writer = BufWriter::new(File::create(&final_bin_file_path)?);
            let mut logical_item_counts: Vec<u64> = Vec::new();
            let mut total_bytes_written: u64 = 0;
            let mut total_units_processed: u64 = 0;
            let mut items_processed_count: u64 = 0;

            while let Some(mmap_item) = incoming_mmap_item_rx.recv().await {
                processing_pb.inc(1);
                items_processed_count += 1;
                if mmap_item.tokens.is_empty() {
                    logical_item_counts.push(0);
                    continue;
                }
                if mmap_item.tokens.len() % self.token_unit_len != 0 {
                    return Err(Box::new(FrameworkError::InternalError(format!(
                        "[Device {}] MmapItem with {} units, not divisible by unit_len {}. Error.", 
                        device_id, mmap_item.tokens.len(), self.token_unit_len
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
            processing_pb.finish_with_message(format!("[Device {} DirectWrite] Finished. Total: {}", device_id, processing_pb.position()));
            self.write_device_idx_file(device_id, &final_bin_file_path, &logical_item_counts, Arc::clone(&mp))?;
            mp.println(format!(
                "[MmapWriter Device {} ST] Finished in {:?}. Items: {}. Units: {}. Size: {:.2}MB.",
                device_id, overall_start_time_device.elapsed(), items_processed_count, total_units_processed, total_bytes_written as f64 / (1024.0 * 1024.0)
            )).unwrap_or_default();
            let total_logical_tokens_for_device: u64 = logical_item_counts.iter().sum();
            if let Some(ctx_len) = self.context_length {
                let context_length = ctx_len as u64;
                if context_length > 0 && total_logical_tokens_for_device > context_length * 3 {
                    let n_chunk = (total_logical_tokens_for_device / context_length).saturating_sub(1);
                    let mut magic_prime_device: u64 = 0;
                    for i in (0..n_chunk).rev() { if i % 3 == 2 && primes::is_prime(i) { magic_prime_device = i; break; } }
                    mp.println(format!("[Device {} ST] Magic prime: {}. (Tokens: {}, Context: {})", device_id, magic_prime_device, total_logical_tokens_for_device, context_length)).unwrap_or_default();
                }
            }
            Ok(())
        } else {
            let processing_pb = mp.add(ProgressBar::new(0));
            processing_pb.enable_steady_tick(Duration::from_millis(120));
            let pb_template = format!("[Device {} {{elapsed_precise}}] [TempWrite] {{wide_bar:.yellow/blue}} {{pos}} items ({{per_sec}}, ETA: {{eta}})", device_id);
            processing_pb.set_style(
                ProgressStyle::with_template(&pb_template.replace("{", "{{").replace("}", "}}"))
                    .unwrap()
                    .progress_chars("=> "),
            );
            let (coordinator_input_tx, mut coordinator_input_rx): (TokioSender<Option<MmapItem<TokenUnit>>>, TokioReceiver<Option<MmapItem<TokenUnit>>>) = channel(self.threads_per_device * 2);
            let (worker_result_tx, worker_result_rx): (StdSender<TempBinWorkerResult>, StdReceiver<TempBinWorkerResult>) = mpsc::channel();
            let num_workers_for_coord = self.threads_per_device;
            let filename_for_coord = device_output_filename_base.clone();
            let base_path_for_coord = self.output_base_path.clone();
            let token_unit_len_for_worker = self.token_unit_len;
            let mp_for_coord = Arc::clone(&mp);
            let device_id_for_coord_thread = device_id;

            let coordinator_handle = thread::spawn(move || -> Result<u64, String> {
                let rt = tokio::runtime::Runtime::new().map_err(|e| format!("[Device {} Coord] Failed Tokio RT: {}", device_id_for_coord_thread, e))?;
                let mut items_distributed: u64 = 0;
                rt.block_on(async {
                    let mut worker_handles = Vec::with_capacity(num_workers_for_coord);
                    let mut worker_senders = Vec::with_capacity(num_workers_for_coord);
                    for worker_idx in 0..num_workers_for_coord {
                        let (tx_to_worker_task, mut rx_for_worker_task): (TokioSender<Option<MmapItem<TokenUnit>>>, TokioReceiver<Option<MmapItem<TokenUnit>>>) = channel(100);
                        worker_senders.push(tx_to_worker_task);
                        let result_sender_for_worker_task = worker_result_tx.clone();
                        let temp_file_base_for_worker_task = base_path_for_coord.join(format!("{}_temp_worker_{}", filename_for_coord, worker_idx));
                        let current_token_unit_len_for_task = token_unit_len_for_worker;
                        let current_device_id_for_task = device_id_for_coord_thread;
                        worker_handles.push(tokio::spawn(async move {
                            let temp_bin_file_path_task = temp_file_base_for_worker_task.with_extension("bin.tmp");
                            let mut temp_bin_writer_task = BufWriter::new(File::create(&temp_bin_file_path_task).unwrap_or_else(|e| panic!("[D{}W{}] Failed create tmp '{}': {}", current_device_id_for_task, worker_idx, temp_bin_file_path_task.display(), e)));
                            let mut logical_counts_task = Vec::new(); let mut bytes_written_task = 0; let mut units_processed_task = 0;
                            while let Some(Some(mmap_item_in_task)) = rx_for_worker_task.recv().await {
                                if mmap_item_in_task.tokens.is_empty() { logical_counts_task.push(0); continue; }
                                if mmap_item_in_task.tokens.len() % current_token_unit_len_for_task != 0 { panic!("[D{}W{}] Item units {} not div by unit_len {}. Error.", current_device_id_for_task, worker_idx, mmap_item_in_task.tokens.len(), current_token_unit_len_for_task); }
                                let num_logical_for_item_task = (mmap_item_in_task.tokens.len() / current_token_unit_len_for_task) as u32;
                                if num_logical_for_item_task as usize * current_token_unit_len_for_task != mmap_item_in_task.tokens.len() { panic!("[D{}W{}] Overflow u32 logical tokens.", current_device_id_for_task, worker_idx); }
                                logical_counts_task.push(num_logical_for_item_task); units_processed_task += mmap_item_in_task.tokens.len() as u64;
                                let slice_task = bytemuck::cast_slice(&mmap_item_in_task.tokens);
                                temp_bin_writer_task.write_all(slice_task).unwrap_or_else(|e| panic!("[D{}W{}] Failed write tmp '{}': {}", current_device_id_for_task, worker_idx, temp_bin_file_path_task.display(), e));
                                bytes_written_task += slice_task.len() as u64;
                            }
                            temp_bin_writer_task.flush().unwrap_or_else(|e| panic!("[D{}W{}] Failed flush tmp '{}': {}", current_device_id_for_task, worker_idx, temp_bin_file_path_task.display(), e));
                            if result_sender_for_worker_task.send(TempBinWorkerResult{worker_id: worker_idx, temp_bin_file_path: temp_bin_file_path_task, logical_item_counts: logical_counts_task, bytes_written_to_temp_bin: bytes_written_task, total_units_processed_by_worker: units_processed_task}).is_err(){
                                eprintln!("[D{}W{}] Failed send result, coord likely closed.", current_device_id_for_task, worker_idx);
                            }
                        }));
                    }
                    loop {
                        tokio::select! {
                            biased;
                            Some(opt_mmap_item_for_coord) = coordinator_input_rx.recv() => {
                                if let Some(mmap_item_for_dist) = opt_mmap_item_for_coord {
                                    let target_worker_idx = (items_distributed % num_workers_for_coord as u64) as usize;
                                    if worker_senders[target_worker_idx].send(Some(mmap_item_for_dist)).await.is_err() { eprintln!("[D{} Coord] Failed send to Worker {}. Channel closed.", device_id_for_coord_thread, target_worker_idx);}
                                    items_distributed += 1;
                                } else { 
                                    mp_for_coord.println(format!("[D{} Coord] End signal received. Signaling {} workers to stop.", device_id_for_coord_thread, worker_senders.len())).unwrap_or_default();
                                    for (idx_stop, sender_stop) in worker_senders.iter().enumerate() { if sender_stop.send(None).await.is_err() { eprintln!("[D{} Coord] Failed stop signal to Worker {}.", device_id_for_coord_thread, idx_stop);}}
                                    break; 
                                }
                            }
                            else => { 
                                mp_for_coord.println(format!("[D{} Coord] Input channel closed unexpectedly. Signaling {} workers.", device_id_for_coord_thread, worker_senders.len())).unwrap_or_default();
                                for (idx_stop_unexpected, sender_stop_unexpected) in worker_senders.iter().enumerate() { if sender_stop_unexpected.send(None).await.is_err() { eprintln!("[D{} Coord] Failed stop signal to Worker {} on unexpected close.", device_id_for_coord_thread, idx_stop_unexpected);}}
                                break;
                            }
                        }
                    }
                    mp_for_coord.println(format!("[D{} Coord] Waiting for all {} worker tasks to complete...", device_id_for_coord_thread, worker_handles.len())).unwrap_or_default();
                    join_all(worker_handles).await;
                    mp_for_coord.println(format!("[D{} Coord] All worker tasks completed. Items distributed: {}", device_id_for_coord_thread, items_distributed)).unwrap_or_default();
                });
                drop(worker_result_tx);
                Ok(items_distributed)
            });

            let pb_clone_for_forwarder = processing_pb.clone(); 
            let local_device_id_for_fwd = device_id;
            let forward_to_coord_handle = tokio::spawn(async move {
                let mut local_incoming_mmap_item_rx = incoming_mmap_item_rx;
                while let Some(mmap_item) = local_incoming_mmap_item_rx.recv().await {
                    pb_clone_for_forwarder.inc(1);
                    if coordinator_input_tx.send(Some(mmap_item)).await.is_err() {
                        eprintln!("[D{} FwdToCoord] Failed to send item to its coordinator. Coordinator likely terminated.", local_device_id_for_fwd);
                        break;
                    }
                }
                if coordinator_input_tx.send(None).await.is_err() { 
                    eprintln!("[D{} FwdToCoord] Failed to send end signal to its coordinator.", local_device_id_for_fwd);
                }
                pb_clone_for_forwarder.finish_with_message(format!("[D{} TempWrite] Finished processing items. Total: {}", local_device_id_for_fwd, pb_clone_for_forwarder.position()));
            });

            forward_to_coord_handle.await.map_err(|e| Box::new(FrameworkError::InternalError(format!("[D{}] Item forwarding task to coordinator panicked: {:?}", device_id, e))) as Box<dyn std::error::Error + Send + Sync + 'static>)?;
            mp.println(format!("[D{}] Item forwarding to its coordinator complete.", device_id)).unwrap_or_default();
            let approx_items_via_coord = match coordinator_handle.join() {
                Ok(Ok(count)) => { mp.println(format!("[D{}] Coordinator thread finished. Approx items distributed: {}", device_id, count)).unwrap_or_default(); count }
                Ok(Err(e)) => return Err(Box::new(FrameworkError::InternalError(format!("[D{}] Coordinator thread failed: {}", device_id, e)))),
                Err(panic_err) => return Err(Box::new(FrameworkError::InternalError(format!("[D{}] Coordinator thread panicked: {:?}", device_id, panic_err)))),
            };
            let collected_worker_results: Vec<TempBinWorkerResult> = worker_result_rx.iter().collect();
            mp.println(format!("[D{}] Collected {} worker results.", device_id, collected_worker_results.len())).unwrap_or_default();
            if collected_worker_results.is_empty() && approx_items_via_coord > 0 && self.threads_per_device > 0 {
                mp.println(format!("[D{}] Warning: No worker results, but {} items proxied. Check worker logic or if items were empty.", device_id, approx_items_via_coord)).unwrap_or_default();
                let final_bin_file_path_empty = self.output_base_path.join(format!("{}.bin", device_output_filename_base));
                File::create(&final_bin_file_path_empty)?.flush()?;
                self.write_device_idx_file(device_id, &final_bin_file_path_empty, &[], Arc::clone(&mp))?;
                return Ok(());
            }
            let mut final_worker_results = collected_worker_results; final_worker_results.sort_by_key(|r| r.worker_id);
            let total_docs_from_workers: usize = final_worker_results.iter().map(|r| r.logical_item_counts.len()).sum();
            let total_tokens_from_workers: u64 = final_worker_results.iter().map(|r| r.total_units_processed_by_worker).sum();
            let (final_bin_file_path_merged, total_bytes_in_final_bin_merged) = self.merge_device_temp_bin_files(device_id, &device_output_filename_base, &final_worker_results, Arc::clone(&mp))?;
            let all_item_token_counts_merged: Vec<u64> = final_worker_results.iter().flat_map(|r| r.logical_item_counts.iter().map(|&count| count as u64)).collect();
            self.write_device_idx_file(device_id, &final_bin_file_path_merged, &all_item_token_counts_merged, Arc::clone(&mp))?;
            self.cleanup_device_temp_bin_files(device_id, &final_worker_results, Arc::clone(&mp));
            mp.println(format!(
                "[D{} MT] Finished in {:?}. Items via coord: {}. Items from workers: {}. Tokens from workers: {}. Final .bin size: {:.2}MB.",
                device_id, overall_start_time_device.elapsed(), approx_items_via_coord, total_docs_from_workers, total_tokens_from_workers, total_bytes_in_final_bin_merged as f64 / (1024.0 * 1024.0)
            )).unwrap_or_default();
            let total_logical_tokens_for_device_mt: u64 = all_item_token_counts_merged.iter().sum();
            if let Some(ctx_len_mt) = self.context_length {
                let context_length_mt = ctx_len_mt as u64;
                if context_length_mt > 0 && total_logical_tokens_for_device_mt > context_length_mt * 3 {
                    let n_chunk_mt = (total_logical_tokens_for_device_mt / context_length_mt).saturating_sub(1);
                    let mut magic_prime_device_mt: u64 = 0;
                    for i in (0..n_chunk_mt).rev() { if i % 3 == 2 && primes::is_prime(i) { magic_prime_device_mt = i; break; } }
                    mp.println(format!("[D{} MT] Magic prime: {}. (Total logical tokens: {}, Context: {})", device_id, magic_prime_device_mt, total_logical_tokens_for_device_mt, context_length_mt)).unwrap_or_default();
                }
            }
            Ok(())
        }
    }

    fn merge_device_temp_bin_files(
        &self,
        device_id: usize,
        device_output_filename_base: &str, 
        worker_results: &[TempBinWorkerResult],
        mp: Arc<MultiProgress>,
    ) -> Result<(PathBuf, u64), FrameworkError> {
        let final_bin_file_path = self.output_base_path.join(format!("{}.bin", device_output_filename_base)); 
        let mut final_bin_file_writer = BufWriter::new(File::create(&final_bin_file_path)?);
        let mut total_bytes_written_to_final_bin: u64 = 0;
        mp.println(format!("[D{}] Merging {} temp .bin files into {}", device_id, worker_results.len(), final_bin_file_path.display())).unwrap_or_default();
        let merge_pb = mp.add(ProgressBar::new(worker_results.iter().map(|r| r.bytes_written_to_temp_bin).sum()));
        let pb_template = format!("[D{} {{elapsed_precise}}] Merging temp bins [{{bar:40.green/blue}}] {{bytes}}/{{total_bytes}} ({{eta}})", device_id);
        merge_pb.set_style(ProgressStyle::with_template(&pb_template.replace("{","{{").replace("}","}}")).unwrap().progress_chars("##-"));
        const FAST_COPY_BUFFER_SIZE: usize = 1024 * 1024;
        for result in worker_results {
            let temp_file = File::open(&result.temp_bin_file_path)?;
            let bytes_copied = fast_copy(temp_file, &mut final_bin_file_writer, FAST_COPY_BUFFER_SIZE)?;
            total_bytes_written_to_final_bin += bytes_copied;
            merge_pb.inc(bytes_copied);
        }
        final_bin_file_writer.flush()?;
        merge_pb.finish_with_message(format!("[D{}] Merging temp .bin files complete.", device_id));
        Ok((final_bin_file_path, total_bytes_written_to_final_bin))
    }

    fn write_device_idx_file(
        &self,
        device_id: usize,
        final_bin_path: &Path, 
        all_item_token_counts: &[u64],
        mp: Arc<MultiProgress>,
    ) -> Result<(), FrameworkError> {
        let final_idx_path = final_bin_path.with_extension("idx");
        mp.println(format!("[D{}] Writing final .idx file to {}", device_id, final_idx_path.display())).unwrap_or_default();
        let mut idx_file_writer = BufWriter::new(File::create(&final_idx_path)?);
        let mut write_bytes = |bytes: &[u8]| -> Result<(), io::Error> { idx_file_writer.write_all(bytes) };

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
                    "[D{}] Token count {} exceeds {}. Cannot write to .idx.", device_id, token_count_u64, u32::MAX
                )));
            }
            let token_count_u32 = token_count_u64 as u32;
             write_bytes(&token_count_u32.to_le_bytes())?;
        }
        let mut current_byte_offset: u64 = 0;
        let size_of_token_unit = std::mem::size_of::<TokenUnit>() as u64;
        for &token_count_u64 in all_item_token_counts {
            write_bytes(&current_byte_offset.to_le_bytes())?;
            let item_byte_size_result = token_count_u64.checked_mul(self.token_unit_len as u64).and_then(|v| v.checked_mul(size_of_token_unit));
            let item_size_in_bytes = match item_byte_size_result {
                 Some(size) => size,
                 None => return Err(FrameworkError::InternalError(format!(
                     "[D{}] Overflow calc byte size ({} toks * {} ulen * {} bytes/u) for .idx.",
                     device_id, token_count_u64, self.token_unit_len, size_of_token_unit))),
            };
            current_byte_offset = match current_byte_offset.checked_add(item_size_in_bytes) {
                Some(offset) => offset,
                None => return Err(FrameworkError::InternalError(format!(
                   "[D{}] Overflow calc total offset at token count {} (add {} bytes) for .idx.", 
                   device_id, token_count_u64, item_size_in_bytes))),
            };
        }
        write_bytes(&current_byte_offset.to_le_bytes())?;
        for i in 0..=num_items {
            write_bytes(&(i as u64).to_le_bytes())?;
        }
        idx_file_writer.flush()?;
        mp.println(format!("[D{}] Finished .idx file (Format: {}).", device_id, if self.is_legacy_rwkv_format { "Legacy RWKV" } else { "Generic" })).unwrap_or_default();
        Ok(())
    }

    fn cleanup_device_temp_bin_files(&self, device_id: usize, worker_results: &[TempBinWorkerResult], mp: Arc<MultiProgress>) {
        mp.println(format!("[D{}] Cleaning up {} temp .bin files...", device_id, worker_results.len())).unwrap_or_default();
        for result in worker_results {
            if let Err(e) = fs::remove_file(&result.temp_bin_file_path) {
                eprintln!("[D{}] Failed to remove temp file {}: {}", device_id, result.temp_bin_file_path.display(), e);
            }
        }
        mp.println(format!("[D{}] Temp .bin files cleanup complete.", device_id)).unwrap_or_default();
    }
}

fn fast_copy(mut src: File, dst: &mut BufWriter<File>, buffer_size: usize) -> std::io::Result<u64> {
    let mut buf = vec![0u8; buffer_size];
    let mut total = 0;
    loop {
        let n = src.read(&mut buf)?;
        if n == 0 { break; }
        dst.write_all(&buf[..n])?;
        total += n as u64;
    }
    Ok(total)
}
