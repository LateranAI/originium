use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memchr::memchr_iter;
use num_cpus;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use tokio::sync::mpsc;

use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader as TokioBufReader};

use crate::custom_tasks::LineFormat;

const JSONL_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "JsonlReader",
    scan_progress_template: "[{elapsed_precise}] [{reader_name} Scan] {bar:40.cyan/blue} {percent:>3}% ({bytes}/{total_bytes}) {bytes_per_sec}, ETA: {eta}",
    process_progress_template: "[{elapsed_precise}] [{reader_name} Process] {bar:40.green/blue} {percent:>3}% ({pos}/{len}) {per_sec}, ETA: {eta}",
};

const TSV_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "TsvReader",
    scan_progress_template: "[{elapsed_precise}] [{reader_name} Scan] {bar:40.cyan/blue} {percent:>3}% ({bytes}/{total_bytes}) {bytes_per_sec}, ETA: {eta}",
    process_progress_template: "[{elapsed_precise}] [{reader_name} Process] {bar:40.green/blue} {percent:>3}% ({pos}/{len}) {per_sec}, ETA: {eta}",
};

const PLAINTEXT_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "PlainTextReader",
    scan_progress_template: "[{elapsed_precise}] [{reader_name} Scan] {bar:40.cyan/blue} {percent:>3}% ({bytes}/{total_bytes}) {bytes_per_sec}, ETA: {eta}",
    process_progress_template: "[{elapsed_precise}] [{reader_name} Process] {bar:40.green/blue} {percent:>3}% ({pos}/{len}) {per_sec}, ETA: {eta}",
};

pub struct LineReader {
    path: String,
    num_threads: usize,
    config: Arc<LineReaderConfig>,
}

impl LineReader {
    pub fn new(path: String, format: LineFormat) -> Self {
        let num_threads = num_cpus::get();

        let selected_config = match format {
            LineFormat::Jsonl => JSONL_READER_CONFIG,
            LineFormat::Tsv => TSV_READER_CONFIG,
            LineFormat::PlainText => PLAINTEXT_READER_CONFIG,
        };

        eprintln!(
            "[{}] Using {} threads for parallel reading file: {}",
            selected_config.reader_type_name, num_threads, path
        );
        Self {
            path,
            num_threads,
            config: Arc::new(selected_config),
        }
    }
}

#[async_trait]
impl<Item> Reader<Item> for LineReader
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        pipeline_core(
            self.path.clone(),
            self.num_threads,
            read_fn,
            Arc::clone(&self.config),
            mp,
        )
        .await
    }
}

pub struct LineReaderConfig {
    pub reader_type_name: &'static str,
    pub scan_progress_template: &'static str,
    pub process_progress_template: &'static str,
}

fn open_std_file_for_scan(
    file_path_str: &str,
    reader_type_name: &'static str,
) -> std::io::Result<std::fs::File> {
    std::fs::File::open(file_path_str).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "[{}] Failed to open file in scan thread (path: {}): {}",
                reader_type_name, file_path_str, e
            ),
        )
    })
}

pub fn scan_line_offsets_core(
    file_path_str: &str,
    num_threads: usize,
    file_size: u64,
    config: &LineReaderConfig,
    mp: Arc<MultiProgress>,
) -> Arc<Vec<u64>> {
    let pb_scan = mp.add(ProgressBar::new(file_size));
    let scan_template = config.scan_progress_template.replace("{reader_name}", config.reader_type_name);
    pb_scan.set_style(
        ProgressStyle::with_template(&scan_template)
            .unwrap()
            .progress_chars("=> "),
    );
    pb_scan.enable_steady_tick(std::time::Duration::from_millis(100));

    let chunk_size = (file_size + num_threads as u64 - 1) / num_threads as u64;

    let offsets_from_threads: Vec<Vec<u64>> = (0..num_threads)
        .into_par_iter()
        .map(|i| {
            let physical_chunk_start = i as u64 * chunk_size;
            let mut physical_chunk_end = (i as u64 + 1) * chunk_size;
            if physical_chunk_end > file_size {
                physical_chunk_end = file_size;
            }

            if physical_chunk_start >= physical_chunk_end {
                pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start));
                return Vec::new();
            }

            let mut local_offsets = Vec::new();
            let mut f = match open_std_file_for_scan(file_path_str, config.reader_type_name) {
                Ok(file) => file,
                Err(e) => {
                    eprintln!("{}", e);
                    pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start));
                    return Vec::new();
                }
            };

            let mut current_pos_in_file = physical_chunk_start;
            let mut bytes_processed_in_chunk = 0u64;

            if physical_chunk_start == 0 {
                local_offsets.push(0);
            } else {
                if let Err(e) = f.seek(SeekFrom::Start(physical_chunk_start)) {
                    eprintln!(
                        "[{}] Seek failed in scan thread (path: {}, offset: {}): {}",
                        config.reader_type_name, file_path_str, physical_chunk_start, e
                    );
                    pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start));
                    return Vec::new();
                }

                let mut buffer = [0; 1024];
                loop {
                    if current_pos_in_file >= physical_chunk_end {
                        bytes_processed_in_chunk = physical_chunk_end - physical_chunk_start;
                        current_pos_in_file = physical_chunk_end;
                        break;
                    }
                    match f.read(&mut buffer) {
                        Ok(0) => {
                            bytes_processed_in_chunk = physical_chunk_end - physical_chunk_start;
                            current_pos_in_file = physical_chunk_end;
                            break;
                        }
                        Ok(n) => {
                            let bytes_in_buffer_to_check =
                                n.min((physical_chunk_end - current_pos_in_file) as usize);
                            if let Some(newline_idx_in_buffer) =
                                memchr::memchr(b'\n', &buffer[..bytes_in_buffer_to_check])
                            {
                                current_pos_in_file += (newline_idx_in_buffer + 1) as u64;
                                bytes_processed_in_chunk =
                                    current_pos_in_file - physical_chunk_start;
                                break;
                            } else {
                                current_pos_in_file += bytes_in_buffer_to_check as u64;
                                if current_pos_in_file >= physical_chunk_end {
                                    bytes_processed_in_chunk =
                                        physical_chunk_end - physical_chunk_start;
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "[{}] Error reading during offset adjustment (path: {}): {}",
                                config.reader_type_name, file_path_str, e
                            );
                            pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start));
                            return Vec::new();
                        }
                    }
                }
            }

            if current_pos_in_file >= physical_chunk_end {
                pb_scan.inc(
                    bytes_processed_in_chunk
                        .min(physical_chunk_end.saturating_sub(physical_chunk_start)),
                );
                return local_offsets;
            }

            if let Err(e) = f.seek(SeekFrom::Start(current_pos_in_file)) {
                eprintln!(
                    "[{}] Seek failed before reading effective chunk (path: {}, offset: {}): {}",
                    config.reader_type_name, file_path_str, current_pos_in_file, e
                );
                pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start).saturating_sub(bytes_processed_in_chunk));
                return local_offsets;
            }

            pb_scan.inc(bytes_processed_in_chunk);

            let mut effective_offset_scan_pos = current_pos_in_file;
            let mut bytes_left_to_scan_for_offsets = physical_chunk_end.saturating_sub(effective_offset_scan_pos);
            let mut temp_read_buffer = vec![0u8; 64 * 1024]; // 64KB reusable buffer

            while bytes_left_to_scan_for_offsets > 0 {
                let bytes_to_read_this_iteration = temp_read_buffer.len().min(bytes_left_to_scan_for_offsets as usize);
                if bytes_to_read_this_iteration == 0 { 
                    break;
                }

                match f.read(&mut temp_read_buffer[..bytes_to_read_this_iteration]) {
                    Ok(0) => { // EOF reached unexpectedly in the middle of our expected chunk.
                        pb_scan.inc(bytes_left_to_scan_for_offsets); // Count remaining as processed
                        bytes_left_to_scan_for_offsets = 0; // To exit loop
                        break;
                    }
                    Ok(bytes_actually_read_iter) => {
                        if bytes_actually_read_iter == 0 { // Should be caught by Ok(0), but double-check.
                            pb_scan.inc(bytes_left_to_scan_for_offsets);
                            bytes_left_to_scan_for_offsets = 0;
                            break;
                        }

                        for idx in memchr_iter(b'\n', &temp_read_buffer[..bytes_actually_read_iter]) {
                            let offset_in_file = effective_offset_scan_pos + idx as u64 + 1;
                            if offset_in_file < physical_chunk_end {
                                local_offsets.push(offset_in_file);
                            } else {
                                break; // Newline is beyond this thread's chunk
                            }
                        }
                        
                        effective_offset_scan_pos += bytes_actually_read_iter as u64;
                        pb_scan.inc(bytes_actually_read_iter as u64);
                        bytes_left_to_scan_for_offsets = bytes_left_to_scan_for_offsets.saturating_sub(bytes_actually_read_iter as u64);
                    }
                    Err(e) => {
                        eprintln!(
                            "[{}] Error reading during buffered offset scan (path: {}, offset: {}): {}",
                            config.reader_type_name, file_path_str, effective_offset_scan_pos, e
                        );
                        pb_scan.inc(bytes_left_to_scan_for_offsets); // Count remaining as processed
                        bytes_left_to_scan_for_offsets = 0; // To exit loop
                        break;
                    }
                }
            }
            local_offsets
        })
        .collect();

    let final_scan_msg = format!(
        "[{reader_name} Scan] Complete. Found ~{count} offsets. ({elapsed})",
        reader_name = config.reader_type_name,
        count = offsets_from_threads.iter().map(|v| v.len()).sum::<usize>(),
        elapsed = format!("{:.2?}", pb_scan.elapsed())
    );
    pb_scan.finish_with_message(final_scan_msg);

    let mut combined_offsets: Vec<u64> = offsets_from_threads.into_iter().flatten().collect();

    if file_size > 0 {
        combined_offsets.sort_unstable();
        combined_offsets.dedup();

        if combined_offsets.is_empty() {
            combined_offsets.push(0);
        } else if combined_offsets[0] != 0 {
            if !combined_offsets.contains(&0) {
                combined_offsets.push(0);
            }
            combined_offsets.sort_unstable();
            combined_offsets.dedup();
        }
    } else {
        combined_offsets.clear();
    }

    Arc::new(combined_offsets)
}

pub async fn pipeline_core<Item>(
    file_path_str: String,
    num_threads: usize,
    read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
    config: Arc<LineReaderConfig>,
    mp: Arc<MultiProgress>,
) -> mpsc::Receiver<Item>
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug,
{
    let (tx, rx) = mpsc::channel::<Item>(num_threads * 2);

    let file_size = match TokioFile::open(file_path_str.clone()).await {
        Ok(file) => match file.metadata().await {
            Ok(metadata) => metadata.len(),
            Err(e) => {
                eprintln!(
                    "[{}] Failed to get metadata for {}: {}. Cannot determine file size for progress.",
                    config.reader_type_name, file_path_str, e
                );
                0
            }
        },
        Err(e) => {
            eprintln!(
                "[{}] Failed to open file {}: {}. Cannot determine file size for progress.",
                config.reader_type_name, file_path_str, e
            );
            0
        }
    };

    if file_size == 0 {
        eprintln!(
            "[{}] File size is 0 or could not be determined for {}. No lines will be processed.",
            config.reader_type_name, file_path_str
        );
        drop(tx);
        return rx;
    }

    let line_offsets = tokio::task::spawn_blocking({
        let path_clone_for_scan = file_path_str.clone();
        let config_clone_for_scan = Arc::clone(&config);
        let mp_clone_for_scan = Arc::clone(&mp);
        move || {
            scan_line_offsets_core(
                &path_clone_for_scan,
                num_threads,
                file_size,
                &config_clone_for_scan,
                mp_clone_for_scan,
            )
        }
    })
    .await
    .unwrap_or_else(|e| {
        eprintln!(
            "[{}] Panic in scan_line_offsets_core for {}: {:?}. Returning empty offsets.",
            config.reader_type_name, file_path_str, e
        );
        Arc::new(Vec::new())
    });

    if line_offsets.is_empty() && file_size > 0 {
        eprintln!(
            "[{}] No line offsets found for {}, though file size is {}. Check file content and newline characters.",
            config.reader_type_name, file_path_str, file_size
        );
    }

    let total_lines = line_offsets.len();
    let pb_process = mp.add(ProgressBar::new(total_lines as u64));
    let process_template = config.process_progress_template.replace("{reader_name}", config.reader_type_name);
    pb_process.set_style(
        ProgressStyle::with_template(&process_template)
            .unwrap()
            .progress_chars("=> "),
    );
    pb_process.enable_steady_tick(std::time::Duration::from_millis(100));

    let parser = Arc::new(read_fn);

    let mut worker_handles = vec![];

    let lines_per_thread_ideal = (total_lines + num_threads - 1) / num_threads;

    for i in 0..num_threads {
        let tx_clone = tx.clone();
        let parser_clone = Arc::clone(&parser);
        let file_path_clone_str = file_path_str.clone();
        let offsets_clone = Arc::clone(&line_offsets);
        let pb_clone = pb_process.clone();
        let reader_type_name_clone = config.reader_type_name;

        let start_line_idx_in_offsets = i * lines_per_thread_ideal;
        let mut end_line_idx_in_offsets = ((i + 1) * lines_per_thread_ideal).min(total_lines);

        if start_line_idx_in_offsets >= end_line_idx_in_offsets {
            continue;
        }

        if i == num_threads - 1 {
            end_line_idx_in_offsets = total_lines;
        }

        worker_handles.push(tokio::spawn(async move {
            let actual_start_line_index_in_offsets = start_line_idx_in_offsets;
            let actual_end_line_exclusive_index_in_offsets = end_line_idx_in_offsets;

            let start_byte_offset = offsets_clone[actual_start_line_index_in_offsets];

            let end_byte_offset =
                if actual_end_line_exclusive_index_in_offsets >= offsets_clone.len() {
                    file_size
                } else {
                    offsets_clone[actual_end_line_exclusive_index_in_offsets]
                };

            if start_byte_offset >= end_byte_offset
                && !(start_byte_offset == end_byte_offset
                    && start_byte_offset == file_size
                    && file_size == 0)
            {
                return;
            }

            let mut file_handle = match TokioFile::open(&file_path_clone_str).await {
                Ok(f) => f,
                Err(e) => {
                    pb_clone.println(format!(
                        "[{}] Worker {} Error opening {}: {}",
                        reader_type_name_clone, i, file_path_clone_str, e
                    ));
                    return;
                }
            };

            if let Err(e) = file_handle.seek(SeekFrom::Start(start_byte_offset)).await {
                pb_clone.println(format!(
                    "[{}] Worker {} Error seeking in {}: {}",
                    reader_type_name_clone, i, file_path_clone_str, e
                ));
                return;
            }

            let chunk_size_for_thread_reader = end_byte_offset - start_byte_offset;
            let targeted_chunk_reader = file_handle.take(chunk_size_for_thread_reader);
            let buf_reader_for_lines = TokioBufReader::new(targeted_chunk_reader);
            let mut lines_stream_async = buf_reader_for_lines.lines();

            for line_num_within_thread_responsibility in
                actual_start_line_index_in_offsets..actual_end_line_exclusive_index_in_offsets
            {
                match lines_stream_async.next_line().await {
                    Ok(Some(line_content_str)) => {
                        let item = parser_clone(InputItem::String(line_content_str));
                        if tx_clone.send(item).await.is_err() {
                            pb_clone.println(format!(
                                "[{}] Worker {} Receiver dropped. Stopping.",
                                reader_type_name_clone, i
                            ));
                            break;
                        }
                        pb_clone.inc(1);
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        let current_offset_for_error_msg = offsets_clone
                            .get(line_num_within_thread_responsibility)
                            .unwrap_or(&0);
                        pb_clone.println(format!(
                            "[{}] Worker {} Error reading line (approx. offset {}): {}. Path: {}",
                            reader_type_name_clone,
                            i,
                            current_offset_for_error_msg,
                            e,
                            file_path_clone_str
                        ));
                        break;
                    }
                }
            }
        }));
    }

    drop(tx);

    tokio::spawn(async move {
        for handle in worker_handles {
            if let Err(e) = handle.await {
                eprintln!(
                    "[{}] Worker thread panicked: {:?}",
                    config.reader_type_name, e
                );
            }
        }
        if !pb_process.is_finished() {
            let final_process_msg = format!(
                "[{reader_name} Process] Complete. {pos} lines. ({elapsed})",
                reader_name = config.reader_type_name,
                pos = pb_process.position(),
                elapsed = format!("{:.2?}", pb_process.elapsed())
            );
            pb_process.finish_with_message(final_process_msg);
        }
    });

    rx
}
