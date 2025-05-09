use crate::readers::Reader;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::io::{SeekFrom, Read, Seek};
use std::sync::Arc;
use indicatif::{ProgressBar, ProgressStyle};
use memchr::memchr_iter;
use tokio::sync::mpsc;
use num_cpus;
use rayon::prelude::*;


use tokio::fs::File as TokioFile;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader as TokioBufReader};

use crate::custom_tasks::LineFormat;


const JSONL_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "JsonlReader",
    scan_progress_template: "[{elapsed_precise}] [Scanning JSONL lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    process_progress_template: "[{elapsed_precise}] [Processing JSONL lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})",
};

const TSV_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "TsvReader",
    scan_progress_template: "[{elapsed_precise}] [Scanning TSV lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    process_progress_template: "[{elapsed_precise}] [Processing TSV lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})",
};

const PLAINTEXT_READER_CONFIG: LineReaderConfig = LineReaderConfig {
    reader_type_name: "PlainTextReader",
    scan_progress_template: "[{elapsed_precise}] [Scanning Text lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
    process_progress_template: "[{elapsed_precise}] [Processing Text lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})",
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

        println!(
            "[{}] Using {} threads for parallel reading file: {}",
            selected_config.reader_type_name,
            num_threads,
            path
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
        read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        pipeline_core(
            self.path.clone(),
            self.num_threads,
            read_fn,
            Arc::clone(&self.config),
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
) -> Arc<Vec<u64>> {
    let pb_scan = ProgressBar::new(file_size);
    pb_scan.set_style(
        ProgressStyle::with_template(config.scan_progress_template)
            .unwrap()
            .progress_chars("##-"),
    );

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
                pb_scan.inc(physical_chunk_end.saturating_sub(physical_chunk_start));
                return local_offsets;
            }

            let bytes_to_read_for_offsets = physical_chunk_end.saturating_sub(current_pos_in_file);
            let mut chunk_data_buf = Vec::with_capacity(bytes_to_read_for_offsets as usize);

            match f
                .take(bytes_to_read_for_offsets)
                .read_to_end(&mut chunk_data_buf)
            {
                Ok(bytes_actually_read_for_offsets) => {
                    for idx in
                        memchr_iter(b'\n', &chunk_data_buf[..bytes_actually_read_for_offsets])
                    {
                        let offset_in_file = current_pos_in_file + idx as u64 + 1;

                        if offset_in_file < physical_chunk_end {
                            local_offsets.push(offset_in_file);
                        } else {
                            break;
                        }
                    }

                    let initial_skip_bytes = current_pos_in_file - physical_chunk_start;
                    pb_scan.inc(initial_skip_bytes + bytes_actually_read_for_offsets as u64);
                }
                Err(e) => {
                    eprintln!(
                        "[{}] Failed to read effective chunk into buffer (path: {}): {}",
                        config.reader_type_name, file_path_str, e
                    );

                    pb_scan.inc(bytes_to_read_for_offsets);
                }
            }
            local_offsets
        })
        .collect();

    pb_scan.finish_with_message(format!(
        "[{}] Line scan complete for {}. Found approx. {} line starting offsets.",
        config.reader_type_name,
        file_path_str,
        offsets_from_threads.iter().map(|v| v.len()).sum::<usize>()
    ));

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
    read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    config: Arc<LineReaderConfig>,
) -> mpsc::Receiver<Item>
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug,
{
    let file_metadata = match std::fs::metadata(&file_path_str) {
        Ok(meta) => meta,
        Err(e) => {
            eprintln!(
                "[{}] Failed to get file metadata for {}: {}",
                config.reader_type_name, file_path_str, e
            );
            let (tx, rx_consumer) = mpsc::channel(1);
            drop(tx);
            return rx_consumer;
        }
    };
    let file_size = file_metadata.len();

    if file_size == 0 {
        println!(
            "[{}] Input file is empty: {}.",
            config.reader_type_name, file_path_str
        );
        let (tx, rx_consumer) = mpsc::channel(1);
        drop(tx);
        return rx_consumer;
    }

    let line_offsets = scan_line_offsets_core(&file_path_str, num_threads, file_size, &config);

    let num_actual_lines_to_process = if line_offsets.is_empty() {
        0
    } else {
        line_offsets.len()
    };

    if num_actual_lines_to_process == 0 {
        println!(
            "[{}] No lines to process (file empty or scan found no lines/valid segments): {}. Offsets: {:?}",
            config.reader_type_name, file_path_str, line_offsets
        );
        let (tx, rx_consumer) = mpsc::channel(1);
        drop(tx);
        return rx_consumer;
    }

    let (tx_producer, rx_consumer) = mpsc::channel(num_threads * 100);
    let parser = Arc::new(read_fn);

    let pb_process = ProgressBar::new(num_actual_lines_to_process as u64);
    pb_process.set_style(
        ProgressStyle::with_template(config.process_progress_template)
            .unwrap()
            .progress_chars("##-"),
    );

    let mut worker_handles = vec![];

    let lines_per_thread_ideal = (num_actual_lines_to_process + num_threads - 1) / num_threads;

    for i in 0..num_threads {
        let tx_clone = tx_producer.clone();
        let parser_clone = Arc::clone(&parser);
        let file_path_clone_str = file_path_str.clone();
        let offsets_clone = Arc::clone(&line_offsets);
        let pb_clone = pb_process.clone();
        let reader_type_name_clone = config.reader_type_name;

        let start_line_idx_in_offsets = i * lines_per_thread_ideal;
        let mut end_line_idx_in_offsets =
            ((i + 1) * lines_per_thread_ideal).min(num_actual_lines_to_process);

        if start_line_idx_in_offsets >= end_line_idx_in_offsets {
            continue;
        }

        if i == num_threads - 1 {
            end_line_idx_in_offsets = num_actual_lines_to_process;
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
                        let item = parser_clone(line_content_str);
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

    drop(tx_producer);

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
            pb_process.finish_with_message(format!(
                "[{}] Finished processing all lines for {}.",
                config.reader_type_name, file_path_str
            ));
        }
    });

    rx_consumer
}
