use crate::readers::Reader;
use async_trait::async_trait;
use memchr::memchr_iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned; // Kept for consistency, actual parsing is in read_logic
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File as TokioFile; // Renamed to avoid conflict
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader as TokioBufReader};
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};

// Renamed from FileReader struct conceptually, but keeping the name for now
pub struct FileReader {
    path: String,
    num_threads: usize,
}

impl FileReader {
    pub fn new(path: String) -> Self {
        let num_threads = num_cpus::get();
        println!(
            "[FileReader] Using {} threads for parallel reading file: {}",
            num_threads, path
        );
        Self { path, num_threads }
    }

    fn scan_line_offsets(&self) -> Arc<Vec<u64>> {
        let file_path = Path::new(&self.path);
        let file_size = match std::fs::metadata(file_path) {
            Ok(meta) => meta.len(),
            Err(e) => {
                eprintln!("[FileReader] Failed to get file metadata for {}: {}", self.path, e);
                return Arc::new(vec![]);
            }
        };

        if file_size == 0 {
            println!("[FileReader] Input file is empty: {}.", self.path);
            return Arc::new(vec![]);
        }

        let pb_scan = ProgressBar::new(file_size);
        pb_scan.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Scanning File lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        let chunk_size = (file_size + self.num_threads as u64 - 1) / self.num_threads as u64;

        let offsets_vec: Vec<Vec<u64>> = (0..self.num_threads)
            .into_par_iter()
            .map(|i| {
                let start = i as u64 * chunk_size;
                let mut end = (i as u64 + 1) * chunk_size;
                if end > file_size {
                    end = file_size;
                }
                if start >= end {
                    pb_scan.inc(end.saturating_sub(start));
                    return Vec::new();
                }

                let mut local_offsets = Vec::new();
                let mut f = match std::fs::File::open(file_path) {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("[FileReader] Failed to open {} in scan thread: {}", self.path, e);
                        return Vec::new();
                    }
                };
                let mut current_pos = start;
                let mut bytes_processed_in_chunk_segment = 0;

                if start != 0 {
                    if let Err(e) = f.seek(SeekFrom::Start(start)) {
                        eprintln!("[FileReader] Seek failed in scan thread for {}: {}", self.path, e);
                        return Vec::new();
                    }
                    let mut buffer = [0; 1024];
                    loop {
                        match f.read(&mut buffer) {
                            Ok(0) => {
                                bytes_processed_in_chunk_segment = end.saturating_sub(current_pos);
                                current_pos = end;
                                break;
                            }
                            Ok(n) => {
                                if let Some(newline_idx_in_buffer) = memchr::memchr(b'\n', &buffer[..n]) {
                                    current_pos += (newline_idx_in_buffer + 1) as u64;
                                    bytes_processed_in_chunk_segment = current_pos - start;
                                    break;
                                } else {
                                    current_pos += n as u64;
                                    if current_pos >= end {
                                        bytes_processed_in_chunk_segment = end - start;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[FileReader] Error reading during offset adjustment for {}: {}", self.path, e);
                                pb_scan.inc(end.saturating_sub(start));
                                return Vec::new();
                            }
                        }
                    }
                    if current_pos >= end {
                        pb_scan.inc(bytes_processed_in_chunk_segment.min(end.saturating_sub(start)));
                        return local_offsets;
                    }
                } else {
                    local_offsets.push(0);
                }

                if let Err(e) = f.seek(SeekFrom::Start(current_pos)) {
                     eprintln!("[FileReader] Seek failed before reading effective chunk for {}: {}", self.path, e);
                     pb_scan.inc(end.saturating_sub(current_pos));
                     return local_offsets;
                }
                let mut chunk_buf = Vec::with_capacity((end - current_pos).min(file_size - current_pos) as usize);
                let bytes_to_read_in_effective_chunk = end.saturating_sub(current_pos);
                
                match f.take(bytes_to_read_in_effective_chunk).read_to_end(&mut chunk_buf) {
                    Ok(bytes_actually_read) => {
                        for idx in memchr_iter(b'\n', &chunk_buf[..bytes_actually_read]) {
                            let offset_in_file = current_pos + idx as u64 + 1;
                            if offset_in_file < end {
                                local_offsets.push(offset_in_file);
                            } else {
                                break;
                            }
                        }
                         pb_scan.inc(bytes_actually_read as u64 + bytes_processed_in_chunk_segment);
                    }
                    Err(e) => {
                        eprintln!("[FileReader] Failed to read effective chunk into buffer for {}: {}", self.path, e);
                        pb_scan.inc(bytes_to_read_in_effective_chunk);
                    }
                }
                local_offsets
            })
            .collect();
        
        pb_scan.finish_with_message(format!(
            "[FileReader] Line scan complete for {}. Found approx. {} lines.",
            self.path,
            offsets_vec.iter().map(|v| v.len()).sum::<usize>()
        ));

        let mut combined_offsets: Vec<u64> = offsets_vec.into_iter().flatten().collect();
        if file_size > 0 && (!combined_offsets.contains(&0) ) {
            let mut has_content_implying_lines = false;
            if !combined_offsets.is_empty() {
                 has_content_implying_lines = true;
            } else {
                if let Ok(mut f_check) = std::fs::File::open(file_path) {
                    let mut buf_check = [0;1];
                    if f_check.read(&mut buf_check).unwrap_or(0) > 0 {
                        has_content_implying_lines = true;
                    }
                }
            }
            if has_content_implying_lines {
                 combined_offsets.push(0);
            }
        }
        combined_offsets.sort_unstable();
        combined_offsets.dedup();
        Arc::new(combined_offsets)
    }
}

#[async_trait]
impl<Item> Reader<Item> for FileReader
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug, // Actual parsing to Item is done by read_logic
{
    async fn pipeline(
        &self,
        read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.num_threads * 100);
        let file_path_str = self.path.clone();
        let parser = Arc::new(read_logic);

        let line_offsets = self.scan_line_offsets();
        let num_actual_lines_to_process = if line_offsets.is_empty() { 0 }
        else if line_offsets.len() == 1 {
            match std::fs::metadata(&file_path_str) {
                Ok(meta) if meta.len() > 0 => 1, _ => 0,
            }
        } else { line_offsets.len() };

        if num_actual_lines_to_process == 0 {
            if std::fs::metadata(&file_path_str).map_or(true, |m| m.len() == 0) {
                 println!("[FileReader] No lines to process (file empty or scan found no lines): {}", file_path_str);
            } else {
                 println!("[FileReader] File {} has content but effectively zero processable lines. Offsets: {:?}.", file_path_str, line_offsets);
            }
            drop(tx); return rx;
        }
        
        let file_size = match std::fs::metadata(&file_path_str) {
             Ok(meta) => meta.len(),
             Err(_) => {
                eprintln!("[FileReader] Failed to get file metadata for processing: {}", file_path_str);
                drop(tx); return rx;
             }
        };

        let pb_process = ProgressBar::new(num_actual_lines_to_process as u64);
        pb_process.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Processing File lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        let mut handles = vec![];
        let lines_per_thread_ideal = (num_actual_lines_to_process + self.num_threads - 1) / self.num_threads;

        for i in 0..self.num_threads {
            let tx_clone = tx.clone();
            let parser_clone = Arc::clone(&parser);
            let file_path_clone_str = file_path_str.clone();
            let offsets_clone = Arc::clone(&line_offsets);
            let pb_clone = pb_process.clone();

            let start_line_idx_in_offsets_vec = i * lines_per_thread_ideal;
            let mut end_line_idx_in_offsets_vec = (i + 1) * lines_per_thread_ideal;
            
            if start_line_idx_in_offsets_vec >= num_actual_lines_to_process { continue; }
            if i == self.num_threads - 1 { end_line_idx_in_offsets_vec = num_actual_lines_to_process; }
            end_line_idx_in_offsets_vec = end_line_idx_in_offsets_vec.min(num_actual_lines_to_process);

            if start_line_idx_in_offsets_vec >= end_line_idx_in_offsets_vec { continue; }
            
            handles.push(tokio::spawn(async move {
                let actual_start_line_index_in_offsets = start_line_idx_in_offsets_vec;
                let actual_end_line_exclusive_index_in_offsets = end_line_idx_in_offsets_vec;
                let start_byte_offset = offsets_clone[actual_start_line_index_in_offsets];
                let end_byte_offset = if actual_end_line_exclusive_index_in_offsets >= offsets_clone.len() {
                    file_size
                } else {
                    offsets_clone[actual_end_line_exclusive_index_in_offsets]
                };

                if start_byte_offset >= end_byte_offset && !(start_byte_offset == end_byte_offset && start_byte_offset == file_size && file_size == 0) {
                    return;
                }
                
                let mut file = match TokioFile::open(&file_path_clone_str).await {
                    Ok(f) => f,
                    Err(e) => {
                        pb_clone.println(format!("[FileReader {}] Error opening {}: {}", i, file_path_clone_str, e));
                        return;
                    }
                };

                if let Err(e) = file.seek(SeekFrom::Start(start_byte_offset)).await {
                     pb_clone.println(format!("[FileReader {}] Error seeking in {}: {}", i, file_path_clone_str, e));
                     return;
                }
                
                let chunk_size_for_thread = end_byte_offset - start_byte_offset;
                let chunk_reader = file.take(chunk_size_for_thread);
                let buf_reader_async = TokioBufReader::new(chunk_reader);
                let mut lines_stream = buf_reader_async.lines();

                for _ in actual_start_line_index_in_offsets..actual_end_line_exclusive_index_in_offsets {
                     match lines_stream.next_line().await {
                        Ok(Some(line_content)) => {
                            let item = parser_clone(line_content);
                            if tx_clone.send(item).await.is_err() {
                                pb_clone.println(format!("[FileReader {}] Receiver dropped.", i));
                                break;
                            }
                            pb_clone.inc(1);
                        }
                        Ok(None) => { break; }
                        Err(e) => {
                            pb_clone.println(format!("[FileReader {}] Error reading line from {}: {}", i, file_path_clone_str, e));
                            break; 
                        }
                    }
                }
            }));
        }
        drop(tx);
        tokio::spawn(async move {
            for handle in handles {
                if let Err(e) = handle.await {
                     eprintln!("[FileReader] Worker thread panicked: {:?}", e);
                }
            }
            if !pb_process.is_finished() {
                pb_process.finish_with_message(format!("[FileReader] Finished processing {}.", file_path_str));
            }
        });
        rx
    }
} 