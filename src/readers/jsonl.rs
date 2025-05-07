use crate::readers::Reader;
use async_trait::async_trait;
use memchr::memchr_iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};

pub struct JsonlReader {
    path: String,
    num_threads: usize,
}

impl JsonlReader {
    pub fn new(path: String) -> Self {
        let num_threads = num_cpus::get();
        println!(
            "[JsonlReader] Using {} threads for parallel reading.",
            num_threads
        );
        Self { path, num_threads }
    }

    fn scan_line_offsets(&self) -> Arc<Vec<u64>> {
        let file_path = Path::new(&self.path);
        let file_size = std::fs::metadata(file_path)
            .expect("[JsonlReader] Failed to get file metadata for offset scanning.")
            .len();

        if file_size == 0 {
            println!("[JsonlReader] Input file is empty.");
            return Arc::new(vec![]);
        }

        let pb_scan = ProgressBar::new(file_size);
        pb_scan.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Scanning lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
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
                let mut f = std::fs::File::open(file_path)
                    .expect("[JsonlReader] Failed to open file in scan thread.");
                let mut current_pos = start;
                let mut bytes_processed_in_chunk = 0;

                if start != 0 {
                    f.seek(SeekFrom::Start(start))
                        .expect("[JsonlReader] Seek failed.");
                    let mut buffer = [0; 1];
                    loop {
                        match f.read(&mut buffer) {
                            Ok(0) => break,
                            Ok(1) => {
                                current_pos += 1;
                                bytes_processed_in_chunk += 1;
                                if buffer[0] == b'\n' {
                                    break;
                                }
                                if current_pos >= end {
                                    break;
                                }
                            }
                            Err(e) => panic!(
                                "[JsonlReader] Error reading during offset adjustment: {}",
                                e
                            ),
                            _ => unreachable!(),
                        }
                    }
                    if current_pos >= end {
                        pb_scan.inc(bytes_processed_in_chunk);
                        return local_offsets;
                    }
                } else {
                    local_offsets.push(0);
                }

                let mut chunk_buf = Vec::with_capacity((end - current_pos) as usize);
                f.seek(SeekFrom::Start(current_pos))
                    .expect("[JsonlReader] Seek failed.");
                let bytes_to_read_in_chunk = end - current_pos;
                let bytes_actually_read = f
                    .take(bytes_to_read_in_chunk)
                    .read_to_end(&mut chunk_buf)
                    .expect("[JsonlReader] Failed to read chunk into buffer.");
                
                bytes_processed_in_chunk += bytes_actually_read as u64;

                for idx in memchr_iter(b'\n', &chunk_buf[..bytes_actually_read]) {
                    let offset = current_pos + idx as u64 + 1;
                    if offset < end {
                        local_offsets.push(offset);
                    } else {
                        break; 
                    }
                }
                pb_scan.inc(bytes_processed_in_chunk.saturating_sub( (current_pos - start) ) );
                local_offsets
            })
            .collect();
        
        pb_scan.finish_with_message(format!(
            "Line scan complete. Found {} line starting offsets.",
            offsets_vec.iter().map(|v| v.len()).sum::<usize>()
        ));

        let mut combined_offsets = Vec::new();
        let mut initial_zero_added = false;
        for thread_offsets in offsets_vec.into_iter() {
            if !initial_zero_added && thread_offsets.first().map_or(true, |&o| o != 0) {
                 if combined_offsets.is_empty() || combined_offsets.last() != Some(&0) {
                    if !combined_offsets.contains(&0) {
                        let mut temp = vec![0];
                        temp.extend(thread_offsets);
                        combined_offsets.extend(temp);
                        initial_zero_added = true;
                        continue;
                    }
                 }
            }
            combined_offsets.extend(thread_offsets);
        }
        
        if !combined_offsets.is_empty() {
            combined_offsets.sort_unstable();
            combined_offsets.dedup();
            if combined_offsets[0] != 0 {
                let mut new_offsets = vec![0];
                new_offsets.extend(combined_offsets.into_iter().filter(|&o| o != 0));
                combined_offsets = new_offsets;
            }
        } else if file_size > 0 {
            combined_offsets.push(0);
        }

        Arc::new(combined_offsets)
    }
}

#[async_trait]
impl<Item> Reader<Item> for JsonlReader
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.num_threads * 100);
        let file_path = self.path.clone();
        let parser = Arc::new(read_logic);

        let line_offsets = self.scan_line_offsets();
        let total_lines = if line_offsets.is_empty() { 0 } else { line_offsets.len() -1 };

        if total_lines == 0 && !line_offsets.is_empty() && std::fs::metadata(&file_path).map_or(0, |m| m.len()) > 0 {
            
        }
        
        let num_actual_lines_to_process = if line_offsets.is_empty() {
            0
        } else if line_offsets.len() == 1 && std::fs::metadata(&file_path).map_or(0, |m| m.len()) > 0 {
            1
        } else {
            line_offsets.len() -1
        };

        if num_actual_lines_to_process == 0 {
            if std::fs::metadata(&file_path).map_or(true, |m| m.len() == 0) {
                 println!("[JsonlReader] No lines found or file is empty: {}", file_path);
            } else {
                 println!("[JsonlReader] File has content but effectively zero processable line segments based on offsets: {}. Offsets: {:?}", file_path, line_offsets);
            }
            drop(tx);
            return rx;
        }
        
        let file_size = std::fs::metadata(&file_path)
            .expect("[JsonlReader] Failed to get file metadata for processing.")
            .len();

        let pb_process = ProgressBar::new(num_actual_lines_to_process as u64);
        pb_process.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Processing lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        let mut handles = vec![];
        let lines_per_thread_ideal = (num_actual_lines_to_process + self.num_threads - 1) / self.num_threads;

        for i in 0..self.num_threads {
            let tx_clone = tx.clone();
            let parser_clone = Arc::clone(&parser);
            let file_path_clone = file_path.clone();
            let offsets_clone = Arc::clone(&line_offsets);
            let pb_clone = pb_process.clone();

            let start_line_idx_in_offsets = i * lines_per_thread_ideal;
            let end_line_idx_in_offsets = ((i + 1) * lines_per_thread_ideal).min(num_actual_lines_to_process);

            if start_line_idx_in_offsets >= end_line_idx_in_offsets {
                continue;
            }

            handles.push(tokio::spawn(async move {
                let actual_start_line_index = start_line_idx_in_offsets;
                let actual_end_line_exclusive_index = end_line_idx_in_offsets;

                if actual_start_line_index >= offsets_clone.len() || actual_end_line_exclusive_index > offsets_clone.len() -1 && actual_end_line_exclusive_index != offsets_clone.len(){
                    
                }

                let start_byte_offset = offsets_clone[actual_start_line_index];
                let end_byte_offset = if actual_end_line_exclusive_index >= num_actual_lines_to_process {
                    if actual_end_line_exclusive_index == num_actual_lines_to_process {
                        file_size 
                    } else {
                        offsets_clone[actual_end_line_exclusive_index]
                    }
                } else {
                     offsets_clone[actual_end_line_exclusive_index]
                };

                if start_byte_offset >= end_byte_offset {
                    return;
                }

                let mut file = File::open(&file_path_clone)
                    .await
                    .expect(&format!("[JsonlReader {}] Failed to open file", i));
                file.seek(SeekFrom::Start(start_byte_offset))
                    .await
                    .expect(&format!("[JsonlReader {}] Failed to seek", i));

                let chunk_reader = file.take(end_byte_offset - start_byte_offset);
                let buf_reader = BufReader::new(chunk_reader);
                let mut lines_stream = buf_reader.lines();

                while let Ok(Some(line_result)) = lines_stream.next_line().await {
                    let item = parser_clone(line_result);
                    if tx_clone.send(item).await.is_err() {
                        pb_clone.println(format!("[JsonlReader {}] Receiver dropped. Worker stopping.", i));
                        break;
                    }
                    pb_clone.inc(1);
                }
            }));
        }

        drop(tx);

        tokio::spawn(async move {
            for handle in handles {
                handle.await.expect("[JsonlReader] Worker thread panicked");
            }
            pb_process.finish_with_message("All worker threads finished processing lines.");
        });

        rx
    }
}
