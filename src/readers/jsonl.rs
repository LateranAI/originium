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

        println!(
            "[JsonlReader] Starting parallel line offset scan for {} bytes...",
            file_size
        );
        let start_time = std::time::Instant::now();

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
                    return Vec::new();
                }

                let mut local_offsets = Vec::new();
                let mut f = std::fs::File::open(file_path)
                    .expect("[JsonlReader] Failed to open file in scan thread.");
                let mut current_pos = start;

                if start != 0 {
                    f.seek(SeekFrom::Start(start))
                        .expect("[JsonlReader] Seek failed.");
                    let mut buffer = [0; 1];
                    loop {
                        match f.read(&mut buffer) {
                            Ok(0) => break,
                            Ok(1) => {
                                current_pos += 1;
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
                        return local_offsets;
                    }
                } else {
                    local_offsets.push(0);
                }

                let mut chunk_buf = Vec::with_capacity((end - current_pos) as usize);
                f.seek(SeekFrom::Start(current_pos))
                    .expect("[JsonlReader] Seek failed.");
                let bytes_read = f
                    .take(end - current_pos)
                    .read_to_end(&mut chunk_buf)
                    .expect("[JsonlReader] Failed to read chunk into buffer.");

                for idx in memchr_iter(b'\n', &chunk_buf[..bytes_read]) {
                    let offset = current_pos + idx as u64 + 1;
                    if offset < end {
                        local_offsets.push(offset);
                    } else {
                        break;
                    }
                }
                local_offsets
            })
            .collect();

        let mut combined_offsets = Vec::new();
        for mut thread_offsets in offsets_vec.into_iter() {
            combined_offsets.append(&mut thread_offsets);
        }

        let duration = start_time.elapsed();
        println!(
            "[JsonlReader] Finished offset scan in {:?}. Found {} line starting offsets.",
            duration,
            combined_offsets.len()
        );

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
        let total_lines = line_offsets.len();

        if total_lines == 0 {
            println!("[JsonlReader] No lines found in file: {}", file_path);
            drop(tx);
            return rx;
        }

        let file_size = std::fs::metadata(&file_path)
            .expect("[JsonlReader] Failed to get file metadata for processing.")
            .len();

        println!(
            "[JsonlReader] Starting to process {} lines across {} threads.",
            total_lines, self.num_threads
        );
        let mut handles = vec![];

        for i in 0..self.num_threads {
            let tx_clone = tx.clone();
            let parser_clone = Arc::clone(&parser);
            let file_path_clone = file_path.clone();
            let offsets_clone = Arc::clone(&line_offsets);
            let start_line_idx = i * ((total_lines + self.num_threads - 1) / self.num_threads);
            let end_line_idx = ((i + 1)
                * ((total_lines + self.num_threads - 1) / self.num_threads))
                .min(total_lines);

            if start_line_idx >= end_line_idx {
                continue;
            }

            handles.push(tokio::spawn(async move {
                let start_byte_offset = offsets_clone[start_line_idx];
                let end_byte_offset = if end_line_idx >= total_lines {
                    file_size
                } else {
                    offsets_clone[end_line_idx]
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
                let mut line_count_in_chunk = 0;

                while let Ok(Some(line_result)) = lines_stream.next_line().await {
                    let item = parser_clone(line_result);
                    if tx_clone.send(item).await.is_err() {
                        eprintln!("[JsonlReader {}] Receiver dropped. Worker stopping.", i);
                        break;
                    }
                    line_count_in_chunk += 1;
                }
                println!(
                    "[JsonlReader {}] Worker finished processing {} lines.",
                    i, line_count_in_chunk
                );
            }));
        }

        drop(tx);

        tokio::spawn(async move {
            for handle in handles {
                handle.await.expect("[JsonlReader] Worker thread panicked");
            }
            println!("[JsonlReader] All worker threads finished.");
        });

        rx
    }
}
