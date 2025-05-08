use crate::readers::Reader;
use async_trait::async_trait;
use memchr::memchr_iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned; // Kept for consistency, actual parsing is in read_logic
use std::fmt::Debug;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader as TokioBufReader}; // Renamed to avoid conflict if std::io::BufReader is used
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};

pub struct TsvReader {
    path: String,
    num_threads: usize,
}

impl TsvReader {
    pub fn new(path: String) -> Self {
        let num_threads = num_cpus::get();
        // Using println! as log macros were removed
        println!(
            "[TsvReader] Using {} threads for parallel reading.",
            num_threads
        );
        Self { path, num_threads }
    }

    // Scans the file to find byte offsets for the start of each line.
    // This is crucial for enabling parallel processing of the file by different threads.
    fn scan_line_offsets(&self) -> Arc<Vec<u64>> {
        let file_path = Path::new(&self.path);
        let file_size = match std::fs::metadata(file_path) {
            Ok(meta) => meta.len(),
            Err(e) => {
                eprintln!("[TsvReader] Failed to get file metadata for offset scanning (path: {}): {}", self.path, e);
                return Arc::new(vec![]);
            }
        };

        if file_size == 0 {
            println!("[TsvReader] Input file is empty: {}.", self.path);
            return Arc::new(vec![]);
        }

        let pb_scan = ProgressBar::new(file_size);
        pb_scan.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Scanning TSV lines {bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        let chunk_size = (file_size + self.num_threads as u64 - 1) / self.num_threads as u64;

        // Parallel iteration over chunks of the file
        let offsets_vec: Vec<Vec<u64>> = (0..self.num_threads)
            .into_par_iter()
            .map(|i| {
                let start = i as u64 * chunk_size;
                let mut end = (i as u64 + 1) * chunk_size;
                if end > file_size {
                    end = file_size;
                }
                // If chunk is zero-size or invalid, return empty offsets for this chunk
                if start >= end {
                    pb_scan.inc(end.saturating_sub(start)); // Ensure progress bar reflects scanned empty part
                    return Vec::new();
                }

                let mut local_offsets = Vec::new();
                let mut f = match std::fs::File::open(file_path) {
                    Ok(file) => file,
                    Err(e) => {
                        eprintln!("[TsvReader] Failed to open file in scan thread (path: {}): {}", self.path, e);
                        return Vec::new(); // Return empty if file can't be opened
                    }
                };
                let mut current_pos = start; // The current byte position in the file
                let mut bytes_processed_in_chunk_segment = 0; // For pb_scan.inc

                // If not the first chunk, adjust `current_pos` to the start of the next line
                if start != 0 {
                    if let Err(e) = f.seek(SeekFrom::Start(start)) {
                        eprintln!("[TsvReader] Seek failed in scan thread (path: {}, offset: {}): {}", self.path, start, e);
                        return Vec::new();
                    }
                    let mut buffer = [0; 1024]; // Read in reasonably sized blocks
                    loop {
                        match f.read(&mut buffer) {
                            Ok(0) => { // EOF reached before finding newline or end of chunk
                                bytes_processed_in_chunk_segment = end.saturating_sub(current_pos); // count remaining part as processed
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
                                    if current_pos >= end { // Reached end of designated chunk without newline
                                        bytes_processed_in_chunk_segment = end - start;
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("[TsvReader] Error reading during offset adjustment (path: {}): {}", self.path, e);
                                pb_scan.inc(end.saturating_sub(start)); // Assume rest of chunk is problematic but scanned
                                return Vec::new();
                            }
                        }
                    }
                    if current_pos >= end { // If adjustment pushed us past the chunk end
                        pb_scan.inc(bytes_processed_in_chunk_segment.min(end.saturating_sub(start)));
                        return local_offsets; // No processable lines in this adjusted segment
                    }
                } else {
                    // First chunk always starts at offset 0
                    local_offsets.push(0);
                }

                // Read the effective chunk for this thread
                if let Err(e) = f.seek(SeekFrom::Start(current_pos)) {
                     eprintln!("[TsvReader] Seek failed before reading effective chunk (path: {}, offset: {}): {}", self.path, current_pos, e);
                     pb_scan.inc(end.saturating_sub(current_pos));
                     return local_offsets;
                }
                let mut chunk_buf = Vec::with_capacity((end - current_pos).min(file_size - current_pos) as usize); // Ensure capacity is not negative or overflowing
                
                let bytes_to_read_in_effective_chunk = end.saturating_sub(current_pos);
                
                match f.take(bytes_to_read_in_effective_chunk).read_to_end(&mut chunk_buf) {
                    Ok(bytes_actually_read) => {
                        for idx in memchr_iter(b'\n', &chunk_buf[..bytes_actually_read]) {
                            let offset_in_file = current_pos + idx as u64 + 1;
                            // Only add offset if it's within the current thread's designated chunk boundary (end)
                            // This prevents adding an offset that belongs to the very start of the next chunk
                            if offset_in_file < end { 
                                local_offsets.push(offset_in_file);
                            } else {
                                break; // Found newline at/after chunk boundary
                            }
                        }
                         pb_scan.inc(bytes_actually_read as u64 + bytes_processed_in_chunk_segment);
                    }
                    Err(e) => {
                        eprintln!("[TsvReader] Failed to read effective chunk into buffer (path: {}): {}", self.path, e);
                        pb_scan.inc(bytes_to_read_in_effective_chunk); // Assume this part was "scanned" even if error
                    }
                }
                local_offsets
            })
            .collect();
        
        pb_scan.finish_with_message(format!(
            "[TsvReader] Line scan complete for {}. Found approx. {} line starting offsets.",
            self.path,
            offsets_vec.iter().map(|v| v.len()).sum::<usize>()
        ));

        // Combine and sort offsets from all threads
        let mut combined_offsets: Vec<u64> = offsets_vec.into_iter().flatten().collect();
        
        // Ensure 0 is present if the file is not empty and offsets were found, or if it's the only offset.
        if file_size > 0 && (!combined_offsets.contains(&0) ) {
            let mut has_content_implying_lines = false;
            if !combined_offsets.is_empty() { // if any offsets were found at all
                 has_content_implying_lines = true;
            } else { // if no newlines were found, but file has content, it's a single line file
                let mut f_check = std::fs::File::open(file_path).unwrap();
                let mut buf_check = [0;1];
                if f_check.read(&mut buf_check).unwrap_or(0) > 0 {
                    has_content_implying_lines = true;
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
impl<Item> Reader<Item> for TsvReader
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug, // DeserializeOwned for consistency with Task read_logic
{
    async fn pipeline(
        &self,
        read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.num_threads * 100); // Buffer size based on num_threads
        let file_path_str = self.path.clone();
        let parser = Arc::new(read_logic);

        let line_offsets = self.scan_line_offsets();
        
        let num_actual_lines_to_process = if line_offsets.is_empty() {
            0
        } else if line_offsets.len() == 1 { // Single offset [0] means one line (or empty file if size is 0)
            match std::fs::metadata(&file_path_str) {
                Ok(meta) if meta.len() > 0 => 1,
                _ => 0,
            }
        } else {
            line_offsets.len() // Number of segments is effectively the number of starting offsets found.
                               // Each offset marks the beginning of a line to be processed.
        };


        if num_actual_lines_to_process == 0 {
            if std::fs::metadata(&file_path_str).map_or(true, |m| m.len() == 0) {
                 println!("[TsvReader] No lines to process (file empty or scan found no lines): {}", file_path_str);
            } else {
                 println!("[TsvReader] File has content but effectively zero processable line segments based on offsets: {}. Offsets: {:?}. Lines to process: {}", file_path_str, line_offsets, num_actual_lines_to_process);
            }
            drop(tx); // Close channel if no work
            return rx;
        }
        
        let file_size = match std::fs::metadata(&file_path_str) {
             Ok(meta) => meta.len(),
             Err(_) => { // Should have been caught by scan_line_offsets, but as a safeguard
                eprintln!("[TsvReader] Failed to get file metadata for processing: {}", file_path_str);
                drop(tx); return rx;
             }
        };

        let pb_process = ProgressBar::new(num_actual_lines_to_process as u64);
        pb_process.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [Processing TSV lines {bar:40.green/black}] {pos}/{len} ({per_sec}, {eta})")
                .unwrap()
                .progress_chars("##-"),
        );

        let mut handles = vec![];
        // Calculate lines per thread more carefully for the last thread
        let lines_per_thread_ideal = (num_actual_lines_to_process + self.num_threads - 1) / self.num_threads;

        for i in 0..self.num_threads {
            let tx_clone = tx.clone();
            let parser_clone = Arc::clone(&parser);
            let file_path_clone_str = file_path_str.clone();
            let offsets_clone = Arc::clone(&line_offsets);
            let pb_clone = pb_process.clone();

            // Determine the range of line *indices* (into the offsets_clone Vec) this thread will handle
            let start_line_idx_in_offsets_vec = i * lines_per_thread_ideal;
            let mut end_line_idx_in_offsets_vec = ((i + 1) * lines_per_thread_ideal);
            
            if start_line_idx_in_offsets_vec >= num_actual_lines_to_process {
                continue; // No lines for this thread
            }
            // Ensure the last thread processes all remaining lines
            if i == self.num_threads - 1 {
                end_line_idx_in_offsets_vec = num_actual_lines_to_process;
            }
            end_line_idx_in_offsets_vec = end_line_idx_in_offsets_vec.min(num_actual_lines_to_process);


            if start_line_idx_in_offsets_vec >= end_line_idx_in_offsets_vec {
                continue; // Should not happen if logic above is correct, but safeguard
            }
            
            handles.push(tokio::spawn(async move {
                // actual_start_line_index is an index into the offsets_clone vector
                let actual_start_line_index_in_offsets = start_line_idx_in_offsets_vec;
                // actual_end_line_exclusive_index_in_offsets is also an index for the *next* line's start offset
                let actual_end_line_exclusive_index_in_offsets = end_line_idx_in_offsets_vec;

                // Get the byte offset for the start of the first line this thread handles
                let start_byte_offset = offsets_clone[actual_start_line_index_in_offsets];
                
                // Determine the end byte offset for this thread's chunk
                // If this thread processes up to the last line of the file, its chunk ends at EOF.
                // Otherwise, its chunk ends at the start of the first line the *next* thread would process.
                let end_byte_offset = if actual_end_line_exclusive_index_in_offsets >= offsets_clone.len() {
                    file_size // This thread reads to the end of the file
                } else {
                    offsets_clone[actual_end_line_exclusive_index_in_offsets]
                };

                if start_byte_offset >= end_byte_offset && !(start_byte_offset == end_byte_offset && start_byte_offset == file_size && file_size == 0) { // Allow empty file case
                    // This condition implies an empty segment or an issue with offset calculation.
                    // pb_clone.println(format!("[TsvReader {}] Empty or invalid byte range: {}-{}", i, start_byte_offset, end_byte_offset));
                    return;
                }
                
                let mut file = match File::open(&file_path_clone_str).await {
                    Ok(f) => f,
                    Err(e) => {
                        pb_clone.println(format!("[TsvReader {}] Failed to open file: {}. Error: {}", i, file_path_clone_str, e));
                        return;
                    }
                };

                if let Err(e) = file.seek(SeekFrom::Start(start_byte_offset)).await {
                     pb_clone.println(format!("[TsvReader {}] Failed to seek to {}. Error: {}", i, start_byte_offset, e));
                     return;
                }
                
                // Limit the reading to the calculated chunk size for this thread
                let chunk_size_for_thread = end_byte_offset - start_byte_offset;
                let chunk_reader = file.take(chunk_size_for_thread);
                let buf_reader_async = TokioBufReader::new(chunk_reader); // Use renamed import
                let mut lines_stream = buf_reader_async.lines();

                // Iterate through lines within this thread's assigned chunk
                for line_num_in_thread_chunk in actual_start_line_index_in_offsets..actual_end_line_exclusive_index_in_offsets {
                     match lines_stream.next_line().await {
                        Ok(Some(line_content)) => {
                            let item = parser_clone(line_content);
                            if tx_clone.send(item).await.is_err() {
                                pb_clone.println(format!("[TsvReader {}] Receiver dropped. Worker stopping.", i));
                                break; // Stop processing if receiver is gone
                            }
                            pb_clone.inc(1);
                        }
                        Ok(None) => { 
                            // End of lines in this chunk, as expected if it's the last chunk or lines perfectly aligned
                            break; 
                        }
                        Err(e) => {
                            pb_clone.println(format!("[TsvReader {}] Error reading line {}(offset {}): {}", i, line_num_in_thread_chunk, offsets_clone[line_num_in_thread_chunk], e));
                            // Optionally decide to continue or break based on error
                            break; 
                        }
                    }
                }
            }));
        }

        drop(tx); // Drop the original sender, channel closes when all workers (& clones) are done

        // Spawn a task to await all worker handles and finalize the progress bar
        tokio::spawn(async move {
            for handle in handles {
                if let Err(e) = handle.await {
                     // Using eprintln directly as pb might be finished by other means or not accessible here easily
                     eprintln!("[TsvReader] Worker thread panicked: {:?}", e);
                }
            }
            if !pb_process.is_finished() { // Check if not already finished due to some error path
                pb_process.finish_with_message(format!("[TsvReader] All worker threads finished processing lines for {}.", file_path_str));
            }
        });

        rx // Return the receiver for the main task to consume items
    }
} 