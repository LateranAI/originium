use crate::writers::Writer;
use async_trait::async_trait;
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;
use tokio::sync::mpsc::{self, Receiver, Sender as TokioSender};
use tokio::task::JoinHandle;
use std::fmt::Debug;

// Renamed from FileWriter struct conceptually, but keeping the name for now
pub struct FileWriter<T: Display + Send + Sync + 'static + Debug> {
    final_path: PathBuf,
    num_concurrent_writers: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Display + Send + Sync + 'static + Debug> FileWriter<T> {
    pub fn new(path: String, num_concurrent_writers: Option<usize>) -> Self {
        let writers = num_concurrent_writers.unwrap_or_else(|| num_cpus::get().max(1));
        println!(
            "[FileWriter] Initialized for path: {}. Using {} concurrent temp writers.",
            path, writers
        );
        Self {
            final_path: PathBuf::from(path),
            num_concurrent_writers: writers,
            _phantom: std::marker::PhantomData,
        }
    }

    fn get_temp_file_path(&self, writer_idx: usize) -> PathBuf {
        let final_path_str = self.final_path.to_string_lossy();
        PathBuf::from(format!("{}.tmp.{}", final_path_str, writer_idx))
    }
}

#[async_trait]
impl<T: Display + Send + Sync + 'static + Debug> Writer<T> for FileWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let overall_start_time = std::time::Instant::now();
        let mut item_count: u64 = 0;

        let pb_items = ProgressBar::new_spinner();
        pb_items.enable_steady_tick(std::time::Duration::from_millis(120));
        pb_items.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [FileWriter Processing {spinner:.blue}] {pos} items processed ({per_sec})",
            ).unwrap()
        );

        let mut writer_task_handles = Vec::new();
        let mut temp_file_paths = Vec::new();
        let mut worker_senders = Vec::new();

        // Create and store senders for worker tasks
        for i in 0..self.num_concurrent_writers {
            let (tx_to_worker, mut rx_from_main): (TokioSender<Option<T>>, Receiver<Option<T>>) = mpsc::channel(100);
            worker_senders.push(tx_to_worker);
            let temp_path = self.get_temp_file_path(i);
            temp_file_paths.push(temp_path.clone());

            let handle: JoinHandle<io::Result<u64>> = tokio::spawn(async move {
                let file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&temp_path)?;
                let mut writer = BufWriter::new(file);
                let mut lines_written_in_task: u64 = 0;

                while let Some(maybe_item) = rx_from_main.recv().await {
                    if let Some(item) = maybe_item {
                        writeln!(writer, "{}", item)?;
                        lines_written_in_task += 1;
                    } else {
                        break;
                    }
                }
                writer.flush()?;
                Ok(lines_written_in_task)
            });
            writer_task_handles.push(handle);
        }

        // Distribute items to worker tasks
        let mut current_worker_idx = 0;
        while let Some(item) = rx.recv().await {
            item_count += 1;
            pb_items.inc(1);
            if worker_senders[current_worker_idx].send(Some(item)).await.is_err() {
                let err_msg = format!("FileWriter: Worker {} channel closed unexpectedly.", current_worker_idx);
                pb_items.abandon_with_message(format!("{}. Aborting.", err_msg));
                for i in 0..self.num_concurrent_writers {
                    if i != current_worker_idx {
                        let _ = worker_senders[i].send(None).await;
                    }
                }
                return Err(Box::new(io::Error::new(io::ErrorKind::BrokenPipe, err_msg)));
            }
            current_worker_idx = (current_worker_idx + 1) % self.num_concurrent_writers;
        }
        pb_items.finish_with_message(format!("[FileWriter] Item processing complete. {} items processed.", item_count));

        // Signal all workers that no more items are coming
        for tx_to_worker in worker_senders {
            let _ = tx_to_worker.send(None).await;
        }

        // Await all worker tasks
        let mut total_lines_written_by_workers: u64 = 0;
        for (i, handle) in writer_task_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(lines_in_worker)) => {
                    total_lines_written_by_workers += lines_in_worker;
                    println!("[FileWriter] Worker {} finished, wrote {} lines to {:?}.", i, lines_in_worker, temp_file_paths[i]);
                }
                Ok(Err(io_err)) => {
                    let err_msg = format!("[FileWriter] Worker {} IO error: {}. Temp file: {:?}", i, io_err, temp_file_paths[i]);
                    return Err(Box::new(io::Error::new(io_err.kind(), err_msg)));
                }
                Err(join_err) => {
                    let err_msg = format!("[FileWriter] Worker {} panicked: {:?}. Temp file: {:?}", i, join_err, temp_file_paths[i]);
                    eprintln!("{}", err_msg);
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        println!("[FileWriter] All {} worker tasks completed. Total lines written to temp files: {}.", self.num_concurrent_writers, total_lines_written_by_workers);

        if item_count == 0 && total_lines_written_by_workers == 0 {
            println!("[FileWriter] No items were processed, final file {} will be empty or not created if it doesn't exist.", self.final_path.display());
            for temp_path in &temp_file_paths {
                if temp_path.exists() {
                    if let Err(e) = std::fs::remove_file(temp_path) {
                        eprintln!("[FileWriter] Warning: Failed to remove empty temp file {:?}: {}", temp_path, e);
                    }
                }
            }
            let duration = overall_start_time.elapsed();
            println!("[FileWriter] Finished in {:?}. Output: {}.", duration, self.final_path.display());
            return Ok(());
        }

        // Merge temporary files into the final output file
        println!("[FileWriter] Merging {} temporary files into {}", temp_file_paths.len(), self.final_path.display());
        let pb_merge = ProgressBar::new(temp_file_paths.len() as u64);
        pb_merge.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [FileWriter Merging {bar:40.green/black}] {pos}/{len} files",
            ).unwrap()
        );

        let final_output_file = OpenOptions::new().create(true).write(true).truncate(true).open(&self.final_path)?;
        let mut final_writer = BufWriter::new(final_output_file);
        let mut buffer = [0; 8192];

        for (idx, temp_path) in temp_file_paths.iter().enumerate() {
            if !temp_path.exists() {
                println!("[FileWriter] Warning: Temp file {:?} for worker {} does not exist at merge time.", temp_path, idx);
                pb_merge.inc(1);
                continue;
            }
            let mut temp_file = File::open(temp_path)?;
            loop {
                let bytes_read = temp_file.read(&mut buffer)?;
                if bytes_read == 0 {
                    break;
                }
                final_writer.write_all(&buffer[..bytes_read])?;
            }
            pb_merge.inc(1);
        }
        final_writer.flush()?;
        pb_merge.finish_with_message("[FileWriter] Merging complete.");

        // Clean up temporary files
        for temp_path in temp_file_paths {
            if temp_path.exists() {
                if let Err(e) = std::fs::remove_file(&temp_path) {
                    eprintln!("[FileWriter] Warning: Failed to remove temp file {:?}: {}", temp_path, e);
                }
            }
        }

        let duration = overall_start_time.elapsed();
        println!("[FileWriter] Finished successfully in {:?}. Output: {}. Total lines: {}", duration, self.final_path.display(), total_lines_written_by_workers);
        Ok(())
    }
} 