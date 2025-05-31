use crate::writers::Writer;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fmt::Debug;
use std::fmt::Display;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver, Sender as TokioSender};
use tokio::task::JoinHandle;

pub struct FileWriter<T: Display + Send + Sync + 'static + Debug> {
    final_path: PathBuf,
    num_concurrent_writers: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Display + Send + Sync + 'static + Debug> FileWriter<T> {
    pub fn new(path: String, num_concurrent_writers: Option<usize>) -> Self {
        let writers = num_concurrent_writers.unwrap_or_else(|| num_cpus::get().max(1));
        eprintln!(
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
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let overall_start_time = std::time::Instant::now();
        let mut item_count: u64 = 0;

        let pb_items = mp.add(ProgressBar::new_spinner());
        let pb_items_template = format!(
            "[FileWriter Distribute {{elapsed_precise}}] {{spinner:.blue}} {{pos}} items ({{per_sec}})"
        );
        pb_items.set_style(
            ProgressStyle::with_template(&pb_items_template)
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb_items.enable_steady_tick(std::time::Duration::from_millis(100));

        let mut writer_task_handles = Vec::new();
        let mut temp_file_paths = Vec::new();
        let mut worker_senders = Vec::new();

        for i in 0..self.num_concurrent_writers {
            let (tx_to_worker, mut rx_from_main): (TokioSender<Option<T>>, Receiver<Option<T>>) =
                mpsc::channel(100);
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

        let mut current_worker_idx = 0;
        while let Some(item) = rx.recv().await {
            item_count += 1;
            pb_items.inc(1);
            if worker_senders[current_worker_idx]
                .send(Some(item))
                .await
                .is_err()
            {
                let err_msg = format!(
                    "FileWriter: Worker {} channel closed unexpectedly.",
                    current_worker_idx
                );
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
        let pb_items_final_msg = format!(
            "[FileWriter Distribute] Complete. {pos} items distributed. ({elapsed})",
            pos = item_count,
            elapsed = format!("{:.2?}", pb_items.elapsed())
        );
        pb_items.finish_with_message(pb_items_final_msg);

        for tx_to_worker in worker_senders {
            let _ = tx_to_worker.send(None).await;
        }

        let mut total_lines_written_by_workers: u64 = 0;
        for (i, handle) in writer_task_handles.into_iter().enumerate() {
            match handle.await {
                Ok(Ok(lines_in_worker)) => {
                    total_lines_written_by_workers += lines_in_worker;
                    mp.println(format!(
                        "[FileWriter] Worker {} finished, wrote {} lines to {:?}.",
                        i, lines_in_worker, temp_file_paths[i]
                    ))
                    .unwrap_or_default();
                }
                Ok(Err(io_err)) => {
                    let err_msg = format!(
                        "[FileWriter] Worker {} IO error: {}. Temp file: {:?}",
                        i, io_err, temp_file_paths[i]
                    );
                    return Err(Box::new(io::Error::new(io_err.kind(), err_msg)));
                }
                Err(join_err) => {
                    let err_msg = format!(
                        "[FileWriter] Worker {} panicked: {:?}. Temp file: {:?}",
                        i, join_err, temp_file_paths[i]
                    );
                    eprintln!("{}", err_msg);
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        mp.println(format!(
            "[FileWriter] All {} worker tasks completed. Total lines written to temp files: {}.",
            self.num_concurrent_writers, total_lines_written_by_workers
        ))
        .unwrap_or_default();

        if item_count == 0 && total_lines_written_by_workers == 0 {
            mp.println(format!(
                "[FileWriter] No items were processed, final file {} will be empty or not created if it doesn't exist.",
                self.final_path.display()
            )).unwrap_or_default();
            for temp_path in &temp_file_paths {
                if temp_path.exists() {
                    if let Err(e) = std::fs::remove_file(temp_path) {
                        eprintln!(
                            "[FileWriter] Warning: Failed to remove empty temp file {:?}: {}",
                            temp_path, e
                        );
                    }
                }
            }
            let duration = overall_start_time.elapsed();
            mp.println(format!(
                "[FileWriter] Finished in {:?}. Output: {}.",
                duration,
                self.final_path.display()
            ))
            .unwrap_or_default();
            return Ok(());
        }

        mp.println(format!("[FileWriter] Merging {} temp files...", temp_file_paths.len()))
            .unwrap_or_default();

        let total_bytes_to_merge = temp_file_paths.iter().filter_map(|p| std::fs::metadata(p).ok().map(|m| m.len())).sum();
        let pb_merge = mp.add(ProgressBar::new(total_bytes_to_merge));

        let pb_merge_template = format!(
            "[FileWriter Merge {{elapsed_precise}}] {{bar:40.green/blue}} {{percent:>3}}% ({{bytes}}/{{total_bytes}}) {{bytes_per_sec}}, ETA: {{eta}}"
        );
        pb_merge.set_style(
            ProgressStyle::with_template(&pb_merge_template)
                .unwrap()
                .progress_chars("=> "),
        );
        pb_merge.enable_steady_tick(std::time::Duration::from_millis(100));

        let final_output_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.final_path)?;
        let mut final_writer = BufWriter::new(final_output_file);
        let mut buffer = [0; 8192];

        for (idx, temp_path) in temp_file_paths.iter().enumerate() {
            if !temp_path.exists() {
                mp.println(format!(
                    "[FileWriter] Warning: Temp file {:?} for worker {} does not exist at merge time.",
                    temp_path, idx
                )).unwrap_or_default();
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
        let final_path_short = self.final_path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let pb_merge_final_msg = format!(
            "[FileWriter Merge] Complete. {pos} temp files merged into '{final_path_short}'. ({elapsed})",
            pos = pb_merge.position(),
            final_path_short = final_path_short,
            elapsed = format!("{:.2?}", pb_merge.elapsed())
        );
        pb_merge.finish_with_message(pb_merge_final_msg);

        for temp_path in temp_file_paths {
            if temp_path.exists() {
                if let Err(e) = std::fs::remove_file(&temp_path) {
                    eprintln!(
                        "[FileWriter] Warning: Failed to remove temp file {:?}: {}",
                        temp_path, e
                    );
                }
            }
        }

        let duration = overall_start_time.elapsed();
        mp.println(format!(
            "[FileWriter] Finished successfully in {:?}. Output: {}. Total lines: {}",
            duration,
            self.final_path.display(),
            total_lines_written_by_workers
        ))
        .unwrap_or_default();
        Ok(())
    }
}
