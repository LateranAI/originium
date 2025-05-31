use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use crate::utils::common_type::FastaItem;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use needletail::{parse_fastx_file /*, Sequence*/};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct FastaReader {
    path: String,
}

impl FastaReader {
    pub fn new(path: String) -> Self {
        eprintln!("[FastaReader] Initialized for file: {}", path);
        Self { path }
    }
}

#[async_trait]
impl<Item> Reader<Item> for FastaReader
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100);
        let file_path_str = self.path.clone();
        let parser = Arc::new(read_fn);

        let file_size = std::fs::metadata(&file_path_str)
            .map(|m| m.len())
            .unwrap_or(0);
        let pb_process = mp.add(ProgressBar::new(file_size));
        let pb_template = format!(
            "[FastaReader Scan {{elapsed_precise}}] {{bar:40.cyan/blue}} {{percent:>3}}% ({{bytes}}/{{total_bytes}}) {{bytes_per_sec}}, ETA: {{eta}}"
        );
        pb_process.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .progress_chars("=> "),
        );
        pb_process.enable_steady_tick(std::time::Duration::from_millis(100));

        tokio::task::spawn_blocking(move || {
            let mut reader = match parse_fastx_file(&file_path_str) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!(
                        "[FastaReader] Error opening/parsing file {}: {}",
                        file_path_str, e
                    );
                    pb_process.finish_with_message(format!("Error opening file: {}", e));
                    return;
                }
            };

            let mut item_count: u64 = 0;

            while let Some(record_result) = reader.next() {
                match record_result {
                    Ok(record) => {
                        let full_header_bytes = record.id();
                        let full_header_str = String::from_utf8_lossy(full_header_bytes);

                        let mut parts = full_header_str.splitn(2, |c: char| c.is_whitespace());
                        let truncated_id = parts.next().unwrap_or("").to_string();

                        let description = parts
                            .next()
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                            .map(str::to_string);

                        let sequence_str = String::from_utf8_lossy(&record.seq()).to_string();

                        let fasta_item = FastaItem {
                            id: truncated_id,
                            desc: description,
                            seq: sequence_str,
                        };

                        let item = parser(InputItem::FastaItem(fasta_item));

                        if tx.blocking_send(item).is_err() {
                            eprintln!(
                                "[FastaReader] Receiver dropped. Stopping Fasta processing for {}.",
                                file_path_str
                            );
                            break;
                        }
                        item_count += 1;
                        pb_process.inc(record.num_bases() as u64);
                    }
                    Err(e) => {
                        eprintln!(
                            "[FastaReader] Error reading Fasta record from {}: {}",
                            file_path_str, e
                        );
                        break;
                    }
                }
            }

            if pb_process.position() < file_size && item_count > 0 {}
            let file_path_short = file_path_str.split('/').last().unwrap_or_default().to_string();
            let final_msg = format!(
                "[FastaReader Scan] Complete. {item_count} records from '{file_path_short}'. ({elapsed})",
                item_count = item_count,
                file_path_short = file_path_short,
                elapsed = format!("{:.2?}", pb_process.elapsed())
            );
            pb_process.finish_with_message(final_msg);
        });

        rx
    }
}
