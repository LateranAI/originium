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
        pb_process.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [Fasta {file_path}] {bar:40.cyan/blue} {bytes}/{total_bytes} ({bytes_per_sec}, {eta})"
            ).unwrap().progress_chars("##-"),
        );
        pb_process.set_message(
            file_path_str
                .split('/')
                .last()
                .unwrap_or_default()
                .to_string(),
        );

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
            pb_process.finish_with_message(format!(
                "[FastaReader] Done: {}. Records: {}.",
                file_path_str.split('/').last().unwrap_or_default(),
                item_count
            ));
        });

        rx
    }
}
