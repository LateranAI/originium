use crate::readers::Reader;
use async_trait::async_trait;
use indicatif::{ProgressBar, ProgressStyle};
use needletail::{parse_fastx_file, /*parser::SequenceRecord,*/ /*FastxReader*/};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Represents a single record read from a Fasta/Fastq file.
/// Uses Vec<u8> to avoid potentially invalid UTF-8 in sequences or IDs.
#[derive(Debug, Clone)]
pub struct FastaRecord {
    pub id: Vec<u8>,
    pub seq: Vec<u8>,
    // Optional: pub qual: Option<Vec<u8>> for Fastq support?
}

pub struct FastaReader {
    path: String,
}

impl FastaReader {
    pub fn new(path: String) -> Self {
        println!(
            "[FastaReader] Initialized for file: {}",
            path
        );
        Self { path }
    }
}

#[async_trait]
impl<Item> Reader<Item> for FastaReader
where
    // Task::InputItem needs to be constructible from FastaRecord
    // Typically, read_logic will handle this conversion.
    // The type constraint reflects the *output* of the pipeline channel.
    Item: Send + Sync + 'static + Debug, 
    // We constrain read_logic input internally
{
    async fn pipeline(
        &self,
        // Now expects read_logic to take String (the sequence)
        read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100);
        let file_path_str = self.path.clone();
        let parser = Arc::new(read_logic);

        let file_size = std::fs::metadata(&file_path_str).map(|m| m.len()).unwrap_or(0);
        let pb_process = ProgressBar::new(file_size);
        pb_process.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [Processing Fasta {bar:40.yellow/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})"
            ).unwrap()
        );

        tokio::task::spawn_blocking(move || {
            let mut reader = match parse_fastx_file(&file_path_str) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("[FastaReader] Error opening/parsing file {}: {}", file_path_str, e);
                    pb_process.finish_with_message(format!("Error opening file: {}", e));
                    return;
                }
            };
            
            let mut item_count: u64 = 0;

            while let Some(record_result) = reader.next() {
                match record_result {
                    Ok(record) => {
                        // Convert sequence Vec<u8> to String before passing to read_logic
                        let seq_string = String::from_utf8_lossy(&record.seq()).to_string();
                        
                        // Now call read_logic with the sequence String
                        let item = parser(seq_string);
                        
                        if tx.blocking_send(item).is_err() {
                            eprintln!("[FastaReader] Receiver dropped. Stopping Fasta processing.");
                            break;
                        }
                        item_count += 1;

                    }
                    Err(e) => {
                        eprintln!("[FastaReader] Error reading Fasta record: {}", e);
                        break;
                    }
                }
            }
            
            pb_process.set_position(file_size);
            pb_process.finish_with_message(format!("[FastaReader] Finished processing {}. Records found: {}", file_path_str, item_count));
        });

        rx
    }
} 