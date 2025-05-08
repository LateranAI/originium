use crate::readers::Reader;
use async_trait::async_trait;
use indicatif::{ProgressBar, ProgressStyle};
use needletail::{parse_fastx_file, /*parser::SequenceRecord,*/ /*FastxReader*/};
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;



#[derive(Debug, Clone)]
pub struct FastaRecord {
    pub id: Vec<u8>,
    pub seq: Vec<u8>,

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



    Item: Send + Sync + 'static + Debug, 

{
    async fn pipeline(
        &self,

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

                        let seq_string = String::from_utf8_lossy(&record.seq()).to_string();
                        

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