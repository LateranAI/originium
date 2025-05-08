use crate::writers::Writer;
use crate::readers::fasta::FastaRecord; // Assuming FastaRecord is the common representation
use async_trait::async_trait;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use tokio::sync::mpsc::Receiver;
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::Debug;

pub struct FastaWriter<T: Send + Sync + 'static + Debug + Into<FastaRecord>> {
    final_path: PathBuf,
    line_width: usize, // Width for sequence line wrapping
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Send + Sync + 'static + Debug + Into<FastaRecord>> FastaWriter<T> {
    pub fn new(path: String, line_width: Option<usize>) -> Self {
        let width = line_width.unwrap_or(70); // Default line width
        println!(
            "[FastaWriter] Initialized for path: {}. Sequence line width: {}",
            path, width
        );
        Self {
            final_path: PathBuf::from(path),
            line_width: width,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Send + Sync + 'static + Debug + Into<FastaRecord>> Writer<T> for FastaWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        let mut items_written: u64 = 0;

        let file = File::create(&self.final_path)?;
        let mut writer = BufWriter::new(file);

        let pb_items = ProgressBar::new_spinner();
        pb_items.enable_steady_tick(std::time::Duration::from_millis(120));
        pb_items.set_style(
            ProgressStyle::with_template(
                 "[{elapsed_precise}] [Writing Fasta {spinner:.green}] {pos} records written ({per_sec})"
            ).unwrap()
        );

        while let Some(item) = rx.recv().await {
            let record: FastaRecord = item.into();

            // --- Write ID Line --- 
            // Attempt to convert ID bytes to UTF-8. Handle potential errors.
            let id_str = String::from_utf8_lossy(&record.id); // Use lossy conversion for simplicity
            // Alternatively, return an error if ID is not valid UTF-8:
            // let id_str = String::from_utf8(record.id).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("Fasta ID is not valid UTF-8: {}", e)))?;
            
            writer.write_all(b">")?;
            writer.write_all(id_str.as_bytes())?;
            writer.write_all(b"\n")?;

            // --- Write Sequence Lines --- 
            for chunk in record.seq.chunks(self.line_width) {
                writer.write_all(chunk)?;
                writer.write_all(b"\n")?;
            }

            items_written += 1;
            pb_items.inc(1);
        }

        writer.flush()?; // Ensure all data is written

        pb_items.finish_with_message(format!("[FastaWriter] Record writing complete. {} records written.", items_written));

        let duration = start_time.elapsed();
        println!(
            "[FastaWriter] Finished successfully in {:?}. Output: {}. Total records: {}",
            duration,
            self.final_path.display(),
            items_written
        );

        Ok(())
    }
} 