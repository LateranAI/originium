use crate::utils::common_type::FastaItem;
use crate::writers::Writer;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub struct FastaWriter<T: Send + Sync + 'static + Debug + Into<FastaItem>> {
    final_path: PathBuf,
    line_width: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Send + Sync + 'static + Debug + Into<FastaItem>> FastaWriter<T> {
    pub fn new(path: String, line_width: Option<usize>) -> Self {
        let width = line_width.unwrap_or(70);
        eprintln!(
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
impl<T: Send + Sync + 'static + Debug + Into<FastaItem>> Writer<T> for FastaWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        let mut items_written: u64 = 0;

        let file = File::create(&self.final_path)?;
        let mut writer = BufWriter::new(file);

        let pb_items = mp.add(ProgressBar::new_spinner());
        let pb_template = format!(
            "[FastaWriter Write {{elapsed_precise}}] {{spinner:.green}} {{pos}} records ({{per_sec}})"
        );
        pb_items.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb_items.enable_steady_tick(std::time::Duration::from_millis(100));

        while let Some(item) = rx.recv().await {
            let record: FastaItem = item.into();

            writer.write_all(b">")?;
            writer.write_all(record.id.as_bytes())?;
            if let Some(desc_str) = &record.desc {
                if !desc_str.is_empty() {
                    writer.write_all(b" ")?;
                    writer.write_all(desc_str.as_bytes())?;
                }
            }
            writer.write_all(b"\n")?;

            for chunk in record.seq.as_bytes().chunks(self.line_width) {
                writer.write_all(chunk)?;
                writer.write_all(b"\n")?;
            }

            items_written += 1;
            pb_items.inc(1);
        }

        writer.flush()?;

        let final_path_short = self.final_path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let final_msg = format!(
            "[FastaWriter Write] Complete. {pos} records written to '{final_path_short}'. ({elapsed})",
            pos = items_written,
            final_path_short = final_path_short,
            elapsed = format!("{:.2?}", pb_items.elapsed())
        );
        pb_items.finish_with_message(final_msg);

        let duration = start_time.elapsed();
        mp.println(format!(
            "[FastaWriter] Finished successfully in {:?}. Output: {}. Total records: {}",
            duration,
            self.final_path.display(),
            items_written
        ))
        .unwrap_or_default();

        Ok(())
    }
}
