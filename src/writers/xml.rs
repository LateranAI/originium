use crate::writers::Writer;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::Serialize;
use std::fmt::Debug;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub struct XmlWriter<T: Serialize + Send + Sync + 'static + Debug> {
    final_path: PathBuf,
    root_tag: Option<String>,
    item_tag: String,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + Send + Sync + 'static + Debug> XmlWriter<T> {
    pub fn new(path: String, root_tag: Option<String>, item_tag: String) -> Self {
        eprintln!(
            "[XmlWriter] Initialized for path: {}. Root tag: {:?}, Item tag: <{}>",
            path, root_tag, item_tag
        );
        Self {
            final_path: PathBuf::from(path),
            root_tag,
            item_tag,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: Serialize + Send + Sync + 'static + Debug> Writer<T> for XmlWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        let mut items_written: u64 = 0;

        let file = File::create(&self.final_path)?;
        let mut writer = BufWriter::new(file);

        writer.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")?;

        if let Some(tag) = &self.root_tag {
            writer.write_all(format!("<{}>\n", tag).as_bytes())?;
        }

        let pb_items = mp.add(ProgressBar::new_spinner());
        let pb_template = format!(
            "[XmlWriter Serialize {{elapsed_precise}}] {{spinner:.blue}} {{pos}} items ({{per_sec}})"
        );
        pb_items.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb_items.enable_steady_tick(std::time::Duration::from_millis(100));

        while let Some(item) = rx.recv().await {
            match quick_xml::se::to_string_with_root(&self.item_tag, &item) {
                Ok(xml_string) => {
                    writer.write_all(xml_string.as_bytes())?;
                    writer.write_all(b"\n")?;
                    items_written += 1;
                    pb_items.inc(1);
                }
                Err(e) => {
                    let err_msg = format!(
                        "[XmlWriter] Failed to serialize item {:?} to XML: {}",
                        item, e
                    );
                    eprintln!("{}", err_msg);
                    pb_items.abandon_with_message(err_msg.clone());

                    return Err(Box::new(io::Error::new(
                        io::ErrorKind::InvalidData,
                        err_msg,
                    )));
                }
            }
        }

        if let Some(tag) = &self.root_tag {
            writer.write_all(format!("</{}>\n", tag).as_bytes())?;
        }

        writer.flush()?;

        let final_path_short = self.final_path.file_name().unwrap_or_default().to_string_lossy().to_string();
        let final_msg = format!(
            "[XmlWriter Serialize] Complete. {pos} <{item_tag}> items written to '{final_path_short}'. ({elapsed})",
            pos = items_written,
            item_tag = self.item_tag,
            final_path_short = final_path_short,
            elapsed = format!("{:.2?}", pb_items.elapsed())
        );
        pb_items.finish_with_message(final_msg);

        let duration = start_time.elapsed();
        mp.println(format!(
            "[XmlWriter] Finished successfully in {:?}. Output: {}. Total items: {}",
            duration,
            self.final_path.display(),
            items_written
        ))
        .unwrap_or_default();

        Ok(())
    }
}
