use crate::writers::Writer;
use async_trait::async_trait;
use serde::Serialize;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use tokio::sync::mpsc::Receiver;
use indicatif::{ProgressBar, ProgressStyle};
use std::fmt::Debug;

pub struct XmlWriter<T: Serialize + Send + Sync + 'static + Debug> {
    final_path: PathBuf,
    root_tag: Option<String>, // Optional root element tag name
    item_tag: String, // Tag name for each item/record, needed for serialization
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Serialize + Send + Sync + 'static + Debug> XmlWriter<T> {
    // Item tag is required to wrap each serialized item
    pub fn new(path: String, root_tag: Option<String>, item_tag: String) -> Self {
        println!(
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
    // Writes items sequentially to ensure a well-formed XML document.
    // Parallel XML writing is complex to manage correctly.
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        let mut items_written: u64 = 0;

        let file = File::create(&self.final_path)?;
        let mut writer = BufWriter::new(file);

        // Write XML declaration
        writer.write_all(b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")?;

        // Write root start tag if provided
        if let Some(tag) = &self.root_tag {
            writer.write_all(format!("<{}>\n", tag).as_bytes())?;
        }

        let pb_items = ProgressBar::new_spinner();
        pb_items.enable_steady_tick(std::time::Duration::from_millis(120));
        pb_items.set_style(
            ProgressStyle::with_template(
                 "[{elapsed_precise}] [Writing XML {spinner:.blue}] {pos} items written ({per_sec})"
            ).unwrap()
        );

        // Loop through incoming items
        while let Some(item) = rx.recv().await {
            // Serialize item to XML string with the specified item tag as root
            // Using quick_xml::se::to_string_with_root is simpler than manual Serializer for each item,
            // although less performant if items are very large or numerous due to string allocation.
            // A more performant approach would use the Serializer directly on the writer,
            // but requires careful state management.
            match quick_xml::se::to_string_with_root(&self.item_tag, &item) {
                Ok(xml_string) => {
                    writer.write_all(xml_string.as_bytes())?;
                    writer.write_all(b"\n")?; // Add newline for readability
                    items_written += 1;
                    pb_items.inc(1);
                }
                Err(e) => {
                    let err_msg = format!("[XmlWriter] Failed to serialize item {:?} to XML: {}", item, e);
                    eprintln!("{}", err_msg);
                    pb_items.abandon_with_message(err_msg.clone());
                    // Depending on policy, either continue, or return error
                    // For now, let's stop on serialization error
                    return Err(Box::new(io::Error::new(io::ErrorKind::InvalidData, err_msg)));
                }
            }
        }

        // Write root end tag if provided
        if let Some(tag) = &self.root_tag {
            writer.write_all(format!("</{}>\n", tag).as_bytes())?;
        }

        writer.flush()?; // Ensure all data is written

        pb_items.finish_with_message(format!("[XmlWriter] Item writing complete. {} items written.", items_written));

        let duration = start_time.elapsed();
        println!(
            "[XmlWriter] Finished successfully in {:?}. Output: {}. Total items: {}",
            duration,
            self.final_path.display(),
            items_written
        );

        Ok(())
    }
} 