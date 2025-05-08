use crate::readers::Reader;
use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlQuickReader; // Alias to avoid conflict
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};

pub struct XmlReader {
    path: String,
    record_tag: String, // The XML tag name that encloses each record/item
}

impl XmlReader {
    pub fn new(path: String, record_tag: String) -> Self {
        println!(
            "[XmlReader] Initialized for file: {}, extracting elements tagged <{}>",
            path, record_tag
        );
        Self { path, record_tag }
    }
}

#[async_trait]
impl<Item> Reader<Item> for XmlReader
where
    Item: DeserializeOwned + Send + Sync + 'static + Debug,
{
    // Basic single-threaded XML element extractor.
    // Passes XML fragment strings matching `record_tag` to `read_logic`.
    // Parallel XML reading is significantly more complex and not implemented here.
    async fn pipeline(
        &self,
        read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100); // Modest buffer size for single-threaded reader
        let file_path_str = self.path.clone();
        let record_tag_bytes = self.record_tag.as_bytes().to_vec(); // Convert tag to bytes for comparison
        let parser = Arc::new(read_logic);

        // Progress bar based on file size (approximates progress)
        let file_size = std::fs::metadata(&file_path_str).map(|m| m.len()).unwrap_or(0);
        let pb_process = ProgressBar::new(file_size);
        pb_process.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [Processing XML {bar:40.blue/white}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})"
            ).unwrap()
        );

        // Blocking file reading and parsing in a dedicated thread
        // because quick-xml reader can be blocking, especially with from_file.
        // Using spawn_blocking to avoid blocking the async executor.
        tokio::task::spawn_blocking(move || {
            let file = match File::open(&file_path_str) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("[XmlReader] Error opening file {}: {}", file_path_str, e);
                    pb_process.finish_with_message(format!("Error opening file: {}", e));
                    // Channel drops automatically when tx goes out of scope
                    return;
                }
            };

            let buf_reader = BufReader::new(file);
            let mut xml_reader = XmlQuickReader::from_reader(buf_reader);
            xml_reader.config_mut().trim_text(true);

            let mut buf = Vec::new();
            let mut record_buf = Vec::new(); // Buffer to reconstruct the record's XML string
            let mut depth = 0;
            let mut is_in_record = false;
            let mut item_count: u64 = 0;

            loop {
                let event_result = xml_reader.read_event_into(&mut buf);
                pb_process.set_position(xml_reader.buffer_position());

                match event_result {
                    Ok(Event::Start(ref e)) => {
                        if e.name().as_ref() == &record_tag_bytes {
                            if !is_in_record { // Start of a new top-level record
                                is_in_record = true;
                                depth = 1;
                                record_buf.clear();
                                // Write the start tag to the record buffer
                                // Note: This basic reconstruction might miss attributes/namespaces.
                                // A robust solution would use quick_xml::Writer with BytesText events.
                                record_buf.extend_from_slice(b"<");
                                record_buf.extend_from_slice(e.name().as_ref());
                                record_buf.extend_from_slice(b">" ); // Simple tag
                            } else {
                                // Nested element with the same name, increment depth
                                depth += 1;
                                if is_in_record {
                                    // Reconstruct nested tag start
                                    record_buf.extend_from_slice(b"<");
                                    record_buf.extend_from_slice(e.name().as_ref());
                                    record_buf.extend_from_slice(b">" );
                                }
                            }
                        } else if is_in_record {
                            // Nested element start
                            depth += 1;
                             record_buf.extend_from_slice(b"<");
                             record_buf.extend_from_slice(e.name().as_ref());
                             record_buf.extend_from_slice(b">" );
                        }
                    }
                    Ok(Event::End(ref e)) => {
                         if is_in_record {
                            // Write end tag to buffer before checking depth
                            record_buf.extend_from_slice(b"</");
                            record_buf.extend_from_slice(e.name().as_ref());
                            record_buf.extend_from_slice(b">");

                            depth -= 1;
                            if depth == 0 && e.name().as_ref() == &record_tag_bytes {
                                // End of the top-level record
                                is_in_record = false;
                                let record_xml_string = String::from_utf8(record_buf.clone());

                                match record_xml_string {
                                    Ok(xml_str) => {
                                        // Use the captured parser (read_logic)
                                        let item = parser(xml_str);
                                        // Send the parsed item asynchronously
                                        let send_result = tx.blocking_send(item);
                                        if send_result.is_err() {
                                            eprintln!("[XmlReader] Receiver dropped. Stopping XML processing.");
                                            break; // Stop if channel is closed
                                        }
                                        item_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("[XmlReader] Failed to convert record bytes to UTF-8 string: {}", e);
                                        // Decide whether to skip or stop
                                    }
                                }
                            }
                         }
                    }
                    Ok(Event::Text(e)) => {
                        if is_in_record {
                            // Append text content, potentially needs unescaping depending on reader config
                            record_buf.extend_from_slice(&e.into_inner());
                        }
                    }
                    Ok(Event::CData(e)) => { 
                        if is_in_record {
                            record_buf.extend_from_slice(b"<![CDATA[");
                            record_buf.extend_from_slice(&e.into_inner());
                            record_buf.extend_from_slice(b"]]>");
                        }
                    }
                    Ok(Event::Comment(_)) | Ok(Event::Decl(_)) | Ok(Event::PI(_)) | Ok(Event::DocType(_)) => {
                        // Ignore these events for now, or handle as needed
                    }
                    Ok(Event::Empty(ref e)) => {
                         if is_in_record {
                            // Handle self-closing tags within the record if needed
                            record_buf.extend_from_slice(b"<");
                            record_buf.extend_from_slice(e.name().as_ref());
                            record_buf.extend_from_slice(b"/>"); // Simple self-closing tag
                         }
                    }
                    Ok(Event::Eof) => {
                        break; // End of file
                    }
                    Err(e) => {
                        eprintln!(
                            "[XmlReader] XML parsing error at position {}: {:?}",
                            xml_reader.buffer_position(),
                            e
                        );
                        pb_process.abandon_with_message(format!("XML parsing error: {}", e));
                        break;
                    }
                }
                buf.clear();
            }
            pb_process.finish_with_message(format!("[XmlReader] Finished processing {}. Items found: {}", file_path_str, item_count));
        });

        rx
    }
} 