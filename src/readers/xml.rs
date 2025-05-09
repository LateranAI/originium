use crate::readers::Reader;
use async_trait::async_trait;
use quick_xml::events::Event;
use quick_xml::reader::Reader as XmlQuickReader;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};

pub struct XmlReader {
    path: String,
    record_tag: String,
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



    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100);
        let file_path_str = self.path.clone();
        let record_tag_bytes = self.record_tag.as_bytes().to_vec();
        let parser = Arc::new(read_fn);


        let file_size = std::fs::metadata(&file_path_str).map(|m| m.len()).unwrap_or(0);
        let pb_process = ProgressBar::new(file_size);
        pb_process.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] [Processing XML {bar:40.blue/white}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})"
            ).unwrap()
        );




        tokio::task::spawn_blocking(move || {
            let file = match File::open(&file_path_str) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("[XmlReader] Error opening file {}: {}", file_path_str, e);
                    pb_process.finish_with_message(format!("Error opening file: {}", e));

                    return;
                }
            };

            let buf_reader = BufReader::new(file);
            let mut xml_reader = XmlQuickReader::from_reader(buf_reader);
            xml_reader.config_mut().trim_text(true);

            let mut buf = Vec::new();
            let mut record_buf = Vec::new();
            let mut depth = 0;
            let mut is_in_record = false;
            let mut item_count: u64 = 0;

            loop {
                let event_result = xml_reader.read_event_into(&mut buf);
                pb_process.set_position(xml_reader.buffer_position());

                match event_result {
                    Ok(Event::Start(ref e)) => {
                        if e.name().as_ref() == &record_tag_bytes {
                            if !is_in_record {
                                is_in_record = true;
                                depth = 1;
                                record_buf.clear();



                                record_buf.extend_from_slice(b"<");
                                record_buf.extend_from_slice(e.name().as_ref());
                                record_buf.extend_from_slice(b">" );
                            } else {

                                depth += 1;
                                if is_in_record {

                                    record_buf.extend_from_slice(b"<");
                                    record_buf.extend_from_slice(e.name().as_ref());
                                    record_buf.extend_from_slice(b">" );
                                }
                            }
                        } else if is_in_record {

                            depth += 1;
                             record_buf.extend_from_slice(b"<");
                             record_buf.extend_from_slice(e.name().as_ref());
                             record_buf.extend_from_slice(b">" );
                        }
                    }
                    Ok(Event::End(ref e)) => {
                         if is_in_record {

                            record_buf.extend_from_slice(b"</");
                            record_buf.extend_from_slice(e.name().as_ref());
                            record_buf.extend_from_slice(b">");

                            depth -= 1;
                            if depth == 0 && e.name().as_ref() == &record_tag_bytes {

                                is_in_record = false;
                                let record_xml_string = String::from_utf8(record_buf.clone());

                                match record_xml_string {
                                    Ok(xml_str) => {

                                        let item = parser(xml_str);

                                        let send_result = tx.blocking_send(item);
                                        if send_result.is_err() {
                                            eprintln!("[XmlReader] Receiver dropped. Stopping XML processing.");
                                            break;
                                        }
                                        item_count += 1;
                                    }
                                    Err(e) => {
                                        eprintln!("[XmlReader] Failed to convert record bytes to UTF-8 string: {}", e);

                                    }
                                }
                            }
                         }
                    }
                    Ok(Event::Text(e)) => {
                        if is_in_record {

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

                    }
                    Ok(Event::Empty(ref e)) => {
                         if is_in_record {

                            record_buf.extend_from_slice(b"<");
                            record_buf.extend_from_slice(e.name().as_ref());
                            record_buf.extend_from_slice(b"/>");
                         }
                    }
                    Ok(Event::Eof) => {
                        break;
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