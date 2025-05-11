use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::utils::common_type::LineInput;
use crate::utils::tokenizer::Tokenizer;
use crate::writers::debug::DebugWriter;
use crate::writers::mmap::{MmapBinidxItem, MmapBinidxWriter};
use serde::Deserialize;
use serde_json;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct TextRecord {
    pub text: String,
}

#[derive(Clone)]
pub struct TaskRwkvJsonlBindix {
    pub inputs_info: Vec<DataEndpoint>,
    pub outputs_info: Vec<DataEndpoint>,
    tokenizer: Arc<Tokenizer>,
}

impl TaskRwkvJsonlBindix {
    pub fn new() -> Self {
        let tokenizer = Arc::new(
            Tokenizer::new("/public/home/ssjxzkz/Projects/originium/assets/vocab_v20230424.txt")
                .expect("Failed to create tokenizer"),
        );
        Self {
            inputs_info: Self::get_inputs_info(),
            outputs_info: Self::get_outputs_info(),
            tokenizer,
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskRwkvJsonlBindix {
    type ReadItem = LineInput;
    type ProcessedItem = MmapBinidxItem;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "./data/input.jsonl".to_string(),
            format: LineFormat::Jsonl,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Mmap {
            base_path: "./data/output".to_string(),
            filename_prefix: "rwkv_data".to_string(),
            num_threads: num_cpus::get().max(1),
        }]
    }

    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static> {
        Box::new(|input_item: InputItem| -> Self::ReadItem {
            match input_item {
                InputItem::String(line_str) => LineInput { content: line_str },
                _ => panic!("Expected InputItem::String, got {:?}", input_item),
            }
        })
    }

    async fn process(
        &self,
        input_item: Self::ReadItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError> {
        let json_line_str = input_item.content;

        let text_record: TextRecord = match serde_json::from_str(&json_line_str) {
            Ok(record) => record,
            Err(e) => {
                return Err(FrameworkError::PipelineError {
                    component_name: "TaskRwkvJsonlBindix::process".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("JSON line parsing failed: {}. Line: {}", e, json_line_str),
                    )),
                });
            }
        };

        let tokens = self.tokenizer.encode(&text_record.text, true);
        Ok(Some(MmapBinidxItem { tokens }))
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Mmap {
                base_path,
                filename_prefix,
                num_threads,
            } => {
                println!("Configuring RwkvBinidxWriter for output.");
                let writer =
                    MmapBinidxWriter::new(base_path.clone(), filename_prefix.clone(), *num_threads);
                Ok(Box::new(writer))
            }
            DataEndpoint::Debug { prefix } => {
                println!("Configuring DebugWriter for output.");
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p.as_str()),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "get_writer in TaskRwkvJsonlBindix".to_string(),
            }),
        }
    }
}
