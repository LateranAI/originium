use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::utils::common_type::{LineInput, MmapItem, MmapTokenUnitType};
use crate::utils::tokenizer::Tokenizer;
use crate::writers::debug::DebugWriter;
use crate::writers::mmap::MmapWriter;
use serde::Deserialize;
use serde_json;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct TextRecord {
    pub text: String,
}

#[derive(Clone)]
pub struct TaskRwkvJsonl2Mmap {
    tokenizer: Arc<Tokenizer>,
}

impl TaskRwkvJsonl2Mmap {
    pub fn new() -> Self {
        let tokenizer = Arc::new(
            Tokenizer::new("/public/home/ssjxzkz/Projects/originium/assets/vocab_v20230424.txt")
                .expect("Failed to create tokenizer"),
        );
        Self { tokenizer }
    }
}

#[async_trait::async_trait]
impl Task for TaskRwkvJsonl2Mmap {
    type ReadItem = LineInput;
    type ProcessedItem = MmapItem<u16>;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/lm/OptimalScale_ClimbLab/merged_output.jsonl".to_string(),
            format: LineFormat::Jsonl,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Mmap {
            base_path: "/public/home/ssjxzkz/Datasets/lm/OptimalScale_ClimbLab/mmap".to_string(),
            filename: "rwkv_data".to_string(),
            num_devices: 1,
            threads_per_device: num_cpus::get().max(1),
            token_unit_type: MmapTokenUnitType::U16,
            token_unit_len: 1,
            is_legacy_rwkv_format: false,
            context_length: Some(4096),
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
                    component_name: "TaskRwkvJsonl2Mmap::process".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("JSON line parsing failed: {}. Line: {}", e, json_line_str),
                    )),
                });
            }
        };

        let tokens = self.tokenizer.encode(&text_record.text, true);
        Ok(Some(MmapItem { tokens }))
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Mmap { .. } => {
                let writer = MmapWriter::<Self::ProcessedItem, u16>::new(endpoint_config);
                Ok(Box::new(writer))
            }
            DataEndpoint::Debug { prefix } => {
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p.as_str()),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "get_writer in TaskRwkvJsonl2Mmap".to_string(),
            }),
        }
    }
}
