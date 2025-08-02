use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::utils::common_type::{LineInput, MmapItem, MmapTokenUnitType};
use crate::writers::debug::DebugWriter;
use crate::writers::mmap::MmapWriter;
use serde::Deserialize;
use serde_json;

#[derive(Debug, Clone, Deserialize)]
pub struct TextRecord {
    pub text: String,
}

#[derive(Clone)]
pub struct TaskBlockBLMJsonl2Mmap {}

impl TaskBlockBLMJsonl2Mmap {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Task for TaskBlockBLMJsonl2Mmap {
    type ReadItem = LineInput;
    type ProcessedItem = MmapItem<u8>;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/lm/OptimalScale_ClimbLab/merged_output.jsonl".to_string(),
            format: LineFormat::Jsonl,
            line_limit: Some(5_000),
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Mmap {
            base_path: "/public/home/ssjxzkz/Datasets/lm/OptimalScale_ClimbLab/mmap".to_string(),
            filename: "block_blm_partial_data".to_string(),
            num_devices: 1,
            threads_per_device: 1,
            token_unit_type: MmapTokenUnitType::U8,
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

        let mut text_record: TextRecord = match serde_json::from_str(&json_line_str) {
            Ok(record) => record,
            Err(e) => {
                return Err(FrameworkError::ConfigError(
                    format!("JSON line parsing failed: {}. Line: {}", e, json_line_str),
                ));
            }
        };

        // 推荐使用 '\u{0003}'（ETX）作为 eos 符号，兼容 UTF-8
        text_record.text.push('\u{0003}');

        // 转为 UTF-8 字节流：Vec<u8>
        let bytes: Vec<u8> = text_record.text.as_bytes().to_vec();

        Ok(Some(MmapItem { tokens: bytes }))
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Mmap { .. } => {
                // Ensure we are matching the primary (first) endpoint for MmapWriter if that's intended
                // For now, this branch might not be hit if Debug is always first.
                let writer = MmapWriter::<Self::ProcessedItem, u8>::new(endpoint_config);
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
                operation_description: "get_writer in TaskBlockBLMJsonl2Mmap".to_string(),
            }),
        }
    }
}