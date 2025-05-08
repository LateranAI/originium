use crate::custom_tasks::{DataEndpoint, FrameworkError, Task, Writer, LineFormat};
use crate::utils::tokenizer::Tokenizer;
use crate::writers::debug::DebugWriter;
use crate::writers::rwkv_binidx::{BinidxItem, RwkvBinidxWriter};
use serde::Deserialize;
use serde_json;
use std::sync::Arc;
use crate::utils::common_type::LineInput;

#[derive(Debug, Clone, Deserialize)]
pub struct TextRecord {
    pub text: String,
}

pub struct TaskRwkvJsonlBindix {
    pub inputs_info: Vec<DataEndpoint>,
    pub outputs_info: Vec<DataEndpoint>,
    tokenizer: Arc<Tokenizer>,
}

impl TaskRwkvJsonlBindix {
    pub fn new(vocab_path: &str) -> Self {
        let tokenizer = Arc::new(Tokenizer::new(vocab_path).expect("Failed to create tokenizer"));
        Self {
            inputs_info: Self::get_inputs_info(),
            outputs_info: Self::get_outputs_info(),
            tokenizer,
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskRwkvJsonlBindix {
    type InputItem = LineInput;
    type ProcessedItem = BinidxItem;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "./data/input.jsonl".to_string(),
            format: LineFormat::Jsonl,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::RwkvBinidx {
            base_path: "./data/output".to_string(),
            filename_prefix: "rwkv_data".to_string(),
            num_threads: num_cpus::get().max(1),
        }]
    }

    fn read(
        &self,
    ) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static> {
        Box::new(|line_str: String| -> Self::InputItem { 
            LineInput { content: line_str } 
        })
    }

    fn process(
        &self,
    ) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static> {
        let tokenizer_clone = Arc::clone(&self.tokenizer);
        Box::new(
            move |input_item: Self::InputItem| -> Option<Self::ProcessedItem> {
                let json_line_str = input_item.content;
                
                let text_record: TextRecord = serde_json::from_str(&json_line_str).unwrap_or_else(|e| {
                    panic!("Panic: JSON line parsing failed in process: {}. Line: {}", e, json_line_str)
                });

                let tokens = tokenizer_clone.encode(&text_record.text, true);
                Some(BinidxItem { tokens })
            },
        )
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::RwkvBinidx {
                base_path,
                filename_prefix,
                num_threads,
            } => {
                println!("Configuring RwkvBinidxWriter for output.");
                let writer =
                    RwkvBinidxWriter::new(base_path.clone(), filename_prefix.clone(), *num_threads)
                        .map_err(|e| FrameworkError::ComponentBuildError {
                            component_type: "RwkvBinidxWriter".to_string(),
                            endpoint_description: format!("{:?}", endpoint_config),
                            reason: e.to_string(),
                        })?;
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
