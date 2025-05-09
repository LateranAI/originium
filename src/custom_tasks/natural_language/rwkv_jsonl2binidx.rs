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

#[derive(Clone)]
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

    async fn process(
        &self,
        input_item: Self::InputItem,
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
        Ok(Some(BinidxItem { tokens }))
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
