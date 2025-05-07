use crate::custom_tasks::{DataEndpoint, Task, Writer, FrameworkError};
use crate::utils::tokenizer::Tokenizer;
use crate::writers::rwkv_binidx::{BinidxItem, RwkvBinidxWriter};
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::Arc;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    type InputItem = TextRecord;
    type ProcessedItem = BinidxItem;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Jsonl {
            path: "./data/input.jsonl".to_string(),
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
        Box::new(|line: String| -> Self::InputItem {
            serde_json::from_str(&line).unwrap_or_else(|e| {
                panic!("Panic: JSON line parsing failed: {}. Line: {}", e, line)
            })
        })
    }

    fn process(
        &self,
    ) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static> {
        let tokenizer_clone = Arc::clone(&self.tokenizer);
        Box::new(move |item: Self::InputItem| -> Option<Self::ProcessedItem> {
            let tokens = tokenizer_clone.encode(&item.text, true);
            Some(BinidxItem { tokens })
        })
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::RwkvBinidx { base_path, filename_prefix, num_threads } => {
                let writer = RwkvBinidxWriter::new(
                    Path::new(base_path),
                    filename_prefix,
                    *num_threads,
                ).map_err(|e| FrameworkError::ComponentBuildError {
                    component_type: "RwkvBinidxWriter".to_string(),
                    endpoint_description: format!("{:?}", endpoint_config),
                    reason: e.to_string(),
                })?;
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: format!("Writer creation in TaskRwkvJsonlBindix for ProcessedItem type BinidxItem"),
            }),
        }
    }
}