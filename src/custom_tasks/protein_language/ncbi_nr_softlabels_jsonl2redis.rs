use crate::custom_tasks::{DataEndpoint, FrameworkError, Task, Writer, LineFormat};
use crate::writers::redis::RedisWriter;
use std::fmt::Display;

use crate::utils::common_type::{LineInput, RedisKVPair};
use crate::writers::debug::DebugWriter;
use crate::TEST_MODE;
use serde::Deserialize;
use serde_json;
use sqlx::FromRow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, FromRow, Deserialize)]
pub struct TextLine {
    pub value: String,
}

pub struct TaskNcbiNrSoftlabelsJsonl2Redis {
    pub inputs_info: Vec<DataEndpoint>,
    pub outputs_info: Vec<DataEndpoint>,
}

impl TaskNcbiNrSoftlabelsJsonl2Redis {
    pub fn new() -> Self {
        Self {
            inputs_info: Self::get_inputs_info(),
            outputs_info: Self::get_outputs_info(),
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskNcbiNrSoftlabelsJsonl2Redis {
    type InputItem = LineInput;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/prot/ncbi_nr/processed/nr.softlabel.jsonl"
                .to_string(),
            format: LineFormat::Jsonl,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        if TEST_MODE {
            vec![DataEndpoint::Debug { prefix: None }]
        } else {
            vec![DataEndpoint::Redis {
                url: "redis://:ssjxzkz@10.100.1.98:6379/1".to_string(),
                key_prefix: "softlabel:".to_string(),
                max_concurrent_tasks: 100,
            }]
        }
    }

    fn read(&self) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static> {
        Box::new(|line_str: String| -> Self::InputItem { LineInput { content: line_str } })
    }

    fn process(
        &self,
    ) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static> {
        let key_prefix_cloned = if let DataEndpoint::Redis { key_prefix, .. } =
            &self.outputs_info[0]
        {
            key_prefix.clone()
        } else {
            if TEST_MODE {
                "".to_string()
            } else {
                panic!(
                    "TaskNcbiNrSoftlabelsJsonl2Redis requires a Redis endpoint configured as the first output to determine key_prefix."
                );
            }
        };

        let id_counter = Arc::new(AtomicUsize::new(0));

        Box::new(
            move |input_item: Self::InputItem| -> Option<Self::ProcessedItem> {
                let json_line_str = input_item.content;
                let current_id = id_counter.fetch_add(1, Ordering::SeqCst);
                let key = format!("{}{}", key_prefix_cloned, current_id);

                let original_json_value: serde_json::Value = serde_json::from_str(&json_line_str)
                    .unwrap_or_else(|e| {
                        panic!(
                            "Panic: JSON行解析失败 in process: {} for line: {}",
                            e, json_line_str
                        )
                    });

                let protein_id_val = original_json_value
                    .get(0)
                    .expect("Missing protein_id_list in JSON array");
                let softlabel_seq_val = original_json_value
                    .get(1)
                    .expect("Missing softlabel_seq in JSON array");

                let output_redis_value_json = serde_json::json!({
                    "protein_id_list": protein_id_val,
                    "softlabel_seq": softlabel_seq_val
                });

                let value_as_string = serde_json::to_string(&output_redis_value_json)
                    .expect("Failed to serialize final JSON object to string for Redis");

                Some(RedisKVPair {
                    key,
                    value: value_as_string,
                })
            },
        )
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Redis {
                url,
                key_prefix: _,
                max_concurrent_tasks,
            } => {
                let writer =
                    RedisWriter::<Self::ProcessedItem>::new(url.clone(), *max_concurrent_tasks)
                        .await
                        .map_err(|e| FrameworkError::ComponentBuildError {
                            component_type: "RedisWriter".to_string(),
                            endpoint_description: format!("{:?}", endpoint_config),
                            reason: e.to_string(),
                        })?;
                Ok(Box::new(writer))
            }
            DataEndpoint::Debug { prefix } => {
                println!("Configuring DebugWriter for output.");
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrSoftlabelsJsonl2Redis"
                    .to_string(),
            }),
        }
    }
}
