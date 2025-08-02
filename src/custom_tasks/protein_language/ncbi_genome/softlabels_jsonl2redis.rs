use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::writers::redis::RedisWriter;

use crate::TEST_MODE;
use crate::utils::common_type::{LineInput, RedisKVPair};
use crate::writers::debug::DebugWriter;
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct TaskNcbiGenomeSoftlabelsJsonl2Redis {
    pub inputs_info: Vec<DataEndpoint>,
    pub outputs_info: Vec<DataEndpoint>,
    pub id_counter: Arc<AtomicUsize>,
}

impl TaskNcbiGenomeSoftlabelsJsonl2Redis {
    pub fn new() -> Self {
        Self {
            inputs_info: Self::get_inputs_info(),
            outputs_info: Self::get_outputs_info(),
            id_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskNcbiGenomeSoftlabelsJsonl2Redis {
    type ReadItem = LineInput;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/prot/ncbi_genome/softlabel.jsonl"
                .to_string(),
            format: LineFormat::Jsonl,
            line_limit: None,
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
        let key_prefix = if TEST_MODE {
            "".to_string()
        } else {
            self.outputs_info.get(0).unwrap().unwrap_redis().1
        };

        let json_line_str = input_item.content;
        let current_id = self.id_counter.fetch_add(1, Ordering::SeqCst);
        let key = format!("{}{}", key_prefix, current_id);

        let original_json_value: serde_json::Value = match serde_json::from_str(&json_line_str) {
            Ok(val) => val,
            Err(e) => {
                return Err(FrameworkError::PipelineError {
                    component_name: "TaskNcbiNrSoftlabelsJsonl2Redis::process".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "JSON line parsing failed: {} for line: {}",
                            e, json_line_str
                        ),
                    )),
                });
            }
        };

        let output_redis_value_json = serde_json::json!({
            "protein_id_list": "?",
            "softlabel_seq": original_json_value
        });

        let value_as_string = match serde_json::to_string(&output_redis_value_json) {
            Ok(s) => s,
            Err(e) => {
                return Err(FrameworkError::PipelineError {
                    component_name: "TaskNcbiNrSoftlabelsJsonl2Redis::process".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Failed to serialize final JSON object to string for Redis: {}",
                            e
                        ),
                    )),
                });
            }
        };

        Ok(Some(RedisKVPair {
            key,
            value: value_as_string,
        }))
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
