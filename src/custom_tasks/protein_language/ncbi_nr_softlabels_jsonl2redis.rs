use crate::custom_tasks::{DataEndpoint, FrameworkError, Task, Writer};
use crate::writers::redis::RedisWriter;

use crate::writers::debug::DebugWriter;
use crate::TEST_MODE;
use serde::{Deserialize, Serialize};
use serde_json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

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
    type InputItem = SoftLabelEntry;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Jsonl {
            path: "/public/home/ssjxzkz/Datasets/prot/ncbi_nr/processed/nr.softlabel.jsonl"
                .to_string(),
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
        Box::new(|line: String| -> Self::InputItem {
            let line: serde_json::Value = serde_json::from_str(&line).expect(&format!("Panic: JSON行解析失败: {}", line));
            SoftLabelEntry {
                protein_id_list: line.get(0).unwrap().clone(),
                softlabel_seq: line.get(1).unwrap().clone(),
            }
        })
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
            move |item: Self::InputItem| -> Option<Self::ProcessedItem> {
                let current_id = id_counter.fetch_add(1, Ordering::SeqCst);
                let key = format!("{}{}", key_prefix_cloned, current_id);
                let value_json = serde_json::to_string(&item).expect(&format!(
                    "Panic: InputItem (id_list: {:?}, seq: {:?}) 序列化至 JSON 以写入 Redis 失败",
                    item.protein_id_list,
                    item.softlabel_seq,
                ));
                Some(RedisKVPair {
                    key,
                    value: value_json,
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
                println!("Configuring DebugWriter for output."); // Added log
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                // Since ProcessedItem must impl Debug for DebugWriter<T>,
                // and Task requires ProcessedItem: Debug, this should work.
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SoftLabelEntry {
    pub protein_id_list: serde_json::Value,
    pub softlabel_seq: serde_json::Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedisKVPair {
    pub key: String,
    pub value: String,
}

// Implement Display for RedisKVPair
impl std::fmt::Display for RedisKVPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Define how RedisKVPair should be displayed as a string.
        // Example: "key -> value". Adjust format as needed.
        write!(f, "{} -> {}", self.key, self.value)
    }
}
