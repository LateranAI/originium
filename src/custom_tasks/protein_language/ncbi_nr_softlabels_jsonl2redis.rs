use crate::custom_tasks::{DataEndpoint, Task, Writer, FrameworkError};
use crate::writers::redis::RedisWriter;

use serde::{Deserialize, Serialize};
use serde_json;


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
        vec![DataEndpoint::Redis {
            url: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
            key_prefix: "softlabel:".to_string(),
            max_concurrent_tasks: 100,
        }]
    }

    fn read(
        &self,
    ) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static> {
        Box::new(|line: String| -> Self::InputItem {
            serde_json::from_str(&line).expect(&format!("Panic: JSON行解析失败: {}", line))
        })
    }

    fn process(
        &self,
    ) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static> {
        if let DataEndpoint::Redis { key_prefix, .. } = &self.outputs_info[0] {
            let key_prefix_cloned = key_prefix.clone();
            Box::new(move |item: Self::InputItem| -> Option<Self::ProcessedItem> {
                let key = format!("{}{}", key_prefix_cloned, item.id);
                let value_json = serde_json::to_string(&item).expect(&format!(
                    "Panic: InputItem (id: {}) 序列化至 JSON 以写入 Redis 失败",
                    item.id
                ));
                Some(RedisKVPair {
                    key,
                    value: value_json,
                })
            })
        } else {
            panic!("TaskNcbiNrSoftlabelsJsonl2Redis expects its first output_info to be a Redis DataEndpoint.");
        }
    }

    async fn get_writer(&self, endpoint_config: &DataEndpoint)
        -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Redis { url, key_prefix: _, max_concurrent_tasks } => {
                let writer = RedisWriter::<Self::ProcessedItem>::new(url.clone(), *max_concurrent_tasks).await
                    .map_err(|e| FrameworkError::ComponentBuildError {
                        component_type: "RedisWriter".to_string(),
                        endpoint_description: format!("{:?}", endpoint_config),
                        reason: e.to_string(),
                    })?;
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrSoftlabelsJsonl2Redis".to_string(),
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SoftLabelEntry {
    pub id: String,
    pub sequence: String,
    pub soft_label: Vec<f32>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedisKVPair {
    pub key: String,
    pub value: String,
}
