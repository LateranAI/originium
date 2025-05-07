use crate::custom_tasks::{DataEndpoint, Task};

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
    type OutputItem = RedisKVPair;

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
        _endpoint_config: &DataEndpoint,
    ) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static> {
        Box::new(|line: String| -> Self::InputItem {
            serde_json::from_str(&line).expect(&format!("Panic: JSON行解析失败: {}", line))
        })
    }

    fn process(
        &self,
    ) -> Box<dyn Fn(Self::InputItem) -> Option<Self::OutputItem> + Send + Sync + 'static> {
        let (_, key_prefix_owned, _) = self.outputs_info[0].unwrap_redis();

        Box::new(move |item: Self::InputItem| -> Option<Self::OutputItem> {
            let key = format!("{}{}", key_prefix_owned, item.id);
            let value_json = serde_json::to_string(&item).expect(&format!(
                "Panic: InputItem (id: {}) 序列化至 JSON 以写入 Redis 失败",
                item.id
            ));
            Some(RedisKVPair {
                key,
                value: value_json,
            })
        })
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
