use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, Task, Writer};
use crate::utils::common_type::RedisKVPair;
use crate::writers::redis::RedisWriter;

use serde::Deserialize;
use sqlx::FromRow;
use crate::TEST_MODE;

#[derive(Debug, Clone, Deserialize, FromRow)]
pub struct RedisJsonString {
    pub content: String,
}

#[derive(Clone)]
pub struct TaskNcbiGenomeMixtureRedisToRedis {
    pub inputs_info: Vec<DataEndpoint>,
    pub outputs_info: Vec<DataEndpoint>,
    pub id_counter: Arc<AtomicUsize>,
}

impl TaskNcbiGenomeMixtureRedisToRedis {
    pub fn new() -> Self {
        Self {
            inputs_info: Self::get_inputs_info(),
            outputs_info: Self::get_outputs_info(),
            id_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskNcbiGenomeMixtureRedisToRedis {
    type ReadItem = RedisJsonString;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        let mut data_endpoints = vec![DataEndpoint::Redis {
            url: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
            key_prefix: "softlabel:".to_string(),
            max_concurrent_tasks: 2048,
        }];
        data_endpoints.append(&mut vec![
            DataEndpoint::Redis {
                url: "redis://:ssjxzkz@10.100.1.98:6379/1".to_string(),
                key_prefix: "softlabel:".to_string(),
                max_concurrent_tasks: 2048
            };
            2
        ]);
        data_endpoints
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Redis {
            url: "redis://:ssjxzkz@10.100.1.98:6379/2".to_string(),
            key_prefix: "softlabel:".to_string(),
            max_concurrent_tasks: 2048,
        }]
    }

    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static> {
        Box::new(|input_item: InputItem| -> Self::ReadItem {
            match input_item {
                InputItem::String(line_str) => RedisJsonString { content: line_str },
                _ => panic!("Expected InputItem::String, got {:?}", input_item),
            }
        })
    }

    async fn process(
        &self,
        input_item: Self::ReadItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError> {
        let key_prefix = if TEST_MODE {
            self.outputs_info.get(0).unwrap().unwrap_redis().1
        } else {
            "".to_string()
        };

        let current_id = self.id_counter.fetch_add(1, Ordering::SeqCst);

        Ok(Some(RedisKVPair {
            key: format!("{}{}", key_prefix, current_id),
            value: input_item.content,
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
                            component_type: "RedisWriter for TaskNcbiNrMixtureRedisToRedis"
                                .to_string(),
                            endpoint_description: format!("{:?}", endpoint_config),
                            reason: e.to_string(),
                        })?;
                Ok(Box::new(writer))
            }

            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrMixtureRedisToRedis"
                    .to_string(),
            }),
        }
    }
}
