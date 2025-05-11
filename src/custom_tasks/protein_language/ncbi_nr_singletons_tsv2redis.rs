use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::writers::redis::RedisWriter;

use crate::TEST_MODE;
use crate::utils::common_type::{LineInput, RedisKVPair};
use crate::writers::debug::DebugWriter;
use redis::AsyncCommands;
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Clone)]
pub struct TaskNcbiNrSingletonsTsvToRedis {
    id_counter: Arc<AtomicUsize>,
    seq_query_redis_conn: redis::aio::MultiplexedConnection,
}

impl TaskNcbiNrSingletonsTsvToRedis {
    pub async fn new() -> Result<Self, FrameworkError> {
        let initial_id_counter_val = 0;

        let client = redis::Client::open("redis://:ssjxzkz@10.100.1.98:6379/0").map_err(|e| {
            FrameworkError::ComponentBuildError {
                component_type: "RedisClientForSeqQuery".to_string(),
                endpoint_description: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                reason: e.to_string(),
            }
        })?;

        let seq_query_redis_conn =
            client
                .get_multiplexed_tokio_connection()
                .await
                .map_err(|e| FrameworkError::ComponentBuildError {
                    component_type: "MultiplexedRedisConnectionForSeqQuery".to_string(),
                    endpoint_description: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                    reason: e.to_string(),
                })?;

        println!(
            "[Task:NcbiNrSingletonsTsvToRedis] Initialized. Protein sequence query Redis (DB0: {}). Output softlabel ID counter starts at {}.",
            "redis://:ssjxzkz@10.100.1.98:6379/0", initial_id_counter_val
        );

        Ok(Self {
            id_counter: Arc::new(AtomicUsize::new(initial_id_counter_val)),
            seq_query_redis_conn,
        })
    }
}

#[async_trait::async_trait]
impl Task for TaskNcbiNrSingletonsTsvToRedis {
    type ReadItem = LineInput;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/prot/ncbi_nr/processed/nr.singletons.tsv"
                .to_string(),
            format: LineFormat::Tsv,
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
        let tsv_line = input_item.content.trim();
        if tsv_line.is_empty() {
            return Ok(None);
        }

        let protein_id_from_tsv = tsv_line.split("\t").next().unwrap_or(tsv_line).to_string();
        if protein_id_from_tsv.is_empty() {
            return Ok(None);
        }

        let mut conn_for_query = self.seq_query_redis_conn.clone();

        let seq_query_key = format!("{}{}", "protein:", protein_id_from_tsv);

        let protein_seq_result: Result<Option<String>, redis::RedisError> =
            conn_for_query.get(&seq_query_key).await;

        match protein_seq_result {
            Ok(Some(protein_seq)) => {
                let protein_id_list = vec![protein_id_from_tsv.clone()];

                let mut softlabel_seq = Vec::new();
                for char_val in protein_seq.chars() {
                    softlabel_seq.push(serde_json::json!({ char_val.to_string(): 1 }));
                }

                let output_json_value = serde_json::json!({
                    "protein_id_list": protein_id_list,
                    "softlabel_seq": softlabel_seq,
                });

                let value_as_string = serde_json::to_string(&output_json_value).map_err(|e| {
                    FrameworkError::PipelineError {
                        component_name:
                            "TaskNcbiNrSingletonsTsvToRedis::process (json serialization)"
                                .to_string(),
                        source: Box::new(e),
                    }
                })?;

                let current_id = self.id_counter.fetch_add(1, Ordering::SeqCst);
                let (_, key_prefix, _) = Self::get_outputs_info().get(0).unwrap().unwrap_redis();
                let output_key = format!("{}{}", key_prefix, current_id);

                Ok(Some(RedisKVPair {
                    key: output_key,
                    value: value_as_string,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                eprintln!(
                    "[Task:NcbiNrSingletonsTsvToRedis] Redis GET error for protein ID '{}' (key: '{}'): {:?}. Skipping item.",
                    protein_id_from_tsv, seq_query_key, e
                );
                Err(FrameworkError::PipelineError {
                    component_name: format!(
                        "TaskNcbiNrSingletonsTsvToRedis::process (redis GET for key {})",
                        seq_query_key
                    ),
                    source: Box::new(e),
                })
            }
        }
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
                println!("[Task:NcbiNrSingletonsTsvToRedis] Configuring DebugWriter for output.");
                let writer = match prefix {
                    Some(p_string) => {
                        DebugWriter::<Self::ProcessedItem>::with_prefix(p_string.as_str())
                    }
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrSingletonsTsvToRedis"
                    .to_string(),
            }),
        }
    }
}
