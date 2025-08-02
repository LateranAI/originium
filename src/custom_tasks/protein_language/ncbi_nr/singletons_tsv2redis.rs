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

        let outputs_info = Self::get_outputs_info();
        let output_endpoint = outputs_info.get(0).ok_or_else(|| {
            FrameworkError::ComponentBuildError {
                component_type: "TaskNcbiNrSingletonsTsvToRedis".to_string(),
                endpoint_description: "Output Redis (DB1)".to_string(),
                reason: "Failed to get output Redis endpoint info from get_outputs_info."
                    .to_string(),
            }
        })?;

        let (output_redis_url, output_key_prefix) = match output_endpoint {
            DataEndpoint::Redis { url, key_prefix, .. } => (url.clone(), key_prefix.clone()),
            _ => {
                return Err(FrameworkError::ComponentBuildError {
                    component_type: "TaskNcbiNrSingletonsTsvToRedis".to_string(),
                    endpoint_description: format!("Output Redis (DB1) expected, got {:?}", output_endpoint),
                    reason: "First output endpoint is not a Redis endpoint.".to_string(),
                });
            }
        };

        let output_redis_client = redis::Client::open(output_redis_url.as_str()).map_err(|e| {
            FrameworkError::ComponentBuildError {
                component_type: "RedisClientForMaxIdScan (DB1)".to_string(),
                endpoint_description: output_redis_url.clone(),
                reason: e.to_string(),
            }
        })?;

        let mut conn_for_max_id_scan = output_redis_client
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| FrameworkError::ComponentBuildError {
                component_type: "MultiplexedRedisConnectionForMaxIdScan (DB1)".to_string(),
                endpoint_description: output_redis_url.clone(),
                reason: e.to_string(),
            })?;

        let mut max_id: Option<usize> = None;
        let scan_match_pattern = format!("{}*", output_key_prefix);
        let mut iter: redis::AsyncIter<String> = conn_for_max_id_scan
            .scan_match(&scan_match_pattern)
            .await
            .map_err(|e| FrameworkError::PipelineError { 
                component_name: "TaskNcbiNrSingletonsTsvToRedis::new (SCAN setup)".to_string(),
                source: Box::new(e),
            })?;

        while let Some(key_str) = iter.next_item().await {
            if let Some(id_str) = key_str.strip_prefix(&output_key_prefix) {
                if let Ok(current_key_id) = id_str.parse::<usize>() {
                    max_id = Some(max_id.map_or(current_key_id, |m_id| m_id.max(current_key_id)));
                }
            }
        }

        let initial_id_counter_val = max_id.map_or(0, |id| id + 1);

        println!(
            "[Task:NcbiNrSingletonsTsvToRedis] Determined initial softlabel ID counter for DB1 (prefix: '{}') to: {}. (Max existing was: {:?})",
            output_key_prefix, initial_id_counter_val, max_id
        );


        let client_db0 = redis::Client::open("redis://:ssjxzkz@10.100.1.98:6379/0").map_err(|e| {
            FrameworkError::ComponentBuildError {
                component_type: "RedisClientForSeqQuery (DB0)".to_string(),
                endpoint_description: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                reason: e.to_string(),
            }
        })?;

        let seq_query_redis_conn_db0 = client_db0
            .get_multiplexed_tokio_connection()
            .await
            .map_err(|e| FrameworkError::ComponentBuildError {
                component_type: "MultiplexedRedisConnectionForSeqQuery (DB0)".to_string(),
                endpoint_description: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                reason: e.to_string(),
            })?;

        println!(
            "[Task:NcbiNrSingletonsTsvToRedis] Initialized. Protein sequence query Redis (DB0: {}). Output softlabel ID counter (DB1) starts at {}.",
            "redis://:ssjxzkz@10.100.1.98:6379/0", initial_id_counter_val
        );


        Ok(Self {
            id_counter: Arc::new(AtomicUsize::new(initial_id_counter_val)),
            seq_query_redis_conn: seq_query_redis_conn_db0, 
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
                
                let outputs_info_for_key = Self::get_outputs_info();
                let output_endpoint_for_key = outputs_info_for_key
                    .get(0)
                    .ok_or_else(|| FrameworkError::PipelineError {
                        component_name: "TaskNcbiNrSingletonsTsvToRedis::process".to_string(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Could not get output info for key_prefix",
                        )),
                    })?;
                
                let key_prefix_from_outputs = match output_endpoint_for_key {
                    DataEndpoint::Redis { key_prefix, .. } => key_prefix.clone(),
                    _ => {
                        return Err(FrameworkError::PipelineError {
                            component_name: "TaskNcbiNrSingletonsTsvToRedis::process".to_string(),
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("First output endpoint is not Redis, cannot get key_prefix. Got: {:?}", output_endpoint_for_key),
                            )),
                        });
                    }
                };

                let output_key = format!("{}{}", key_prefix_from_outputs, current_id);

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
