use crate::TEST_MODE;
use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, Task, Writer};

use crate::utils::common_type::{FastaItem, RedisKVPair};
use crate::writers::debug::DebugWriter;
use crate::writers::redis::RedisWriter;
use async_trait::async_trait;
use glob::glob;
use serde_json; // 新增序列化

#[derive(Clone)]
pub struct TaskNcbiGenomeSingletonsFastaToRedis;

impl TaskNcbiGenomeSingletonsFastaToRedis {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Task for TaskNcbiGenomeSingletonsFastaToRedis {
    type ReadItem = FastaItem;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        let pattern = "/public/home/ssjxzkz/Datasets/prot/ncbi_genome/singletons.fa";
        let mut inputs = Vec::new();

        match glob(pattern) {
            Ok(paths) => {
                for entry in paths {
                    match entry {
                        Ok(path_buf) => {
                            if path_buf.is_file() {
                                inputs.push(DataEndpoint::Fasta {
                                    path: path_buf.to_string_lossy().into_owned(),
                                });
                            }
                        }
                        Err(e) => eprintln!(
                            "[TaskNcbiNrEukaryotaFastaToRedis] Error matching glob pattern entry: {:?}",
                            e
                        ),
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "[TaskNcbiNrEukaryotaFastaToRedis] Failed to read glob pattern \"{}\": {:?}",
                    pattern, e
                );
            }
        }
        if inputs.is_empty() {
            eprintln!(
                "[TaskNcbiNrEukaryotaFastaToRedis] No input FASTA files found for pattern: {}. Task will not process any data.",
                pattern
            );
        } else {
            println!(
                "[TaskNcbiNrEukaryotaFastaToRedis] Found {} input FASTA files.",
                inputs.len()
            );
        }
        inputs
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        if TEST_MODE {
            vec![DataEndpoint::Debug { prefix: Some("softlabel_out:".to_string()) }]
        } else {
            vec![DataEndpoint::Redis {
                url: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                key_prefix: "softlabel:".to_string(),
                max_concurrent_tasks: 100,
            }]
        }
    }

    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static> {
        Box::new(|input_item: InputItem| -> Self::ReadItem {
            match input_item {
                InputItem::FastaItem(fasta_item) => fasta_item,
                _ => panic!("Expected InputItem::FastaItem, got {:?}", input_item),
            }
        })
    }

    async fn process(
        &self,
        input_item: Self::ReadItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError> {
        if input_item.id.is_empty() {
            eprintln!(
                "[TaskNcbiNrEukaryotaFastaToRedis::process] Received FastaItem with empty ID. Seq length: {}. Skipping.",
                input_item.seq.len()
            );
            return Ok(None);
        }

        // 生成 softlabel JSON
        let softlabel_seq = input_item
            .seq
            .chars()
            .map(|c| serde_json::json!({ c.to_string(): 1 }))
            .collect::<Vec<_>>();

        let output_json_value = serde_json::json!({
            "protein_id_list": vec![input_item.id.clone()],
            "softlabel_seq": softlabel_seq,
        });

        let value_as_string = serde_json::to_string(&output_json_value).map_err(|e| {
            FrameworkError::PipelineError {
                component_name: "TaskNcbiGenomeSingletonsFastaToRedis::process (json serialization)".to_string(),
                source: Box::new(e),
            }
        })?;

        let key = format!("softlabel:{}", input_item.id);

        Ok(Some(RedisKVPair { key, value: value_as_string }))
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
                let writer = match prefix {
                    Some(p_string) => DebugWriter::<Self::ProcessedItem>::with_prefix(p_string),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrEukaryotaFastaToRedis"
                    .to_string(),
            }),
        }
    }
}
