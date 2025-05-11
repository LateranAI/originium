use crate::TEST_MODE;
use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, Task, Writer};

use crate::utils::common_type::{FastaItem, RedisKVPair};
use crate::writers::debug::DebugWriter;
use crate::writers::redis::RedisWriter;
use async_trait::async_trait;
use glob::glob;

#[derive(Clone)]
pub struct TaskNcbiNrEukaryotaFastaToRedis;

impl TaskNcbiNrEukaryotaFastaToRedis {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Task for TaskNcbiNrEukaryotaFastaToRedis {
    type ReadItem = FastaItem;
    type ProcessedItem = RedisKVPair;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        let pattern = "/public/home/ssjxzkz/Datasets/prot/ncbi_nr/processed/eukaryota.*.fa";
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
            vec![DataEndpoint::Debug {
                prefix: Some("eukaryota_fasta_out:".to_string()),
            }]
        } else {
            vec![DataEndpoint::Redis {
                url: "redis://:ssjxzkz@10.100.1.98:6379/0".to_string(),
                key_prefix: "protein:".to_string(),
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

        let key = format!("protein:{}", input_item.id);
        let value = input_item.seq;

        Ok(Some(RedisKVPair { key, value }))
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
