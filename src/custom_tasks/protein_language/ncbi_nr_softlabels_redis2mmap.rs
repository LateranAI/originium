use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, Task, Writer};
use crate::utils::common_type::{MmapItem, MmapTokenUnitType};
use crate::writers::debug::DebugWriter;
use crate::writers::mmap::MmapWriter;

use dashmap::DashMap;
use serde::Deserialize;
use serde_json::Value;
use sqlx::FromRow;
use std::fmt::Debug;
use std::panic::{catch_unwind, AssertUnwindSafe};

#[derive(Debug, Clone, Deserialize, FromRow)]
pub struct RedisJsonString {
    pub content: String,
}

#[derive(Clone)]
pub struct TaskNcbiNrSoftlabelsRedis2Mmap {
    vocab_to_index: DashMap<&'static str, usize>,
}

impl TaskNcbiNrSoftlabelsRedis2Mmap {
    pub fn new() -> Self {
        let vocab_map: DashMap<&'static str, usize> = VOCABULARY_TO_INDEX.iter().cloned().collect();
        Self {
            vocab_to_index: vocab_map,
        }
    }
}

const VOCABULARY_TO_INDEX: [(&str, usize); 30] = [
    ("A", 0),
    ("R", 1),
    ("N", 2),
    ("D", 3),
    ("C", 4),
    ("Q", 5),
    ("E", 6),
    ("G", 7),
    ("H", 8),
    ("I", 9),
    ("L", 10),
    ("K", 11),
    ("M", 12),
    ("F", 13),
    ("P", 14),
    ("S", 15),
    ("T", 16),
    ("W", 17),
    ("Y", 18),
    ("V", 19),
    ("B", 20),
    ("Z", 21),
    ("X", 22),
    ("U", 23),
    ("O", 24),
    ("J", 25),
    ("<end>", 26),
    ("<pad>", 27),
    ("<gap>", 28),
    ("<unknown>", 29),
];

const VECTOR_DIM: usize = 64;

#[async_trait::async_trait]
impl Task for TaskNcbiNrSoftlabelsRedis2Mmap {
    type ReadItem = RedisJsonString;
    type ProcessedItem = MmapItem<f32>;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Redis {
            url: "redis://:ssjxzkz@10.100.1.98:6379/1".to_string(),
            key_prefix: "softlabel:".to_string(),
            max_concurrent_tasks: 512,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![
            DataEndpoint::Mmap {
                base_path: "/public/home/ssjxzkz/Projects/originium/data".to_string(),
                filename: "softlabel".to_string(),
                num_threads: num_cpus::get().max(1),
                token_unit_type: MmapTokenUnitType::F32,
                token_unit_len: VECTOR_DIM,
                is_legacy_rwkv_format: false,
            },
        ]
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
        let result = catch_unwind(AssertUnwindSafe(|| {
            let json_string = input_item.content;

            let parsed_value: Value = match serde_json::from_str(&json_string) {
                Ok(val) => val,
                Err(e) => {
                    return Err(FrameworkError::PipelineError {
                        component_name: "TaskNcbiNrSoftlabelsRedis2Mmap::process(json_parse)"
                            .to_string(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Failed to parse JSON from Redis value: {}", e),
                        )),
                    });
                }
            };

            let softlabel_seq = match parsed_value.get("softlabel_seq") {
                Some(seq) => seq,
                None => {
                    return Ok(None);
                }
            };

            let softlabel_array = match softlabel_seq.as_array() {
                Some(arr) => arr,
                None => {
                    return Err(FrameworkError::PipelineError {
                        component_name: "TaskNcbiNrSoftlabelsRedis2Mmap::process(array_cast)"
                            .to_string(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "softlabel_seq is not a JSON array".to_string(),
                        )),
                    });
                }
            };

            let mut all_position_vectors: Vec<f32> = Vec::new();

            let vocab_to_index = &self.vocab_to_index;

            for position_freqs_value in softlabel_array {
                let position_freqs_obj = match position_freqs_value.as_object() {
                    Some(obj) => obj,
                    None => {
                        eprintln!("Warning: Expected object in softlabel_seq array, skipping.");
                        continue;
                    }
                };

                let total_freq: f32 = position_freqs_obj
                    .values()
                    .filter_map(|v| v.as_f64())
                    .sum::<f64>() as f32;

                let mut position_vector = vec![0.0; VECTOR_DIM];

                if total_freq > 0.0 {
                    for (label, freq_value) in position_freqs_obj {
                        if let Some(freq) = freq_value.as_f64() {
                            let normalized_freq = (freq as f32) / total_freq;

                            if let Some(index_ref) = vocab_to_index.get(label.as_str()) {
                                let index = *index_ref;
                                if index < VECTOR_DIM {
                                    position_vector[index] = normalized_freq;
                                } else {
                                    eprintln!("Error: Vocab index {} out of bounds for vector dimension {}. Label: '{}'", index, VECTOR_DIM, label);
                                    if let Some(unknown_index_ref) = vocab_to_index.get("<unknown>") {
                                        if *unknown_index_ref < VECTOR_DIM {
                                            position_vector[*unknown_index_ref] += normalized_freq;
                                        } else {
                                            eprintln!("Error: <unknown> index {} also out of bounds!", *unknown_index_ref);
                                        }
                                    }
                                }
                            } else {
                                if let Some(unknown_index_ref) = vocab_to_index.get("<unknown>") {
                                    let unknown_index = *unknown_index_ref;
                                    if unknown_index < VECTOR_DIM {
                                        position_vector[unknown_index] += normalized_freq;
                                    } else {
                                        eprintln!("Error: <unknown> index {} out of bounds!", unknown_index);
                                    }
                                } else {
                                    eprintln!("Critical Error: <unknown> token not found in vocab map during processing!");
                                }
                                eprintln!(
                                    "Warning: Encountered unmapped label '{}', mapping to <unknown>.",
                                    label
                                );
                            }
                        } else {
                            eprintln!(
                                "Warning: Expected numeric frequency value for label '{}', skipping.",
                                label
                            );
                        }
                    }
                } else {
                    eprintln!("Warning: Total frequency is zero for a position. Resulting vector will be all zeros.");
                }

                if position_vector.len() != VECTOR_DIM {
                    eprintln!("Critical Error: Position vector length mismatch. Expected {}, got {}. Skipping position.", VECTOR_DIM, position_vector.len());
                    continue;
                }
                all_position_vectors.extend_from_slice(&position_vector);
            }

            if all_position_vectors.is_empty() {
                if softlabel_array.is_empty() {
                    eprintln!("Warning: Input softlabel_seq was empty. Returning None.");
                    return Ok(None);
                } else {
                    eprintln!(
                        "Warning: Processed non-empty softlabel_seq but resulted in an empty vector. Skipping item."
                    );
                    return Ok(None);
                }
            }

            if all_position_vectors.len() % VECTOR_DIM != 0 {
                eprintln!("Critical Error: Final vector length {} is not a multiple of VECTOR_DIM {}. Corrupted data? Skipping item.", all_position_vectors.len(), VECTOR_DIM);
                return Err(FrameworkError::PipelineError {
                    component_name: "TaskNcbiNrSoftlabelsRedis2Mmap::process(final_length_check)".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Final vector length {} not multiple of {}", all_position_vectors.len(), VECTOR_DIM),
                    )),
                });
            }

            Ok(Some(MmapItem {
                tokens: all_position_vectors,
            }))
        }));

        match result {
            Ok(Ok(processed_item_option)) => Ok(processed_item_option),
            Ok(Err(framework_error)) => Err(framework_error),
            Err(panic_payload) => {
                let panic_message = if let Some(s) = panic_payload.downcast_ref::<&'static str>() {
                    *s
                } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                    s.as_str()
                } else {
                    "Unknown panic payload type"
                };
                eprintln!(
                    "Worker panicked in TaskNcbiNrSoftlabelsRedis2Mmap::process: {}",
                    panic_message
                );
                Err(FrameworkError::PipelineError {
                    component_name: "TaskNcbiNrSoftlabelsRedis2Mmap::process(panic)".to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Worker panic: {}", panic_message),
                    )),
                })
            }
        }
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Mmap { .. } => {
                let writer = MmapWriter::<Self::ProcessedItem, f32>::new(endpoint_config);
                Ok(Box::new(writer))
            }
            DataEndpoint::Debug { prefix } => {
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "Writer creation in TaskNcbiNrSoftlabelsRedis2Mmap"
                    .to_string(),
            }),
        }
    }
}
