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
pub struct TaskNcbiGenomeSoftlabelsRedis2Mmap {
    vocab_to_index: DashMap<&'static str, usize>,
}

impl TaskNcbiGenomeSoftlabelsRedis2Mmap {
    pub fn new() -> Self {
        let vocab_map: DashMap<&'static str, usize> = VOCABULARY.iter().cloned().collect();
        Self {
            vocab_to_index: vocab_map,
        }
    }
}

const VOCABULARY: [(&str, usize); 39] = [
    ("<eos>", 0),
    ("<pad>", 1),
    ("<gap>", 2),
    ("<unk>", 3),
    ("<msk>", 4),
    ("<fim_pre>", 5),
    ("<fim_mid>", 6),
    ("<fim_suf>", 7),
    ("A", 8),
    ("B", 9),
    ("C", 10),
    ("D", 11),
    ("E", 12),
    ("F", 13),
    ("G", 14),
    ("H", 15),
    ("I", 16),
    ("J", 17),
    ("K", 18),
    ("L", 19),
    ("M", 20),
    ("N", 21),
    ("O", 22),
    ("P", 23),
    ("Q", 24),
    ("R", 25),
    ("S", 26),
    ("T", 27),
    ("U", 28),
    ("V", 29),
    ("W", 30),
    ("X", 31),
    ("Y", 32),
    ("Z", 33),
    ("d", 34),
    ("h", 35),
    ("k", 36),
    ("n", 37),
    ("s", 38),
];

const VECTOR_DIM: usize = 39;

#[async_trait::async_trait]
impl Task for TaskNcbiGenomeSoftlabelsRedis2Mmap {
    type ReadItem = RedisJsonString;
    type ProcessedItem = MmapItem<f32>;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Redis {
            url: "redis://:ssjxzkz@10.100.1.98:6379/2".to_string(),
            key_prefix: "".to_string(),
            max_concurrent_tasks: 8192,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Mmap {
            base_path: "/public/home/ssjxzkz/Projects/originium/data".to_string(),
            filename: "softlabel".to_string(),
            num_devices: 1,
            threads_per_device: 1,
            token_unit_type: MmapTokenUnitType::F32,
            num_units_per_token: VECTOR_DIM,
            is_legacy_rwkv_format: false,
            context_length: Some(4096),
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

                let mut processed_freqs: std::collections::HashMap<String, f32> =
                    std::collections::HashMap::new();
                const MIN_FREQ: f32 = 2e-3;

                for (label, freq_value) in position_freqs_obj {
                    if let Some(freq) = freq_value.as_f64() {
                        let freq_f32 = freq as f32;
                        processed_freqs.insert(
                            label.clone(),
                            if freq_f32 == 0.0 { MIN_FREQ } else { freq_f32 },
                        );
                    } else {
                        eprintln!(
                            "Warning: Expected numeric frequency value for label '{}' in input, skipping this label.",
                            label
                        );
                    }
                }

                let special_tokens: std::collections::HashSet<&str> = [
                    "<eos>",
                    "<pad>",
                    "<gap>",
                    "<unk>",
                    "<msk>",
                    "<fim_pre>",
                    "<fim_mid>",
                    "<fim_suf>",
                ]
                .iter()
                .cloned()
                .collect();

                for (vocab_label, _index) in VOCABULARY.iter() {
                    if !special_tokens.contains(vocab_label) {
                        processed_freqs
                            .entry(vocab_label.to_string())
                            .or_insert(MIN_FREQ);
                    }
                }

                let new_total_freq: f32 = processed_freqs.values().sum();

                let mut position_vector = vec![0.0; VECTOR_DIM];

                if new_total_freq > 0.0 {
                    for (label, freq) in processed_freqs {
                        let normalized_freq = freq / new_total_freq;
                        if let Some(index_ref) = vocab_to_index.get(label.as_str()) {
                            let index = *index_ref;
                            if index < VECTOR_DIM {
                                position_vector[index] = normalized_freq;
                            } else {
                                eprintln!(
                                    "Error: Vocab index {} out of bounds for vector dimension {}. Label: '{}'",
                                    index, VECTOR_DIM, label
                                );

                                if let Some(unknown_index_ref) = vocab_to_index.get("<unk>") {
                                    let unknown_index = *unknown_index_ref;
                                    if unknown_index < VECTOR_DIM {
                                        position_vector[unknown_index] += normalized_freq;
                                    } else {
                                        eprintln!(
                                            "Error: <unk> index {} also out of bounds!",
                                            unknown_index
                                        );
                                    }
                                } else {
                                    eprintln!(
                                        "Critical Error: <unk> token not found in vocab map!"
                                    );
                                }
                            }
                        } else {
                            eprintln!(
                                "Warning: Encountered unmapped label '{}' during final vector construction, mapping to <unk>.",
                                label
                            );
                            if let Some(unknown_index_ref) = vocab_to_index.get("<unk>") {
                                let unknown_index = *unknown_index_ref;
                                if unknown_index < VECTOR_DIM {
                                    position_vector[unknown_index] += normalized_freq;
                                } else {
                                    eprintln!(
                                        "Error: <unk> index {} out of bounds!",
                                        unknown_index
                                    );
                                }
                            } else {
                                eprintln!(
                                    "Critical Error: <unk> token not found in vocab map during processing!"
                                );
                            }
                        }
                    }
                } else {
                    eprintln!(
                        "Warning: Total frequency is zero or negative after processing for a position. Resulting vector will be all zeros or invalid."
                    );
                }

                if position_vector.len() != VECTOR_DIM {
                    eprintln!(
                        "Critical Error: Position vector length mismatch. Expected {}, got {}. Skipping position.",
                        VECTOR_DIM,
                        position_vector.len()
                    );
                    continue;
                }
                all_position_vectors.extend_from_slice(&position_vector);
            }

            if !all_position_vectors.is_empty() || !softlabel_array.is_empty() {
                if let Some(eos_index_ref) = self.vocab_to_index.get("<eos>") {
                    let eos_index = *eos_index_ref;
                    if eos_index < VECTOR_DIM {
                        let mut eos_vector = vec![0.0; VECTOR_DIM];
                        eos_vector[eos_index] = 1.0;
                        all_position_vectors.extend_from_slice(&eos_vector);
                    } else {
                        eprintln!(
                            "Critical Error: <eos> index {} is out of bounds for VECTOR_DIM {}! Cannot append EOS token.",
                            eos_index, VECTOR_DIM
                        );
                    }
                } else {
                    eprintln!(
                        "Critical Error: <eos> token not found in vocab map! Cannot append EOS token."
                    );
                }
            }

            if all_position_vectors.is_empty() {
                return if softlabel_array.is_empty() {
                    eprintln!("Warning: Input softlabel_seq was empty. Returning None.");
                    Ok(None)
                } else {
                    eprintln!(
                        "Warning: Processed non-empty softlabel_seq but resulted in an empty vector. Skipping item."
                    );
                    Ok(None)
                }
            }

            if all_position_vectors.len() % VECTOR_DIM != 0 {
                eprintln!(
                    "Critical Error: Final vector length {} is not a multiple of VECTOR_DIM {}. Corrupted data? Skipping item.",
                    all_position_vectors.len(),
                    VECTOR_DIM
                );
                return Err(FrameworkError::PipelineError {
                    component_name: "TaskNcbiNrSoftlabelsRedis2Mmap::process(final_length_check)"
                        .to_string(),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Final vector length {} not multiple of {}",
                            all_position_vectors.len(),
                            VECTOR_DIM
                        ),
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
