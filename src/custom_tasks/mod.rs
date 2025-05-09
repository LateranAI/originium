mod natural_language;
pub mod protein_language;

use crate::errors::FrameworkError;

use serde::Deserialize;
use std::fmt::{Debug, Display};

use sqlx::any::AnyRow;
use sqlx::FromRow;

use crate::readers::Reader;
use crate::writers::Writer;

use serde::de::DeserializeOwned;
use serde::Serialize;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use num_cpus;
use std::env;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::readers::fasta::FastaReader;
use crate::readers::sql::SqlReader;
use crate::readers::xml::XmlReader;

const ADJUSTMENT_BATCH_SIZE: u32 = 100;

#[derive(Clone, Copy, Debug)]
struct PerfRecord {
    duration: Duration,
    concurrency: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LineFormat {
    Jsonl,
    Tsv,
    PlainText,
}

#[async_trait::async_trait]
pub trait Task: Clone + Send + Sync + 'static {
    type InputItem: Send
        + Sync
        + 'static
        + Debug
        + Clone
        + DeserializeOwned
        + Unpin
        + for<'r> FromRow<'r, AnyRow>;
    type ProcessedItem: Send + Sync + 'static + Debug + Clone + Serialize + Display;

    fn get_inputs_info() -> Vec<DataEndpoint>;
    fn get_outputs_info() -> Vec<DataEndpoint>;

    fn read(&self) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static>;

    async fn process(
        &self,
        item: Self::InputItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError>;

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError>;

    async fn run(&self) -> Result<(), FrameworkError> {
        println!("Starting Task (Framework Run V9 - Perf Metrics & Tunable Concurrency)");
        let start_time = std::time::Instant::now();

        let input_configs = Self::get_inputs_info();
        let output_configs = Self::get_outputs_info();

        if input_configs.is_empty() {
            println!("No input endpoints configured. Task will not process any data.");
            let duration = start_time.elapsed();
            println!("Task finished (no input) in {:?}.", duration);
            println!("  Total items read into broker: 0");
            println!("  Total items processed and sent to writer(s): 0");
            return Ok(());
        }

        let total_items_read_to_broker = Arc::new(AtomicUsize::new(0));
        let total_items_successfully_processed = Arc::new(AtomicUsize::new(0));

        let (main_input_broker_tx, mut main_input_broker_rx) =
            mpsc::channel::<Self::InputItem>(100);
        let mut reader_handles = FuturesUnordered::new();

        let total_items_read_to_broker_for_readers = Arc::clone(&total_items_read_to_broker);

        {
            let items_counter_clone_for_reader = Arc::clone(&total_items_read_to_broker_for_readers);
            for input_config in input_configs {
                let reader_instance: Box<dyn Reader<Self::InputItem>> = match &input_config {
                    DataEndpoint::LineDelimited { path, format } => Box::new(
                        crate::readers::line::LineReader::new(path.clone(), format.clone()),
                    ),
                    DataEndpoint::Xml { path } => {
                        let record_tag = "record".to_string();
                        Box::new(XmlReader::new(path.clone(), record_tag))
                    }
                    DataEndpoint::Fasta { path } => Box::new(FastaReader::new(path.clone())),
                    DataEndpoint::Postgres { url, table } => {
                        let query = format!("SELECT * FROM {}", table);
                        Box::new(SqlReader::<Self::InputItem>::new(url.clone(), query))
                    }
                    DataEndpoint::MySQL { url, table } => {
                        let query = format!("SELECT * FROM {}", table);
                        Box::new(SqlReader::<Self::InputItem>::new(url.clone(), query))
                    }
                    DataEndpoint::Redis {
                        url,
                        key_prefix,
                        max_concurrent_tasks,
                    } => {
                        Box::new(crate::readers::redis::RedisReader::<Self::InputItem>::new(
                            url.clone(), 
                            key_prefix.clone(), 
                            *max_concurrent_tasks
                        ))
                    }
                    DataEndpoint::RwkvBinidx {
                        base_path: _base_path,
                        filename_prefix: _filename_prefix,
                        num_threads: _num_threads,
                    } => {
                        return Err(FrameworkError::UnsupportedEndpointType {
                            endpoint_description: format!(
                                "SQL Reader (RwkvBinidx) for {:?} pending FromRow solution for Task::InputItem",
                                input_config
                            ),
                            operation_description: "Automated reader creation in Task::run".to_string(),
                        });
                    }
                    DataEndpoint::Debug { .. } => {
                        return Err(FrameworkError::UnsupportedEndpointType {
                            endpoint_description: format!(
                                "Debug endpoint cannot be used as a direct reader source in this factory version."
                            ),
                            operation_description: "Automated reader creation in Task::run".to_string(),
                        });
                    }
                };
                let read_fn = self.read();

                let mut reader_output_rx = reader_instance.pipeline(read_fn).await;

                let tx_clone = main_input_broker_tx.clone();
                let config_desc_for_error = format!("{:?}", input_config);
                let current_reader_counter_clone = Arc::clone(&items_counter_clone_for_reader);

                let forward_handle = tokio::spawn(async move {
                    while let Some(item) = reader_output_rx.recv().await {
                        if tx_clone.send(item).await.is_err() {
                            eprintln!(
                                "FrameworkError: Main input broker receiver dropped for input {}. Cannot forward item.",
                                config_desc_for_error
                            );

                            return Err(FrameworkError::ChannelSendError {
                                channel_description: format!(
                                    "main input broker from reader for {}",
                                    config_desc_for_error
                                ),
                                error_message: "Receiver dropped".to_string(),
                            });
                        }
                        current_reader_counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    Ok(())
                });
                reader_handles.push(forward_handle);
            }
        }

        drop(main_input_broker_tx);

        println!("All reader pipelines configured and forwarding tasks spawned.");

        // Handles for transformation tasks that return a count of processed items.
        let mut transform_handles =
            FuturesUnordered::<JoinHandle<Result<usize, FrameworkError>>>::new();
        
        // Handles for writer tasks that complete without a specific item count (Result<(), FrameworkError>).
        let mut writer_completion_handles = 
            FuturesUnordered::<JoinHandle<Result<(), FrameworkError>>>::new();

        if output_configs.is_empty() {
            println!("No output endpoints configured. Consuming and discarding all input items.");
            let mut count = 0;
            while let Some(_item) = main_input_broker_rx.recv().await {
                count += 1;
            }
            println!("Drained {} items from input broker.", count);
        } else {
            let mut transform_targets = Vec::new();

            let transform_targets_for_closure = transform_targets.clone();

            for output_config in &output_configs {
                let writer_instance = self.get_writer(output_config).await?;

                let (tx_to_writer, rx_for_writer_pipeline) =
                    mpsc::channel::<Self::ProcessedItem>(100);

                let component_name_for_error = format!("Writer for {:?}", output_config);
                let writer_handle = tokio::spawn(async move {
                    writer_instance
                        .pipeline(rx_for_writer_pipeline)
                        .await
                        .map_err(|e_box| FrameworkError::PipelineError {
                            component_name: component_name_for_error,
                            source: e_box,
                        })
                });

                // Add writer_handle to its specific collection.
                writer_completion_handles.push(writer_handle);

                transform_targets.push((output_config.clone(), tx_to_writer));
            }

            let task_processor = self.clone();
            
            let num_cpu_cores = num_cpus::get();
            println!(
                "[Task::run] Initializing transform and dispatch stage. Dynamic concurrency enabled. Base CPU cores: {}.", 
                num_cpu_cores
            );

            let transform_and_dispatch_handle = tokio::spawn(async move {
                let mut active_processing_tasks = FuturesUnordered::new();
                
                let task_processor_arc = Arc::new(task_processor); 
                
                let mut processed_and_sent_count: usize = 0; 

                let current_transform_targets = transform_targets_for_closure;

                let initial_concurrency = num_cpus::get().max(1);
                let min_concurrency = (initial_concurrency / 2).max(1);
                let max_concurrency = initial_concurrency * 8;
    
                let mut current_max_concurrency = initial_concurrency.clamp(min_concurrency, max_concurrency);
                let mut items_processed_current_adjustment_batch: u32 = 0;
                let mut adjustment_batch_start_time = Instant::now();
                let mut previous_perf_record: Option<PerfRecord> = None;
                let mut dynamic_adjustment_enabled: bool = true;
    
                let task_name = "ProcessingStage"; 

                println!(
                    "[Task: {}] Initializing dynamic concurrency. Start: {}, Min: {}, Max: {}",
                    task_name, current_max_concurrency, min_concurrency, max_concurrency
                );
    
                loop {
                    tokio::select! {
                        biased;

                        Some(processed_result_from_join) = active_processing_tasks.next(), if !active_processing_tasks.is_empty() => {
                            match processed_result_from_join {
                                Ok(Ok(Some(output_item))) => {
                                    let mut sent_to_at_least_one_writer = false;
                                    
                                    let concrete_output_item: <Self as Task>::ProcessedItem = output_item;
                                    let item_for_sending: <Self as Task>::ProcessedItem = concrete_output_item.clone();

                                    for (_output_config, writer_tx) in &current_transform_targets {
                                        if writer_tx.send(item_for_sending.clone()).await.is_err() {
                                            eprintln!(
                                                "[Task: {}] Receiver for writer {:?} dropped.", 
                                                task_name, _output_config
                                            );
                                        } else {
                                            sent_to_at_least_one_writer = true;
                                        }
                                    }
    
                                    if sent_to_at_least_one_writer {
                                        processed_and_sent_count += 1;
                                        items_processed_current_adjustment_batch += 1;
    
                                        if dynamic_adjustment_enabled && items_processed_current_adjustment_batch >= ADJUSTMENT_BATCH_SIZE {
                                            let current_batch_duration = adjustment_batch_start_time.elapsed();
                                            let concurrency_during_this_batch = current_max_concurrency;
                                            
                                            let current_perf = PerfRecord {
                                                duration: current_batch_duration,
                                                concurrency: concurrency_during_this_batch,
                                            };
    
                                            if let Some(prev_perf) = previous_perf_record {
                                                let prev_rate = ADJUSTMENT_BATCH_SIZE as f64 / prev_perf.duration.as_secs_f64().max(f64::EPSILON);
                                                let current_rate = ADJUSTMENT_BATCH_SIZE as f64 / current_batch_duration.as_secs_f64().max(f64::EPSILON);
                                                let old_concurrency_for_log = current_max_concurrency;
    
                                                if current_rate > prev_rate * 1.10 {
                                                    current_max_concurrency = (current_max_concurrency * 2).clamp(min_concurrency, max_concurrency);
                                                    println!(
                                                        "[Task: {}] Concurrency increased: {} -> {} (Rate: {:.2}items/s vs {:.2}items/s @ {} tasks).",
                                                        task_name, old_concurrency_for_log, current_max_concurrency, current_rate, prev_rate, prev_perf.concurrency
                                                    );
                                                } else if current_rate < prev_rate * 0.90 {
                                                    let increase_amount = if concurrency_during_this_batch > prev_perf.concurrency {
                                                        concurrency_during_this_batch - prev_perf.concurrency
                                                    } else {
                                                        0
                                                    };
    
                                                    if increase_amount > 0 {
                                                        let reduction_amount = increase_amount / 2;
                                                        current_max_concurrency = (prev_perf.concurrency + reduction_amount).clamp(min_concurrency, max_concurrency);
                                                    } else {
                                                        current_max_concurrency = (current_max_concurrency * 3 / 4).clamp(min_concurrency, max_concurrency);
                                                    }
                                                    println!(
                                                        "[Task: {}] Concurrency decreased: {} -> {} (Rate: {:.2}items/s vs {:.2}items/s @ {} tasks).",
                                                        task_name, old_concurrency_for_log, current_max_concurrency, current_rate, prev_rate, prev_perf.concurrency
                                                    );
                                                } else {
                                                    dynamic_adjustment_enabled = false;
                                                    println!(
                                                        "[Task: {}] Concurrency stable at {}. Locking. (Rate: {:.2}items/s vs {:.2}items/s @ {} tasks).",
                                                        task_name, current_max_concurrency, current_rate, prev_rate, prev_perf.concurrency
                                                    );
                                                }
                                            } else {
                                                let old_concurrency_for_log = current_max_concurrency;
                                                current_max_concurrency = (current_max_concurrency * 2).clamp(min_concurrency, max_concurrency);
                                                 println!(
                                                    "[Task: {}] Initial concurrency adjustment: {} -> {} (Batch time: {:.2}s).",
                                                    task_name, old_concurrency_for_log, current_max_concurrency, current_batch_duration.as_secs_f32()
                                                );
                                            }
                                            
                                            previous_perf_record = Some(current_perf);
                                            items_processed_current_adjustment_batch = 0;
                                            adjustment_batch_start_time = Instant::now();
                                        }
                                    }
                                }
                                Ok(Ok(None)) => {
                                }
                                Ok(Err(e)) => {
                                    eprintln!("[Task: {}] Error processing item: {:?}", task_name, e);
                                }
                                Err(join_error) => {
                                    eprintln!("[Task: {}] Panicked/cancelled processing task: {:?}", task_name, join_error);
                                }
                            }
                        },
    
                        maybe_item_from_broker = main_input_broker_rx.recv(), if active_processing_tasks.len() < current_max_concurrency => {
                            match maybe_item_from_broker {
                                Some(item_from_broker) => {
                                    let task_processor_clone = Arc::clone(&task_processor_arc);
                                    active_processing_tasks.push(tokio::spawn(async move {
                                        task_processor_clone.process(item_from_broker).await
                                    }));
                                }
                                None => {
                                    break;
                                }
                            }
                        },
                        else => { 
                            if active_processing_tasks.is_empty() && main_input_broker_rx.is_closed() {
                                break;
                            }
                        }
                    }
                }
                while let Some(processed_result_from_join) = active_processing_tasks.next().await {
                    match processed_result_from_join {
                        Ok(Ok(Some(output_item))) => {
                            let mut sent_to_at_least_one_writer = false;
                            
                            let concrete_output_item_drain: <Self as Task>::ProcessedItem = output_item;
                            let item_for_draining: <Self as Task>::ProcessedItem = concrete_output_item_drain.clone();

                            for (_output_config, writer_tx) in &current_transform_targets {
                                if writer_tx.send(item_for_draining.clone()).await.is_err() {
                                    eprintln!(
                                        "[Task: {}] Receiver for writer {:?} dropped during final drain.",
                                        task_name, _output_config
                                    );
                                } else {
                                    sent_to_at_least_one_writer = true;
                                }
                            }
                            if sent_to_at_least_one_writer {
                                processed_and_sent_count += 1;
                            }
                        }
                        Ok(Ok(None)) => { /* Skip */ }
                        Ok(Err(e)) => eprintln!("[Task: {}] Error processing item during final drain: {:?}", task_name, e),
                        Err(e) => eprintln!("[Task: {}] Panicked/cancelled task during final drain: {:?}", task_name, e),
                    }
                }
                Ok(processed_and_sent_count)
            });
            // Add transform_and_dispatch_handle to its specific collection.
            transform_handles.push(transform_and_dispatch_handle);
        }

        println!("Awaiting reader forwarding tasks...");
        while let Some(result) = reader_handles.next().await {
            match result {
                Ok(Ok(())) => { /* Task completed successfully */ }
                Ok(Err(framework_err)) => {
                    eprintln!(
                        "A reader forwarding task failed with FrameworkError: {:?}",
                        framework_err
                    );

                    return Err(framework_err);
                }
                Err(join_err) => {
                    eprintln!(
                        "A reader forwarding task panicked: {:?}. Propagating panic.",
                        join_err
                    );
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        println!("All reader forwarding tasks completed.");

        println!("Awaiting transformation tasks...");
        // Await transformation handles first, as they feed the writers.
        while let Some(result) = transform_handles.next().await {
            match result {
                Ok(Ok(processed_count_from_handle)) => { // This is usize
                    total_items_successfully_processed.fetch_add(processed_count_from_handle, AtomicOrdering::Relaxed);
                }
                Ok(Err(framework_err)) => {
                    eprintln!(
                        "A transformation task failed with FrameworkError: {:?}",
                        framework_err
                    );
                    // Depending on desired behavior, might want to signal writers to shutdown or just log and continue.
                    return Err(framework_err); // For now, exiting on transform error.
                }
                Err(join_err) => {
                    eprintln!(
                        "A transformation task panicked: {:?}. Propagating panic.",
                        join_err
                    );
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        println!("All transformation tasks completed.");

        println!("Awaiting writer completion tasks...");
        while let Some(result) = writer_completion_handles.next().await {
            match result {
                Ok(Ok(())) => { /* Writer completed successfully */ }
                Ok(Err(framework_err)) => {
                    eprintln!(
                        "A writer task failed with FrameworkError: {:?}",
                        framework_err
                    );
                    // This error means data might not have been fully written.
                    return Err(framework_err); // Exit on writer error.
                }
                Err(join_err) => {
                    eprintln!(
                        "A writer task panicked: {:?}. Propagating panic.",
                        join_err
                    );
                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        println!("All writer tasks completed.");

        let duration = start_time.elapsed();
        let final_read_count = total_items_read_to_broker.load(AtomicOrdering::Relaxed);
        let final_processed_and_sent_count = total_items_successfully_processed.load(AtomicOrdering::Relaxed);

        println!("Task finished successfully in {:?}.", duration);
        println!("  Total items read into broker: {}", final_read_count);
        println!("  Total items processed and sent to writer(s): {}", final_processed_and_sent_count);

        let duration_sec = duration.as_secs_f64();
        if duration_sec > 0.0 {
            if final_read_count > 0 {
                println!("  Approximate reader throughput: {:.2} items/sec", final_read_count as f64 / duration_sec);
            }
            if final_processed_and_sent_count > 0 {
                println!("  Approximate processing throughput (to writer): {:.2} items/sec", final_processed_and_sent_count as f64 / duration_sec);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataEndpoint {
    Debug {
        prefix: Option<String>,
    },
    LineDelimited {
        path: String,
        format: LineFormat,
    },
    Xml {
        path: String,
    },
    Fasta {
        path: String,
    },
    Postgres {
        url: String,
        table: String,
    },
    MySQL {
        url: String,
        table: String,
    },
    Redis {
        url: String,
        key_prefix: String,
        max_concurrent_tasks: usize,
    },
    RwkvBinidx {
        base_path: String,
        filename_prefix: String,
        num_threads: usize,
    },
}

impl DataEndpoint {
    pub fn unwrap_xml(&self) -> String {
        if let DataEndpoint::Xml { path } = self {
            path.clone()
        } else {
            panic!("Called unwrap_xml() on non-Xml endpoint");
        }
    }

    pub fn unwrap_fasta(&self) -> String {
        if let DataEndpoint::Fasta { path } = self {
            path.clone()
        } else {
            panic!("Called unwrap_fasta() on non-Fasta endpoint");
        }
    }

    pub fn unwrap_postgres(&self) -> (String, String) {
        if let DataEndpoint::Postgres { url, table } = self {
            (url.clone(), table.clone())
        } else {
            panic!("Called unwrap_postgres() on non-Postgres endpoint");
        }
    }

    pub fn unwrap_mysql(&self) -> (String, String) {
        if let DataEndpoint::MySQL { url, table } = self {
            (url.clone(), table.clone())
        } else {
            panic!("Called unwrap_mysql() on non-MySQL endpoint");
        }
    }

    pub fn unwrap_redis(&self) -> (String, String, usize) {
        if let DataEndpoint::Redis {
            url,
            key_prefix,
            max_concurrent_tasks,
        } = self
        {
            (url.clone(), key_prefix.clone(), *max_concurrent_tasks)
        } else {
            panic!("Called unwrap_redis() on non-Redis endpoint");
        }
    }
}
