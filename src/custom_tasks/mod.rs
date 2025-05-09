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

use crate::readers::fasta::FastaReader;
use crate::readers::sql::SqlReader;
use crate::readers::xml::XmlReader;


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

        {
            let items_counter_clone_for_reader = Arc::clone(&total_items_read_to_broker);
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

        let mut processing_handles =
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
            let successfully_processed_counter_clone = Arc::clone(&total_items_successfully_processed);

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

                processing_handles.push(writer_handle);

                transform_targets.push((output_config.clone(), tx_to_writer));
            }

            let task_processor = self.clone();
            
            let num_cpu_cores = num_cpus::get();
            let max_concurrent_processing_ops: usize = env::var("MAX_CONCURRENT_PROCESS_OPS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| {
                    let default_ops = num_cpu_cores.saturating_mul(4).max(1);
                    println!("[Task::run] MAX_CONCURRENT_PROCESS_OPS not set or invalid, defaulting to {} ({} cores * 4)", default_ops, num_cpu_cores);
                    default_ops
                });

            let transform_and_dispatch_handle = tokio::spawn(async move {
                let mut active_processing_tasks = FuturesUnordered::new();
                let items_picked_up_for_processing = Arc::new(AtomicUsize::new(0));

                loop {
                    tokio::select! {
                        biased;

                        Some(join_result) = active_processing_tasks.next(), if !active_processing_tasks.is_empty() => {
                            match join_result {
                                Ok(process_result) => {
                                    match process_result {
                                        Ok(Some(generic_output_item)) => {
                                            let output_item: <Self as Task>::ProcessedItem = generic_output_item;
                                            let mut sent_to_at_least_one_writer = false;
                                            for (_output_config, writer_tx) in &transform_targets_for_closure {
                                                let item_to_send = output_item.clone();
                                                match writer_tx.send(item_to_send).await {
                                                    Ok(()) => { sent_to_at_least_one_writer = true; /* Successfully sent */ }
                                                    Err(_send_error) => {
                                                        let err_msg = format!("Writer channel closed for output {:?}. Cannot send item.", _output_config);
                                                        eprintln!("FrameworkError: {}", err_msg);
                                                        return Err(FrameworkError::ChannelSendError {
                                                            channel_description: format!("Writer channel for output {:?}", _output_config),
                                                            error_message: "Receiver dropped or writer task failed".to_string(),
                                                        });
                                                    }
                                                }
                                            }
                                            if sent_to_at_least_one_writer {
                                                successfully_processed_counter_clone.fetch_add(1, AtomicOrdering::Relaxed);
                                            }
                                        }
                                        Ok(None) => { /* Task::process returned Ok(None), item successfully skipped */ }
                                        Err(process_framework_error) => {
                                            eprintln!("FrameworkError from Task::process: {:?}. Aborting.", process_framework_error);
                                            return Err(process_framework_error);
                                        }
                                    }
                                }
                                Err(task_panic_error) => {
                                    eprintln!("Spawned processing task panicked: {:?}. Aborting.", task_panic_error);
                                    return Err(FrameworkError::InternalError(format!("Spawned processing task panicked: {}", task_panic_error)));
                                }
                            }
                        }

                        maybe_input_item = main_input_broker_rx.recv(), if active_processing_tasks.len() < max_concurrent_processing_ops => {
                            if let Some(input_item) = maybe_input_item {
                                items_picked_up_for_processing.fetch_add(1, AtomicOrdering::Relaxed);
                                let processor_clone = task_processor.clone();
                                active_processing_tasks.push(tokio::spawn(async move {
                                    processor_clone.process(input_item.clone()).await
                                }));
                            } else {
                                if active_processing_tasks.is_empty() {
                                    break;
                                }
                            }
                        }
                        
                        else => {
                            if active_processing_tasks.is_empty() {
                                break;
                            }
                        }
                    }
                }
                let final_picked_up_count = items_picked_up_for_processing.load(AtomicOrdering::Relaxed);
                println!("[Task::run::transform_stage] Total items picked up from broker for processing: {}", final_picked_up_count);
                Ok(())
            });
            processing_handles.push(transform_and_dispatch_handle);
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

        println!("Awaiting transformation and writer tasks...");
        while let Some(result) = processing_handles.next().await {
            match result {
                Ok(Ok(())) => { /* Task completed successfully */ }
                Ok(Err(framework_err)) => {
                    eprintln!(
                        "A processing task failed with FrameworkError: {:?}",
                        framework_err
                    );

                    return Err(framework_err);
                }
                Err(join_err) => {
                    eprintln!(
                        "A processing task panicked: {:?}. Propagating panic.",
                        join_err
                    );

                    std::panic::resume_unwind(join_err.into_panic());
                }
            }
        }
        println!("All transformation and writer tasks completed.");

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
