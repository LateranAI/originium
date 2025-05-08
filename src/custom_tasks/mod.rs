pub mod protein_language;
mod natural_language;

use crate::errors::FrameworkError;

use serde::Deserialize;
use std::fmt::{Debug, Display};

// use sqlx::FromRow; // No longer needed here
// use sqlx::any::AnyRow; // No longer needed here

use crate::readers::Reader;
use crate::writers::Writer;

use serde::de::DeserializeOwned;
use serde::Serialize;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::readers::jsonl::JsonlReader;
use crate::readers::line_reader::FileReader;
use crate::readers::xml::XmlReader;
use crate::readers::fasta::FastaReader;
// use crate::readers::sql::SqlReader; // Commented out as SqlReader usage is disabled

// use crate::writers::xml::XmlWriter; // Unused import
// use crate::writers::fasta::FastaWriter; // Unused import
// use crate::writers::sql::{SqlWriter, SqlBindable}; // Unused imports

#[async_trait::async_trait]
pub trait Task: Send + Sync + 'static {
    type InputItem: Send + Sync + 'static + Debug + Clone + DeserializeOwned + Unpin;
    type ProcessedItem: Send + Sync + 'static + Debug + Clone + Serialize + Display;

    fn get_inputs_info() -> Vec<DataEndpoint>;
    fn get_outputs_info() -> Vec<DataEndpoint>;

    fn read(&self) -> Box<dyn Fn(String) -> Self::InputItem + Send + Sync + 'static>;

    fn process(&self) -> Box<dyn Fn(Self::InputItem) -> Option<Self::ProcessedItem> + Send + Sync + 'static>;

    async fn get_writer(&self, endpoint_config: &DataEndpoint)
        -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError>;

    async fn run(&self) -> Result<(), FrameworkError> {
        println!("Starting Task (Framework Run V7)");
        let start_time = std::time::Instant::now();

        let input_configs = Self::get_inputs_info();
        let output_configs = Self::get_outputs_info();

        if input_configs.is_empty() {
            println!("No input endpoints configured. Task will not process any data.");
            return Ok(());
        }

        let (main_input_broker_tx, mut main_input_broker_rx) =
            mpsc::channel::<Self::InputItem>(100);
        let mut reader_handles = FuturesUnordered::new();

        for input_config in input_configs {
            let reader_instance: Box<dyn Reader<Self::InputItem>> = match &input_config {
                DataEndpoint::Jsonl { path } => {
                    Box::new(JsonlReader::new(path.clone()))
                }
                DataEndpoint::File { path } => {
                    Box::new(FileReader::new(path.clone()))
                }
                DataEndpoint::Xml { path } => {
                    let record_tag = "record".to_string();
                    Box::new(XmlReader::new(path.clone(), record_tag))
                }
                DataEndpoint::Fasta { path } => {
                    Box::new(FastaReader::new(path.clone()))
                }
                DataEndpoint::Postgres { url: _url, table: _table } => {
                    // let query = format!("SELECT * FROM {}", table);
                    // Box::new(SqlReader::<Self::InputItem>::new(url.clone(), query))
                    // Temporarily commented out to allow compilation without global FromRow
                    return Err(FrameworkError::UnsupportedEndpointType {
                        endpoint_description: format!("SQL Reader (Postgres) for {:?} pending FromRow solution for Task::InputItem", input_config),
                        operation_description: "Automated reader creation in Task::run".to_string(),
                    });
                }
                DataEndpoint::MySQL { url: _url, table: _table } => {
                    // let query = format!("SELECT * FROM {}", table);
                    // Box::new(SqlReader::<Self::InputItem>::new(url.clone(), query))
                    // Temporarily commented out to allow compilation without global FromRow
                    return Err(FrameworkError::UnsupportedEndpointType {
                        endpoint_description: format!("SQL Reader (MySQL) for {:?} pending FromRow solution for Task::InputItem", input_config),
                        operation_description: "Automated reader creation in Task::run".to_string(),
                    });
                }
                _ => {
                    return Err(FrameworkError::UnsupportedEndpointType {
                        endpoint_description: format!("{:?}", input_config),
                        operation_description: "Automated reader creation in Task::run".to_string(),
                    });
                }
            };
            let read_fn = self.read();

            let mut reader_output_rx = reader_instance.pipeline(read_fn).await;

            let tx_clone = main_input_broker_tx.clone();
            let config_desc_for_error = format!("{:?}", input_config);

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
                }
                Ok(())
            });
            reader_handles.push(forward_handle);
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

            for output_config in &output_configs {
                let writer_instance = self.get_writer(output_config).await?;
                
                let (tx_to_writer, rx_for_writer_pipeline) = mpsc::channel::<Self::ProcessedItem>(100);

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

            let process_fn = self.process();

            let transform_task_handle = tokio::spawn(async move {
                while let Some(input_item) = main_input_broker_rx.recv().await {
                    if let Some(output_item) = process_fn(input_item.clone()) {
                        for (_output_config, tx_to_writer) in &transform_targets {
                            if tx_to_writer.send(output_item.clone()).await.is_err() {
                                let err_msg = format!("Writer channel closed for output {:?}. Cannot send transformed item.", _output_config);
                                eprintln!("FrameworkError: {}", err_msg);

                                return Err(FrameworkError::ChannelSendError {
                                    channel_description: format!(
                                        "writer channel for output {:?}",
                                        _output_config
                                    ),
                                    error_message: "Receiver dropped or writer task failed"
                                        .to_string(),
                                });
                            }
                        }
                    }
                }
                Ok(())
            });
            processing_handles.push(transform_task_handle);
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
        println!("Task finished successfully in {:?}.", duration);
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type")]
pub enum DataEndpoint {
    Debug {
        prefix: Option<String>,
    },
    File {
        path: String,
    },
    Jsonl {
        path: String,
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
    pub fn unwrap_file(&self) -> String {
        if let DataEndpoint::File { path } = self {
            path.clone()
        } else {
            panic!("Called unwrap_file() on non-File endpoint");
        }
    }

    pub fn unwrap_jsonl(&self) -> String {
        if let DataEndpoint::Jsonl { path } = self {
            path.clone()
        } else {
            panic!("Called unwrap_jsonl() on non-Jsonl endpoint");
        }
    }

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
