use crate::readers::Reader;
use async_trait::async_trait;
use redis::AsyncCommands; // For GET, SCAN etc.
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};
use futures::stream::{self, StreamExt};

const DEFAULT_SCAN_COUNT: u64 = 100; // Default number of keys to fetch per SCAN iteration

pub struct RedisReader<Item> {
    connection_url: String,
    key_prefix: String,
    scan_count: u64,
    _marker: PhantomData<Item>,
}

impl<Item> RedisReader<Item>
where
    Item: Send + Sync + 'static + Debug,
{
    pub fn new(connection_url: String, key_prefix: String) -> Self {
        println!(
            "[RedisReader] Initialized for URL: {}, Key Prefix: {}",
            connection_url, key_prefix
        );
        Self {
            connection_url,
            key_prefix,
            scan_count: DEFAULT_SCAN_COUNT,
            _marker: PhantomData,
        }
    }

    // Optional: Allow customizing scan_count
    #[allow(dead_code)]
    pub fn with_scan_count(mut self, scan_count: u64) -> Self {
        self.scan_count = scan_count;
        self
    }
}

#[async_trait]
impl<Item> Reader<Item> for RedisReader<Item>
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100); // Buffer size can be configured
        let connection_url = self.connection_url.clone();
        let key_prefix_pattern = format!("{}*", self.key_prefix); // SCAN pattern
        let scan_count = self.scan_count;
        let parser = Arc::new(read_fn);

        tokio::spawn(async move {
            let client = match redis::Client::open(connection_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[RedisReader] Error creating Redis client for {}: {}", connection_url, e);
                    return; // tx will be dropped, signaling error to receiver
                }
            };

            let mut con = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[RedisReader] Error connecting to Redis at {}: {}", connection_url, e);
                    return;
                }
            };
            println!("[RedisReader] Connected to Redis: {}", connection_url);

            let pb_process = ProgressBar::new_spinner();
            pb_process.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] [Scanning Redis keys {spinner:.blue}] {pos} keys processed ({per_sec})")
                    .unwrap()
            );
            pb_process.enable_steady_tick(std::time::Duration::from_millis(120));

            let mut cursor: u64 = 0;
            let mut items_processed: u64 = 0;

            loop {
                // Scan for keys
                let scan_result: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&key_prefix_pattern)
                    .arg("COUNT")
                    .arg(scan_count)
                    .query_async(&mut con)
                    .await;

                let (next_cursor, keys) = match scan_result {
                    Ok((nc, ks)) => (nc, ks),
                    Err(e) => {
                        eprintln!("[RedisReader] Error during SCAN operation: {}", e);
                        pb_process.abandon_with_message(format!("[RedisReader] SCAN error: {}", e));
                        break;
                    }
                };
                
                cursor = next_cursor;

                if !keys.is_empty() {
                    // Process keys in parallel/concurrently using FuturesUnordered
                    // For simplicity, we'll process sequentially here, but FuturesUnordered is an option for more concurrency
                    // let key_futures = FuturesUnordered::new();
                    // keys.into_iter().for_each(|key| {
                    //     // TODO: Use FuturesUnordered for concurrent GETs if performance requires it
                    // });
                    
                    for key in keys {
                        match con.get::<_, Option<String>>(&key).await {
                            Ok(Some(value_str)) => {
                                let item = parser(value_str);
                                if tx.send(item).await.is_err() {
                                    pb_process.println("[RedisReader] Receiver dropped. Stopping key processing.");
                                    cursor = 0; // Force loop to terminate
                                    break;
                                }
                                items_processed += 1;
                                pb_process.inc(1);
                            }
                            Ok(None) => {
                                pb_process.println(format!("[RedisReader] Key {} found by SCAN but GET returned None.", key));
                            }
                            Err(e) => {
                                pb_process.println(format!("[RedisReader] Error GETting key {}: {}", key, e));
                                // Decide if we should continue or break on GET error
                            }
                        }
                    }
                }

                if cursor == 0 {
                    break; // SCAN iteration complete
                }
            }

            if !pb_process.is_finished() {
                pb_process.finish_with_message(format!("[RedisReader] Finished scanning. Total keys processed: {}", items_processed));
            }
            println!("[RedisReader] Disconnecting from Redis: {}", connection_url);
            // Connection will be closed when `con` is dropped.
        });

        rx
    }
}

// Example of how this might be integrated into custom_tasks/mod.rs (conceptual)
/*
use crate::readers::redis_reader::RedisReader; // Assuming this path

// ... inside Task::run match for DataEndpoint
DataEndpoint::Redis { url, key_prefix, .. } => { // max_concurrent_tasks currently unused by reader
    Box::new(RedisReader::<Self::InputItem>::new(url.clone(), key_prefix.clone()))
}
*/ 