use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use redis::AsyncCommands;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc;

const DEFAULT_SCAN_COUNT: u64 = 100;
const DEFAULT_MAX_CONCURRENT_GETS: usize = 10;

pub struct RedisReader<Item> {
    connection_url: String,
    key_prefix: String,
    scan_count: u64,
    max_concurrent_gets: usize,
    _marker: PhantomData<Item>,
}

impl<Item> RedisReader<Item>
where
    Item: Send + Sync + 'static + Debug,
{
    pub fn new(connection_url: String, key_prefix: String, max_concurrent_gets: usize) -> Self {
        eprintln!(
            "[RedisReader] Initialized for URL: {}, Key Prefix: {}, Max Concurrent GETs: {}",
            connection_url, key_prefix, max_concurrent_gets
        );
        Self {
            connection_url,
            key_prefix,
            scan_count: DEFAULT_SCAN_COUNT,
            max_concurrent_gets: if max_concurrent_gets == 0 {
                DEFAULT_MAX_CONCURRENT_GETS
            } else {
                max_concurrent_gets
            },
            _marker: PhantomData,
        }
    }

    pub fn with_scan_count(mut self, scan_count: u64) -> Self {
        self.scan_count = scan_count;
        self
    }
}

#[async_trait]
impl<Item> Reader<Item> for RedisReader<Item>
where
    Item: Send + Sync + 'static + Debug + Clone,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.max_concurrent_gets * 2);
        let connection_url = self.connection_url.clone();
        let key_prefix_pattern = format!("{}*", self.key_prefix);
        let scan_count = self.scan_count;
        let parser = Arc::new(read_fn);
        let max_concurrent_gets = self.max_concurrent_gets;

        tokio::spawn(async move {
            let client = match redis::Client::open(connection_url.as_str()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "[RedisReader] Error creating Redis client for {}: {}",
                        connection_url, e
                    );

                    return;
                }
            };

            let mut scan_con = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!(
                        "[RedisReader] Error connecting to Redis for SCAN at {}: {}",
                        connection_url, e
                    );
                    return;
                }
            };
            mp.println(format!(
                "[RedisReader] Connected to Redis: {}",
                connection_url
            ))
            .unwrap_or_default();

            let pb_process = mp.add(ProgressBar::new_spinner());
            pb_process.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] [Scanning Redis {spinner:.blue}] {pos} items processed ({per_sec}) | Keys: {len}")
                    .unwrap()
                    .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
            );
            pb_process.enable_steady_tick(std::time::Duration::from_millis(100));

            let mut cursor: u64 = 0;
            let mut items_processed: u64 = 0;
            let mut total_keys_scanned: u64 = 0;

            loop {
                let scan_result: redis::RedisResult<(u64, Vec<String>)> = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(&key_prefix_pattern)
                    .arg("COUNT")
                    .arg(scan_count)
                    .query_async(&mut scan_con)
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
                total_keys_scanned += keys.len() as u64;
                pb_process.set_length(total_keys_scanned);

                if !keys.is_empty() {
                    let key_futures = stream::iter(keys)
                        .map(|key| {
                            let mut task_con = scan_con.clone();
                            let tx_clone = tx.clone();
                            let parser_clone = parser.clone();
                            let pb_clone = pb_process.clone();

                            async move {
                                match task_con.get::<_, Option<String>>(&key).await {
                                    Ok(Some(value_str)) => {
                                        let item = parser_clone(InputItem::String(value_str));
                                        if tx_clone.send(item).await.is_err() {
                                            return Err(());
                                        }

                                        Ok(())
                                    }
                                    Ok(None) => {
                                        pb_clone.println(format!("[RedisReader] Key {} found by SCAN but GET returned None.", key));
                                        Err(())
                                    }
                                    Err(e) => {
                                        pb_clone.println(format!("[RedisReader] Error GETting key {}: {}", key, e));
                                        Err(())
                                    }
                                }
                            }
                        })
                        .buffer_unordered(max_concurrent_gets);

                    let results: Vec<Result<(), ()>> = key_futures.collect().await;
                    for result in results {
                        if result.is_ok() {
                            items_processed += 1;
                            pb_process.inc(1);
                        }
                    }

                    if tx.is_closed() {
                        pb_process.println("[RedisReader] Main channel closed by receiver. Stopping key processing.");
                        break;
                    }
                }

                if cursor == 0 {
                    break;
                }
            }

            if !pb_process.is_finished() {
                pb_process.finish_with_message(format!("[RedisReader] Finished scanning. Total items processed: {}. Total keys scanned: {}", items_processed, total_keys_scanned));
            }
            mp.println(format!(
                "[RedisReader] Disconnecting from Redis: {}",
                connection_url
            ))
            .unwrap_or_default();
        });

        rx
    }
}

/*
use crate::readers::redis_reader::RedisReader;


DataEndpoint::Redis { url, key_prefix, max_concurrent_tasks } => {
    Box::new(RedisReader::<Self::ReadItem>::new(url.clone(), key_prefix.clone(), *max_concurrent_tasks))
}
*/
