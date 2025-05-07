use super::Writer;
use crate::custom_tasks::protein_language::ncbi_nr_softlabels_jsonl2redis::RedisKVPair;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use log::debug;
use redis::aio::MultiplexedConnection;
use redis::AsyncCommands;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex};
use std::marker::PhantomData;

pub struct RedisWriter<T> {
    pub client: Arc<Mutex<MultiplexedConnection>>,
    pub max_concurrent_tasks: usize,
    _phantom: PhantomData<T>,
}

impl<T> RedisWriter<T> {
    pub async fn new(
        redis_url: String,
        max_concurrent_tasks: usize,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = redis::Client::open(redis_url)?;
        let conn = client.get_multiplexed_tokio_connection().await?;
        Ok(RedisWriter {
            client: Arc::new(Mutex::new(conn)),
            max_concurrent_tasks,
            _phantom: PhantomData,
        })
    }
}

#[async_trait]
impl<T: Send + Sync + 'static> Writer<T> for RedisWriter<T>
where
    T: Into<RedisKVPair>,
    RedisKVPair: Send + Sync + 'static,
{
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut workers = FuturesUnordered::new();
        let max_concurrent_tasks = self.max_concurrent_tasks;

        loop {
            while workers.len() >= max_concurrent_tasks {
                workers.next().await;
            }

            if rx.is_closed() && workers.is_empty() {
                debug!("Receiver closed and all workers finished.");
                break;
            }

            tokio::select! {
                biased;
                Some(_) = workers.next(), if !workers.is_empty() => {}
                maybe_item_t = rx.recv(), if !workers.is_empty() || workers.len() < max_concurrent_tasks => {
                    match maybe_item_t {
                        Some(output_item_t) => {
                            let output_item: RedisKVPair = output_item_t.into();
                            let key = output_item.key;
                            let value = output_item.value;

                            let client_clone = Arc::clone(&self.client);
                            workers.push(tokio::spawn(async move {
                                let mut conn = client_clone.lock().await;

                                let _: () = conn.set(key.clone(), value)
                                    .await
                                    .expect(&format!("Redis SET failed for key: {}", key));
                                debug!("Redis SET successful for key: {}", key);
                            }));
                        }
                        None => {
                            debug!("Receiver channel closed. Draining workers.");
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
