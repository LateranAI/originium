use super::Writer;
use crate::utils::common_type::RedisKVPair;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;

pub struct RedisWriter<T> {
    pub client: MultiplexedConnection,
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
            client: conn,
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
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut workers = FuturesUnordered::new();
        let max_concurrent_tasks = self.max_concurrent_tasks;

        let pb = mp.add(ProgressBar::new_spinner());
        let pb_template = format!(
            "[RedisWriter SET {{elapsed_precise}}] {{spinner:.green}} {{pos}} items ({{per_sec}})"
        );
        pb.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb.enable_steady_tick(Duration::from_millis(100));

        loop {
            while workers.len() >= max_concurrent_tasks {
                if workers.next().await.is_none() && rx.is_closed() && workers.is_empty() {
                    break;
                }
            }

            if rx.is_closed() && workers.is_empty() {
                break;
            }

            tokio::select! {
                biased;
                _ = workers.next(), if !workers.is_empty() => {
                }
                maybe_item_t = rx.recv(), if workers.len() < max_concurrent_tasks => {
                    match maybe_item_t {
                        Some(output_item_t) => {
                            let output_item: RedisKVPair = output_item_t.into();
                            let key = output_item.key;
                            let value = output_item.value;

                            let mut task_conn = self.client.clone();
                            let pb_clone = pb.clone();
                            workers.push(tokio::spawn(async move {
                                match task_conn.set(key.clone(), value).await {
                                    Ok(()) => {
                                        pb_clone.inc(1);
                                    }
                                    Err(e) => {
                                        eprintln!("[RedisWriter] Redis SET failed for key '{}': {:?}", key, e);
                                    }
                                }
                            }));
                        }
                        None => {
                        }
                    }
                }
            }
        }
        let final_msg = format!(
            "[RedisWriter SET] Complete. {pos} items written. ({elapsed})",
            pos = pb.position(),
            elapsed = format!("{:.2?}", pb.elapsed())
        );
        pb.finish_with_message(final_msg);
        Ok(())
    }
}
