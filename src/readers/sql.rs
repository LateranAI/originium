use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use async_trait::async_trait;
use futures::stream::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sqlx::FromRow;
use sqlx::any::{AnyPoolOptions, AnyRow};
use std::fmt::Debug;
use std::marker::{PhantomData, Unpin};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct SqlReader<Item> {
    connection_url: String,
    query: String,
    _marker: PhantomData<Item>,
}

impl<Item> SqlReader<Item>
where
    for<'r> Item: FromRow<'r, AnyRow> + Unpin + Send + Sync + 'static + Debug,
{
    pub fn new(connection_url: String, query: String) -> Self {
        eprintln!(
            "[SqlReader] Initialized for URL: {}. Query: {}",
            connection_url, query
        );
        Self {
            connection_url,
            query,
            _marker: PhantomData,
        }
    }
}

#[async_trait]
impl<Item> Reader<Item> for SqlReader<Item>
where
    for<'r> Item: FromRow<'r, AnyRow> + Unpin + Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        _read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100);
        let pool_options = AnyPoolOptions::new().max_connections(5);

        let query = self.query.clone();
        let url = self.connection_url.clone();
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            let pool = match pool_options.connect(&url).await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[SqlReader] Error connecting to database ({}): {}", url, e);
                    return;
                }
            };
            mp.println(format!("[SqlReader] Connected to database: {}", url))
                .unwrap_or_default();

            let pb_process = mp.add(ProgressBar::new_spinner());
            let pb_template = format!("[SqlReader Fetch {{elapsed_precise}}] {{spinner:.blue}} {{pos}} rows ({{per_sec}})");
            pb_process.set_style(
                 ProgressStyle::with_template(&pb_template)
                    .unwrap()
                    .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "), // Standard tick chars
            );
            pb_process.enable_steady_tick(std::time::Duration::from_millis(100));

            let mut stream = sqlx::query_as::<sqlx::Any, Item>(&query).fetch(&pool);
            let mut items_processed: u64 = 0;

            while let Some(result) = stream.next().await {
                match result {
                    Ok(item) => {
                        if tx_clone.send(item).await.is_err() {
                            eprintln!("[SqlReader] Receiver dropped. Stopping SQL query fetching.");
                            break;
                        }
                        items_processed += 1;
                        pb_process.inc(1);
                    }
                    Err(e) => {
                        eprintln!("[SqlReader] Error fetching row: {}", e);
                        pb_process.abandon_with_message(format!("Error fetching row: {}", e));
                        break;
                    }
                }
            }

            if !pb_process.is_finished() {
                let final_msg = format!(
                    "[SqlReader Fetch] Complete. {pos} rows fetched. ({elapsed})",
                    pos = items_processed,
                    elapsed = format!("{:.2?}", pb_process.elapsed())
                );
                pb_process.finish_with_message(final_msg);
            }
            mp.println(format!("[SqlReader] Disconnecting from database: {}", url))
                .unwrap_or_default();
            pool.close().await;
        });

        rx
    }
}
