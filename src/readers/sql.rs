use crate::readers::Reader;
use async_trait::async_trait;
use sqlx::any::{AnyPoolOptions, AnyRow};
use sqlx::{FromRow};
use futures::stream::StreamExt; // For stream.next()
use std::fmt::Debug;
use tokio::sync::mpsc;
use indicatif::{ProgressBar, ProgressStyle};
use std::marker::{Unpin, PhantomData}; // Added PhantomData

pub struct SqlReader<Item> {
    connection_url: String,
    query: String, // SQL query to execute
    _marker: PhantomData<Item>, // Marker for the generic type Item
}

impl<Item> SqlReader<Item>
where
    for<'r> Item: FromRow<'r, AnyRow> + Unpin + Send + Sync + 'static + Debug,
{
    pub fn new(connection_url: String, query: String) -> Self {
        println!(
            "[SqlReader] Initialized for URL: {}. Query: {}",
            connection_url, query
        );
        Self { connection_url, query, _marker: PhantomData }
    }
}

#[async_trait]
impl<Item> Reader<Item> for SqlReader<Item>
where
    for<'r> Item: FromRow<'r, AnyRow> + Unpin + Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        _read_logic: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(100);
        let pool_options = AnyPoolOptions::new()
            .max_connections(5); // Removed connect_timeout
            
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
            println!("[SqlReader] Connected to database: {}", url);

            let pb_process = ProgressBar::new_spinner();
            pb_process.set_style(
                 ProgressStyle::with_template("[{elapsed_precise}] [Reading SQL rows {spinner:.blue}] {pos} rows fetched ({per_sec})")
                    .unwrap()
            );
            pb_process.enable_steady_tick(std::time::Duration::from_millis(120));

            // We need to specify the DB type for query_as, matching the FromRow bound (Any)
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
                 pb_process.finish_with_message(format!("[SqlReader] Finished fetching rows. Total rows: {}", items_processed));
            }
            println!("[SqlReader] Disconnecting from database: {}", url);
            pool.close().await;
        });

        rx
    }
} 