use crate::writers::Writer;
use async_trait::async_trait;
use sqlx::AnyPool;
use sqlx::any::AnyPoolOptions;
use sqlx::{Error as SqlxError, query::Query, Database};
use std::fmt::Debug;
use tokio::sync::mpsc::Receiver;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::marker::PhantomData;
use std::sync::Arc;





pub trait SqlBindable {




    fn bind_parameters<'q, DB: Database>(self, query: Query<'q, DB, DB::Arguments<'q>>) -> Query<'q, DB, DB::Arguments<'q>> 
    where Self: Sized + Send + 'q;
}

pub struct SqlWriter<T: Send + Sync + 'static + Debug + SqlBindable> {
    connection_url: String,
    table_name: String,
    column_names: Vec<String>,
    pool: AnyPool,
    _phantom: PhantomData<T>,
}

impl<T: Send + Sync + 'static + Debug + SqlBindable> SqlWriter<T> {

    pub async fn new(connection_url: String, table_name: String, column_names: Vec<String>) -> Result<Self, SqlxError> {
        if column_names.is_empty() {


             return Err(SqlxError::Configuration("Column names cannot be empty for SqlWriter".into()));
        }
        eprintln!(
            "[SqlWriter] Initializing for table '{}' at {}. Columns: {:?}",
            table_name, connection_url, column_names
        );
        let pool_options = AnyPoolOptions::new()
            .max_connections(10);
        let pool = pool_options.connect(&connection_url).await?;
        eprintln!("[SqlWriter] Connected to database.");

        Ok(Self {
            connection_url,
            table_name,
            column_names,
            pool,
            _phantom: PhantomData,
        })
    }


    fn generate_insert_sql(&self) -> String {
        let columns = self.column_names.join(", ");



        let placeholders = self.column_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        format!("INSERT INTO {} ({}) VALUES ({})", self.table_name, columns, placeholders)
    }
}

#[async_trait]
impl<T: Send + Sync + 'static + Debug + SqlBindable> Writer<T> for SqlWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let start_time = std::time::Instant::now();
        let mut items_written: u64 = 0;
        let insert_sql = self.generate_insert_sql();

        let pb_items = mp.add(ProgressBar::new_spinner());
        pb_items.enable_steady_tick(std::time::Duration::from_millis(120));
        pb_items.set_style(
            ProgressStyle::with_template(
                 "[{elapsed_precise}] [Writing SQL rows {spinner:.cyan}] {pos} rows written ({per_sec})"
            ).unwrap()
        );

        let mut batch = Vec::with_capacity(100);
        let batch_size = 100;

        while let Some(item) = rx.recv().await {
            batch.push(item);

            if batch.len() >= batch_size {
                let mut transaction = self.pool.begin().await?;
                let current_batch = std::mem::take(&mut batch);
                 mp.println(format!("[SqlWriter] Writing batch of {} items...", current_batch.len())).unwrap_or_default();
                for batch_item in current_batch {
                    let query = sqlx::query(&insert_sql);

                    let bound_query = batch_item.bind_parameters(query);
                    bound_query.execute(&mut *transaction).await.map_err(|e| {
                        pb_items.abandon_with_message(format!("SQL Error: {}", e));
                        format!("Error executing insert: {}", e)
                    })?;
                    items_written += 1;
                    pb_items.inc(1);
                }
                transaction.commit().await?;
                 mp.println(format!("[SqlWriter] Batch committed.")).unwrap_or_default();
            }
        }


        if !batch.is_empty() {
             let mut transaction = self.pool.begin().await?;
             mp.println(format!("[SqlWriter] Writing final batch of {} items...", batch.len())).unwrap_or_default();
            for batch_item in batch {
                 let query = sqlx::query(&insert_sql);
                 let bound_query = batch_item.bind_parameters(query);
                 bound_query.execute(&mut *transaction).await.map_err(|e| {
                     pb_items.abandon_with_message(format!("SQL Error: {}", e));
                     format!("Error executing insert: {}", e)
                 })?;
                 items_written += 1;
                 pb_items.inc(1);
            }
             transaction.commit().await?;
             mp.println(format!("[SqlWriter] Final batch committed.")).unwrap_or_default();
        }

        pb_items.finish_with_message(format!("[SqlWriter] Row writing complete. {} rows written.", items_written));

        let duration = start_time.elapsed();
        mp.println(format!(
            "[SqlWriter] Finished successfully in {:?}. Table: {}. Total rows: {}",
            duration,
            self.table_name,
            items_written
        )).unwrap_or_default();
        


        Ok(())
    }
}


/*
#[derive(Debug)]
struct MyItem {
    id: i32,
    name: String,
    value: Option<f64>,
}

impl SqlBindable for MyItem {
    fn bind_parameters<'q, DB: Database>(self, query: Query<'q, DB, <DB as HasArguments<'q>>::Arguments>) -> Query<'q, DB, <DB as HasArguments<'q>>::Arguments>
    where
        Self: Sized + Send + 'q,
        i32: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        String: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
        Option<f64>: sqlx::Encode<'q, DB> + sqlx::Type<DB>,
    {
        query.bind(self.id).bind(self.name).bind(self.value)
    }
}
*/ 