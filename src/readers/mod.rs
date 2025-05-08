pub mod line_reader;
pub mod xml;
pub mod fasta;
pub mod sql;

use std::fmt::Debug;
use tokio::sync::mpsc;

#[async_trait::async_trait]
pub trait Reader<Item>: Send + Sync
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(String) -> Item + Send + Sync + 'static>,
    ) -> mpsc::Receiver<Item>
    where
        Item: Send + Sync + 'static + Debug;
}
