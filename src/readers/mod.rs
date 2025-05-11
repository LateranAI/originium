pub mod fasta;
pub mod line;
pub mod mmap;
pub mod redis;
pub mod sql;
pub mod xml;

use crate::custom_tasks::InputItem;
use indicatif::MultiProgress;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc;

#[async_trait::async_trait]
pub trait Reader<Item>: Send + Sync
where
    Item: Send + Sync + 'static + Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item>
    where
        Item: Send + Sync + 'static + Debug;
}
