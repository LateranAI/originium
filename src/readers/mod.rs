pub mod line;
pub mod xml;
pub mod fasta;
pub mod sql;
pub mod redis;

use crate::custom_tasks::InputItem;
use std::fmt::Debug;
use tokio::sync::mpsc;
use std::sync::Arc;
use indicatif::MultiProgress;

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
