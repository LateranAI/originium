pub mod redis;
pub mod rwkv_binidx;
pub mod debug;
pub mod line;
pub mod xml;
pub mod fasta;
pub mod sql;

use tokio::sync::mpsc::Receiver;
use std::error::Error;
use async_trait::async_trait;
use std::sync::Arc;
use indicatif::MultiProgress;

#[async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: Receiver<OutputItem>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}
