pub mod redis;
pub mod rwkv_binidx;
pub mod debug;
pub mod line_writer;
pub mod xml;
pub mod fasta;
pub mod sql;

use tokio::sync::mpsc::Receiver;
use std::error::Error;
use async_trait::async_trait;

#[async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: Receiver<OutputItem>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}
