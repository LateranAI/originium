pub mod debug;
pub mod fasta;
pub mod line;
pub mod mmap;
pub mod redis;
pub mod sql;
pub mod xml;

use async_trait::async_trait;
use indicatif::MultiProgress;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

#[async_trait]
pub trait Writer<OutputItem: Send + 'static>: Send + Sync {
    async fn pipeline(
        &self,
        rx: Receiver<OutputItem>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}
