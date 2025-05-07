use crate::writers::Writer;
use async_trait::async_trait;
use log::info; // Using info level for debug output, could also use debug! or println!
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::sync::mpsc::Receiver;

/// A Writer implementation that simply prints received items using Debug formatting.
/// Useful for debugging data pipelines.
#[derive(Debug, Clone)] // Clone might be useful if needed elsewhere, Debug is good practice
pub struct DebugWriter<T: Debug + Send + Sync + 'static> {
    prefix: Option<String>, // Optional prefix for each printed line
    _phantom: PhantomData<T>,
}

impl<T: Debug + Send + Sync + 'static> DebugWriter<T> {
    /// Creates a new DebugWriter.
    pub fn new() -> Self {
        Self {
            prefix: None,
            _phantom: PhantomData,
        }
    }

    /// Creates a new DebugWriter with a prefix for each output line.
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            prefix: Some(prefix.to_string()),
            _phantom: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> Writer<T> for DebugWriter<T> {
    async fn pipeline(
        &self,
        mut rx: Receiver<T>, // Receiver for items of type T
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let prefix_str = self.prefix.as_deref().unwrap_or("[DebugWriter]");
        let mut count: u64 = 0;

        info!("{} Pipeline started. Waiting for items...", prefix_str);

        while let Some(item) = rx.recv().await {
            // Print the item using its Debug implementation
            info!("{} Item {}: {:?}", prefix_str, count, item);
            count += 1;
        }

        info!(
            "{} Pipeline finished. Receiver channel closed. Total items received: {}.",
            prefix_str, count
        );
        Ok(())
    }
}

// Default implementation for easier creation without needing a prefix
impl<T: Debug + Send + Sync + 'static> Default for DebugWriter<T> {
    fn default() -> Self {
        Self::new()
    }
} 