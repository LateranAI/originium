use crate::writers::Writer;
use indicatif::MultiProgress;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

#[derive(Debug, Clone)]
pub struct DebugWriter<T: Debug + Send + Sync + 'static> {
    prefix: Option<String>,
    _phantom: PhantomData<T>,
}

impl<T: Debug + Send + Sync + 'static> DebugWriter<T> {
    pub fn new() -> Self {
        Self {
            prefix: None,
            _phantom: PhantomData,
        }
    }

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
        mut rx: Receiver<T>,
        mp: Arc<MultiProgress>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let prefix_str = self.prefix.as_deref().unwrap_or("[DebugWriter]");
        let mut count: u64 = 0;

        mp.println(format!(
            "{} Pipeline started. Waiting for items...",
            prefix_str
        ))
        .unwrap_or_default();

        while let Some(item) = rx.recv().await {
            mp.println(format!("{} Item {}: {:?}", prefix_str, count, item))
                .unwrap_or_default();
            count += 1;
        }

        mp.println(format!(
            "{} Pipeline finished. Receiver channel closed. Total items received: {}.",
            prefix_str, count
        ))
        .unwrap_or_default();
        Ok(())
    }
}

impl<T: Debug + Send + Sync + 'static> Default for DebugWriter<T> {
    fn default() -> Self {
        Self::new()
    }
}
