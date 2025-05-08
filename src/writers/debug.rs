use crate::writers::Writer;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
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
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let prefix_str = self.prefix.as_deref().unwrap_or("[DebugWriter]");
        let mut count: u64 = 0;

        println!("{} Pipeline started. Waiting for items...", prefix_str);

        while let Some(item) = rx.recv().await {

            println!("{} Item {}: {:?}", prefix_str, count, item);
            count += 1;
        }

        println!(
            "{} Pipeline finished. Receiver channel closed. Total items received: {}.",
            prefix_str, count
        );
        Ok(())
    }
}


impl<T: Debug + Send + Sync + 'static> Default for DebugWriter<T> {
    fn default() -> Self {
        Self::new()
    }
} 