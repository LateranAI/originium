use crate::writers::Writer;
use indicatif::MultiProgress;
use std::error::Error;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;

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
        let prefix_str = self.prefix.as_deref().unwrap_or("[DebugWriter]").to_string();
        let mut count: u64 = 0;

        let pb = mp.add(ProgressBar::new_spinner());
        let pb_template = format!(
            "{} {{elapsed_precise}} {{spinner:.magenta}} {{pos}} items debugged",
            prefix_str
        );
        pb.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .tick_chars("⠁⠂⠄⡀⢀⠠⠐⠈ "),
        );
        pb.enable_steady_tick(std::time::Duration::from_millis(100));

        mp.println(format!(
            "{} Pipeline started. Waiting for items...",
            prefix_str
        ))
        .unwrap_or_default();

        while let Some(_item) = rx.recv().await {
            count += 1;
            pb.inc(1);
        }

        let final_msg = format!(
            "{} Complete. {pos} items debugged. ({elapsed})",
            prefix_str,
            pos = count,
            elapsed = format!("{:.2?}", pb.elapsed())
        );
        pb.finish_with_message(final_msg);

        mp.println(format!(
            "{} Pipeline finished summary. Total items: {}.",
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
