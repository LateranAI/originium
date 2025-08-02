use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, LineFormat, Task, Writer};
use crate::utils::common_type::LineInput;
use crate::writers::debug::DebugWriter;
use serde::Deserialize;
use serde_json;
use std::fmt::{self, Display};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize)]
pub struct TextRecord {
    pub text: String,
}

#[derive(Clone)]
pub struct TaskBlockBLMJsonl2Freq {
    freq: Arc<Vec<AtomicU64>>, // 0-255 计数器
    output_path: String,
}

impl TaskBlockBLMJsonl2Freq {
    /// 使用默认文件名 `byte_freq.csv`
    pub fn new() -> Self {
        Self::with_path("byte_freq.csv")
    }

    pub fn with_path<P: Into<String>>(path: P) -> Self {
        let counters = (0..256).map(|_| AtomicU64::new(0)).collect::<Vec<_>>();
        Self {
            freq: Arc::new(counters),
            output_path: path.into(),
        }
    }
}

// 用于满足 Task 关联类型要求，但本任务不会真正输出任何数据
#[derive(Debug, Clone, serde::Serialize)]
pub struct EmptyOutput;

impl Display for EmptyOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<empty>")
    }
}

impl Drop for TaskBlockBLMJsonl2Freq {
    fn drop(&mut self) {
        // 仅在最后一个实例被销毁时打印累计结果
        if Arc::strong_count(&self.freq) == 1 {
            let snapshot: Vec<u64> = self
                .freq
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect();
            // 写入 CSV: byte,count\n
            match std::fs::write(
                &self.output_path,
                snapshot
                    .iter()
                    .enumerate()
                    .map(|(byte, cnt)| format!("{},{}\n", byte, cnt))
                    .collect::<String>(),
            ) {
                Ok(_) => println!("[ByteFreq] 已写入统计结果到 {}", self.output_path),
                Err(e) => println!("[ByteFreq] 写文件失败 {}: {}", self.output_path, e),
            }
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskBlockBLMJsonl2Freq {
    type ReadItem = LineInput;
    type ProcessedItem = EmptyOutput;

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::LineDelimited {
            path: "/public/home/ssjxzkz/Datasets/lm/OptimalScale_ClimbLab/merged_output.jsonl".to_string(),
            format: LineFormat::Jsonl,
            line_limit: None,
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        // 仅放置一个 Debug endpoint，占位但不会收到任何数据
        vec![DataEndpoint::Debug { prefix: Some("ByteFreqTask".to_string()) }]
    }

    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static> {
        Box::new(|input_item: InputItem| -> Self::ReadItem {
            match input_item {
                InputItem::String(line_str) => LineInput { content: line_str },
                _ => panic!("Expected InputItem::String, got {:?}", input_item),
            }
        })
    }

    async fn process(
        &self,
        input_item: Self::ReadItem,
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError> {
        let json_line_str = input_item.content;

        let mut text_record: TextRecord = match serde_json::from_str(&json_line_str) {
            Ok(record) => record,
            Err(e) => {
                return Err(FrameworkError::ConfigError(format!(
                    "JSON line parsing failed: {}. Line: {}",
                    e, json_line_str
                )));
            }
        };

        // 推荐使用 '\u{0003}'（ETX）作为 eos 符号，兼容 UTF-8
        text_record.text.push('\u{0003}');

        for &b in text_record.text.as_bytes() {
            self.freq[b as usize].fetch_add(1, Ordering::Relaxed);
        }

        // 不向后续管道发送任何数据
        Ok(None)
    }

    async fn get_writer(
        &self,
        _endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        // 使用 DebugWriter<EmptyOutput> 作为占位
        Ok(Box::new(DebugWriter::<EmptyOutput>::new()))
    }
}