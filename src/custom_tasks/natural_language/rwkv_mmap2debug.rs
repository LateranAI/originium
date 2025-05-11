use crate::custom_tasks::{DataEndpoint, FrameworkError, InputItem, Task, Writer}; 
use crate::utils::common_type::LineInput; // Ensure LineInput is available and derives FromRow, DeserializeOwned
use crate::writers::debug::DebugWriter;
use crate::writers::mmap::MmapBinidxItem; // This is our target logical item for processing
use serde_json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

const MAX_ITEMS_TO_PRINT: usize = 5;

#[derive(Clone)]
pub struct TaskRwkvMmap2Debug {
    item_counter: Arc<AtomicUsize>,
}

impl TaskRwkvMmap2Debug {
    pub fn new() -> Self {
        Self {
            item_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Task for TaskRwkvMmap2Debug {
    // ReadItem is LineInput, which satisfies FromRow and DeserializeOwned.
    // MmapReader will output InputItem::String containing JSON of Vec<u16>.
    // Our read() fn will wrap this string into LineInput.
    type ReadItem = LineInput; 
    // ProcessedItem is what we actually want to debug print.
    type ProcessedItem = MmapBinidxItem; 

    fn get_inputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Mmap {
            // Paths as per TaskRwkvJsonl2Mmap's output configuration
            base_path: "/public/home/ssjxzkz/Projects/rhineai/data/target/datasets.bin".to_string(), 
            filename: "rwkv_data".to_string(),         
            num_threads: 1, // Not directly used by MmapReader for its core logic but part of DataEndpoint::Mmap
        }]
    }

    fn get_outputs_info() -> Vec<DataEndpoint> {
        vec![DataEndpoint::Debug {
            prefix: Some("[TaskMmap2Debug] Item: ".to_string()),
        }]
    }

    // This fn converts InputItem from reader (which is InputItem::String with JSON tokens) to Self::ReadItem (LineInput)
    fn read(&self) -> Box<dyn Fn(InputItem) -> Self::ReadItem + Send + Sync + 'static> {
        Box::new(|input_item: InputItem| -> Self::ReadItem {
            match input_item {
                InputItem::String(json_str) => {
                    LineInput { content: json_str }
                }
                _ => panic!(
                    "TaskMmap2Debug: Expected InputItem::String from MmapReader, got {:?}",
                    input_item
                ),
            }
        })
    }

    // This fn takes Self::ReadItem (LineInput) and converts to Option<Self::ProcessedItem> (Option<MmapBinidxItem>)
    async fn process(
        &self,
        item: Self::ReadItem, // item is LineInput, item.content is the JSON string of Vec<u16>
    ) -> Result<Option<Self::ProcessedItem>, FrameworkError> {
        let current_count = self.item_counter.fetch_add(1, AtomicOrdering::Relaxed);
        if current_count < MAX_ITEMS_TO_PRINT {
            // Deserialize the JSON string from LineInput.content into Vec<u16>
            let tokens: Vec<u16> = serde_json::from_str(&item.content)
                .map_err(|e| FrameworkError::TransformError {
                    item_description: format!("LineInput content (first 100 chars): {:.100}", item.content),
                    reason: format!("TaskMmap2Debug: Failed to deserialize Vec<u16> from JSON: {}", e)
                })?;
            // Construct the MmapBinidxItem (our ProcessedItem)
            Ok(Some(MmapBinidxItem { tokens }))
        } else {
            if current_count == MAX_ITEMS_TO_PRINT { // Log only once when limit is reached
                 println!("[TaskMmap2Debug] Processed {} items. Further items will be filtered out from printing.", MAX_ITEMS_TO_PRINT);
            }
            Ok(None) // Filter out subsequent items by returning None
        }
    }

    async fn get_writer(
        &self,
        endpoint_config: &DataEndpoint,
    ) -> Result<Box<dyn Writer<Self::ProcessedItem>>, FrameworkError> {
        match endpoint_config {
            DataEndpoint::Debug { prefix } => {
                let writer = match prefix {
                    Some(p) => DebugWriter::<Self::ProcessedItem>::with_prefix(p.as_str()),
                    None => DebugWriter::<Self::ProcessedItem>::new(),
                };
                Ok(Box::new(writer))
            }
            _ => Err(FrameworkError::UnsupportedEndpointType {
                endpoint_description: format!("{:?}", endpoint_config),
                operation_description: "get_writer in TaskMmap2Debug".to_string(),
            }),
        }
    }
} 