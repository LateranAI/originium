use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use async_trait::async_trait;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::{Mmap, MmapOptions};
use moka::sync::Cache;
use serde_json;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 100;

const MMAP_BINIDX_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const MMAP_BINIDX_VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
const MMAP_BINIDX_DTYPE: [u8; 1] = [8u8];

#[derive(Debug)]
struct IdxFileHeader {
    magic: [u8; 9],
    version: u64,
    data_type: u8,
    num_items: u64,
    doc_indices_len: u64,
}

pub struct MmapReader<Item> {
    bin_mmap: Arc<Mmap>,
    item_token_counts: Arc<Vec<u32>>,
    item_byte_offsets: Arc<Vec<u64>>,

    document_item_indices: Arc<Vec<u64>>,
    num_items: usize,
    item_cache: Cache<usize, Arc<Vec<u16>>>,
    channel_buffer_size: usize,
    _marker: PhantomData<Item>,
}

fn get_item_tokens_from_data(
    item_index: usize,
    num_items: usize,
    bin_mmap: &Mmap,
    item_token_counts: &[u32],
    item_byte_offsets: &[u64],
    item_cache: &Cache<usize, Arc<Vec<u16>>>,
) -> Option<Arc<Vec<u16>>> {
    if item_index >= num_items {
        return None;
    }

    if let Some(cached_tokens) = item_cache.get(&item_index) {
        return Some(cached_tokens);
    }

    let token_count = item_token_counts.get(item_index).copied().unwrap_or(0) as usize;
    let byte_offset = item_byte_offsets.get(item_index).copied().unwrap_or(0) as usize;

    if byte_offset >= bin_mmap.len() || token_count == 0 {
        return None;
    }

    let byte_length = token_count.checked_mul(size_of::<u16>())?;
    let end_offset = byte_offset.checked_add(byte_length)?;

    if end_offset > bin_mmap.len() {
        return None;
    }

    let token_bytes_slice = bin_mmap.get(byte_offset..end_offset)?;
    let tokens_vec: Vec<u16> = token_bytes_slice
        .chunks_exact(std::mem::size_of::<u16>())
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    let arc_tokens = Arc::new(tokens_vec);
    item_cache.insert(item_index, arc_tokens.clone());
    Some(arc_tokens)
}

impl<Item> MmapReader<Item>
where
    Item: Send + Sync + 'static + std::fmt::Debug,
{
    pub fn new(base_path_str: String, filename_prefix: String) -> Self {
        let base_path = PathBuf::from(base_path_str);
        let bin_file_path = base_path.join(format!("{}.bin", filename_prefix));
        let idx_file_path = base_path.join(format!("{}.idx", filename_prefix));

        let mut idx_file_handle = File::open(&idx_file_path).expect(&format!(
            "Configuration error: Failed to open index file {}",
            idx_file_path.display()
        ));

        idx_file_handle
            .seek(SeekFrom::Start(0))
            .expect("Failed to seek in index file");

        let mut read_magic = [0u8; 9];
        idx_file_handle
            .read_exact(&mut read_magic)
            .expect("Failed to read magic header from index file");
        assert_eq!(
            read_magic, MMAP_BINIDX_MAGIC_HDR,
            "Index file magic header mismatch"
        );

        let mut read_version_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut read_version_buf)
            .expect("Failed to read version from index file");
        assert_eq!(
            read_version_buf, MMAP_BINIDX_VERSION,
            "Index file version mismatch"
        );

        let mut read_data_type_buf = [0u8; 1];
        idx_file_handle
            .read_exact(&mut read_data_type_buf)
            .expect("Failed to read data type from index file");
        assert_eq!(
            read_data_type_buf, MMAP_BINIDX_DTYPE,
            "Index file data type mismatch"
        );

        let mut num_items_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut num_items_buf)
            .expect("Failed to read num_items from index file");
        let num_items = u64::from_le_bytes(num_items_buf) as usize;

        let mut doc_indices_len_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut doc_indices_len_buf)
            .expect("Failed to read doc_indices_len from index file");
        let doc_indices_len = u64::from_le_bytes(doc_indices_len_buf) as usize;

        let mut item_token_counts_vec = Vec::with_capacity(num_items);
        for _ in 0..num_items {
            let mut buf = [0u8; 4];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read item token count");
            item_token_counts_vec.push(u32::from_le_bytes(buf));
        }

        let mut item_byte_offsets_vec = Vec::with_capacity(num_items);
        for _ in 0..num_items {
            let mut buf = [0u8; 8];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read item byte offset");
            item_byte_offsets_vec.push(u64::from_le_bytes(buf));
        }

        let mut document_item_indices_vec = Vec::with_capacity(doc_indices_len);
        for _ in 0..doc_indices_len {
            let mut buf = [0u8; 8];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read document item index");
            document_item_indices_vec.push(u64::from_le_bytes(buf));
        }

        let bin_file_handle = File::open(&bin_file_path).expect(&format!(
            "Configuration error: Failed to open binary file {}",
            bin_file_path.display()
        ));
        let bin_mmap_instance = unsafe {
            MmapOptions::new()
                .map(&bin_file_handle)
                .expect("Failed to mmap binary file")
        };

        let item_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(600))
            .max_capacity(10_000)
            .build();

        Self {
            bin_mmap: Arc::new(bin_mmap_instance),
            item_token_counts: Arc::new(item_token_counts_vec),
            item_byte_offsets: Arc::new(item_byte_offsets_vec),
            document_item_indices: Arc::new(document_item_indices_vec),
            num_items,
            item_cache,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            _marker: PhantomData,
        }
    }


    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.channel_buffer_size = if buffer_size == 0 {
            DEFAULT_CHANNEL_BUFFER_SIZE
        } else {
            buffer_size
        };
        self
    }


    pub fn len(&self) -> usize {
        self.num_items
    }
}

#[async_trait]
impl<Item> Reader<Item> for MmapReader<Item>
where
    Item: Send + Sync + 'static + std::fmt::Debug,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.channel_buffer_size);
        let parser = Arc::new(read_fn);

        let bin_mmap_clone = Arc::clone(&self.bin_mmap);
        let item_token_counts_clone = Arc::clone(&self.item_token_counts);
        let item_byte_offsets_clone = Arc::clone(&self.item_byte_offsets);
        let item_cache_clone = self.item_cache.clone();
        let num_items_clone = self.num_items;

        tokio::spawn(async move {
            mp.println(format!(
                "[MmapReader] Opened dataset. Total items: {}",
                num_items_clone
            ));

            let pb = mp.add(ProgressBar::new(num_items_clone as u64));
            pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [Reading MmapReader {bar:40.cyan/blue}] {pos}/{len} ({percent}%) {bytes_per_sec} {eta}",
                )
                .expect("Failed to set progress bar style")
                .progress_chars("##-"),
            );

            for i in 0..num_items_clone {
                if tx.is_closed() {
                    pb.println("[MmapReader] Channel closed by receiver, stopping reading.");
                    break;
                }

                match get_item_tokens_from_data(
                    i,
                    num_items_clone,
                    &bin_mmap_clone,
                    &item_token_counts_clone,
                    &item_byte_offsets_clone,
                    &item_cache_clone,
                ) {
                    Some(arc_tokens) => {
                        let json_string = serde_json::to_string(&*arc_tokens)
                            .expect("Failed to serialize token data to JSON string");

                        let input_item = InputItem::String(json_string);
                        let final_item = parser(input_item);
                        if tx.send(final_item).await.is_err() {
                            pb.println("[MmapReader] Failed to send item to channel, receiver likely dropped.");
                            break;
                        }
                        pb.inc(1);
                    }
                    None => {
                        pb.println(format!(
                            "[MmapReader] Warning: Failed to get item at index {} (total items {}). Possible data inconsistency.",
                            i, num_items_clone
                        ));
                    }
                }
            }

            if !pb.is_finished() {
                pb.finish_with_message(format!(
                    "[MmapReader] Finished reading. Processed {} items.",
                    pb.position()
                ));
            }
            drop(tx);
        });
        rx
    }
}
