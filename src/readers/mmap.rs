use crate::custom_tasks::DataEndpoint;
use crate::custom_tasks::InputItem;
use crate::readers::Reader;
use crate::utils::common_type::MmapTokenUnitType;
use async_trait::async_trait;
use bytemuck::{Pod, Zeroable};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use memmap2::{Mmap, MmapOptions};
use moka::sync::Cache;
use serde_json;
use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use crate::errors::FrameworkError;

const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 100;

const LEGACY_MMAP_BINIDX_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const LEGACY_MMAP_BINIDX_VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
const LEGACY_MMAP_BINIDX_DTYPE_U16: u8 = 8;

const GENERIC_MMAP_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const GENERIC_MMAP_VERSION_V2: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];

pub struct MmapReader<Item, TokenUnit>
where
    Item: Send + Sync + 'static + Debug,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    bin_mmap: Arc<Mmap>,
    item_logical_token_counts: Arc<Vec<u32>>,
    item_byte_offsets: Arc<Vec<u64>>,
    num_logical_items: usize,
    item_cache: Cache<usize, Arc<Vec<TokenUnit>>>,
    channel_buffer_size: usize,
    token_unit_type: MmapTokenUnitType,
    token_unit_len: usize,
    is_legacy_format_file: bool,
    _marker: PhantomData<(Item, TokenUnit)>,
}

fn get_item_units_from_data<TokenUnit>(
    item_index: usize,
    num_logical_items: usize,
    bin_mmap: &Mmap,
    item_logical_token_counts: &[u32],
    item_byte_offsets: &[u64],
    token_unit_len: usize,
    item_cache: &Cache<usize, Arc<Vec<TokenUnit>>>,
) -> Option<Arc<Vec<TokenUnit>>>
where
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    if item_index >= num_logical_items {
        return None;
    }

    if let Some(cached_units) = item_cache.get(&item_index) {
        return Some(cached_units);
    }

    let logical_token_count = item_logical_token_counts
        .get(item_index)
        .copied()
        .unwrap_or(0) as usize;
    if logical_token_count == 0 {
        let arc_units = Arc::new(Vec::new());
        item_cache.insert(item_index, arc_units.clone());
        return Some(arc_units);
    }

    let total_units_for_item = logical_token_count.checked_mul(token_unit_len)?;
    let byte_offset = item_byte_offsets.get(item_index).copied().unwrap_or(0) as usize;

    if byte_offset >= bin_mmap.len() {
        return None;
    }

    let unit_size_bytes = std::mem::size_of::<TokenUnit>();
    if unit_size_bytes == 0 {
        return None;
    }
    let byte_length = total_units_for_item.checked_mul(unit_size_bytes)?;
    let end_offset = byte_offset.checked_add(byte_length)?;

    if end_offset > bin_mmap.len() {
        return None;
    }

    let token_bytes_slice = bin_mmap.get(byte_offset..end_offset)?;

    match bytemuck::try_cast_slice::<u8, TokenUnit>(token_bytes_slice) {
        Ok(units_slice) => {
            let units_vec: Vec<TokenUnit> = units_slice.to_vec();
            let arc_units = Arc::new(units_vec);
            item_cache.insert(item_index, arc_units.clone());
            Some(arc_units)
        }
        Err(e) => {
            eprintln!(
                "[MmapReader::get_item_units_from_data] Failed to cast slice for item {}: {:?}. Bytes len: {}, Expected units: {}. Unit size: {}",
                item_index,
                e,
                token_bytes_slice.len(),
                total_units_for_item,
                unit_size_bytes
            );
            None
        }
    }
}

impl<Item, TokenUnit> MmapReader<Item, TokenUnit>
where
    Item: Send + Sync + 'static + Debug,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    pub fn new(endpoint_config: &DataEndpoint, _reader_id: Option<String>) -> Result<Self, FrameworkError> {
        let (base_path_str, 
            filename_str, 
            _num_devices, 
            _threads_per_device, 
            expected_token_unit_type, 
            expected_token_unit_len, 
            expected_is_legacy_format, 
            _expected_context_length // Destructure, but ignore for reader assertions
        ) = endpoint_config.unwrap_mmap();

        let idx_file_path = PathBuf::from(&base_path_str).join(format!("{}.idx", filename_str));

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
            .expect("Failed to read magic header");

        let mut read_version_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut read_version_buf)
            .expect("Failed to read version");

        let file_is_legacy_format: bool;
        let file_token_unit_type: MmapTokenUnitType;
        let file_token_unit_len: usize;

        if read_magic == LEGACY_MMAP_BINIDX_MAGIC_HDR
            && read_version_buf == LEGACY_MMAP_BINIDX_VERSION
        {
            file_is_legacy_format = true;
            file_token_unit_type = MmapTokenUnitType::U16;
            file_token_unit_len = 1;
            let mut legacy_dtype_buf = [0u8; 1];
            idx_file_handle
                .read_exact(&mut legacy_dtype_buf)
                .expect("Failed to read legacy DTYPE");
            assert_eq!(
                legacy_dtype_buf[0], LEGACY_MMAP_BINIDX_DTYPE_U16,
                "Legacy .idx DTYPE mismatch"
            );
        } else if read_magic == GENERIC_MMAP_MAGIC_HDR
            && read_version_buf == GENERIC_MMAP_VERSION_V2
        {
            file_is_legacy_format = false;
            let mut dtype_buf = [0u8; 1];
            idx_file_handle
                .read_exact(&mut dtype_buf)
                .expect("Failed to read generic DTYPE");
            file_token_unit_type = match dtype_buf[0] {
                1 => MmapTokenUnitType::U16,
                2 => MmapTokenUnitType::F32,
                _ => panic!("Unsupported DTYPE {} in generic .idx file", dtype_buf[0]),
            };
            let mut unit_len_buf = [0u8; 4];
            idx_file_handle
                .read_exact(&mut unit_len_buf)
                .expect("Failed to read generic TOKEN_UNIT_LEN");
            file_token_unit_len = u32::from_le_bytes(unit_len_buf) as usize;
            if file_token_unit_len == 0 {
                panic!("TOKEN_UNIT_LEN in .idx file cannot be zero.");
            }
        } else {
            panic!(
                "Unknown .idx file format (magic or version mismatch). Magic: {:?}, Version: {:?}",
                read_magic, read_version_buf
            );
        }

        assert_eq!(
            file_is_legacy_format,
            expected_is_legacy_format,
            "Mismatch in legacy format flag: file says {} but config expects {}. File: {}",
            file_is_legacy_format,
            expected_is_legacy_format,
            idx_file_path.display()
        );
        assert_eq!(
            file_token_unit_type,
            expected_token_unit_type,
            "Mismatch in token unit type: file has {:?} but config expects {:?}. File: {}",
            file_token_unit_type,
            expected_token_unit_type,
            idx_file_path.display()
        );
        assert_eq!(
            file_token_unit_len,
            expected_token_unit_len,
            "Mismatch in token unit length: file has {} but config expects {}. File: {}",
            file_token_unit_len,
            expected_token_unit_len,
            idx_file_path.display()
        );

        let mut num_items_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut num_items_buf)
            .expect("Failed to read num_items");
        let num_logical_items = u64::from_le_bytes(num_items_buf) as usize;

        let mut doc_indices_len_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut doc_indices_len_buf)
            .expect("Failed to read doc_indices_len");

        let mut item_logical_token_counts_vec = Vec::with_capacity(num_logical_items);
        for _ in 0..num_logical_items {
            let mut buf = [0u8; 4];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read item logical token count");
            item_logical_token_counts_vec.push(u32::from_le_bytes(buf));
        }

        let mut item_byte_offsets_vec = Vec::with_capacity(num_logical_items);
        for _ in 0..num_logical_items {
            let mut buf = [0u8; 8];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read item byte offset");
            item_byte_offsets_vec.push(u64::from_le_bytes(buf));
        }

        let bin_file_path = PathBuf::from(&base_path_str).join(format!("{}.bin", filename_str));
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

        Ok(Self {
            bin_mmap: Arc::new(bin_mmap_instance),
            item_logical_token_counts: Arc::new(item_logical_token_counts_vec),
            item_byte_offsets: Arc::new(item_byte_offsets_vec),
            num_logical_items,
            item_cache,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            token_unit_type: file_token_unit_type,
            token_unit_len: file_token_unit_len,
            is_legacy_format_file: file_is_legacy_format,
            _marker: PhantomData,
        })
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
        self.num_logical_items
    }
}

#[async_trait]
impl<Item, TokenUnit> Reader<Item> for MmapReader<Item, TokenUnit>
where
    Item: Send + Sync + 'static + Debug,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    async fn pipeline(
        &self,
        read_fn: Box<dyn Fn(InputItem) -> Item + Send + Sync + 'static>,
        mp: Arc<MultiProgress>,
    ) -> mpsc::Receiver<Item> {
        let (tx, rx) = mpsc::channel(self.channel_buffer_size);
        let parser = Arc::new(read_fn);

        let bin_mmap_clone = Arc::clone(&self.bin_mmap);
        let item_logical_token_counts_clone = Arc::clone(&self.item_logical_token_counts);
        let item_byte_offsets_clone = Arc::clone(&self.item_byte_offsets);
        let item_cache_clone = self.item_cache.clone();
        let num_logical_items_clone = self.num_logical_items;
        let token_unit_len_clone = self.token_unit_len;
        let reader_name = format!(
            "[MmapReader Format: {}, Unit: {:?}, UnitLen: {}]",
            if self.is_legacy_format_file {
                "Legacy"
            } else {
                "Generic"
            },
            self.token_unit_type,
            self.token_unit_len
        );

        tokio::spawn(async move {
            mp.println(format!(
                "{} Opened dataset. Total logical items: {}",
                reader_name, num_logical_items_clone
            ))
            .unwrap_or_default();

            let pb = mp.add(ProgressBar::new(num_logical_items_clone as u64));
            pb.set_style(
                ProgressStyle::with_template(&format!(
                    "[{{elapsed_precise}}] [Reading {} {{bar:40.cyan/blue}}] {{pos}}/{{len}} ({{percent}}%) {{bytes_per_sec}} {{eta}}",
                    reader_name
                ))
                .expect("Failed to set progress bar style")
                .progress_chars("##-"),
            );

            for i in 0..num_logical_items_clone {
                if tx.is_closed() {
                    pb.println(format!(
                        "{} Channel closed by receiver, stopping reading.",
                        reader_name
                    ));
                    break;
                }

                match get_item_units_from_data::<TokenUnit>(
                    i,
                    num_logical_items_clone,
                    &bin_mmap_clone,
                    &item_logical_token_counts_clone,
                    &item_byte_offsets_clone,
                    token_unit_len_clone,
                    &item_cache_clone,
                ) {
                    Some(arc_units) => {
                        let json_string = serde_json::to_string(&*arc_units)
                            .expect("Failed to serialize token unit data to JSON string");

                        let input_item = InputItem::String(json_string);
                        let final_item = parser(input_item);
                        if tx.send(final_item).await.is_err() {
                            pb.println(format!(
                                "{} Failed to send item to channel, receiver likely dropped.",
                                reader_name
                            ));
                            break;
                        }
                        pb.inc(1);
                    }
                    None => {
                        pb.println(format!(
                            "{} Warning: Failed to get item at index {} (total items {}). Possible data inconsistency or mmap read error.",
                            reader_name, i, num_logical_items_clone
                        ));
                    }
                }
            }

            if !pb.is_finished() {
                pb.finish_with_message(format!(
                    "{} Finished reading. Processed {} items.",
                    reader_name,
                    pb.position()
                ));
            }
            drop(tx);
        });
        rx
    }
}
