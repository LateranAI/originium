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

const MMAP_MAGIC_HDR: &[u8] = b"MMIDIDX\x00\x00";
const RWKV_MMAP_VERSION: [u8; 8] = [1, 0, 0, 0, 0, 0, 0, 0];
const RWKV_MMAP_DTYPE_U16: u8 = 8;

const ORIGINIUM_MMAP_VERSION: [u8; 8] = [2, 0, 0, 0, 0, 0, 0, 0];

pub struct MmapReader<Item, TokenUnit>
where
    Item: Send + Sync + 'static + Debug,
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    bin_mmap: Arc<Mmap>,
    nums_tokens_per_line: Arc<Vec<u32>>,
    line_offsets: Arc<Vec<u64>>,
    num_lines: usize,
    line_cache: Cache<usize, Arc<Vec<TokenUnit>>>,
    channel_buffer_size: usize,
    token_unit_type: MmapTokenUnitType,
    num_units_per_token: usize,
    is_legacy_format_file: bool,
    _marker: PhantomData<(Item, TokenUnit)>,
}

fn get_item_units_from_data<TokenUnit>(
    line_index: usize,
    num_lines: usize,
    bin_mmap: &Mmap,
    nums_tokens_per_line: &[u32],
    line_offsets: &[u64],
    num_units_per_token: usize,
    line_cache: &Cache<usize, Arc<Vec<TokenUnit>>>,
) -> Option<Arc<Vec<TokenUnit>>>
where
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + serde::Serialize + Send + Sync + 'static,
{
    if line_index >= num_lines {
        return None;
    }

    if let Some(cached_units) = line_cache.get(&line_index) {
        return Some(cached_units);
    }

    let num_tokens = nums_tokens_per_line
        .get(line_index)
        .copied()
        .unwrap_or(0) as usize;
    if num_tokens == 0 {
        let arc_units = Arc::new(Vec::new());
        line_cache.insert(line_index, arc_units.clone());
        return Some(arc_units);
    }

    let total_token_units_for_line = num_tokens.checked_mul(num_units_per_token)?;
    let byte_offset = line_offsets.get(line_index).copied().unwrap_or(0) as usize;

    if byte_offset >= bin_mmap.len() {
        return None;
    }

    let unit_size_bytes = std::mem::size_of::<TokenUnit>();
    if unit_size_bytes == 0 {
        return None;
    }
    let byte_length = total_token_units_for_line.checked_mul(unit_size_bytes)?;
    let end_offset = byte_offset.checked_add(byte_length)?;

    if end_offset > bin_mmap.len() {
        return None;
    }

    let token_bytes_slice = bin_mmap.get(byte_offset..end_offset)?;

    match bytemuck::try_cast_slice::<u8, TokenUnit>(token_bytes_slice) {
        Ok(units_slice) => {
            let units_vec: Vec<TokenUnit> = units_slice.to_vec();
            let arc_units = Arc::new(units_vec);
            line_cache.insert(line_index, arc_units.clone());
            Some(arc_units)
        }
        Err(e) => {
            eprintln!(
                "[MmapReader::get_item_units_from_data] Failed to cast slice for line {}: {:?}. Bytes len: {}, Expected units: {}. Unit size: {}",
                line_index,
                e,
                token_bytes_slice.len(),
                total_token_units_for_line,
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
            expected_num_units_per_token, 
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
        let file_num_units_per_token: usize;

        if read_magic == MMAP_MAGIC_HDR
            && read_version_buf == RWKV_MMAP_VERSION
        {
            file_is_legacy_format = true;
            file_token_unit_type = MmapTokenUnitType::U16;
            file_num_units_per_token = 1;
            let mut legacy_dtype_buf = [0u8; 1];
            idx_file_handle
                .read_exact(&mut legacy_dtype_buf)
                .expect("Failed to read legacy DTYPE");
            assert_eq!(
                legacy_dtype_buf[0], RWKV_MMAP_DTYPE_U16,
                "Legacy .idx DTYPE mismatch"
            );
        } else if read_magic == MMAP_MAGIC_HDR
            && read_version_buf == ORIGINIUM_MMAP_VERSION
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
            file_num_units_per_token = u32::from_le_bytes(unit_len_buf) as usize;
            if file_num_units_per_token == 0 {
                panic!("num_units_per_token in .idx file cannot be zero.");
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
            file_num_units_per_token,
            expected_num_units_per_token,
            "Mismatch in num_units_per_token: file has {} but config expects {}. File: {}",
            file_num_units_per_token,
            expected_num_units_per_token,
            idx_file_path.display()
        );

        let mut num_lines_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut num_lines_buf)
            .expect("Failed to read num_lines");
        let num_lines = u64::from_le_bytes(num_lines_buf) as usize;

        let mut doc_indices_len_buf = [0u8; 8];
        idx_file_handle
            .read_exact(&mut doc_indices_len_buf)
            .expect("Failed to read doc_indices_len");

        let mut nums_tokens_per_line_vec = Vec::with_capacity(num_lines);
        for _ in 0..num_lines {
            let mut buf = [0u8; 4];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read num tokens for line");
            nums_tokens_per_line_vec.push(u32::from_le_bytes(buf));
        }

        let mut line_offsets_vec = Vec::with_capacity(num_lines);
        for _ in 0..num_lines {
            let mut buf = [0u8; 8];
            idx_file_handle
                .read_exact(&mut buf)
                .expect("Failed to read line byte offset");
            line_offsets_vec.push(u64::from_le_bytes(buf));
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

        let line_cache = Cache::builder()
            .time_to_idle(Duration::from_secs(600))
            .max_capacity(10_000)
            .build();

        Ok(Self {
            bin_mmap: Arc::new(bin_mmap_instance),
            nums_tokens_per_line: Arc::new(nums_tokens_per_line_vec),
            line_offsets: Arc::new(line_offsets_vec),
            num_lines,
            line_cache,
            channel_buffer_size: DEFAULT_CHANNEL_BUFFER_SIZE,
            token_unit_type: file_token_unit_type,
            num_units_per_token: file_num_units_per_token,
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
        self.num_lines
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
        let total_items = self.num_lines;

        let pb = mp.add(ProgressBar::new(total_items as u64));
        let pb_template = format!(
            "[MmapReader {{elapsed_precise}}] {{wide_bar:.blue}} {{percent:>3}}% ({{pos}}/{{len}}) {{per_sec}}, ETA: {{eta}}"
        );
        pb.set_style(
            ProgressStyle::with_template(&pb_template)
                .unwrap()
                .progress_chars("=> "),
        );
        pb.enable_steady_tick(Duration::from_millis(100));

        let bin_mmap_clone = Arc::clone(&self.bin_mmap);
        let nums_tokens_per_line_clone = Arc::clone(&self.nums_tokens_per_line);
        let line_offsets_clone = Arc::clone(&self.line_offsets);
        let line_cache_clone = self.line_cache.clone();
        let num_units_per_token_clone = self.num_units_per_token;
        let parser = Arc::new(read_fn);

        tokio::spawn(async move {
            for i in 0..total_items {
                if tx.is_closed() {
                    pb.println("[MmapReader] Channel closed, stopping.");
                    break;
                }

                match get_item_units_from_data(
                    i,
                    total_items,
                    &bin_mmap_clone,
                    &nums_tokens_per_line_clone,
                    &line_offsets_clone,
                    num_units_per_token_clone,
                    &line_cache_clone,
                ) {
                    Some(units_arc) => {
                        let json_string = serde_json::to_string(&*units_arc).expect("Failed to serialize Mmap item (Vec<TokenUnit>) to JSON string");
                        let input_item = InputItem::String(json_string); 
                        let processed_item = parser(input_item);
                        if tx.send(processed_item).await.is_err() {
                            pb.println("[MmapReader] Failed to send item, receiver dropped.");
                            break;
                        }
                    }
                    None => {
                        pb.println(format!("[MmapReader] Failed to retrieve units for item index {}. Skipping.", i));
                        // Decide if we should send a placeholder or just skip. For now, skipping.
                    }
                }
                pb.inc(1);
            }
            if !pb.is_finished() {
                let final_msg = format!(
                    "[MmapReader] Complete. {pos} items. ({elapsed})",
                    pos = pb.position(),
                    elapsed = format!("{:.2?}", pb.elapsed())
                );
                pb.finish_with_message(final_msg);
            }
        });

        rx
    }
}
