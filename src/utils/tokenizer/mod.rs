mod trie;
mod get_vocab;

use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::str;
use trie::Trie;
use unescape::unescape;


/// 世界分词器
/// 基于Trie树实现的文本分词工具
#[derive(Debug)]
pub struct Tokenizer {
    /// token列表
    tokens: Vec<Vec<u8>>,
    /// 用于搜索token的Trie树
    trie: Trie
}

impl Tokenizer {
    /// 创建一个新的WorldTokenizer
    ///
    /// # 参数
    /// * `vocab_filepath` - 可选的词汇表文件路径，如果为None则使用默认路径
    ///
    /// # 返回
    /// * 分词器实例或IO错误
    pub fn new(vocab_filepath: &str) -> io::Result<Self> {
        let mut tokenizer = Tokenizer {
            tokens: Vec::with_capacity(65536), // 预分配空间以减少重新分配
            trie: Trie::new()
        };

        // 获取词汇表路径
        // let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        //     .join("../../../assets")
        //     .join("vocab_v20230424.txt");

        // 加载词汇表
        tokenizer.load_vocabulary(vocab_filepath)?;

        Ok(tokenizer)
    }

    /// 加载词汇表
    ///
    /// # 参数
    /// * `vocab_path` - 词汇表文件路径
    ///
    /// # 返回
    /// * 成功或IO错误
    fn load_vocabulary<P: AsRef<Path>>(&mut self, vocab_path: P) -> io::Result<()> {
        let file = File::open(vocab_path)?;
        let reader = BufReader::new(file);

        let re = Regex::new(r"(\d+)\s+(b?)(.+)\s+(\d+)").unwrap();

        // 初始化第一个token为空字节
        self.tokens.push(vec![0]);

        for line in reader.lines() {
            let line = line?;
            if let Some(captures) = re.captures(&line) {
                let id = captures[1].parse::<u16>().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData,
                                  format!("无法解析token ID: {} - {}", &captures[1], e))
                })?;

                let is_byte = captures[2].to_string();
                let length = captures[4].parse::<usize>().map_err(|e| {
                    io::Error::new(io::ErrorKind::InvalidData,
                                  format!("无法解析token长度: {} - {}", &captures[4], e))
                })?;

                let mut string: String = captures[3].to_string();
                string = string[1..string.len()-1].to_string();

                let token_bytes = if is_byte.is_empty() {
                    // 文本token
                    match unescape(&string) {
                        Some(unescaped) => unescaped.into_bytes(),
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("无法解析转义字符串: {}", string)
                            ));
                        }
                    }
                } else {
                    // 二进制token
                    Self::hex_to_bytes(&string).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("无效的十六进制字符串: {}", string)
                        )
                    })?
                };

                // 验证token长度
                if token_bytes.len() != length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Token长度不匹配: 期望 {}, 实际 {}", length, token_bytes.len())
                    ));
                }

                // 确保tokens足够大以容纳id
                let id_usize = id as usize;
                while self.tokens.len() <= id_usize {
                    self.tokens.push(Vec::new());
                }

                self.tokens[id_usize] = token_bytes.clone();
                self.trie.insert(&token_bytes, id);
            }
            else {
                eprintln!("警告: 无法解析词汇表行: {:?}", line);
            }
        }

        Ok(())
    }

    /// 将文本编码为token ID序列
    ///
    /// # 参数
    /// * `text` - 要编码的文本
    ///
    /// # 返回
    /// * token ID序列
    pub fn encode(&self, text: &str, add_end_of_doc: bool) -> Vec<u16> {
        self.trie.tokenize(text, add_end_of_doc)
    }

    /// 批量编码多个文本
    ///
    /// # 参数
    /// * `texts` - 要编码的文本列表
    ///
    /// # 返回
    /// * 各文本对应的token ID序列列表
    pub fn encode_batch(&self, texts: Vec<String>, add_end_of_doc: bool) -> Vec<Vec<u16>> {
        texts.par_iter()
            .map(|text| self.encode(text, add_end_of_doc))
            .collect()
    }

    /// 将token ID序列解码为文本
    ///
    /// # 参数
    /// * `token_ids` - 要解码的token ID序列
    ///
    /// # 返回
    /// * 解码后的文本
    pub fn decode(&self, token_ids: Vec<u16>) -> String {
        let mut result = Vec::new();

        for &id in &token_ids {
            let id_usize = id as usize;
            if id_usize < self.tokens.len() {
                result.extend_from_slice(&self.tokens[id_usize]);
            } else {
                eprintln!("警告: 无效的token ID: {}", id);
            }
        }

        // 尝试解码为UTF-8字符串，如果失败则替换无效字符
        String::from_utf8_lossy(&result).into_owned()
    }

    /// 获取词汇表大小
    ///
    /// # 返回
    /// * 词汇表大小
    pub fn vocab_size(&self) -> usize {
        self.tokens.len()
    }

    /// 获取完整词汇表
    ///
    /// # 返回
    /// * 词汇表映射 (token文本 -> token ID)
    pub fn get_vocab(&self) -> HashMap<String, usize> {
        let mut vocab = HashMap::with_capacity(self.tokens.len());

        for (index, token) in self.tokens.iter().enumerate() {
            if token.is_empty() {
                continue;
            }

            let text = match String::from_utf8(token.clone()) {
                Ok(s) => s,
                Err(_) => format!("[Binary token {:?}]", token),
            };

            vocab.insert(text, index);
        }

        vocab
    }

    /// 二进制字符串转字节数组
    ///
    /// # 参数
    /// * `hex` - 十六进制字符串
    ///
    /// # 返回
    /// * 字节数组或None（如果输入无效）
    fn hex_to_bytes(hex: &str) -> Option<Vec<u8>> {
        let hex = hex.replace("\\x", "");

        if hex.len() % 2 != 0 {
            return None;
        }

        let mut result = Vec::with_capacity(hex.len() / 2);

        for i in (0..hex.len()).step_by(2) {
            if let Some(sub) = hex.get(i..i + 2) {
                if let Ok(byte) = u8::from_str_radix(sub, 16) {
                    result.push(byte);
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }

        Some(result)
    }
}

