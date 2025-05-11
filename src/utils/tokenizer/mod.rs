mod get_vocab;
mod trie;

use rayon::prelude::*;
use regex::Regex;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::Path;
use std::str;
use trie::Trie;
use unescape::unescape;

#[derive(Debug)]
pub struct Tokenizer {
    tokens: Vec<Vec<u8>>,
    trie: Trie,
}

impl Tokenizer {
    pub fn new(vocab_filepath: &str) -> io::Result<Self> {
        let mut tokenizer = Tokenizer {
            tokens: Vec::with_capacity(65536),
            trie: Trie::new(),
        };

        tokenizer.load_vocabulary(vocab_filepath)?;

        Ok(tokenizer)
    }

    fn load_vocabulary<P: AsRef<Path>>(&mut self, vocab_path: P) -> io::Result<()> {
        let file = File::open(vocab_path)?;
        let reader = BufReader::new(file);

        let re = Regex::new(r"(\d+)\s+(b?)(.+)\s+(\d+)").unwrap();

        self.tokens.push(vec![0]);

        for line in reader.lines() {
            let line = line?;
            if let Some(captures) = re.captures(&line) {
                let id = captures[1].parse::<u16>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("无法解析token ID: {} - {}", &captures[1], e),
                    )
                })?;

                let is_byte = captures[2].to_string();
                let length = captures[4].parse::<usize>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("无法解析token长度: {} - {}", &captures[4], e),
                    )
                })?;

                let mut string: String = captures[3].to_string();
                string = string[1..string.len() - 1].to_string();

                let token_bytes = if is_byte.is_empty() {
                    match unescape(&string) {
                        Some(unescaped) => unescaped.into_bytes(),
                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("无法解析转义字符串: {}", string),
                            ));
                        }
                    }
                } else {
                    Self::hex_to_bytes(&string).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("无效的十六进制字符串: {}", string),
                        )
                    })?
                };

                if token_bytes.len() != length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Token长度不匹配: 期望 {}, 实际 {}",
                            length,
                            token_bytes.len()
                        ),
                    ));
                }

                let id_usize = id as usize;
                while self.tokens.len() <= id_usize {
                    self.tokens.push(Vec::new());
                }

                self.tokens[id_usize] = token_bytes.clone();
                self.trie.insert(&token_bytes, id);
            } else {
                eprintln!("警告: 无法解析词汇表行: {:?}", line);
            }
        }

        Ok(())
    }

    pub fn encode(&self, text: &str, add_end_of_doc: bool) -> Vec<u16> {
        self.trie.tokenize(text, add_end_of_doc)
    }

    pub fn encode_batch(&self, texts: Vec<String>, add_end_of_doc: bool) -> Vec<Vec<u16>> {
        texts
            .par_iter()
            .map(|text| self.encode(text, add_end_of_doc))
            .collect()
    }

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

        String::from_utf8_lossy(&result).into_owned()
    }

    pub fn vocab_size(&self) -> usize {
        self.tokens.len()
    }

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
