use serde::de::DeserializeOwned;
use std::fs;

pub fn load_toml<T: DeserializeOwned + 'static>(path: &str) -> T {
    let content = fs::read_to_string(path).expect("Failed to read file at path: {path}");
    toml::from_str(&content).expect("Invalid TOML format in file: {path}")
}
