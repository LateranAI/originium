use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Row, Column, any::AnyRow, types::Type, Decode};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Deserialize, FromRow)]
pub struct LineInput {
    #[sqlx(rename = "content")]
    pub content: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RedisKVPair {
    pub key: String,
    pub value: String,
}

impl Display for RedisKVPair {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.key, self.value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct FastaItem {
    pub id: String,
    pub desc: Option<String>,
    pub seq: String,
}