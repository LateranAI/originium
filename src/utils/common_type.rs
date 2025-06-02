use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use std::fmt::Debug;
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

impl Display for FastaItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            ">{} [length: {}]\\n{}",
            self.id,
            self.seq.len(),
            self.seq
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum MmapTokenUnitType {
    U8 = 0,
    U16 = 1,
    F32 = 2,
}

impl Default for MmapTokenUnitType {
    fn default() -> Self {
        MmapTokenUnitType::U16
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MmapItem<TokenUnit>
where
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    pub tokens: Vec<TokenUnit>,
}

impl<TokenUnit> Display for MmapItem<TokenUnit>
where
    TokenUnit: Pod + Zeroable + Copy + Clone + Debug + Serialize + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MmapItem(units_count: {})", self.tokens.len())
    }
}
