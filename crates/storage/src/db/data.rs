use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use borsh::{BorshDeserialize, BorshSerialize};

use sqd_primitives::{BlockNumber, ShortHash};

use crate::db::table_id::TableId;
use crate::table::write::TableOptions;


#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct DatasetId {
    bytes: [u8; 48]
}


impl DatasetId {
    pub fn new(bytes: [u8; 48]) -> Self {
        Self { bytes }
    }
}


impl AsRef<[u8]> for DatasetId {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}


impl Display for DatasetId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for byte in &self.bytes {
            write!(f, "{:02X}", byte)?;
        }
        Ok(())
    }
}


impl Default for DatasetId {
    fn default() -> Self {
        Self::new([0; 48])
    }
}


pub type DatasetVersion = u64;


#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct DatasetKind {
    bytes: [u8; 16]
}


impl DatasetKind {
    pub fn new(bytes: [u8; 16]) -> Self {
        Self { bytes }
    }
}


#[derive(Copy, Clone, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct DatasetLabel {
    pub kind: DatasetKind,
    pub version: DatasetVersion
}


#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct ChunkId {
    bytes: [u8; 56]
}


impl ChunkId {
    pub fn new(dataset_id: DatasetId, last_block: BlockNumber) -> Self {
        let mut bytes = [0; 56];
        bytes[..48].copy_from_slice(dataset_id.as_ref());
        bytes[48..].copy_from_slice(&last_block.to_be_bytes());
        Self {
            bytes
        }
    }

    pub fn new_for_chunk(dataset_id: DatasetId, chunk: &Chunk) -> Self {
        Self::new(dataset_id, chunk.last_block)
    }

    pub fn dataset_id(&self) -> DatasetId {
        DatasetId::new(self.bytes[0..48].try_into().unwrap())
    }

    pub fn last_block(&self) -> BlockNumber {
        BlockNumber::from_be_bytes(self.bytes[48..].try_into().unwrap())
    }
}


impl AsRef<[u8]> for ChunkId {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}


impl Display for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.dataset_id(), self.last_block())
    }
}


#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct Chunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: ShortHash,
    pub max_num_rows: u32,
    pub tables: HashMap<String, TableId>
}


pub type DatasetDescriptionRef = std::sync::Arc<DatasetDescription>;


#[derive(Clone, Debug, Default)]
pub struct DatasetDescription {
    pub tables: Vec<TableDescription>
}


#[derive(Clone, Debug)]
pub struct TableDescription {
    pub name: String,
    pub sort_key: Vec<String>,
    pub options: TableOptions
}