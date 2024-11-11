use crate::db::table_id::TableId;
use borsh::{BorshDeserialize, BorshSerialize};
use sqd_primitives::{BlockNumber, ShortHash, SID};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};


pub type DatasetId = SID<48>;


pub type DatasetVersion = u64;


pub type DatasetKind = SID<16>;


#[derive(Copy, Clone, Eq, PartialEq, Debug, BorshSerialize, BorshDeserialize)]
pub struct DatasetLabel {
    pub kind: DatasetKind,
    pub version: DatasetVersion
}


#[derive(Copy, Clone, Hash, Ord, PartialOrd, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
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
        DatasetId::try_from(&self.bytes[0..48]).unwrap()
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


impl Debug for ChunkId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChunkId({})", self)
    }
}


#[derive(BorshSerialize, BorshDeserialize, Clone, Debug)]
pub struct Chunk {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber,
    pub last_block_hash: ShortHash,
    pub max_num_rows: u32,
    pub tables: BTreeMap<String, TableId>
}