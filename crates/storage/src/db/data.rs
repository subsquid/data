use crate::db::table_id::TableId;
use borsh::{BorshDeserialize, BorshSerialize};
use sqd_primitives::sid::SID;
use sqd_primitives::{BlockNumber, BlockRef};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};


pub type DatasetId = SID<48>;


pub type DatasetVersion = u64;


pub type DatasetKind = SID<16>;


#[derive(Debug, Clone, Eq, PartialEq, BorshSerialize, BorshDeserialize)]
pub enum DatasetLabel {
    V0 {
        kind: DatasetKind,
        version: DatasetVersion,
        finalized_head: Option<BlockRef>
    }
}


impl DatasetLabel {
    pub fn kind(&self) -> DatasetKind {
        match self { 
            DatasetLabel::V0 { kind, .. } => *kind 
        }
    }
    
    pub fn version(&self) -> DatasetVersion {
        match self {
            DatasetLabel::V0 { version, .. } => *version
        }
    }
    
    pub fn bump_version(&mut self) {
        match self {
            DatasetLabel::V0 { version, .. } => *version += 1
        }
    }

    pub fn finalized_head(&self) -> Option<&BlockRef> {
        match self {
            DatasetLabel::V0 { finalized_head, .. } => finalized_head.as_ref()
        }
    }

    pub fn set_finalized_head(&mut self, block_ref: Option<BlockRef>) {
        match self {
            DatasetLabel::V0 { finalized_head, .. } => *finalized_head = block_ref
        }
    }
}


#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Dataset {
    pub id: DatasetId,
    pub label: DatasetLabel
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
        Self::new(dataset_id, chunk.last_block())
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


#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Eq, PartialEq)]
pub enum Chunk {
    V0 {
        first_block: BlockNumber,
        last_block: BlockNumber,
        last_block_hash: String,
        parent_block_hash: String,
        tables: BTreeMap<String, TableId>
    },
    V1 {
        first_block: BlockNumber,
        last_block: BlockNumber,
        last_block_hash: String,
        parent_block_hash: String,
        first_block_time: Option<i64>,
        last_block_time: Option<i64>,
        tables: BTreeMap<String, TableId>
    }
}


impl Chunk {
    pub fn first_block(&self) -> BlockNumber {
        match self {
            Chunk::V0 { first_block, .. } => *first_block,
            Chunk::V1 { first_block, .. } => *first_block,
        }
    }

    pub fn last_block(&self) -> BlockNumber {
        match self {
            Chunk::V0 { last_block, .. } => *last_block,
            Chunk::V1 { last_block, .. } => *last_block,
        }
    }

    pub fn last_block_hash(&self) -> &str {
        match self {
            Chunk::V0 { last_block_hash, .. } => last_block_hash,
            Chunk::V1 { last_block_hash, .. } => last_block_hash,
        }
    }

    pub fn parent_block_hash(&self) -> &str {
        match self {
            Chunk::V0 { parent_block_hash, .. } => parent_block_hash,
            Chunk::V1 { parent_block_hash, .. } => parent_block_hash,
        }
    }
    
    pub fn first_block_time(&self) -> Option<i64> {
        match self {
            Chunk::V0 { .. } => None,
            Chunk::V1 { first_block_time, .. } => *first_block_time
        }
    }

    pub fn last_block_time(&self) -> Option<i64> {
        match self {
            Chunk::V0 { .. } => None,
            Chunk::V1 { last_block_time, .. } => *last_block_time
        }
    }

    pub fn tables(&self) -> &BTreeMap<String, TableId> {
        match self {
            Chunk::V0 { tables, .. } => tables,
            Chunk::V1 { tables, .. } => tables,
        }
    }

    pub fn blocks_count(&self) -> u64 {
        match self {
            Chunk::V0 { first_block, last_block, .. } => *last_block - *first_block + 1,
            Chunk::V1 { first_block, last_block, .. } => *last_block - *first_block + 1,
        }
    }

    pub fn next_block(&self) -> BlockNumber {
        match self {
            Chunk::V0 { last_block, .. } => *last_block + 1,
            Chunk::V1 { last_block, .. } => *last_block + 1,
        }
    }
}


impl Display for Chunk {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, 
            "{}-{}-{}",
            self.first_block(),
            self.last_block(),
            self.last_block_hash()
        )
    }
}