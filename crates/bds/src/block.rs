use bytes::Bytes;
use sqd_primitives::BlockNumber;
use std::borrow::{Borrow, Cow};
use std::ops::{Deref, Range};
use std::sync::Arc;


pub type BlockRange = Range<BlockNumber>;


#[derive(Clone, Debug)]
pub struct BlockHeader<'a> {
    pub number: BlockNumber,
    pub hash: Cow<'a, str>,
    pub parent_number: BlockNumber,
    pub parent_hash: Cow<'a, str>,
    pub timestamp: Option<i64>,
    pub is_final: bool
}


#[derive(Clone, Debug)]
pub struct Block<'a> {
    pub header: BlockHeader<'a>,
    pub data: ByteRef<'a>
}


#[derive(Debug)]
pub enum ByteRef<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes)
}


impl<'a> Deref for ByteRef<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            ByteRef::Borrowed(s) => s,
            ByteRef::Owned(b) => b.as_ref()
        }
    }
}


impl<'a> Clone for ByteRef<'a> {
    fn clone(&self) -> Self {
        match self {
            ByteRef::Borrowed(s) => ByteRef::Borrowed(s),
            ByteRef::Owned(b) => ByteRef::Owned(b.clone())
        }
    }
}


impl<'a> From<&'a [u8]> for ByteRef<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self::Borrowed(value)
    }
}


impl<'a> From<Bytes> for ByteRef<'a> {
    fn from(value: Bytes) -> Self {
        Self::Owned(value)
    }
}


pub type BlockArc = Arc<Block<'static>>;


impl <'a> sqd_primitives::Block for Block<'a> {
    fn number(&self) -> BlockNumber {
        self.header.number
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.parent_number
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        self.header.timestamp
    }
}


impl <'a> sqd_primitives::Block for BlockHeader<'a> {
    fn number(&self) -> BlockNumber {
        self.number
    }

    fn hash(&self) -> &str {
        &self.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.parent_number
    }

    fn parent_hash(&self) -> &str {
        &self.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }
}