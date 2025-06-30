use sqd_primitives::BlockNumber;
use std::borrow::Cow;
use std::ops::Range;
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
    pub data: Cow<'a, [u8]>
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