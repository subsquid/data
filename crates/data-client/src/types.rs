use bytes::Bytes;
use serde::Deserialize;


pub type BlockNumber = u64;


#[derive(Deserialize, Clone, Debug, Default)]
pub struct BlockRef {
    number: BlockNumber,
    hash: String
}


impl BlockRef {
    pub fn new(number: BlockNumber, hash: &str) -> Self {
        Self {
            number,
            hash: hash.to_string()
        }
    }
    
    pub fn number(&self) -> BlockNumber {
        self.number
    }
    
    pub fn hash(&self) -> &str {
        &self.hash
    }
}


pub trait Block: Sized {
    fn try_from_bytes(bytes: Bytes) -> anyhow::Result<Self>;
    
    fn number(&self) -> BlockNumber;
    
    fn hash(&self) -> &str;
    
    fn parent_number(&self) -> BlockNumber;
    
    fn parent_hash(&self) -> &str;
}