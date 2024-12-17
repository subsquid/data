use serde::Deserialize;
use sqd_data_types::BlockNumber;


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