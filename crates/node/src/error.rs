pub use super::query::user_error::*;
pub use sqd_query::UnexpectedBaseBlock;
use sqd_storage::db::DatasetId;
use std::fmt::{Display, Formatter};
use sqd_primitives::BlockNumber;


#[derive(Debug)]
pub struct Busy;


impl Display for Busy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "node is busy")
    }
}


impl std::error::Error for Busy {}


#[derive(Debug)]
pub struct UnknownDataset {
    pub dataset_id: DatasetId
}


impl Display for UnknownDataset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "dataset {} does not exist", self.dataset_id)
    }
}


impl std::error::Error for UnknownDataset {}


#[derive(Debug)]
pub struct BlockRangeMissing {
    pub first_block: BlockNumber,
    pub last_block: BlockNumber
}


impl Display for BlockRangeMissing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "blocks from {} to {} are not available in the dataset", 
            self.first_block, 
            self.last_block
        )
    }
}


impl std::error::Error for BlockRangeMissing {}