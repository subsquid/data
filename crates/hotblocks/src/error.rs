pub use super::query::user_error::*;
use sqd_primitives::{BlockNumber, BlockRef};
pub use sqd_query::UnexpectedBaseBlock;
use sqd_storage::db::DatasetId;
use std::fmt::{Display, Formatter};


#[derive(Debug)]
pub struct Busy;


impl Display for Busy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "hotblocks server is busy")
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


#[derive(Debug)]
pub struct QueryIsAboveTheHead {
    pub finalized_head: Option<BlockRef>
}


impl Display for QueryIsAboveTheHead {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "first block requested by the query is above the current dataset head")
    }
}


impl std::error::Error for QueryIsAboveTheHead {}