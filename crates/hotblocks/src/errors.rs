use sqd_primitives::{BlockNumber, BlockRef};
use sqd_storage::db::DatasetId;
use std::fmt::{Display, Formatter};


#[derive(Debug)]
pub struct Busy;


impl Display for Busy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "service is busy")
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


#[derive(Debug)]
pub struct QueryKindMismatch {
    pub query_kind: sqd_storage::db::DatasetKind,
    pub dataset_kind: sqd_storage::db::DatasetKind
}


impl Display for QueryKindMismatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} query was issued against {} dataset", self.query_kind, self.dataset_kind)
    }
}


impl std::error::Error for QueryKindMismatch {}