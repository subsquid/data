use super::row_batch::{Row, RowBatch};
use sqd_primitives::BlockNumber;


pub struct BlockBatch<B: Row> {
    inner: RowBatch<B>,
    partition_start: BlockNumber,
    partition_end: BlockNumber
}


impl <B: Row> BlockBatch<B> {
    pub(super) fn new(inner: RowBatch<B>, partition_start: BlockNumber, partition_end: BlockNumber) -> Self {
        Self {
            inner,
            partition_start,
            partition_end
        }
    }
    
    pub fn blocks(&self) -> &[B::Type<'_>] {
        self.inner.items()
    }
    
    pub fn partition_start(&self) -> BlockNumber {
        self.partition_start
    }

    pub fn partition_end(&self) -> BlockNumber {
        self.partition_end
    }
}