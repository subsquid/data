mod cassandra;


use sqd_primitives::BlockNumber;
use std::borrow::Cow;


#[derive(Clone, Debug)]
pub struct BlockHeader<'a> {
    pub number: BlockNumber,
    pub hash: Cow<'a, str>,
    pub parent_number: BlockNumber,
    pub parent_hash: Cow<'a, str>,
    pub block_timestamp: Option<i64>,
    pub ingest_timestamp: i64
}


#[derive(Clone, Debug)]
pub struct Block<'a> {
    header: BlockHeader<'a>,
    data: Cow<'a, [u8]>
}


pub trait BlockBatch {
    fn items(&self) -> &[Block<'_>];
}


pub trait BlockHeaderBatch {
    fn items(&self) -> &[BlockHeader<'_>];
}


pub type BlockBatchBox = Box<dyn BlockBatch>;
pub type BlockHeaderBatchBox = Box<dyn BlockHeaderBatch>;


pub trait Batch: 'static {
    type Item<'a>;

    fn items(&self) -> &[Self::Item<'_>];
}


impl<T: Batch> BlockBatch for T
where
    T: for<'a> Batch<Item<'a> = Block<'a>>
{
    #[inline]
    fn items(&self) -> &[Block<'_>] {
        self.items()
    }
}


#[async_trait::async_trait]
pub trait Storage {
    async fn save_block(&self, block: &Block<'_>) -> anyhow::Result<()>;
}