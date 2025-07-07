use futures::Stream;
use sqd_primitives::{Block, BlockNumber, BlockRef};


pub enum DataEvent<B> {
    FinalizedHead(BlockRef),
    Block {
        block: B,
        is_final: bool
    },
    Fork(Vec<BlockRef>),
    MaybeOnHead
}


pub trait DataSource: Stream<Item = DataEvent<Self::Block>> + Unpin {
    type Block: Block;

    fn set_position(&mut self, next_block: BlockNumber, parent_block_hash: Option<&str>);

    fn get_next_block(&self) -> BlockNumber;

    fn get_parent_block_hash(&self) -> Option<&str>;
}


impl<B> DataEvent<B> {
    pub fn map<T>(self, f: impl FnOnce(B, bool) -> T) -> DataEvent<T> {
        match self {
            DataEvent::FinalizedHead(head) => DataEvent::FinalizedHead(head),
            DataEvent::Block { block, is_final } => DataEvent::Block {
                block: f(block, is_final),
                is_final
            },
            DataEvent::Fork(prev) => DataEvent::Fork(prev),
            DataEvent::MaybeOnHead => DataEvent::MaybeOnHead
        }
    }
}