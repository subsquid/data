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

    fn set_position(&mut self, next_block: BlockNumber, parent_block_hash: Option<String>);
}
