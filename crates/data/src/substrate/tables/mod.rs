mod block;
mod call;
mod common;
mod event;
mod extrinsic;


pub use block::*;
pub use call::*;
pub use event::*;
pub use extrinsic::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


chunk_builder! {
    SubstrateChunkBuilder {
        blocks: BlockBuilder,
        extrinsics: ExtrinsicBuilder,
        calls: CallBuilder,
        events: EventBuilder,
    }
}


impl sqd_data_core::BlockChunkBuilder for SubstrateChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) {
        let block_number = block.header.height;

        self.blocks.push(&block.header);

        for row in block.extrinsics.iter() {
            self.extrinsics.push(block_number, row)
        }

        for row in block.calls.iter() {
            self.calls.push(block_number, row)
        }

        for row in block.events.iter() {
            self.events.push(block_number, row)
        }
    }
}
