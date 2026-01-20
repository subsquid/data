mod action;
mod block;

pub use action::*;
pub use block::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


chunk_builder! {
    HyperliquidReplicaCmdsChunkBuilder {
        blocks: BlockBuilder,
        actions: ActionBuilder,
    }
}


impl sqd_data_core::BlockChunkBuilder for HyperliquidReplicaCmdsChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header)?;

        for row in &block.actions {
            self.actions.push(block, row)?;
        }

        Ok(())
    }
}

