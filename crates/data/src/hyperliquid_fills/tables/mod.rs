mod block;
mod fill;

pub use block::*;
pub use fill::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


chunk_builder! {
    HyperliquidFillsChunkBuilder {
        blocks: BlockBuilder,
        fills: FillBuilder,
    }
}


impl sqd_data_core::BlockChunkBuilder for HyperliquidFillsChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        for row in block.fills.iter() {
            self.fills.push(block, row)?;
        }

        Ok(())
    }
}

