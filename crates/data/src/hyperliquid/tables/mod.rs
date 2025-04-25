mod block;
mod transaction;

pub use block::*;
pub use transaction::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


chunk_builder! {
    HyperliquidChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
    }
}


impl sqd_data_core::BlockChunkBuilder for HyperliquidChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        for row in block.transactions.iter() {
            self.transactions.push(block, row);
        }

        Ok(())
    }
}

