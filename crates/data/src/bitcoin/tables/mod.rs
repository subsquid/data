mod block;
mod common;
mod input;
mod output;
mod transaction;

pub use block::*;
pub use input::*;
pub use output::*;
pub use transaction::*;

use super::model::Block;
use sqd_data_core::chunk_builder;

chunk_builder! {
    BitcoinChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
        inputs: InputBuilder,
        outputs: OutputBuilder,
    }
}

impl sqd_data_core::BlockChunkBuilder for BitcoinChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        for (tx_idx, tx) in block.transactions.iter().enumerate() {
            let tx_idx = tx_idx as u32;
            self.transactions.push(block, tx_idx, tx);

            for (in_idx, input) in tx.vin.iter().enumerate() {
                self.inputs.push(block, tx_idx, in_idx as u32, input);
            }

            for output in tx.vout.iter() {
                self.outputs.push(block, tx_idx, output);
            }
        }

        Ok(())
    }
}
