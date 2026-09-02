mod block;
mod common;
mod internal_transaction;
mod log;
mod transaction;

pub use block::*;
pub use internal_transaction::*;
pub use log::*;
use sqd_data_core::chunk_builder;
pub use transaction::*;

use super::model::Block;

chunk_builder! {
    TronChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
        logs: LogBuilder,
        internal_transactions: InternalTransactionBuilder,
    }
}

impl sqd_data_core::BlockChunkBuilder for TronChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        for row in block.transactions.iter() {
            self.transactions.push(block, row);
        }

        for row in block.logs.iter() {
            self.logs.push(block, row);
        }

        for row in block.internal_transactions.iter() {
            self.internal_transactions.push(block, row);
        }

        Ok(())
    }
}
