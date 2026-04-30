mod common;
mod block;
mod transaction;
mod log;
mod internal_transaction;

pub use block::*;
pub use transaction::*;
pub use log::*;
pub use internal_transaction::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


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
