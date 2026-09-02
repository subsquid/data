mod block;
mod common;
mod logs;
mod state_diff;
mod trace;
mod transaction;

pub use block::*;
pub use logs::*;
use sqd_data_core::chunk_builder;
pub use state_diff::*;
pub use trace::*;
pub use transaction::*;

use super::model::Block;

chunk_builder! {
    EvmChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
        logs: LogBuilder,
        traces: TraceBuilder,
        statediffs: StateDiffBuilder,
    }
}

impl sqd_data_core::BlockChunkBuilder for EvmChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        for row in block.transactions.iter() {
            self.transactions.push(block, row);
        }

        for row in block.logs.iter().flatten() {
            self.logs.push(block, row);
        }

        for row in block.state_diffs.iter().flatten() {
            self.statediffs.push(block, row);
        }

        for row in block.traces.iter().flatten() {
            self.traces.push(block, row);
        }
        Ok(())
    }
}
