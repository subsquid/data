mod common;
mod block;
// mod transaction;
// mod instruction;
// mod log_message;
// mod balance;
// mod token_balance;
// mod reward;


// pub use balance::*;
pub use block::*;
// pub use instruction::*;
// pub use log_message::*;
// pub use reward::*;
// pub use token_balance::*;
// pub use transaction::*;

use super::model::Block;
use sqd_data_core::chunk_builder;


chunk_builder! {
    EVMChunkBuilder {
        blocks: BlockBuilder,
        // transactions: TransactionBuilder,
        // instructions: InstructionBuilder,
        // balances: BalanceBuilder,
        // token_balances: TokenBalanceBuilder,
        // logs: LogMessageBuilder,
        // rewards: RewardBuilder,
    }
}


impl sqd_data_core::BlockChunkBuilder for EVMChunkBuilder {
    type Block = Block;

    fn push(&mut self, block: &Self::Block) -> anyhow::Result<()> {
        self.blocks.push(&block.header);

        // for row in block.transactions.iter() {
        //     self.transactions.push(block, row)?
        // }

        // for row in block.instructions.iter() {
        //     self.instructions.push(block, row)?
        // }

        // for row in block.logs.iter() {
        //     self.logs.push(block, row)?
        // }

        // for row in block.balances.iter() {
        //     self.balances.push(block, row)?
        // }

        // for row in block.token_balances.iter() {
        //     self.token_balances.push(block, row)?
        // }

        // for row in block.rewards.iter() {
        //     self.rewards.push(block, row)?
        // }
        
        Ok(())
    }
}