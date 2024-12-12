mod common;
mod block;
mod transaction;
mod instruction;
mod log_message;
mod balance;
mod token_balance;
mod reward;


pub use balance::*;
pub use block::*;
pub use instruction::*;
pub use log_message::*;
pub use reward::*;
pub use token_balance::*;
pub use transaction::*;


use super::model::Block;
use sqd_data_core::{chunk_builder, HashAndHeight};


chunk_builder! {
    SolanaChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
        instructions: InstructionBuilder,
        balances: BalanceBuilder,
        token_balances: TokenBalanceBuilder,
        logs: LogMessageBuilder,
        rewards: RewardBuilder,
    }
}


impl sqd_data_core::Builder for SolanaChunkBuilder {
    fn push(&mut self, line: &String) -> anyhow::Result<HashAndHeight> {
        let block: Block = serde_json::from_str(line).unwrap();
        let hash_and_height = HashAndHeight {
            hash: block.header.hash.clone(),
            height: block.header.height,
        };
        let block_number = block.header.height;

        self.blocks.push(&block.header);

        for row in block.transactions.iter() {
            self.transactions.push(block_number, row)
        }

        for row in block.instructions.iter() {
            self.instructions.push(block_number, row)
        }

        for row in block.logs.iter() {
            self.logs.push(block_number, row)
        }

        for row in block.balances.iter() {
            self.balances.push(block_number, row)
        }

        for row in block.token_balances.iter() {
            self.token_balances.push(block_number, row)
        }

        for row in block.rewards.iter() {
            self.rewards.push(block_number, row)
        }

        Ok(hash_and_height)
    }
}