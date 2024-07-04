use crate::chunk_builder;
use crate::solana::model::Block;
use crate::solana::tables::balance::BalanceBuilder;
use crate::solana::tables::block::BlockBuilder;
use crate::solana::tables::instruction::InstructionBuilder;
use crate::solana::tables::log_message::LogMessageBuilder;
use crate::solana::tables::reward::RewardBuilder;
use crate::solana::tables::token_balance::TokenBalanceBuilder;
use crate::solana::tables::transaction::TransactionBuilder;


mod common;
pub mod block;
pub mod transaction;
pub mod instruction;
pub mod log_message;
pub mod balance;
pub mod token_balance;
pub mod reward;


chunk_builder! {
    SolanaChunkBuilder {
        blocks: BlockBuilder,
        transactions: TransactionBuilder,
        instructions: InstructionBuilder,
        logs: LogMessageBuilder,
        balances: BalanceBuilder,
        token_balances: TokenBalanceBuilder,
        rewards: RewardBuilder,
    }
}


impl SolanaChunkBuilder {
    pub fn push(&mut self, block: &Block) {
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
    }
}