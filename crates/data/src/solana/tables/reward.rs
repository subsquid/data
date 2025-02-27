use crate::solana::model::{Block, Reward};
use crate::solana::tables::common::Base58Builder;
use sqd_array::builder::{Int64Builder, StringBuilder, UInt64Builder, UInt8Builder};
use sqd_data_core::table_builder;


table_builder! {
    RewardBuilder {
        block_number: UInt64Builder,
        pubkey: Base58Builder,
        lamports: Int64Builder,
        post_balance: UInt64Builder,
        reward_type: StringBuilder,
        commission: UInt8Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.sort_key = vec!["pubkey", "block_number"];
        d.options.add_stats("pubkey");
        d.options.use_dictionary("pubkey");
        d.options.use_dictionary("reward_type");
        d.options.row_group_size = 5_000;
    }
}


impl RewardBuilder {
    pub fn push(&mut self, block: &Block, row: &Reward) -> anyhow::Result<()> {
        self.block_number.append(block.header.number);
        self.pubkey.append(block.get_account(row.pubkey)?);
        self.lamports.append(row.lamports);
        self.post_balance.append(row.post_balance);
        self.reward_type.append_option(row.reward_type.as_deref());
        self.commission.append_option(row.commission);
        Ok(())
    }
}