use crate::solana::model::Reward;
use crate::solana::tables::common::Base58Builder;
use crate::types::BlockNumber;
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
    }
}


impl RewardBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Reward) {
        self.block_number.append(block_number);
        self.pubkey.append(&row.pubkey);
        self.lamports.append(row.lamports);
        self.post_balance.append(row.post_balance);
        self.reward_type.append_option(row.reward_type.as_ref().map(|s| s.as_str()));
        self.commission.append_option(row.commission);
    }
}