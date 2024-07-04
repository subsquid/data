use arrow::array::{Int64Builder, StringBuilder, UInt64Builder, UInt8Builder};

use sqd_primitives::BlockNumber;

use crate::solana::model::Reward;
use crate::solana::tables::common::Base58Builder;
use crate::table_builder;

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
    }
}


impl RewardBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Reward) {
        self.block_number.append_value(block_number);
        self.pubkey.append_value(&row.pubkey);
        self.lamports.append_value(row.lamports);
        self.post_balance.append_value(row.post_balance);
        self.reward_type.append_option(row.reward_type.as_ref());
        self.commission.append_option(row.commission);
    }
}