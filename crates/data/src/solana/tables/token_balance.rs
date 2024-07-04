use arrow::array::{UInt16Builder, UInt32Builder, UInt64Builder};

use sqd_primitives::BlockNumber;

use crate::solana::model::TokenBalance;
use crate::solana::tables::common::Base58Builder;
use crate::table_builder;


table_builder! {
    TokenBalanceBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        account: Base58Builder,
        pre_mint: Base58Builder,
        post_mint: Base58Builder,
        pre_decimals: UInt16Builder,
        post_decimals: UInt16Builder,
        pre_program_id: Base58Builder,
        post_program_id: Base58Builder,
        pre_owner: Base58Builder,
        post_owner: Base58Builder,
        pre_amount: UInt64Builder,
        post_amount: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["post_program_id", "post_mint", "account", "block_number", "transaction_index"];
    }
}


impl TokenBalanceBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &TokenBalance) {
        self.block_number.append_value(block_number);
        self.transaction_index.append_value(row.transaction_index);
        self.account.append_value(&row.account);
        self.pre_mint.append_option(row.pre_mint.as_ref());
        self.post_mint.append_option(row.post_mint.as_ref());
        self.pre_decimals.append_option(row.pre_decimals);
        self.post_decimals.append_option(row.post_decimals);
        self.pre_program_id.append_option(row.pre_program_id.as_ref());
        self.post_program_id.append_option(row.post_program_id.as_ref());
        self.pre_owner.append_option(row.pre_owner.as_ref());
        self.post_owner.append_option(row.post_owner.as_ref());
        self.pre_amount.append_option(row.pre_amount);
        self.post_amount.append_option(row.post_amount);
    }
}