use arrow::array::{UInt32Builder, UInt64Builder};

use sqd_primitives::BlockNumber;

use crate::solana::model::Balance;
use crate::solana::tables::common::Base58Builder;
use crate::table_builder;


table_builder! {
    BalanceBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        account: Base58Builder,
        pre: UInt64Builder,
        post: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["account", "block_number", "transaction_index"];
        d.options.add_stats("account");
        d.options.add_stats("block_number")
    }
}


impl BalanceBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Balance) {
        self.block_number.append_value(block_number);
        self.transaction_index.append_value(row.transaction_index);
        self.account.append_value(&row.account);
        self.pre.append_value(row.pre);
        self.post.append_value(row.post);
    }
}