use crate::solana::model::{Balance, Block};
use crate::solana::tables::common::Base58Builder;
use sqd_array::builder::{UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


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
        d.options.add_stats("block_number");
        d.options.use_dictionary("account");
        d.options.row_group_size = 5_000;
    }
}


impl BalanceBuilder {
    pub fn push(&mut self, block: &Block, row: &Balance) -> anyhow::Result<()> {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);
        self.account.append(block.get_account(row.account)?);
        self.pre.append(row.pre);
        self.post.append(row.post);
        Ok(())
    }
}