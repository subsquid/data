use crate::hyperliquid::model::{Block, Transaction};
use sqd_array::builder::{StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


type JsonBuilder = StringBuilder;


table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        user: StringBuilder,
        actions: JsonBuilder,
        actions_size: UInt64Builder,
        raw_tx_hash: StringBuilder,
        error: StringBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec![
            "user",
            "block_number",
            "transaction_index"
        ];
        d.options.add_stats("user");
        d.options.add_stats("block_number");
        d.options.use_dictionary("user");
        d.options.row_group_size = 20_000;
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, transaction: &Transaction) -> anyhow::Result<()> {
        self.block_number.append(block.header.height);
        self.transaction_index.append(transaction.transaction_index);
        self.user.append(&transaction.user);

        let actions = serde_json::to_string(&transaction.actions)?;
        self.actions.append(&actions);
        self.actions_size.append(actions.len() as u64);

        self.raw_tx_hash.append_option(transaction.raw_tx_hash.as_deref());
        self.error.append_option(transaction.error.as_deref());
        Ok(())
    }
}
