use crate::tron::model::{Block, InternalTransaction};
use crate::tron::tables::common::*;
use sqd_array::builder::{BooleanBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    InternalTransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        internal_transaction_index: UInt32Builder,
        hash: HexBytesBuilder,
        caller_address: HexBytesBuilder,
        transfer_to_address: HexBytesBuilder,
        call_value_info: JsonBuilder,
        note: HexBytesBuilder,
        rejected: BooleanBuilder,
        extra: JsonBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "internal_transaction_index"];
        d.sort_key = vec![
            "transfer_to_address",
            "caller_address",
            "block_number",
            "transaction_index",
            "internal_transaction_index",
        ];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("internal_transaction_index");
        d.options.add_stats("transfer_to_address");
        d.options.add_stats("caller_address");
        d.options.row_group_size = 10_000;
    }
}


impl InternalTransactionBuilder {
    pub fn push(&mut self, block: &Block, row: &InternalTransaction) {
        self.block_number.append(block.header.height);
        self.transaction_index.append(row.transaction_index);
        self.internal_transaction_index.append(row.internal_transaction_index);
        self.hash.append(&row.hash);
        self.caller_address.append(&row.caller_address);
        self.transfer_to_address.append_option(row.transfer_to_address.as_deref());

        let call_value_info = serde_json::to_string(&row.call_value_info).unwrap();
        self.call_value_info.append(&call_value_info);

        self.note.append(&row.note);
        self.rejected.append_option(row.rejected);
        self.extra.append_option(row.extra.as_deref());
    }
}
