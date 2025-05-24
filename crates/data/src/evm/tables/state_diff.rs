use crate::evm::model::{Block, StateDiff};
use sqd_array::builder::{StringBuilder, UInt64Builder, UInt32Builder};
use sqd_data_core::table_builder;

use super::common::HexBytesBuilder;


table_builder! {
    StateDiffBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        address: HexBytesBuilder,
        key: StringBuilder,
        kind: StringBuilder,
        prev: HexBytesBuilder,
        next: HexBytesBuilder,

        prev_size: UInt64Builder,
        next_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["address", "block_number", "transaction_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("address");
        d.options.add_stats("key");
        d.options.use_dictionary("address");
        d.options.use_dictionary("kind");
        d.options.row_group_size = 10_000;
    }
}

impl StateDiffBuilder {
    pub fn push(&mut self, block: &Block, row: &StateDiff) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);
        self.address.append(&row.address);
        self.key.append(&row.key);
        self.kind.append(&row.kind);
        self.prev.append_option(row.prev.as_deref());
        self.next.append_option(row.next.as_deref());

        self.prev_size.append(row.prev.as_ref().map_or(0, |v| v.len()) as u64);
        self.next_size.append(row.next.as_ref().map_or(0, |v| v.len()) as u64);
    }
}
