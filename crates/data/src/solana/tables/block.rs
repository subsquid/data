use crate::solana::model::BlockHeader;
use crate::solana::tables::common::*;
use sqd_array::builder::{TimestampSecondBuilder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: Base58Builder,
        slot: UInt64Builder,
        parent_slot: UInt64Builder,
        parent_hash: Base58Builder,
        timestamp: TimestampSecondBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number", "slot", "parent_slot"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.height);
        self.hash.append(&row.hash);
        self.slot.append(row.slot);
        self.parent_slot.append(row.parent_slot);
        self.parent_hash.append(&row.parent_hash);
        self.timestamp.append(row.timestamp);
    }
}
