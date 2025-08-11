use crate::solana::model::BlockHeader;
use crate::solana::tables::common::*;
use sqd_array::builder::{TimestampSecondBuilder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: Base58Builder,
        parent_number: UInt64Builder,
        parent_hash: Base58Builder,
        height: UInt64Builder,
        timestamp: TimestampSecondBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number", "parent_number", "height"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.number);
        self.hash.append(&row.hash);
        self.parent_number.append(row.parent_number);
        self.parent_hash.append(&row.parent_hash);
        self.height.append_option(row.height);
        self.timestamp.append_option(row.timestamp);
    }
}
