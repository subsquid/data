use crate::hyperliquid_fills::model::BlockHeader;
use sqd_array::builder::{StringBuilder, UInt64Builder, TimestampMillisecondBuilder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: StringBuilder,
        parent_hash: StringBuilder,
        timestamp: TimestampMillisecondBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, block: &BlockHeader) {
        self.number.append(block.number);
        self.hash.append(&block.hash);
        self.parent_hash.append(&block.parent_hash);
        self.timestamp.append(block.timestamp);
    }
}
