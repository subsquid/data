use crate::evm::model::{Block, Log};
use crate::evm::tables::common::*;
use sqd_array::builder::UInt64Builder;
use sqd_data_core::table_builder;

use super::common::HexBytesBuilder;


table_builder! {
    LogBuilder {
        block_number: UInt64Builder,
        log_index: UInt64Builder,
        transaction_index: UInt64Builder,
        transaction_hash: HexBytesBuilder,
        address: HexBytesBuilder,
        data: HexBytesBuilder,
        topics: TopicListBuilder,

        data_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["block_number", "transaction_index", "log_index"];
        d.options.add_stats("block_number");
        d.options.row_group_size = 5_000;
    }
}

impl LogBuilder {
    pub fn push(&mut self, block: &Block, row: &Log) {
        self.block_number.append(block.header.number);
        self.log_index.append(row.log_index);
        self.transaction_index.append(row.transaction_index);
        self.transaction_hash.append(&row.transaction_hash);
        self.address.append(&row.address);
        self.data.append(&row.data);
        for topic in &row.topics {
            self.topics.values().append(topic);
        }
        self.topics.append();

        self.data_size.append(row.data.len() as u64);
    }
}
