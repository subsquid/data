use crate::evm::model::{Block, Log};
use sqd_array::builder::{UInt64Builder, UInt32Builder};
use sqd_data_core::table_builder;

use super::common::HexBytesBuilder;


table_builder! {
    LogBuilder {
        block_number: UInt64Builder,
        log_index: UInt32Builder,
        transaction_index: UInt32Builder,
        transaction_hash: HexBytesBuilder,
        address: HexBytesBuilder,
        data: HexBytesBuilder,
        topic0: HexBytesBuilder,
        topic1: HexBytesBuilder,
        topic2: HexBytesBuilder,
        topic3: HexBytesBuilder,
        data_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "log_index"];
        d.sort_key = vec!["topic0", "address", "block_number", "log_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("log_index");
        d.options.add_stats("transaction_index");
        d.options.add_stats("address");
        d.options.add_stats("topic0");
        d.options.use_dictionary("address");
        d.options.use_dictionary("topic0");
        d.options.row_group_size = 10_000;
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

        self.topic0.append_option(row.topics.first().map(|x| x.as_str()));
        self.topic1.append_option(row.topics.get(1).map(|x| x.as_str()));
        self.topic2.append_option(row.topics.get(2).map(|x| x.as_str()));
        self.topic3.append_option(row.topics.get(3).map(|x| x.as_str()));

        self.data_size.append(row.data.len() as u64);
    }
}
