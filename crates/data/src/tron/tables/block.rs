use crate::tron::model::BlockHeader;
use crate::tron::tables::common::*;
use sqd_array::builder::{Int32Builder, TimestampMillisecondBuilder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: HexBytesBuilder,
        parent_hash: HexBytesBuilder,
        tx_trie_root: HexBytesBuilder,
        version: Int32Builder,
        timestamp: TimestampMillisecondBuilder,
        witness_address: HexBytesBuilder,
        witness_signature: HexBytesBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number"];
        d.sort_key = vec!["number"];
        d.options.add_stats("number");
        d.options.row_group_size = 5_000;
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append(row.height);
        self.hash.append(&row.hash);
        self.parent_hash.append(&row.parent_hash);
        self.tx_trie_root.append(&row.tx_trie_root);
        self.version.append_option(row.version);
        self.timestamp.append(row.timestamp);
        self.witness_address.append(&row.witness_address);
        self.witness_signature.append_option(row.witness_signature.as_deref());
    }
}
