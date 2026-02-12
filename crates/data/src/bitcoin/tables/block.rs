use crate::bitcoin::model::BlockHeader;
use crate::bitcoin::tables::common::*;
use sqd_array::builder::{
    Float64Builder, TimestampSecondBuilder, UInt32Builder, UInt64Builder,
};
use sqd_data_core::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: HexBytesBuilder,
        parent_hash: HexBytesBuilder,
        timestamp: TimestampSecondBuilder,
        version: UInt32Builder,
        merkle_root: HexBytesBuilder,
        nonce: UInt32Builder,
        target: HexBytesBuilder,
        bits: HexBytesBuilder,
        difficulty: Float64Builder,
        chain_work: HexBytesBuilder,
        stripped_size: UInt64Builder,
        size: UInt64Builder,
        weight: UInt64Builder,
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
        self.number.append(row.number);
        self.hash.append(&row.hash);
        self.parent_hash.append(&row.parent_hash);
        self.timestamp.append(row.timestamp as i64);
        self.version.append(row.version);
        self.merkle_root.append(&row.merkle_root);
        self.nonce.append(row.nonce);
        self.target.append(&row.target);
        self.bits.append(&row.bits);
        self.difficulty.append(row.difficulty);
        self.chain_work.append(&row.chain_work);
        self.stripped_size.append(row.stripped_size);
        self.size.append(row.size);
        self.weight.append(row.weight);
    }
}
