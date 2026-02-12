use crate::bitcoin::model::{Block, Transaction};
use crate::bitcoin::tables::common::*;
use sqd_array::builder::{UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        hex: HexBytesBuilder,
        txid: HexBytesBuilder,
        hash: HexBytesBuilder,
        size: UInt64Builder,
        vsize: UInt64Builder,
        weight: UInt64Builder,
        version: UInt32Builder,
        locktime: UInt32Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["block_number", "transaction_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.row_group_size = 10_000;
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, transaction_index: u32, row: &Transaction) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(transaction_index);
        self.hex.append(&row.hex);
        self.txid.append(&row.txid);
        self.hash.append(&row.hash);
        self.size.append(row.size);
        self.vsize.append(row.vsize);
        self.weight.append(row.weight);
        self.version.append(row.version);
        self.locktime.append(row.locktime);
    }
}
