use crate::bitcoin::model::{Block, TransactionOutput};
use crate::bitcoin::tables::common::*;
use sqd_array::builder::{Float64Builder, StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    OutputBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        output_index: UInt32Builder,
        value: Float64Builder,
        script_pub_key_hex: HexBytesBuilder,
        script_pub_key_asm: StringBuilder,
        script_pub_key_desc: StringBuilder,
        script_pub_key_type: StringBuilder,
        script_pub_key_address: StringBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "output_index"];
        d.sort_key = vec!["script_pub_key_address", "block_number", "transaction_index", "output_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("script_pub_key_address");
        d.options.add_stats("script_pub_key_type");
        d.options.use_dictionary("script_pub_key_type");
        d.options.use_dictionary("script_pub_key_address");
        d.options.row_group_size = 10_000;
    }
}


impl OutputBuilder {
    pub fn push(
        &mut self,
        block: &Block,
        transaction_index: u32,
        row: &TransactionOutput,
    ) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(transaction_index);
        self.output_index.append(row.n);
        self.value.append(row.value);
        self.script_pub_key_hex.append(&row.script_pub_key.hex);
        self.script_pub_key_asm
            .append_option(row.script_pub_key.asm.as_deref());
        self.script_pub_key_desc
            .append_option(row.script_pub_key.desc.as_deref());
        self.script_pub_key_type
            .append_option(row.script_pub_key.type_.as_deref());
        self.script_pub_key_address
            .append_option(row.script_pub_key.address.as_deref());
    }
}
