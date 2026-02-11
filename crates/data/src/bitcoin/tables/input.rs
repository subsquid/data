use crate::bitcoin::model::{Block, TransactionInput};
use crate::bitcoin::tables::common::*;
use sqd_array::builder::{
    BooleanBuilder, Float64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use sqd_data_core::table_builder;


table_builder! {
    InputBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        input_index: UInt32Builder,
        r#type: StringBuilder,
        txid: HexBytesBuilder,
        vout: UInt32Builder,
        script_sig_hex: HexBytesBuilder,
        script_sig_asm: StringBuilder,
        sequence: UInt32Builder,
        coinbase: HexBytesBuilder,
        tx_in_witness: WitnessListBuilder,

        // Denormalized prevout data
        prevout_generated: BooleanBuilder,
        prevout_height: UInt64Builder,
        prevout_value: Float64Builder,
        prevout_script_pub_key_hex: HexBytesBuilder,
        prevout_script_pub_key_asm: StringBuilder,
        prevout_script_pub_key_desc: StringBuilder,
        prevout_script_pub_key_type: StringBuilder,
        prevout_script_pub_key_address: StringBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number", "prevout_height"];
        d.downcast.item_index = vec!["transaction_index", "input_index"];
        d.sort_key = vec!["block_number", "transaction_index", "input_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.row_group_size = 10_000;
    }
}


impl InputBuilder {
    pub fn push(
        &mut self,
        block: &Block,
        transaction_index: u32,
        input_index: u32,
        row: &TransactionInput,
    ) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(transaction_index);
        self.input_index.append(input_index);

        match row {
            TransactionInput::Tx(tx_input) => {
                self.r#type.append("tx");
                self.txid.append(&tx_input.txid);
                self.vout.append(tx_input.vout);
                self.script_sig_hex.append(&tx_input.script_sig.hex);
                self.script_sig_asm
                    .append_option(tx_input.script_sig.asm.as_deref());
                self.sequence.append(tx_input.sequence);
                self.coinbase.append_option(None::<&str>);

                for witness in tx_input.tx_in_witness.iter().flatten() {
                    self.tx_in_witness.values().append(witness);
                }
                self.tx_in_witness.append();

                // Denormalized prevout data
                if let Some(prevout) = &tx_input.prevout {
                    self.prevout_generated.append(prevout.generated);
                    self.prevout_height.append(prevout.height);
                    self.prevout_value.append(prevout.value);
                    self.prevout_script_pub_key_hex
                        .append(&prevout.script_pub_key.hex);
                    self.prevout_script_pub_key_asm
                        .append_option(prevout.script_pub_key.asm.as_deref());
                    self.prevout_script_pub_key_desc
                        .append_option(prevout.script_pub_key.desc.as_deref());
                    self.prevout_script_pub_key_type
                        .append_option(prevout.script_pub_key.type_.as_deref());
                    self.prevout_script_pub_key_address
                        .append_option(prevout.script_pub_key.address.as_deref());
                } else {
                    self.prevout_generated.append_option(None::<bool>);
                    self.prevout_height.append_option(None::<u64>);
                    self.prevout_value.append_option(None::<f64>);
                    self.prevout_script_pub_key_hex.append_option(None::<&str>);
                    self.prevout_script_pub_key_asm.append_option(None::<&str>);
                    self.prevout_script_pub_key_desc.append_option(None::<&str>);
                    self.prevout_script_pub_key_type.append_option(None::<&str>);
                    self.prevout_script_pub_key_address
                        .append_option(None::<&str>);
                }
            }
            TransactionInput::Coinbase(cb_input) => {
                self.r#type.append("coinbase");
                self.txid.append_option(None::<&str>);
                self.vout.append_option(None::<u32>);
                self.script_sig_hex.append_option(None::<&str>);
                self.script_sig_asm.append_option(None::<&str>);
                self.sequence.append(cb_input.sequence);
                self.coinbase.append(&cb_input.coinbase);

                for witness in cb_input.tx_in_witness.iter().flatten() {
                    self.tx_in_witness.values().append(witness);
                }
                self.tx_in_witness.append();

                // Coinbase inputs have no prevout
                self.prevout_generated.append_option(None::<bool>);
                self.prevout_height.append_option(None::<u64>);
                self.prevout_value.append_option(None::<f64>);
                self.prevout_script_pub_key_hex.append_option(None::<&str>);
                self.prevout_script_pub_key_asm.append_option(None::<&str>);
                self.prevout_script_pub_key_desc.append_option(None::<&str>);
                self.prevout_script_pub_key_type.append_option(None::<&str>);
                self.prevout_script_pub_key_address
                    .append_option(None::<&str>);
            }
        }
    }
}
