use super::common::{sighash, HexBytesBuilder, TraceAddressListBuilder};
use crate::evm::model::{Block, Trace, TraceOp};
use sqd_array::builder::{StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


table_builder! {
    TraceBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        trace_address: TraceAddressListBuilder,
        subtraces: UInt32Builder,
        r#type: StringBuilder,
        error: StringBuilder,
        revert_reason: StringBuilder,

        create_from: HexBytesBuilder,
        create_value: HexBytesBuilder,
        create_gas: HexBytesBuilder,
        create_init: HexBytesBuilder,
        create_result_gas_used: HexBytesBuilder,
        create_result_code: HexBytesBuilder,
        create_result_address: HexBytesBuilder,

        call_from: HexBytesBuilder,
        call_to: HexBytesBuilder,
        call_value: HexBytesBuilder,
        call_gas: HexBytesBuilder,
        call_input: HexBytesBuilder,
        call_sighash: HexBytesBuilder,
        call_type: StringBuilder,
        call_call_type: HexBytesBuilder,
        call_result_gas_used: HexBytesBuilder,
        call_result_output: HexBytesBuilder,

        suicide_address: HexBytesBuilder,
        suicide_refund_address: HexBytesBuilder,
        suicide_balance: UInt64Builder,
        reward_author: HexBytesBuilder,
        reward_value: UInt64Builder,
        reward_type: StringBuilder,

        create_init_size: UInt64Builder,
        create_result_code_size: UInt64Builder,
        call_input_size: UInt64Builder,
        call_result_output_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["type", "create_from", "call_sighash", "call_to", "block_number", "transaction_index", "trace_address"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("type");
        d.options.add_stats("create_from");
        d.options.add_stats("call_from");
        d.options.add_stats("call_to");
        d.options.add_stats("call_sighash");
        d.options.use_dictionary("type");
        d.options.use_dictionary("create_from");
        d.options.use_dictionary("call_from");
        d.options.use_dictionary("call_to");
        d.options.use_dictionary("call_sighash");
        d.options.use_dictionary("call_type");
        d.options.row_group_size = 10_000;
    }
}

impl TraceBuilder {
    pub fn push(&mut self, block: &Block, row: &Trace) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);

        for address in &row.trace_address {
            self.trace_address.values().append(*address);
        }
        self.trace_address.append();

        self.subtraces.append(row.subtraces);
        self.error.append_option(row.error.as_deref());
        self.revert_reason.append_option(row.revert_reason.as_deref());
        if let TraceOp::Create { action, result } = &row.op {
            self.r#type.append("create");
            self.create_from.append(&action.from);
            self.create_value.append_option(action.value.as_deref());
            self.create_gas.append(&action.gas);
            self.create_init.append(&action.init);
            self.create_init_size.append(action.init.len() as u64);
            if let Some(res) = result {
                self.create_result_gas_used.append(&res.gas_used);
                self.create_result_code.append(&res.code);
                self.create_result_address.append(&res.address);
                self.create_result_code_size.append(res.code.len() as u64);
            } else {
                self.create_result_gas_used.append_null();
                self.create_result_code.append_null();
                self.create_result_address.append_null();
                self.create_result_code_size.append(0);
            }
        } else {
            self.create_from.append_null();
            self.create_value.append_null();
            self.create_gas.append_null();
            self.create_init.append_null();
            self.create_init_size.append(0);
            self.create_result_gas_used.append_null();
            self.create_result_code.append_null();
            self.create_result_address.append_null();
            self.create_result_code_size.append(0);
        }

        if let TraceOp::Call { action, result } = &row.op {
            self.r#type.append("call");
            self.call_from.append(&action.from);
            self.call_to.append(&action.to);
            self.call_value.append_option(action.value.as_deref());
            self.call_gas.append(&action.gas);
            self.call_input.append(&action.input);
            self.call_sighash.append_option(sighash(&action.input));
            self.call_type.append(&action.r#type);
            self.call_call_type.append(&action.call_type);
            self.call_input_size.append(action.input.len() as u64);
            if let Some(res) = result {
                self.call_result_gas_used.append(&res.gas_used);
                self.call_result_output.append_option(res.output.as_deref());
                self.call_result_output_size.append(res.output.as_ref().map_or(0, |v| v.len()) as u64);
            } else {
                self.call_result_gas_used.append_null();
                self.call_result_output.append_null();
                self.call_result_output_size.append(0);
            }
        } else {
            self.call_from.append_null();
            self.call_to.append_null();
            self.call_value.append_null();
            self.call_gas.append_null();
            self.call_input.append_null();
            self.call_sighash.append_null();
            self.call_type.append_null();
            self.call_call_type.append_null();
            self.call_input_size.append(0);
            self.call_result_gas_used.append_null();
            self.call_result_output.append_null();
            self.call_result_output_size.append(0);
        }

        if let TraceOp::Reward { action } = &row.op {
            self.r#type.append("reward");
            self.reward_author.append(&action.author);
            self.reward_value.append(action.value);
            self.reward_type.append(&action.reward_type);
        } else {
            self.reward_author.append_null();
            self.reward_value.append_option(None);
            self.reward_type.append_null();
        }

        if let TraceOp::SelfDestruct { action} = &row.op {
            self.r#type.append("suicide");
            self.suicide_address.append(&action.address);
            self.suicide_refund_address.append(&action.refund_address);
            self.suicide_balance.append(action.balance);
        } else {
            self.suicide_address.append_null();
            self.suicide_refund_address.append_null();
            self.suicide_balance.append_option(None);
        }
    }
}