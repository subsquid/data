use crate::solana::model::LogMessage;
use crate::solana::tables::common::{Base58Builder, InstructionAddressListBuilder};
use sqd_array::builder::{StringBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;
use sqd_primitives::BlockNumber;

table_builder! {
    LogMessageBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        log_index: UInt32Builder,
        instruction_address: InstructionAddressListBuilder,
        program_id: Base58Builder,
        kind: StringBuilder,
        message: StringBuilder,
        message_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "log_index", "instruction_address"];
        d.sort_key = vec!["program_id", "block_number", "transaction_index", "log_index"];
        d.options.add_stats("program_id");
        d.options.add_stats("block_number");
        d.options.use_dictionary("program_id");
        d.options.use_dictionary("kind");
        d.options.row_group_size = 5_000;
    }
}


impl LogMessageBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &LogMessage) {
        self.block_number.append(block_number);
        self.transaction_index.append(row.transaction_index);
        self.log_index.append(row.log_index);

        for address in &row.instruction_address {
            self.instruction_address.values().append(*address);
        }
        self.instruction_address.append();

        self.program_id.append(&row.program_id);
        self.kind.append(row.kind.to_str());
        self.message.append(&row.message);
        self.message_size.append(row.message.len() as u64);
    }
}