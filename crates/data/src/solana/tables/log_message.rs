use arrow::array::{StringBuilder, UInt32Builder, UInt64Builder};

use sqd_primitives::BlockNumber;

use crate::solana::model::LogMessage;
use crate::solana::tables::common::{Base58Builder, InstructionAddressListBuilder};
use crate::table_builder;


table_builder! {
    LogMessageBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        log_index: UInt32Builder,
        instruction_address: InstructionAddressListBuilder,
        program_id: Base58Builder,
        kind: StringBuilder,
        message: StringBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "log_index", "instruction_address"];
        d.sort_key = vec!["program_id", "block_number", "transaction_index", "log_index"];
    }
}


impl LogMessageBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &LogMessage) {
        self.block_number.append_value(block_number);
        self.transaction_index.append_value(row.transaction_index);
        self.log_index.append_value(row.log_index);

        for address in &row.instruction_address {
            self.instruction_address.values().append_value(*address);
        }
        self.instruction_address.append(true);

        self.program_id.append_value(&row.program_id);
        self.kind.append_value(row.kind.to_str());
        self.message.append_value(&row.message);
    }
}