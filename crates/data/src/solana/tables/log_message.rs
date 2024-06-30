use arrow::array::{ArrayRef, StringBuilder, UInt32Builder, UInt64Builder};

use sqd_primitives::{BlockNumber, ItemIndex};

use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::core::downcast::Downcast;
use crate::solana::model::LogMessage;
use crate::solana::tables::common::{Base58Builder, InstructionAddressListBuilder};
use crate::struct_builder;


struct_builder! {
    LogMessageBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        log_index: UInt32Builder,
        instruction_address: InstructionAddressListBuilder,
        program_id: Base58Builder,
        kind: StringBuilder,
        message: StringBuilder,
    }
}


#[derive(Default)]
pub struct LogMessageProcessor {
    downcast: Downcast
}


impl RowProcessor for LogMessageProcessor {
    type Row = LogMessage;
    type Builder = LogMessageBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.transaction_index.append_value(row.transaction_index);
        builder.log_index.append_value(row.log_index);

        for address in &row.instruction_address {
            builder.instruction_address.values().append_value(*address);
        }
        builder.instruction_address.append(true);

        builder.program_id.append_value(&row.program_id);
        builder.kind.append_value(serde_json::to_string(&row.kind).unwrap());
        builder.message.append_value(&row.message);
        builder.append(true);
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.block_number);
        self.downcast.item.reg(row.transaction_index);
    }

    fn post(&mut self, array: ArrayRef) -> ArrayRef {
        let array = self.downcast.block_number.downcast_columns(array, &["block_number"]);
        self.downcast.item.downcast_columns(array, &["transaction_index"])
    }
}


impl Row for LogMessage {
    type Key = (Vec<u8>, Vec<u8>, BlockNumber, ItemIndex);

    fn key(&self) -> Self::Key {
        let program_id = self.program_id.as_bytes().to_vec();
        let kind = serde_json::to_string(&self.kind).unwrap().as_bytes().to_vec();
        (program_id, kind, self.block_number, self.transaction_index)
    }
}