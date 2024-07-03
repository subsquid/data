use arrow::array::{TimestampSecondBuilder, UInt64Builder};

use crate::core::ArrowDataType;
use crate::solana::model::BlockHeader;
use crate::solana::tables::common::*;
use crate::table_builder;


table_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: Base58Builder,
        slot: UInt64Builder,
        parent_slot: UInt64Builder,
        parent_hash: Base58Builder,
        timestamp: TimestampSecondBuilder,
    }

    description(d) {
        d.downcast.block_number = vec!["number", "slot", "parent_slot"];
    }
}


impl BlockBuilder {
    pub fn push(&mut self, row: &BlockHeader) {
        self.number.append_value(row.height);
        self.hash.append_value(&row.parent_hash);
        self.slot.append_value(row.slot);
        self.parent_slot.append_value(row.parent_slot);
        self.parent_hash.append_value(&row.parent_hash);
        self.timestamp.append_value(row.timestamp);
    }
}
