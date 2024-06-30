use arrow::array::{ArrayRef, TimestampSecondBuilder, UInt64Builder};

use sqd_primitives::BlockNumber;

use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::core::downcast::Downcast;
use crate::solana::model::BlockHeader;
use crate::solana::tables::common::*;
use crate::struct_builder;


struct_builder! {
    BlockBuilder {
        number: UInt64Builder,
        hash: Base58Builder,
        slot: UInt64Builder,
        parent_slot: UInt64Builder,
        parent_hash: Base58Builder,
        timestamp: TimestampSecondBuilder,
    }
}


#[derive(Default)]
pub struct BlockProcessor {
    downcast: Downcast
}


impl RowProcessor for BlockProcessor {
    type Row = BlockHeader;
    type Builder = BlockBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.number.append_value(row.height);
        builder.hash.append_value(&row.parent_hash);
        builder.slot.append_value(row.slot);
        builder.parent_slot.append_value(row.parent_slot);
        builder.parent_hash.append_value(&row.parent_hash);
        builder.timestamp.append_value(row.timestamp);
        builder.append(true)
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.height);
    }

    fn post(&mut self, array: ArrayRef) -> ArrayRef {
        self.downcast.block_number.downcast_columns(array, &["number"])
    }

    fn capacity(&self) -> usize {
        2000
    }
}


impl Row for BlockHeader {
    type Key = BlockNumber;

    fn key(&self) -> Self::Key {
        self.height
    }
}