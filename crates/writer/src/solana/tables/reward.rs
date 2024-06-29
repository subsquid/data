use arrow::array::{ArrayRef, UInt64Builder, StringBuilder, Int64Builder, UInt8Builder};

use crate::array_builder::*;
use crate::downcast::Downcast;
use crate::primitives::BlockNumber;
use crate::row::Row;
use crate::row_processor::RowProcessor;
use crate::solana::model::Reward;
use crate::solana::tables::common::Base58Builder;


struct_builder! {
    RewardBuilder {
        block_number: UInt64Builder,
        pubkey: Base58Builder,
        lamports: Int64Builder, // todo: change to uint64?
        post_balance: UInt64Builder,
        reward_type: StringBuilder,
        commission: UInt8Builder,
    }
}


#[derive(Default)]
pub struct RewardProcessor {
    downcast: Downcast
}


impl RowProcessor for RewardProcessor {
    type Row = Reward;
    type Builder = RewardBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.pubkey.append_value(&row.pubkey);
        builder.lamports.append_value(row.lamports.parse::<i64>().unwrap());
        builder.post_balance.append_value(row.post_balance.parse::<u64>().unwrap());
        builder.reward_type.append_option(row.reward_type.as_ref());
        builder.commission.append_option(row.commission);
        builder.append(true);
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.block_number);
    }

    fn post(&mut self, array: ArrayRef) -> ArrayRef {
        self.downcast.block_number.downcast_columns(array, &["block_number"])
    }
}


impl Row for Reward {
    type Key = (Vec<u8>, BlockNumber);

    fn key(&self) -> Self::Key {
        let pubkey = self.pubkey.as_bytes().to_vec();
        (pubkey, self.block_number)
    }
}