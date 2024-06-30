use arrow::array::{ArrayRef, Int64Builder, StringBuilder, UInt64Builder, UInt8Builder};

use sqd_primitives::BlockNumber;

use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::core::downcast::Downcast;
use crate::solana::model::Reward;
use crate::solana::tables::common::Base58Builder;
use crate::struct_builder;


struct_builder! {
    RewardBuilder {
        block_number: UInt64Builder,
        pubkey: Base58Builder,
        lamports: Int64Builder,
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
        builder.lamports.append_value(row.lamports.0);
        builder.post_balance.append_value(row.post_balance.0);
        builder.reward_type.append_option(row.reward_type.as_ref());
        builder.commission.append_option(row.commission);
        builder.append(true);
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.block_number);
    }

    fn post(&self, array: ArrayRef) -> ArrayRef {
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