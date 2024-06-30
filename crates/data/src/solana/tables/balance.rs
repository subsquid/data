use arrow::array::{ArrayRef, UInt32Builder, UInt64Builder};

use sqd_primitives::{BlockNumber, ItemIndex};

use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::core::downcast::Downcast;
use crate::solana::model::Balance;
use crate::solana::tables::common::Base58Builder;
use crate::struct_builder;


struct_builder! {
    BalanceBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        account: Base58Builder,
        pre: UInt64Builder,
        post: UInt64Builder,
    }
}


#[derive(Default)]
pub struct BalanceProcessor {
    downcast: Downcast
}


impl RowProcessor for BalanceProcessor {
    type Row = Balance;
    type Builder = BalanceBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.transaction_index.append_value(row.transaction_index);
        builder.account.append_value(&row.account);
        builder.pre.append_value(row.pre.0);
        builder.post.append_value(row.post.0);
        builder.append(true);
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.block_number);
        self.downcast.item.reg(row.transaction_index);
    }

    fn post(&self, array: ArrayRef) -> ArrayRef {
        let array = self.downcast.block_number.downcast_columns(array, &["block_number"]);
        self.downcast.item.downcast_columns(array, &["transaction_index"])
    }
}


impl Row for Balance {
    type Key = (Vec<u8>, BlockNumber, ItemIndex);

    fn key(&self) -> Self::Key {
        let account = self.account.as_bytes().to_vec();
        (account, self.block_number, self.transaction_index)
    }
}