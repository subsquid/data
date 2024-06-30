use arrow::array::{ArrayRef, UInt32Builder, UInt64Builder, UInt16Builder};
use sqd_primitives::{BlockNumber, ItemIndex};


use crate::core::downcast::Downcast;
use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::solana::model::TokenBalance;
use crate::solana::tables::common::Base58Builder;
use crate::struct_builder;


struct_builder! {
    TokenBalanceBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        account: Base58Builder,
        pre_mint: Base58Builder,
        post_mint: Base58Builder,
        pre_decimals: UInt16Builder,
        post_decimals: UInt16Builder,
        pre_program_id: Base58Builder,
        post_program_id: Base58Builder,
        pre_owner: Base58Builder,
        post_owner: Base58Builder,
        pre_amount: UInt64Builder,
        post_amount: UInt64Builder,
    }
}


#[derive(Default)]
pub struct TokenBalanceProcessor {
    downcast: Downcast
}


impl RowProcessor for TokenBalanceProcessor {
    type Row = TokenBalance;
    type Builder = TokenBalanceBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.transaction_index.append_value(row.transaction_index);
        builder.account.append_value(&row.account);
        builder.pre_mint.append_option(row.pre_mint.as_ref());
        builder.post_mint.append_option(row.post_mint.as_ref());
        builder.pre_decimals.append_option(row.pre_decimals);
        builder.post_decimals.append_option(row.post_decimals);
        builder.pre_program_id.append_option(row.pre_program_id.as_ref());
        builder.post_program_id.append_option(row.post_program_id.as_ref());
        builder.pre_owner.append_option(row.pre_owner.as_ref());
        builder.post_owner.append_option(row.post_owner.as_ref());
        builder.pre_amount.append_option(row.pre_amount.map(|v| v.0));
        builder.post_amount.append_option(row.post_amount.map(|v| v.0));
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


impl Row for TokenBalance {
    type Key = (Option<Vec<u8>>, Option<Vec<u8>>, Vec<u8>, BlockNumber, ItemIndex);

    fn key(&self) -> Self::Key {
        let post_program_id = self.post_program_id.as_ref().map(|val| val.as_bytes().to_vec());
        let post_mint = self.post_mint.as_ref().map(|val| val.as_bytes().to_vec());
        let account = self.account.as_bytes().to_vec();
        (post_program_id, post_mint, account, self.block_number, self.transaction_index)
    }
}