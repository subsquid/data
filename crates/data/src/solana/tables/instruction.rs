use arrow::array::{ArrayRef, UInt32Builder, UInt64Builder, StringBuilder, BooleanBuilder};
use sqd_primitives::{BlockNumber, ItemIndex};

use crate::core::downcast::Downcast;
use crate::core::{ArrowDataType, Row, RowProcessor};
use crate::solana::model::Instruction;
use crate::solana::tables::common::{InstructionAddressListBuilder, Base58Builder, BytesBuilder, AccountListBuilder};
use crate::struct_builder;


struct_builder! {
    InstructionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        instruction_address: InstructionAddressListBuilder,
        program_id: Base58Builder,
        data: Base58Builder,
        a0: Base58Builder,
        a1: Base58Builder,
        a2: Base58Builder,
        a3: Base58Builder,
        a4: Base58Builder,
        a5: Base58Builder,
        a6: Base58Builder,
        a7: Base58Builder,
        a8: Base58Builder,
        a9: Base58Builder,
        a10: Base58Builder,
        a11: Base58Builder,
        a12: Base58Builder,
        a13: Base58Builder,
        a14: Base58Builder,
        a15: Base58Builder,
        rest_accounts: AccountListBuilder,
        accounts_size: UInt64Builder,

        compute_units_consumed: UInt64Builder,
        error: StringBuilder,
        is_committed: BooleanBuilder,
        has_dropped_log_messages: BooleanBuilder,

        d1: BytesBuilder,
        d2: BytesBuilder,
        d4: BytesBuilder,
        d8: BytesBuilder,
    }
}


#[derive(Default)]
pub struct InstructionProcessor {
    downcast: Downcast
}


impl RowProcessor for InstructionProcessor {
    type Row = Instruction;
    type Builder = InstructionBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.transaction_index.append_value(row.transaction_index);

        for address in &row.instruction_address {
            builder.instruction_address.values().append_value(*address);
        }
        builder.instruction_address.append(true);

        builder.program_id.append_value(&row.program_id);
        builder.data.append_value(&row.data);
        builder.a0.append_option(row.accounts.get(0));
        builder.a1.append_option(row.accounts.get(1));
        builder.a2.append_option(row.accounts.get(2));
        builder.a3.append_option(row.accounts.get(3));
        builder.a4.append_option(row.accounts.get(4));
        builder.a5.append_option(row.accounts.get(5));
        builder.a6.append_option(row.accounts.get(6));
        builder.a7.append_option(row.accounts.get(7));
        builder.a8.append_option(row.accounts.get(8));
        builder.a9.append_option(row.accounts.get(9));
        builder.a10.append_option(row.accounts.get(10));
        builder.a11.append_option(row.accounts.get(11));
        builder.a12.append_option(row.accounts.get(12));
        builder.a13.append_option(row.accounts.get(13));
        builder.a14.append_option(row.accounts.get(14));
        builder.a15.append_option(row.accounts.get(15));

        if let Some(accounts) = row.accounts.get(16..) {
            for account in accounts {
                builder.rest_accounts.values().append_value(account);
            }
            builder.rest_accounts.append(true);
        } else {
            builder.rest_accounts.append_null();
        }

        let accounts_size = row.accounts.iter().map(|val| val.len() as u64).sum();
        builder.accounts_size.append_value(accounts_size);

        // meta
        let compute_units_consumed = row.compute_units_consumed.as_ref().map(|val| val.0);
        builder.compute_units_consumed.append_option(compute_units_consumed);
        builder.error.append_option(row.error.as_ref().map(|json| json.to_string()));
        builder.is_committed.append_value(row.is_committed);
        builder.has_dropped_log_messages.append_value(row.has_dropped_log_messages);

        // discriminators
        // todo: check that decoding works as expected
        let data = bs58::decode(&row.data).into_vec().unwrap();
        builder.d1.append_value(format!("{:x?}", data.get(..1).unwrap_or_default()));
        builder.d2.append_value(format!("{:x?}", data.get(..2).unwrap_or_default()));
        builder.d4.append_value(format!("{:x?}", data.get(..4).unwrap_or_default()));
        builder.d8.append_value(format!("{:x?}", data.get(..8).unwrap_or_default()));

        builder.append(true)
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


impl Row for Instruction {
    type Key = (Option<u8>, Vec<u8>, BlockNumber, ItemIndex);

    fn key(&self) -> Self::Key {
        let data = bs58::decode(&self.data).into_vec().unwrap();
        let d1 = data.get(0).copied();
        let program_id = self.program_id.as_bytes().to_vec();
        (d1, program_id, self.block_number, self.transaction_index)
    }
}