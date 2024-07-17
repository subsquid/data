use std::fmt::Write;
use arrow::array::{BooleanBuilder, StringBuilder, UInt32Builder, UInt64Builder};

use sqd_primitives::BlockNumber;

use crate::solana::model::Instruction;
use crate::solana::tables::common::{AccountListBuilder, Base58Builder, BytesBuilder, InstructionAddressListBuilder};
use crate::table_builder;


table_builder! {
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

        compute_units_consumed: UInt64Builder,
        error: StringBuilder,
        is_committed: BooleanBuilder,
        has_dropped_log_messages: BooleanBuilder,

        d1: BytesBuilder,
        d2: BytesBuilder,
        d4: BytesBuilder,
        d8: BytesBuilder,

        accounts_size: UInt64Builder,
        data_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "instruction_address"]
    }
}


impl InstructionBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Instruction) {
        self.block_number.append_value(block_number);
        self.transaction_index.append_value(row.transaction_index);

        for address in &row.instruction_address {
            self.instruction_address.values().append_value(*address);
        }
        self.instruction_address.append(true);

        self.program_id.append_value(&row.program_id);
        self.data.append_value(&row.data);
        self.data_size.append_value(row.data.len() as u64);
        self.a0.append_option(row.accounts.get(0));
        self.a1.append_option(row.accounts.get(1));
        self.a2.append_option(row.accounts.get(2));
        self.a3.append_option(row.accounts.get(3));
        self.a4.append_option(row.accounts.get(4));
        self.a5.append_option(row.accounts.get(5));
        self.a6.append_option(row.accounts.get(6));
        self.a7.append_option(row.accounts.get(7));
        self.a8.append_option(row.accounts.get(8));
        self.a9.append_option(row.accounts.get(9));
        self.a10.append_option(row.accounts.get(10));
        self.a11.append_option(row.accounts.get(11));
        self.a12.append_option(row.accounts.get(12));
        self.a13.append_option(row.accounts.get(13));
        self.a14.append_option(row.accounts.get(14));
        self.a15.append_option(row.accounts.get(15));

        if let Some(accounts) = row.accounts.get(16..) {
            for account in accounts {
                self.rest_accounts.values().append_value(account);
            }
            self.rest_accounts.append(true);
        } else {
            self.rest_accounts.append_null();
        }

        let accounts_size = row.accounts.iter().map(|val| val.len() as u64).sum();
        self.accounts_size.append_value(accounts_size);

        // meta
        self.compute_units_consumed.append_option(row.compute_units_consumed);
        self.error.append_option(row.error.as_ref().map(|json| json.to_string()));
        self.is_committed.append_value(row.is_committed);
        self.has_dropped_log_messages.append_value(row.has_dropped_log_messages);

        // discriminators
        let data = bs58::decode(&row.data).into_vec().unwrap();
        write_hex(&mut self.d1, data.get(..1).unwrap_or_default());
        write_hex(&mut self.d2, data.get(..2).unwrap_or_default());
        write_hex(&mut self.d4, data.get(..4).unwrap_or_default());
        write_hex(&mut self.d8, data.get(..8).unwrap_or_default());
    }
}


fn write_hex(builder: &mut BytesBuilder, bytes: &[u8]) {
    write!(builder, "0x").unwrap();
    for b in bytes.iter().copied() {
        write!(builder, "{:02x}", b).unwrap();
    }
    builder.append_value("")
}