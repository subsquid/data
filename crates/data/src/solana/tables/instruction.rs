use crate::solana::model::{AccountIndex, Block, Instruction};
use crate::solana::tables::common::{AccountListBuilder, Base58Builder, InstructionAddressListBuilder};
use anyhow::Context;
use sqd_array::builder::{BooleanBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder};
use sqd_bloom_filter::BloomFilter;
use sqd_data_core::table_builder;


pub const BYTES: usize = 64;
pub const NUM_HASHES: usize = 7;


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
        accounts_bloom: FixedSizeBinaryBuilder = FixedSizeBinaryBuilder::new(64, 0),

        compute_units_consumed: UInt64Builder,
        error: StringBuilder,
        is_committed: BooleanBuilder,
        has_dropped_log_messages: BooleanBuilder,

        d1: UInt8Builder,
        d2: UInt16Builder,
        d4: UInt32Builder,
        d8: UInt64Builder,

        accounts_size: UInt64Builder,
        data_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index", "instruction_address"];
        d.sort_key = vec![
            "d1",
            "program_id",
            "block_number",
            "transaction_index"
        ];
        d.options.add_stats("d1");
        d.options.add_stats("d2");
        d.options.add_stats("d4");
        d.options.add_stats("d8");
        d.options.add_stats("program_id");
        d.options.add_stats("block_number");
        d.options.use_dictionary("program_id");
        d.options.use_dictionary("a0");
        d.options.use_dictionary("a1");
        d.options.use_dictionary("a2");
        d.options.use_dictionary("a3");
        d.options.use_dictionary("a4");
        d.options.use_dictionary("a5");
        d.options.use_dictionary("a6");
        d.options.use_dictionary("a8");
        d.options.use_dictionary("a9");
        d.options.use_dictionary("a10");
        d.options.use_dictionary("a11");
        d.options.use_dictionary("a12");
        d.options.use_dictionary("a13");
        d.options.use_dictionary("a14");
        d.options.use_dictionary("a15");
        d.options.use_dictionary("rest_accounts.list.element");
        d.options.row_group_size = 20_000;
    }
}


impl InstructionBuilder {
    pub fn push(&mut self, block: &Block, row: &Instruction) -> anyhow::Result<()> {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);

        for address in &row.instruction_address {
            self.instruction_address.values().append(*address);
        }
        self.instruction_address.append();

        self.program_id.append(block.get_account(row.program_id)?);
        self.data.append(&row.data);
        self.data_size.append(row.data.len() as u64);
        self.a0.append_option(row.accounts.first().map(|i| block.get_account(*i)).transpose()?);
        self.a1.append_option(row.accounts.get(1).map(|i| block.get_account(*i)).transpose()?);
        self.a2.append_option(row.accounts.get(2).map(|i| block.get_account(*i)).transpose()?);
        self.a3.append_option(row.accounts.get(3).map(|i| block.get_account(*i)).transpose()?);
        self.a4.append_option(row.accounts.get(4).map(|i| block.get_account(*i)).transpose()?);
        self.a5.append_option(row.accounts.get(5).map(|i| block.get_account(*i)).transpose()?);
        self.a6.append_option(row.accounts.get(6).map(|i| block.get_account(*i)).transpose()?);
        self.a7.append_option(row.accounts.get(7).map(|i| block.get_account(*i)).transpose()?);
        self.a8.append_option(row.accounts.get(8).map(|i| block.get_account(*i)).transpose()?);
        self.a9.append_option(row.accounts.get(9).map(|i| block.get_account(*i)).transpose()?);
        self.a10.append_option(row.accounts.get(10).map(|i| block.get_account(*i)).transpose()?);
        self.a11.append_option(row.accounts.get(11).map(|i| block.get_account(*i)).transpose()?);
        self.a12.append_option(row.accounts.get(12).map(|i| block.get_account(*i)).transpose()?);
        self.a13.append_option(row.accounts.get(13).map(|i| block.get_account(*i)).transpose()?);
        self.a14.append_option(row.accounts.get(14).map(|i| block.get_account(*i)).transpose()?);
        self.a15.append_option(row.accounts.get(15).map(|i| block.get_account(*i)).transpose()?);

        if let Some(accounts) = row.accounts.get(16..) {
            for account in accounts {
                self.rest_accounts.values().append(block.get_account(*account)?);
            }
            self.rest_accounts.append();
        } else {
            self.rest_accounts.append_null();
        }

        self.accounts_size.append(row.accounts.len() as u64 * 44);
        self.append_accounts_bloom(block, &row.accounts)?;

        // meta
        self.compute_units_consumed.append_option(row.compute_units_consumed);
        {
            let err = row.error.as_ref().map(|json| json.to_string());
            let err = err.as_deref();
            self.error.append_option(err);
        }
        self.is_committed.append(row.is_committed);
        self.has_dropped_log_messages.append(row.has_dropped_log_messages);

        // discriminators
        let data = bs58::decode(&row.data).into_vec().context("failed to decode instruction data")?;
        self.d1.append_option(
            data.get(..1)
                .map(|slice| u8::from_be_bytes(slice.try_into().unwrap()))
        );
        self.d2.append_option(
            data.get(..2)
                .map(|slice| u16::from_be_bytes(slice.try_into().unwrap()))
        );
        self.d4.append_option(
            data.get(..4)
                .map(|slice| u32::from_be_bytes(slice.try_into().unwrap()))
        );
        self.d8.append_option(
            data.get(..8)
                .map(|slice| u64::from_be_bytes(slice.try_into().unwrap()))
        );
        
        Ok(())
    }

    fn append_accounts_bloom(&mut self, block: &Block, accounts: &[AccountIndex]) -> anyhow::Result<()> {
        if accounts.len() > 0 {
            let mut bloom = BloomFilter::<BYTES>::new(NUM_HASHES);
            for account in accounts {
                bloom.insert(&block.get_account(*account)?);
            }
            let byte_array = bloom.to_byte_array();
            self.accounts_bloom.append(&byte_array);
        } else {
            self.accounts_bloom.append_null();
        }

        Ok(())
    }
}
