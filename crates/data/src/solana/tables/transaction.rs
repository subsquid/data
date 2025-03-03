use crate::solana::model::{Block, Transaction, TransactionVersion};
use crate::solana::tables::common::{AccountIndexList, AccountListBuilder, AddressListBuilder, Base58Builder, JsonBuilder, SignatureListBuilder};
use sqd_array::builder::{BooleanBuilder, FixedSizeBinaryBuilder, Int16Builder, ListBuilder, UInt32Builder, UInt64Builder, UInt8Builder};
use sqd_bloom_filter::BloomFilter;
use sqd_data_core::{struct_builder, table_builder};


type AddressTableLookupListBuilder = ListBuilder<AddressTableLookupBuilder>;
struct_builder! {
    AddressTableLookupBuilder {
        account_key: Base58Builder,
        readonly_indexes: AccountIndexList,
        writable_indexes: AccountIndexList,
    }
}


struct_builder! {
    LoadedAddressBuilder {
        readonly: AddressListBuilder,
        writable: AddressListBuilder,
    }
}


const ACCOUNT_BLOOM_BYTES: usize = 64;
const ACCOUNT_BLOOM_NUM_HASHES: usize = 7;


table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        version: Int16Builder,
        account_keys: AccountListBuilder,
        address_table_lookups: AddressTableLookupListBuilder,
        num_readonly_signed_accounts: UInt8Builder,
        num_readonly_unsigned_accounts: UInt8Builder,
        num_required_signatures: UInt8Builder,
        recent_block_hash: Base58Builder,
        signatures: SignatureListBuilder,
        err: JsonBuilder,
        compute_units_consumed: UInt64Builder,
        fee: UInt64Builder,
        loaded_addresses: LoadedAddressBuilder,
        has_dropped_log_messages: BooleanBuilder,
        fee_payer: Base58Builder,
        account_keys_size: UInt64Builder,
        address_table_lookups_size: UInt64Builder,
        signatures_size: UInt64Builder,
        loaded_addresses_size: UInt64Builder,
        accounts_bloom: FixedSizeBinaryBuilder = FixedSizeBinaryBuilder::new(ACCOUNT_BLOOM_BYTES, 0),
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["fee_payer", "block_number", "transaction_index"];
        d.options.add_stats("fee_payer");
        d.options.add_stats("block_number");
        d.options.use_dictionary("account_keys.list.element");
        d.options.use_dictionary("address_table_lookups.list.element.account_key");
        d.options.use_dictionary("loaded_addresses.readonly.list.element");
        d.options.use_dictionary("loaded_addresses.writable.list.element");
        d.options.use_dictionary("fee_payer");
        d.options.row_group_size = 5_000;
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, row: &Transaction) -> anyhow::Result<()> {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);

        self.version.append(match row.version {
            TransactionVersion::Legacy => -1,
            TransactionVersion::Other(v) => v as i16
        });

        for account in row.account_keys.iter().copied() {
            self.account_keys.values().append(
                block.get_account(account)?
            )
        }
        self.account_keys.append();

        for lookup in row.address_table_lookups.iter() {
            let item = self.address_table_lookups.values();
            item.account_key.append(block.get_account(lookup.account_key)?);
            for account_index in lookup.readonly_indexes.iter() {
                item.readonly_indexes.values().append(*account_index);
            }
            item.readonly_indexes.append();
            for account_index in lookup.writable_indexes.iter() {
                item.writable_indexes.values().append(*account_index)
            }
            item.writable_indexes.append();
            item.append(true);
        }
        self.address_table_lookups.append();

        self.num_readonly_signed_accounts.append(row.num_readonly_signed_accounts);
        self.num_readonly_unsigned_accounts.append(row.num_readonly_unsigned_accounts);
        self.num_required_signatures.append(row.num_required_signatures);
        self.recent_block_hash.append(&row.recent_blockhash);

        for signature in &row.signatures {
            self.signatures.values().append(signature);
        }
        self.signatures.append();

        {
            let err = row.err.as_ref().map(|val| val.to_string());
            self.err.append_option(err.as_ref().map(|s| s.as_ref()));
        }
       
        self.compute_units_consumed.append_option(row.compute_units_consumed);
        self.fee.append(row.fee);

        for address in &row.loaded_addresses.readonly {
            self.loaded_addresses.readonly.values().append(
                block.get_account(*address)?
            );
        }
        self.loaded_addresses.readonly.append();
        for address in &row.loaded_addresses.writable {
            self.loaded_addresses.writable.values().append(
                block.get_account(*address)?
            );
        }
        self.loaded_addresses.writable.append();
        self.loaded_addresses.append(true);

        self.has_dropped_log_messages.append(row.has_dropped_log_messages);
        self.fee_payer.append(block.get_account(row.account_keys[0])?);

        let account_keys_size = row.account_keys.len() * 44;
        self.account_keys_size.append(account_keys_size as u64);

        let address_table_lookups_size = 44usize * row.address_table_lookups.iter()
            .map(|l| 1 + l.readonly_indexes.len() + l.writable_indexes.len())
            .sum::<usize>();
        self.address_table_lookups_size.append(address_table_lookups_size as u64);

        let signatures_size = row.signatures.iter().map(|val| val.len() as u64).sum();
        self.signatures_size.append(signatures_size);

        let readonly_size = row.loaded_addresses.readonly.len() * 44;
        let writable_size = row.loaded_addresses.writable.len() * 44;
        let loaded_addresses_size = readonly_size + writable_size;
        self.loaded_addresses_size.append(loaded_addresses_size as u64);

        self.append_accounts_bloom(block, row)
    }

    fn append_accounts_bloom(&mut self, block: &Block, row: &Transaction) -> anyhow::Result<()> {
        let mut bloom = BloomFilter::new(ACCOUNT_BLOOM_BYTES, ACCOUNT_BLOOM_NUM_HASHES);
        for i in row.account_keys.iter() {
            bloom.insert(block.get_account(*i)?);
        }
        for i in row.loaded_addresses.readonly.iter() {
            bloom.insert(block.get_account(*i)?);
        }
        for i in row.loaded_addresses.writable.iter() {
            bloom.insert(block.get_account(*i)?);
        }
        self.accounts_bloom.append(bloom.bytes());
        Ok(())
    }
}
