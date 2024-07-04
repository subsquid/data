use arrow::array::{BooleanBuilder, Int16Builder, ListBuilder, UInt32Builder, UInt64Builder, UInt8Builder};

use sqd_primitives::BlockNumber;

use crate::{struct_builder, table_builder};
use crate::solana::model::{Transaction, TransactionVersion};
use crate::solana::tables::common::{AccountIndexList, AccountListBuilder, AddressListBuilder, Base58Builder, JsonBuilder, SignatureListBuilder};


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
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["fee_payer", "block_number", "transaction_index"];
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block_number: BlockNumber, row: &Transaction) {
        self.block_number.append_value(block_number);
        self.transaction_index.append_value(row.transaction_index);

        self.version.append_value(match row.version {
            TransactionVersion::Legacy => -1,
            TransactionVersion::Other(v) => v as i16
        });

        for account in row.account_keys.iter() {
            self.account_keys.values().append_value(account)
        }
        self.account_keys.append(true);

        for lookup in row.address_table_lookups.iter() {
            let item = self.address_table_lookups.values();
            item.account_key.append_value(&lookup.account_key);
            for account_index in lookup.readonly_indexes.iter() {
                item.readonly_indexes.values().append_value(*account_index);
            }
            item.readonly_indexes.append(true);
            for account_index in lookup.writable_indexes.iter() {
                item.writable_indexes.values().append_value(*account_index)
            }
            item.writable_indexes.append(true);
        }
        self.address_table_lookups.append(true);

        self.num_readonly_signed_accounts.append_value(row.num_readonly_signed_accounts);
        self.num_readonly_unsigned_accounts.append_value(row.num_readonly_unsigned_accounts);
        self.num_required_signatures.append_value(row.num_required_signatures);
        self.recent_block_hash.append_value(&row.recent_blockhash);

        for signature in &row.signatures {
            self.signatures.values().append_value(signature);
        }
        self.signatures.append(true);

        self.err.append_option(row.err.as_ref().map(|val| serde_json::to_string(&val).unwrap()));
        self.compute_units_consumed.append_option(row.compute_units_consumed);
        self.fee.append_value(row.fee);

        for address in &row.loaded_addresses.readonly {
            self.loaded_addresses.readonly.values().append_value(address);
        }
        self.loaded_addresses.readonly.append(true);
        for address in &row.loaded_addresses.writable {
            self.loaded_addresses.writable.values().append_value(address);
        }
        self.loaded_addresses.writable.append(true);
        self.loaded_addresses.append(true);

        self.has_dropped_log_messages.append_value(row.has_dropped_log_messages);
        self.fee_payer.append_value(&row.account_keys[0]);

        let account_keys_size = row.account_keys.iter().map(|val| val.len() as u64).sum();
        self.account_keys_size.append_value(account_keys_size);

        let address_table_lookups_size = row.address_table_lookups.iter()
            .map(|l| l.account_key.len() as u64 + l.readonly_indexes.len() as u64 + l.writable_indexes.len() as u64)
            .sum();
        self.address_table_lookups_size.append_value(address_table_lookups_size);

        let signatures_size = row.signatures.iter().map(|val| val.len() as u64).sum();
        self.signatures_size.append_value(signatures_size);

        let readonly_size: u64 = row.loaded_addresses.readonly.iter().map(|val| val.len() as u64).sum();
        let writable_size: u64 = row.loaded_addresses.writable.iter().map(|val| val.len() as u64).sum();
        let loaded_addresses_size = readonly_size + writable_size;
        self.loaded_addresses_size.append_value(loaded_addresses_size);
    }
}
