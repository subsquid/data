use arrow::array::{ArrayRef, Int16Builder, ListBuilder, UInt32Builder, UInt64Builder, UInt8Builder, BooleanBuilder};

use crate::array_builder::*;
use crate::downcast::Downcast;
use crate::primitives::{BlockNumber, ItemIndex};
use crate::row::Row;
use crate::row_processor::RowProcessor;
use crate::solana::model::{Transaction, TransactionVersion};
use crate::solana::tables::common::{AccountIndexList, AccountListBuilder, Base58Builder, SignatureListBuilder, JsonBuilder, AddressListBuilder};


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


struct_builder! {
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
}


#[derive(Default)]
pub struct TransactionProcessor {
    downcast: Downcast
}


impl RowProcessor for TransactionProcessor {
    type Row = Transaction;
    type Builder = TransactionBuilder;

    fn map(&mut self, builder: &mut Self::Builder, row: &Self::Row) {
        builder.block_number.append_value(row.block_number);
        builder.transaction_index.append_value(row.transaction_index);

        builder.version.append_value(match row.version {
            TransactionVersion::Legacy => -1,
            TransactionVersion::Other(v) => v as i16
        });

        for account in row.account_keys.iter() {
            builder.account_keys.values().append_value(account)
        }
        builder.account_keys.append(true);

        for lookup in row.address_table_lookups.iter() {
            let item = builder.address_table_lookups.values();
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
        builder.address_table_lookups.append(true);

        builder.num_readonly_signed_accounts.append_value(row.num_readonly_signed_accounts);
        builder.num_readonly_unsigned_accounts.append_value(row.num_readonly_unsigned_accounts);
        builder.num_required_signatures.append_value(row.num_required_signatures);
        builder.recent_block_hash.append_value(&row.recent_blockhash);

        for signature in &row.signatures {
            builder.signatures.values().append_value(signature);
        }
        builder.signatures.append(true);

        builder.err.append_option(row.err.as_ref().map(|val| serde_json::to_string(&val).unwrap()));
        builder.compute_units_consumed.append_option(row.compute_units_consumed.as_ref().map(|val| val.parse::<u64>().unwrap()));
        builder.fee.append_value(row.fee.parse().unwrap());

        for address in &row.loaded_addresses.readonly {
            builder.loaded_addresses.readonly.values().append_value(address);
        }
        builder.loaded_addresses.readonly.append(true);
        for address in &row.loaded_addresses.writable {
            builder.loaded_addresses.writable.values().append_value(address);
        }
        builder.loaded_addresses.writable.append(true);
        builder.loaded_addresses.append(true);

        builder.has_dropped_log_messages.append_value(row.has_dropped_log_messages);
        builder.fee_payer.append_value(&row.account_keys[0]);

        let account_keys_size = row.account_keys.iter().map(|val| val.len() as u64).sum();
        builder.account_keys_size.append_value(account_keys_size);

        let address_table_lookups_size = row.address_table_lookups.iter()
            .map(|l| l.account_key.len() as u64 + l.readonly_indexes.len() as u64 + l.writable_indexes.len() as u64)
            .sum();
        builder.address_table_lookups_size.append_value(address_table_lookups_size);

        let signatures_size = row.signatures.iter().map(|val| val.len() as u64).sum();
        builder.signatures_size.append_value(signatures_size);

        let readonly_size: u64 = row.loaded_addresses.readonly.iter().map(|val| val.len() as u64).sum();
        let writable_size: u64 = row.loaded_addresses.writable.iter().map(|val| val.len() as u64).sum();
        let loaded_addresses_size = readonly_size + writable_size;
        builder.loaded_addresses_size.append_value(loaded_addresses_size);

        builder.append(true)
    }

    fn pre(&mut self, row: &Self::Row) {
        self.downcast.block_number.reg(row.block_number);
        self.downcast.item.reg(row.transaction_index)
    }

    fn post(&mut self, array: ArrayRef) -> ArrayRef {
        let array = self.downcast.block_number.downcast_columns(array, &["block_number"]);
        self.downcast.item.downcast_columns(array, &["transaction_index"])
    }
}


impl Row for Transaction {
    type Key = (Vec<u8>, BlockNumber, ItemIndex);

    fn key(&self) -> Self::Key {
        let fee_payer = self.account_keys[0].as_bytes().to_vec();
        (fee_payer, self.block_number, self.transaction_index)
    }
}

