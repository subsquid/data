use crate::evm::model::{Block, Transaction};
use crate::evm::tables::common::*;
use sqd_array::builder::{UInt64Builder, UInt8Builder};
use sqd_data_core::table_builder;

use super::common::HexBytesBuilder;

// type EIP7702AuthorizationListBuilder = ListBuilder<EIP7702AuthorizationBuilder>;
// struct_builder! {
//     EIP7702AuthorizationBuilder {
//         chain_id: UInt64Builder,
//         address: HexBytesBuilder,
//         nonce: UInt64Builder,
//         y_parity: UInt8Builder,
//         r: HexBytesBuilder,
//         s: HexBytesBuilder,
//     }
// }

table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt64Builder,
        hash: HexBytesBuilder,
        nonce: UInt64Builder,
        from: HexBytesBuilder,
        to: HexBytesBuilder,
        input: HexBytesBuilder,
        value: HexBytesBuilder,
        r#type: UInt64Builder,
        gas: HexBytesBuilder,
        gas_price: HexBytesBuilder,
        max_fee_per_gas: HexBytesBuilder,
        max_priority_fee_per_gas: HexBytesBuilder,
        v: HexBytesBuilder,
        r: HexBytesBuilder,
        s: HexBytesBuilder,
        y_parity: UInt8Builder,
        chain_id: UInt64Builder,
        max_fee_per_blob_gas: HexBytesBuilder,
        blob_versioned_hashes: BlobHashesListBuilder,
        // authorization_list: EIP7702AuthorizationListBuilder,

        contract_address: HexBytesBuilder,
        cumulative_gas_used: HexBytesBuilder,
        effective_gas_price: HexBytesBuilder,
        gas_used: HexBytesBuilder,
        sighash: HexBytesBuilder,
        status: UInt8Builder,

        l1_base_fee_scalar: UInt64Builder,
        l1_blob_base_fee: HexBytesBuilder,
        l1_blob_base_fee_scalar: UInt64Builder,
        l1_fee: HexBytesBuilder,
        l1_fee_scalar: UInt64Builder,
        l1_gas_price: HexBytesBuilder,
        l1_gas_used: HexBytesBuilder,

        input_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["block_number", "transaction_index"];
        d.options.add_stats("block_number");
        d.options.row_group_size = 5_000;
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, row: &Transaction) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);
        self.hash.append(&row.hash);
        self.nonce.append(row.nonce);
        self.from.append(&row.from);
        self.to.append_option(row.to.as_deref());
        self.input.append(&row.input);
        self.value.append(&row.value);
        self.r#type.append_option(row.r#type);
        self.gas.append(&row.gas);
        self.gas_price.append_option(row.gas_price.as_deref());
        self.max_fee_per_gas.append_option(row.max_fee_per_gas.as_deref());
        self.max_priority_fee_per_gas.append_option(row.max_priority_fee_per_gas.as_deref());
        self.v.append_option(row.v.as_deref());
        self.r.append_option(row.r.as_deref());
        self.s.append_option(row.s.as_deref());
        self.y_parity.append_option(row.y_parity);
        self.chain_id.append_option(row.chain_id);
        self.max_fee_per_blob_gas.append_option(row.max_fee_per_blob_gas.as_deref());


        for blob_hash in row.blob_versioned_hashes.iter().flatten() {
            self.blob_versioned_hashes.values().append(blob_hash);
        }

        self.blob_versioned_hashes.append();
        

        // for auth in row.authorization_list.iter().flatten() {
        //     let item = self.authorization_list.values();
        //     item.chain_id.append(auth.chain_id);
        //     item.address.append(&auth.address);
        //     item.nonce.append(auth.nonce);
        //     item.y_parity.append(auth.y_parity);
        //     item.r.append(&auth.r);
        //     item.s.append(&auth.s);
        // }

        // self.authorization_list.append();

        self.contract_address.append_option(row.contract_address.as_deref());
        self.cumulative_gas_used.append(&row.cumulative_gas_used);
        self.effective_gas_price.append(&row.effective_gas_price);
        self.gas_used.append(&row.gas_used);
        self.sighash.append_option(row.sighash.as_deref());
        self.status.append_option(row.status);

        self.l1_base_fee_scalar.append_option(row.l1_base_fee_scalar);
        self.l1_blob_base_fee.append_option(row.l1_blob_base_fee.as_deref());
        self.l1_blob_base_fee_scalar.append_option(row.l1_blob_base_fee_scalar);
        self.l1_fee.append_option(row.l1_fee.as_deref());
        self.l1_fee_scalar.append_option(row.l1_fee_scalar);
        self.l1_gas_price.append_option(row.l1_gas_price.as_deref());
        self.l1_gas_used.append_option(row.l1_gas_used.as_deref());

        self.input_size.append(row.input.len() as u64);
    }

}
