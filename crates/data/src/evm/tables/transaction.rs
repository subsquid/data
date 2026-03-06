use crate::evm::model::{Block, Transaction, TempoPrimitiveSignature, TempoSignature, TempoKeychainSignature};
use crate::evm::tables::common::*;
use sqd_array::builder::{BooleanBuilder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder, UInt8Builder, Float64Builder};
use sqd_data_core::{struct_builder, table_builder};

use super::common::HexBytesBuilder;


type EIP7702AuthorizationListBuilder = ListBuilder<EIP7702AuthorizationBuilder>;
struct_builder! {
    EIP7702AuthorizationBuilder {
        chain_id: HexBytesBuilder,
        address: HexBytesBuilder,
        nonce: UInt64Builder,
        y_parity: UInt8Builder,
        r: HexBytesBuilder,
        s: HexBytesBuilder,
    }
}

type TempoCallsListBuilder = ListBuilder<TempoCallBuilder>;
struct_builder! {
    TempoCallBuilder {
        to: HexBytesBuilder,
        value: HexBytesBuilder,
        input: HexBytesBuilder,
    }
}

// Flattened TempoPrimitiveSignature union — all variant fields are nullable,
// `sig_type` discriminates which fields are populated.
struct_builder! {
    TempoPrimSigBuilder {
        sig_type: StringBuilder,
        r: HexBytesBuilder,
        s: HexBytesBuilder,
        y_parity: UInt8Builder,
        v: UInt8Builder,
        pub_key_x: HexBytesBuilder,
        pub_key_y: HexBytesBuilder,
        pre_hash: BooleanBuilder,
        webauthn_data: HexBytesBuilder,
    }
}

// TempoSignature = TempoPrimitiveSignature | TempoKeychainSignature.
// `user_address` being non-null indicates a TempoKeychainSignature wrapper.
struct_builder! {
    TempoSigBuilder {
        primitive: TempoPrimSigBuilder,
        user_address: HexBytesBuilder,
        version: StringBuilder,
    }
}

type TempoAAAuthListBuilder = ListBuilder<TempoSignedAuthBuilder>;
struct_builder! {
    TempoSignedAuthBuilder {
        chain_id: HexBytesBuilder,
        address: HexBytesBuilder,
        nonce: UInt64Builder,
        signature: TempoSigBuilder,
    }
}

type TempoTokenLimitListBuilder = ListBuilder<TempoTokenLimitBuilder>;
struct_builder! {
    TempoTokenLimitBuilder {
        token: HexBytesBuilder,
        limit: HexBytesBuilder,
    }
}

struct_builder! {
    TempoKeyAuthBuilder {
        chain_id: HexBytesBuilder,
        key_type: StringBuilder,
        key_id: HexBytesBuilder,
        expiry: HexBytesBuilder,
        limits: TempoTokenLimitListBuilder,
        signature: TempoPrimSigBuilder,
    }
}


table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
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
        authorization_list: EIP7702AuthorizationListBuilder,

        // Tempo 0x76 transaction fields
        calls: TempoCallsListBuilder,
        nonce_key: HexBytesBuilder,
        signature: TempoSigBuilder,
        fee_token: HexBytesBuilder,
        fee_payer_v: UInt8Builder,
        fee_payer_r: HexBytesBuilder,
        fee_payer_s: HexBytesBuilder,
        valid_before: HexBytesBuilder,
        valid_after: HexBytesBuilder,
        aa_authorization_list: TempoAAAuthListBuilder,
        key_authorization: TempoKeyAuthBuilder,

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
        l1_fee_scalar: Float64Builder,
        l1_gas_price: HexBytesBuilder,
        l1_gas_used: HexBytesBuilder,

        input_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec!["sighash", "to", "block_number", "transaction_index"];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("to");
        d.options.add_stats("from");
        d.options.add_stats("sighash");
        d.options.use_dictionary("to");
        d.options.use_dictionary("sighash");
        d.options.row_group_size = 10_000;
    }
}


fn push_prim_sig(b: &mut TempoPrimSigBuilder, sig: &TempoPrimitiveSignature) {
    match sig {
        TempoPrimitiveSignature::Secp256k1 { r, s, y_parity, v } => {
            b.sig_type.append("secp256k1");
            b.r.append(r);
            b.s.append(s);
            b.y_parity.append_option(*y_parity);
            b.v.append_option(*v);
            b.pub_key_x.append_null();
            b.pub_key_y.append_null();
            b.pre_hash.append_option(None);
            b.webauthn_data.append_null();
        }
        TempoPrimitiveSignature::P256 { r, s, pub_key_x, pub_key_y, pre_hash } => {
            b.sig_type.append("p256");
            b.r.append(r);
            b.s.append(s);
            b.y_parity.append_option(None);
            b.v.append_option(None);
            b.pub_key_x.append(pub_key_x);
            b.pub_key_y.append(pub_key_y);
            b.pre_hash.append(*pre_hash);
            b.webauthn_data.append_null();
        }
        TempoPrimitiveSignature::WebAuthn { r, s, pub_key_x, pub_key_y, webauthn_data } => {
            b.sig_type.append("webAuthn");
            b.r.append(r);
            b.s.append(s);
            b.y_parity.append_option(None);
            b.v.append_option(None);
            b.pub_key_x.append(pub_key_x);
            b.pub_key_y.append(pub_key_y);
            b.pre_hash.append_option(None);
            b.webauthn_data.append(webauthn_data);
        }
    }
    b.append_valid();
}

fn push_prim_sig_null(b: &mut TempoPrimSigBuilder) {
    b.sig_type.append_null();
    b.r.append_null();
    b.s.append_null();
    b.y_parity.append_option(None);
    b.v.append_option(None);
    b.pub_key_x.append_null();
    b.pub_key_y.append_null();
    b.pre_hash.append_option(None);
    b.webauthn_data.append_null();
    b.append_null();
}

fn push_tempo_sig(b: &mut TempoSigBuilder, sig: &TempoSignature) {
    match sig {
        TempoSignature::Primitive(prim) => {
            push_prim_sig(&mut b.primitive, prim);
            b.user_address.append_null();
            b.version.append_null();
        }
        TempoSignature::Keychain(TempoKeychainSignature { user_address, signature, version }) => {
            push_prim_sig(&mut b.primitive, signature);
            b.user_address.append(user_address);
            b.version.append_option(version.as_deref());
        }
    }
    b.append_valid();
}

fn push_tempo_sig_null(b: &mut TempoSigBuilder) {
    push_prim_sig_null(&mut b.primitive);
    b.user_address.append_null();
    b.version.append_null();
    b.append_null();
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, row: &Transaction) {
        self.block_number.append(block.header.number);
        self.transaction_index.append(row.transaction_index);
        self.hash.append(&row.hash);
        self.nonce.append(row.nonce);
        self.from.append(&row.from);
        self.to.append_option(row.to.as_deref());
        self.input.append_option(row.input.as_deref());
        self.value.append_option(row.value.as_deref());
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

        for auth in row.authorization_list.iter().flatten() {
            let item = self.authorization_list.values();
            item.chain_id.append(&auth.chain_id);
            item.address.append(&auth.address);
            item.nonce.append(auth.nonce);
            item.y_parity.append(auth.y_parity);
            item.r.append(&auth.r);
            item.s.append(&auth.s);
            item.append_valid();
        }
        self.authorization_list.append();

        // Tempo: calls
        for call in row.calls.iter().flatten() {
            let item = self.calls.values();
            item.to.append_option(call.to.as_deref());
            item.value.append(&call.value);
            item.input.append(&call.input);
            item.append_valid();
        }
        self.calls.append();

        self.nonce_key.append_option(row.nonce_key.as_deref());

        // Tempo: signature
        if let Some(ref sig) = row.signature {
            push_tempo_sig(&mut self.signature, sig);
        } else {
            push_tempo_sig_null(&mut self.signature);
        }

        self.fee_token.append_option(row.fee_token.as_deref());

        if let Some(ref sig) = row.fee_payer_signature {
            self.fee_payer_v.append(sig.v);
            self.fee_payer_r.append(&sig.r);
            self.fee_payer_s.append(&sig.s);
        } else {
            self.fee_payer_v.append_option(None);
            self.fee_payer_r.append_null();
            self.fee_payer_s.append_null();
        }

        self.valid_before.append_option(row.valid_before.as_deref());
        self.valid_after.append_option(row.valid_after.as_deref());

        // Tempo: aa_authorization_list
        for auth in row.aa_authorization_list.iter().flatten() {
            let item = self.aa_authorization_list.values();
            item.chain_id.append(&auth.chain_id);
            item.address.append(&auth.address);
            item.nonce.append(auth.nonce);
            push_tempo_sig(&mut item.signature, &auth.signature);
            item.append_valid();
        }
        self.aa_authorization_list.append();

        // Tempo: key_authorization
        if let Some(ref ka) = row.key_authorization {
            let b = &mut self.key_authorization;
            b.chain_id.append(&ka.chain_id);
            b.key_type.append(&ka.key_type);
            b.key_id.append(&ka.key_id);
            b.expiry.append_option(ka.expiry.as_deref());

            for limit in ka.limits.iter().flatten() {
                let item = b.limits.values();
                item.token.append(&limit.token);
                item.limit.append(&limit.limit);
                item.append_valid();
            }
            b.limits.append();

            push_prim_sig(&mut b.signature, &ka.signature);
            b.append_valid();
        } else {
            let b = &mut self.key_authorization;
            b.chain_id.append_null();
            b.key_type.append_null();
            b.key_id.append_null();
            b.expiry.append_null();
            b.limits.append();
            push_prim_sig_null(&mut b.signature);
            b.append_null();
        }

        self.contract_address.append_option(row.contract_address.as_deref());
        self.cumulative_gas_used.append(&row.cumulative_gas_used);
        self.effective_gas_price.append_option(row.effective_gas_price.as_deref());
        self.gas_used.append(&row.gas_used);
        self.sighash.append_option(row.input.as_deref().and_then(sighash));
        self.status.append_option(row.status);

        self.l1_base_fee_scalar.append_option(row.l1_base_fee_scalar);
        self.l1_blob_base_fee.append_option(row.l1_blob_base_fee.as_deref());
        self.l1_blob_base_fee_scalar.append_option(row.l1_blob_base_fee_scalar);
        self.l1_fee.append_option(row.l1_fee.as_deref());
        self.l1_fee_scalar.append_option(row.l1_fee_scalar);
        self.l1_gas_price.append_option(row.l1_gas_price.as_deref());
        self.l1_gas_used.append_option(row.l1_gas_used.as_deref());

        self.input_size.append(row.input.as_ref().map_or(0, |i| i.len() as u64));
    }
}
