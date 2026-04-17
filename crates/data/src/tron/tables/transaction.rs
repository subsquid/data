use crate::tron::model::{Block, Transaction};
use crate::tron::tables::common::*;
use sqd_array::builder::{Int32Builder, Int64Builder, ListBuilder, StringBuilder, TimestampMillisecondBuilder, UInt32Builder, UInt64Builder};
use sqd_data_core::table_builder;


type SignatureListBuilder = ListBuilder<HexBytesBuilder>;


table_builder! {
    TransactionBuilder {
        block_number: UInt64Builder,
        transaction_index: UInt32Builder,
        hash: HexBytesBuilder,
        ret: JsonBuilder,
        signature: SignatureListBuilder,
        r#type: StringBuilder,
        parameter: JsonBuilder,
        permission_id: Int32Builder,
        ref_block_bytes: HexBytesBuilder,
        ref_block_hash: HexBytesBuilder,
        fee_limit: Int64Builder,
        expiration: TimestampMillisecondBuilder,
        timestamp: Int64Builder,
        raw_data_hex: HexBytesBuilder,

        // info
        fee: Int64Builder,
        contract_result: HexBytesBuilder,
        contract_address: HexBytesBuilder,
        res_message: HexBytesBuilder,
        withdraw_amount: Int64Builder,
        unfreeze_amount: Int64Builder,
        withdraw_expire_amount: Int64Builder,
        cancel_unfreeze_v2_amount: JsonBuilder,

        // receipt
        result: StringBuilder,
        energy_fee: Int64Builder,
        energy_usage: Int64Builder,
        energy_usage_total: Int64Builder,
        net_usage: Int64Builder,
        net_fee: Int64Builder,
        origin_energy_usage: Int64Builder,
        energy_penalty_total: Int64Builder,

        // TransferContract
        _transfer_contract_owner: HexBytesBuilder,
        _transfer_contract_to: HexBytesBuilder,

        // TransferAssetContract
        _transfer_asset_contract_owner: HexBytesBuilder,
        _transfer_asset_contract_to: HexBytesBuilder,
        _transfer_asset_contract_asset: StringBuilder,

        // TriggerSmartContract
        _trigger_smart_contract_owner: HexBytesBuilder,
        _trigger_smart_contract_contract: HexBytesBuilder,
        _trigger_smart_contract_sighash: HexBytesBuilder,

        raw_data_hex_size: UInt64Builder,
    }

    description(d) {
        d.downcast.block_number = vec!["block_number"];
        d.downcast.item_index = vec!["transaction_index"];
        d.sort_key = vec![
            "type",
            "_trigger_smart_contract_sighash",
            "_trigger_smart_contract_contract",
            "_trigger_smart_contract_owner",
            "_transfer_contract_owner",
            "_transfer_contract_to",
            "_transfer_asset_contract_owner",
            "_transfer_asset_contract_to",
            "_transfer_asset_contract_asset",
            "block_number",
            "transaction_index",
        ];
        d.options.add_stats("block_number");
        d.options.add_stats("transaction_index");
        d.options.add_stats("type");
        d.options.add_stats("_transfer_contract_owner");
        d.options.add_stats("_transfer_contract_to");
        d.options.add_stats("_transfer_asset_contract_owner");
        d.options.add_stats("_transfer_asset_contract_to");
        d.options.add_stats("_transfer_asset_contract_asset");
        d.options.add_stats("_trigger_smart_contract_owner");
        d.options.add_stats("_trigger_smart_contract_contract");
        d.options.add_stats("_trigger_smart_contract_sighash");
        d.options.use_dictionary("type");
        d.options.use_dictionary("ret");
        d.options.row_group_size = 10_000;
    }
}


impl TransactionBuilder {
    pub fn push(&mut self, block: &Block, row: &Transaction) {
        self.block_number.append(block.header.height);
        self.transaction_index.append(row.transaction_index);
        self.hash.append(&row.hash);

        let ret = row.ret.as_ref().map(|val| serde_json::to_string(val).unwrap());
        self.ret.append_option(ret.as_deref());

        for sig in row.signature.iter().flatten() {
            self.signature.values().append(sig);
        }
        self.signature.append();

        self.r#type.append(&row.r#type);

        let parameter = serde_json::to_string(&row.parameter).unwrap();
        self.parameter.append(&parameter);

        self.permission_id.append_option(row.permission_id);
        self.ref_block_bytes.append_option(row.ref_block_bytes.as_deref());
        self.ref_block_hash.append_option(row.ref_block_hash.as_deref());
        self.fee_limit.append_option(row.fee_limit.map(|v| v as i64));
        self.expiration.append_option(row.expiration);
        self.timestamp.append_option(row.timestamp);
        self.raw_data_hex.append(&row.raw_data_hex);

        self.fee.append_option(row.fee.map(|v| v as i64));
        self.contract_result.append_option(row.contract_result.as_deref());
        self.contract_address.append_option(row.contract_address.as_deref());
        self.res_message.append_option(row.res_message.as_deref());
        self.withdraw_amount.append_option(row.withdraw_amount.map(|v| v as i64));
        self.unfreeze_amount.append_option(row.unfreeze_amount.map(|v| v as i64));
        self.withdraw_expire_amount.append_option(row.withdraw_expire_amount.map(|v| v as i64));

        let cancel = row.cancel_unfreeze_v2_amount.as_ref().map(|val| serde_json::to_string(val).unwrap());
        self.cancel_unfreeze_v2_amount.append_option(cancel.as_deref());

        self.result.append_option(row.result.as_deref());
        self.energy_fee.append_option(row.energy_fee.map(|v| v as i64));
        self.energy_usage.append_option(row.energy_usage.map(|v| v as i64));
        self.energy_usage_total.append_option(row.energy_usage_total.map(|v| v as i64));
        self.net_usage.append_option(row.net_usage.map(|v| v as i64));
        self.net_fee.append_option(row.net_fee.map(|v| v as i64));
        self.origin_energy_usage.append_option(row.origin_energy_usage.map(|v| v as i64));
        self.energy_penalty_total.append_option(row.energy_penalty_total.map(|v| v as i64));

        let value = &row.parameter["value"];

        if row.r#type == "TransferContract" {
            self._transfer_contract_owner.append_option(value["owner_address"].as_str());
            self._transfer_contract_to.append_option(value["to_address"].as_str());
        } else {
            self._transfer_contract_owner.append_null();
            self._transfer_contract_to.append_null();
        }

        if row.r#type == "TransferAssetContract" {
            self._transfer_asset_contract_owner.append_option(value["owner_address"].as_str());
            self._transfer_asset_contract_to.append_option(value["to_address"].as_str());
            self._transfer_asset_contract_asset.append_option(value["asset_name"].as_str());
        } else {
            self._transfer_asset_contract_owner.append_null();
            self._transfer_asset_contract_to.append_null();
            self._transfer_asset_contract_asset.append_null();
        }

        if row.r#type == "TriggerSmartContract" {
            self._trigger_smart_contract_owner.append_option(value["owner_address"].as_str());
            self._trigger_smart_contract_contract.append_option(value["contract_address"].as_str());
            self._trigger_smart_contract_sighash.append_option(
                value["data"].as_str().and_then(sighash)
            );
        } else {
            self._trigger_smart_contract_owner.append_null();
            self._trigger_smart_contract_contract.append_null();
            self._trigger_smart_contract_sighash.append_null();
        }

        self.raw_data_hex_size.append(row.raw_data_hex.len() as u64);
    }
}
