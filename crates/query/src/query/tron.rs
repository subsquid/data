use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{Plan, ScanBuilder, TableSet};
use crate::primitives::BlockNumber;
use crate::query::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request, to_lowercase_list, PredicateBuilder};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;


static TABLES: LazyLock<TableSet> = LazyLock::new(|| {
    let mut tables = TableSet::new();

    tables.add_table("blocks", vec![
        "number"
    ]);

    tables.add_table("transactions", vec![
        "block_number",
        "transaction_index"
    ])
    .add_child("logs", vec!["block_number", "transaction_index"])
    .add_child("internal_transactions", vec!["block_number", "transaction_index"])
    .set_weight_column("raw_data_hex", "raw_data_hex_size");

    tables.add_table("logs", vec![
        "block_number",
        "transaction_index",
        "log_index"
    ])
    .set_weight_column("data", "data_size");

    tables.add_table("internal_transactions", vec![
        "block_number",
        "transaction_index",
        "internal_transaction_index"
    ])
    .set_result_item_name("internalTransactions");

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    log: LogFieldSelection,
    internal_transaction: InternalTransactionFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        tx_trie_root,
        version,
        timestamp,
        witness_address,
        witness_signature,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        this.tx_trie_root,
        this.version,
        this.witness_address,
        this.witness_signature,
        [this.timestamp]: TimestampMillisecond,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_index,
        hash,
        ret,
        signature,
        r#type,
        parameter,
        permission_id,
        ref_block_bytes,
        ref_block_hash,
        fee_limit,
        expiration,
        timestamp,
        raw_data_hex,
        fee,
        contract_result,
        contract_address,
        res_message,
        withdraw_amount,
        unfreeze_amount,
        withdraw_expire_amount,
        cancel_unfreeze_v2_amount,
        result,
        energy_fee,
        energy_usage,
        energy_usage_total,
        net_usage,
        net_fee,
        origin_energy_usage,
        energy_penalty_total,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.hash,
        this.signature,
        this.r#type,
        this.permission_id,
        this.ref_block_bytes,
        this.ref_block_hash,
        this.raw_data_hex,
        this.contract_result,
        this.contract_address,
        this.res_message,
        this.result,
        [this.ret]: Json,
        [this.parameter]: Json,
        [this.fee_limit]: BigNum,
        [this.expiration]: TimestampMillisecond,
        [this.timestamp]: TimestampMillisecond,
        [this.fee]: BigNum,
        [this.withdraw_amount]: BigNum,
        [this.unfreeze_amount]: BigNum,
        [this.withdraw_expire_amount]: BigNum,
        [this.cancel_unfreeze_v2_amount]: Json,
        [this.energy_fee]: BigNum,
        [this.energy_usage]: BigNum,
        [this.energy_usage_total]: BigNum,
        [this.net_usage]: BigNum,
        [this.net_fee]: BigNum,
        [this.origin_energy_usage]: BigNum,
        [this.energy_penalty_total]: BigNum,
    }}
}


item_field_selection! {
    LogFieldSelection {
        transaction_index,
        log_index,
        address,
        data,
        topics,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.log_index,
        this.address,
        this.data,
        |obj| {
            if this.topics {
                obj.add("topics", roll(Exp::Value, vec![
                    "topic0",
                    "topic1",
                    "topic2",
                    "topic3",
                ]));
            }
        }
    }}
}


item_field_selection! {
    InternalTransactionFieldSelection {
        transaction_index,
        internal_transaction_index,
        hash,
        caller_address,
        transfer_to_address,
        call_value_info,
        note,
        rejected,
        extra,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.internal_transaction_index,
        this.hash,
        this.caller_address,
        this.transfer_to_address,
        this.note,
        this.rejected,
        this.extra,
        [this.call_value_info]: Json,
    }}
}


type Bytes = String;


request! {
    pub struct TransactionRequest {
        pub r#type: Option<Vec<String>>,
        pub logs: bool,
        pub internal_transactions: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.internal_transactions {
            scan.join(
                "internal_transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct TransferTransactionRequest {
        pub owner: Option<Vec<Bytes>>,
        pub to: Option<Vec<Bytes>>,
        pub logs: bool,
        pub internal_transactions: bool,
    }
}


impl TransferTransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("type", Some("TransferContract"));
        p.col_in_list("_transfer_contract_owner", to_lowercase_list(&self.owner));
        p.col_in_list("_transfer_contract_to", to_lowercase_list(&self.to));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.internal_transactions {
            scan.join(
                "internal_transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct TransferAssetTransactionRequest {
        pub owner: Option<Vec<Bytes>>,
        pub to: Option<Vec<Bytes>>,
        pub asset: Option<Vec<String>>,
        pub logs: bool,
        pub internal_transactions: bool,
    }
}


impl TransferAssetTransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("type", Some("TransferAssetContract"));
        p.col_in_list("_transfer_asset_contract_owner", to_lowercase_list(&self.owner));
        p.col_in_list("_transfer_asset_contract_to", to_lowercase_list(&self.to));
        p.col_in_list("_transfer_asset_contract_asset", self.asset.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.internal_transactions {
            scan.join(
                "internal_transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct TriggerSmartContractTransactionRequest {
        pub owner: Option<Vec<Bytes>>,
        pub contract: Option<Vec<Bytes>>,
        pub sighash: Option<Vec<Bytes>>,
        pub logs: bool,
        pub internal_transactions: bool,
    }
}


impl TriggerSmartContractTransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("type", Some("TriggerSmartContract"));
        p.col_in_list("_trigger_smart_contract_owner", to_lowercase_list(&self.owner));
        p.col_in_list("_trigger_smart_contract_contract", to_lowercase_list(&self.contract));
        p.col_in_list("_trigger_smart_contract_sighash", to_lowercase_list(&self.sighash));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.internal_transactions {
            scan.join(
                "internal_transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct LogRequest {
        pub address: Option<Vec<Bytes>>,
        pub topic0: Option<Vec<Bytes>>,
        pub topic1: Option<Vec<Bytes>>,
        pub topic2: Option<Vec<Bytes>>,
        pub topic3: Option<Vec<Bytes>>,
        pub transaction: bool,
    }
}


impl LogRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("address", to_lowercase_list(&self.address));
        p.col_in_list("topic0", to_lowercase_list(&self.topic0));
        p.col_in_list("topic1", to_lowercase_list(&self.topic1));
        p.col_in_list("topic2", to_lowercase_list(&self.topic2));
        p.col_in_list("topic3", to_lowercase_list(&self.topic3));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct InternalTransactionRequest {
        pub caller: Option<Vec<Bytes>>,
        pub transfer_to: Option<Vec<Bytes>>,
        pub transaction: bool,
    }
}


impl InternalTransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("caller_address", to_lowercase_list(&self.caller));
        p.col_in_list("transfer_to_address", to_lowercase_list(&self.transfer_to));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct TronQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub transfer_transactions: Vec<TransferTransactionRequest>,
        pub transfer_asset_transactions: Vec<TransferAssetTransactionRequest>,
        pub trigger_smart_contract_transactions: Vec<TriggerSmartContractTransactionRequest>,
        pub logs: Vec<LogRequest>,
        pub internal_transactions: Vec<InternalTransactionRequest>,
    }
}


impl TronQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(
            self,
            transactions,
            transfer_transactions,
            transfer_asset_transactions,
            trigger_smart_contract_transactions,
            logs,
            internal_transactions
        );
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [logs: self.fields.log.project()],
            [internal_transactions: self.fields.internal_transaction.project()],
            transactions,
            logs,
            internal_transactions,
            <transfer_transactions: transactions>,
            <transfer_asset_transactions: transactions>,
            <trigger_smart_contract_transactions: transactions>,
        )
    }
}
