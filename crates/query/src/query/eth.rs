use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{ScanBuilder, TableSet};
use crate::query::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request, PredicateBuilder};
use crate::{BlockNumber, Plan};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;


static TABLES: LazyLock<TableSet> = LazyLock::new(|| {
    let mut tables = TableSet::new();

    tables.add_table("blocks", vec![
        "number"
    ])
    .set_weight("logs_bloom", 512)
    .set_weight_column("extra_data", "extra_data_size");

    tables.add_table("transactions", vec![
        "block_number",
        "transaction_index"
    ])
    .add_child("logs", vec!["block_number", "transaction_index"])
    .add_child("traces", vec!["block_number", "transaction_index"])
    .add_child("statediffs", vec!["block_number", "transaction_index"])
    .set_weight_column("input", "input_size");

    tables.add_table("logs", vec![
        "block_number",
        "log_index"
    ])
    .set_weight_column("data", "data_size");

    tables.add_table("traces", vec![
        "block_number",
        "transaction_index",
        "trace_address"
    ])
    .set_weight_column("create_init", "create_init_size")
    .set_weight_column("create_result_code", "create_result_code_size")
    .set_weight_column("call_input", "call_input_size")
    .set_weight_column("call_result_output", "call_result_output_size");

    tables.add_table("statediffs", vec![
        "block_number",
        "transaction_index",
        "address",
        "key"
    ])
    .set_weight_column("prev", "prev_size")
    .set_weight_column("next", "next_size")
    .set_result_item_name("stateDiffs");

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    log: LogFieldSelection,
    trace: TraceFieldSelection,
    state_diff: StateDiffFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        timestamp,
        transactions_root,
        receipts_root,
        state_root,
        logs_bloom,
        sha3_uncles,
        extra_data,
        miner,
        nonce,
        mix_hash,
        size,
        gas_limit,
        gas_used,
        difficulty,
        total_difficulty,
        base_fee_per_gas,
        blob_gas_used,
        excess_blob_gas,
        l1_block_number,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        [this.timestamp]: TimestampSecond,
        [this.transactions_root]: Value,
        [this.receipts_root]: Value,
        [this.state_root]: Value,
        [this.logs_bloom]: Value,
        [this.sha3_uncles]: Value,
        [this.extra_data]: Value,
        [this.miner]: Value,
        [this.nonce]: Value,
        [this.mix_hash]: Value,
        [this.size]: Value,
        [this.gas_limit]: Value,
        [this.gas_used]: Value,
        [this.difficulty]: Value,
        [this.total_difficulty]: Value,
        [this.base_fee_per_gas]: Value,
        [this.blob_gas_used]: Value,
        [this.excess_blob_gas]: Value,
        [this.l1_block_number]: Value,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_index,
        hash,
        nonce,
        from,
        to,
        input,
        value,
        gas,
        gas_price,
        max_fee_per_gas,
        max_priority_fee_per_gas,
        v,
        r,
        s,
        y_parity,
        chain_id,
        sighash,
        contract_address,
        gas_used,
        cumulative_gas_used,
        effective_gas_price,
        r#type,
        status,
        max_fee_per_blob_gas,
        blob_versioned_hashes,
        l1_fee,
        l1_fee_scalar,
        l1_gas_price,
        l1_gas_used,
        l1_blob_base_fee,
        l1_blob_base_fee_scalar,
        l1_base_fee_scalar,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.hash,
        this.nonce,
        this.from,
        this.to,
        this.input,
        this.value,
        this.gas,
        this.gas_price,
        this.max_fee_per_gas,
        this.max_priority_fee_per_gas,
        this.v,
        this.r,
        this.s,
        this.y_parity,
        this.chain_id,
        this.sighash,
        this.contract_address,
        this.gas_used,
        this.cumulative_gas_used,
        this.effective_gas_price,
        this.r#type,
        this.status,
        this.max_fee_per_blob_gas,
        this.blob_versioned_hashes,
        this.l1_fee,
        this.l1_fee_scalar,
        this.l1_gas_price,
        this.l1_gas_used,
        this.l1_blob_base_fee,
        this.l1_blob_base_fee_scalar,
        this.l1_base_fee_scalar,
    }}
}


item_field_selection! {
    LogFieldSelection {
        log_index,
        transaction_index,
        transaction_hash,
        address,
        data,
        topics,
    }

    project(this) json_object! {{
        this.log_index,
        this.transaction_index,
        this.transaction_hash,
        this.address,
        this.data,
        |obj| {
            if this.topics {
                obj.add("topics", roll(Exp::Value, vec![
                    "topic0",
                    "topic1",
                    "topic2",
                    "topic3"
                ]));
            }
        }
    }}
}


item_field_selection! {
    TraceFieldSelection {
        transaction_index,
        trace_address,
        subtraces,
        r#type,
        error,
        revert_reason,
        create_from,
        create_value,
        create_gas,
        create_init,
        create_result_gas_used,
        create_result_code,
        create_result_address,
        call_from,
        call_to,
        call_value,
        call_gas,
        call_input,
        call_sighash,
        call_type,
        call_call_type,
        call_result_gas_used,
        call_result_output,
        suicide_address,
        suicide_refund_address,
        suicide_balance,
        reward_author,
        reward_value,
        reward_type,
    }

    project(this) {
        let base = json_object! {{
            this.transaction_index,
            this.trace_address,
            this.r#type,
            this.subtraces,
            this.error,
            this.revert_reason,
        }};

        let mut create = base.clone();

        let mut create_action = JsonObject::new();
        if this.create_from {
            create_action.add("from", prop("create_from", Exp::Value));
        }
        if this.create_value {
            create_action.add("value", prop("create_value", Exp::Value));
        }
        if this.create_gas {
            create_action.add("gas", prop("create_gas", Exp::Value));
        }
        if this.create_init {
            create_action.add("init", prop("create_init", Exp::Value));
        }
        if !create_action.is_empty() {
            create.add("action", create_action);
        }

        let mut create_result = JsonObject::new();
        if this.create_result_gas_used {
            create_result.add("gasUsed", prop("create_result_gas_used", Exp::Value));
        }
        if this.create_result_code {
            create_result.add("code", prop("create_result_code", Exp::Value));
        }
        if this.create_result_address {
            create_result.add("address", prop("create_result_address", Exp::Value));
        }
        if !create_result.is_empty() {
            create.add("result", create_result);
        }

        let mut call = base.clone();

        let mut call_action = JsonObject::new();
        if this.call_from {
            call_action.add("from", prop("call_from", Exp::Value));
        }
        if this.call_to {
            call_action.add("to", prop("call_to", Exp::Value));
        }
        if this.call_value {
            call_action.add("value", prop("call_value", Exp::Value));
        }
        if this.call_gas {
            call_action.add("gas", prop("call_gas", Exp::Value));
        }
        if this.call_input {
            call_action.add("input", prop("call_input", Exp::Value));
        }
        if this.call_sighash {
            call_action.add("sighash", prop("call_sighash", Exp::Value));
        }
        if this.call_type {
            call_action.add("type", prop("call_type", Exp::Value));
        }
        if this.call_call_type {
            call_action.add("callType", prop("call_type", Exp::Value));    
        }
        if !call_action.is_empty() {
            call.add("action", call_action);
        }

        let mut call_result = JsonObject::new();
        if this.call_result_gas_used {
            call_result.add("gasUsed", prop("call_result_gas_used", Exp::Value));
        }
        if this.call_result_output {
            call_result.add("output", prop("call_result_output", Exp::Value));
        }
        if !call_result.is_empty() {
            call.add("result", call_result);
        }

        let mut suicide = base.clone();
        let mut suicide_action = JsonObject::new();
        if this.suicide_address {
            suicide_action.add("address", prop("suicide_address", Exp::Value));
        }
        if this.suicide_refund_address {
            suicide_action.add("refundAddress", prop("suicide_refund_address", Exp::Value));
        }
        if this.suicide_balance {
            suicide_action.add("balance", prop("suicide_balance", Exp::Value));
        }
        if !suicide_action.is_empty() {
            suicide.add("action", suicide_action);
        }

        let mut reward = base.clone();
        let mut reward_action = JsonObject::new();
        if this.reward_author {
            reward_action.add("author", prop("reward_author", Exp::Value));
        }
        if this.reward_value {
            reward_action.add("value", prop("reward_value", Exp::Value));
        }
        if this.reward_type {
            reward_action.add("type", prop("reward_type", Exp::Value));
        }
        if !reward_action.is_empty() {
            reward.add("action", reward_action);
        }

        Exp::Enum {
            tag_column: "type",
            variants: vec![
                ("create", create.into()),
                ("call", call.into()),
                ("suicide", suicide.into()),
                ("reward", reward.into())
            ]
        }
    }
}


item_field_selection! {
    StateDiffFieldSelection {
        transaction_index,
        address,
        key,
        kind,
        prev,
        next,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.address,
        this.key,
        this.kind,
        this.prev,
        this.next,
    }}
}


type Bytes = String;


request! {
    pub struct TransactionRequest {
        pub from: Option<Vec<Bytes>>,
        pub to: Option<Vec<Bytes>>,
        pub sighash: Option<Vec<Bytes>>,
        pub first_nonce: Option<u64>,
        pub last_nonce: Option<u64>,
        pub logs: bool,
        pub traces: bool,
        pub state_diffs: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("from", self.from.clone());
        p.col_in_list("to", self.to.clone());
        p.col_in_list("sighash", self.sighash.clone());
        p.col_gt_eq("nonce", self.first_nonce);
        p.col_lt_eq("nonce", self.last_nonce);
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.traces {
            scan.join(
                "traces",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.state_diffs {
            scan.join(
                "statediffs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
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
        pub transaction_traces: bool,
        pub transaction_logs: bool,
        pub transaction_state_diffs: bool,
    }
}


impl LogRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("address", self.address.clone());
        p.col_in_list("topic0", self.topic0.clone());
        p.col_in_list("topic1", self.topic1.clone());
        p.col_in_list("topic2", self.topic2.clone());
        p.col_in_list("topic3", self.topic3.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_traces {
            scan.join(
                "traces",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_state_diffs {
            scan.join(
                "statediffs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct TraceRequest {
        pub r#type: Option<Vec<String>>,
        pub create_from: Option<Vec<Bytes>>,
        pub create_result_address: Option<Vec<Bytes>>,
        pub call_from: Option<Vec<Bytes>>,
        pub call_to: Option<Vec<Bytes>>,
        pub call_sighash: Option<Vec<Bytes>>,
        pub suicide_refund_address: Option<Vec<Bytes>>,
        pub reward_author: Option<Vec<Bytes>>,
        pub transaction: bool,
        pub transaction_logs: bool,
        pub subtraces: bool,
        pub parents: bool,
    }
}


impl TraceRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.clone());
        p.col_in_list("create_from", self.create_from.clone());
        p.col_in_list("create_result_address", self.create_result_address.clone());
        p.col_in_list("call_from", self.call_from.clone());
        p.col_in_list("call_to", self.call_to.clone());
        p.col_in_list("call_sighash", self.call_sighash.clone());
        p.col_in_list("suicide_refund_address", self.suicide_refund_address.clone());
        p.col_in_list("reward_author", self.reward_author.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.subtraces {
            scan.include_children();
        }
        if self.parents {
            scan.include_parents();
        }
    }
}


request! {
    pub struct StateDiffRequest {
        pub address: Option<Vec<Bytes>>,
        pub key: Option<Vec<Bytes>>,
        pub kind: Option<Vec<String>>,
        pub transaction: bool,
    }
}


impl StateDiffRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("address", self.address.clone());
        p.col_in_list("key", self.key.clone());
        p.col_in_list("kind", self.kind.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct EthQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub logs: Vec<LogRequest>,
        pub traces: Vec<TraceRequest>,
        #[serde(rename = "stateDiffs")]
        pub statediffs: Vec<StateDiffRequest>,
    }
}


impl EthQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions, logs, traces, statediffs);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [logs: self.fields.log.project()],
            [traces: self.fields.trace.project()],
            [statediffs: self.fields.state_diff.project()],
            transactions,
            logs,
            traces,
            statediffs,
        )
    }
}