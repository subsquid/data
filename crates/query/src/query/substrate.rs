use std::sync::LazyLock;

use serde::{Deserialize, Serialize};

use crate::{
    json::{exp::Exp, lang::*},
    plan::{Plan, ScanBuilder, TableSet},
    primitives::BlockNumber,
    query::util::{
        compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request,
        to_lowercase_list, PredicateBuilder
    }
};

static TABLES: LazyLock<TableSet> = LazyLock::new(|| {
    let mut tables = TableSet::new();

    tables.add_table("blocks", vec!["number"]).set_weight("digest", 32 * 4);

    tables
        .add_table("events", vec!["block_number", "index"])
        .set_weight_column("args", "args_size");

    tables
        .add_table("calls", vec!["block_number", "extrinsic_index", "address"])
        .set_weight_column("args", "args_size");

    tables
        .add_table("extrinsics", vec!["block_number", "index"])
        .add_child("calls", vec!["block_number", "extrinsic_index"])
        .set_weight("signature", 4 * 32);

    tables
});

field_selection! {
    block: BlockFieldSelection,
    extrinsic: ExtrinsicFieldSelection,
    call: CallFieldSelection,
    event: EventFieldSelection,
}

item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        state_root,
        extrinsics_root,
        digest,
        spec_name,
        spec_version,
        impl_name,
        impl_version,
        validator,
        timestamp,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        this.state_root,
        this.extrinsics_root,
        [this.digest]: Json,
        [this.spec_name]: Value,
        [this.spec_version]: Value,
        [this.impl_name]: Value,
        [this.impl_version]: Value,
        [this.validator]: Value,
        [this.timestamp]: TimestampMillisecond,
    }}
}

item_field_selection! {
    ExtrinsicFieldSelection {
        index,
        version,
        success,
        hash,
        fee,
        tip,
        signature,
        error,
    }

    project(this) json_object! {{
        this.index,
        this.version,
        this.success,
        this.hash,
        [this.fee]: BigNum,
        [this.tip]: BigNum,
        [this.signature]: Json,
        [this.error]: Json,
    }}
}

item_field_selection! {
    CallFieldSelection {
        extrinsic_index,
        address,
        name,
        success,
        args,
        origin,
        error,
    }

    project(this) json_object! {{
        this.extrinsic_index,
        this.address,
        this.name,
        this.success,
        [this.args]: Json,
        [this.origin]: Json,
        [this.error]: Json,
    }}
}

item_field_selection! {
    EventFieldSelection {
        index,
        extrinsic_index,
        name,
        phase,
        call_address,
        topics,
        args,
    }

    project(this) json_object! {{
        this.index,
        this.extrinsic_index,
        this.call_address,
        this.name,
        this.phase,
        this.topics,
        [this.args]: Json,
    }}
}

type Bytes = String;

request! {
    pub struct EventRequest {
        pub name: Option<Vec<String>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl EventRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("name", self.name.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

request! {
    pub struct CallRequest {
        pub name: Option<Vec<String>>,
        pub subcalls: bool,
        pub extrinsic: bool,
        pub stack: bool,
        pub events: bool,
    }
}

impl CallRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("name", self.name.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.subcalls {
            scan.include_children();
        }

        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.stack {
            scan.include_parents();
        }

        if self.events {
            scan.include_foreign_children(
                "events",
                vec!["block_number", "extrinsic_index", "call_address"],
                vec!["block_number", "extrinsic_index", "address"]
            );
        }
    }
}

request! {
    pub struct EvmLogRequest {
        pub address: Option<Vec<Bytes>>,
        pub topic0: Option<Vec<Bytes>>,
        pub topic1: Option<Vec<Bytes>>,
        pub topic2: Option<Vec<Bytes>>,
        pub topic3: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl EvmLogRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("EVM.Log"));
        p.col_in_list("_evm_log_address", to_lowercase_list(&self.address));
        p.col_in_list("_evm_log_topic0", to_lowercase_list(&self.topic0));
        p.col_in_list("_evm_log_topic1", to_lowercase_list(&self.topic1));
        p.col_in_list("_evm_log_topic2", to_lowercase_list(&self.topic2));
        p.col_in_list("_evm_log_topic3", to_lowercase_list(&self.topic3));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

request! {
    pub struct EthereumTransactionRequest {
        pub to: Option<Vec<Bytes>>,
        pub sighash: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub stack: bool,
        pub events: bool,
    }
}

impl EthereumTransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("Ethereum.transact"));
        p.col_in_list("_ethereum_transact_to", to_lowercase_list(&self.to));
        p.col_in_list("_ethereum_transact_sighash", to_lowercase_list(&self.sighash));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.stack {
            scan.include_parents();
        }

        if self.events {
            scan.include_foreign_children(
                "events",
                vec!["block_number", "extrinsic_index", "call_address"],
                vec!["block_number", "extrinsic_index", "address"]
            );
        }
    }
}

request! {
    pub struct ContractsContractEmittedRequest {
        pub contract_address: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl ContractsContractEmittedRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("Contracts.ContractEmitted"));
        p.col_in_list("_contract_address", self.contract_address.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

// Selects `Gear.MessageQueued` events, the ones that put a message into a
// program's queue; `program_id` is the destination program.
//
// The request is served under its historical name `gearMessagesEnqueued`
// and under `gearMessagesQueued`, the name the SDK uses. It used to pin
// `Gear.UserMessageEnqueued`, an event no Gear runtime ever had, and so
// matched nothing.
request! {
    pub struct GearMessageEnqueuedRequest {
        pub program_id: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl GearMessageEnqueuedRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("Gear.MessageQueued"));
        p.col_in_list("_gear_program_id", self.program_id.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

request! {
    pub struct GearUserMessageSentRequest {
        pub program_id: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl GearUserMessageSentRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("Gear.UserMessageSent"));
        p.col_in_list("_gear_program_id", self.program_id.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

request! {
    pub struct ReviveContractEmittedRequest {
        pub contract: Option<Vec<Bytes>>,
        pub topic0: Option<Vec<Bytes>>,
        pub topic1: Option<Vec<Bytes>>,
        pub topic2: Option<Vec<Bytes>>,
        pub topic3: Option<Vec<Bytes>>,
        pub extrinsic: bool,
        pub call: bool,
        pub stack: bool,
    }
}

impl ReviveContractEmittedRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_eq("name", Some("Revive.ContractEmitted"));
        p.col_in_list("_revive_contract", self.contract.as_deref());
        p.col_in_list("_revive_topic0", self.topic0.as_deref());
        p.col_in_list("_revive_topic1", self.topic1.as_deref());
        p.col_in_list("_revive_topic2", self.topic2.as_deref());
        p.col_in_list("_revive_topic3", self.topic3.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.extrinsic {
            scan.join(
                "extrinsics",
                vec!["block_number", "index"],
                vec!["block_number", "extrinsic_index"]
            );
        }

        if self.call {
            scan.join(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }

        if self.stack {
            scan.include_foreign_parents(
                "calls",
                vec!["block_number", "extrinsic_index", "address"],
                vec!["block_number", "extrinsic_index", "call_address"]
            );
        }
    }
}

request! {
    pub struct SubstrateQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub calls: Vec<CallRequest>,
        pub events: Vec<EventRequest>,
        pub evm_logs: Vec<EvmLogRequest>,
        pub ethereum_transactions: Vec<EthereumTransactionRequest>,
        pub contracts_events: Vec<ContractsContractEmittedRequest>,
        #[serde(alias = "gearMessagesQueued")]
        pub gear_messages_enqueued: Vec<GearMessageEnqueuedRequest>,
        pub gear_user_messages_sent: Vec<GearUserMessageSentRequest>,
        pub revive_contract_emitted: Vec<ReviveContractEmittedRequest>,
    }
}

impl SubstrateQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(
            self,
            calls,
            events,
            ethereum_transactions,
            contracts_events,
            gear_messages_enqueued,
            gear_user_messages_sent,
            evm_logs,
            revive_contract_emitted
        );
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [extrinsics: self.fields.extrinsic.project()],
            [calls: self.fields.call.project()],
            [events: self.fields.event.project()],
            calls,
            events,
            <ethereum_transactions: calls>,
            <contracts_events: events>,
            <gear_messages_enqueued: events>,
            <gear_user_messages_sent: events>,
            <evm_logs: events>,
            <revive_contract_emitted: events>,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{BooleanArray, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema}
    };

    use super::*;
    use crate::query::Query;

    const PROGRAM: &str = "0xa749ccd80d71637b450789e12e3d94524e9ae17877d1b59f5ddda784f89a2cba";
    const OTHER_PROGRAM: &str = "0x0000000000000000000000000000000000000000000000000000000000000001";

    fn substrate(json: &str) -> anyhow::Result<SubstrateQuery> {
        match Query::from_json_bytes(json.as_bytes())? {
            Query::Substrate(q) => Ok(q),
            _ => panic!("not a substrate query")
        }
    }

    /// Events as the ingest writes them: the program id is extracted for the two
    /// Gear events that carry one, and the third name is the one the request used
    /// to pin, which no Gear runtime emits.
    fn gear_events() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("_gear_program_id", DataType::Utf8, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(vec![
                    "Gear.MessageQueued",
                    "Gear.MessageQueued",
                    "Gear.UserMessageSent",
                    "Gear.UserMessageEnqueued",
                    "Balances.Transfer",
                ])),
                Arc::new(StringArray::from(vec![
                    Some(PROGRAM),
                    Some(OTHER_PROGRAM),
                    Some(PROGRAM),
                    Some(PROGRAM),
                    None,
                ])),
            ]
        )
        .unwrap()
    }

    fn matches(predicate: impl FnOnce(&mut PredicateBuilder)) -> Vec<bool> {
        let mut builder = PredicateBuilder::new();
        predicate(&mut builder);
        let mask: BooleanArray = builder
            .build()
            .expect("a gear request always pins the event name")
            .evaluate(&gear_events())
            .unwrap();
        mask.iter().map(|v| v.unwrap_or(false)).collect()
    }

    #[test]
    fn gear_messages_enqueued_selects_message_queued_events() {
        let q = substrate(r#"{"type":"substrate","fromBlock":0,"gearMessagesEnqueued":[{}]}"#).unwrap();
        assert_eq!(
            matches(|p| q.gear_messages_enqueued[0].predicate(p)),
            vec![true, true, false, false, false]
        );

        let q = substrate(&format!(
            r#"{{"type":"substrate","fromBlock":0,"gearMessagesEnqueued":[{{"programId":["{PROGRAM}"]}}]}}"#
        ))
        .unwrap();
        assert_eq!(
            matches(|p| q.gear_messages_enqueued[0].predicate(p)),
            vec![true, false, false, false, false]
        );
    }

    #[test]
    fn gear_messages_queued_is_the_same_request() {
        let body = format!(r#"[{{"programId":["{PROGRAM}"],"extrinsic":true,"call":true,"stack":true}}]"#);
        let queued = substrate(&format!(
            r#"{{"type":"substrate","fromBlock":0,"gearMessagesQueued":{body}}}"#
        ))
        .unwrap();
        let enqueued = substrate(&format!(
            r#"{{"type":"substrate","fromBlock":0,"gearMessagesEnqueued":{body}}}"#
        ))
        .unwrap();
        assert_eq!(queued, enqueued);
        assert_eq!(queued.gear_messages_enqueued.len(), 1);
    }

    /// Both names in one query name the same field twice, which serde refuses.
    #[test]
    fn both_gear_message_queued_names_at_once_are_refused() {
        let err =
            substrate(r#"{"type":"substrate","fromBlock":0,"gearMessagesEnqueued":[{}],"gearMessagesQueued":[{}]}"#)
                .unwrap_err();
        assert!(err.to_string().contains("duplicate field"), "{err}");
    }

    #[test]
    fn gear_user_messages_sent_is_unchanged() {
        let q = substrate(&format!(
            r#"{{"type":"substrate","fromBlock":0,"gearUserMessagesSent":[{{}},{{"programId":["{PROGRAM}"]}}]}}"#
        ))
        .unwrap();
        assert_eq!(
            matches(|p| q.gear_user_messages_sent[0].predicate(p)),
            vec![false, false, true, false, false]
        );
        assert_eq!(
            matches(|p| q.gear_user_messages_sent[1].predicate(p)),
            vec![false, false, true, false, false]
        );
    }
}
