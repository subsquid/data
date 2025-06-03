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
    ])
    .set_weight("digest", 32 * 4);

    tables.add_table("events", vec![
        "block_number",
        "index"
    ])
    .set_weight_column("args", "args_size");

    tables.add_table("calls", vec![
        "block_number",
        "extrinsic_index",
        "address"
    ])
    .set_weight_column("args", "args_size");

    tables.add_table("extrinsics", vec![
        "block_number",
        "index"
    ])
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
        this.call_address,
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
        p.col_eq("name", Some("Gear.UserMessageEnqueued"));
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
        pub gear_messages_enqueued: Vec<GearMessageEnqueuedRequest>,
        pub gear_user_messages_sent: Vec<GearUserMessageSentRequest>,
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
            evm_logs
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
        )
    }
}