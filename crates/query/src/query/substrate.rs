use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{Plan, ScanBuilder, TableSet};
use crate::primitives::BlockNumber;
use crate::query::util::{compile_plan, ensure_block_range, field_selection, item_field_selection, request, PredicateBuilder};


lazy_static! {
    static ref TABLES: TableSet = {
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
        .set_weight("signature", 4 * 32);

        tables
    };
}


field_selection! {
    block: BlockFieldSelection,
    extrinsic: ExtrinsicFieldSelection,
    call: CallFieldSelection,
    event: EventFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
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
        number,
        hash, // backwards compatibility
        parent_hash, // backwards compatibility
        [this.state_root],
        [this.extrinsics_root],
        [this.digest],
        [this.spec_name],
        [this.spec_version],
        [this.impl_name],
        [this.impl_version],
        [this.validator],
        <this.timestamp>: TimestampSecond,
    }}
}


item_field_selection! {
    ExtrinsicFieldSelection {
        version,
        success,
        hash,
        fee,
        tip,
        signature,
        error,
    }

    project(this) json_object! {{
        index,
        [this.version],
        [this.success],
        [this.hash],
        <this.fee>: BigNum,
        <this.tip>: BigNum,
        <this.signature>: Json,
        <this.error>: Json,
    }}
}


item_field_selection! {
    CallFieldSelection {
        name,
        success,
        args,
        origin,
        error,
    }

    project(this) json_object! {{
        extrinsic_index,
        address,
        [this.name],
        [this.success],
        <this.args>: Json,
        <this.origin>: Json,
        <this.error>: Json,
    }}
}


item_field_selection! {
    EventFieldSelection {
        extrinsic_index, // backwards compatibility
        name,
        phase,
        call_address,
        topics,
        args,
    }

    project(this) json_object! {{
        index,
        extrinsic_index,
        call_address,
        [this.name],
        [this.phase],
        [this.call_address],
        [this.topics],
        <this.args>: Json,
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
        p.col_in_list("name", self.name.clone());
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
        p.col_in_list("name", self.name.clone());
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
        p.col_in_list("name", Some(["EVM.Log"]));
        p.col_in_list("_evm_log_address", self.address.clone());
        p.col_in_list("_evm_log_topic0", self.topic0.clone());
        p.col_in_list("_evm_log_topic1", self.topic1.clone());
        p.col_in_list("_evm_log_topic2", self.topic2.clone());
        p.col_in_list("_evm_log_topic3", self.topic3.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {
        // if self.transaction {
        //     scan.join(
        //         "transactions",
        //         vec!["block_number", "index"],
        //         vec!["block_number", "transaction_index"]
        //     );
        // }
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
        p.col_in_list("name", Some(["Ethereum.transact"]));
        p.col_in_list("_ethereum_transact_to", self.to.clone());
        p.col_in_list("_ethereum_transact_sighash", self.sighash.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {
        // if self.transaction {
        //     scan.join(
        //         "transactions",
        //         vec!["block_number", "index"],
        //         vec!["block_number", "transaction_index"]
        //     );
        // }
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
        p.col_in_list("name", Some(["Contracts.ContractEmitted"]));
        p.col_in_list("_contract_address", self.contract_address.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {
        // if self.transaction {
        //     scan.join(
        //         "transactions",
        //         vec!["block_number", "index"],
        //         vec!["block_number", "transaction_index"]
        //     );
        // }
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
        p.col_in_list("name", Some(["Gear.UserMessageEnqueued"]));
        p.col_in_list("_gear_program_id", self.program_id.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {
        // if self.transaction {
        //     scan.join(
        //         "transactions",
        //         vec!["block_number", "index"],
        //         vec!["block_number", "transaction_index"]
        //     );
        // }
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
        p.col_in_list("name", Some(["Gear.UserMessageSent"]));
        p.col_in_list("_gear_program_id", self.program_id.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {
        // if self.extrinsic {
        //     scan.join(
        //         "transactions",
        //         vec!["block_number", "index"],
        //         vec!["block_number", "transaction_index"]
        //     );
        // }
    }
}


request! {
    pub struct SubstrateQuery {
        pub from_block: Option<BlockNumber>,
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
            ethereum_transactions,
            contracts_events,
            gear_messages_enqueued,
            gear_user_messages_sent,
            <evm_logs: events>,
        )
    }
}