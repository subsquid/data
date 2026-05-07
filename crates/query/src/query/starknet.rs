use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{Plan, ScanBuilder, TableSet};
use crate::primitives::BlockNumber;
use crate::query::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request, PredicateBuilder};


lazy_static! {
    static ref TABLES: TableSet = {
        let mut tables = TableSet::new();

        tables.add_table("blocks", vec![
            "number"
        ]);

        tables.add_table("transactions", vec![
            "block_number",
            "transaction_index"
        ])
        .set_weight_column("calldata", "calldata_size")
        .set_weight_column("signature", "signature_size")
        .set_weight_column("constructor_calldata", "constructor_calldata_size");

        tables.add_table("events", vec![
            "block_number",
            "transaction_index",
            "event_index"
        ])
        .set_weight_column("key0", "keys_size")
        .set_weight("key1", 0)
        .set_weight("key2", 0)
        .set_weight("key3", 0)
        .set_weight("rest_keys", 0)
        .set_weight_column("data", "data_size");

        tables
    };
}


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    event: EventFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        parent_hash,
        status,
        new_root,
        timestamp,
        sequencer_address,
    }

    project(this) json_object! {{
        number,
        hash,
        [this.parent_hash],
        [this.status],
        [this.new_root],
        [this.sequencer_address],
        <this.timestamp>: TimestampSecond,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_hash,
        contract_address,
        entry_point_selector,
        calldata,
        max_fee,
        r#type,
        sender_address,
        version,
        signature,
        nonce,
        class_hash,
        compiled_class_hash,
        contract_address_salt,
        constructor_calldata,
    }

    project(this) json_object! {{
        transaction_index,
        [this.transaction_hash],
        [this.contract_address],
        [this.entry_point_selector],
        [this.calldata],
        [this.max_fee],
        [this.r#type],
        [this.sender_address],
        [this.version],
        [this.signature],
        [this.nonce],
        [this.class_hash],
        [this.compiled_class_hash],
        [this.contract_address_salt],
        [this.constructor_calldata],
    }}
}


item_field_selection! {
    EventFieldSelection {
        from_address,
        keys,
        data,
    }

    project(this) json_object! {{
        transaction_index,
        event_index,
        [this.from_address],
        [this.data],
        |obj| {
            if this.keys {
                obj.add("keys", roll(Exp::Value, vec![
                    "key0",
                    "key1",
                    "key2",
                    "key3",
                    "rest_keys"
                ]));
            }
        }
    }}
}


type Bytes = String;


request! {
    pub struct EventRequest {
        pub from_address: Option<Vec<Bytes>>,
        pub key0: Option<Vec<Bytes>>,
        pub key1: Option<Vec<Bytes>>,
        pub key2: Option<Vec<Bytes>>,
        pub key3: Option<Vec<Bytes>>,
        pub transaction: bool,
    }
}


impl EventRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("from_address", self.from_address.clone());
        p.col_in_list("key0", self.key0.clone());
        p.col_in_list("key1", self.key1.clone());
        p.col_in_list("key2", self.key2.clone());
        p.col_in_list("key3", self.key3.clone());
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
    pub struct TransactionRequest {
        pub contract_address: Option<Vec<Bytes>>,
        pub sender_address: Option<Vec<Bytes>>,
        pub r#type: Option<Vec<String>>,
        pub first_nonce: Option<u64>,
        pub last_nonce: Option<u64>,
        pub events: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("contract_address", self.contract_address.clone());
        p.col_in_list("sender_address", self.sender_address.clone());
        p.col_in_list("type", self.r#type.clone());
        p.col_gt_eq("nonce", self.first_nonce);
        p.col_lt_eq("nonce", self.last_nonce);
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.events {
            scan.join(
                "events",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct StarknetQuery {
        pub from_block: Option<BlockNumber>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub events: Vec<EventRequest>,
    }
}


impl StarknetQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions, events);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [events: self.fields.event.project()],
            transactions,
            events,
        )
    }
}