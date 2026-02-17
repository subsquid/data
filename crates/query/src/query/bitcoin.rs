use super::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request, PredicateBuilder};
use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{Plan, ScanBuilder, TableSet};
use crate::primitives::BlockNumber;
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
    .add_child("inputs", vec!["block_number", "transaction_index"])
    .add_child("outputs", vec!["block_number", "transaction_index"]);

    tables.add_table("inputs", vec![
        "block_number",
        "transaction_index",
        "input_index"
    ]);

    tables.add_table("outputs", vec![
        "block_number",
        "transaction_index",
        "output_index"
    ]);

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    input: InputFieldSelection,
    output: OutputFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        timestamp,
        median_time,
        version,
        merkle_root,
        nonce,
        target,
        bits,
        difficulty,
        chain_work,
        stripped_size,
        size,
        weight,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        [this.timestamp]: TimestampSecond,
        [this.median_time]: TimestampSecond,
        [this.version]: Value,
        [this.merkle_root]: Value,
        [this.nonce]: Value,
        [this.target]: Value,
        [this.bits]: Value,
        [this.difficulty]: Value,
        [this.chain_work]: Value,
        [this.stripped_size]: Value,
        [this.size]: Value,
        [this.weight]: Value,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_index,
        hex,
        txid,
        hash,
        size,
        vsize,
        weight,
        version,
        locktime,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.hex,
        this.txid,
        this.hash,
        this.size,
        this.vsize,
        this.weight,
        this.version,
        this.locktime,
    }}
}


item_field_selection! {
    InputFieldSelection {
        transaction_index,
        input_index,
        r#type,
        txid,
        vout,
        script_sig_hex,
        script_sig_asm,
        sequence,
        coinbase,
        tx_in_witness,
        prevout_generated,
        prevout_height,
        prevout_value,
        prevout_script_pub_key_hex,
        prevout_script_pub_key_asm,
        prevout_script_pub_key_desc,
        prevout_script_pub_key_type,
        prevout_script_pub_key_address,
    }

    project(this) json_object! {
        {
            this.transaction_index,
            this.input_index,
            this.r#type,
            this.txid,
            this.vout,
            this.script_sig_hex,
            this.script_sig_asm,
            this.sequence,
            this.coinbase,
            this.tx_in_witness,
            this.prevout_generated,
            this.prevout_height,
            this.prevout_value,
            this.prevout_script_pub_key_hex,
            this.prevout_script_pub_key_asm,
            this.prevout_script_pub_key_desc,
            this.prevout_script_pub_key_type,
            this.prevout_script_pub_key_address,
        }
    }
}


item_field_selection! {
    OutputFieldSelection {
        transaction_index,
        output_index,
        value,
        script_pub_key_hex,
        script_pub_key_asm,
        script_pub_key_desc,
        script_pub_key_type,
        script_pub_key_address,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.output_index,
        this.value,
        this.script_pub_key_hex,
        this.script_pub_key_asm,
        this.script_pub_key_desc,
        this.script_pub_key_type,
        this.script_pub_key_address,
    }}
}


type Bytes = String;


request! {
    pub struct TransactionRequest {
        pub txid: Option<Vec<Bytes>>,
        pub inputs: bool,
        pub outputs: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("txid", self.txid.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.inputs {
            scan.join(
                "inputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.outputs {
            scan.join(
                "outputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct InputRequest {
        pub r#type: Option<Vec<String>>,
        pub prevout_script_pub_key_address: Option<Vec<Bytes>>,
        pub prevout_script_pub_key_type: Option<Vec<String>>,
        pub prevout_generated: Option<bool>,
        pub transaction: bool,
        pub transaction_inputs: bool,
        pub transaction_outputs: bool,
    }
}


impl InputRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.as_deref());
        p.col_in_list("prevout_script_pub_key_address", self.prevout_script_pub_key_address.as_deref());
        p.col_in_list("prevout_script_pub_key_type", self.prevout_script_pub_key_type.as_deref());
        p.col_eq("prevout_generated", self.prevout_generated);
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_inputs {
            scan.join(
                "inputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_outputs {
            scan.join(
                "outputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct OutputRequest {
        pub script_pub_key_address: Option<Vec<Bytes>>,
        pub script_pub_key_type: Option<Vec<String>>,
        pub transaction: bool,
        pub transaction_inputs: bool,
        pub transaction_outputs: bool,
    }
}


impl OutputRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("script_pub_key_address", self.script_pub_key_address.as_deref());
        p.col_in_list("script_pub_key_type", self.script_pub_key_type.as_deref());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_inputs {
            scan.join(
                "inputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_outputs {
            scan.join(
                "outputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct BitcoinQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub inputs: Vec<InputRequest>,
        pub outputs: Vec<OutputRequest>,
    }
}


impl BitcoinQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions, inputs, outputs);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [inputs: self.fields.input.project()],
            [outputs: self.fields.output.project()],
            transactions,
            inputs,
            outputs,
        )
    }
}
