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
        "index"
    ])
    .set_weight_column("input_asset_ids", "input_asset_ids_size")
    .set_weight_column("input_contracts", "input_contracts_size")
    .set_weight_column("witnesses", "witnesses_size")
    .set_weight_column("storage_slots", "storage_slots_size")
    .set_weight_column("proof_set", "proof_set_size")
    .set_weight_column("script_data", "script_data_size")
    .set_weight_column("raw_payload", "raw_payload_size");

    tables.add_table("receipts", vec![
        "block_number",
        "transaction_index",
        "index"
    ])
    .set_weight_column("data", "data_size");

    tables.add_table("inputs", vec![
        "block_number",
        "transaction_index",
        "index"
    ])
    .set_weight_column("coin_predicate", "coin_predicate_size")
    .set_weight_column("message_predicate", "message_predicate_size");

    tables.add_table("outputs", vec![
        "block_number",
        "transaction_index",
        "index"
    ]);

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    receipt: ReceiptFieldSelection,
    input: InputFieldSelection,
    output: OutputFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        transactions_root,
        transactions_count,
        message_receipt_count,
        prev_root,
        application_hash,
        event_inbox_root,
        consensus_parameters_version,
        state_transition_bytecode_version,
        message_outbox_root,
        da_height,
        time,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.transactions_root,
        this.transactions_count,
        this.message_receipt_count,
        this.prev_root,
        this.application_hash,
        this.event_inbox_root,
        this.consensus_parameters_version,
        this.state_transition_bytecode_version,
        this.message_outbox_root,
        [this.da_height]: BigNum,
        [this.time]: BigNum,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        index,
        hash,
        r#type,
        input_asset_ids,
        input_contracts,
        input_contract_utxo_id,
        input_contract_balance_root,
        input_contract_state_root,
        input_contract_tx_pointer,
        input_contract_contract_id,
        output_contract_input_index,
        output_contract_balance_root,
        output_contract_state_root,
        maturity,
        mint_asset_id,
        tx_pointer,
        is_script,
        is_create,
        is_mint,
        is_upgrade,
        is_upload,
        witnesses,
        receipts_root,
        script,
        script_data,
        bytecode_witness_index,
        bytecode_root,
        salt,
        storage_slots,
        raw_payload,
        subsection_index,
        subsections_number,
        proof_set,
        policies_maturity,
        policies_tip,
        policies_witness_limit,
        policies_max_fee,
        script_gas_limit,
        mint_amount,
        mint_gas_price,
        status,
        upgrade_purpose,
    }

    project(this) json_object! {{
        this.index,
        this.hash,
        this.r#type,
        this.input_asset_ids,
        this.input_contracts,
        this.input_contract_utxo_id,
        this.input_contract_balance_root,
        this.input_contract_state_root,
        this.input_contract_tx_pointer,
        this.input_contract_contract_id,
        this.output_contract_input_index,
        this.output_contract_balance_root,
        this.output_contract_state_root,
        this.maturity,
        this.mint_asset_id,
        this.tx_pointer,
        this.is_script,
        this.is_create,
        this.is_mint,
        this.is_upgrade,
        this.is_upload,
        this.witnesses,
        this.receipts_root,
        this.script,
        this.script_data,
        this.bytecode_witness_index,
        this.bytecode_root,
        this.salt,
        this.storage_slots,
        this.raw_payload,
        this.subsection_index,
        this.subsections_number,
        this.proof_set,
        this.policies_maturity,
        [this.policies_tip]: BigNum,
        [this.policies_witness_limit]: BigNum,
        [this.policies_max_fee]: BigNum,
        [this.script_gas_limit]: BigNum,
        [this.mint_amount]: BigNum,
        [this.mint_gas_price]: BigNum,
        [this.status]: Json,
        [this.upgrade_purpose]: Json,
    }}
}


item_field_selection! {
    ReceiptFieldSelection {
        index,
        transaction_index,
        contract,
        to,
        to_address,
        asset_id,
        digest,
        receipt_type,
        data,
        sender,
        recipient,
        nonce,
        contract_id,
        sub_id,
        pc,
        is,
        amount,
        gas,
        param1,
        param2,
        val,
        ptr,
        reason,
        ra,
        rb,
        rc,
        rd,
        len,
        result,
        gas_used,
    }

    project(this) json_object! {{
        this.index,
        this.transaction_index,
        this.contract,
        this.to,
        this.to_address,
        this.asset_id,
        this.digest,
        this.receipt_type,
        this.data,
        this.sender,
        this.recipient,
        this.nonce,
        this.contract_id,
        this.sub_id,
        [this.pc]: BigNum,
        [this.is]: BigNum,
        [this.amount]: BigNum,
        [this.gas]: BigNum,
        [this.param1]: BigNum,
        [this.param2]: BigNum,
        [this.val]: BigNum,
        [this.ptr]: BigNum,
        [this.reason]: BigNum,
        [this.ra]: BigNum,
        [this.rb]: BigNum,
        [this.rc]: BigNum,
        [this.rd]: BigNum,
        [this.len]: BigNum,
        [this.result]: BigNum,
        [this.gas_used]: BigNum,
    }}
}


item_field_selection! {
    InputFieldSelection {
        transaction_index,
        index,
        r#type,
        coin_utxo_id,
        coin_owner,
        coin_amount,
        coin_asset_id,
        coin_tx_pointer,
        coin_witness_index,
        coin_predicate_gas_used,
        coin_predicate,
        coin_predicate_data,
        contract_utxo_id,
        contract_balance_root,
        contract_state_root,
        contract_tx_pointer,
        contract_contract_id,
        message_sender,
        message_recipient,
        message_amount,
        message_nonce,
        message_witness_index,
        message_predicate_gas_used,
        message_data,
        message_predicate,
        message_predicate_data,
    }

    project(this) {
        let base = json_object! {{
            this.transaction_index,
            this.index,
            this.r#type,
        }};

        let mut coin = base.clone();
        if this.coin_utxo_id {
            coin.add("utxoId", prop("coin_utxo_id", Exp::Value));
        }
        if this.coin_owner {
            coin.add("owner", prop("coin_owner", Exp::Value));
        }
        if this.coin_amount {
            coin.add("amount", prop("coin_amount", Exp::BigNum));
        }
        if this.coin_asset_id {
            coin.add("assetId", prop("coin_asset_id", Exp::Value));
        }
        if this.coin_tx_pointer {
            coin.add("txPointer", prop("coin_tx_pointer", Exp::Value));
        }
        if this.coin_witness_index {
            coin.add("witnessIndex", prop("coin_witness_index", Exp::Value));
        }
        if this.coin_predicate_gas_used {
            coin.add("predicateGasUsed", prop("coin_predicate_gas_used", Exp::BigNum));
        }
        if this.coin_predicate {
            coin.add("predicate", prop("coin_predicate", Exp::Value));
        }
        if this.coin_predicate_data {
            coin.add("predicateData", prop("coin_predicate_data", Exp::Value));
        }

        let mut contract = base.clone();
        if this.contract_utxo_id {
            contract.add("utxoId", prop("contract_utxo_id", Exp::Value));
        }
        if this.contract_balance_root {
            contract.add("balanceRoot", prop("contract_balance_root", Exp::Value));
        }
        if this.contract_state_root {
            contract.add("stateRoot", prop("contract_state_root", Exp::Value));
        }
        if this.contract_tx_pointer {
            contract.add("txPointer", prop("contract_tx_pointer", Exp::Value));
        }
        if this.contract_contract_id {
            contract.add("contractId", prop("contract_contract_id", Exp::Value));
        }

        let mut message = base.clone();
        if this.message_sender {
            message.add("sender", prop("message_sender", Exp::Value));
        }
        if this.message_recipient {
            message.add("recipient", prop("message_recipient", Exp::Value));
        }
        if this.message_amount {
            message.add("amount", prop("message_amount", Exp::BigNum));
        }
        if this.message_nonce {
            message.add("nonce", prop("message_nonce", Exp::Value));
        }
        if this.message_witness_index {
            message.add("witnessIndex", prop("message_witness_index", Exp::Value));
        }
        if this.message_predicate_gas_used {
            message.add("predicateGasUsed", prop("message_predicate_gas_used", Exp::BigNum));
        }
        if this.message_data {
            message.add("data", prop("message_data", Exp::Value));
        }
        if this.message_predicate {
            message.add("predicate", prop("message_predicate", Exp::Value));
        }
        if this.message_predicate_data {
            message.add("predicateData", prop("message_predicate_data", Exp::Value));
        }

        Exp::Enum {
            tag_column: "type",
            variants: vec![
                ("InputCoin", coin.into()),
                ("InputContract", contract.into()),
                ("InputMessage", message.into())
            ]
        }
    }
}


item_field_selection! {
    OutputFieldSelection {
        transaction_index,
        index,
        r#type,
        coin_to,
        coin_amount,
        coin_asset_id,
        contract_input_index,
        contract_balance_root,
        contract_state_root,
        change_to,
        change_amount,
        change_asset_id,
        variable_to,
        variable_amount,
        variable_asset_id,
        contract_created_contract,
        contract_created_state_root,
    }

    project(this) {
        let base = json_object! {{
            this.transaction_index,
            this.index,
            this.r#type,
        }};

        let mut coin = base.clone();
        if this.coin_to {
            coin.add("to", prop("coin_to", Exp::Value));
        }
        if this.coin_amount {
            coin.add("amount", prop("coin_amount", Exp::Value));
        }
        if this.coin_asset_id {
            coin.add("assetId", prop("coin_asset_id", Exp::Value));
        }

        let mut contract = base.clone();
        if this.contract_input_index {
            contract.add("inputIndex", prop("contract_input_index", Exp::Value));
        }
        if this.contract_balance_root {
            contract.add("balanceRoot", prop("contract_balance_root", Exp::Value));
        }
        if this.contract_state_root {
            contract.add("stateRoot", prop("state_root", Exp::Value));
        }

        let mut change = base.clone();
        if this.change_to {
            change.add("to", prop("change_to", Exp::Value));
        }
        if this.change_amount {
            change.add("amount", prop("change_amount", Exp::Value));
        }
        if this.change_asset_id {
            change.add("assetId", prop("change_asset_id", Exp::Value));
        }

        let mut variable = base.clone();
        if this.variable_to {
            variable.add("to", prop("variable_to", Exp::Value));
        }
        if this.variable_amount {
            variable.add("amount", prop("variable_amount", Exp::Value));
        }
        if this.variable_asset_id {
            variable.add("assetId", prop("variable_asset_id", Exp::Value));
        }

        let mut contract_created = base.clone();
        if this.contract_created_contract {
            contract_created.add("contract", prop("contract_created_contract", Exp::Value));
        }
        if this.contract_created_state_root {
            contract_created.add("stateRoot", prop("contract_created_state_root", Exp::Value));
        }

        Exp::Enum {
            tag_column: "type",
            variants: vec![
                ("CoinOutput", coin.into()),
                ("ContractOutput", contract.into()),
                ("ChangeOutput", change.into()),
                ("VariableOutput", variable.into()),
                ("ContractCreated", contract_created.into())
            ]
        }
    }
}


type Bytes = String;


request! {
    pub struct ReceiptRequest {
        pub r#type: Option<Vec<String>>,
        pub contract: Option<Vec<Bytes>>,
        pub transaction: bool,
    }
}


impl ReceiptRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("receipt_type", self.r#type.clone());
        p.col_in_list("contract", self.contract.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "index"],
                vec!["block_number", "transaction_index"],
            );
        }
    }
}


request! {
    pub struct TransactionRequest {
        pub r#type: Option<Vec<String>>,
        pub receipts: bool,
        pub inputs: bool,
        pub outputs: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.receipts {
            scan.join(
                "receipts",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.inputs {
            scan.join(
                "inputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.outputs {
            scan.join(
                "outputs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct InputRequest {
        pub r#type: Option<Vec<String>>,
        pub coin_owner: Option<Vec<Bytes>>,
        pub coin_asset_id: Option<Vec<Bytes>>,
        pub contract_contract: Option<Vec<Bytes>>,
        pub message_sender: Option<Vec<Bytes>>,
        pub message_recipient: Option<Vec<Bytes>>,
        pub transaction: bool,
    }
}


impl InputRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.clone());
        p.col_in_list("coin_owner", self.coin_owner.clone());
        p.col_in_list("coin_asset_id", self.coin_asset_id.clone());
        p.col_in_list("contract_contract", self.contract_contract.clone());
        p.col_in_list("message_sender", self.message_sender.clone());
        p.col_in_list("message_recipient", self.message_recipient.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct OutputRequest {
        pub r#type: Option<Vec<String>>,
        pub transaction: bool,
    }
}


impl OutputRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("type", self.r#type.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct FuelQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub receipts: Vec<ReceiptRequest>,
        pub inputs: Vec<InputRequest>,
        pub outputs: Vec<OutputRequest>,
    }
}


impl FuelQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions, receipts, inputs, outputs);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [receipts: self.fields.receipt.project()],
            [inputs: self.fields.input.project()],
            [outputs: self.fields.output.project()],
            transactions,
            receipts,
            inputs,
            outputs,
        )
    }
}