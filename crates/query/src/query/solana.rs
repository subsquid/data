use super::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, convert_from_hex_lossy, item_field_selection, request, PredicateBuilder};
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
    .add_child("logs", vec!["block_number", "transaction_index"])
    .add_child("balances", vec!["block_number", "transaction_index"])
    .add_child("token_balances", vec!["block_number", "transaction_index"])
    .set_weight_column("account_keys", "account_keys_size")
    .set_weight_column("address_table_lookups", "address_table_lookups_size")
    .set_weight_column("signatures", "signatures_size")
    .set_weight_column("loaded_addresses", "loaded_addresses_size");

    tables.add_table("instructions", vec![
        "block_number",
        "transaction_index",
        "instruction_address"
    ])
    .set_weight_column("data", "data_size")
    .set_weight_column("a0", "accounts_size")
    .set_weight("a1", 0)
    .set_weight("a2", 0)
    .set_weight("a3", 0)
    .set_weight("a4", 0)
    .set_weight("a5", 0)
    .set_weight("a6", 0)
    .set_weight("a7", 0)
    .set_weight("a8", 0)
    .set_weight("a9", 0)
    .set_weight("a10", 0)
    .set_weight("a11", 0)
    .set_weight("a12", 0)
    .set_weight("a13", 0)
    .set_weight("a14", 0)
    .set_weight("a15", 0)
    .set_weight("rest_accounts", 0);

    tables.add_table("logs", vec![
        "block_number",
        "transaction_index",
        "log_index"
    ])
    .set_weight_column("message", "message_size");

    tables.add_table("balances", vec![
        "block_number",
        "transaction_index",
        "account"
    ]);

    tables.add_table("token_balances", vec![
        "block_number",
        "transaction_index",
        "account"
    ]);

    tables.add_table("rewards", vec![
        "block_number",
        "pubkey"
    ]);

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
    instruction: InstructionFieldSelection,
    log: LogFieldSelection,
    balance: BalanceFieldSelection,
    token_balance: TokenBalanceFieldSelection,
    reward: RewardFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_number,
        parent_hash,
        height,
        timestamp,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_number,
        this.parent_hash,
        this.height,
        [this.timestamp]: TimestampSecond,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_index,
        version,
        account_keys,
        address_table_lookups,
        num_readonly_signed_accounts,
        num_readonly_unsigned_accounts,
        num_required_signatures,
        recent_blockhash,
        signatures,
        err,
        fee,
        compute_units_consumed,
        loaded_addresses,
        fee_payer,
        has_dropped_log_messages,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.version,
        this.account_keys,
        this.address_table_lookups,
        this.num_readonly_signed_accounts,
        this.num_readonly_unsigned_accounts,
        this.num_required_signatures,
        this.recent_blockhash,
        this.signatures,
        [this.err]: Json,
        [this.fee]: BigNum,
        [this.compute_units_consumed]: BigNum,
        [this.loaded_addresses]: Value,
        [this.fee_payer]: Value,
        [this.has_dropped_log_messages]: Value,
    }}
}


item_field_selection! {
    InstructionFieldSelection {
        transaction_index,
        instruction_address,
        program_id,
        accounts,
        data,
        d1,
        d2,
        d4,
        d8,
        error,
        compute_units_consumed,
        is_committed,
        has_dropped_log_messages,
    }

    project(this) json_object! {
        {
            this.transaction_index,
            this.instruction_address,
            this.program_id,
        },
        {
            |obj| {
                if this.accounts {
                    obj.add("accounts", roll(Exp::Value, vec![
                        "a0",
                        "a1",
                        "a2",
                        "a3",
                        "a4",
                        "a5",
                        "a6",
                        "a7",
                        "a8",
                        "a9",
                        "a10",
                        "a11",
                        "a12",
                        "a13",
                        "a14",
                        "a15",
                        "rest_accounts",
                    ]));
                }
            }
        },
        {
            this.data,
            [this.d1]: HexNum,
            [this.d2]: HexNum,
            [this.d4]: HexNum,
            [this.d8]: HexNum,
            [this.error]: Value,
            [this.compute_units_consumed]: BigNum,
            [this.is_committed]: Value,
            [this.has_dropped_log_messages]: Value,
        }
    }
}


item_field_selection! {
    LogFieldSelection {
        transaction_index,
        log_index,
        instruction_address,
        program_id,
        kind,
        message,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.log_index,
        this.instruction_address,
        this.program_id,
        this.kind,
        this.message,
    }}
}


item_field_selection! {
    BalanceFieldSelection {
        transaction_index,
        account,
        pre,
        post,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.account,
        [this.pre]: BigNum,
        [this.post]: BigNum,
    }}
}


item_field_selection! {
    TokenBalanceFieldSelection {
        transaction_index,
        account,
        pre_mint,
        post_mint,
        pre_decimals,
        post_decimals,
        pre_program_id,
        post_program_id,
        pre_owner,
        post_owner,
        pre_amount,
        post_amount,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.account,
        this.pre_mint,
        this.post_mint,
        this.pre_decimals,
        this.post_decimals,
        this.pre_program_id,
        this.post_program_id,
        this.pre_owner,
        this.post_owner,
        [this.pre_amount]: BigNum,
        [this.post_amount]: BigNum,
    }}
}


item_field_selection! {
    RewardFieldSelection {
        pubkey,
        lamports,
        post_balance,
        reward_type,
        commission,
    }

    project(this) json_object! {{
        this.pubkey,
        [this.lamports]: BigNum,
        [this.post_balance]: BigNum,
        [this.reward_type]: Value,
        [this.commission]: Value,
    }}
}


type Bytes = String;
type Base58Bytes = String;


request! {
    pub struct InstructionRequest {
        pub program_id: Option<Vec<Base58Bytes>>,
        pub d1: Option<Vec<Bytes>>,
        pub d2: Option<Vec<Bytes>>,
        pub d4: Option<Vec<Bytes>>,
        pub d8: Option<Vec<Bytes>>,
        pub a0: Option<Vec<Bytes>>,
        pub a1: Option<Vec<Bytes>>,
        pub a2: Option<Vec<Bytes>>,
        pub a3: Option<Vec<Bytes>>,
        pub a4: Option<Vec<Bytes>>,
        pub a5: Option<Vec<Bytes>>,
        pub a6: Option<Vec<Bytes>>,
        pub a7: Option<Vec<Bytes>>,
        pub a8: Option<Vec<Bytes>>,
        pub a9: Option<Vec<Bytes>>,
        pub a10: Option<Vec<Bytes>>,
        pub a11: Option<Vec<Bytes>>,
        pub a12: Option<Vec<Bytes>>,
        pub a13: Option<Vec<Bytes>>,
        pub a14: Option<Vec<Bytes>>,
        pub a15: Option<Vec<Bytes>>,
        pub is_committed: Option<bool>,
        pub transaction: bool,
        pub transaction_token_balances: bool,
        pub inner_instructions: bool,
        pub logs: bool,
    }
}


impl InstructionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("program_id", self.program_id.clone());
        p.col_in_list("d1", self.d1.as_ref().map(convert_from_hex_lossy::<u8>));
        p.col_in_list("d2", self.d2.as_ref().map(convert_from_hex_lossy::<u16>));
        p.col_in_list("d4", self.d4.as_ref().map(convert_from_hex_lossy::<u32>));
        p.col_in_list("d8", self.d8.as_ref().map(convert_from_hex_lossy::<u64>));
        p.col_in_list("a0", self.a0.clone());
        p.col_in_list("a1", self.a1.clone());
        p.col_in_list("a2", self.a2.clone());
        p.col_in_list("a3", self.a3.clone());
        p.col_in_list("a4", self.a4.clone());
        p.col_in_list("a5", self.a5.clone());
        p.col_in_list("a6", self.a6.clone());
        p.col_in_list("a7", self.a7.clone());
        p.col_in_list("a8", self.a8.clone());
        p.col_in_list("a9", self.a9.clone());
        p.col_in_list("a10", self.a10.clone());
        p.col_in_list("a11", self.a11.clone());
        p.col_in_list("a12", self.a12.clone());
        p.col_in_list("a13", self.a13.clone());
        p.col_in_list("a14", self.a14.clone());
        p.col_in_list("a15", self.a15.clone());
        p.col_eq("is_committed", self.is_committed);
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_token_balances {
            scan.join(
                "token_balances",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.inner_instructions {
            scan.include_children();
        }
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index", "instruction_address"],
                vec!["block_number", "transaction_index", "instruction_address"],
            );
        }
    }
}


request! {
    pub struct TransactionRequest {
        pub fee_payer: Option<Vec<Bytes>>,
        pub instructions: bool,
        pub logs: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("fee_payer", self.fee_payer.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.instructions {
            scan.join(
                "instructions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.logs {
            scan.join(
                "logs",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct LogRequest {
        pub program_id: Option<Vec<Bytes>>,
        pub kind: Option<Vec<String>>,
        pub instruction: bool,
        pub transaction: bool,
    }
}


impl LogRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("program_id", self.program_id.clone());
        p.col_in_list("kind", self.kind.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.instruction {
            scan.join(
                "instructions",
                vec!["block_number", "transaction_index", "instruction_address"],
                vec!["block_number", "transaction_index", "instruction_address"]
            );
        }

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
    pub struct BalanceRequest {
        pub account: Option<Vec<Bytes>>,
        pub transaction: bool,
        pub transaction_instructions: bool,
    }
}


impl BalanceRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("account", self.account.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_instructions {
            scan.join(
                "instructions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct TokenBalanceRequest {
        pub account: Option<Vec<Bytes>>,
        pub pre_mint: Option<Vec<Bytes>>,
        pub post_mint: Option<Vec<Bytes>>,
        pub pre_program_id: Option<Vec<Bytes>>,
        pub post_program_id: Option<Vec<Bytes>>,
        pub pre_owner: Option<Vec<Bytes>>,
        pub post_owner: Option<Vec<Bytes>>,
        pub transaction: bool,
        pub transaction_instructions: bool,
    }
}


impl TokenBalanceRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("account", self.account.clone());
        p.col_in_list("pre_mint", self.pre_mint.clone());
        p.col_in_list("post_mint", self.post_mint.clone());
        p.col_in_list("pre_program_id", self.pre_program_id.clone());
        p.col_in_list("post_program_id", self.post_program_id.clone());
        p.col_in_list("pre_owner", self.pre_owner.clone());
        p.col_in_list("post_owner", self.post_owner.clone());
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.transaction_instructions {
            scan.join(
                "instructions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
    }
}


request! {
    pub struct RewardRequest {
        pub pubkey: Option<Vec<Bytes>>,
    }
}


impl RewardRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("pubkey", self.pubkey.clone());
    }

    fn relations(&self, _scan: &mut ScanBuilder) {}
}


request! {
    pub struct SolanaQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
        pub instructions: Vec<InstructionRequest>,
        pub logs: Vec<LogRequest>,
        pub balances: Vec<BalanceRequest>,
        pub token_balances: Vec<TokenBalanceRequest>,
        pub rewards: Vec<RewardRequest>,
    }
}


impl SolanaQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions, instructions, logs, balances, token_balances, rewards);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            [instructions: self.fields.instruction.project()],
            [logs: self.fields.log.project()],
            [balances: self.fields.balance.project()],
            [token_balances: self.fields.token_balance.project()],
            [rewards: self.fields.reward.project()],
            transactions,
            instructions,
            logs,
            balances,
            token_balances,
            rewards,
        )
    }
}