use super::util::{check_hex, compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, parse_hex, parse_static_hex, request, PredicateBuilder};
use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{Plan, ScanBuilder, TableSet};
use crate::primitives::BlockNumber;
use crate::scan::{col_in_list, or, RowPredicateRef};
use anyhow::{anyhow, ensure};
use arrow::array::{ArrayRef, FixedSizeBinaryBuilder, UInt16Array, UInt32Array, UInt64Array, UInt8Array};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, LazyLock};


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
        "pubkey",
        "reward_type"
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

    project(this) json_object! {
        {
            this.transaction_index,
        },
        {
            [this.version]: SolanaTransactionVersion,
        },
        {
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
        }
    }
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
        pub discriminator: Option<Vec<Bytes>>,
        pub d1: Option<Vec<Bytes>>,
        pub d2: Option<Vec<Bytes>>,
        pub d4: Option<Vec<Bytes>>,
        pub d8: Option<Vec<Bytes>>,
        pub mentions_account: Option<Vec<Bytes>>,
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
        pub transaction_balances: bool,
        pub transaction_token_balances: bool,
        pub transaction_instructions: bool,
        pub inner_instructions: bool,
        pub parent_instructions: bool,
        pub logs: bool,
    }
}


impl InstructionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("program_id", self.program_id.as_deref());
        self.discriminator_predicate(p);
        p.col_in_list("d1", self.d1.as_ref().map(|list| list.iter().filter_map(|s| parse_static_hex::<1>(s)).map(u8::from_be_bytes).collect::<Vec<_>>()));
        p.col_in_list("d2", self.d2.as_ref().map(|list| list.iter().filter_map(|s| parse_static_hex::<2>(s)).map(u16::from_be_bytes).collect::<Vec<_>>()));
        p.col_in_list("d4", self.d4.as_ref().map(|list| list.iter().filter_map(|s| parse_static_hex::<4>(s)).map(u32::from_be_bytes).collect::<Vec<_>>()));
        p.col_in_list("d8", self.d8.as_ref().map(|list| list.iter().filter_map(|s| parse_static_hex::<8>(s)).map(u64::from_be_bytes).collect::<Vec<_>>()));
        p.bloom_filter("accounts_bloom", 64, 7, self.mentions_account.as_deref());
        p.col_in_list("a0", self.a0.as_deref());
        p.col_in_list("a1", self.a1.as_deref());
        p.col_in_list("a2", self.a2.as_deref());
        p.col_in_list("a3", self.a3.as_deref());
        p.col_in_list("a4", self.a4.as_deref());
        p.col_in_list("a5", self.a5.as_deref());
        p.col_in_list("a6", self.a6.as_deref());
        p.col_in_list("a7", self.a7.as_deref());
        p.col_in_list("a8", self.a8.as_deref());
        p.col_in_list("a9", self.a9.as_deref());
        p.col_in_list("a10", self.a10.as_deref());
        p.col_in_list("a11", self.a11.as_deref());
        p.col_in_list("a12", self.a12.as_deref());
        p.col_in_list("a13", self.a13.as_deref());
        p.col_in_list("a14", self.a14.as_deref());
        p.col_in_list("a15", self.a15.as_deref());
        p.col_eq("is_committed", self.is_committed);
    }

    fn discriminator_predicate(&self, p: &mut PredicateBuilder) {
        let Some(list) = self.discriminator.as_ref() else { return };

        let list: Vec<Vec<u8>> = list.iter().filter_map(|s| {
            let d = parse_hex(s)?;
            if d.len() > 16 {
                None
            } else {
                Some(d)
            }
        }).collect();

        if list.is_empty()  {
            p.mark_as_never();
            return
        }

        if list.iter().any(|d| d.is_empty()) {
            // empty prefix always matches
            return
        }

        let mut ds: Vec<Vec<Vec<u8>>> = vec![vec![]; 17];
        for d in list {
            ds[d.len()].push(d);
        }

        let mut predicates: Vec<RowPredicateRef> = Vec::new();

        for (i, list) in ds.into_iter().enumerate() {
            if list.is_empty() {
                continue
            }

            macro_rules! disc {
                ($t:ty, $array:ty) => {
                    Arc::new(<$array>::from_iter_values(
                        list.into_iter().map(|d| <$t>::from_be_bytes(d.try_into().unwrap()))
                    ))
                };
            }

            let array: ArrayRef = match i {
                1 => disc!(u8, UInt8Array),
                2 => disc!(u16, UInt16Array),
                4 => disc!(u32, UInt32Array),
                8 => disc!(u64, UInt64Array),
                _ => {
                    let mut builder = FixedSizeBinaryBuilder::with_capacity(list.len(), i as i32);
                    for d in list.iter() {
                        builder.append_value(d).unwrap();
                    }
                    Arc::new(builder.finish())
                }
            };

            let col = match i {
                1 => "d1",
                2 => "d2",
                3 => "d3",
                4 => "d4",
                5 => "d5",
                6 => "d6",
                7 => "d7",
                8 => "d8",
                9 => "d9",
                10 => "d10",
                11 => "d11",
                12 => "d12",
                13 => "d13",
                14 => "d14",
                15 => "d15",
                16 => "d16",
                _ => unreachable!()
            };

            predicates.push(
                col_in_list(col, array)
            )
        }

        p.add(or(predicates));
    }

    fn relations(&self, scan: &mut ScanBuilder) {
        if self.transaction {
            scan.join(
                "transactions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"],
            );
        }
        if self.transaction_balances {
            scan.join(
                "balances",
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
        if self.transaction_instructions {
            scan.join(
                "instructions",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.inner_instructions {
            scan.include_children();
        }
        if self.parent_instructions {
            scan.include_parents();
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
        pub mentions_account: Option<Vec<Bytes>>,
        pub instructions: bool,
        pub logs: bool,
        pub balances: bool,
        pub token_balances: bool,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("fee_payer", self.fee_payer.as_deref());
        p.bloom_filter("accounts_bloom", 64, 7, self.mentions_account.as_deref());
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
        if self.balances {
            scan.join(
                "balances",
                vec!["block_number", "transaction_index"],
                vec!["block_number", "transaction_index"]
            );
        }
        if self.token_balances {
            scan.join(
                "token_balances",
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
        p.col_in_list("program_id", self.program_id.as_deref());
        p.col_in_list("kind", self.kind.as_deref());
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
        p.col_in_list("account", self.account.as_deref());
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
        pub transaction_balances: bool,
        pub transaction_token_balances: bool,
    }
}


impl TokenBalanceRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("account", self.account.as_deref());
        p.col_in_list("pre_mint", self.pre_mint.as_deref());
        p.col_in_list("post_mint", self.post_mint.as_deref());
        p.col_in_list("pre_program_id", self.pre_program_id.as_deref());
        p.col_in_list("post_program_id", self.post_program_id.as_deref());
        p.col_in_list("pre_owner", self.pre_owner.as_deref());
        p.col_in_list("post_owner", self.post_owner.as_deref());
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
        if self.transaction_balances {
            scan.join(
                "balances",
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
    }
}


request! {
    pub struct RewardRequest {
        pub pubkey: Option<Vec<Bytes>>,
    }
}


impl RewardRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("pubkey", self.pubkey.as_deref());
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
        for (i, tx) in self.transactions.iter().enumerate() {
            let len = tx.mentions_account.as_ref().map_or(0, |list| list.len());
            ensure!(
                len <= 10,
                "'.transactions[{}].mentionsAccount' filter has {} cases, but maximum is 10",
                i,
                len
            )
        }
        for (i, ins) in self.instructions.iter().enumerate() {
            let len = ins.mentions_account.as_ref().map_or(0, |list| list.len());
            ensure!(
                len <= 10,
                "'.instructions[{}].mentionsAccount' filter has {} cases, but maximum is 10",
                i,
                len
            );

            let mut discriminator_filters: Vec<&'static str> = Vec::new();
            if ins.d1.is_some() {
                discriminator_filters.push("d1")
            }
            if ins.d2.is_some() {
                discriminator_filters.push("d2")
            }
            if ins.d4.is_some() {
                discriminator_filters.push("d4")
            }
            if ins.d8.is_some() {
                discriminator_filters.push("d8")
            }
            if ins.discriminator.is_some() {
                discriminator_filters.push("discriminator")
            }
            ensure!(
                discriminator_filters.len() < 2,
                "invalid instruction request at '.instructions[{}]': '{}' and '{}' filters can't be specified simultaneously",
                i,
                discriminator_filters[0],
                discriminator_filters[1]
            );

            if let Some(ds) = ins.discriminator.as_ref() {
                for (dix, d) in ds.iter().enumerate() {
                    check_hex(d).and_then(|_| {
                        if d.len() > 34 {
                            Err("discriminator can't be longer than 16 bytes")
                        } else {
                            Ok(())
                        }
                    }).map_err(|msg| anyhow!(
                        "invalid discriminator at .instructions[{}].discriminator[{}]: {}",
                        i, dix, msg
                    ))?;
                }
            }
        }
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