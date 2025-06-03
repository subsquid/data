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
    ]);

    tables.add_table("transactions", vec![
        "block_number",
        "transaction_index"
    ])
    .set_weight_column("actions", "actions_size");

    tables
});


field_selection! {
    block: BlockFieldSelection,
    transaction: TransactionFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        proposer,
        block_time,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.proposer,
        this.block_time,
    }}
}


item_field_selection! {
    TransactionFieldSelection {
        transaction_index,
        user,
        actions,
        raw_tx_hash,
        error,
    }

    project(this) json_object! {{
        this.transaction_index,
        this.user,
        [this.actions]: Json,
        [this.raw_tx_hash]: Value,
        [this.error]: Value,
    }}
}


type Bytes = String;


request! {
    pub struct TransactionRequest {
        pub user: Option<Vec<Bytes>>,
    }
}


impl TransactionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("user", self.user.as_deref());
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct HyperliquidQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub transactions: Vec<TransactionRequest>,
    }
}


impl HyperliquidQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, transactions);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [transactions: self.fields.transaction.project()],
            transactions,
        )
    }
}
