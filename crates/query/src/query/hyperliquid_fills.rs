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

    tables.add_table("fills", vec![
        "block_number",
        "fill_index"
    ]);

    tables
});


field_selection! {
    block: BlockFieldSelection,
    fill: FillFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        timestamp,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        this.timestamp,
    }}
}


item_field_selection! {
    FillFieldSelection {
        fill_index,
        user,
        coin,
        px,
        sz,
        side,
        time,
        start_position,
        dir,
        closed_pnl,
        hash,
        oid,
        crossed,
        fee,
        builder_fee,
        tid,
        cloid,
        fee_token,
        builder,
        twap_id,
    }

    project(this) json_object! {{
        this.fill_index,
        this.user,
        this.coin,
        this.px,
        this.sz,
        this.side,
        this.time,
        this.start_position,
        this.dir,
        this.closed_pnl,
        this.hash,
        this.oid,
        this.crossed,
        this.fee,
        this.builder_fee,
        this.tid,
        this.cloid,
        this.fee_token,
        this.builder,
        this.twap_id,
    }}
}


type Bytes = String;


request! {
    pub struct FillRequest {
        pub user: Option<Vec<Bytes>>,
        pub coin: Option<Vec<String>>,
        pub dir: Option<Vec<String>>,
        pub cloid: Option<Vec<Bytes>>,
        pub fee_token: Option<Vec<String>>,
        pub builder: Option<Vec<Bytes>>,
    }
}


impl FillRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("coin", self.coin.as_deref());
        p.col_in_list("dir", self.dir.as_deref());
        p.col_in_list("cloid", self.cloid.as_deref());
        p.col_in_list("fee_token", self.fee_token.as_deref());
        p.col_in_list("builder", self.builder.as_deref());
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct HyperliquidFillsQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub fills: Vec<FillRequest>,
    }
}


impl HyperliquidFillsQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, fills);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [fills: self.fields.fill.project()],
            fills,
        )
    }
}
