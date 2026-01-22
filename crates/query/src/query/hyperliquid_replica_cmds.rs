use crate::json::exp::Exp;
use crate::json::lang::*;
use crate::plan::{ScanBuilder, TableSet};
use crate::query::util::{compile_plan, ensure_block_range, ensure_item_count, field_selection, item_field_selection, request, PredicateBuilder};
use crate::{BlockNumber, Plan};
use arrow::datatypes::UInt32Type;
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;


static TABLES: LazyLock<TableSet> = LazyLock::new(|| {
    let mut tables = TableSet::new();

    tables.add_table("blocks", vec![
        "number"
    ]);

    tables.add_table("actions", vec![
        "block_number",
        "action_index"
    ])
    .set_weight_column("action", "action_size")
    .set_weight_column("response", "response_size");

    tables
});


field_selection! {
    block: BlockFieldSelection,
    action: ActionFieldSelection,
}


item_field_selection! {
    BlockFieldSelection {
        number,
        hash,
        parent_hash,
        round,
        parent_round,
        proposer,
        timestamp,
        hardfork,
    }

    project(this) json_object! {{
        this.number,
        this.hash,
        this.parent_hash,
        this.round,
        this.parent_round,
        this.proposer,
        this.timestamp,
        [this.hardfork]: Json,
    }}
}


item_field_selection! {
    ActionFieldSelection {
        action_index,
        user,
        action,
        signature,
        nonce,
        vault_address,
        status,
        response,
    }

    project(this) json_object! {{
        this.action_index,
        this.user,
        [this.action]: Json,
        [this.signature]: Json,
        [this.nonce]: Value,
        [this.vault_address]: Value,
        [this.status]: Value,
        [this.response]: Json,
    }}
}


type Bytes = String;
type AssetIndex = u32;


#[derive(Deserialize, Serialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Err,
}


request! {
    pub struct ActionRequest {
        pub action_type: Option<Vec<String>>,
        pub user: Option<Vec<Bytes>>,
        pub vault_address: Option<Vec<Bytes>>,
        pub status: Option<Status>,
    }
}


impl ActionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_in_list("action_type", self.action_type.as_deref());
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("vault_address", self.vault_address.as_deref());
        p.col_eq("status", self.status.as_ref().map(|val| match val {
            Status::Ok => "ok",
            Status::Err => "err"
        }));
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct OrderActionRequest {
        pub contains_asset: Option<Vec<AssetIndex>>,
        pub contains_cloid: Option<Vec<Bytes>>,
        pub user: Option<Vec<Bytes>>,
        pub vault_address: Option<Vec<Bytes>>,
        pub status: Option<Status>,
    }
}


impl OrderActionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_primitive_list_contains_any::<UInt32Type>("order_asset", self.contains_asset.as_deref());
        p.col_string_list_contains_any("order_cloid", self.contains_cloid.as_deref());
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("vault_address", self.vault_address.as_deref());
        p.col_eq("status", self.status.as_ref().map(|val| match val {
            Status::Ok => "ok",
            Status::Err => "err"
        }));
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct CancelActionRequest {
        pub contains_asset: Option<Vec<AssetIndex>>,
        pub user: Option<Vec<Bytes>>,
        pub vault_address: Option<Vec<Bytes>>,
        pub status: Option<Status>,
    }
}


impl CancelActionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_primitive_list_contains_any::<UInt32Type>("cancel_asset", self.contains_asset.as_deref());
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("vault_address", self.vault_address.as_deref());
        p.col_eq("status", self.status.as_ref().map(|val| match val {
            Status::Ok => "ok",
            Status::Err => "err"
        }));
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct CancelByCloidActionRequest {
        pub contains_asset: Option<Vec<AssetIndex>>,
        pub contains_cloid: Option<Vec<Bytes>>,
        pub user: Option<Vec<Bytes>>,
        pub vault_address: Option<Vec<Bytes>>,
        pub status: Option<Status>,
    }
}


impl CancelByCloidActionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_primitive_list_contains_any::<UInt32Type>("asset", self.contains_asset.as_deref());
        p.col_string_list_contains_any("cloid", self.contains_cloid.as_deref());
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("vault_address", self.vault_address.as_deref());
        p.col_eq("status", self.status.as_ref().map(|val| match val {
            Status::Ok => "ok",
            Status::Err => "err"
        }));
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct BatchModifyActionRequest {
        pub contains_asset: Option<Vec<AssetIndex>>,
        pub contains_cloid: Option<Vec<Bytes>>,
        pub user: Option<Vec<Bytes>>,
        pub vault_address: Option<Vec<Bytes>>,
        pub status: Option<Status>,
    }
}


impl BatchModifyActionRequest {
    fn predicate(&self, p: &mut PredicateBuilder) {
        p.col_primitive_list_contains_any::<UInt32Type>("batch_modify_asset", self.contains_asset.as_deref());
        p.col_string_list_contains_any("batch_modify_cloid", self.contains_cloid.as_deref());
        p.col_in_list("user", self.user.as_deref());
        p.col_in_list("vault_address", self.vault_address.as_deref());
        p.col_eq("status", self.status.as_ref().map(|val| match val {
            Status::Ok => "ok",
            Status::Err => "err"
        }));
    }

    fn relations(&self, _scan: &mut ScanBuilder) { }
}


request! {
    pub struct HyperliquidReplicaCmdsQuery {
        pub from_block: BlockNumber,
        pub parent_block_hash: Option<String>,
        pub to_block: Option<BlockNumber>,
        pub fields: FieldSelection,
        pub include_all_blocks: bool,
        pub actions: Vec<ActionRequest>,
        pub order_actions: Vec<OrderActionRequest>,
        pub cancel_actions: Vec<CancelActionRequest>,
        pub cancel_by_cloid_actions: Vec<CancelByCloidActionRequest>,
        pub batch_modify_actions: Vec<BatchModifyActionRequest>,
    }
}


impl HyperliquidReplicaCmdsQuery {
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure_block_range!(self);
        ensure_item_count!(self, actions);
        Ok(())
    }

    pub fn compile(&self) -> Plan {
        compile_plan!(self, &TABLES,
            [blocks: self.fields.block.project()],
            [actions: self.fields.action.project()],
            actions,
            <order_actions: actions>,
            <cancel_actions: actions>,
            <cancel_by_cloid_actions: actions>,
            <batch_modify_actions: actions>,
        )
    }
}
