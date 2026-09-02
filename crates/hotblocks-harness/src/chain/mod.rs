//! Kind-parametric payloads (DEF-5): what the source serves, what a client asks for, what the
//! service must emit back. Everything above this module is kind-agnostic.

mod evm;
mod hl_fills;
mod solana;

pub use evm::Evm;
pub use hl_fills::HlFills;
use serde_json::Value;
pub use solana::Solana;

use crate::types::{Block, BlockNumber};

pub trait Chain: Send + Sync + 'static {
    /// The `kind` value of the dataset config (YAML).
    fn config_kind(&self) -> &'static str;

    /// The kind string the service reports back in STATUS, which is *not* always the config one
    /// (`hyperliquid-fills` in, `hl-fills` out).
    fn storage_kind(&self) -> &'static str;

    /// The query dialect tag — the `type` field of a query body (IB-3).
    fn dialect(&self) -> &'static str;

    /// One JSONL record of the source's `/stream` response (DEF-12 `Blocks`, IB §7).
    fn source_block(&self, b: &Block) -> Value;

    /// A full-window scan. It sets `include_all`, so the last emitted block *is* the coverage
    /// end `L` (RP-9) and the scanner can always advance; a filter-sparse query cannot (GAP-8).
    fn scan_query(&self, from: BlockNumber, to: Option<BlockNumber>, expected_parent: Option<&str>) -> Value;

    /// What the service must emit for `b` in answer to [`Chain::scan_query`] — the INV-22 oracle.
    fn expected_emission(&self, b: &Block) -> Value;
}

/// Assemble a scan query out of the parts every dialect shares.
fn scan_query(
    dialect: &str,
    from: BlockNumber,
    to: Option<BlockNumber>,
    expected_parent: Option<&str>,
    fields: Value,
    items: Vec<(&str, Value)>
) -> Value {
    let mut q = serde_json::json!({
        "type": dialect,
        "fromBlock": from,
        "includeAllBlocks": true,
        "fields": fields
    });
    let o = q.as_object_mut().expect("query is an object");
    if let Some(to) = to {
        o.insert("toBlock".into(), serde_json::json!(to));
    }
    if let Some(parent) = expected_parent {
        o.insert("parentBlockHash".into(), serde_json::json!(parent));
    }
    for (name, request) in items {
        o.insert(name.into(), request);
    }
    q
}

/// An emitted block: the header, plus each item collection that has items. The service omits an
/// empty collection rather than emitting `[]`.
fn emission(header: Value, collections: Vec<(&str, Vec<Value>)>) -> Value {
    let mut block = serde_json::json!({ "header": header });
    let o = block.as_object_mut().expect("block is an object");
    for (name, items) in collections {
        if !items.is_empty() {
            o.insert(name.into(), Value::Array(items));
        }
    }
    block
}
