use serde::{Deserialize, Serialize};
use crate::plan::Plan;
use crate::primitives::BlockNumber;

pub mod eth;
pub mod solana;
mod util;


#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Query {
    #[serde(rename = "eth")]
    Eth(eth::EthQuery),
    #[serde(rename = "solana")]
    Solana(solana::SolanaQuery)
}


impl Query {
    pub fn from_json_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let json: serde_json::Value = serde_json::from_slice(bytes)?;
        Self::from_json_value(json)
    }
    
    pub fn from_json_value(mut json: serde_json::Value) -> anyhow::Result<Self> {
        if let Some(m) = json.as_object_mut() {
            if !m.contains_key("type") {
                m.insert("type".to_string(), serde_json::Value::String("eth".to_string()));
            }
        }
        let query: Self = serde_json::from_value(json)?;
        query.validate()?;
        Ok(query)
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Query::Eth(q) => q.validate(),
            Query::Solana(q) => q.validate()
        }
    }

    pub fn first_block(&self) -> Option<BlockNumber> {
        match self {
            Query::Eth(q) => q.from_block,
            Query::Solana(q) => q.from_block
        }
    }

    pub fn last_block(&self) -> Option<BlockNumber> {
        match self {
            Query::Eth(q) => q.to_block,
            Query::Solana(q) => q.to_block
        }
    }

    pub fn compile(&self) -> Plan {
        match self {
            Query::Eth(q) => q.compile(),
            Query::Solana(q) => q.compile()
        }
    }
}