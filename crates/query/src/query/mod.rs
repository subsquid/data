use crate::plan::Plan;
use crate::primitives::BlockNumber;
use serde::{Deserialize, Serialize};


pub mod eth;
pub mod solana;
pub mod substrate;
pub mod fuel;
mod util;


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Query {
    #[serde(rename = "evm")]
    Eth(eth::EthQuery),
    #[serde(rename = "solana")]
    Solana(solana::SolanaQuery),
    #[serde(rename = "substrate")]
    Substrate(substrate::SubstrateQuery),
    #[serde(rename = "fuel")]
    Fuel(fuel::FuelQuery)
}


impl Query {
    pub fn from_json_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let query: Self = serde_json::from_slice(bytes)?;
        query.validate()?;
        Ok(query)
    }
    
    pub fn from_json_value(json: serde_json::Value) -> anyhow::Result<Self> {
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
            Query::Solana(q) => q.validate(),
            Query::Substrate(q) => q.validate(),
            Query::Fuel(q) => q.validate(),
        }
    }

    pub fn first_block(&self) -> BlockNumber {
        match self {
            Query::Eth(q) => q.from_block,
            Query::Solana(q) => q.from_block,
            Query::Substrate(q) => q.from_block,
            Query::Fuel(q) => q.from_block,
        }
    }

    pub fn set_first_block(&mut self, block_number: BlockNumber) {
        match self {
            Query::Eth(q) => q.from_block = block_number,
            Query::Solana(q) => q.from_block = block_number,
            Query::Substrate(q) => q.from_block = block_number,
            Query::Fuel(q) => q.from_block = block_number,
        }
    }

    pub fn last_block(&self) -> Option<BlockNumber> {
        match self {
            Query::Eth(q) => q.to_block,
            Query::Solana(q) => q.to_block,
            Query::Substrate(q) => q.to_block,
            Query::Fuel(q) => q.to_block,
        }
    }

    pub fn set_last_block(&mut self, block_number: impl Into<Option<BlockNumber>>) {
        let block_number = block_number.into();
        match self {
            Query::Eth(q) => q.to_block = block_number,
            Query::Solana(q) => q.to_block = block_number,
            Query::Substrate(q) => q.to_block = block_number,
            Query::Fuel(q) => q.to_block = block_number,
        }
    }

    pub fn compile(&self) -> Plan {
        match self {
            Query::Eth(q) => q.compile(),
            Query::Solana(q) => q.compile(),
            Query::Substrate(q) => q.compile(),
            Query::Fuel(q) => q.compile(),
        }
    }
}