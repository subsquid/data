use crate::block::{Block, BlockArc, BlockHeader};
use anyhow::ensure;
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_data_source::{DataSource, StandardDataSource};
use sqd_primitives::BlockNumber;
use std::sync::Arc;


#[derive(Deserialize)]
struct JsonBlock {
    header: JsonBlockHeader
}


#[derive(Deserialize)]
struct JsonBlockHeader {
    number: BlockNumber,
    hash: String,
    parent_hash: String,
    parent_number: Option<BlockNumber>,
    timestamp: Option<i64>
}


fn parse_block(bytes: Bytes) -> anyhow::Result<BlockArc> {
    let json: JsonBlock = serde_json::from_slice(&bytes)?;
    
    if let Some(parent_number) = json.header.parent_number {
        ensure!(parent_number < json.header.number || json.header.number == 0);    
    }
    
    let header = BlockHeader::<'static> {
        number: json.header.number,
        hash: json.header.hash.into(),
        parent_number: json.header.parent_number.unwrap_or_else(|| json.header.number.saturating_sub(1)),
        parent_hash: json.header.parent_hash.into(),
        timestamp: json.header.timestamp,
        is_final: false
    };
    
    let block = Block {
        header,
        data: bytes.into()
    };
    
    Ok(Arc::new(block))
}


pub type ReqwestDataSource = StandardDataSource<
    ReqwestDataClient, 
    &'static (dyn (Fn(Bytes) -> anyhow::Result<BlockArc>) + Sync)
>;


pub fn create_data_source(clients: Vec<ReqwestDataClient>) -> ReqwestDataSource {
    StandardDataSource::new(clients, &parse_block)
}