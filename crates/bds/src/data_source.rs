use crate::block::{Block, BlockArc, BlockHeader};
use anyhow::ensure;
use bytes::Bytes;
use futures::StreamExt;
use serde::Deserialize;
use sqd_data_client::reqwest::ReqwestDataClient;
use sqd_data_source::{DataSource, MappedDataSource, StandardDataSource};
use sqd_primitives::BlockNumber;
use std::io::Write;
use std::sync::Arc;


#[derive(Deserialize)]
struct JsonBlock {
    header: JsonBlockHeader
}


#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JsonBlockHeader {
    number: BlockNumber,
    hash: String,
    parent_hash: String,
    parent_number: Option<BlockNumber>,
    timestamp: Option<i64>
}



#[derive(Debug)]
struct ParsedBlock {
    pub header: BlockHeader<'static>,
    pub data: Bytes
}



fn parse_block(bytes: Bytes) -> anyhow::Result<ParsedBlock> {
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
    
    Ok(ParsedBlock {
        header,
        data: bytes
    })
}


pub fn create_data_source(clients: Vec<ReqwestDataClient>) -> impl DataSource<Block = BlockArc> {
    MappedDataSource::new(
        StandardDataSource::new(clients, &parse_block),
        |mut parsed_block: ParsedBlock, is_final: bool| {
            let compressed = {
                use flate2::*;
                use flate2::write::GzEncoder;
                let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
                encoder.write_all(&parsed_block.data);
                encoder.finish().expect("IO errors are not possible")
            };
            parsed_block.header.is_final = is_final;
            Arc::new(Block {
                header: parsed_block.header,
                data: compressed.into()
            })
        }
    )
}


impl sqd_primitives::Block for ParsedBlock {
    fn number(&self) -> BlockNumber {
        self.header.number
    }

    fn hash(&self) -> &str {
        &self.header.hash
    }

    fn parent_number(&self) -> BlockNumber {
        self.header.parent_number
    }

    fn parent_hash(&self) -> &str {
        &self.header.parent_hash
    }

    fn timestamp(&self) -> Option<i64> {
        self.header.timestamp
    }
}