use std::io::{BufRead, BufReader};

use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

use crate::primitives::BlockNumber;


#[derive(Debug, Serialize, Deserialize)]
pub struct BlockRange {
    pub from: BlockNumber,
    pub to: Option<BlockNumber>
}


impl BlockRange {
    pub fn new(first_block: BlockNumber, last_block: Option<BlockNumber>) -> Self {
        Self {
            from: first_block,
            to: last_block
        }
    }

    pub fn is_valid(&self) -> bool {
        if let Some(to) = self.to {
            self.from <= to
        } else {
            true
        }
    }
}


pub fn ingest_from_service<T: DeserializeOwned + 'static>(
    url: &str,
    block_range: &BlockRange
) -> anyhow::Result<impl Iterator<Item = anyhow::Result<T>>>
{
    // todo: handle errors
    let response = ureq::post(url).send_json(block_range)?;
    let mut reader = BufReader::new(response.into_reader());
    let mut line = String::new();

    let mut next = move || -> anyhow::Result<Option<T>> {
        if 0 == reader.read_line(&mut line)? {
            return Ok(None)
        }
        let block: T = serde_json::from_str(&line)?;
        line.clear();
        Ok(Some(block))
    };

    Ok(std::iter::from_fn(move || next().transpose()))
}