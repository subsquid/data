use super::row_batch::Row;
use crate::block::{Block, BlockHeader};
use anyhow::Context;
use sqd_primitives::{BlockNumber, BlockRef};
use uuid::Uuid;


impl Row for BlockHeader<'static> {
    type Type<'a> = BlockHeader<'a>;
    
    type Tuple<'a> = (
        i64, // number
        &'a str, // hash
        i64, // parent_number
        &'a str, // parent_hash
        Option<i64>, // block_timestamp,
        Option<bool> // is_final
    );

    fn convert(row: Self::Tuple<'_>) -> anyhow::Result<Self::Type<'_>> {
        let number: BlockNumber = row.0.try_into().context("got negative block number")?;
        let parent_number: BlockNumber = row.2.try_into().context("got negative parent block number")?;
        Ok(BlockHeader {
            number,
            hash: row.1.into(),
            parent_number,
            parent_hash: row.3.into(),
            timestamp: row.4,
            is_final: row.5.unwrap_or(false)
        })
    }

    fn reborrow<'a, 'this>(slice: &'a [Self::Type<'this>]) -> &'a [Self::Type<'a>] {
        slice
    }
}


impl Row for Block<'static> {
    type Type<'a> = Block<'a>;
    
    type Tuple<'a> = (<BlockHeader<'static> as Row>::Tuple<'a>, &'a [u8]);

    fn convert(row: Self::Tuple<'_>) -> anyhow::Result<Self::Type<'_>> {
        let header = BlockHeader::convert(row.0)?;
        Ok(Block {
            header,
            data: row.1.into()
        })
    }

    fn reborrow<'a, 'this>(slice: &'a [Self::Type<'this>]) -> &'a [Self::Type<'a>] {
        slice
    }
}


pub struct WriteState {
    pub id: Uuid,
    pub head: BlockRef,
    pub finalized_head: Option<BlockRef>
}