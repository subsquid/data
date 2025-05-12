use crate::storage::{Block, Storage};
use anyhow::Context;
use scylla::client::session::Session;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::Consistency;
use sqd_primitives::BlockNumber;
use std::sync::Arc;


mod row_batch;
mod types;


fn get_partition(block_number: BlockNumber) -> i64 {
    (block_number / 10) as i64 * 10
}


pub struct CassandraStorage {
    session: Arc<Session>,
    update_statement: PreparedStatement,
    fetch_statement: PreparedStatement
}


impl CassandraStorage {
    pub async fn new(session: Arc<Session>, keyspace: &str) -> anyhow::Result<Self> {
        let mut update_statement = session.prepare(format!(
            "UPDATE {}.blocks SET parent_number = ?, parent_hash = ?, block_timestamp = ?, ingest_timestamp = ingest_timestamp + ?, data = ? WHERE partition = ? AND number = ? AND hash = ?", 
            keyspace
        )).await?;
        update_statement.set_consistency(Consistency::Quorum);
        
        let mut fetch_statement = session.prepare(format!(
            "SELECT number, hash, parent_number, parent_hash, block_timestamp, ingest_timestamp, data FROM {}.blocks WHERE partition = ? AND number >= ? AND number <= ?",
            keyspace
        )).await?;
        fetch_statement.set_consistency(Consistency::One);
        fetch_statement.set_is_idempotent(true);

        Ok(Self {
            session,
            update_statement,
            fetch_statement
        })
    }
    
    pub async fn save_block(&self, block: &Block<'_>) -> anyhow::Result<()> {
        self.session.execute_unpaged(&self.update_statement, (
            block.header.parent_number as i64,
            block.header.parent_hash.as_ref(),
            block.header.block_timestamp,
            block.header.ingest_timestamp,
            block.data.as_ref(),
            get_partition(block.header.number),
            block.header.number as i64,
            block.header.hash.as_ref()
        )).await?;
        Ok(())
    }
    
    async fn list_blocks(&self, first_block: BlockNumber) {
        
    }
}