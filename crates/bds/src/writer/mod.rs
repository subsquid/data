use crate::cassandra::CassandraStorage;
use sqd_primitives::{BlockNumber, BlockRef};


pub struct Writer {
    storage: CassandraStorage,
    finalized_head: Option<BlockRef>,
    first_block: BlockNumber,
    parent_block_hash: Option<String>
}