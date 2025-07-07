use sqd_primitives::BlockRef;
use crate::block::BlockArc;
use crate::cassandra::CassandraStorage;
use crate::ingest::Store;


impl Store for CassandraStorage {
    type Block = BlockArc;

    fn max_pending_writes(&self) -> usize {
        todo!()
    }

    async fn compute_fork(&self, prev: &[BlockRef]) -> anyhow::Result<Option<usize>> {
        todo!()
    }

    async fn save(&self, block: Self::Block) -> anyhow::Result<Self::Block> {
        todo!()
    }
}