use crate::db::data::{Chunk, ChunkId, DatasetId};
use crate::kv::KvReadCursor;
use anyhow::{ensure, Context};
use sqd_primitives::BlockNumber;


pub struct ChunkIterator<C> {
    cursor: C,
    dataset_id: DatasetId,
    from_block: BlockNumber,
    to_block: Option<BlockNumber>,
    first_seek: bool,
    is_reversed: bool
}


impl<C: KvReadCursor> ChunkIterator<C> {
    pub fn new(
        cursor: C, 
        dataset_id: DatasetId, 
        from_block: BlockNumber, 
        to_block: Option<BlockNumber>
    ) -> Self 
    {
        Self {
            cursor,
            dataset_id,
            from_block,
            to_block,
            first_seek: true,
            is_reversed: false
        }
    }
    
    pub fn into_reversed(self) -> Self {
        Self {
            cursor: self.cursor,
            dataset_id: self.dataset_id,
            from_block: self.from_block,
            to_block: self.to_block,
            first_seek: true,
            is_reversed: !self.is_reversed
        }
    }
    
    fn chunk_id(&self, last_block: BlockNumber) -> ChunkId {
        ChunkId::new(self.dataset_id, last_block)
    }

    fn seek_first(&mut self) -> anyhow::Result<()> {
        let min_chunk_id = self.chunk_id(self.from_block);
        self.cursor.seek(min_chunk_id.as_ref())
    }

    fn seek_last(&mut self) -> anyhow::Result<()> {
        if let Some(to_block) = self.to_block {
            self.cursor.seek_prev(
                self.chunk_id(to_block).as_ref()
            )
        } else {
            self.cursor.seek_prev(
                self.chunk_id(BlockNumber::MAX).as_ref()
            )
        }
    }

    fn next_chunk(&mut self) -> anyhow::Result<Option<Chunk>> {
        if self.first_seek {
            if self.is_reversed {
                self.seek_last()?;
            } else {
                self.seek_first()?;
            }
            self.first_seek = false;
        } else {
            if self.is_reversed {
                self.cursor.prev()?;
            } else {
                self.cursor.next()?;
            }
        }

        if !self.cursor.is_valid() {
            return Ok(None)
        }

        let current_id: ChunkId = borsh::from_slice(self.cursor.key())?;
        if current_id.dataset_id() != self.dataset_id {
            return Ok(None)
        }

        let chunk: Chunk = borsh::from_slice(self.cursor.value()).with_context(|| {
            format!("failed to deserialize chunk {}", current_id)
        })?;

        validate_chunk(&current_id, &chunk)?;
        
        if self.from_block <= chunk.last_block() && self.to_block.map_or(true, |end| chunk.first_block() <= end) {
            return Ok(Some(chunk))
        }

        Ok(None)
    }
}


fn validate_chunk(chunk_id: &ChunkId, chunk: &Chunk) -> anyhow::Result<()> {
    ensure!(
        chunk_id.last_block() == chunk.last_block(),
        "chunk {} has unexpected last block - {}",
        chunk_id,
        chunk.last_block()
    );
    ensure!(
        chunk.first_block() <= chunk.last_block(),
        "chunk {} is invalid: last_block = {} is less than first_block = {}",
        chunk_id,
        chunk.first_block(),
        chunk.last_block()
    );
    Ok(())
}


impl<C: KvReadCursor> Iterator for ChunkIterator<C> {
    type Item = anyhow::Result<Chunk>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.next_chunk().transpose()
    }
}
