use anyhow::{Context, ensure};
use crate::db::data::{Chunk, ChunkId, DatasetId};
use crate::kv::KvReadCursor;
use sqd_primitives::BlockNumber;


pub fn list_chunks<C: KvReadCursor>(
    mut cursor: C,
    dataset_id: DatasetId,
    from_block: BlockNumber
) -> impl Iterator<Item=anyhow::Result<Chunk>>
{
    let first_chunk_id = ChunkId::new(dataset_id, from_block);

    let mut first_seek = true;

    let mut next = move || -> anyhow::Result<Option<Chunk>> {
        if first_seek {
            cursor.seek(first_chunk_id.as_ref())?;
            first_seek = false
        } else {
            cursor.next()?;
        }
        
        read_current_chunk(&cursor, dataset_id)
    };

    std::iter::from_fn(move || next().transpose())
}


pub fn read_current_chunk(
    cursor: &impl KvReadCursor,
    dataset_id: DatasetId
) -> anyhow::Result<Option<Chunk>>
{
    if !cursor.is_valid() {
        return Ok(None)
    }

    let current_id: ChunkId = borsh::from_slice(cursor.key())?;
    if current_id.dataset_id() != dataset_id {
        return Ok(None)
    }

    let chunk: Chunk = borsh::from_slice(cursor.value())
        .with_context(|| {
            format!("failed to deserialize a chunk {}", current_id)
        })?;

    validate_chunk(&current_id, &chunk)?;
    
    Ok(Some(chunk))
}


fn validate_chunk(chunk_id: &ChunkId, chunk: &Chunk) -> anyhow::Result<()> {
    ensure!(
        chunk_id.last_block() == chunk.last_block,
        "chunk {} has unexpected last block - {}",
        chunk_id,
        chunk.last_block
    );
    ensure!(
        chunk.first_block <= chunk.last_block,
        "chunk {} is invalid: last_block = {} is less than first_block = {}",
        chunk_id,
        chunk.first_block,
        chunk.last_block
    );
    Ok(())
}
