use std::sync::Arc;
use sqd_data::solana::tables::SolanaChunkBuilder;
use sqd_primitives::{BlockNumber, short_hash, ShortHash};
use sqd_storage::db::{Database, DatasetId, NewChunk};
use crate::api::error::ApiError;
use crate::dataset_kind::DatasetKind;


enum Builder {
    Solana(SolanaChunkBuilder)
}


pub struct DataPush {
    db: Arc<Database>,
    dataset_id: DatasetId,
    builder: Builder,
    prev_block_hash: ShortHash,
    first_block: BlockNumber,
    last_block: BlockNumber,
    last_block_hash: ShortHash,
    blocks_buffered: usize,
    line: usize
}


impl DataPush {
    pub fn new(db: Arc<Database>, dataset_id: DatasetId, dataset_kind: DatasetKind) -> Self {
        Self {
            db,
            dataset_id,
            builder: match dataset_kind {
                DatasetKind::Eth => panic!("eth datasets are not supported"),
                DatasetKind::Solana => Builder::Solana(SolanaChunkBuilder::default())
            },
            prev_block_hash: ShortHash::default(),
            first_block: 0,
            last_block: 0,
            last_block_hash: ShortHash::default(),
            blocks_buffered: 0,
            line: 1
        }
    }
    
    pub fn push(&mut self, line: &str) -> Result<(), ApiError> {
        macro_rules! push {
            ($b:expr, $m:ident) => {{
                use sqd_data::$m::model::Block;
                let block: Block = serde_json::from_str(line).map_err(|err| ApiError::UserError(
                    format!("failed to parse block at line {}: {}", self.line, err)
                ))?;
                $b.push(&block);
                (
                    block.header.height,
                    short_hash(&block.header.hash),
                    short_hash(&block.header.parent_hash)
                )
            }};
        }

        let (block_number, block_hash, parent_hash) = match &mut self.builder {
            Builder::Solana(b) => push!(b, solana)
        };

        if self.blocks_buffered == 0 {
            self.prev_block_hash = parent_hash;
            self.first_block = block_number;
            self.last_block = block_number;
            self.last_block_hash = block_hash;
        } else {
            if self.last_block >= block_number {
                return Err(ApiError::UserError(
                    format!("block order was violated: {} follows {}", block_number, self.last_block)
                ))
            }
            self.last_block = block_number;
            self.last_block_hash = block_hash;
        }

        self.line += 1;
        self.blocks_buffered += 1;

        if self.blocks_buffered >= 50 {
            self.write_chunk()?;
        }

        Ok(())
    }
    
    pub fn flush(&mut self) -> Result<(), ApiError> {
        if self.blocks_buffered > 0 {
            self.write_chunk()
        } else {
            Ok(())
        }
    }

    fn write_chunk(&mut self) -> Result<(), ApiError> {
        self.blocks_buffered = 0;
        
        let (dataset_description, tables) = match &mut self.builder {
            Builder::Solana(b) => {
                (SolanaChunkBuilder::dataset_description(), b.finish())
            }
        };

        let chunk_builder = self.db.new_chunk_builder(dataset_description);

        for (name, table) in tables {
            let mut writer = chunk_builder.add_table(name, table.schema)?;
            for record_batch in table.rows {
                writer.write_record_batch(&record_batch)?;
            }
            writer.finish()?;
        }

        let chunk_tables = chunk_builder.finish();

        let new_chunk = NewChunk {
            prev_block_hash: Some(self.prev_block_hash),
            first_block: self.first_block,
            last_block: self.last_block,
            last_block_hash: self.last_block_hash,
            tables: chunk_tables
        };
        
        self.db.insert_chunk(self.dataset_id, new_chunk)?;

        Ok(())
    }
}