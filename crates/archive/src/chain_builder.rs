use anyhow::ensure;
use sqd_data_core::{BlockChunkBuilder, ChunkBuilder};
use sqd_primitives::{Block, BlockNumber};


pub type ChainBuilderBox = Box<dyn AnyChainBuilder>;


pub trait AnyChainBuilder: Send + Sync {
    fn push(&mut self, json_block: &[u8]) -> anyhow::Result<()>;

    fn chunk_builder(&self) -> &dyn ChunkBuilder;

    fn chunk_builder_mut(&mut self) -> &mut dyn ChunkBuilder;

    fn last_block_number(&self) -> BlockNumber;

    fn last_block_hash(&self) -> &str;

    fn last_parent_block_number(&self) -> BlockNumber;

    fn last_parent_block_hash(&self) -> &str;
}


pub struct ChainBuilder<B> {
    chunk_builder: B,
    last_block_number: BlockNumber,
    last_block_hash: String,
    last_parent_block_hash: String,
    last_parent_block_number: BlockNumber,
}


impl<B: Default> ChainBuilder<B> {
    pub fn new(
        base_block_number: BlockNumber,
        base_block_hash: String,
        base_parent_block_number: BlockNumber,
        base_parent_block_hash: String,
    ) -> Self {
        Self {
            chunk_builder: B::default(),
            last_block_number: base_block_number,
            last_block_hash: base_block_hash,
            last_parent_block_number: base_parent_block_number,
            last_parent_block_hash: base_parent_block_hash,
        }
    }
}


impl<B: Default> Default for ChainBuilder<B> {
    fn default() -> Self {
        Self::new(0, String::new(), 0, String::new())
    }
}


impl <B> AnyChainBuilder for ChainBuilder<B>
where
    B: BlockChunkBuilder + Send + Sync,
    B::Block: Block + serde::de::DeserializeOwned
{
    fn push(&mut self, line: &[u8]) -> anyhow::Result<()> {
        let block: B::Block = serde_json::from_slice(line)?;
        if !self.last_block_hash.is_empty() {
            ensure!(
                &self.last_block_hash == block.parent_hash(),
                "chain continuity was violated"
            );
        }
        self.chunk_builder.push(&block)?;
        self.last_block_number = block.number();
        self.last_block_hash.clear();
        self.last_block_hash.insert_str(0, block.hash());
        self.last_parent_block_number = block.parent_number();
        self.last_parent_block_hash.clear();
        self.last_parent_block_hash.insert_str(0, block.parent_hash());
        Ok(())
    }

    #[inline]
    fn chunk_builder(&self) -> &dyn ChunkBuilder {
        &self.chunk_builder
    }

    #[inline]
    fn chunk_builder_mut(&mut self) -> &mut dyn ChunkBuilder {
        &mut self.chunk_builder
    }

    #[inline]
    fn last_block_number(&self) -> BlockNumber {
        self.last_block_number
    }

    #[inline]
    fn last_block_hash(&self) -> &str {
        &self.last_block_hash
    }

    #[inline]
    fn last_parent_block_number(&self) -> BlockNumber {
        self.last_parent_block_number
    }

    #[inline]
    fn last_parent_block_hash(&self) -> &str {
        &self.last_parent_block_hash
    }
}


impl AnyChainBuilder for ChainBuilderBox {
    #[inline]
    fn push(&mut self, json_block: &[u8]) -> anyhow::Result<()> {
        self.as_mut().push(json_block)
    }

    #[inline]
    fn chunk_builder(&self) -> &dyn ChunkBuilder {
        self.as_ref().chunk_builder()
    }

    #[inline]
    fn chunk_builder_mut(&mut self) -> &mut dyn ChunkBuilder {
        self.as_mut().chunk_builder_mut()
    }

    #[inline]
    fn last_block_number(&self) -> BlockNumber {
        self.as_ref().last_block_number()
    }

    #[inline]
    fn last_block_hash(&self) -> &str {
        self.as_ref().last_block_hash()
    }

    #[inline]
    fn last_parent_block_number(&self) -> BlockNumber {
        self.as_ref().last_parent_block_number()
    }

    #[inline]
    fn last_parent_block_hash(&self) -> &str {
        self.as_ref().last_parent_block_hash()
    }
}