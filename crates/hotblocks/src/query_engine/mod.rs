//! The new query engine (`sqd-query-engine`) over this service's storage.

mod chunk;

use std::{path::Path, sync::Arc};

use anyhow::Context;
use sqd_primitives::BlockNumber;
use sqd_query_engine::{
    metadata::{DatasetDescription, load_dataset_description_strict},
    output::{ExecOptions, QueryOutput, execute_chunk_with},
    query::{compile, parse_query},
    scan::ColumnarChunkReader
};
use sqd_storage::db::ChunkReader;

use self::chunk::HotChunk;
use crate::types::DatasetKind;

/// The engine's metadata catalogs, one for each dataset kind it can answer.
#[derive(Default)]
pub struct MetadataCatalogs(Vec<(DatasetKind, Arc<DatasetDescription>)>);

impl MetadataCatalogs {
    /// Load `<kind>.yaml` for each dataset kind that has one in `dir`. The
    /// files must come from the engine revision this binary is built with:
    /// strict parsing refuses a key that revision does not know.
    pub fn load(dir: &Path) -> anyhow::Result<Self> {
        use DatasetKind::*;

        let mut catalogs = Vec::new();
        for kind in [Evm, Solana, Bitcoin, HyperliquidFills, HyperliquidReplicaCmds, Tron] {
            let path = dir.join(format!("{}.yaml", catalog_name(kind)));
            if !path
                .try_exists()
                .with_context(|| format!("checking {}", path.display()))?
            {
                continue;
            }
            catalogs.push((kind, Arc::new(load_dataset_description_strict(&path)?)));
        }
        Ok(Self(catalogs))
    }

    pub fn kinds(&self) -> Vec<DatasetKind> {
        self.0.iter().map(|(kind, _)| *kind).collect()
    }

    pub fn get(&self, kind: DatasetKind) -> Option<Arc<DatasetDescription>> {
        self.0
            .iter()
            .find(|(loaded, _)| *loaded == kind)
            .map(|(_, catalog)| catalog.clone())
    }
}

fn catalog_name(kind: DatasetKind) -> &'static str {
    match kind {
        DatasetKind::Evm => "evm",
        DatasetKind::Solana => "solana",
        DatasetKind::Bitcoin => "bitcoin",
        DatasetKind::HyperliquidFills => "hyperliquid_fills",
        DatasetKind::HyperliquidReplicaCmds => "hyperliquid_replica_cmds",
        DatasetKind::Tron => "tron"
    }
}

/// A query compiled by the new engine.
pub struct Plan {
    inner: sqd_query_engine::query::Plan,
    catalog: Arc<DatasetDescription>,
    options: ExecOptions
}

impl Plan {
    pub fn compile(query: &[u8], catalog: Arc<DatasetDescription>) -> anyhow::Result<Self> {
        let parsed = parse_query(query, &catalog)?;
        let inner = compile(&parsed, &catalog)?;
        Ok(Self {
            inner,
            catalog,
            options: ExecOptions::default()
        })
    }

    /// Read `first_block..=last_block`; `parent_block_hash` is the hash the
    /// first block's parent must have.
    pub fn set_range(
        &mut self,
        first_block: BlockNumber,
        last_block: Option<BlockNumber>,
        parent_block_hash: Option<String>
    ) {
        self.inner.from_block = first_block;
        self.inner.to_block = last_block;
        self.inner.parent_block_hash = parent_block_hash;
    }

    /// The block weight an answer may carry before it is cut.
    pub fn set_weight_budget(&mut self, budget: u64) {
        self.options.weight_budget = budget;
    }

    /// The chunk's blocks, cut at the response weight budget; `None` when no
    /// block is selected.
    pub fn execute(&self, chunk: &ChunkReader<'_>) -> anyhow::Result<Option<QueryOutput>> {
        let chunk = ColumnarChunkReader::new(HotChunk { reader: chunk });
        execute_chunk_with(&self.inner, &self.catalog, &chunk, self.options)
    }
}
