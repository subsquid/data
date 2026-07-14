//! The glue: one script drives the source simulator and the reference model in lockstep, the
//! real service sits between them as a black box.
//!
//! ```text
//!   script ──┬──▶ SourceSim ──HTTP──▶ [ sqd-hotblocks ] ──HTTP──▶ Client ──▶ validators
//!            └──▶ Model ◀────────────── compare at quiescence ───────┘
//! ```

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};

use crate::{
    chain::Chain,
    compare::{self, Quiescence},
    driver::{Client, Follower},
    model::{Finalize, Model},
    sim::{Numbering, SimConfig, SimDataset, SourceSim},
    sut::{DatasetSpec, Retention, Sut, SutConfig},
    types::{Anchor, BlockNumber, block_hash}
};

pub struct HarnessConfig {
    pub bin: std::path::PathBuf,
    pub dataset: String,
    pub chain: Arc<dyn Chain>,
    pub start_block: BlockNumber,
    pub base_timestamp_ms: i64,
    /// Dense (evm, hyperliquid) or sparse (Solana slots) block numbering.
    pub numbering: Numbering,
    pub retention: Retention,
    /// Whether the service is told the anchor hash. If not, the anchor is `⊥` (DEF-7) and the
    /// first block's parent is unverifiable.
    pub anchored: bool,
    /// How long the simulator holds a `/stream` request that has nothing to serve.
    pub source_poll: Duration,
    pub rust_log: String,
    pub quiescence: Quiescence,
    pub sut_args: Vec<String>
}

impl HarnessConfig {
    /// One dataset, ingest pinned at `start_block`, anchored on the block below it.
    pub fn from_block(bin: impl AsRef<Path>, chain: Arc<dyn Chain>, start_block: BlockNumber) -> Self {
        let sut = SutConfig::new(bin.as_ref(), Vec::new());
        Self {
            bin: sut.bin,
            dataset: "ds0".to_string(),
            chain,
            start_block,
            base_timestamp_ms: 1_760_000_000_000,
            numbering: Numbering::Dense,
            retention: Retention::FromBlock {
                number: start_block,
                parent_hash: Some(block_hash(start_block - 1, 0))
            },
            anchored: true,
            source_poll: Duration::from_millis(200),
            rust_log: "info".to_string(),
            quiescence: Quiescence::default(),
            sut_args: sut.args
        }
    }
}

pub struct Harness {
    pub sim: SourceSim,
    pub sut: Sut,
    pub client: Client,
    pub model: Model,
    pub chain: Arc<dyn Chain>,
    pub dataset: String,
    pub start_block: BlockNumber,
    pub numbering: Numbering,
    pub quiescence: Quiescence
}

impl Harness {
    pub async fn start(cfg: HarnessConfig) -> Result<Self> {
        let sim = SourceSim::start(SimConfig {
            datasets: vec![SimDataset {
                id: cfg.dataset.clone(),
                chain: cfg.chain.clone(),
                start_block: cfg.start_block,
                base_timestamp_ms: cfg.base_timestamp_ms,
                numbering: cfg.numbering
            }],
            poll_timeout: cfg.source_poll
        })
        .await
        .context("failed to start the source simulator")?;

        let mut sut_cfg = SutConfig::new(
            &cfg.bin,
            vec![DatasetSpec {
                id: cfg.dataset.clone(),
                kind: cfg.chain.config_kind().to_string(),
                retention: cfg.retention.clone(),
                sources: vec![sim.base_url(&cfg.dataset)]
            }]
        );
        sut_cfg.args = cfg.sut_args;
        sut_cfg.rust_log = cfg.rust_log;
        let sut = Sut::start(sut_cfg).await?;

        let client = Client::new(sut.base_url(), &cfg.dataset)?;
        let anchor_hash = cfg.anchored.then(|| sim.anchor_hash(&cfg.dataset));
        let model = Model::new(Anchor::new(cfg.start_block - 1, anchor_hash));

        Ok(Self {
            sim,
            sut,
            client,
            model,
            chain: cfg.chain,
            dataset: cfg.dataset,
            start_block: cfg.start_block,
            numbering: cfg.numbering,
            quiescence: cfg.quiescence
        })
    }

    /// The source mints `n` more blocks; the model EXTENDs by the same run.
    pub fn produce(&mut self, n: u32) -> Result<()> {
        let blocks = self.sim.produce(&self.dataset, n);
        self.model.extend(&blocks, None)
    }

    /// The source reorgs: the suffix at `from` is replaced by `len` freshly-minted blocks.
    pub fn fork(&mut self, from: BlockNumber, len: u32) -> Result<()> {
        let blocks = self.sim.fork(&self.dataset, from, len)?;
        self.model.replace(from, &blocks, None)
    }

    /// The source declares block `number` final.
    pub fn finalize(&mut self, number: BlockNumber) -> Result<()> {
        let r = self.sim.finalize(&self.dataset, number)?;
        match self.model.finalize(&r) {
            Finalize::Applied | Finalize::Ignored => Ok(()),
            Finalize::IntegrityFault => bail!("the script declared a finality that contradicts the model: {r}")
        }
    }

    /// Finality trailing the tip by `lag` *blocks*, as on a real chain. Blocks, not numbers:
    /// on a slot-numbered chain `tip - lag` may name a slot that produced nothing.
    pub fn finalize_with_lag(&mut self, lag: u64) -> Result<()> {
        let Some(i) = self.model.span().checked_sub(1 + lag as usize) else {
            return Ok(()); // not deep enough yet
        };
        let number = self.model.seg[i].number;
        self.finalize(number)
    }

    /// Wait until the service has caught up with the script and holds still. On failure it
    /// reports how the source was driven: "never asked" and "asked, threw the answer away" are
    /// very different bugs.
    pub async fn settle(&self) -> Result<()> {
        compare::await_quiescence(&self.client, &self.model, &self.quiescence)
            .await
            .with_context(|| format!("the source saw: {:?}", self.sim.stats(&self.dataset)))
    }

    /// Diff every observable against the model.
    pub async fn assert_conforms(&self) -> Result<()> {
        compare::assert_conforms(&self.client, &self.model, &*self.chain, &self.dataset).await
    }

    /// Diff the opt-in block-hash lookup endpoint against every canonical block in the model.
    pub async fn assert_hash_index_conforms(&self) -> Result<()> {
        compare::assert_hash_index_conforms(&self.client, &self.model).await
    }

    /// An anchored follower positioned at the bottom of the window (04 §7).
    pub fn follower(&self) -> Follower {
        Follower::new(
            self.model.first().unwrap_or(self.start_block),
            self.model.anchor.hash.clone()
        )
    }
}
