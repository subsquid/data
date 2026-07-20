//! The glue: one script drives the source simulator and the reference model in lockstep, the
//! real service sits between them as a black box.
//!
//! ```text
//!   script ──┬──▶ SourceSim ──HTTP──▶ [ sqd-hotblocks ] ──HTTP──▶ Client ──▶ validators
//!            └──▶ Model ◀────────────── compare at quiescence ───────┘
//! ```

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};

use crate::{
    chain::Chain,
    compare::{self, Quiescence},
    driver::{Client, Follower},
    model::{Finalize, Model},
    sim::{Numbering, SimConfig, SimDataset, SimStats, SourceSim},
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
    /// How many independent simulators serve the dataset. Production configures several
    /// endpoints per dataset, so anything above 1 exercises `StandardDataSource`'s
    /// multi-endpoint path: the fixed-order poll race, the fork consensus, and the
    /// per-endpoint `MaybeOnHead` flush trigger.
    pub sources: usize,
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
            sources: 1,
            rust_log: "info".to_string(),
            quiescence: Quiescence::default(),
            sut_args: sut.args
        }
    }
}

pub struct Harness {
    /// The primary source — the one a script drives by default.
    pub sim: SourceSim,
    /// The remaining endpoints serving the same dataset. Block hashes are a pure function of
    /// ⟨number, fork⟩, so a peer fed the same script holds a byte-identical chain and one fed
    /// less holds an exact prefix: a source that is *behind*, not one that disagrees.
    pub peers: Vec<SourceSim>,
    pub sut: Sut,
    pub client: Client,
    pub model: Model,
    pub chain: Arc<dyn Chain>,
    pub dataset: String,
    pub start_block: BlockNumber,
    pub numbering: Numbering,
    pub quiescence: Quiescence,
    /// Per peer, the blocks it owes the primary. Per peer because production's laggard is a
    /// *minority* — one endpoint of three, not all of them.
    peer_deficits: Vec<u32>
}

impl Harness {
    pub async fn start(cfg: HarnessConfig) -> Result<Self> {
        ensure!(cfg.sources >= 1, "a dataset needs at least one source");

        let mut sims = Vec::with_capacity(cfg.sources);
        for _ in 0..cfg.sources {
            sims.push(
                SourceSim::start(SimConfig {
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
                .context("failed to start the source simulator")?
            );
        }
        let peers = sims.split_off(1);
        let sim = sims.pop().expect("at least one source");

        let mut sut_cfg = SutConfig::new(
            &cfg.bin,
            vec![DatasetSpec {
                id: cfg.dataset.clone(),
                kind: cfg.chain.config_kind().to_string(),
                retention: cfg.retention.clone(),
                sources: std::iter::once(&sim)
                    .chain(peers.iter())
                    .map(|s| s.base_url(&cfg.dataset))
                    .collect()
            }]
        );
        sut_cfg.args = cfg.sut_args;
        sut_cfg.rust_log = cfg.rust_log;
        let sut = Sut::start(sut_cfg).await?;

        let client = Client::new(sut.base_url(), &cfg.dataset)?;
        let anchor_hash = cfg.anchored.then(|| sim.anchor_hash(&cfg.dataset));
        let model = Model::new(Anchor::new(cfg.start_block - 1, anchor_hash));

        Ok(Self {
            peer_deficits: vec![0; peers.len()],
            sim,
            peers,
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

    /// Every source mints `n` more blocks; the model EXTENDs by the same run.
    pub fn produce(&mut self, n: u32) -> Result<()> {
        self.produce_lagging(&[], n)
    }

    /// Only the primary mints: *every* peer falls `n` blocks behind.
    pub fn produce_ahead(&mut self, n: u32) -> Result<()> {
        let all: Vec<usize> = (0..self.peers.len()).collect();
        self.produce_lagging(&all, n)
    }

    /// The primary and every peer outside `behind` mint `n` blocks; those named hold and fall
    /// `n` further back.
    ///
    /// A peer left behind holds an exact prefix of the canonical chain: healthy, agreeing, merely
    /// not caught up — the one state `StandardDataSource` cannot name, since an endpoint leaves
    /// the rotation by erroring and never by being slow.
    pub fn produce_lagging(&mut self, behind: &[usize], n: u32) -> Result<()> {
        for &i in behind {
            ensure!(
                i < self.peers.len(),
                "peer {i} does not exist — {} configured",
                self.peers.len()
            );
        }
        let blocks = self.sim.produce(&self.dataset, n);
        self.model.extend(&blocks, None)?;
        for i in 0..self.peers.len() {
            if behind.contains(&i) {
                self.peer_deficits[i] += n;
            } else {
                self.mint_on_peer(i, n)?;
            }
        }
        Ok(())
    }

    /// Bring every peer back up to the primary's tip.
    pub fn catch_up_peers(&mut self) -> Result<()> {
        for i in 0..self.peers.len() {
            let owed = std::mem::take(&mut self.peer_deficits[i]);
            self.mint_on_peer(i, owed)?;
        }
        Ok(())
    }

    /// Mint on a peer and check it reproduced the primary's chain. Determinism is the premise
    /// of every multi-source script: the moment it breaks, a lag test silently becomes a fork
    /// test and stops testing what it claims to.
    fn mint_on_peer(&self, i: usize, n: u32) -> Result<()> {
        let minted = self.peers[i].produce(&self.dataset, n);
        let canonical = self.model.blocks_in(
            minted.first().map_or(0, |b| b.number),
            minted.last().map_or(0, |b| b.number)
        );
        // `zip` stops at the shorter side: without this a length mismatch passes vacuously.
        ensure!(
            minted.len() == canonical.len(),
            "peer {i} minted {} blocks over a range the canonical chain fills with {}",
            minted.len(),
            canonical.len()
        );
        for (got, want) in minted.iter().zip(canonical) {
            ensure!(
                got.hash == want.hash && got.number == want.number,
                "peer {i} diverged from the primary at block {}: {} vs {}",
                got.number,
                got.hash,
                want.hash
            );
        }
        Ok(())
    }

    /// The source reorgs: the suffix at `from` is replaced by `len` freshly-minted blocks.
    ///
    /// Single-source only. Reorging one endpoint of several is not "a fork" but a disagreement,
    /// and what the service should do with it is the fork-consensus question
    /// (`StandardDataSource::poll_next_event`: majority, or all-active, or a 2 s timeout) —
    /// that needs its own script, not an accidental one.
    pub fn fork(&mut self, from: BlockNumber, len: u32) -> Result<()> {
        ensure!(
            self.peers.is_empty(),
            "fork() drives the primary only; with peers configured it would script a source \
             disagreement, not a reorg"
        );
        let blocks = self.sim.fork(&self.dataset, from, len)?;
        self.model.replace(from, &blocks, None)
    }

    /// The sources declare block `number` final. A peer that has not reached it yet stays
    /// silent — a lagging source cannot declare a block it does not hold.
    pub fn finalize(&mut self, number: BlockNumber) -> Result<()> {
        let r = self.sim.finalize(&self.dataset, number)?;
        for peer in &self.peers {
            if peer.tip(&self.dataset).is_some_and(|t| t.number >= number) {
                peer.finalize(&self.dataset, number)?;
            }
        }
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
            .with_context(|| format!("the sources saw: {:?}", self.source_stats()))
    }

    /// What each endpoint saw, primary first — in a multi-source script the endpoint that
    /// explains a failure is rarely the driven one.
    pub fn source_stats(&self) -> Vec<SimStats> {
        std::iter::once(&self.sim)
            .chain(self.peers.iter())
            .map(|s| s.stats(&self.dataset))
            .collect()
    }

    /// Diff every observable against the model.
    pub async fn assert_conforms(&self) -> Result<()> {
        compare::assert_conforms(&self.client, &self.model, &*self.chain, &self.dataset).await
    }

    /// Diff the opt-in block-hash lookup endpoint against every canonical block in the model.
    pub async fn assert_hash_index_conforms(&self) -> Result<()> {
        compare::assert_hash_index_conforms(&self.client, &self.model).await
    }

    /// Diff the opt-in transaction-hash endpoint against every canonical
    /// transaction in the model's projected chain.
    pub async fn assert_transaction_hash_index_conforms(&self) -> Result<()> {
        compare::assert_transaction_hash_index_conforms(&self.client, &self.model, &*self.chain).await
    }

    /// An anchored follower positioned at the bottom of the window (04 §7).
    pub fn follower(&self) -> Follower {
        Follower::new(
            self.model.first().unwrap_or(self.start_block),
            self.model.anchor.hash.clone()
        )
    }
}
