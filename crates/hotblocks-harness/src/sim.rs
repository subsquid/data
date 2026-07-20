//! The source simulator — a scriptable chain generator speaking the source side of the binding
//! (spec 13 §7, DEF-12). It is the truth of a run: whatever it served is what the service must
//! converge to.
//!
//! Two facts about the service shape it, and undoing either silently breaks every test:
//!
//! 1. It commits a batch only when the source's response *ends* (`MaybeOnHead`), so each
//!    `/stream` request serves what exists now and closes. Hold the stream open and the blocks
//!    stay buffered and invisible, forever.
//! 2. It re-requests the instant a response ends, so a request with nothing to serve is held for
//!    `poll_timeout`. Answer `204` at once and the ingest loop spins at full CPU.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant}
};

use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post}
};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::watch;

use crate::{
    P_CONFLICT_WINDOW,
    chain::Chain,
    types::{Block, BlockNumber, BlockRef, Rng, block_hash}
};

pub struct SimConfig {
    pub datasets: Vec<SimDataset>,
    /// How long a `/stream` request with nothing to serve is held before answering `NO_DATA`.
    pub poll_timeout: Duration
}

pub struct SimDataset {
    pub id: String,
    pub chain: Arc<dyn Chain>,
    pub start_block: BlockNumber,
    pub base_timestamp_ms: i64,
    pub numbering: Numbering
}

/// How block numbers advance. Solana numbers blocks by time-based *slots*, and a slot that
/// produces nothing leaves a hole — so the numbering carries no invariant, the parent pointer
/// does (INV-1/INV-2). evm and hyperliquid number densely.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Numbering {
    #[default]
    Dense,
    /// Deterministic holes: 0 to 2 slots skipped between blocks.
    Sparse,
    /// A fixed number of positions skipped after every produced block.
    FixedGap(u64)
}

impl Numbering {
    fn skipped_after(self, number: BlockNumber) -> u64 {
        match self {
            Numbering::Dense => 0,
            Numbering::Sparse => Rng::new(number).below(3),
            Numbering::FixedGap(size) => size
        }
    }
}

/// Source-side faults the service must survive (FM-SRC-*, CT-4/CT-9).
///
/// FUTURE: the rest of the repertoire lands here — stalls, disconnects, malformed JSON, broken
/// linkage, regressive watermarks, conflicting hints.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimFaults {
    /// Serve a JSONL body whose final record is not newline-terminated.
    pub unterminated_final_line: bool
}

/// Counters a test can assert on (how the SUT actually drove the source).
#[derive(Clone, Copy, Debug, Default)]
pub struct SimStats {
    pub stream_requests: u64,
    /// `fromBlock` on the most recent request, for rollback-position assertions.
    pub last_stream_from: Option<BlockNumber>,
    pub blocks_served: u64,
    pub fork_signals: u64,
    pub no_data: u64,
    /// The service asked for history below the source's first block — always a defect.
    pub below_history: u64
}

pub struct SourceSim {
    shared: Arc<Shared>,
    addr: SocketAddr,
    server: tokio::task::JoinHandle<()>
}

struct Shared {
    datasets: Mutex<HashMap<String, DatasetSim>>,
    /// Bumped after every mutation; wakes the long-polling `/stream` handlers.
    bump: watch::Sender<u64>,
    poll_timeout: Duration
}

impl SourceSim {
    pub async fn start(cfg: SimConfig) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let datasets = cfg
            .datasets
            .into_iter()
            .map(|d| (d.id.clone(), DatasetSim::new(d)))
            .collect();

        let shared = Arc::new(Shared {
            datasets: Mutex::new(datasets),
            bump: watch::channel(0).0,
            poll_timeout: cfg.poll_timeout
        });

        let app = Router::new()
            .route("/{ds}/stream", post(stream))
            .route("/{ds}/head", get(head))
            .route("/{ds}/finalized-head", get(finalized_head))
            .with_state(shared.clone());

        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        Ok(Self { shared, addr, server })
    }

    /// The `data_sources` URL — the service appends `/stream`, `/head`, ... to it.
    pub fn base_url(&self, dataset: &str) -> String {
        format!("http://{}/{}", self.addr, dataset)
    }

    /// The hash of the block below the dataset's first block (DEF-7).
    pub fn anchor_hash(&self, dataset: &str) -> String {
        self.with(dataset, |d| d.anchor_hash.clone())
    }

    /// Extend the canonical chain by `n` blocks; returns them so the model can EXTEND too.
    pub fn produce(&self, dataset: &str, n: u32) -> Vec<Block> {
        let blocks = self.with(dataset, |d| d.produce(n));
        self.bump();
        blocks
    }

    /// Replace the canonical suffix at `from` with `len` freshly-minted blocks (a reorg).
    pub fn fork(&self, dataset: &str, from: BlockNumber, len: u32) -> Result<Vec<Block>> {
        let blocks = self.try_with(dataset, |d| d.fork(from, len))?;
        self.bump();
        Ok(blocks)
    }

    /// Fault injection for CT-4/FM-SRC-5: replace a suffix that includes the source's own
    /// finalized head and claim the replacement tip as final.
    ///
    /// Unlike [`Self::fork`], this deliberately violates source finality. It has no model-side
    /// counterpart: the last accepted model state remains the oracle while the SUT rejects the
    /// equivocating source.
    pub fn equivocate_finalized_prefix(&self, dataset: &str, from: BlockNumber, len: u32) -> Result<()> {
        self.try_with(dataset, |d| d.equivocate_finalized_prefix(from, len))?;
        self.bump();
        Ok(())
    }

    /// Declare `number` (and everything below it) final.
    pub fn finalize(&self, dataset: &str, number: BlockNumber) -> Result<BlockRef> {
        let r = self.try_with(dataset, |d| d.finalize(number))?;
        self.bump();
        Ok(r)
    }

    pub fn tip(&self, dataset: &str) -> Option<BlockRef> {
        self.with(dataset, |d| d.chain.last().map(Block::as_ref))
    }

    pub fn stats(&self, dataset: &str) -> SimStats {
        self.with(dataset, |d| d.stats)
    }

    /// Turn a source-side fault on or off (FM-SRC-*).
    pub fn inject_fault(&self, dataset: &str, f: impl FnOnce(&mut SimFaults)) {
        self.with(dataset, |d| f(&mut d.faults));
    }

    fn with<T>(&self, dataset: &str, f: impl FnOnce(&mut DatasetSim) -> T) -> T {
        let mut guard = self.shared.datasets.lock().expect("simulator state is poisoned");
        let d = guard
            .get_mut(dataset)
            .unwrap_or_else(|| panic!("unknown dataset '{dataset}'"));
        f(d)
    }

    fn try_with<T>(&self, dataset: &str, f: impl FnOnce(&mut DatasetSim) -> Result<T>) -> Result<T> {
        self.with(dataset, f)
    }

    fn bump(&self) {
        self.shared.bump.send_modify(|v| *v += 1);
    }
}

impl Drop for SourceSim {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct DatasetSim {
    chain_kind: Arc<dyn Chain>,
    start: BlockNumber,
    base_ts: i64,
    anchor_hash: String,
    numbering: Numbering,
    chain: Vec<Block>,
    fin: Option<BlockRef>,
    fork_id: u32,
    next_fork_id: u32,
    faults: SimFaults,
    stats: SimStats
}

enum Reply {
    Blocks { blocks: Vec<Block>, fin: Option<BlockRef> },
    Fork(Vec<BlockRef>),
    NoData(Option<BlockRef>)
}

impl DatasetSim {
    fn new(cfg: SimDataset) -> Self {
        Self {
            chain_kind: cfg.chain,
            start: cfg.start_block,
            base_ts: cfg.base_timestamp_ms,
            anchor_hash: block_hash(cfg.start_block - 1, 0),
            numbering: cfg.numbering,
            chain: Vec::new(),
            fin: None,
            fork_id: 0,
            next_fork_id: 1,
            faults: SimFaults::default(),
            stats: SimStats::default()
        }
    }

    fn tip_number(&self) -> Option<BlockNumber> {
        self.chain.last().map(|b| b.number)
    }

    /// The number the next produced block will carry. Under [`Numbering::Sparse`] it skips
    /// slots, so it is *not* `tip + 1`.
    fn next_number(&self) -> BlockNumber {
        match self.chain.last() {
            None => self.start,
            Some(last) => last.number + 1 + self.numbering.skipped_after(last.number)
        }
    }

    /// The anchor, then every block of the canonical chain. `None` for a slot that produced none.
    fn hash_at(&self, n: BlockNumber) -> Option<&str> {
        if n + 1 == self.start {
            return Some(&self.anchor_hash);
        }
        self.chain
            .binary_search_by_key(&n, |b| b.number)
            .ok()
            .map(|i| self.chain[i].hash.as_str())
    }

    /// The last canonical hash at or below `n`. Unlike [`Self::hash_at`], this crosses empty
    /// slots on sparse-numbered chains. Positions above the tip remain unknown.
    fn chain_hash_at(&self, n: BlockNumber) -> Option<&str> {
        if self.chain.last().is_some_and(|tip| n > tip.number) {
            return None;
        }
        let i = self.chain.partition_point(|b| b.number <= n);
        if i > 0 {
            return Some(&self.chain[i - 1].hash);
        }
        (n.checked_add(1) == Some(self.start)).then_some(self.anchor_hash.as_str())
    }

    fn produce(&mut self, n: u32) -> Vec<Block> {
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let number = self.next_number();
            let (parent_number, parent_hash) = match self.chain.last() {
                Some(last) => (last.number, last.hash.clone()),
                None => (self.start - 1, self.anchor_hash.clone())
            };
            let block = Block {
                number,
                hash: block_hash(number, self.fork_id),
                parent_number,
                parent_hash,
                timestamp_ms: self.base_ts + (number - self.start) as i64 * 1000,
                fork_id: self.fork_id
            };
            self.chain.push(block.clone());
            out.push(block);
        }
        out
    }

    fn fork(&mut self, from: BlockNumber, len: u32) -> Result<Vec<Block>> {
        self.validate_fork_position(from)?;
        ensure!(
            self.fin.as_ref().is_none_or(|f| f.number < from),
            "the script forks at or below the source's own finalized head — an equivocating source \
             belongs to the CT-4 fault corpus, not to a well-formed script"
        );
        Ok(self.replace_suffix(from, len))
    }

    fn equivocate_finalized_prefix(&mut self, from: BlockNumber, len: u32) -> Result<()> {
        self.validate_fork_position(from)?;
        ensure!(len > 0, "a finality-equivocation fault must mint a replacement tip");
        let finalized = self
            .fin
            .as_ref()
            .context("a finality-equivocation fault requires an existing finalized head")?;
        ensure!(
            from <= finalized.number,
            "equivocation at {from} does not replace finalized block {}",
            finalized.number
        );

        let replacement = self.replace_suffix(from, len);
        self.fin = Some(replacement.last().expect("a non-empty replacement has a tip").as_ref());
        Ok(())
    }

    fn validate_fork_position(&self, from: BlockNumber) -> Result<()> {
        ensure!(
            from >= self.start,
            "fork at {from} is below the source's first block {}",
            self.start
        );
        ensure!(from <= self.next_number(), "fork at {from} is above the source's chain");
        Ok(())
    }

    fn replace_suffix(&mut self, from: BlockNumber, len: u32) -> Vec<Block> {
        let keep = self.chain.partition_point(|b| b.number < from);
        self.chain.truncate(keep);
        self.fork_id = self.next_fork_id;
        self.next_fork_id += 1;
        self.produce(len)
    }

    fn finalize(&mut self, number: BlockNumber) -> Result<BlockRef> {
        let hash = self
            .hash_at(number)
            .with_context(|| format!("cannot finalize block {number}: the source does not have it"))?
            .to_string();
        let r = BlockRef::new(number, hash);
        self.fin = Some(r.clone());
        Ok(r)
    }

    /// RP-11/DEF-12 — the ancestors of the divergence point, ascending.
    fn hints_ending_at(&self, n: BlockNumber) -> Vec<BlockRef> {
        let hi = self.chain.partition_point(|b| b.number <= n);
        if hi == 0 {
            // Nothing of ours at or below the divergence: all we know is the anchor.
            return vec![BlockRef::new(self.start - 1, self.anchor_hash.clone())];
        }
        let lo = hi.saturating_sub(P_CONFLICT_WINDOW as usize);
        self.chain[lo..hi].iter().map(Block::as_ref).collect()
    }

    fn respond(&mut self, req: &StreamReq) -> Reply {
        self.stats.stream_requests += 1;
        self.stats.last_stream_from = Some(req.from_block);

        let parent_pos = req.from_block.saturating_sub(1);
        if let Some(asserted) = &req.parent_block_hash {
            match self.chain_hash_at(parent_pos) {
                Some(known) if known == asserted => {}
                Some(_) => {
                    self.stats.fork_signals += 1;
                    return Reply::Fork(self.hints_ending_at(parent_pos));
                }
                None => {
                    // Above our tip: it claims blocks we do not have — disagree, hint at our tip.
                    if let Some(tip) = self.tip_number()
                        && parent_pos > tip
                    {
                        self.stats.fork_signals += 1;
                        return Reply::Fork(self.hints_ending_at(tip));
                    }
                    // Below our history: unverifiable, serve as-is (the `⊥`-anchor case).
                }
            }
        }

        if req.from_block < self.start {
            self.stats.below_history += 1;
            return Reply::NoData(self.fin.clone());
        }
        let Some(tip) = self.tip_number() else {
            self.stats.no_data += 1;
            return Reply::NoData(self.fin.clone());
        };
        if req.from_block > tip {
            self.stats.no_data += 1;
            return Reply::NoData(self.fin.clone());
        }

        let lo = self.chain.partition_point(|b| b.number < req.from_block);
        let blocks = self.chain[lo..].to_vec();
        self.stats.blocks_served += blocks.len() as u64;
        Reply::Blocks {
            blocks,
            fin: self.fin.clone()
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamReq {
    from_block: BlockNumber,
    #[serde(default)]
    parent_block_hash: Option<String>
}

async fn stream(State(shared): State<Arc<Shared>>, Path(ds): Path<String>, body: Bytes) -> Response {
    let req: StreamReq = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(err) => return (StatusCode::BAD_REQUEST, format!("bad stream request: {err}")).into_response()
    };

    let mut bump = shared.bump.subscribe();
    let deadline = Instant::now() + shared.poll_timeout;

    loop {
        // Mark the version seen *before* reading the state, or a racing mutation is lost.
        bump.borrow_and_update();

        let reply = {
            let mut guard = shared.datasets.lock().expect("simulator state is poisoned");
            let Some(d) = guard.get_mut(&ds) else {
                return (StatusCode::NOT_FOUND, format!("unknown dataset '{ds}'")).into_response();
            };
            match d.respond(&req) {
                Reply::Blocks { blocks, fin } => {
                    let mut body = String::new();
                    for block in &blocks {
                        body.push_str(&d.chain_kind.source_block(block).to_string());
                        body.push('\n');
                    }
                    // A fault: well-formed sources terminate every record.
                    if d.faults.unterminated_final_line {
                        body.pop();
                    }
                    Some(jsonl(body, fin.as_ref()))
                }
                Reply::Fork(hints) => {
                    Some((StatusCode::CONFLICT, Json(json!({ "previousBlocks": hints }))).into_response())
                }
                Reply::NoData(fin) => {
                    if Instant::now() >= deadline {
                        Some(no_data(fin.as_ref()))
                    } else {
                        None
                    }
                }
            }
        };

        if let Some(reply) = reply {
            return reply;
        }

        let wait = deadline.saturating_duration_since(Instant::now());
        let _ = tokio::time::timeout(wait, bump.changed()).await;
    }
}

async fn head(State(shared): State<Arc<Shared>>, Path(ds): Path<String>) -> Response {
    watermark(&shared, &ds, |d| d.chain.last().map(Block::as_ref))
}

async fn finalized_head(State(shared): State<Arc<Shared>>, Path(ds): Path<String>) -> Response {
    watermark(&shared, &ds, |d| d.fin.clone())
}

fn watermark(shared: &Shared, ds: &str, f: impl FnOnce(&DatasetSim) -> Option<BlockRef>) -> Response {
    let guard = shared.datasets.lock().expect("simulator state is poisoned");
    match guard.get(ds) {
        Some(d) => Json(f(d)).into_response(),
        None => (StatusCode::NOT_FOUND, format!("unknown dataset '{ds}'")).into_response()
    }
}

fn jsonl(body: String, fin: Option<&BlockRef>) -> Response {
    finality_headers(Response::builder().status(StatusCode::OK), fin)
        .header("content-type", "text/plain")
        .body(Body::from(body))
        .expect("a well-formed response")
}

fn no_data(fin: Option<&BlockRef>) -> Response {
    finality_headers(Response::builder().status(StatusCode::NO_CONTENT), fin)
        .body(Body::empty())
        .expect("a well-formed response")
}

/// All-or-nothing: the client rejects a response carrying one half of the pair.
fn finality_headers(builder: axum::http::response::Builder, fin: Option<&BlockRef>) -> axum::http::response::Builder {
    match fin {
        Some(fin) => builder
            .header("x-sqd-finalized-head-number", fin.number)
            .header("x-sqd-finalized-head-hash", fin.hash.as_str()),
        None => builder
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::chain::HlFills;

    const DS: &str = "ds0";
    const START: BlockNumber = 1_000;

    async fn sim() -> SourceSim {
        SourceSim::start(SimConfig {
            datasets: vec![SimDataset {
                id: DS.to_string(),
                chain: Arc::new(HlFills),
                start_block: START,
                base_timestamp_ms: 1_760_000_000_000,
                numbering: Numbering::Dense
            }],
            poll_timeout: Duration::from_millis(100)
        })
        .await
        .unwrap()
    }

    /// The service's data client is the only consumer that matters, so the simulator is tested
    /// against what that client actually does with a response — not against what it looks like.
    #[tokio::test]
    async fn serves_a_terminated_jsonl_stream_with_finality_headers() {
        let sim = sim().await;
        sim.produce(DS, 3);
        sim.finalize(DS, START + 1).unwrap();

        let res = reqwest::Client::new()
            .post(format!("{}/stream", sim.base_url(DS)))
            .json(&json!({"fromBlock": START, "parentBlockHash": sim.anchor_hash(DS)}))
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), 200);
        assert_eq!(res.headers()["x-sqd-finalized-head-number"], "1001");
        assert!(res.headers().contains_key("x-sqd-finalized-head-hash"));
        // The client reads the body as a stream and only commits when it *ends*: a response
        // without a length that terminates is a service that never sees the blocks.
        assert!(res.content_length().is_some(), "the body must be length-delimited");

        let body = res.text().await.unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(lines.len(), 3, "one JSON object per block");
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["header"]["number"], 1000);
        assert_eq!(first["header"]["parentHash"], sim.anchor_hash(DS));

        // Every record is newline-terminated, the last one included. This is not cosmetic: an
        // unterminated final record panics the service's line reader (GAP-19), so a simulator
        // that got this wrong would fail every test for the wrong reason.
        assert!(body.ends_with('\n'));
    }

    #[tokio::test]
    async fn disagrees_with_a_wrong_parent_by_a_fork_signal() {
        let sim = sim().await;
        sim.produce(DS, 5);

        let res = reqwest::Client::new()
            .post(format!("{}/stream", sim.base_url(DS)))
            .json(&json!({"fromBlock": START + 3, "parentBlockHash": "0xnot-our-block"}))
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), 409);
        let body: serde_json::Value = res.json().await.unwrap();
        let hints = body["previousBlocks"].as_array().unwrap();
        assert!(!hints.is_empty(), "the client rejects an empty hint list");
        assert_eq!(
            hints.last().unwrap()["number"],
            START + 2,
            "hints end at the disputed position"
        );
    }

    #[tokio::test]
    async fn validates_parent_hash_across_a_sparse_numbering_hole() {
        let sim = SourceSim::start(SimConfig {
            datasets: vec![SimDataset {
                id: DS.to_string(),
                chain: Arc::new(HlFills),
                start_block: START,
                base_timestamp_ms: 1_760_000_000_000,
                numbering: Numbering::Sparse
            }],
            poll_timeout: Duration::from_millis(100)
        })
        .await
        .unwrap();
        let blocks = sim.produce(DS, 10);
        let [parent, child] = blocks
            .windows(2)
            .find(|w| w[1].number > w[0].number + 1)
            .expect("the deterministic sparse sequence contains a hole")
        else {
            unreachable!()
        };

        let wrong = reqwest::Client::new()
            .post(format!("{}/stream", sim.base_url(DS)))
            .json(&json!({"fromBlock": child.number, "parentBlockHash": "0xwrong"}))
            .send()
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::CONFLICT);

        let correct = reqwest::Client::new()
            .post(format!("{}/stream", sim.base_url(DS)))
            .json(&json!({"fromBlock": child.number, "parentBlockHash": parent.hash}))
            .send()
            .await
            .unwrap();
        assert_eq!(correct.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn holds_a_request_it_cannot_serve_and_then_answers_no_data() {
        let sim = sim().await;
        let started = Instant::now();

        let res = reqwest::Client::new()
            .post(format!("{}/stream", sim.base_url(DS)))
            .json(&json!({"fromBlock": START}))
            .send()
            .await
            .unwrap();

        assert_eq!(res.status(), 204);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "the request must long-poll"
        );
    }
}
