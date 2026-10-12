//! Shadow traffic for the new query engine (`sqd-query-engine`).
//!
//! A sampled share of queries is answered as usual, and each chunk the answer
//! reads is handed to a background task. The task runs the legacy engine and
//! the new one on that chunk, from the same storage snapshot and with the same
//! block bounds, in random order so that neither always reads a warm cache.
//! The client never waits for it and never sees its result. A dataset kind
//! with no metadata catalog is not shadowed.
//!
//! A difference in values, an error only one engine raises, and a run where the
//! new engine is much slower are logged with the query that reproduces them and
//! counted in metrics.

mod compare;

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering}
    },
    time::{Duration, Instant}
};

use anyhow::Context;
use bytes::Bytes;
use parking_lot::Mutex;
use sqd_primitives::BlockNumber;
use sqd_query::{JsonLinesWriter, Query};
use sqd_query_engine::metadata::DatasetDescription;
use sqd_storage::db::DatasetId;
use tracing::{error, warn};

use self::compare::Difference;
use crate::{
    metrics::{
        ShadowOutcome, report_shadow_query, report_shadow_run, report_shadow_run_finished, report_shadow_run_started,
        report_shadow_timing
    },
    query::StaticChunkReader,
    query_engine::{self, MetadataCatalogs},
    settings::{SettingsRef, ShadowSettings},
    types::DatasetKind
};

/// How much of a query a log line carries: a query can list tens of thousands
/// of addresses.
const MAX_LOGGED_QUERY_BYTES: usize = 16 * 1024;
/// The window [`ShadowSettings::log_limit`] counts log lines in.
const LOG_WINDOW: Duration = Duration::from_secs(300);

pub type ShadowRef = Arc<Shadow>;

pub struct Shadow {
    settings: SettingsRef,
    max_in_flight: usize,
    catalogs: Arc<MetadataCatalogs>,
    /// Runs get their own threads, so that they never queue ahead of a client's
    /// query. The legacy engine's polars operations still use the shared pool.
    pool: rayon::ThreadPool,
    in_flight: AtomicUsize,
    log_window: Mutex<LogWindow>
}

impl Shadow {
    pub fn new(max_in_flight: usize, settings: SettingsRef, catalogs: Arc<MetadataCatalogs>) -> anyhow::Result<Self> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(max_in_flight.max(1))
            .thread_name(|index| format!("shadow-{index}"))
            .build()
            .context("failed to start the shadow threads")?;
        Ok(Self {
            in_flight: AtomicUsize::new(0),
            log_window: Mutex::new(LogWindow {
                started: Instant::now(),
                logged: HashSet::new()
            }),
            settings,
            max_in_flight,
            catalogs,
            pool
        })
    }

    /// Decide whether a query is shadowed. `body` is the request as the client
    /// sent it, which the new engine parses itself.
    pub fn sample(self: &Arc<Self>, dataset_id: DatasetId, query: &Query, body: &Bytes) -> Option<ShadowQuery> {
        let percent = self.settings.shadow().percent;
        if percent <= 0.0 || rand::random::<f64>() * 100.0 >= percent {
            return None;
        }
        let kind = DatasetKind::from_query(query).ok()?;
        let catalog = self.catalogs.get(kind)?;

        report_shadow_query(dataset_id);

        Some(ShadowQuery {
            shadow: self.clone(),
            dataset_id,
            catalog,
            query: Arc::new(query.clone()),
            body: body.clone()
        })
    }

    fn try_reserve(self: &Arc<Self>) -> Option<InFlight> {
        let previous = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if previous < self.max_in_flight {
            report_shadow_run_started();
            Some(InFlight(self.clone()))
        } else {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            None
        }
    }

    fn record(&self, run: &Run, outcome: &Outcome) {
        let settings = self.settings.shadow();
        let dataset_id = run.query.dataset_id;
        let label = outcome.label();
        report_shadow_run(dataset_id, label);

        let slow = match outcome {
            Outcome::Compared { legacy, new, .. } => {
                let slow = is_slow(&settings, *legacy, *new);
                report_shadow_timing(dataset_id, *legacy, *new, slow);
                slow
            }
            Outcome::Failed { .. } => false
        };

        let reason = match label {
            ShadowOutcome::Mismatch | ShadowOutcome::LegacyError | ShadowOutcome::NewError => label.as_str(),
            ShadowOutcome::Match if slow => "slow",
            _ => return
        };
        let fingerprint = run.query.fingerprint();
        if !self.log_window.lock().claim(fingerprint, reason, settings.log_limit) {
            return;
        }

        match outcome {
            Outcome::Compared {
                difference: Some(difference),
                ..
            } => log_difference(run, outcome, difference),
            Outcome::Compared { .. } => log_slow(run, outcome),
            Outcome::Failed { legacy, new } => log_failure(run, legacy, new)
        }
    }

    fn record_skip(&self, dataset_id: DatasetId) {
        report_shadow_run(dataset_id, ShadowOutcome::Skipped);
    }
}

fn is_slow(settings: &ShadowSettings, legacy: Duration, new: Duration) -> bool {
    let margin = new.saturating_sub(legacy);
    new.as_secs_f64() > legacy.as_secs_f64() * settings.slow_ratio && margin >= settings.slow_margin()
}

/// A shadowed query, carried by the response it shadows.
#[derive(Clone)]
pub struct ShadowQuery {
    shadow: ShadowRef,
    dataset_id: DatasetId,
    catalog: Arc<DatasetDescription>,
    query: Arc<Query>,
    body: Bytes
}

/// The block bounds one chunk of a response is read with: the legacy plan's
/// state when it ran.
#[derive(Clone, Debug)]
pub struct ChunkBounds {
    pub first_block: Option<BlockNumber>,
    pub last_block: Option<BlockNumber>,
    pub parent_block_hash: Option<String>
}

impl ShadowQuery {
    /// Compare the engines on one chunk of the response, in the background.
    /// A chunk that finds every slot taken is skipped: shadow work must not
    /// queue behind itself, and each queued chunk pins a storage snapshot.
    pub fn submit(&self, chunk: &StaticChunkReader, bounds: ChunkBounds) {
        let Some(slot) = self.shadow.try_reserve() else {
            self.shadow.record_skip(self.dataset_id);
            return;
        };

        let run = Run {
            query: self.clone(),
            chunk: chunk.clone(),
            bounds
        };
        self.shadow.pool.spawn(move || {
            let outcome = run.execute();
            run.query.shadow.record(&run, &outcome);
            drop(slot);
        });
    }

    /// The query with its block range and parent hash removed: what stays the
    /// same while a client follows the chain.
    fn fingerprint(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.dataset_id.as_str().hash(&mut hasher);
        match serde_json::from_slice::<serde_json::Value>(&self.body) {
            Ok(serde_json::Value::Object(mut query)) => {
                for key in ["fromBlock", "toBlock", "parentBlockHash"] {
                    query.remove(key);
                }
                serde_json::Value::Object(query).to_string().hash(&mut hasher);
            }
            _ => self.body.hash(&mut hasher)
        }
        hasher.finish()
    }
}

struct InFlight(ShadowRef);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        report_shadow_run_finished();
    }
}

struct Run {
    query: ShadowQuery,
    chunk: StaticChunkReader,
    bounds: ChunkBounds
}

enum Outcome {
    Compared {
        legacy: Duration,
        new: Duration,
        difference: Option<Difference>,
        legacy_blocks: usize,
        new_blocks: usize
    },
    /// At least one engine failed; a panic counts as a failure. Both failing is
    /// counted but not logged: the engines agree that the query cannot run.
    Failed {
        legacy: Result<(), String>,
        new: Result<(), String>
    }
}

impl Outcome {
    fn label(&self) -> ShadowOutcome {
        match self {
            Outcome::Compared { difference: None, .. } => ShadowOutcome::Match,
            Outcome::Compared { .. } => ShadowOutcome::Mismatch,
            Outcome::Failed {
                legacy: Err(_),
                new: Err(_)
            } => ShadowOutcome::BothError,
            Outcome::Failed { legacy: Err(_), .. } => ShadowOutcome::LegacyError,
            Outcome::Failed { .. } => ShadowOutcome::NewError
        }
    }
}

struct EngineRun {
    response: Result<Response, String>,
    elapsed: Duration
}

#[derive(Default)]
struct Response {
    json_lines: Vec<u8>,
    /// From the engine, not the JSON: a query need not select block numbers.
    last_block: Option<BlockNumber>
}

impl Run {
    /// Plans are compiled before the clock starts: a response compiles once and
    /// reads many chunks, so only reading and encoding a chunk is compared.
    fn execute(&self) -> Outcome {
        let legacy_plan = self.legacy_plan();
        let mut new_plan = guarded(|| self.new_plan());

        let legacy = || timed(|| self.run_legacy(&legacy_plan));
        let new = || match &new_plan {
            Ok(plan) => timed(|| self.run_new(plan)),
            Err(err) => EngineRun {
                response: Err(err.clone()),
                elapsed: Duration::ZERO
            }
        };
        let (legacy, mut new) = if rand::random() {
            let legacy = legacy();
            (legacy, new())
        } else {
            let new = new();
            (legacy(), new)
        };

        // Each engine cuts an answer at its own weight budget, and the two weigh
        // blocks differently. When they stop at different blocks, the new engine
        // answers again for exactly the legacy blocks, with no budget, so that
        // both answers and both times cover the same work.
        if let (Ok(l), Ok(n), Ok(plan)) = (&legacy.response, &new.response, &mut new_plan)
            && let Some(legacy_end) = l.last_block
            && n.last_block != Some(legacy_end)
        {
            plan.set_range(
                self.first_block(),
                Some(legacy_end),
                self.bounds.parent_block_hash.clone()
            );
            plan.set_weight_budget(u64::MAX);
            new = timed(|| self.run_new(plan));
        }

        match (legacy.response, new.response) {
            (Ok(l), Ok(n)) => Outcome::Compared {
                legacy: legacy.elapsed,
                new: new.elapsed,
                difference: compare::first_difference(&l.json_lines, &n.json_lines),
                legacy_blocks: compare::count_lines(&l.json_lines),
                new_blocks: compare::count_lines(&n.json_lines)
            },
            (l, n) => Outcome::Failed {
                legacy: l.map(drop),
                new: n.map(drop)
            }
        }
    }

    fn first_block(&self) -> BlockNumber {
        self.bounds.first_block.unwrap_or(self.chunk.first_block())
    }

    fn legacy_plan(&self) -> sqd_query::Plan {
        let mut plan = self.query.query.compile();
        plan.set_first_block(self.bounds.first_block);
        plan.set_last_block(self.bounds.last_block);
        plan.set_parent_block_hash(self.bounds.parent_block_hash.clone());
        plan
    }

    fn new_plan(&self) -> anyhow::Result<query_engine::Plan> {
        let mut plan = query_engine::Plan::compile(&self.query.body, self.query.catalog.clone())?;
        plan.set_range(
            self.first_block(),
            self.bounds.last_block,
            self.bounds.parent_block_hash.clone()
        );
        Ok(plan)
    }

    fn run_legacy(&self, plan: &sqd_query::Plan) -> anyhow::Result<Response> {
        let Some(mut blocks) = self.chunk.with_reader(|reader| plan.execute(reader))? else {
            return Ok(Response::default());
        };
        let last_block = blocks.last_block();

        let mut writer = JsonLinesWriter::new(Vec::new());
        writer.write_blocks(&mut blocks)?;
        Ok(Response {
            json_lines: writer.finish()?,
            last_block: Some(last_block)
        })
    }

    fn run_new(&self, plan: &query_engine::Plan) -> anyhow::Result<Response> {
        let Some(output) = self.chunk.with_reader(|reader| plan.execute(reader))? else {
            return Ok(Response::default());
        };
        let last_block = output.last_block();

        Ok(Response {
            json_lines: output.into_json_lines(),
            last_block: Some(last_block)
        })
    }

    /// The client's query narrowed to this chunk's bounds, which replays the run
    /// against any copy of these blocks.
    fn replay_query(&self) -> String {
        let Ok(serde_json::Value::Object(mut query)) = serde_json::from_slice(&self.query.body) else {
            return truncate_query(String::from_utf8_lossy(&self.query.body).into_owned());
        };
        let last_block = self.bounds.last_block.unwrap_or(self.chunk.last_block());
        query.insert("fromBlock".into(), self.first_block().into());
        query.insert("toBlock".into(), last_block.into());
        match &self.bounds.parent_block_hash {
            Some(hash) => query.insert("parentBlockHash".into(), hash.clone().into()),
            None => query.remove("parentBlockHash")
        };
        truncate_query(serde_json::Value::Object(query).to_string())
    }

    fn chunk_range(&self) -> String {
        format!("{}-{}", self.chunk.first_block(), self.chunk.last_block())
    }
}

fn log_difference(run: &Run, outcome: &Outcome, difference: &Difference) {
    let Outcome::Compared {
        legacy,
        new,
        legacy_blocks,
        new_blocks,
        ..
    } = outcome
    else {
        return;
    };
    error!(
        dataset = %run.query.dataset_id,
        chunk = run.chunk_range(),
        line = difference.line,
        block = difference.block,
        path = difference.path,
        legacy = difference.legacy,
        new = difference.new,
        legacy_blocks,
        new_blocks,
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        query = run.replay_query(),
        query_bytes = run.query.body.len(),
        "shadow engine answered differently"
    );
}

fn log_failure(run: &Run, legacy: &Result<(), String>, new: &Result<(), String>) {
    error!(
        dataset = %run.query.dataset_id,
        chunk = run.chunk_range(),
        legacy_error = legacy.as_ref().err(),
        new_error = new.as_ref().err(),
        query = run.replay_query(),
        query_bytes = run.query.body.len(),
        "shadow engine failed where the other answered"
    );
}

fn log_slow(run: &Run, outcome: &Outcome) {
    let Outcome::Compared {
        legacy,
        new,
        new_blocks,
        ..
    } = outcome
    else {
        return;
    };
    warn!(
        dataset = %run.query.dataset_id,
        chunk = run.chunk_range(),
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        ratio = new.as_secs_f64() / legacy.as_secs_f64().max(f64::MIN_POSITIVE),
        blocks = new_blocks,
        query = run.replay_query(),
        query_bytes = run.query.body.len(),
        "shadow engine is slower"
    );
}

fn timed(run: impl FnOnce() -> anyhow::Result<Response>) -> EngineRun {
    let started = Instant::now();
    let response = guarded(run);
    EngineRun {
        response,
        elapsed: started.elapsed()
    }
}

/// An engine's error or panic, as the text a log line carries.
fn guarded<T>(run: impl FnOnce() -> anyhow::Result<T>) -> Result<T, String> {
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(format!("{err:#}")),
        Err(panic) => Err(format!("panicked: {}", panic_message(&panic)))
    }
}

fn truncate_query(mut query: String) -> String {
    if query.len() > MAX_LOGGED_QUERY_BYTES {
        let mut cut = MAX_LOGGED_QUERY_BYTES;
        while !query.is_char_boundary(cut) {
            cut -= 1;
        }
        query.truncate(cut);
        query.push_str("…<truncated>");
    }
    query
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload")
}

/// The queries logged in detail in the current [`LOG_WINDOW`].
struct LogWindow {
    started: Instant,
    /// Query fingerprints and reasons.
    logged: HashSet<(u64, &'static str)>
}

impl LogWindow {
    /// Whether this query may be logged in detail for this reason now.
    fn claim(&mut self, fingerprint: u64, reason: &'static str, limit: usize) -> bool {
        if self.started.elapsed() >= LOG_WINDOW {
            self.started = Instant::now();
            self.logged.clear();
        }
        self.logged.len() < limit && self.logged.insert((fingerprint, reason))
    }
}
