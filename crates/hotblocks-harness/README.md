# Hotblocks test harness

A black-box test harness built against the **hotblocks behavioral specification** — the
implementation-free statement of what the service must do: state model, invariants (`INV-*`),
liveness (`LIV-*`), failure model (`FM-*`), read/write-path requirements (`RP-*` / `WP-*`),
observability (`OB-*`), and the conformance plan (test classes `CT-*`, gap register `GAP-*`).
Those identifiers are the vocabulary of this crate and are cited on every assertion; the spec
documents themselves are maintained separately and land in the repository on their own.

The system under test is the real `sqd-hotblocks` **binary**, spawned as a child process and
driven over HTTP. Nothing here links against its internals: an assertion that cannot be made
through the binding is an assertion a client could not rely on either — and owning the process
is what makes the crash/restart and shutdown classes expressible at all.

```
  script ──┬──▶ SourceSim ──HTTP──▶ [ sqd-hotblocks ] ──HTTP──▶ Client ──▶ validators
           └──▶ Model  ◀──────── compare at quiescence ────────────┘
```

## Running

```bash
cargo test -p sqd-hotblocks-harness          # the harness's own unit tests (model, chain, simulator)
cargo test -p sqd-hotblocks --test ct1_happy_path   # CT-1 — the Phase 0 exit criterion
cargo test -p sqd-hotblocks --test ct9_source_faults
# Explicit endurance lane (ignored by default; includes the reclaim convergence wait):
cargo test -p sqd-hotblocks --test ct7_stall_and_churn ct7_churn_soak -- --ignored
```

The CT tests live in `crates/hotblocks/tests/` because only a test inside that package gets
`env!("CARGO_BIN_EXE_sqd-hotblocks")` — the path to the freshly built binary. Everything
reusable lives here, so a future soak or benchmark runner can use it outside `cargo test`.

## The pieces

| Module | What it is | Spec |
|---|---|---|
| [`sim`](src/sim.rs) | source simulator: scripted chain, fork signals, finality headers, fault knobs | 13 §7, DEF-12 |
| [`model`](src/model.rs) | the reference model — the oracle. Block-exact, well-formedness asserted after every transition | 12 §2 |
| [`driver`](src/driver.rs) | client: the read binding, the structural validators, the anchored follower and backfill scanner | 04 §7, 12 §4 |
| [`compare`](src/compare.rs) | quiescence comparator: diffs every observable, collects *all* violations before failing | 12 §1 |
| [`sut`](src/sut.rs) | process supervisor: config, spawn, readiness, SIGTERM, SIGKILL, restart-on-same-db | — |
| [`chain`](src/chain/) | kind-parametric payloads: `evm`, `solana`, `hyperliquid-fills` | DEF-5 |
| [`harness`](src/harness.rs) | glue: one script drives the simulator and the model in lockstep | 12 §1 |

## What a test looks like

```rust
let mut h = Harness::start(HarnessConfig::from_block(
    env!("CARGO_BIN_EXE_sqd-hotblocks"), Arc::new(HlFills), 1_000
)).await?;

h.produce(50)?;              // the source mints 50 blocks; the model EXTENDs by the same run
h.finalize_with_lag(5)?;     // finality trails the tip, as on a real chain
h.settle().await?;           // wait for the service to converge on the script
h.assert_conforms().await?;  // HEAD, FINALIZED-HEAD, STATUS, METADATA, OB-1 gauges,
                             // and a full-window scan diffed against the model, block for block
```

`assert_conforms` fails with every divergence it found, not just the first, and a failing test
prints the service's own log tail (the HTTP access log filtered out — it is never the
interesting part).

## Design decisions worth knowing

**The simulator closes every response.** The service only commits a batch when the source's
response *ends* and it has caught up with what it was given (`MaybeOnHead`). A simulator holding
one long-lived push stream open would leave every block buffered and invisible — forever, and
silently. So `/stream` serves what exists now and closes.

**And it long-polls.** The service re-requests the instant a response ends, so answering `204`
immediately would spin the ingest loop at full CPU. A request with nothing to serve is held for
`poll_timeout` (200 ms by default) — what a real source does.

**Scans use `includeAllBlocks`.** Coverage is only recoverable by a client when at least one
block is emitted (RP-9); a filter-sparse query returning nothing tells the client nothing about
how far it got (GAP-8). Scanning with `include_all` sidesteps that, so the harness can always
advance. Do not "optimize" it away.

**Numbering may be sparse.** Solana numbers blocks by time-based slots and a slot that produced
nothing leaves a hole, so contiguous *numbering* is not an invariant — being parent and child is
(`Block::parent_number`). The service agrees: it links batches and chunks by hash and never by
number, and a chunk's range starts at the position that was *requested*, not at the first block
that arrived. `Numbering::Sparse` exercises this end to end (`ct1_sparse_numbering`).

**Quiescence is shorter than `P-QUIESCENCE`.** The spec proposes `2 × P-CLEANUP-PERIOD` (20 s),
which is about *space* observables converging after a deferred sweep. Chain-state observables
settle as soon as the write path commits, so the default here is 300 ms of stability. A
space-sensitive class (CT-7) must raise it back.

**Tests run with `--rocksdb-disable-direct-io`.** Direct I/O behaves differently across the
platforms tests run on, and nothing structural depends on it. CT-6/CT-7 must drop that flag —
they are measuring the storage engine, not working around it.

## What CT-1 covers today

Run once per kind — `ct1_evm`, `ct1_solana` (on sparse slots), `ct1_hyperliquid_fills`:

INV-1..3 (structural chain), INV-5/6 (watermark bounds), INV-7 (provenance — payload included),
INV-11 (append), INV-12 (finality monotone), INV-21/22 (response shape and completeness),
INV-23 (anchored ancestry across responses), INV-25 (progress), INV-27 (range honesty),
INV-30 (reporting), RP-5 (bounded wait), RP-9/RP-10 (coverage, client-driven continuation),
OB-1 (chain gauges).

Each kind sets its own traps, and the oracle has to predict all of them: evm serves `timestamp`
in **seconds** while its transaction `nonce` is a plain number among hex strings; solana serves
eight collections that must all be present, `lamports` as a **string**, and a reward `pubkey` as
an **index** the service resolves into an account. A wrong guess there fails as if the service
were broken, which is why the emission oracle sits next to the source payload in the same file.

## What it found on day one

The first green-path run crashed the ingest task: a JSONL body whose final record carried no
trailing newline made `LineStream::take_final_line` leave its scan position past the end of an
emptied buffer, and the next poll indexed out of bounds. The dataset then parked for
`P-EPOCH-RETRY` (60 s) and crash-looped, because the source served the same body again.

Fixed in `crates/data-client/src/reqwest/lines.rs`; pinned by a unit test there and by
`ct9_source_faults` end to end. It is the first entry of the fault corpus, and the reason
`SimFaults` exists.

## Where the next phases plug in

- **CT-2 (crash/restart)** — `Sut::crash()`, `Sut::stop()`, `Sut::restart()` already exist and
  keep the same database directory and port across boots. What is missing is the kill-point
  matrix.
- **CT-4 (fork/finality corpus)** — `Harness::fork()` and the model's `resolve_fork` /
  `Finalize::IntegrityFault` are implemented and unit-tested; the follower implements the
  normative CONFLICT recovery of 04 §7. What is missing is the scripts.
- **CT-5 (error taxonomy)** — `Model::predict_query` returns the outcome class for any query;
  `Client::query` already classifies every response. What is missing is the request matrix.
- **CT-7 (soak)** — the ignored S4 runner in `ct7_stall_and_churn` drives API-controlled
  moving-window retention and samples `total-sst-files-size`, `estimate-live-data-size`, and
  memtable bytes through the existing RocksDB property endpoint. Prometheus OB-2/3/6 series are
  intentionally deferred to a separate change.
- **CT-9 (fuzz)** — `SimFaults` is the injection point.

## Known open questions

- With `includeAllBlocks: false`, the query engine appears to emit a header-only record for
  blocks with no matching items (see `crates/query/fixtures/hyperliquid/queries/coin_fills`).
  Whether that is the coverage carrier, an accident, or something else is unresolved — the model
  predicts emissions only under `include_all` until it is. This is a CT-5/INV-22 question.
