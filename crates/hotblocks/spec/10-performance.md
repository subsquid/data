# 10 — Performance model

This document makes performance testable: a workload vocabulary, precise SLI definitions,
target SLOs (provisional until ratified), resource-bound requirements, and a hazard
register that tells the TDD framework where degradation is expected to hide.

## 1. Workload model

A benchmark/soak scenario is described by these parameters (per dataset unless noted):

| Param | Meaning | Typical envelope |
|---|---|---|
| `W-DATASETS` | concurrently hosted datasets (global) | 1 … ~50 |
| `W-BLOCK-RATE` | source block production rate | 0.1 … 50 blocks/s |
| `W-BLOCK-SIZE` | payload size distribution incl. heavy tails | 1 KB … 10s of MB |
| `W-ITEM-DENSITY` | items per block per collection | sparse … 10⁴ |
| `W-REORG` | fork frequency × depth distribution | rare-shallow … storm (bounded by window) |
| `W-FINALITY-LAG` | source finality distance behind tip | 0 … window size |
| `W-RETENTION` | policy + `k` | minutes … months of blocks |
| `W-QUERY-MIX` | tip-followers (small anchored ranges, high rate) / backfills (long ranges) / watermark pollers | site-specific |
| `W-CLIENTS` | concurrent clients incl. herd events | 1 … 10⁴ |

Reference scenarios (used by CT-6/CT-7, defined precisely in the harness):
`S1 steady-tip` (all datasets tip-following + poller-heavy query mix), `S2 backfill-storm`
(cold clients scanning whole windows), `S3 reorg-storm`, `S4 churn-soak` (days of S1 +
retention churn), `S5 cold-start` (boot with large existing state under S1 load),
`S6 noisy-neighbor` (one dataset driven to saturation, others on S1).

## 2. Service-level indicators (definitions)

All SLIs are measured black-box by the harness (source simulator + client driver +
observability), per dataset unless noted.

| SLI | Definition |
|---|---|
| SLI-1 **Ingest lag** | time from the source simulator offering block `b` at the service's resume position to `b` being query-visible (commit observed). Steady-state distribution + worst interval. |
| SLI-2 **Query latency** | admission→first byte (TTFB) and admission→completion, per query class (tip / backfill / watermark). |
| SLI-3 **Stream throughput** | decompressed payload bytes/s and blocks/s sustained by one backfill stream; aggregate under `W-CLIENTS`. |
| SLI-4 **Watermark read latency** | HEAD/STATUS round trip. |
| SLI-5 **Startup-accept time** | process start → first accepted connection (LIV-5a). |
| SLI-6 **Startup-ready time** | process start → dataset readable (per dataset; LIV-5b), and → all ready. |
| SLI-7 **Recovery time** | crash → LIV-5 readiness (breakdown by phase via OB-8). |
| SLI-8 **Space amplification** | `disk_bytes / Σ live_bytes` sampled through churn (RS-6). |
| SLI-9 **Stall time** | longest interval with zero commit progress on any dataset whose source offers data (LIV-2); plus count of intervals > 1 s. |
| SLI-10 **Error budget** | rate of `INTERNAL` (must be ~0), rate of `OVERLOADED` under nominal load (must be 0), truncation rate (RP-15). |
| SLI-11 **Head granularity** | distribution of head advancement step size and inter-commit interval at fixed `W-BLOCK-RATE` (batching-induced freshness quantization, WP-3). |
| SLI-12 **Fork convergence time** | fork signal offered → REPLACE committed (LIV-9a). |

## 3. SLO targets (provisional)

Targets marked ⚠ are initial engineering proposals to be ratified; "baseline" records the
currently known behavior where incident data exists. A conformance run reports pass/fail
per row under the named scenario.

| SLI | Scenario | Target ⚠ | Known baseline |
|---|---|---|---|
| SLI-1 p99 | S1 | ≤ 2 s + batch quantum | — |
| SLI-9 max | S1/S4/S6 | ≤ 5 s; zero intervals ≥ 60 s | **351–458 s global freezes observed post-deploy** (GAP-1) |
| SLI-2 TTFB p99 (tip) | S1 | ≤ 300 ms | — |
| SLI-2 TTFB p99 (backfill) | S2 | ≤ 2 s | — |
| SLI-3 per stream | S2 | ≥ 50 MB/s decompressed | — |
| SLI-4 p99 | S1 | ≤ 50 ms | — |
| SLI-5 | S5 | ≤ 3 s, independent of state size | **~35 s refused-connection window observed** (GAP-7) |
| SLI-6 all-ready | S5 | ≤ 60 s for ~50 datasets of nominal size | ~35 s+ (same incident) |
| SLI-7 | S5 | SLI-5/6 bounds + no data loss (INV-40) | — |
| SLI-8 | S4 | ≤ 2.0× steady; bounded monotone convergence after churn bursts | unbounded growth in default config until 2026-07; reclaim path since fixed, bound unmeasured (GAP-6) |
| SLI-10 INTERNAL | all | 0 per 10⁶ requests | unsupported dialects are classified and counted by CT-5; target not load-tested |
| SLI-11 | S1 | inter-commit ≤ max(1 s, 1/`W-BLOCK-RATE`); no artificial multi-second quantization at high rates | batch bounds imply stepping (HZ-6) |
| SLI-12 p99 | S3 | ≤ 2 s + one batch time | — |

## 4. Resource-bound requirements

- **PF-1 (Memory ceiling).** Total process memory is bounded by a configuration-derivable
  ceiling: per-query buffers (≤ `P-RESP-WEIGHT` + `P-RESP-FLUSH` order), per-dataset
  ingest accumulation (≤ `P-BATCH-BYTES` order — including for pathological payloads:
  GAP-13), caches (explicit sizes), executor concurrency (`P-EXEC-SLOTS`). No workload may
  drive memory unboundedly (test: CT-6 with adversarial `W-BLOCK-SIZE`/`W-ITEM-DENSITY`).
  Caveat: the read side's "+ one block" allowance (RP-17, INV-25) is configuration-derivable
  only if single-block size is bounded at ingest — no such bound exists today
  (`P-MAX-BLOCK-BYTES`, GAP-37).
- **PF-2 (End-to-end backpressure).** Every producer→consumer edge in the write pipeline
  is bounded-buffer; source intake slows when commits slow. Corollary: backpressure is the
  designed response to storage pressure — but it must respect LIV-2/LIV-8 (bounded stall,
  no cross-dataset halt).
- **PF-3 (Admission control, read side).** RP-3: refuse beyond capacity with
  `OVERLOADED`; admitted work completes within LIV-3. Under saturation the system sheds
  load; it does not thrash (LIV-10).
- **PF-4 (Fairness).** Shared execution capacity is apportioned such that no query class
  or dataset can starve others indefinitely (HZ-7); a documented fair-share/priority
  policy governs contention.
- **PF-5 (Maintenance budget).** Background maintenance (reclamation, reorganization) runs
  within an I/O/CPU budget that preserves LIV-2 and SLI-2 targets while still meeting
  LIV-7 (a two-sided constraint: too little maintenance breaks space/liveness later, too
  much breaks latency now).
- **PF-6 (Startup work scheduling).** Work at boot is split: what must precede accepting
  connections (minimal), what must precede per-dataset readiness (that dataset's
  recovery), what can run behind serving (boot maintenance where reader-safety allows,
  RS-8 constraints permitting). LIV-5 encodes the bounds.

## 5. Hazard register

Known mechanisms by which this class of system degrades. Each hazard names the property it
threatens and the probe the TDD framework should build. These are *risk pointers*, not
defect reports — the dated defect list lives in the gap register (12 §6).

| HZ | Mechanism | Threatens | Probe |
|---|---|---|---|
| HZ-1 | Shared storage write path: engine-level backpressure/stall is global, halting all datasets at once | LIV-2, LIV-8 | S6 + storage-pressure injection; assert per-dataset stall budgets (GAP-1) |
| HZ-2 | Maintenance debt accumulation (deferred deletion → reorganization backlog → write throttling) | LIV-2, SLI-2 | S4 churn soak with OB-3/OB-6 tracking |
| HZ-3 | Boot work scaling with total state (recovery scans, residue purge) | LIV-5, SLI-5/6 | S5 with grown state; regression-track SLI-5 vs state size |
| HZ-4 | Retention/trim bursts (large trims committed at once) | SLI-1/2 latency spikes | step-function retention instructions under load |
| HZ-5 | Space amplification: churn outrunning reclamation; residue pinning (one dataset's artifact blocking global reclamation) | RS-6, LIV-7 | S4 with per-dataset churn skew; residue-age assertions (GAP-6) |
| HZ-6 | Freshness quantization: batch bounds delay head advance at low block rates or large blocks | SLI-1, SLI-11 | sweep `W-BLOCK-RATE` × `W-BLOCK-SIZE`; assert flush-on-tip behavior (WP-3) |
| HZ-7 | Heavy backfill queries starving tip-followers on shared executor capacity | SLI-2 (tip), PF-4 | S2+S1 mixed; per-class latency split |
| HZ-8 | Waiter herds: mass long-poll release on one commit (thundering release), waiter-cap exhaustion | LIV-4, SLI-10 | herd scenario: 10⁴ waiters, single block arrival |
| HZ-9 | Long-lived read snapshots pinning superseded data (MVCC bloat) if response lifetimes are ever unbounded | CN-7, RS-6 | slow-reader clients; assert snapshot lifetime ≤ `P-QUERY-TIME` |
| HZ-10 | Ancestry checks / conflict-hint construction scanning linearly with window size | SLI-2 (anchored tip queries) | anchored-query latency vs window size sweep |
| HZ-11 | Transaction/commit retry storms under contention (optimistic concurrency) | SLI-1, LIV-2 | multi-writer-pattern stress within one process; commit-retry observability |
| HZ-12 | Compression/encoding cost dominating small hot responses | SLI-2 TTFB | encoding on/off comparative bench |

## 6. Performance testing requirements

- **PF-7 (Baselining).** Every SLI has a recorded baseline per reference scenario;
  CI-grade runs compare against baselines with explicit tolerances; regressions beyond
  tolerance fail.
- **PF-8 (Saturation characterization).** For each scenario the harness records the knee
  (throughput at which SLOs first break) so capacity planning is data, not folklore.
- **PF-9 (Overload behavior is part of the contract).** Benchmarks MUST include
  beyond-capacity phases asserting RP-3/LIV-10 behavior (shed-and-recover), not only
  below-capacity happy paths.
