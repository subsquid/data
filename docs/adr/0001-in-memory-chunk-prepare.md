# ADR 0001 — Prepare unspilled chunks in memory

Status: proposed (PR #98) · Date: 2026-07-16 · Scope: `sqd-data-core`, `sqd-hotblocks`

## Context

Hotblocks ingest flushes a chunk on every upstream response, which at chain head means
every block. Each flush built a full out-of-core `ChunkProcessor` — the spill machinery
`crates/archive` uses to sort chunks of millions of rows — for what is usually a single
block: several hundred temp files created, written, read back and torn down per flush
(488 for the EVM schema, 229 for Solana; one per buffer per non-sort-key column).

Measured on production-class hardware ([2026-07-16 flush bench]): a head flush costs
~52 ms, ~90% of it kernel time, and the cost tracks the schema's file count, not the
payload. A fleet of N datasets pays N × flush-rate × ~52 ms of CPU regardless of data
volume — at 45 datasets roughly 10 cores of syscall and inode churn, growing linearly
with the roster.

The builder side already bounds nearly every unspilled chunk: `maybe_flush` moves
builder contents into the processor once they exceed 30 MiB. The one exception is a
flush triggered by the row-count bound (checked before the byte bound), whose final
block can be arbitrarily large. At head every chunk fits whole.

## Decision

At `finish()`, if no processor exists and the builder is within the 30 MiB spill bound,
prepare the chunk straight from the builder: sort and downcast in memory into a
`TableReader::Mem`-backed `PreparedTable` (`PreparedTable::from_slice`,
`ChunkBuilder::prepare_in_memory`). Chunks that did spill, or an unspilled chunk above
the bound (the row-count-flush exception), keep the disk path bit-for-bit unchanged.
The decision point is the existing spill bound — no new threshold, flag, or config.

## Consequences

- A head flush drops from 52.4 ms to 0.224 ms (−99.6%) on production hardware; no temp
  files, no page-dirtying churn ([2026-07-16 flush bench]).
- Differential tests pin observational equality with the spill path — schemas including
  chunk-wide downcast, row content and order, partial reads; sorted/plain tables,
  strings, lists, nulls, empty tables: `crates/data-core/tests/in_memory_prepare.rs`.
- Chunk data now briefly occupies anon heap instead of page cache. Bounded by the
  30 MiB spill bound × ≤3 in-flight chunks per dataset; a load probe at ~150× the
  per-dataset head rate plateaus ~110 MB above the old path (jemalloc-retained churn,
  no leak shape): `crates/hotblocks/tests/peak_rss.rs`.
- Row order within equal full sort keys is unspecified and may differ from the spill
  path. Ties exist in real schemas (EVM statediffs, Solana instructions) but are not
  client-visible: queries re-sort output by a row-unique primary key. Today the orders
  coincide byte-for-byte anyway (unstable-sort identity on already-sorted input, pinned
  by test).
- `PreparedTable::into_processor()` errors for in-memory tables — processor reuse
  remains possible only on the disk path.

## Alternatives considered

- **Processor reuse** (restore the pre-`dbb896f` spare-cell): measured idealized ceiling
  −78% at head, but the chunk-level reuse API was removed with optional-table support
  (`0550396`), and naive reuse breaks on tables dropped after prepare. Kept as a
  documented fallback, not chosen.
- **Coalescing flushes at head**: proportional win, but trades freshness — hotblocks'
  core value.
- **Relocating TMPDIR / tmpfs**: a measurement tool, not a fix — syscall and allocation
  overhead stays, and production temp storage is already NVMe.

[2026-07-16 flush bench]: ../measurements/2026-07-16-flush-bench-e96c877.md
