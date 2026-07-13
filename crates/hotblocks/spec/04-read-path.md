# 04 — Read path (queries and watermarks)

This document specifies every read-side operation: the ranged query (live and
finalized-only), watermark/status reads, and the retention admin operation. The wire
mapping lives in [13-interface-binding.md](13-interface-binding.md).

## 1. Operations

| Operation | Input | Output |
|---|---|---|
| `QUERY` | DEF-13 query, `finalized_only = false` | streamed block emissions + response watermarks, or error |
| `QUERY-FINALIZED` | DEF-13 query, `finalized_only = true` | same, restricted to the finalized prefix |
| `HEAD` | dataset | `head(D)` Ref or "none" |
| `FINALIZED-HEAD` | dataset | `fin(D)` Ref or "none" |
| `STATUS` | dataset | kind, retention policy, `first`, `head` (+hash, time), `fin` |
| `GET-RETENTION` / `SET-RETENTION` | dataset (+policy) | current policy / acceptance |

## 2. Query admission

- **RP-1 (Validation).** A query MUST be rejected with `MALFORMED_REQUEST` if structurally
  invalid: unparsable, unknown fields, `to < from`, more than `P-MAX-ITEM-REQ` item
  requests, or dialect-specific rule violations. Validation failures MUST be detected
  before any partial response bytes are emitted.
- **RP-2 (Dialect gate).** A query whose dialect differs from the dataset kind MUST fail
  with `KIND_MISMATCH`. A dialect that is expressible but not supported by this service
  MUST fail with `UNSUPPORTED_QUERY`. Neither may crash the process or degrade other
  requests (FM-1).
- **RP-3 (Admission control).** Under resource exhaustion the service MUST refuse new
  query work with `OVERLOADED` (retryable) rather than queue unboundedly or collapse
  (PF-9). Admission decisions are made before the success/error status is committed.

## 3. Effective range and waiting

Let the query's snapshot be `S` (taken per INV-20 after any waiting completes).

- Live query: `hi(S) = head(S).number` (undefined if `seg = ∅`); finalized-only:
  `hi(S) = fin(S).number` (undefined if `fin = ⊥`). An undefined `hi` behaves as
  `from > hi` (RP-5/RP-6). Effective range: `[from, min(to, hi(S))]` with `to = ∞` when
  omitted.
- **RP-4 (Below the window).** If `from < first(S)` the query MUST fail with
  `RANGE_UNAVAILABLE` (not silently serve a later sub-range). The error SHOULD state the
  lowest available block.
- **RP-5 (Bounded wait / long-poll).** If `from > hi(S)` at admission, the service MUST
  wait up to `P-HEAD-WAIT` for the watermark to reach `from`. If reached, proceed with a
  fresh snapshot; else respond `NO_DATA` (empty, retryable). `NO_DATA` responses SHOULD
  carry current watermarks so pollers can pace themselves. The number of concurrent
  waiters MAY be capped (`P-WAITERS`; overflow → `OVERLOADED`).
- **RP-5b (Tip conflict precedes waiting).** Exception to RP-5. Let `T` be the watermark
  block itself (`head(S)` for a live query, `fin(S)` for finalized-only). If
  `expected_parent ≠ ⊥` and `from = T.number + 1` and `expected_parent ≠ T.hash`, the
  service MUST respond `CONFLICT` immediately (hints per RP-11) instead of waiting:
  client cursor and watermark reference the *same exact position* (`from − 1 =
  T.number`), so the mismatch is a definite fork on any chain, and the immediate signal
  is how a tip-follower learns of a reorg without waiting out `P-HEAD-WAIT`. This
  exception MUST NOT be generalized to `from > T.number + 1`: there `expected_parent`
  names a position the service may simply not have ingested yet (a client ahead of a
  lagging service), a mismatch against `hash_at(from − 1) = T.hash` is not evidence of a
  fork, and `NO_DATA` remains the correct response.
- **RP-6 (Queries without a watermark).** `QUERY-FINALIZED` when `fin = ⊥` — and likewise
  any query on an empty dataset (`seg = ∅`, live `hi` undefined) — behaves as `from > hi`:
  wait, then `NO_DATA`. Neither is an internal error.

## 4. Execution and coverage

- **RP-7 (Snapshot execution).** The whole response is computed against the single
  snapshot `S` (INV-20): one chain, one `fin`, one availability view — regardless of
  concurrent writes, forks, or trims.
- **RP-8 (Item availability).** Availability is range-based (DEF-5) and a response MUST
  never silently omit items of a selected collection. If the selection references an item
  collection absent at the *start* of the effective range, the query MUST fail with
  `ITEM_UNAVAILABLE` identifying the collection. If the collection becomes unavailable
  *inside* the effective range, coverage MUST end before the availability boundary (an
  early stop per RP-9); the continuation query then starts inside the unavailable range
  and fails as above. Coverage never crosses an availability boundary of a selected
  collection.
- **RP-9 (Coverage contract).** A successful response has coverage `[from, L]` with:
  - `from ≤ L ≤ min(to, hi(S))` — never beyond the effective range (INV-27);
  - coverage is gap-free: every block in `[from, L]` was evaluated (INV-21/22);
  - **progress:** if the effective range is non-empty then `L ≥ from` — a successful
    response always covers at least one block (INV-25);
  - the response MAY stop early (`L < min(to, hi(S))`) due to budgets
    (`P-RESP-WEIGHT`, `P-QUERY-TIME`) — early stop is normal, not an error;
  - **coverage MUST be recoverable by the client — the carrier is emission itself:** the
    block at the coverage end (the highest stored block ≤ `L`; = block `L` itself when
    `L` is a stored block) MUST be emitted, as a **header-only record** when it matches
    nothing — even with `include_all` unset. Consequently the client always advances
    with `L` = the last emitted block's number, and a successful response over an
    effective range containing at least one stored block emits at least one block. The
    service MAY additionally emit other covered non-matching blocks as header-only
    records (**boundary markers**); which extra markers appear MAY depend on physical
    layout and is exempt from INV-22 determinism — clients MUST NOT attach meaning to a
    header-only record beyond its header fields and the coverage it witnesses. A
    zero-emission success is legal only when the effective range contains no stored
    block at all, and it asserts coverage of the *entire* effective range (GAP-8 tracks
    the residual hazards of that case).
- **RP-10 (Continuation is client-driven).** The server keeps no cursor. The client
  continues with `from' = L + 1` and `expected_parent` = the hash of its last applied
  block (= `hash_at(L)`, DEF-16) — see §7. Consecutive
  responses MAY come from different snapshots; cross-response consistency is exactly the
  anchored-ancestry guarantee (INV-23), nothing more.

## 5. Emission contract

For coverage `[from, L]` against snapshot `S`:

- Emitted blocks are those in coverage that match the selection (plus all covered blocks
  when `include_all` is set), plus the RP-9 coverage-end record and any header-only
  boundary markers of covered blocks; emitted in strictly ascending number order; each
  emitted at most once (INV-21).
- Each emitted block carries its header (per projection) and, per item collection, **all**
  items of that block matching the item requests, projected to the selected fields —
  never a subset (INV-22). Kind-specific "include related items" rules are deterministic
  functions of `(S, query)`.
- Every emitted item belongs to its enclosing block (`item.block_number = block.number`)
  (INV-21).
- Truncation granularity is the block: a block is emitted whole or not at all (INV-21).
  If even the first covered block exceeds the response budget, it MUST still be emitted
  (progress, INV-25).
- Emission content is deterministic given `(S, query, coverage)` (INV-22); coverage
  itself MAY vary run to run (budgets are time-based).

## 6. Watermark and status reads

- **RP-12 (Honesty).** Every watermark read returns values of some committed state
  (INV-30) — never a forecast, never a mix of two states for the (number, hash) pair of
  one watermark.
- **RP-13 (Freshness/monotonicity).** Reads are real-time monotonic in version order
  (INV-31): a read started after a commit completes reflects that commit or a later one.
  Head *numbers* are not monotonic (forks); versions are.
- **RP-14 (Status).** STATUS reports a consistent view: kind, retention policy, `first`,
  `head` (number, hash, time when known), `fin` — all from one snapshot.

## 7. Fork handling — the CONFLICT protocol

- **RP-11 (Anchored queries).** When `expected_parent ≠ ⊥`, it asserts the hash of
  `from`'s **preceding block** on `S`'s chain (`hash_at(from − 1)`, DEF-16):
  - if `hash_at(from − 1) ≠ ⊥` on `S` and differs from `expected_parent`, the query MUST
    fail with `CONFLICT` and MUST NOT emit any blocks. Equivalently: the first stored
    block at or above `from` must carry `parent_hash = expected_parent` (INV-2 makes the
    two readings coincide);
  - the `CONFLICT` payload is a non-empty ascending list of **hints**: entries
    `⟨p, hash_at(p)⟩` of `S`'s chain ending at position `from − 1` (or the nearest stored
    position below), containing at least that final entry and SHOULD contain up to
    `P-CONFLICT-WINDOW` predecessors. A hint at a stored position is a true block ref; a
    hint at a hole position carries the hash of that position's preceding block (DEF-16);
  - if `hash_at(from − 1) = ⊥` (window edge, unknown anchor hash), the assertion is
    accepted as-is.
- **Client recovery algorithm (normative for clients, the basis of conformance tests):**

```
cursor = (from, parent_hash | ⊥)
loop:
  r = QUERY(from = cursor.from, expected_parent = cursor.parent_hash, …)
  case OK(emitted, L):        apply(emitted); cursor = (L + 1, own hash_at(L))
                              # = last emitted block's hash; unchanged when nothing emitted
  case NO_DATA:               wait / re-poll
  case CONFLICT(hints):       a = highest hint h with local_chain.has(h)
                              if a exists: unapply local blocks > a.number; cursor = (a.number + 1, a.hash)
                              else:        unapply local blocks ≥ lowest(hints).number;
                                           cursor = (lowest(hints).number, ⊥)   # walk further back / restart
  case RANGE_UNAVAILABLE:     local history is older than the window — restart from STATUS.first
  case OVERLOADED:            backoff, retry
```

- `local_chain.has(⟨p, h⟩)` is evaluated per DEF-16: the client's own `hash_at(p)` equals
  `h` — its highest applied block at or below `p` has hash `h` (an exact block match on a
  densely-numbered chain).
- The protocol guarantees: a client that always supplies `expected_parent` never applies
  two blocks from different forks at adjacent heights without an intervening explicit
  rollback (INV-23). Reorg recovery cost is bounded by reorg depth /
  `P-CONFLICT-WINDOW` round trips.

## 8. Error taxonomy

Every failure of a read operation MUST map to exactly one class (INV-26); classes are
stable API surface. An error response carries no block data.

| Class | Trigger | Retryable |
|---|---|---|
| `MALFORMED_REQUEST` | RP-1 violations | no — fix the request |
| `UNSUPPORTED_QUERY` | dialect expressible but not served here | no |
| `KIND_MISMATCH` | dialect ≠ dataset kind | no |
| `UNKNOWN_DATASET` | dataset not configured | no |
| `RANGE_UNAVAILABLE` | `from < first` (window moved past it) | no — re-anchor upward |
| `ITEM_UNAVAILABLE` | selection touches an absent item collection (RP-8) | no |
| `NO_DATA` | `from` above watermark after bounded wait (RP-5/6) | yes — poll |
| `CONFLICT` | anchored-ancestry mismatch (RP-11) | yes — after re-anchoring |
| `OVERLOADED` | admission control (RP-3) / waiter cap | yes — backoff |
| `FORBIDDEN` | SET-RETENTION on a non-External dataset | no |
| `INTERNAL` | anything else; MUST be a defect, tracked by OB-5 error budget | maybe |

- **RP-15 (Streaming truncation caveat).** Once a success status is committed and
  streaming has begun, a mid-stream failure MUST terminate the stream such that the bytes
  already sent remain a valid, well-formed prefix per §5 (a decodable response whose
  emitted blocks satisfy all response invariants). The truncation is then observationally
  identical to a budget stop; clients rely on RP-9/RP-10, not on transport signals, for
  completeness. Such truncations MUST be counted observably (OB-4) and SHOULD be signaled
  in-band where the binding allows.
- **RP-16 (No partial-then-error).** An operation MUST NOT emit block data and then
  report an error class for the same response; errors are decided at admission /
  first-computation time (before the success status), truncation afterwards (RP-15).

## 9. Resource bounds (read side)

- **RP-17** Per-response resource use is bounded: emission weight by `P-RESP-WEIGHT`
  (+ one block), buffering by `P-RESP-FLUSH`, execution wall time by `P-QUERY-TIME`
  (LIV-3). Request size is bounded by `P-BODY-LIMIT`.
- **RP-18** A slow or disconnected client MUST cost only its own bounded buffers; its
  query MUST be cancelled/abandoned promptly after disconnect and MUST NOT pin snapshots
  or execution slots beyond `P-QUERY-TIME` (CN-7, HZ-9).
