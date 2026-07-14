# 02 — Data model

This document defines the abstract state on which all other documents operate. Nothing here
prescribes a storage layout; it prescribes what must be *representable* and what the
observable structure of the data is.

## 1. Primitive types

**DEF-1 (Block number).** A natural number `n ∈ ℕ`, strictly increasing along a chain. It does
**not** necessarily increase by exactly 1: Solana numbers blocks by time-based **slots**, and a
slot that produced no block leaves a hole. Numbering therefore carries no structural guarantee —
`parent_number` (DEF-4) does.

**DEF-2 (Hash).** An opaque, non-empty string. Hashes are compared by exact string
equality; the service assigns no meaning to their content. All sources of a dataset MUST
use one consistent representation (a representation mismatch between sources is a source
fault, [FM-SRC-7](08-failure-model.md)).

**DEF-3 (Block reference / Ref).** A pair `⟨number, hash⟩` identifying one block.

**DEF-4 (Block).** A tuple:

```
Block = ⟨ number : ℕ,
          hash : Hash,
          parent_number : ℕ,          — the number of the parent block
          parent_hash : Hash,
          payload : Payload(kind),
          time : Timestamp | ⊥ ⟩
```

`parent_number = number − 1` on a densely-numbered chain (evm, hyperliquid); on a slot-numbered
one (Solana) it may sit further down. Always `parent_number < number`; a block violating this
is structurally invalid ([WP-2](03-write-path.md)). `⟨parent_number, parent_hash⟩` is what makes
a run of blocks a chain — the numbering alone does not (DEF-1).

`time` is informational; no correctness property depends on it (clock-free correctness,
[CN-8](05-consistency-and-durability.md)).

**DEF-5 (Kind and payload schema).** Every dataset has an immutable **kind** — a chain
family identifier (e.g. `evm`, `solana`, `bitcoin`, `tron`, `hyperliquid-fills`,
`hyperliquid-replica-cmds`). A kind defines:

- the **header** attributes of a block;
- a fixed set of named **item collections** (e.g. transactions, logs, traces, state diffs,
  instructions), where each **item** is a record carrying (at least) the number of the
  block it belongs to, plus kind-specific attributes;
- the **query dialect**: which filter predicates and field selections are expressible;
- optional **availability**: an item collection MAY be absent for sub-ranges of the chain
  (the source did not provide it). Availability is a property of stored data and is
  reported per query ([RP-8](04-read-path.md)).

The internal encoding of payloads is out of scope; §6 of [12-conformance-tdd.md](12-conformance-tdd.md)
defines the kind-agnostic structural checks a harness can still perform.

## 2. Dataset state

**DEF-6 (Dataset).** A dataset `D` is the tuple:

```
D = ⟨ kind    : Kind,                      — immutable after creation
      seg     : ⟨b_1 … b_n⟩, n ≥ 0,        — the stored chain segment (window)
      anchor  : ⟨number : ℕ ∪ {−1}, hash : Hash | ⊥⟩,  — a position below seg (DEF-7)
      fin     : Ref | ⊥,                   — finalized watermark
      ret     : RetentionPolicy,
      ver     : ℕ ⟩                        — commit version, increments on every transition
```

Derived values (defined when `seg` is non-empty):

```
first(D) = b_1.number          head(D) = ⟨b_n.number, b_n.hash⟩
next(D)  = head(D).number + 1  (if seg empty: anchor.number + 1)
span(D)  = n                   (window size in blocks)
```

A dataset state is **well-formed** iff it satisfies the structural invariants
INV-1 … INV-7 of [06-invariants.md](06-invariants.md). Every externally observable state
MUST be well-formed.

**DEF-7 (Anchor).** `anchor` names a position below `first(D)` — on a densely-numbered
chain exactly `first(D) − 1`; on a slot-numbered chain possibly lower (a RETAIN may land
on a hole, and the next stored block may sit above it — INV-3). When its hash is
known, it carries `hash_at(anchor.number)` (DEF-16): on a densely-numbered chain the hash of
the block at that position; on a slot-numbered chain the hash of the nearest block at or
below it (the position itself may be a hole). The anchor is how a window "remembers" its
connection to the chain below the window. `anchor.hash = ⊥` is permitted (e.g. a window
starting at genesis — `anchor = ⟨−1, ⊥⟩` — or an operator-supplied start without a hash);
when it is `⊥`, linkage of `b_1` cannot be verified and is accepted as-is.

**DEF-16 (Preceding block; chain hash at a position).** The **preceding block** of a
position `n` is the highest stored block numbered below `n` — the block anything starting
at `n` must link to. Its hash is `hash_at(n − 1)`:

```
hash_at(p) = hash of the highest stored block with number ≤ p,
             or anchor.hash when no stored block lies at or below p and p ≥ anchor.number;
             undefined below the anchor
```

On a densely-numbered chain the preceding block of `n` is simply block `n − 1`; on a
slot-numbered chain it is the nearest block below `n`, wherever it sits. Everywhere the
protocol anchors a client or a source to a position — the anchor (DEF-7),
`expected_parent` (DEF-13), fork-signal hints (DEF-12), CONFLICT hints
([RP-11](04-read-path.md)), continuation cursors ([RP-10](04-read-path.md)) — a pair
`⟨p, h⟩` reads "`h` is the hash of the preceding block of `p + 1`" (`h = hash_at(p)`),
**not** "a block numbered `p` has hash `h`". The two readings coincide exactly on
densely-numbered chains.

**DEF-8 (Finalized watermark).** `fin`, when defined, designates a stored block
(`first ≤ fin.number ≤ head.number`, hash matching the stored block — INV-5/6) that the
sources have declared irreversible. Blocks at or below `fin.number` form the **finalized
prefix**; blocks above it are the **volatile suffix**, subject to replacement by forks.

**DEF-9 (Retention policy).** One of:

| Policy | Meaning |
|---|---|
| `Window(k)` | Keep (at least) all stored blocks in the last `k` **positions** below `next(D)`; trim the rest as the head advances. `k` counts numbers, not stored blocks: on a slot-numbered chain the window holds ≤ `k` blocks. `k ≥ 1`. |
| `Pinned(from, hash?)` | Keep everything from block `from` upward; `hash?` optionally asserts the anchor at `from − 1`. |
| `External` | The lower bound is set at runtime by the retention controller via the SET-RETENTION operation. Until first set: unbounded, and a dataset that is *empty* at activation defers ingestion until the first instruction (WP-5). |
| `Unbounded` | Never trim. |

**DEF-10 (Version order).** For one dataset, committed states are totally ordered by
`ver`. "Later state" always means greater `ver`, never a comparison of head numbers (a
fork can lower the head number while increasing `ver`).

## 3. Snapshots

**DEF-11 (Snapshot).** An immutable, well-formed dataset state as of some committed
version. Every read operation is evaluated against exactly one snapshot
([INV-20](06-invariants.md)). Snapshots of different datasets are independent; no
operation observes a cross-dataset "consistent cut", and none is guaranteed to exist.

## 4. Source-facing events

Ingestion consumes an abstract event stream per dataset (how it is transported is a
binding concern):

**DEF-12 (Source events).**

| Event | Content | Meaning |
|---|---|---|
| `Blocks(⟨b…⟩)` | contiguous, internally linked run of blocks | candidate extension of the chain at the position the service requested |
| `ForkSignal(hints)` | non-empty ascending list of `⟨position, hash⟩` pairs, read per DEF-16 (a hint at a stored position of the source's chain is a true block ref; at a hole it carries the preceding block's hash) | the source disagrees with the service's requested position; `hints` name recent positions of the source's canonical chain near the divergence point |
| `FinalitySignal(ref)` | a Ref | the source declares `ref` (and everything below it on its chain) final |
| `OnTip` | — | the source believes the service has caught up with its tip |

Delivery is at-least-once, per source; ordering is guaranteed only within one source's
stream between reconnects. Multiple sources are reconciled by arbitration
([WP-4](03-write-path.md)).

## 5. Query-facing objects

**DEF-13 (Query).** A read request:

```
Query = ⟨ dialect         : Kind,               — must match the dataset kind
          from            : ℕ,                  — inclusive lower bound
          to              : ℕ | ⊥,              — inclusive upper bound; ⊥ = open-ended
          expected_parent : Hash | ⊥,           — asserted hash of from's preceding block (DEF-16)
          finalized_only  : bool,               — restrict to the finalized prefix
          selection       : kind-specific filters (item requests) and field projections,
          include_all     : bool ⟩              — emit headers even for blocks with no matches
```

**DEF-14 (Coverage).** The result of a successful query is defined by its **coverage**:
a contiguous range `[from, L]` of block numbers, `L ≥ from`, fully processed against the
query's snapshot. The response *emits* the subset of covered blocks required by
`selection`/`include_all`; coverage — not emission — is what the client uses to make
progress ([RP-9](04-read-path.md)).

**DEF-15 (Watermark reads).** Point reads returning the current committed `head`, `fin`,
retention policy, and dataset status. Defined in [04-read-path.md §6](04-read-path.md).

**DEF-17 (Hash indexes).** Two optional per-dataset **partial** maps over the current state:

```
bidx(D) : Hash ⇀ ℕ            — block hash → block number
tidx(D) : Hash ⇀ ℕ × ℕ        — transaction hash → ⟨block number, transaction index⟩
```

They are **derived**, never primary: everything they name already lives in `seg`, no
transition reads them, and dropping them loses no information. They are **partial by
design** — a hash may be absent from an index while its block sits in `seg`. Absence
therefore says nothing about `seg`; only presence does ([RP-19](04-read-path.md)). The
sources of partiality are structural, not incidental: an index is enabled per deployment
(`P-BLOCK-INDEX` / `P-TX-INDEX`), it is never backfilled, so blocks ingested while it was
off stay unnamed for as long as they remain in the window, and it is defined only for
kinds whose schema exposes the hash (EVM today).

Within one dataset each index is a function: a block hash names at most one block, a
transaction hash at most one transaction *of the current chain*. Across branches the two
differ — a block hash belongs to exactly one branch forever, while a transaction hash is
routinely re-included at a different position after a reorg ([INV-47](06-invariants.md)).

## 6. Transitions (summary)

The complete write-side vocabulary; semantics in [03-write-path.md](03-write-path.md):

| Transition | Effect on ⟨seg, anchor, fin⟩ |
|---|---|
| `CREATE(kind, ret)` | new empty dataset with an initial anchor per policy |
| `EXTEND(B, f?)` | append blocks at the tail; optionally advance `fin` |
| `REPLACE(from, B, f?)` | fork: atomically substitute the suffix `≥ from` with `B` |
| `FINALIZE(r)` | advance `fin` (monotone, clamped to the stored chain) |
| `RETAIN(from, h?)` | trim the prefix `< from`; move the anchor up |
| `RESET(a)` | clear the segment, re-anchor at `a` (self-healing / re-targeting) |
| `DROP` | remove the dataset and all its data |

Each transition is atomic and increments `ver` exactly once — except CREATE, which
*establishes* `ver = 0`: it starts the version sequence rather than extending one. A
single commit MAY compose `EXTEND`/`REPLACE` with `FINALIZE` (blocks and a finality
advance arriving together); invariants are evaluated at commit points only.

## 7. Terminology cross-reference

| Term used elsewhere | Defined as |
|---|---|
| window | `seg` (DEF-6) |
| head / first block | derived values of DEF-6 |
| finalized prefix / volatile suffix | DEF-8 |
| anchor | DEF-7 |
| preceding block / `hash_at(p)` | DEF-16 |
| coverage | DEF-14 |
| fork hints | `ForkSignal.hints` (DEF-12), also the payload of the CONFLICT error (RP-11) |
| hash index / `bidx`, `tidx` | DEF-17 |
| batch | the unit of one `EXTEND`/`REPLACE` commit (bounded by P-BATCH-ROWS / P-BATCH-BYTES) |
