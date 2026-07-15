# 13 — Interface binding (HTTP)

This document binds the abstract operations of 03/04 to the concrete wire protocol so a
black-box conformance harness can be built. It describes the **external contract** of the
current service generation — endpoints, encodings, status mapping — not internal
implementation. Where the binding today falls short of a spec requirement, the row is
annotated with the relevant GAP.

## 1. General

- **IB-1** Transport: HTTP/1.1+; all request/response bodies are JSON unless noted.
  Dataset-scoped paths use `/datasets/{id}` where `{id}` is an identifier of bounded
  length (≤ 48 chars, `[A-Za-z0-9_-]`); syntactically invalid ids are client errors,
  unknown ids map to `UNKNOWN_DATASET` (404).
- **IB-2** Request bodies are plain (uncompressed) JSON, size-limited by `P-BODY-LIMIT`;
  responses to query operations are compressed streams — `zstd` if the client's
  `Accept-Encoding` includes it, otherwise `gzip` (default even without the header), with
  `Content-Encoding` and `Vary: Accept-Encoding` set.
- **IB-3** Correlation: responses carry a request-id header; clients MAY send a client
  identity header used for bounded-cardinality attribution (OB-10).

## 2. Operations → routes

| Abstract op | Route | Notes |
|---|---|---|
| QUERY | `POST /datasets/{id}/stream` | body = DEF-13 query (dialect-tagged JSON) |
| QUERY-FINALIZED | `POST /datasets/{id}/finalized-stream` | same body; `finalized_only` semantics (RP-6) |
| HEAD | `GET /datasets/{id}/head` | `{"number":N,"hash":"…"}` or `null` |
| FINALIZED-HEAD | `GET /datasets/{id}/finalized-head` | same shape |
| STATUS | `GET /datasets/{id}/status` | kind, retention, first/last block (+hash/time), finalized head |
| BLOCK-BY-HASH | `GET /datasets/{id}/hashes/{hash}/block` | `{"number":N,"hash":"…"}`; miss = `NOT_FOUND`, **not** proof of absence (RP-19) |
| TX-BY-HASH | `GET /datasets/{id}/hashes/{hash}/transaction` | `{"blockNumber":N,"transactionIndex":i,"hash":"…"}`; miss = `NOT_FOUND`, **not** proof of absence (RP-19) |
| METADATA | `GET /datasets/{id}/metadata` | start block, real-time flag, aliases |
| GET-RETENTION | `GET /datasets/{id}/retention` | current policy JSON |
| SET-RETENTION | `POST /datasets/{id}/retention` | policy JSON; only for `External` datasets, else `FORBIDDEN` (403) |
| observability | `GET /metrics` (+ engine-diagnostic routes) | OB surface, text formats |

Dialects accepted in query bodies: `evm`, `solana`, `bitcoin`, `tron`,
`hyperliquidFills`, `hyperliquidReplicaCmds` — and, expressible in the query schema but
**not served** by this system generation, `substrate`, `fuel` (MUST map to
`UNSUPPORTED_QUERY`).

## 3. Query request (DEF-13 binding)

```jsonc
{
  "type": "evm",                    // dialect tag = dataset kind
  "fromBlock": 123,                 // required, inclusive
  "toBlock": 456,                   // optional, inclusive
  "parentBlockHash": "0x…",         // optional = expected_parent: hash of fromBlock's preceding block (DEF-13/16)
  "includeAllBlocks": false,        // optional
  "fields": { … },                  // per-table field projections
  // item requests (dialect-specific arrays), each ≤ selector rules, total ≤ P-MAX-ITEM-REQ:
  "transactions": [ {…} ], "logs": [ {…} ], …
}
```

`fromBlock ≥ 0`. For `fromBlock = 0` there is no preceding position on the wire:
`parentBlockHash` MUST be omitted (the genesis anchor `⟨−1, ⊥⟩` carries no hash — DEF-7),
and CONFLICT hints can never reference position `−1` (RP-11's "nearest stored position
below" never applies below genesis).

## 4. Query response stream

- **IB-4** Success body: one compressed stream whose decompressed content is
  **JSON Lines** — exactly one JSON object per emitted block, in ascending order:
  `{"header":{…}, "<collection>":[…], …}`. This is the emission contract of 04 §5; the
  §4-of-12 validators parse it generically. The `header` object always carries the block
  `number` — key fields are emitted regardless of the field projection — so the RP-9
  coverage carrier is recoverable under any selection; `hash`/`parentHash` appear when
  projected, and anchored continuation (RP-10) therefore requires projecting them.
- **IB-5** The success status is committed before streaming begins; admission-time errors
  therefore arrive as proper error statuses, while post-admission failures surface as
  RP-15 truncation (stream ends early; already-sent prefix remains valid JSONL). See
  GAP-10 for the observability obligation.
- **IB-6** Response watermark headers (success): finalized head number+hash when defined,
  and a head-number header (for the live stream: at least the snapshot head; may be
  fresher per CN-5). `NO_DATA` (204) SHOULD carry the same watermark headers — currently
  it does not (GAP-9). Coverage reporting per RP-9 is carried by emission itself: the
  coverage-end block is always present in the JSONL stream, header-only when it matches
  nothing (RP-9 carrier; boundary markers may also appear). Residual: effective ranges
  containing no stored block at all (GAP-8).

## 5. Status mapping

| Error class (04 §8) | HTTP |
|---|---|
| success | 200 (streamed) |
| `NO_DATA` | 204, empty body |
| `MALFORMED_REQUEST` | 400 for semantic validation; 422 for syntactically/schema-invalid JSON; free-text body today (GAP-36) |
| `RANGE_UNAVAILABLE` | 400; indistinguishable from other 400 classes except by free text (GAP-36) |
| `ITEM_UNAVAILABLE` | 400 (same note — GAP-36) |
| `KIND_MISMATCH` | 400 (same note — GAP-36) |
| `UNSUPPORTED_QUERY` | 400, free-text body |
| `FORBIDDEN` | 403 |
| `UNKNOWN_DATASET` | 404 |
| `NOT_FOUND` (hash lookup miss) | 404 — indistinguishable from `UNKNOWN_DATASET` except by free-text body (GAP-39) |
| `CONFLICT` | 409, body `{"previousBlocks":[{"number":…,"hash":"…"},…]}` = RP-11 hints (ascending; ≥ 1 entry; up to ~`P-CONFLICT-WINDOW`; entries are `⟨position, hash_at(position)⟩` pairs — DEF-16) |
| `OVERLOADED` | 503 |
| `INTERNAL` | 500 |

- Except for `NO_DATA` and class-specific payloads such as `CONFLICT`, query admission errors
  currently have a `text/plain` diagnostic body. Clients MUST NOT parse this text. A stable
  machine-readable discriminant for classes sharing one HTTP status remains GAP-36.
- **IB-7** Error bodies MUST NOT leak internals in a way clients must parse; conformance
  tests key on status + the structured fields named above only.

## 5a. Hash lookup binding

- **IB-10** `{hash}` is a path segment carrying the hash exactly as it appears in the
  dataset's own data — no normalization, no case folding, no `0x` handling: the service
  compares the string it is given against the string it stored. A client that hashes
  differently than its chain does simply misses. Length is bounded by `P-HASH-MAXLEN`;
  empty or longer is `MALFORMED_REQUEST` (400) decided before any store access (RP-20).
- **IB-11** A miss and a disabled index are the same response (404 `NOT_FOUND`). The
  binding deliberately exposes **no** way to ask "is this index enabled / complete?" —
  RP-19 forbids a client from acting on that difference anyway; operators read it from
  OB-12 instead.

## 6. Retention policy JSON

`{"FromBlock":{"number":N,"parent_hash":"…"?}}` | `{"Head":N}` | `"None"` — mapping to
DEF-9: `FromBlock` = Pinned / External instruction payload, `Head` = Window, `None` =
Unbounded. (Config-level `Api` marks a dataset as External-mode.) A runtime `"None"`
instruction today *parks* the dataset — ingestion stops — instead of behaving as
Unbounded (GAP-35).

## 7. Source-side binding (for the simulator)

The service consumes sources over the same protocol family it serves; the harness's
source simulator implements:

- `POST /stream` with `{"fromBlock":N, "parentBlockHash":"…"?}` → `200` JSONL block
  stream (with finalized-head headers), `204` no-data, or `409` + `previousBlocks` (the
  ForkSignal binding — DEF-12). `parentBlockHash` is the hash of `fromBlock`'s preceding
  block — the parent of the first block to be served (DEF-16); same semantics as the
  client-side query;
- `GET /head`, `GET /finalized-head` for probing;
- finality is conveyed via response headers (`finalized head number/hash`) alongside
  block streams.

Faults for CT-4/CT-9 are injected at this surface: malformed JSONL, broken linkage,
regressive watermarks, conflicting 409 hints, stalls, disconnects.

## 8. Conformance notes

- **IB-8** The binding is versioned by this document; any route/shape/status change MUST
  update this file and the CT-5 matrix in the same change.
- **IB-9** Anything observable at this surface and not specified here or in 04 is
  *unspecified behavior* — conformance tests MUST NOT pin it, and clients MUST NOT rely
  on it.
