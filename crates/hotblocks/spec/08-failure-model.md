# 08 — Failure model

This document enumerates the faults the system MUST tolerate, and the required response to
each. "Required response" uses four verbs:

- **mask** — absorb; no externally visible effect beyond timing;
- **degrade** — keep serving with reduced capability, within spec'd bounds;
- **fail-safe** — refuse the affected operation/dataset with a defined error, preserving
  all invariants;
- **alarm** — enter/emit a distinct observable state (OB-9) within `P-ALARM`.

## 0. Global robustness requirements

- **FM-1 (No externally-triggered termination).** No input from any source or client —
  malformed, oversized, adversarial, or pathological-but-valid — may terminate, deadlock,
  or permanently wedge the process, nor corrupt any dataset. This includes payload
  contents (arbitrary bytes, invalid text encodings, extreme values, huge collections)
  flowing through the write path, and any query through the read path. *Tests:* CT-9 fuzz
  (GAP-11, GAP-12).
- **FM-2 (Fault classification).** Every failure is classified transient vs integrity
  (WP-17); transient → bounded-backoff retry forever; integrity → fail-safe + alarm. A
  fault MUST NOT oscillate between classifications for the same cause.
- **FM-3 (Containment).** Fault handling is per-dataset wherever the fault is
  per-dataset; per-source wherever per-source. A fault's blast radius (which operations
  degrade) MUST be documented by this table and no larger.

## 1. Source faults (FM-SRC)

| # | Fault | Required response |
|---|---|---|
| FM-SRC-1 | Source unreachable / times out / disconnects mid-stream | mask: per-source backoff `P-SOURCE-BACKOFF`, fail over to other sources; dataset degrades to "stale but serving" if all sources down; alarm on `P-SOURCE-DOWN-ALARM` of continuous unavailability |
| FM-SRC-2 | Stale data: blocks/watermarks at or below already-stored positions | mask: deduplicate/ignore (WP-16); MUST NOT crash-loop or regress state |
| FM-SRC-3 | Unlinked or out-of-order delivery within a stream (broken parent linkage, non-ascending numbers; number holes on slot-numbered chains are *not* a fault — DEF-1) | fail-safe per run: reject the run (WP-2), reset that source's session, re-request; no state change |
| FM-SRC-4 | Structurally invalid blocks: broken linkage, wrong numbering, schema-invalid payload | fail-safe: reject run; quarantine source after `P-SOURCE-STRIKES` consecutive rejections; alarm |
| FM-SRC-5 | Finality violation: finality hash contradicting stored finalized chain; fork hints entirely below `fin`; finality regression with conflicting hash | fail-safe + alarm (WP-6/WP-8, LIV-9b): do not apply; keep serving last good state; do NOT silently retry forever (GAP-5) |
| FM-SRC-6 | Equivocation between sources (different chains at same heights) | mask via arbitration (WP-4) while a quorum/timeout resolves; if unresolvable, prefer current stored chain, alarm on persistent split |
| FM-SRC-7 | Representation drift (hash casing/format differing across sources of one dataset) | fail-safe: treated as linkage mismatch (exact-match rule DEF-2); alarm on persistent mismatch pattern |
| FM-SRC-8 | Fork storm: rapid repeated reorgs, possibly deeper than the window | degrade: apply per WP-6 within floors (INV-14); deeper-than-window ⇒ RESET + alarm; throughput may drop, correctness never |

## 2. Storage faults (FM-STOR)

| # | Fault | Required response |
|---|---|---|
| FM-STOR-1 | Slow storage (I/O saturation, maintenance debt) | degrade: writes throttle; bounded by LIV-2 stall budget; reads keep serving; OB-3 signals pressure |
| FM-STOR-2 | Disk approaching full | alarm at `P-DISK-FLOOR`; degrade per documented policy: reads MUST keep working; writes MAY pause (counts toward stall accounting, exempt from LIV-2 only while below floor); the system MUST NOT corrupt state and MUST provide a documented, bounded recovery path (RS-8 boot maintenance) that does not require scratch space proportional to reclaimable data |
| FM-STOR-3 | Disk full (hard) | fail-safe: no committed-state corruption (INV-40 holds for the pre-full history); recovery path documented; startup after freeing space restores service |
| FM-STOR-4 | Detected corruption of stored state (checksum/decode failure) | fail-safe + alarm per dataset (CN-10): the damaged dataset stops, others serve; corruption MUST be detected (never returned as data), and MUST NOT silently disable global functions (e.g. reclamation for everyone) without alarm (GAP-6 adjacent) |
| FM-STOR-5 | Partial write persisted at crash (torn build) | mask: invisible by CN-2/INV-40; residue collected (RS-10) |

## 3. Process faults (FM-PROC)

| # | Fault | Required response |
|---|---|---|
| FM-PROC-1 | Crash / kill -9 at any instant | mask via recovery: INV-40 (exact last committed state), LIV-6 |
| FM-PROC-2 | Crash during recovery/boot maintenance | mask: idempotent recovery (INV-42) |
| FM-PROC-3 | Host/OS/power failure | degrade: bounded suffix loss `P-DUR-SYSTEM` (CN-6b); state still well-formed |
| FM-PROC-4 | Graceful shutdown signal | LIV-12: drain, no panic-class exits, crash-equivalent durability |
| FM-PROC-5 | Internal defect (panic-class error) in one subsystem task | contain: affected dataset/query fails-safe + alarm; process survives where containment is possible; repeated hits → FM-2 integrity classification, not infinite hot-loop |

## 4. Client faults (FM-CLI)

| # | Fault | Required response |
|---|---|---|
| FM-CLI-1 | Malformed / oversized / schema-invalid requests | fail-safe: taxonomy errors (RP-1), FM-1 holds |
| FM-CLI-2 | Pathological valid queries (huge ranges, dense filters, giant blocks) | degrade: budgets bound cost (RP-17); progress guaranteed (INV-25); other clients unaffected beyond fair-share slowdown |
| FM-CLI-3 | Slow reader / stalled connection | mask: bounded buffers, cancel on disconnect (RP-18); never pins snapshots/slots beyond `P-QUERY-TIME` |
| FM-CLI-4 | Thundering herd (mass reconnect/poll) | degrade: admission control `OVERLOADED` (RP-3/LIV-10); waiter caps; no collapse; recovery when herd subsides |
| FM-CLI-5 | Disconnect mid-stream | mask: RP-15 truncation semantics; server resources released promptly |

## 5. Operator faults (FM-OP)

| # | Fault | Required response |
|---|---|---|
| FM-OP-1 | Config/state contradictions: kind change, pinned-anchor mismatch, format incompatibility | fail-safe at boot: INV-43 refusal with distinct diagnostics; unaffected datasets serve (CN-10) |
| FM-OP-2 | Dataset removed from config | defined destructive path: DROP at boot (INV-44); MUST be logged/observable; (hardening SHOULD: a grace/confirmation mechanism, since this is the highest-blast-radius single config edit) |
| FM-OP-3 | Two service instances over one store | fail-safe: detect divergence, stop the losing writer per dataset, alarm (WP-15); MUST NOT interleave-corrupt |
| FM-OP-4 | Retention mistakes (raise far above head, contradictory instructions) | defined semantics (WP-9/RETAIN cases): destructive outcomes are the documented ones only; observable; idempotent |
| FM-OP-5 | Restart with changed parameters (window size, budgets) | mask: state re-converges to policy (trim or backfill-forward per WP-10/WP-5); no invariant violations during convergence |

## 6. Fault → property cross-reference

- Crash matrix: FM-PROC-* ⇢ INV-40/42, LIV-5/6, CT-2.
- Source misbehavior corpus: FM-SRC-* ⇢ INV-7/12/13/14/23, LIV-1/9, CT-4.
- Overload & client abuse: FM-CLI-* ⇢ RP-3/17/18, LIV-3/4/10, CT-6/CT-9.
- Storage pressure: FM-STOR-* ⇢ INV-41, LIV-2/7, RS-6/8, CT-7.
- Operator: FM-OP-* ⇢ INV-43/44, CT-5 boot matrix.
