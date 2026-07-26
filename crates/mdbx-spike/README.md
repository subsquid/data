# mdbx-spike

Churn bench behind the [ADR 0002](../../docs/adr/0002-storage-engine-libmdbx.md)
decision: drives the hotblocks write shape — chunk commits, chunk merges,
retention trims — against libmdbx and a production-tuned RocksDB reference.

The `sqd_storage::kv` traits are implemented over libmdbx (`src/kv_mdbx.rs`)
and exercised for real: commits write through `KvWrite`, and a post-run scan
walks every live entry through `KvReadCursor`, decompressing pages. That is the
seam the ADR claims ports verbatim.

## Decision runs (Linux only)

Write amplification comes from cgroup `io.stat` (authoritative; `/proc/self/io`
write_bytes counts page dirtying, not proven device traffic). macOS gives
latency and footprint only, and its fsync (`F_FULLFSYNC`) makes sync-cadence
tails unrepresentative.

### Getting it onto prod-class hardware

```bash
git push -u origin spike/libmdbx
gh workflow run docker.yaml --ref spike/libmdbx -f target=mdbx-spike -f tag=<tag>
# image: subsquid/data-mdbx-spike:<tag>
```

Run as a Job pinned to an idle prod-class NVMe node (fill the hostname at run
time; keep node names out of this public repo). `emptyDir` lands on the node's
NVMe. Set real resource requests — a zero-request pod is the first eviction
candidate on a full node.

```yaml
apiVersion: batch/v1
kind: Job
metadata: { name: mdbx-spike }
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      nodeSelector: { kubernetes.io/hostname: <idle-prod-node> }
      containers:
        - name: spike
          image: subsquid/data-mdbx-spike:<tag>
          args: ["--engine", "mdbx", "--dir", "/data/spike",
                 "--datasets", "45", "--chunks", "2000", "--compress"]
          resources:
            requests: { cpu: "8", memory: 8Gi }
            limits: { memory: 16Gi }
          volumeMounts: [{ name: data, mountPath: /data }]
      volumes:
        - name: data
          emptyDir: {}
```

Collect with `kubectl logs job/mdbx-spike`; record results in
`docs/measurements/` with node/namespace/pod names scrubbed.

### Run matrix

```bash
# free-run churn, engine amplification at matched stored volume
mdbx-spike --engine mdbx  --dir /nvme/spike --datasets 45 --chunks 2000 --compress
mdbx-spike --engine rocks --dir /nvme/spike --datasets 45 --chunks 2000

# production pace (~1.3 commits/s/dataset), latency under merge contention
mdbx-spike --engine mdbx --dir /nvme/spike --datasets 45 --chunks 600 --compress --paced-ms 750
mdbx-spike --engine mdbx --dir /nvme/spike --datasets 45 --chunks 600 --compress --paced-ms 750 --per-dataset-env

# stale-reader file growth (GAP-29 shape)
mdbx-spike --engine mdbx --dir /nvme/spike --datasets 8 --chunks 2000 --compress --reader-hold-secs 60

# retention wave (downstream-stall shape): file high-water + post-wave reuse;
# single env only — per-dataset envs hide the signal under the 256 MiB
# growth-step floor unless --growth-step-mb is lowered
mdbx-spike --engine mdbx --dir /nvme/spike --datasets 45 --chunks 2000 --compress --mdbx-page 4096 --window-wave 640
mdbx-spike --engine mdbx --dir /nvme/spike --datasets 45 --chunks 1200 --compress --mdbx-page 4096 --window-wave 300 --paced-ms 250
mdbx-spike --engine rocks --dir /nvme/spike --datasets 45 --chunks 2000 --window-wave 640
```

Gate per ADR 0002: proceed only if device writes drop ≥3× vs the rocks
reference without head-freshness (commit tail) regressions.

## Findings

Linux decision runs: [2026-07-26 measurements](../../docs/measurements/2026-07-26-mdbx-churn-bench.md).
Headline: the gate outcome is a page-size decision — 64 KiB pages write 1.4×
*more* to the device than the rocks reference, 4 KiB pages write 3.7× less
and pass. Two hard-won constraints:

- `mdbx_env_sync` from a cadence thread racing `SafeNoSync` commits aborts
  the process on the always-on `ENSURE(legal4overwrite)` (libmdbx 0.13.11,
  Linux; never reproduced on macOS). Durable sync is now serialized with
  write txns per env, so commit tails include the fsync wait — the cost a
  port would carry.
- cgroup `io.stat` re-counts bytes on stacked block devices (md/dm); the
  bench skips major 9/253 lines and cross-checks against `/proc/self/io`.
- Wave runs (15–17): the mdbx file is a permanent high-water mark — ~1.35×
  peak stored live plus growth-step rounding at paced rate (free-run bursts
  park up to a sync-cadence window of churn on top: 2.0× on the free-run
  wave); tail truncation never fires even with an armed `shrink_threshold`
  (80–93% of pages free at end). Reuse
  holds: zero post-wave growth at paced rate, exactly one growth step
  free-running. The rocks reference reclaims back to ~live for 3.7× the
  device writes.

Earlier local findings (macOS):

- Generated pages hit 4.00× LZ4 at `--dup 6` vs the measured 4.03× on prod
  `CF_TABLES`.
- `SafeNoSync` parks freed pages until the next durable point: free-running
  without a sync cadence ballooned the file to 110× the live data. With the
  default 1 s cadence it stayed at one growth step. Cadence and growth-step
  tuning is a real porting decision, not a knob to default.

## Model simplifications

- Merges are single-level (N fresh chunks → one table, never re-merged);
  production `compaction_loop` re-merges up to 200k rows. Understates app-level
  churn equally for both engines.
- `--readers N` adds query-shaped reads (random live table: snapshot, prefix
  seek, cursor scan, decompress), but the bench's live set fits page cache,
  so read numbers compare engine CPU paths, not disk-bound reads.
  `--reader-hold-secs` covers only the freelist-pinning effect of long
  readers.
