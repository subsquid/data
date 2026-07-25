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
```

Gate per ADR 0002: proceed only if device writes drop ≥3× vs the rocks
reference without head-freshness (commit tail) regressions.

## Findings so far (local, macOS)

- Generated pages hit 4.00× LZ4 at `--dup 6` vs the measured 4.03× on prod
  `CF_TABLES`.
- `SafeNoSync` parks freed pages until the next durable point: free-running
  without a sync cadence ballooned the file to 110× the live data. With the
  default 1 s cadence it stayed at one growth step. Cadence and growth-step
  tuning is a real porting decision, not a knob to default.
- `mdbx_env_sync` contends with writers; at 18× prod pace on macOS that put
  fsync into commit tails. Needs re-measuring on Linux where fdatasync is
  ~1 ms, and staggering across envs in per-dataset mode.

## Model simplifications

- Merges are single-level (N fresh chunks → one table, never re-merged);
  production `compaction_loop` re-merges up to 200k rows. Understates app-level
  churn equally for both engines.
- The read side of queries is absent; `--reader-hold-secs` covers only the
  freelist-pinning effect of long readers.
