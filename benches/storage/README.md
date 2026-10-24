# Public storage API benchmarks

This harness measures existing public abstractions. It does not import private
modules, expose implementation details, parse private disk formats, or add hooks
to production code. File sizes are observations of persisted artifacts. The
end-to-end suite uses `KV2` and `AsyncKVEngine`.

## Run

Use a release benchmark build and put the temporary datasets on the filesystem
you intend to measure:

```sh
cargo bench --bench storage -- --run --suite all \
  --records 1000 --value-bytes 256 --repetitions 3 --warmup-runs 1 \
  --data-dir target/benchmark-data --output target/benchmarks/storage.json
```

Use `--suite modules` or `--suite e2e` to select a layer. `--filter TEXT` selects
scenario names containing `TEXT`; required preparation still runs. A smoke run:

```sh
cargo bench --bench storage -- --run --suite all \
  --records 32 --value-bytes 64 --repetitions 1 --warmup-runs 0 \
  --flush-bytes 512 --output target/benchmarks/smoke.json
```

`--run` is required so `cargo test --all-targets` does not accidentally execute a
performance workload. Without it, the binary prints help and exits.
`--flush-bytes` controls the automatic-checkpoint workload and
`--wal-segment-bytes` controls configured segment rotation. `--help` lists all
parameters. Keys are fixed-width 20-byte strings; values follow a deterministic
pattern. The default data directory is under `target`, not a memory filesystem.

Every scenario repetition uses fresh temporary data. Only those generated
directories are removed afterward; the supplied parent directory is preserved.
JSON goes to stdout and optionally `--output`; progress goes to stderr. Existing
output files at that explicit path are replaced.

## Measurement boundaries

Fixtures, initial dataset loading, correctness verification, file-size inspection,
and temporary-directory cleanup are outside timed sections. Allocations performed
by an exposed API remain part of its cost. Some scenarios construct owned inputs
at the call boundary; their scenario definitions describe this explicitly.

Each sample reports the number of public operations, logical bytes processed,
and elapsed nanoseconds. Summaries show min/median/max run durations, aggregate
operations per second, logical MiB per second, and amortized microseconds per
operation. These are batch timings, not individual-request p95/p99 latency.
One reopen, flush, scan, or compaction counts as one operation when that is the
public operation being measured; its dataset size is reported separately.

Durable write scenarios use the normal sync behavior. CPU-only codec/filter
scenarios stay separate from disk I/O. Warmup runs are discarded. The OS page
cache remains enabled; reopening is not a cold-cache or process-crash experiment.
The standalone e2e checkpoint workload disables background compaction to isolate
flush cost; compaction is measured through its own public operation.

The report records the Git revision and tracked changes, Rust version, platform,
worker count, canonical data directory, configuration, raw samples, and summaries.
For comparisons, keep source state, workload, filesystem, build profile, cache
conditions, and machine load consistent. Record CPU/filesystem details alongside
a saved baseline. Read values are verified before reporting successful results.
These measurements establish a local baseline, not a performance guarantee.

## Current recovery scope

WAL segments remain retained after checkpointing. Recovery therefore replays
committed history, including flushed updates. Comparing before/after-checkpoint
reopen time measures current behavior; it does not assume checkpointing shortens
recovery. Raft consensus and multi-node networking are outside this standalone
storage harness.

## Scenario coverage

- `record/encode`, `record/decode`: one public record codec call per input.
- `block/encode`, `block/decode`: one public writer block codec call per
  single-record block. Public block fields construct fixtures outside timing.
- `checksum/calculate`, `checksum/verify`: one checksum call per value payload.
- `bloom/insert`, `bloom/contains`: one call per present key; this does not measure
  the false-positive rate or absent-key lookup performance.
- `index/encode`, `index/decode`, `footer/encode`, `footer/decode`: one public
  metadata codec call per iteration.
- `sstable/serialize`, `sstable/deserialize_metadata`: one complete table encoding
  or metadata-only decoding from an in-memory cursor. The fixture has one record
  per block and is not the writer's normal block-packing layout.
- `sstable/read_blocks`: one full block read from an already-open file with
  metadata loaded. The fixture has been verified beforehand and is cache-warm.
- `wal/durable_append`: one synced single-update transaction per input, through
  the existing WAL handle returned by `KV2::wal_manager`.
- `wal/scan`: one `recover_records` call over the complete committed dataset.
- `wal/rotate`: one synced segment rotation per iteration, retaining history.
- `manifest/register`: one durable metadata registration per iteration using
  synthetic public metadata fixtures; referenced SSTables are not read.
- `manifest/reopen`: one full manifest reopen/replay.
- `writer/put`: one `WriteComponent::put` per input, including its WAL sync.
  `writer/flush` measures one `flush_active_memtable` call, including snapshot
  creation, SSTable persistence, manifest publication, rotation, and cleanup.
- `compaction/merge_l0`: one public compaction call merging two overlapping L0
  SSTables containing two versions of each key. Preparation and verification of
  the surviving newer versions are outside timing. Its setup uses validated
  compaction settings: memtable threshold clamped to 1–64 MiB and WAL segment size
  clamped to 256 KiB–10 MiB.
- `e2e/open_empty`, `e2e/put_sequential_durable`,
  `e2e/get_present_single_pass`, `e2e/get_missing_single_pass`,
  `e2e/overwrite_durable`, `e2e/delete_durable`, `e2e/range_scan`, and
  `e2e/reopen_before_flush`: public database calls with automatic flushing
  suppressed. Deletes affect even key indexes; the range scan verifies live
  values after overwrites/deletes. Reopen, open, and scan each count as one call.
- `e2e/put_automatic_checkpoint`, `e2e/get_after_flush`,
  `e2e/range_scan_after_flush`, and `e2e/reopen_after_flush`: public calls with the
  configured flush threshold. At least one SSTable must be produced. Post-flush
  reads run before reopening and may span SSTables and a remaining memtable.
  This dataset contains puts only; it is distinct from the mutation workload.

Public methods allocate their own return values within timing. Preallocated
result vectors retain outputs where needed for post-timing verification; that
small collection cost is included. Read verification before timing warms caches.
Writer puts, database puts/overwrites/deletes, and manifest registration consume
owned inputs prepared before timing. WAL appends borrow prepared input vectors.
Individual private reader/recovery implementations have no additional benchmark
exports: they are exercised through existing public handles or the engine API.

A [recorded local baseline](BASELINE.md) shows the first validated release run.
Raw results are saved under `target/benchmarks/baseline.json` on the machine where
that run was made; run the command again to produce results on another machine.

## Saved results

The [2026-10-24T02-59-23Z baseline](results/2026-10-24T02-59-23Z/summary.md) is preserved with its
[raw JSON](results/2026-10-24T02-59-23Z/baseline.json) and
[readable HTML](results/2026-10-24T02-59-23Z/results.html). These files are outside
`target` and survive build cleanup and later benchmark runs.
