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
scenario names containing `TEXT`; a full scenario name selects only that result,
while its required fixture setup still runs. A smoke run:

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
For batch workloads, `operations` means mutations (individual puts/deletes),
not calls to `write_batch`; batch call counts are encoded by the scenario's
configured size and the final partial group. The `app/batch_*` scenarios include
CLI tokenization, command parsing, dispatch, and the async engine call;
`app/sequential_set_checkpoint` uses the same parser and executor for individual
SET commands. These app scenarios use identical textual keys and `value-{index}`
values, and include parsing plus execution in timing. Fixture construction,
setup, verification, and cleanup remain outside timing. App payload size is the
actual UTF-8 textual key/value bytes, distinct from `--value-bytes` (the
configured binary engine payload).

FTS documents are deterministic UTF-8 text fixtures with their own byte lengths;
they are not the configured binary value payloads. `fts/index_put_documents`
measures public `FtsIndex::put_document` calls, and the `fts/search_*` cases
measure common, rare, missing, AND, and OR query behavior. Query fixture
creation and correctness checks are outside timed sections. `e2e/fts_*` covers
indexing through `KV2`, reopen/recovery before automatic flush, then a configured
checkpoint and reopen with searches revalidated. It exercises the storage API
and is supplemented by app command workloads below. There is no public
tokenizer API, so no tokenizer-only benchmark is added. `app/doc_put_documents` and
`app/fts_{and,or}_search` include command parsing plus async dispatch in their
timed regions. DOC document text is quoted UTF-8 fixture content. Setup and
response validation remain outside timing.

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
  `writer/batch_{1,16,64,256}` measures the public writer batch API in groups of
  the named maximum mutation size; each sample's operation count is mutations,
  while the number of batch calls is determined by the group size.
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
- `e2e/range_scan_after_flush_delete` and
  `e2e/range_scan_after_flush_reinsert`: load and automatically flush data, delete
  even key indexes, then optionally reinsert them with new values. Only the final
  public range scan is timed. Exact live values, tombstone exclusion, point reads,
  and reinsert/reopen consistency are verified outside timing. These cases were
  added after the archived 35-scenario baseline to cover mixed disk/memory
  mutation behavior.
- `e2e/batch_sequential_checkpoint` is the matched sequential-put baseline for
  the batch workloads, with the same configured automatic flush threshold and
  reopen validation. `e2e/batch_put_{1,16,64,256}` and
  `e2e/batch_mixed_{1,16,64,256}` measure durable KV2 batches of the named
  maximum mutation count. Mixed batches put, delete, and reinsert selected
  keys; resulting values are validated before and after reopen, outside timing.
- `app/sequential_set_checkpoint` and `app/batch_{1,16,64,256}` compare real
  command-parser and async-executor SETs versus atomic BATCH commands using the
  same fresh dataset, textual keys and `value-{index}` payloads, and automatic
  flush threshold. The batch suffix is the maximum mutations per command; the
  final group may be partial. App timings include tokenization, parsing,
  dispatch, and async execution. Reported payload bytes are textual UTF-8 bytes,
  not the configured binary `--value-bytes` engine fixture. Checkpoint production
  and exact values after reopen are verified outside timing. Compare these app
  scenarios with each other; `e2e/batch_sequential_checkpoint` and
  `e2e/batch_put_*` remain a matched binary-payload engine-only comparison.
- `fts/index_put_documents`, `fts/search_common`, `fts/search_rare`,
  `fts/search_missing`, `fts/search_and`, and `fts/search_or` measure the public
  FTS index API over UTF-8 documents. The put case times one insertion per
  document plus a replacement and a delete when the fixture size permits; its
  operation count is the actual number of those document mutations. Search
  fixture setup, document round-trips, stale-posting checks, and exact result
  checks occur outside timing. Search result caps are at most 1000. These are
  public API measurements because no tokenizer abstraction is independently
  exposed.
- `e2e/fts_index_put`, `e2e/fts_reopen_before_flush`,
  `e2e/fts_search_{common,rare,missing,and,or}`, and
  `e2e/fts_reopen_after_flush` exercise FTS records persisted through `KV2`.
  The reopen-after-flush case writes padding records through the raw KV API to
  force the configured checkpoint, then checks documents and search results
  after reopen. Reopen timings include engine open/recovery and FTS metadata
  validation; fixture work and correctness checks remain outside timing.
- `e2e/fts_search_after_flush` measures an OR query on the reopened index after
  the configured checkpoint; the expected hit count and ascending ID order are
  validated immediately after the query timing ends.
- `app/doc_put_documents`, `app/fts_and_search`, and `app/fts_or_search` use
  the public command parser and async command executor with an `FtsIndex`.
  Timings include parsing, dispatch, and document/query execution. Document
  fixture setup for searches and response verification are outside timing;
  operation counts mean document mutations for DOC PUT and one query for FTS.

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

After implementation, a focused batch smoke check can use:

```sh
cargo bench --bench storage -- --run --suite e2e --filter batch \
  --records 32 --value-bytes 64 --repetitions 1 --warmup-runs 0 \
  --flush-bytes 512 --output target/benchmarks/batch-smoke.json
```

The recommended full run is the `cargo bench --bench storage -- --run --suite
all --records 1000 --value-bytes 256 --repetitions 3 --warmup-runs 1` command
above, with data and output paths set for the target machine. Existing archived
scenario results remain historical; these scenarios should be run and archived
as a separate new result set.

## Saved results

The [2026-10-24T02-59-23Z baseline](results/2026-10-24T02-59-23Z/summary.md) is preserved with its
[raw JSON](results/2026-10-24T02-59-23Z/baseline.json) and
[readable HTML](results/2026-10-24T02-59-23Z/results.html). These files are outside
`target` and survive build cleanup and later benchmark runs.
