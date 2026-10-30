# Experimental WAL value storage

New standalone CLI databases use `ExperimentalEngine` by default. It keeps committed
values in the retained write-ahead log and keeps only a fixed-size value reference in the
LSM index. The mode is experimental, single-node, and format-incompatible with `KV2`. It is
not a production storage format and it does not use or extend Raft.

The matched comparison harness lives in `examples/experimental_compare.rs`. It measures
the current build's `KV2` against the current build's `ExperimentalEngine` on the same
machine, configuration, and deterministic workload. It does not include RocksDB or any
other external engine.

## CLI selection and compatibility

- An absent or empty standalone CLI data directory opens as `ExperimentalEngine`.
- A recognized KV2 root with top-level `MANIFEST`, `wal`, and `sstable` directories opens
  as KV2, preserving its format and records. The CLI does not migrate existing data, and
  the public `KV2::open` API remains unchanged.
- An existing experimental root, or a `CURRENT`-before-root-marker bootstrap with its
  selected generation layout present, is routed to `ExperimentalEngine`. The CLI never
  falls back to KV2 after an experimental open or FTS-index error.
- Mixed KV2 and experimental artifacts, unknown non-empty roots, and incomplete bootstrap
  residue that cannot be structurally recovered are rejected before filesystem mutation. The
  selector checks the root marker, `CURRENT`, their temporary files, `NEXT-GENERATION`,
  `SEAL`, `LOCK`, and `generations`; a lone `LOCK` is not enough to initialize a store.
- `ExperimentalEngine` and KV2 refuse each other's roots. There is no migration,
  down-conversion, or compatibility layer.
- Experimental roots hold a `LOCK` file. Two engines cannot open the same root at the same
  time, and reopen requires the previous engine to be dropped.
- The standalone REPL uses the generic `FtsIndex` for both layouts. Raft mode continues to
  use `RaftKV2` and does not support the experimental format or FTS commands.
- Opening an existing experimental root validates `CURRENT`, the generation directory,
  the index marker and manifest, and the generation seal before serving reads. A failed
  validation fails closed.
- The CLI does not run the cleaner automatically and exposes no clean command. Retained
  WAL space is reclaimed only through the explicit library `ExperimentalEngine::clean()`
  call, which performs a stop-the-world full rewrite. The 107-byte reference overhead and
  transaction-sized point reads remain, including for small values.
- The harness reads level metadata through the public
  `ManifestManager::load_or_create` only after the measured engine has shut down; it never
  opens a second live engine on the same root.

## What differs from KV2

`KV2` stores key and value together in the memtable and compacted SSTables, and reclaims
covered WAL segments after a checkpointed flush. `ExperimentalEngine` instead:

- writes each committed operation's user payload to the retained WAL once, in the same
  transaction that commits the operation. The fixed-size index update is derived from the
  WAL write receipts for that transaction; the engine does not append a second
  index-mutating record to the WAL;
- stores `key -> reference` in the LSM index, where a reference is 107 encoded bytes
  (1 version byte, 8 generation bytes, 94 WAL value-reference bytes, 4 checksum bytes);
- resolves a point read by finding the newest index record, decoding its reference, then
  opening the referenced WAL segment and re-reading and CRC-validating the complete
  referenced transaction span, from its start offset through its commit frame, capped at
  256 MiB. The read accepts the referenced frame only after the full frame chain, the
  transaction identity, and the referenced key/value checksums validate. It does not scan
  retained history, but it is O(transaction bytes), not O(value bytes), and there is no
  transaction cache: each read pays for the whole referenced transaction;
- retains the full committed WAL for the current generation rather than reclaiming
  history on flush;
- reclaims space only through an explicit `clean()` generation rewrite.

Consequences that drive the measurements:

- The index is much smaller than the live value set, so LSM flush and compaction payload
  can be much smaller. Whether this helps total space or ingest cost depends on values,
  batching, and how much retained WAL history the workload creates.
- Recovery replays the retained generation log, so reopen cost scales with retained WAL
  size rather than with the size of the live set.
- Read cost is dominated by transaction shape, not only value size. `clean()` copies live
  records in bounded transactions of at most 256 records or 8 MiB of frame bytes, so a
  64 KiB value can belong to a transaction near that bound and reading one such value
  re-validates the whole transaction. This is the deliberate batching trade-off: larger
  copy transactions reduce per-frame overhead in the new log but make every point read
  and every cleaner validation more expensive. The 107-byte reference is small next to
  that re-read cost for large values, and larger than a 64-byte value itself (more than
  ten times an 8-byte value), so it worsens logical space amplification for small values.
- `clean()` validates every live reference it copies, which re-reads each old transaction.
  That validation is legitimate clean work and is included in `clean_pause` and
  `clean_elapsed`; the harness does not subtract it.
- `clean()` is a full rewrite and a stop-the-world pause, not incremental garbage
  collection. Its peak space use and elapsed time are part of the cost, not hidden work.

## On-disk layout

```text
root/
  EXPERIMENTAL-WAL-VALUES        root format marker
  CURRENT                        current generation and sequence floor
  NEXT-GENERATION                reserved generation id
  LOCK                           single-writer lock
  generations/<20 digits>/
    SEAL                         generation seal (segment census and log end)
    wal/<segment>.log            retained value log
    index/
      EXPERIMENTAL-WAL-VALUES
      MANIFEST                   LSM index manifest
      sstable/level-<n>/...      index SSTables
```

`clean()` reserves a new generation, writes live records into the new WAL, builds the new
index, validates that index by resolving every live reference against the new WAL,
flushes at most one index SSTable when there is live data, writes a generation seal, and
publishes `CURRENT` with an atomic rename. It then reclaims the old generation.
Abandoned generations found on open are removed before serving. If cleanup cannot
complete, `CleanStats::deferred_cleanup_bytes` reports the space that remains counted but
unreclaimed; the new generation is still the published one.

`CleanStats` reports both the pause and the full operation:

- `write_pause`: stop-the-world interval through adoption of the new generation;
- `elapsed`: the whole clean operation, including cleanup attempted after adoption;
- `observed_peak_bytes`: one filesystem sample taken after the new generation is durable
  and before `CURRENT` switches, when old and new generations coexist. This is not a
  continuous maximum;
- `total_root_bytes_before` / `total_root_bytes_after`: deterministic recursive file-size
  samples taken by `clean()` itself;
- `copied_payload_bytes`, `copied_transactions`, `live_records`, `live_logical_bytes`,
  `index_sstables_written`, `deferred_cleanup_bytes`.

The harness additionally walks the whole root before clean, after shutdown, and after
reopen, so old generations, orphaned generations, retained WAL, index SSTables, and
metadata are all counted.

## Matched comparison harness

The harness requires a fresh run directory. `--data-dir` must be absent or an existing
empty directory; the harness claims it with an exclusive `.harness-run-owner` file and
never deletes or reuses pre-existing paths. It rejects a path with any symbolic-link
component before creating anything. Each sample directory is created exclusively and the
run fails if that path already exists. The default `target/bench-data/experimental-wal-<timestamp>-<pid>`
is unique per run.

Build and run the functional pilot:

```bash
cargo build --example experimental_compare
./target/debug/examples/experimental_compare --pilot \
  --data-dir target/bench-data/pilot-$(date +%s)
```

No three-repetition performance result is recorded in this document.

The pilot uses 32 records, 4 KiB values, one batch size, one sample, and no warmup. It
checks writes, overwrites, deletes, reinserts, a full exact scan with bounded point
probes, closed preclean recovery, clean statistics, closed postclean recovery, the
closed-engine mode guards, a repeated clean, and FTS Boolean/ranked equivalence both
before and after clean. It writes `<data-dir>/pilot/results.json` and is not a
performance result. A passing pilot validates the harness on one machine; it does not
prove that the default cases are feasible or representative.

Recommended benchmark cases:

```bash
cargo run --release --example experimental_compare -- \
  --profile small-tables --repetitions 3 --warmups 1
```

```bash
cargo run --release --example experimental_compare -- \
  --profile small-tables --value-bytes 4096 --records 512
```

```bash
cargo run --release --example experimental_compare -- \
  --profile default-buffer --value-bytes 64 --records 512 --batch-sizes 1,64
```

The last command is the clearly labelled WAL-only small case: both engines keep the whole
data set in the memtable and WAL, no SSTable is registered during ingest, and the report
says so. It isolates retained-log and reference-overhead effects from SSTable work.

### CLI defaults and limits

- `--profile small-tables`: memtable 1 MiB, 4 KiB blocks, `max_level0_files = 2`,
  target SSTable 16 KiB, L1 base 64 KiB, level multiplier 4, WAL segment 256 KiB.
- `--profile default-buffer`: `StorageConfig::default()` (4 MiB memtable, 2 MiB target
  SSTable, 16 MiB L1 base, multiplier 10, 1 MiB WAL segment).
- `--repetitions` default 3, minimum 3 in benchmark mode; `--warmups` default 1, minimum 1
  in benchmark mode. Each warmup and sample uses a fresh database.
- `--seed` default `0x6A09E667F3BCC909`.
- `--value-bytes` default `64,4096,65536`; comma-separated, bounded to 8 B..1 MiB.
- `--records` overrides the per-value default (64 B: 16384, 4 KiB: 512, 64 KiB: 64).
  The hard cap is 32768 records and 32 MiB of logical input per sample; the harness
  refuses configurations that exceed it. The 64 KiB default is deliberately small because
  exact verification re-reads each referenced transaction; see the caveat below. Default
  bounds are harness guards, not evidence that a default case is feasible anywhere.
- `--batch-sizes` default `1,64`, or `1,16` for 64 KiB values; comma-separated, 1..1024.
  A batch count is not a mutation count: each sample reports both `mutations` and
  `batches`.
- `--overwrite-percent` default 25, `--delete-percent` default 10,
  `--reinsert-percent` default 50 (percentage of deleted keys that are reinserted).
- `--skip-reads` omits only the bounded warm-read timing; verification always runs.
  `--skip-fts` omits the functional FTS case.
- `--data-dir` defaults to a unique directory under `target/bench-data/`. It must be
  absent or an existing empty directory; the harness claims it with an exclusive
  `.harness-run-owner` file and never deletes or reuses existing paths. Any symbolic-link
  component in the path is rejected before anything is created. The directory must be on
  a persistent filesystem: the harness canonicalizes it, resolves the longest matching
  `/proc/self/mountinfo` mount for that canonical path, and refuses `tmpfs`, `ramfs`, and
  `devtmpfs`. Every sample, guard, and FTS database directory is created exclusively and
  the run fails if it already exists. The run root is not a database; censuses cover only
  each sample's database root, so the owner file is not counted in space totals.
- `--output` defaults to `<data-dir>/results.json`.

### Matched conditions

- Both engines receive the same `StorageConfig` value and the same deterministic input:
  a SplitMix64-seeded generator produces value bytes and key suffixes; keys are 24 bytes
  (an ordered 8-byte index followed by 16 generator bytes). Nothing uses a small
  repetitive pattern.
- The same append, overwrite, delete, and reinsert operation order, batch boundaries,
  mutation counts, and seed are used for both engines. Batch sizes count operations, not
  calls; the harness never inserts extra key writes inside a timed region.
- Every sample creates a new database directory exclusively. Open happens before any
  write timer starts, and no pre-existing sample path is deleted or reused.
- Engine order alternates between samples (reference first on even samples, experimental
  first on odd samples) to spread order effects.
- After the write phases and the pending-write barrier, the harness shuts the engine down
  immediately inside the clock, then inspects space, levels, and the manifest only while
  the engine is closed. No verification or census is allowed between the barrier and the
  timed shutdown. No second engine is ever active on a root, and no census runs while an
  engine holds the root.
- Both engines are measured at closed preclean boundaries. The experimental engine then
  gets its own closed postclean boundary and a second recovery, and those extra shutdown
  and reopen phases are reported explicitly.
- Timings come from batched engine calls. The harness reports phase totals only. It does
  not derive per-request p95 or percentile claims from batch times.
- The harness reports `git rev-parse HEAD` for provenance. The comparison is only valid
  for the binary built from the checkout that produced it.

### Timing contract

| Field | Clocked region |
| --- | --- |
| `ingest` | The loop of append `write_batch` calls only |
| `mutate` | The loop of overwrite, delete, and reinsert `write_batch` calls only |
| `settle` | `await_pending_writes` only |
| `clean_pause` | `CleanStats::write_pause` for the experimental engine |
| `clean_elapsed` | `CleanStats::elapsed`, including post-adoption cleanup |
| `shutdown_preclean` | The first `shutdown`, started immediately after `settle` for both engines |
| `reopen_preclean` | The timed open of the closed, uncleaned root; experimental WAL replay is included |
| `shutdown_postclean` | The experimental second `shutdown`, after `clean` |
| `reopen_postclean` | The experimental timed open of the cleaned root |
| `point_get` | Up to 64 deterministic warm point reads after reopened verification |
| `scan` | One bounded scan of up to 128 entries after reopened verification |
| `shutdown_final` | The last `shutdown` of each sample |
| `lifecycle_phase_sum` | Explicit sum of ingest, mutate, settle, clean elapsed (experimental only), all three or five recovery shutdown/reopen phases, and `shutdown_final` |

The following happen outside every phase timer: input construction, expected-map
construction, verification scans and point probes, filesystem censuses, manifest and level
inspection, JSON serialization, and FTS comparisons. `lifecycle_phase_sum` is an explicit
phase sum, never maintenance-inclusive continuous wall time: verification and census gaps
are excluded, and the experimental engine's extra shutdown and reopen costs appear as
separate fields and are included in the sum rather than hidden. No harness census runs
while an engine is open, so no census can race unlink or cleanup.

The core `CleanStats` fields `total_root_bytes_before`, `observed_peak_bytes`, and
`total_root_bytes_after` are sampled by `clean()` while the engine is live. The harness
reports them with `root_bytes_sampling = core_clean_snapshot_while_engine_live` and treats
`observed_peak_bytes` as one observed coexistence snapshot, not a continuous maximum and
not one of the harness closed-root censuses.

The experimental cleaner performs its full live-record validation inside its clean pause.
That validation is legitimate clean work and is included in `clean_pause`/`clean_elapsed`;
the harness does not subtract it or move it outside the clock.

### Configuration caveat: the 1 MiB memtable floor

`StorageConfig::validate` rejects a memtable threshold below 1 MiB. A 16 KiB memtable
therefore cannot be passed to the current public API without changing core validation,
which this comparison deliberately does not do. `small-tables` uses the smallest legal
memtable (1 MiB) together with 16 KiB target SSTables, a 64 KiB L1 budget, and
`max_level0_files = 2` so that flushes and level compactions happen within the bounded
data set.

The reference index and the default LSM count different things. `KV2` charges key plus
value against the memtable threshold. The experimental index charges key plus the 107-byte
reference. At the default record counts, the experimental index for 4 KiB and 64 KiB values
stays below 1 MiB and does not flush during ingest, while `KV2` does flush and compact. The
64 B case uses 16384 records, where 131 bytes per index record crosses the floor for both
engines.

The 64 KiB case defaults to 64 records (4 MiB payload), not 256. Verification is exact but
not free: a full scan resolves every live reference, and each resolution re-reads and
validates its whole referenced transaction; clean copies live records in transactions up
to 8 MiB. At 256 records and 16 MiB payload, roughly two 8 MiB copy transactions would
make one full verification pass decode on the order of 2 GiB, and the harness runs a full
scan at every closed boundary (preclean and cleaned for the experimental engine, reopen
for the reference). The lower default keeps the harness bounded. `--records` can still
raise the case within the 32768-record and 32 MiB-input caps; that is an explicit operator
choice, not a claim that the larger case is cheap. A passing 32-record pilot on one
machine does not establish that any default case is feasible or representative elsewhere.

The harness reads the registered SSTables and levels from the manifest after shutdown and
reports preclean and postclean `compaction_observed` values and per-level table counts for
both engines. It does not assume that compaction happened. Conclusions must be drawn from
the reported levels, and the harness does not require equal flush or compaction counts:
different payload sizes make equal counts neither expected nor meaningful. Reporting the
preclean levels separately preserves the ingest compaction evidence that a later clean
would erase.

### Space accounting

- `total_bytes` is the logical size of every regular file under the root, including all
  generations, inactive or orphaned generations, retained WAL, index SSTables, and
  metadata. This is the space denominator, not an SSTable-only number.
- Breakdown fields are `wal_bytes`, `sstable_bytes`, `metadata_bytes` (manifest, CURRENT,
  locks, seals, markers), and `other_bytes`. `file_count` is included.
- `os_allocated_bytes` is the sum of `st_blocks * 512`. It is informational only. It is
  not unique physical usage on Btrfs: CoW, compression, and reflinks are not resolved by
  this harness.
- `space_amplification` is `total_bytes / live key+value bytes`, where live bytes come
  from the verified expected map after all mutations. When the live set is empty, the
  ratio is JSON `null`; absolute bytes are still reported.
- `preclean` is measured after the first timed shutdown, while the engine is closed and
  the root is uncleaned. It covers the ingest and mutation history and any natural
  checkpoint reclamation. `postclean` (experimental only) is measured after clean and the
  second shutdown, before the cleaned reopen. `final` is measured after the last shutdown.
  Each is the closed database root for one sample, not the run root.
- The reference engine is never force-flushed or force-compacted for the measurement.
  WAL that remains unflushed at shutdown stays in the root and is counted. Natural
  checkpoint reclamation during the write path is allowed to finish normally.
- `copied_payload_bytes` is the clean rewrite payload. It is reported separately from
  space amplification. The harness does not instrument every written byte, so it makes no
  lifetime write-amplification claim.
- The cleaner's own root-byte snapshots are not continuous measurements: the harness
  reports them as core live snapshots and keeps its closed-root censuses separate.

### Verification and FTS case

Every sample verifies, outside the write timers, the full expected map: a full-range scan
must equal the expected ordered key/value set exactly, including every value. That exact
scan is kept at each closed boundary. Point reads are then capped at 16 deterministic
positions covering the first and last live key plus evenly spaced interior keys, together
with one deleted probe and one absent probe; the harness does not issue one get per record
on top of the exact scan. The same verification runs after the preclean reopen and again
after the cleaned reopen. `--skip-reads` removes only the bounded warm-read timing, never
verification. A mismatch aborts the run instead of reporting numbers.

The full scan is not cheap for the experimental engine: each returned record resolves its
reference and re-validates the whole referenced transaction, so the scan still costs about
one transaction read per live record. The internal cleaner performs a comparable full
validation inside `clean()`. Both costs are reported in the phase they belong to and are
not treated as free.

The FTS case is functional equivalence only, one run, and is separate from the sampled
performance cases. It indexes 128 deterministic UTF-8 documents in two `put_documents`
calls of 64. Each document contains a common term, deterministic vocabulary terms, and
rare terms on fixed ID cadences. The reference engine is reopened and observed. The
experimental engine is observed twice: after an uncleaned reopen before clean, and after
the cleaned reopen. Both indexes are opened through the public `FtsIndex` wrapper, and the
harness compares each experimental observation with the reference:

- every document by `get_document`;
- exact Boolean AND and OR result ID sequences;
- exact ranked AND and OR result ID sequences and `f64::to_bits` scores.

Any difference fails the run. The very small pilot variation uses 16 documents in chunks
of 8 to prove the same counters and equivalence path before and after clean. FTS results
are correctness evidence only; they are not a performance sample of the three-case set.

### What this harness does not claim

- No speedup is promised. The hypothesis is that keeping values out of the LSM index can
  reduce indexed payload and post-clean total space because clean rewrites only live
  values; the costs are the retained log, the WAL-backed read path, the full-rewrite clean
  pause, and the 107-byte reference overhead. Measurements decide per workload.
- It does not compare against RocksDB or any other engine, and it does not reuse the
  earlier RocksDB comparison numbers. Both measured engines come from this checkout.
- It does not claim equal flush counts, equal WAL behavior, or that space amplification
  from SSTable bytes alone is a valid comparison.
- It does not measure concurrent writers, group commit, multiple threads, or read
  concurrency. Each engine call is serialized by the harness.
- It does not inject crashes or filesystem failures. Clean fault points exist only in
  `cfg(test)` core tests; deferred-cleanup behavior is reported by core tests, not induced
  here. `observed_peak_bytes` is one sample, not a continuous maximum.
- It does not resolve Btrfs physical allocation, compression, or reflink sharing, and it
  does not claim that allocated blocks are unique physical blocks.
- It does not produce per-request latency percentiles. Phase totals over batched calls
  cannot yield per-request p95.
- It does not claim lifetime write amplification, because not every written byte is
  instrumented.
- It does not claim that `lifecycle_phase_sum` is wall-clock time, or that verification is
  free. The phase sum excludes verification and census gaps and is not a maintenance-
  inclusive continuous measurement.
- It does not claim that its default record counts or the 64 KiB case are feasible on
  every machine. The defaults are bounds chosen to keep exact verification costs finite;
  the 32-record pilot proves the harness path, not default-scale feasibility.
- It does not prove production safety, format stability, backward compatibility, or Raft
  support.

### Known gaps for follow-up

- Failure injection for clean and cleanup is limited to core `cfg(test)` fault points;
  the harness only measures successful clean paths and cannot force deferred cleanup.
- Concurrency, group commit, and mixed read/write contention are out of scope; each
  engine call sequence is serialized.
- The mount record captures mount point, filesystem type, and source, but not Btrfs mount
  options or device layout; two machines with the same filesystem type can still differ.
- The harness does not set CPU affinity, drop caches, or isolate the page cache, so
  absolute timings are machine- and run-dependent even though both engines are matched.
  Clean also runs after full verification reads, so its timing is not a cold-cache number.
- Full exact scans dominate verification for large values because each record re-reads its
  referenced transaction. The bounded point probes reduce redundant gets, not scan cost.
  A smaller default such as 64 records for 64 KiB values is a guard, not a performance
  conclusion.
- Warmup and sample seeds are identical within a case to keep the workload matched, so
  timed samples differ only by fresh storage state and engine order.
