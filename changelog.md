# Changelog

This records the storage work in development order, from recovery correctness to
search and ingestion improvements. Dates are commit dates. Commit links identify
the implementation; benchmark links describe the measured source and conditions,
which can include a working tree that had not yet been committed.

Each milestone below includes quantitative evidence. A measured duration is not
automatically an improvement: speedup ratios are reported only for matched
workloads within the cited run. No cross-milestone speedup is inferred from
different datasets, storage placement, or source revisions.

## 2026-10-24 — Transactional WAL and recovery

Commit: [ee4a093](https://github.com/peacewalker122/cere-db/commit/ee4a093).

- Replaced the earlier WAL path with transaction chains and commit-gated replay.
  Recovery applies committed updates and ignores incomplete transactions.
- Separated physical WAL LSNs from application sequence numbers. Writes sync the
  WAL before acknowledgement and memtable publication.
- Added independent frame-header checksums so corrupted lengths cannot silently
  discard acknowledged history. Added narrow repair of interrupted newest-segment
  headers and retained support for large legacy records.
- Kept failed flush data readable and retryable, and skipped occupied SSTable IDs
  rather than overwriting orphaned files.

This is an ARIES-inspired adaptation for a no-steal LSM, not full ARIES: it does
not implement page LSNs, physical undo, or compensation log records. At this stage,
checkpoints were informational and rotated WAL segments were retained.

**Quantitative evidence:** the [post-redesign baseline](benches/storage/results/2026-10-24T02-59-23Z/summary.md)
measured 256 durable WAL appends with 256-byte values at a **754.386 ms** median
run duration. Sequential durable KV2 puts took **770.469 ms** for 256 calls.
The archived validation reported **183 tests passed, 1 ignored**. There is no
matched pre-redesign capture, so these numbers establish a baseline rather than
a WAL-redesign speedup.

## 2026-10-24 — Public storage benchmarks

Commit: [52ea51f](https://github.com/peacewalker122/cere-db/commit/52ea51f).

- Added module and end-to-end benchmarks through exposed storage APIs.
- Recorded commands, configurations, raw results, and verification boundaries so
  later changes could be assessed against reproducible workloads.

**Quantitative evidence:** the [initial full run](benches/storage/results/2026-10-24T02-59-23Z/summary.md)
covered **35 scenarios and 105 measured samples**, with 256 records, 256-byte
values, three repetitions, and one discarded warmup. Selected median calls were
**18.556 ms** for flush, **15.590 ms** for compaction, and **0.593 / 0.601 ms**
for reopen before/after flush. The first automatic-checkpoint sample retained
**96,180 WAL bytes** alongside **73,664 SSTable bytes**; checkpointing did not
yet reclaim WAL space.

Historical captures remain evidence for their recorded source and configuration;
they are not automatically comparable performance baselines for later features.

## 2026-10-24 — Tombstone correctness and atomic engine batches

Commit: [7c90975](https://github.com/peacewalker122/cere-db/commit/7c90975).

- Fixed scans to select the newest record, including tombstones, before filtering
  deleted keys. Equal-sequence ties prefer deletion.
- Preserved tombstones during compaction when older data outside the merge could
  otherwise become visible; added safe reclamation where outside ranges permit it.
- Added atomic `write_batch` support to KV2 and the CLI. Each batch uses one WAL
  transaction and publishes its visible sequence after all mutations are installed.
- Made admitted writes finish despite caller cancellation. Writes cancelled before
  admission do not apply.
- Kept committed writes successful when subsequent flush or compaction maintenance
  fails; those maintenance failures are logged and can be retried.

Raft-backed atomic batches remain explicitly unsupported.

**Quantitative evidence:** the [atomic-batch run](benches/storage/results/2026-10-24T11-04-42Z-atomic-batches/summary.md)
covered **55 scenarios and 165 measured samples**, using 1,000 records, 256-byte
engine values, three repetitions, and one discarded warmup. Matched results:

- Public writer: **321 → 73,607 mutations/s** for single puts versus batches of
  256 (**229.19×**); median run **3,113.69 → 13.59 ms**.
- KV2 with automatic checkpointing: **290 → 10,658 mutations/s** (**36.81×**);
  median run **3,454.10 → 93.82 ms**.
- App SET versus BATCH over matched textual payloads: **217 → 27,973 mutations/s**
  (**128.65×**); median run **4,599.01 → 35.75 ms**.

These are batching comparisons within that historical build, not an isolated
measurement of tombstone fixes. No matched performance measurement isolates
tombstone retention or reclamation. App text and engine binary payloads differ,
so their rates must not be compared directly.

## 2026-10-24 — Persistent full-text search

Commit: [51d2c8b](https://github.com/peacewalker122/cere-db/commit/51d2c8b).

- Added `FtsIndex` over the KV abstraction, storing documents, forward records,
  term/document postings, and metadata in a reserved KV namespace.
- Added Unicode tokenization, Boolean AND/OR search, and explicit `DOC` and `FTS`
  CLI commands. Ordinary KV writes do not automatically index their values.
- Updated documents and their index records in one atomic engine batch.
- Started background compaction reliably and protected SSTable readers with
  manifest leases while compaction publishes replacements and removes inputs.
- Added owned compaction-worker lifecycle management and async shutdown that
  drains admitted work before returning.

FTS shares the LSM's WAL, memtable, and SSTables; it has no separate index files.
Independent wrappers do not provide cross-instance writer coordination.

**Quantitative evidence:** the [FTS acceptance run](benches/storage/results/2026-10-24T12-59-17Z-fts-final/summary.md)
covered **73 scenarios and 219 measured samples**, with three repetitions and
one discarded warmup. Its 128-document corpus had **127 live documents** after
replacement/deletion. Representative KV2 lifecycle medians were:

- **398.326 ms** for 130 document mutations: 128 inserts, one replacement, one
  deletion, totaling 3,134 document bytes.
- Boolean common/AND/OR queries: **0.523 / 0.468 / 0.510 ms**, returning
  127 / 1 / 127 hits respectively.
- OR search after flush/reopen: **4.711 ms**, over **8 registered SSTables**.
- Reopen before/after flush: **1.034 / 1.642 ms**.

The disk-backed module fixture measured common-term search at **1,924.555 ms**;
its storage placement differs from the KV2 lifecycle fixture. This is a
feature baseline, not evidence that one abstraction is thousands of times faster
or that FTS improved pre-existing search performance.

## 2026-10-25 — Naive BM25 ranking and scale assessment

Commit: [cb0b624](https://github.com/peacewalker122/cere-db/commit/cb0b624).

- Added ranked AND/OR search while preserving Boolean search behavior.
- Persisted posting term frequencies, document lengths, corpus document/token
  totals, and per-term document frequencies in the document's atomic update.
- Added deterministic score ordering, configuration validation, overflow-resistant
  scoring, and query-scoped checks for contradictory statistics.
- Added pending-write barriers to prevent a cancelled admitted write from leaving
  a following FTS read/modify/write operation with stale statistics.
- Bumped FTS metadata to format 2. Format-1 indexes require rebuilding; opening
  does not silently migrate or erase them.

Ranking scores all matching candidates before applying the limit. It has no
top-k pruning, stemming, positions, or phrase search.

The [available-data assessment](benches/storage/results/2026-10-24T15-52-04Z-fts-available/summary.md)
used a snapshot with 97,413 inferred contiguous document IDs, not one million
verified documents. The seed showed declining window throughput as it grew;
that observation did not establish which component caused the slowdown or prove
a comparison against another LSM engine.

**Quantitative evidence:** the [BM25 archive](benches/storage/results/2026-10-24T14-59-43Z-bm25-final/summary.md)
contains **89 scenarios and 267 measured samples**, with 128 initial documents,
127 live documents, three repetitions, and one discarded warmup. Matched Boolean
versus ranked KV2 query medians quantify the added ranking cost:

- Common term: **0.581 → 1.030 ms**, ranked/Boolean duration **1.772×**.
- AND: **0.533 → 0.601 ms**, **1.129×**.
- OR: **0.570 → 1.010 ms**, **1.774×**.
- After flush/reopen, Boolean/ranked OR: **5.381 / 6.429 ms**; reopen:
  **2.199 ms**, with eight registered SSTables per sample.

Ranking added functionality and measurable overhead, not a claimed speedup.
This archive predates the final zero-corpus corruption guard; healthy nonzero
queries bypass that guard, but this is not a benchmark of the later source.
The separate 97,413-document assessment measured ranked unique-term search at
**15.47 ms**, common AND unique at **1.57 s**, and common top-10 at **2.65 s**.
Snapshot reopen took **2.272 s** with **311,092 KiB peak RSS**. These use a
different corpus and cannot form a before/after ranking comparison.

## 2026-10-25 — Checkpoint-driven WAL reclamation

Commits: [45f68b8](https://github.com/peacewalker122/cere-db/commit/45f68b8),
[065f270](https://github.com/peacewalker122/cere-db/commit/065f270).

- Added trusted recovery checkpoints containing a replay segment floor and
  physical/application sequence highwaters. Only a full owning-memtable flush
  can establish this boundary; partial flushes and untracked writes cannot.
- Reclaimed checkpoint-covered WAL segments after durable SSTable and manifest
  publication. Active history is retained, and segment IDs and sequences continue
  increasing rather than cycling through ring-buffer slots.
- Validated required SSTables and WAL history before retrying cleanup on open.
  Added bounded SSTable preflight and contiguous-section validation.
- Fixed oversized-record serialization and checked full snapshot coverage before
  authorizing reclamation. Bound checkpoint authority to the owning writer.
- Made directory durability cover bootstrap retries, relative paths, symlink
  chains, and traversal through `..` before covered WAL deletion.
- Corrected benchmark accounting to exclude three writes performed after timing.

**Quantitative evidence:** the [corrected lifecycle benchmark](benches/storage/results/2026-10-25T13-58-04Z-wal-reclamation-corrected/summary.md)
observed WAL sizes of **411 → 36 bytes**, a **91.24% reduction** at checkpoints.
Each of three measured samples observed **64 checkpoints and 16 reopens**, with
segment IDs advancing through 128. Its **7,746.800 ms** median
includes 128 timed puts, filesystem instrumentation, verification, and 16 reopens;
it is not raw put latency or a matched speedup measurement.

## 2026-10-26 — Document batches and durable group commit

Commits: [3599d24](https://github.com/peacewalker122/cere-db/commit/3599d24),
[f63c90c](https://github.com/peacewalker122/cere-db/commit/f63c90c).

- Added `put_documents` to aggregate document, posting, DF, and corpus-stat changes
  into one atomic batch. Duplicate IDs use the last input document.
- Bounded batches to 256 documents, 4 MiB of input, 8 MiB of complete encoded WAL
  transaction data, and 16,384 mutations. Invalid batches fail before publication.
- Added shared writer admission with a 64-request, 16 MiB weighted budget and
  oversized requests handled alone. Concurrent requests retain distinct WAL
  transactions while sharing a durability sync.
- Preserved cancellation, ordered barriers, checkpoint coverage, shutdown draining,
  and typed errors for all affected callers.

**Quantitative evidence:** the [matched public-API benchmark](benches/storage/results/2026-10-25T17-05-36Z-benchmarkafterr8-resumed/summary.md)
covered **11 cases and 33 measured samples**, using the same 256-document FTS
corpus (6,299 UTF-8 bytes), one warmup, and three measured samples:

- Batches of 16: **2,878.134 → 1,366.839 ms**, **2.11×** sequential/batch
  median-duration ratio.
- Batches of 64: **2,843.337 → 905.351 ms**, **3.14×**.
- One batch of 256: **83.22×**, with a **37.012 ms** median versus **3,080.212 ms**
  for 256 individual document calls.
- For 256 KV puts, writer medians were **821.701 / 214.191 / 57.294 ms** at
  concurrency **1 / 4 / 16**, respectively: **3.84× / 14.34×** lower duration
  relative to concurrency 1. Each sample wrote 70,656 key/value bytes.

These compare public paths and batch/concurrency settings within the new
implementation, not matched old/new builds. The size-1 ranges overlap and do not
support a firm optimization claim. Independent correctness probes observed 32
requests sharing one actual sync; timing alone does not prove grouping. The small
workload does not establish sustained compaction performance or resolve the
earlier large-seed degradation.

## 2026-10-27 — Selective leveled compaction

Commit: [a316ed8](https://github.com/peacewalker122/cere-db/commit/a316ed8).

- Replaced whole-level input selection with selected ranges and required overlaps,
  including transitive L0 overlap closure to preserve visibility.
- Partitioned outputs by an encoded-record byte target, keeping oversized records
  intact and unrelated files outside the merge.
- Added byte-based promotion into deeper levels for KV2 and RaftKV2. Initial
  configurable defaults are 2 MiB per output, a 16 MiB L1 budget, and 10× growth
  per deeper level; these are starting settings, not benchmark-tuned defaults.
- Published multiple output files in one atomic manifest replacement. Existing
  manifests remain readable; older binaries cannot read the new multi-output
  manifest entries.

**Quantitative evidence:** the [selectivity benchmark](benches/storage/results/2026-10-26T-compaction-selectivity/summary.md)
started with 18 L1 partitions and applied 24 localized replacements and eight
deletes. Each run selected **41,034 of 611,092 candidate input bytes**, retained
**17 L1 files byte-for-byte**, and wrote **31,214 output bytes**. The median of
three measured calls was **10.871812 ms**.

The selected fraction was **6.7149%**: **93.2851%** of candidate bytes stayed out
of this merge. That is a whole-level counterfactual, not a measured speedup or
lifetime write-amplification reduction. Fingerprinting pre-read all candidates,
so timing is warm-cache. No matched whole-level baseline or 10k/100k/1M growth
benchmark was performed. Merges still load selected inputs into memory, and dense
overlaps can expand the selected range.

## 2026-10-28 — Matched local comparison against RocksDB

Added an isolated comparison harness in
[`tools/storage_compare`](tools/storage_compare/README.md), without adding
RocksDB to production dependencies.

**Quantitative evidence:** the [reviewed persistent-filesystem run](benches/storage/results/2026-10-28T03-59-08Z-rocksdb-persistent-comparison/summary.md)
compares CereDB with **RocksDB 10.4.2** (Rust crate 0.24.0), on the same
NVMe-backed Btrfs mount with `compress=zstd:1`. Both use WAL-enabled synchronous
writes, identical insertion order, 1,024 unique 20-byte keys, and 256-byte values:
**282,624 logical bytes per sample**. Engine execution order alternates. Each
engine/batch case has one discarded warmup and three measured samples:
**8 cases, 24 measured samples**. Full value verification after writes and
graceful reopen is outside timing.

- Individual puts: CereDB **3,138.720 ms**, RocksDB **3,036.882 ms**;
  CereDB/RocksDB median-duration ratio **1.034×**.
- Batch 16: CereDB **195.973 ms**, RocksDB **196.613 ms**; ratio **0.997×**.
- Batch 64: CereDB **50.806 ms**, RocksDB **53.137 ms**; ratio **0.956×**.
- Batch 256: CereDB **27.960 ms**, RocksDB **25.758 ms**; ratio **1.085×**.

Relative to RocksDB's median duration, CereDB individual puts were **3.4% longer**;
batch 16 was **0.3% shorter**, batch 64 was **4.4% shorter**, and batch 256 was
**8.5% longer**. These percentages describe median elapsed time, not throughput.
Sample ranges overlap at batch sizes 16 and 64. These are public async CereDB
versus blocking synchronous RocksDB API timings, including their different
scheduling and ownership costs, not isolated internal-LSM measurements. The
small dataset stays below the configured 1 MiB flush/write-buffer threshold.
Its values contain only 251 distinct payload patterns, and filesystem compression,
OS caching, and uncontrolled CPU frequency affect interpretation. This does not
establish general performance parity, sustained flush/compaction performance,
large-scale behavior, or power-loss durability.

The [earlier tmpfs run](benches/storage/results/2026-10-28T03-38-09Z-rocksdb-comparison/summary.md)
is retained as a memory-backed filesystem/public-API microbenchmark. Its
**12.714× / 4.699× / 3.285× / 2.941×** CereDB/RocksDB duration ratios are **not
persistent-storage results**. Independent instrumentation confirmed the expected
WAL sync counts in that run; successful sync calls on tmpfs do not imply
power-loss persistence.

**etcd was not benchmarked.** Its replicated networked service needs a separate
comparison with equivalent consensus, durability, and client-call semantics; local
KV2 timings cannot establish an advantage over etcd.

## Remaining measurement gaps

- Measure windowed ingestion throughput as datasets grow through 10k, 100k, and
  one million documents, including flush/compaction debt and shutdown draining.
- Compare old and new compaction under identical data, overlap, durability, and
  cache conditions before claiming a speedup or lifetime write-amplification gain.
- Profile WAL sync, FTS preparation reads, memtable work, and compaction separately
  before attributing growth-related slowdown to any one component.
- Distinguish process-visible fault injection and reopen checks from actual
  power-loss validation.
