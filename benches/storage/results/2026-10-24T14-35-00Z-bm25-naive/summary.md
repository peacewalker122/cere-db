# Naive BM25 benchmark

## Outcome

- Full `--suite all` run passed: 87 distinct scenarios, 3 measured repetitions
  each (261 raw measurements), plus one discarded warm-up of the complete
  suite. The run recorded 18,381 operations across measured samples.
- Release smoke run passed at 32 records, 256-byte values, 512-byte flush
  threshold, one repetition, and no warm-up. It exercised 87 scenarios too;
  its temporary output and logs are under `.tmp/bm25-naive/`, not this archive.
- This archive contains the requested full-run raw JSON. The JSON parser,
  scenario-count check, and `git diff --check` passed. All 87 scenarios have
  exactly three measured samples and the configuration matches the requested
  full run.
- No performance-quality conclusion about BM25 ranking is drawn here.

## Commands

Help was inspected from the actual release benchmark executable using:

```sh
PATH="/home/peacewalker/.cargo/bin:$PATH" rtk cargo bench --manifest-path Cargo.toml --bench storage -- --help
```

Release smoke:

```sh
PATH="/home/peacewalker/.cargo/bin:$PATH" rtk cargo bench --bench storage -- --run --suite all --records 32 --value-bytes 256 --flush-bytes 512 --repetitions 1 --warmup-runs 0 --output .tmp/bm25-naive/smoke.json
```

Full run:

```sh
PATH="/home/peacewalker/.cargo/bin:$PATH" rtk cargo bench --bench storage -- --run --suite all --records 128 --value-bytes 256 --flush-bytes 512 --repetitions 3 --warmup-runs 1 --data-dir .tmp/bm25-naive/full-data --output benches/storage/results/2026-10-24T14-35-00Z-bm25-naive/raw.json
```

The run exited successfully. Build emitted 36 pre-existing Rust warnings; no
warning was promoted to an error. Dataset directories were placed on the
measured filesystem and scenario data is temporary. No `.envrc` or secrets
were read or included in this report.

## Source and environment

- UTC execution window: `2026-10-24T14:35:33Z`–`2026-10-24T14:42:36Z` (about
  423 seconds wall time). The harness reports 417.95 seconds including fixture
  setup and warm-up.
- Source HEAD: `51d2c8b33383df65c053a600df78d33662f2c483`. This is a dirty
  worktree build, not a clean-HEAD result; the captured tracked dirty paths are
  in `raw.json`. Existing changes in `benches/storage/{e2e.rs,modules.rs,
  support.rs}` and `src/fts/mod.rs` were part of the benchmark inputs.
- Rust: `rustc 1.97.1 (8bab26f4f 2026-07-14)`; optimized release profile,
  Linux x86_64; 11th Gen Intel Core i5-1135G7 @ 2.40 GHz; 8 available logical
  CPUs; two Tokio runtime workers.
- Data filesystem: btrfs on `/dev/nvme0n1p3` (SSD). OS page cache remained
  enabled; no cold-cache claim. Warm-up results are discarded. The harness
  reports batch/run times, not per-request p95/p99 latency.
- SHA-256 input fingerprints:
  - `benches/storage/main.rs`: `786861566831785b6d5ed3d6f4407c5cf5bc0ea826ac87543e358ff1262d809f`
  - `benches/storage/modules.rs`: `c3f334aa94f0d78747b871028f8befc4a9b8c75c0c6505697a9f74d2324e6e82`
  - `benches/storage/e2e.rs`: `7b3ad17a4da8011c5d433a1aea6e1c624a5389430d7d931d4e704f6f55ee1fc2`
  - `benches/storage/support.rs`: `4296f3af56572762d322c35bb1af6a52ef57fedfb16978571093c437a1499406`
  - `src/fts/mod.rs`: `d023ca78a676f92af3061eba06c36770a4951213c3367a23e4d08cdf749927e7`
  - `src/command.rs`: `664083c9997c4f9ebd47e1dcb1b64c66a0495b0af1db8a98779a913b9f066520`

## Workload and coverage

- Full configuration: `all`, 128 records, 20-byte keys, 256-byte binary
  storage values, 512-byte automatic-flush threshold, 1 MiB WAL segment,
  3 repetitions, 1 warm-up. No scenario filter was used; coverage is the
  harness's actual 87 emitted scenario names, rather than an assumed count.
- FTS corpus fixtures are deterministic UTF-8 text and are independent of the
  configured binary value size. The 128 initial documents contain 3,110 UTF-8
  bytes and 386 whitespace-token occurrences. The measured index-write case
  inserts 128 docs, replaces one, and deletes one: 130 document mutations,
  3,134 input bytes including the replacement. Its live corpus has 127 docs,
  3,075 UTF-8 bytes, and 382 tokens; document lengths range from 3 to 4 tokens
  (mean 3.008). Each mutation writes updated format-2 corpus statistics in the
  timed storage batch; there is no isolated stats-only timing in this harness.
- Search fixture selectivity on the 127 live documents: `common` 127/127,
  `rareterm` 1/127, missing term 0/127, `common AND rareterm` 1/127,
  `common OR rareterm` 127/127. The Boolean `fts/search_*` and ranked
  `fts/search_ranked_*` cases use the same corpus and corresponding query
  semantics. Query fixture construction and correctness checks are outside
  timed sections.
- The actual app-layer scenarios include `app/fts_and_search` and
  `app/fts_or_search`, but no ranked app/CLI scenario is emitted by this
  harness. Therefore this run reports no app/CLI ranked-search measurement.
- Ranked limit cases use the same OR query and 127 matching candidates, then
  request limits 1, 10, and 100. All three returned the expected result count
  and expected top document; the implementation scores all matching candidates
  before truncating. Median run times were:
  - limit 1: 4,251.092 ms
  - limit 10: 5,458.488 ms
  - limit 100: 6,060.109 ms
- For the post-flush FTS lifecycle, the benchmark awaited `KV2::shutdown()`
  before reopen, inspected the manifest snapshot across all levels, and rejected
  missing/empty referenced SSTables. Each measured checkpointed ranked search
  was backed by 7 or 8 manifest-registered SSTables. Reopened FTS documents,
  postings, and query results were checked, including ranked ordering and
  positive finite scores. The benchmark verifies statistics through persisted
  format-2 index behavior, but does not separately assert/report the decoded
  stats tuple or expose per-level SSTable counts.
- `e2e/fts_*` is an engine lifecycle workload with its own WAL/SSTable placement
  and flush/reopen semantics. Its warm post-flush query timings are not compared
  with module scenarios below.

## Matched Boolean versus ranked measurements

Median single-query batch times from same-placement `fts/*` module cases on the
same fixture. Ratio is ranked divided by Boolean; it is timing overhead only,
not a score-quality measure. Each query run is a single operation, and these
values are run timings rather than individual-request percentiles.

| Query | Boolean median | Ranked median | Ranked / Boolean |
| --- | ---: | ---: | ---: |
| common | 2,647.515 ms | 6,293.450 ms | 2.377× |
| rare | 29.832 ms | 56.692 ms | 1.900× |
| missing | 0.006516 ms | 7.129 ms | 1,094.042× |
| AND (common, rareterm) | 3,049.871 ms | 2,729.747 ms | 0.895× |
| OR (common, rareterm) | 3,832.977 ms | 5,004.076 ms | 1.306× |

The large missing-query ratio compares two very small timings and should not be
read as a general slowdown factor. Timing variation is substantial across the
three repetitions; see raw samples in `raw.json`. The ranked limit timings
also show substantial variance and must not be interpreted as independent
top-k measurements: every limit scores the full 127-candidate set.

## Verification notes

- Release `--help` was executed before the workloads. The smoke run passed all
  87 scenarios at its requested 32-record settings.
- Full run passed, with 87 scenario summaries × 3 measured repetitions and a
  discarded full-suite warm-up. `raw.json` parsed as JSON; its run configuration
  is exact; all scenario sample counts are 3; all three ranked limit cases are
  present; and the post-flush manifest counts are nonzero.
- The existing core/CLI verification was reported by the parent as 228 tests
  passing and 1 ignored; this test subtask did not rerun or modify that suite.
- `git diff --check` passed. No production code or benchmark source was edited
  for this test-only run. Existing result archives were left untouched.
