# Naive BM25 final benchmark

## Outcome and scope

- Both release `--suite all` runs completed and passed the benchmark harness's
  correctness assertions. Smoke: 89 scenarios, 32 records, 256-byte storage
  values, 512-byte flush threshold, one measured sample and no warm-up. Full:
  89 scenarios × 3 measured repetitions = 267 raw measurements, plus one
  discarded full-suite warm-up; the harness reports 252.215 seconds including
  setup and warm-up.
- This final archive supersedes the earlier intermediate
  `2026-10-24T14-35-00Z-bm25-naive` and `2026-10-24T14-43-09Z-bm25-app-ranked`
  archives. Those archives are retained unchanged. The former omitted both
  ranked app scenarios; the latter measured those cases separately. This full
  run contains both app ranked scenarios, with parse and execution inside the
  measured interval.
- No production or benchmark source files were edited by this test subtask.
  The pre-existing dirty worktree was measured as-is. No `.envrc` or secrets
  were read or included. No test count is claimed here; the parent-provided
  232 passing / 1 ignored count was not rerun by this subtask.

## Commands and provenance

Commands run from the repository root with the requested PATH prefix:

```sh
PATH=/home/peacewalker/.cargo/bin:$PATH rtk cargo bench --bench storage -- --run --suite all --records 32 --value-bytes 256 --flush-bytes 512 --repetitions 1 --warmup-runs 0 --data-dir target/bm25-final-smoke-data --output target/bm25-final-smoke.json
PATH=/home/peacewalker/.cargo/bin:$PATH rtk cargo bench --bench storage -- --run --suite all --records 128 --value-bytes 256 --flush-bytes 512 --repetitions 3 --warmup-runs 1 --data-dir target/bm25-final-full-data --output benches/storage/results/2026-10-24T14-59-43Z-bm25-final/raw.json
```

- UTC run started at `2026-10-24T14:59:43Z`; the full measured harness wall
  duration (including setup and warm-up) was 252.215 seconds. The separate
  build took 54.11 seconds before smoke execution.
- HEAD: `51d2c8b33383df65c053a600df78d33662f2c483`, with a dirty worktree.
  `raw.json` records tracked dirty inputs and the qualification that HEAD alone
  does not identify the complete build input. No clean-HEAD claim is made.
- Rust `1.97.1 (8bab26f4f 2026-07-14)`, Linux x86_64, release profile,
  11th Gen Intel Core i5-1135G7 @ 2.40GHz, 8 available logical CPUs, two Tokio
  workers. Data filesystem: btrfs on `/dev/nvme0n1p3`, mounted with
  `compress=zstd:1`, SSD. OS page cache remained enabled; this is not a
  cold-cache run. Build completed with existing compiler warnings.
- Benchmark input SHA-256:
  - `benches/storage/main.rs`: `786861566831785b6d5ed3d6f4407c5cf5bc0ea826ac87543e358ff1262d809f`
  - `benches/storage/modules.rs`: `0e1290883008686302ca332c7fba4e486da63714a78a419c429de2d39160b28e`
  - `benches/storage/e2e.rs`: `6ef80f7899761bab1cad95bcc22dcf47adba4c88f522296ca03b9dbf84dd936e`
  - `benches/storage/support.rs`: `4296f3af56572762d322c35bb1af6a52ef57fedfb16978571093c437a1499406`
  - `src/fts/mod.rs`: `d10c8fdd6c55d749583c02155559f5382a3518270a67c75f136f3e808fe76f77`
  - `src/command.rs`: `664083c9997c4f9ebd47e1dcb1b64c66a0495b0af1db8a98779a913b9f066520`

## Dataset and checks

- Full run configuration: 128 records, 20-byte keys, 256-byte binary values,
  512-byte automatic flush, 1 MiB WAL segments, 3 repetitions, 1 discarded
  warm-up. Full workload is unfiltered. FTS documents are deterministic UTF-8
  text, distinct from the binary storage values. The initial 128-document FTS
  corpus is 3,110 UTF-8 bytes and 386 whitespace-token occurrences. After the
  benchmark's one replacement and one deletion, 127 documents remain, with
  3,075 UTF-8 bytes and 382 tokens; document lengths are 3–4 tokens (mean
  3.008).
- On the 127 live docs: `common` matches 127, `rareterm` matches 1, missing
  matches 0, `common AND rareterm` matches 1, and `common OR rareterm` matches
  127. Ranked common/rare/missing/AND/OR and limits 1/10/100 scenarios all
  appear in the raw JSON. The limit cases share the OR query and score all 127
  matching candidates before truncation; changing top-k does not prune scoring.
- App scenarios present include `app/fts_and_search`, `app/fts_or_search`,
  `app/fts_ranked_and_search`, and `app/fts_ranked_or_search`. The ranked app
  timers include command parsing, dispatch, search execution, and response
  construction.
- Post-flush FTS search/reopen scenarios report 8 registered SSTables in each
  of the three measured runs. Before reporting that count, the benchmark
  inspects manifest entries across all levels and rejects any missing or empty
  referenced SSTable; reopened docs, postings, Boolean results, and ranked
  results are then validated. This is an aggregate across levels, not a
  per-level distribution.
- JSON validation passed: 89 unique scenario summaries and 267 full-run raw
  samples, exactly 3 samples per scenario; required FTS Boolean/ranked, limit,
  app, and post-flush scenario names were asserted present; smoke JSON also
  contained 89 scenarios and the required app cases. All recorded post-flush
  SSTable counts were nonzero and equal to 8. Exact slash-qualified filter
  activation was separately checked at 32 records: `fts/search_ranked_limit_1`
  emitted exactly that result, and `app/fts_ranked_and_search` emitted exactly
  that app result. `git diff --check` passed.

## Measurements

Medians below are single-query run times across 3 runs. Boolean/ranked ratios
compare matching query, corpus, and storage placement only; they are timing
overhead ratios, not a quality measure. Operations/s are the harness's actual
query rate for these single-operation samples. Run times are not individual
request percentiles.

### Disk-backed public FTS module (`fts/*`)

| Query | Boolean median | Boolean queries/s | Ranked median | Ranked queries/s | Ranked / Boolean |
| --- | ---: | ---: | ---: | ---: | ---: |
| common | 2,498.760 ms | 0.399 | 4,868.384 ms | 0.213 | 1.948× |
| rare | 31.101 ms | 34.346 | 44.626 ms | 21.786 | 1.435× |
| missing | 0.004971 ms | 99,436.526 | 5.714 ms | 173.736 | 1,149.430× |
| AND | 2,248.095 ms | 0.447 | 2,370.709 ms | 0.439 | 1.055× |
| OR | 2,406.297 ms | 0.409 | 4,242.290 ms | 0.230 | 1.763× |

The missing-query ratio divides two tiny measurements; use the actual times
rather than treating the ratio as a general slowdown factor.

### KV2 engine lifecycle (`e2e/fts_*`)

| Query | Boolean median | Boolean queries/s | Ranked median | Ranked queries/s | Ranked / Boolean |
| --- | ---: | ---: | ---: | ---: | ---: |
| common | 0.581 ms | 1,718.209 | 1.030 ms | 969.249 | 1.772× |
| rare | 0.007753 ms | 129,115.558 | 0.013384 ms | 65,369.446 | 1.726× |
| missing | 0.001255 ms | 788,229.112 | 0.004437 ms | 245,198.202 | 3.535× |
| AND | 0.533 ms | 1,879.687 | 0.601 ms | 1,689.570 | 1.129× |
| OR | 0.570 ms | 1,753.857 | 1.010 ms | 994.503 | 1.774× |

These e2e timings are kept separate from the disk-backed module timings because
they have a different engine lifecycle and storage placement.

### App parse plus execution and index mutation cost

| App query | Boolean median | Boolean queries/s | Ranked median | Ranked queries/s | Ranked / Boolean |
| --- | ---: | ---: | ---: | ---: | ---: |
| AND | 0.570 ms | 1,770.980 | 0.588 ms | 1,689.923 | 1.032× |
| OR | 0.569 ms | 1,629.677 | 1.019 ms | 984.402 | 1.792× |

`fts/index_put_documents` performs 128 inserts, one replacement, and one
deletion (130 timed document mutations); median batch time is 4,415.188 ms,
with 29.571 mutations/s. The current index updates corpus statistics within
these mutations; this is the aggregate mutation cost, not an isolated
statistics-only measurement.

Ranked module OR-query limits 1, 10, and 100 have median run times 4,597.790,
4,334.248, and 5,050.471 ms respectively, or 0.216, 0.225, and 0.197
queries/s. All score the same 127 matching candidates before applying the
requested limit. The user-facing app keeps its fixed limit of 100; its timings
are shown in the app table and include parsing and execution.

After checkpoint and reopen, Boolean OR query median is 5.381 ms (169.863
queries/s), ranked OR is 6.429 ms (155.728 queries/s), and reopen is 2.199 ms
(448.117 reopens/s). Each sample references eight nonempty SSTables registered
in the manifest across all levels.

No WAND, top-k pruning, ranking-quality claim, or request p95/p99 is measured
or implied. See `raw.json` for all samples, environment, arguments, disk
statistics, and scenario-level summaries.
