# Full storage and FTS benchmark

The interrupted benchmark process was not active when this recovery started.
The newest existing full result was for a different source revision and had no
FTS scenarios, so it was retained unchanged and a new run was performed. The
full run completed successfully; the harness's result, document, query,
checkpoint, and reopen validations passed. Exact raw samples are in
[`raw.json`](raw.json).

## Commands and run record

Full release run:

```sh
cargo bench --bench storage -- --run --suite all \
  --records 1000 --value-bytes 256 --repetitions 3 --warmup-runs 1 \
  --flush-bytes 16384 \
  --data-dir target/2026-10-24T12-07-30Z-fts-data \
  --output benches/storage/results/2026-10-24T12-07-30Z-fts/raw.json
```

- Start: 2026-10-24 12:07:18 UTC. End: 2026-10-24 12:21:00 UTC.
- Harness wall time, including setup and warmup: 760.66 seconds.
- Configuration: all suites, no filter; 1,000 records, 20-byte keys,
  256-byte engine values, 16,384-byte flush threshold, 1 MiB WAL segments,
  three measured repetitions and one discarded warmup.
- Coverage: 73 scenarios and 219 measured samples (three per scenario), plus
  73 warmup scenario executions. The modules, end-to-end, and app suites ran.
  This includes `fts/index_put_documents`, `e2e/fts_*`, and
  `app/doc_put_documents` / `app/fts_*`.
- Source HEAD: `7c90975d3459021468c569a14d6610fe9f65cbf1`; dirty working tree.
  The tracked working-tree changes recorded by the harness are in `raw.json`;
  untracked files were present but are not represented in that field. No source
  or benchmark code was edited for this run.
- Linux x86_64, Fedora kernel `7.1.13-200.fc44.x86_64`, Rust
  `rustc 1.97.1 (8bab26f4f 2026-07-14)`, release profile, 2 Tokio workers,
  8 logical CPUs (11th Gen Intel Core i5-1135G7, 4 cores / 8 threads).
- Dataset filesystem: Btrfs on `/dev/nvme0n1p3[/home]`, mounted at `/home`;
  dataset directories were under `target/`. OS page cache was retained, no
  cold-cache claim; machine load was uncontrolled. No environment secrets were
  read or recorded.

The pre-existing `target/fts-bench-smoke.json` was a filtered `fts_` smoke
(`16` records, `128`-byte flush threshold) and did not cover the module index
write or app document-write scenarios. After the full run, a separate broad
all-suite smoke was run with 32 records, 64-byte values, 512-byte flush
threshold, one repetition, and no warmup; it passed and includes those missing
scenarios. Its raw output is `target/2026-10-24T12-21-30Z-fts-smoke.json`.
The broad smoke therefore supplements, rather than precedes, the full run.

## FTS fixture and limits

The FTS fixture contains 1,000 UTF-8 documents totaling 24,910 document bytes
(23–36 bytes each). The common token is present in all 1,000 original
documents; the fixture then replaces one document and deletes one, leaving
999 live documents. Search result caps are at most 1,000 for FTS API and KV2
scenarios. Common and OR queries return 999 hits; rare and AND return 1; the
missing-term query returns 0. App FTS uses the command's default 100-result
cap (AND 1, OR 100). Result correctness and ordering were checked outside the
timed search calls.

The FTS index-put workloads perform 1,002 timed mutations per run: 1,000
inserts, one replacement, and one delete. Their reported logical document bytes
are 24,934, including the replacement text. Search scenarios time one public
query per measured sample. App document puts time 1,000 parsed and executed
`DOC PUT` commands; app search timings include parse, dispatch, and execution.
Fixture setup and response verification are outside those app search timings.

## Selected FTS results

Times below are median whole-scenario run durations across the three measured
samples, not individual-document or individual-request percentiles. API query
throughput is one query divided by the median run duration; it is shown only to
make the scale legible.

| Scenario | Operations per run | Median run | Approx. query / mutation rate |
| --- | ---: | ---: | ---: |
| `fts/index_put_documents` | 1,002 mutations | 11,927.30 ms | 84.0 mutations/s |
| `fts/search_common` | 1 query, 999 hits | 15,659.42 ms | 0.064 queries/s |
| `fts/search_rare` | 1 query, 1 hit | 18.98 ms | 52.7 queries/s |
| `fts/search_missing` | 1 query, 0 hits | 0.00247 ms | 404,858 queries/s |
| `fts/search_and` | 1 query, 1 hit | 15,916.48 ms | 0.0628 queries/s |
| `fts/search_or` | 1 query, 999 hits | 17,552.18 ms | 0.0570 queries/s |
| `e2e/fts_index_put` | 1,002 mutations | 3,086.91 ms | 324.6 mutations/s |
| `e2e/fts_reopen_before_flush` | 1 reopen | 6.84 ms | — |
| `e2e/fts_search_common` | 1 query, 999 hits | 3.74 ms | 267.3 queries/s |
| `e2e/fts_search_rare` | 1 query, 1 hit | 0.00784 ms | 127,632 queries/s |
| `e2e/fts_search_missing` | 1 query, 0 hits | 0.00100 ms | 1,004,016 queries/s |
| `e2e/fts_search_and` | 1 query, 1 hit | 3.47 ms | 288.0 queries/s |
| `e2e/fts_search_or` | 1 query, 999 hits | 3.93 ms | 254.3 queries/s |
| `e2e/fts_search_after_flush` | 1 query, 999 hits | 9.00 ms | 111.1 queries/s |
| `e2e/fts_reopen_after_flush` | 1 reopen | 8.30 ms | — |
| `app/doc_put_documents` | 1,000 parsed/executed document mutations | 3,156.83 ms | 316.8 mutations/s |
| `app/fts_and_search` | 1 parsed/executed query, 1 hit | 3.62 ms | 276.0 queries/s |
| `app/fts_or_search` | 1 parsed/executed query, 100-hit cap | 3.41 ms | 293.3 queries/s |

The after-flush reopen/query scenarios force the checkpoint before their timed
reopen or search operation; checkpoint setup itself is not included in those
timings. FTS fixture and posting verification is performed around those
measurements. These are batch-level timings, not p95/p99 latency. No speedup
claim is made against a different binary, payload, or historical baseline.

## Validation

- Parsed `raw.json`; verified the exact requested full-run configuration, source
  HEAD, 73 scenario groups, 219 samples, three samples with sample indexes 0–2
  for each scenario, and presence of all FTS module, KV2, and app scenarios,
  including `app/doc_put_documents`.
- The benchmark exited successfully after its built-in correctness and
  checkpoint checks; no scenario failure was reported.
- `git diff --check` passed. The existing 35- and 55-scenario archived result
  directories were left untouched.
- The 220-test/one-ignored result was supplied in the task context; tests were
  not rerun as part of recovering the benchmark.
