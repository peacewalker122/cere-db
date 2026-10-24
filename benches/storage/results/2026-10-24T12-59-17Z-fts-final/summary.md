# Final-source storage and FTS benchmark

## Run and provenance

- Result: all 73 scenarios passed, with 3 measured samples per scenario (219
  measured samples total) and 1 discarded warmup run (73 scenario executions).
- Command:

  ```sh
  PATH="/home/peacewalker/.cargo/bin:$PATH" cargo bench --bench storage -- \
    --run --suite all --records 128 --value-bytes 256 --repetitions 3 \
    --warmup-runs 1 --flush-bytes 512 --data-dir target/fts-final-data \
    --output benches/storage/results/2026-10-24T12-59-17Z-fts-final/raw.json
  ```

- Harness wall time including setup and warmup: 120.126 seconds.
- Source HEAD: `7c90975d3459021468c569a14d6610fe9f65cbf1`. This is a dirty-worktree
  run, not a clean-commit benchmark. The raw metadata records tracked changes;
  the source tree also had untracked FTS files used by the build. The revision
  alone does not identify the complete build input.
- Machine: 11th Gen Intel Core i5-1135G7 @ 2.40 GHz, Linux x86_64, 8 available
  logical CPUs, Rust `1.97.1`, Tokio runtime with 2 workers. Dataset filesystem:
  Btrfs. OS page cache retained; no cold-cache claim.
- Configuration: 128 records, 20-byte keys, 256-byte binary values, 512-byte
  checkpoint threshold, WAL segment size 1 MiB.
- Raw JSON includes exact argv, elapsed wall time, source qualification, CPU,
  filesystem type, all scenarios and samples, timing limits, and correctness
  evidence. Amortized rates use the actual operation count. Batch elapsed times
  are not individual-operation percentiles.
- SHA-256 source snapshot fingerprints:

  | File | SHA-256 |
  | --- | --- |
  | `benches/storage/main.rs` | `786861566831785b6d5ed3d6f4407c5cf5bc0ea826ac87543e358ff1262d809f` |
  | `benches/storage/e2e.rs` | `62dc3dfc4434c7681653f7171670498cc153cf7791f06a58925a89df0b3856ee` |
  | `benches/storage/modules.rs` | `0bb24328777375e49901f442fe500b1bf5b619f2ba21db1d0efc2c9bcb2117b0` |
  | `benches/storage/support.rs` | `b73c39ec21be0d9f15bf95673fefba2a869217778eae39a63d62260c5224dbe4` |
  | `benches/storage/README.md` | `092c811324c85c905fe4a26a0d75afd689e61e3849ee72c93d19146cfea9ddf8` |

## Checkpoint fixture diagnosis

The requested all-suite smoke at 16 records and a 512-byte threshold failed at
`app/sequential_set_checkpoint`, after the other preceding checkpoint workload
had produced an SSTable. This was a fixture-size limit, not a checkpoint or
storage defect: the app fixture's 16 fixed-width keys and `value-N` values total
438 logical bytes, below the 512-byte memtable threshold. A focused run at 19
records totaled 522 logical bytes and passed the checkpoint, verification, and
reopen checks. No checkpoint assertion was removed or weakened. The all-suite
smoke then passed at 32 records, 256-byte values, and a 512-byte threshold.

The FTS after-flush setup drains and joins the compaction worker before checking
the manifest. It counts registered SSTables across every manifest level and
checks that each referenced file exists and is non-empty. The final run recorded
8 registered tables per `e2e/fts_search_after_flush` and
`e2e/fts_reopen_after_flush` measured sample. The after-flush FTS verification
also rechecked document contents, deleted-document absence, postings, query hit
counts, and ascending result order after reopening. With `max_level0_files = 1`,
compaction may move tables out of L0, so the count is deliberately across all
levels.

## FTS and app results

Median run times for the batch described by each scenario, not per-request
percentiles. Search scenarios perform one query per sample. Each row has three
measured samples.

| Scenario | Actual operations per run | Logical bytes per run | Median run |
| --- | ---: | ---: | ---: |
| `fts/index_put_documents` | 130 mutations (128 puts, 1 replacement, 1 delete) | 3,134 document bytes | 2,296.613 ms |
| `fts/search_common` | 1 query | not reported | 1,924.555 ms |
| `fts/search_rare` | 1 query | not reported | 18.816 ms |
| `fts/search_missing` | 1 query | not reported | 0.002 ms |
| `fts/search_and` | 1 query | not reported | 1,626.515 ms |
| `fts/search_or` | 1 query | not reported | 1,936.394 ms |
| `e2e/fts_index_put` | 130 mutations (128 puts, 1 replacement, 1 delete) | 3,134 document bytes | 398.326 ms |
| `e2e/fts_search_common` | 1 query | not reported | 0.523 ms |
| `e2e/fts_search_rare` | 1 query | not reported | 0.007 ms |
| `e2e/fts_search_missing` | 1 query | not reported | 0.001 ms |
| `e2e/fts_search_and` | 1 query | not reported | 0.468 ms |
| `e2e/fts_search_or` | 1 query | not reported | 0.510 ms |
| `e2e/fts_search_after_flush` | 1 OR query | not reported | 4.711 ms |
| `e2e/fts_reopen_before_flush` | 1 reopen | not reported | 1.034 ms |
| `e2e/fts_reopen_after_flush` | 1 reopen | not reported | 1.642 ms |
| `app/doc_put_documents` | 128 parsed and executed DOC PUT commands | 3,110 UTF-8 document bytes | 386.271 ms |
| `app/fts_and_search` | 1 parsed and executed query | not reported | 0.483 ms |
| `app/fts_or_search` | 1 parsed and executed query | not reported | 0.495 ms |
| `app/sequential_set_checkpoint` | 128 parsed and executed SET mutations | 3,602 UTF-8 key/value bytes | 474.083 ms |

App document bytes are the actual UTF-8 command payloads and are not the
configured 256-byte binary engine values. The FTS corpus has 128 documents,
with one document replaced and the last document deleted, leaving 127 live
documents. `fts/search_common` and `fts/search_or` request and verify 127 hits;
rare-term and AND queries request/verify 1 hit, and the missing-term query
requests/verifies 0. The e2e common, OR, and after-flush OR queries request and
verify 127 hits; rare and AND each return 1, and missing returns 0. App command
searches use the executor's 100-hit limit: AND returns 1 and OR returns 100.
These timings should not be read as single-hit latency.
No speedup comparison is made against the earlier 1,000-record run because the
corpus and final storage source differ.

## Historical result status

`2026-10-24T12-07-30Z-fts` remains untouched as an intermediate 1,000-record
archive (219 measured samples). It predates the final reader-lease and worker
shutdown changes and is superseded for final-source acceptance by this
128-record run. The two runs are not comparable baselines.
