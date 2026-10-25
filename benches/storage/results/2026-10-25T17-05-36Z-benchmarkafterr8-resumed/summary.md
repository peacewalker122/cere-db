# Resumed matched FTS and writer benchmark

Archive: `2026-10-25T17-05-36Z-benchmarkafterr8-resumed`

## Qualification

All 11 selected cases completed on the same release benchmark executable, with one discarded warmup and three measured samples per case. The binary SHA-256 is recorded in `binary.sha256`; source SHA-256 files before build and after the last run are identical. The worktree was already dirty and has user/other-agent changes; the recorded HEAD and dirty-file/diff snapshots qualify context but do not make this a clean-commit benchmark.

## Configuration

- Release profile, two Tokio runtime workers; 256 records and configured binary values of 256 bytes for measured runs.
- FTS corpus is UTF-8 fixture text, 6,299 actual bytes per dataset; configured engine value size does not describe FTS document length.
- Writer operations use 20-byte keys plus 256-byte values: 70,656 logical bytes per sample.
- `--flush-bytes 16384`, `--wal-segment-bytes 1048576`; selected cases do not demonstrate that flushing, rotation, or reopen occurred.
- OS page cache retained; not a cold-cache run.
- Benchmark source confirms timed FTS calls include the public `put_document` / `put_documents` processing; fixture construction, open, exact document round trips, Boolean/BM25 validation and cleanup are outside timing. Writer setup inputs are outside timing; task creation/scheduling, calls and joins are inside; full memtable verification is outside.

## FTS: median and min–max run duration

Each row compares same 256-doc corpus between sequential calls and batches of at most the indicated size. Ratio is sequential median duration divided by batch median duration (not a statistical confidence interval).

| Max batch | Sequential ms | Batch ms | Duration ratio |
|---:|---:|---:|---:|
| 1 | 3988.369 [3745.156, 4578.376] | 2923.590 [2857.393, 4296.957] | 1.36× |
| 16 | 2878.134 [2751.168, 3101.688] | 1366.839 [1285.470, 1720.704] | 2.11× |
| 64 | 2843.337 [2794.701, 3566.751] | 905.351 [706.503, 1187.048] | 3.14× |
| 256 | 3080.212 [2994.076, 3844.415] | 37.012 [36.250, 37.499] | 83.22× |

## Concurrent writer: median and min–max run duration

| Tasks | Median ms | Range ms | concurrency-1 / duration |
|---:|---:|---:|---:|
| 1 | 821.701 | [810.616, 837.241] | 1.00× |
| 4 | 214.191 | [201.093, 219.392] | 3.84× |
| 16 | 57.294 | [52.426, 65.730] | 14.34× |

## Smoke and interpretation

A separate 32-record, 64-byte configured-value smoke passed for `fts/put_sequential_1`, `fts/put_batch_1`, and `writer/concurrent_16` with one sample and no warmup. JSON outputs validated exact scenario selection, configuration, operation counts and payload-byte counts; all eleven measured cases have three samples each.

These are aggregate batch durations, not per-request latency percentiles. Concurrent-writer timings alone do not prove sync grouping. These runs make no production capacity or 97k-document degradation-fix claim. No unrelated suite, source edit, commit, push, or large-scale dataset run was performed.

Raw JSON, stdout, stderr, per-command wall time, commands, environment, source hashes and build log are retained in this directory.
