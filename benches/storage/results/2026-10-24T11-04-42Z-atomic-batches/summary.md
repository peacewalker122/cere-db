# Atomic batch benchmark results

Measured on 2026-10-24. The full run completed successfully; all public-value,
reopen, and checkpoint checks in the harness passed. Exact per-sample output is
in [`raw.json`](raw.json).

## Reproduction

Release smoke command (completed first):

```sh
cargo bench --bench storage -- --run --suite all \
  --records 32 --value-bytes 64 --repetitions 1 --warmup-runs 0 \
  --flush-bytes 512 --data-dir target/benchmark-smoke-data \
  --output target/benchmarks/storage-smoke.json
```

Full release run (completed after smoke):

```sh
cargo bench --bench storage -- --run --suite all \
  --records 1000 --value-bytes 256 --repetitions 3 --warmup-runs 1 \
  --flush-bytes 16384 \
  --data-dir target/2026-10-24T11-04-42Z-atomic-batches-data \
  --output benches/storage/results/2026-10-24T11-04-42Z-atomic-batches/raw.json
```

The full run reported a harness wall time of 363.29 seconds, including setup and
warmup. Its report timestamp is 2026-10-24T11:10:57Z. It contains 55 scenarios
and 165 measured samples (three per scenario); 55 warmup scenario runs were
discarded. No scenario failure was reported. Datasets were freshly prepared for
each scenario repetition and correctness/reopen verification was outside timed
sections. The run uses normal durable writes, leaves the OS page cache enabled,
and makes no cold-cache claim. The checkpoint e2e workload disables background
compaction. WAL history remains retained after checkpoint; checkpoint is not
assumed to shorten recovery.

## Environment and source state

- Source `HEAD`: `52ea51fa0a62cd6f82303e9423747a2a001b3a6c`; this is a dirty working
  tree run, not a clean-commit benchmark. At capture, tracked changes included
  the benchmark harness and related storage/API work. Other unrelated untracked
  and modified files were also present and were preserved. The complete tracked
  status captured by the harness is recorded in `raw.json`; untracked files are
  not represented there.
- Linux x86_64, Fedora kernel `7.1.13-200.fc44.x86_64`; Rust
  `rustc 1.97.1 (8bab26f4f 2026-07-14)`; release profile, debug assertions off;
  two Tokio workers; 8 available logical CPUs.
- 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz; four cores / eight threads.
- Dataset filesystem: `/dev/nvme0n1p3[/home]`, Btrfs mounted at `/home` with
  `rw,relatime,seclabel,compress=zstd:1,ssd,discard=async,space_cache=v2`.
  Dataset directory was under `target/`. System load and page-cache state were
  not controlled. No credentials or environment secrets are included.

## Matched mutation comparisons

Rates below are the median of the three individual run mutation rates
(`mutations / elapsed time`); median run time is also shown. Speedup is the
scenario's median mutation rate divided by the matched single-operation
baseline's median mutation rate. Configurations and payloads are matched within
each group; batch counts refer to mutations, not calls. Values are rounded.

### Public writer, single put vs writer batches

All scenarios use the same 1,000-record configured 256-byte value workload.

| Scenario | Median run | Median mutations/s | Speedup vs writer/put |
| --- | ---: | ---: | ---: |
| `writer/put` | 3,113.69 ms | 321 | 1.00× |
| `writer/batch_1` | 3,085.48 ms | 324 | 1.01× |
| `writer/batch_16` | 195.24 ms | 5,122 | 15.95× |
| `writer/batch_64` | 52.25 ms | 19,140 | 59.60× |
| `writer/batch_256` | 13.59 ms | 73,607 | 229.19× |

### KV2 sequential checkpoint vs engine batch puts

`e2e/batch_sequential_checkpoint` and `e2e/batch_put_*` use the configured
binary engine payload, matching automatic flush threshold and reopen validation.

| Scenario | Median run | Median mutations/s | Speedup vs sequential |
| --- | ---: | ---: | ---: |
| `e2e/batch_sequential_checkpoint` | 3,454.10 ms | 290 | 1.00× |
| `e2e/batch_put_1` | 3,379.73 ms | 296 | 1.02× |
| `e2e/batch_put_16` | 508.14 ms | 1,968 | 6.80× |
| `e2e/batch_put_64` | 338.86 ms | 2,951 | 10.19× |
| `e2e/batch_put_256` | 93.82 ms | 10,658 | 36.81× |

### App sequential SET vs atomic BATCH

These run through tokenization, command parsing, dispatch and async execution.
They share textual keys and `value-{index}` values, matching the app-level
contract. These are not directly comparable to the binary-payload engine group.

| Scenario | Median run | Median mutations/s | Speedup vs sequential SET |
| --- | ---: | ---: | ---: |
| `app/sequential_set_checkpoint` | 4,599.01 ms | 217 | 1.00× |
| `app/batch_1` | 3,104.96 ms | 322 | 1.48× |
| `app/batch_16` | 226.18 ms | 4,421 | 20.33× |
| `app/batch_64` | 69.13 ms | 14,466 | 66.53× |
| `app/batch_256` | 35.75 ms | 27,973 | 128.65× |

## Interpretation limits

These are local observations, not production-capacity or cross-machine claims.
Batch run times are not individual-operation latency quantiles; no p95/p99 is
inferred. Setup, reopen verification, and fixture construction are excluded
where documented by the harness, while app parse+execute is included. Differences
in retained WAL, flush behavior, filesystem/cache state, and run-to-run system
load matter when interpreting the results. The smoke run is a functional release
check only and is not part of the three-run comparison tables.
