# Local baseline

Measured at 2026-10-24T02:59:23+00:00 with source revision `ee4a093331061da1f3eaf9d561407d785582aa11` and the uncommitted benchmark harness.

Machine: 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz; linux x86_64; `rustc 1.97.1 (8bab26f4f 2026-07-14)`. Dataset filesystem: `/dev/nvme0n1p3[/home] btrfs /home`. The Tokio runtime uses two workers. Other machine load and the OS page cache were uncontrolled.

Command:

```sh
cargo bench --bench storage -- --run --suite all \
  --records 256 --value-bytes 256 --repetitions 3 --warmup-runs 1 \
  --flush-bytes 16384 --output target/benchmarks/baseline.json
```

All 35 scenarios passed output verification, producing 105 measured samples. Keys are 20 bytes; each dataset has 256 records and 256-byte values. Each scenario ran once for warmup and three times for measurement. The JSON report contains raw samples and every summary.

Selected results:

- `wal/durable_append`: median run 754.386 ms; aggregate 339.4 public calls/s.
- `writer/flush`: median run 18.556 ms; aggregate 54.1 public calls/s.
- `compaction/merge_l0`: median run 15.590 ms; aggregate 48.9 public calls/s.
- `e2e/put_sequential_durable`: median run 770.469 ms; aggregate 237.3 public calls/s.
- `e2e/put_automatic_checkpoint`: median run 857.616 ms; aggregate 224.8 public calls/s.
- `e2e/reopen_before_flush`: median run 0.593 ms; aggregate 1744.9 public calls/s.
- `e2e/reopen_after_flush`: median run 0.601 ms; aggregate 1659.4 public calls/s.

For writes, a run contains 256 calls. A flush, compaction, or reopen run contains one call over its dataset. Aggregate throughput uses total calls divided by total measured seconds; it is not derived from the median run.

- First sample after `e2e/put_sequential_durable`: WAL 96,036 bytes; SSTables 0 bytes; manifest 0 bytes.
- First sample after `e2e/put_automatic_checkpoint`: WAL 96,180 bytes; SSTables 73,664 bytes; manifest 932 bytes.

The checkpoint workload added SSTables while retaining WAL history. At this small size, before/after-checkpoint reopen timings were similar. This does not establish scaling behavior or checkpoint-based recovery acceleration.

These are local baseline observations, not production capacity claims. For CPU microbenchmarks use more records and a focused filter; the small run durations here are sensitive to timer, allocation, and scheduler noise.

Validation: full serial test suite passed 183 tests with 1 ignored; scoped formatting and diff checks passed. CLI selection, invalid zero record counts, unmatched filters, and the no-run test invocation were checked separately.
