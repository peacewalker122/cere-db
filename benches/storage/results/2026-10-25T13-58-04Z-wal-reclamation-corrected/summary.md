# WAL checkpoint reclamation, corrected accounting

## Configuration and result

- Public `KV2` E2E scenario, 128 records, 20-byte generated keys, 256-byte values.
- Requested flush threshold and WAL segment target: 512 bytes each. One warmup and three measured samples, exact focused filter.
- All three samples recorded exactly 128 timed puts and 35,328 logical bytes. Elapsed times: 7,831.757 ms, 7,746.800 ms, and 7,682.863 ms. Median: 7,746.800 ms; range: 7,682.863–7,831.757 ms.
- Each sample observed 64 checkpoints with 411-byte pre-checkpoint and 36-byte post-checkpoint WAL segments, and IDs advancing by two through 128. One active segment of 36 bytes was observed at each of the 16 reopen boundaries. These are filesystem observations, not harness assertions proving garbage collection.

## Timing boundary and limits

Timing begins after fresh engine open and includes the 128 puts, per-put filesystem size and filename scans, group readback verification, 16 closes/reopens, and lifecycle logging. It stops after reopening and verifying the final eight-record group. The further two writes, their validation/reopen, and the last write are outside the timer and are not counted in operations or logical bytes. Thus these measurements describe the instrumented lifecycle, not raw write latency, request percentiles, cold-cache behavior, crash recovery, or production throughput. The OS page cache is retained. The focused timing is not comparable to prior full-suite/small-record smoke runs as a speed baseline.

## Provenance

See `command.txt`, `raw.json`, `run.log`, and `source.txt`. The archive preserves the raw output of a fresh run using the same configuration as the earlier 128-record capture, with a unique data directory. It used HEAD `45f68b8` plus the dirty worktree; no production files were edited for this correction.
