# WAL checkpoint reclamation focused run

This run exercised the existing `e2e/wal_checkpoint_reclamation` benchmark through public `KV2` calls. The harness and tracked source files were not edited. It is a correctness-backed lifecycle timing observation, not an isolated write-throughput benchmark and not a before/after speedup claim.

## Configuration and outcome

- 128 records, each with a 20-byte generated key and a 256-byte value.
- Requested automatic flush threshold: 512 bytes. Requested WAL segment target: 512 bytes, passed through the public `StorageConfig` path without a harness-side adjustment.
- One warmup, three measured runs, focused `e2e` filter only.
- Every measured run completed; the timed workload executed 128 timed puts totaling 35,328 logical bytes; the archived report incorrectly recorded 131 operations. Timing was 7,744.406 ms, 7,703.148 ms, and 7,742.797 ms. Median: 7,742.797 ms (range 7,703.148–7,744.406 ms). The elapsed times are unchanged; the previous derived operations/second and amortized microseconds/operation values are invalid because the archived raw report counted 131 operations although the timer excluded three writes.
- Each run observed 64 automatic checkpoints. For each checkpoint, the preceding segment was 411 bytes and the post-checkpoint active segment was 36 bytes. The segment ID advanced monotonically by two, from 0 through 128, without reuse. The workload observed one active WAL segment after each checkpoint and one 36-byte segment at all 16 eight-record reopen boundaries. These are filesystem observations, not harness assertions that establish garbage collection.
- The observed per-checkpoint file-length difference is 375 bytes (about 91.2% of the pre-checkpoint segment length). This describes only the file(s) measured around these checkpoints, not total storage reclaimed, a guaranteed WAL cap, or behavior under stalled checkpoint progress.
- Values are read and validated before and after each eight-record reopen boundary. Two more writes are then verified through another reopen, followed by a final write on the reopened engine.

## Timing boundary and limitations

The timer starts after opening a fresh engine. It includes all 128 public puts, a filesystem byte/count and WAL-filename scan immediately before and after every put, per-group readback verification, 16 closes/reopens, and lifecycle logging before and after each reopen. The timer stops after the sixteenth group has been reopened and verified. The final two writes, their readback, the subsequent reopen and validation, and one final write occur after the timer. Setup and the one warmup are not included in per-sample elapsed time. Thus the timing is for this intentionally instrumented checkpoint/reopen lifecycle, not raw KV2 write cost, cold-cache behavior, crash recovery, or production throughput. It includes durable write/checkpoint work but does not separate sync, checkpoint, reopen, or verification costs. No request p95/p99 can be derived from these whole-workload durations.

The run retains the OS page cache and does not model process-crash recovery or cold-cache open. Its fresh temp databases are automatically removed by the harness, so persisted before/after WAL sizes are captured in `run.log`, rather than left as database directories. `raw.json` has no disk samples because this scenario does not attach them to its `Measurement` object; the per-checkpoint filesystem observations are in `run.log`.

## Review of prior WAL attempts

The archived 08:11:20Z and 08:31:34Z attempts each used 32 records, 256-byte values, a 512-byte flush threshold, one run, no warmup, and the same lifecycle timing boundary. Both successfully crossed the threshold: they reported sixteen checkpoints per run, 411-to-36-byte segment observations, and monotonic IDs through 32. The smaller attempt is not a failed checkpoint-precondition run. The previously noted 438/522-byte threshold failure belonged to `app/sequential_set_checkpoint`, with `key` and `value-N` payloads, not the FTS text-documents fixture.

Prior WAL artifacts are correctness-smoke results, not speed baselines. The 08:11:20Z attempt used `--suite all`, 512-byte target segment setting, and 90 scenarios; the 08:31:34Z attempt also used `--suite all` and 90 scenarios, requesting a segment target of 1. Configuration is passed to `KV2::open` unchanged through the benchmark path; no claim is made here about an implementation minimum. The current run uses only the WAL scenario and has 128 records, so its total timings cannot be compared as a speedup. Config/source fingerprints differ, too. The prior 32-record numbers (744.054 ms and 694.862 ms) are retained as qualified historical observations only.

Earlier non-WAL harness issues are out of scope here: filter matching is exact when the filter contains `/`, while unqualified filters use substring matching; the archived FTS post-flush fixture requires enough payload to create a registered SSTable. This run uses the exact fully-qualified filter and verifies actual checkpoint effects via the observed WAL segment files and bytes, not just SSTable/file counts.

The app/text workload's parse boundary is not relevant: this scenario calls the public raw KV2 API directly, with no app text payload parsing. The per-put filesystem instrumentation does add work around KV2, so these measurements should not be interpreted as production ingestion rates. Checkpointing is synchronous in this lifecycle path; the measured durations aggregate its effects with writes, checks, metadata scans, and reopens, and do not establish which component dominates.

## Reproduction and evidence

See `command.txt` for the command, `raw.json` for machine/configuration metadata and sample durations, `run.log` for every checkpoint and reopen's file IDs/byte counts, and `source.txt` for HEAD, dirty-worktree qualification, environment, and relevant source fingerprints. The raw report's `--output` argument retains the pre-rename archive path; its contents are unchanged. The historical raw data and dataset were not modified; this summary was corrected to qualify the operation-count mismatch and historical fixture attribution.
