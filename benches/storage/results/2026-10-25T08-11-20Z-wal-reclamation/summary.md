# WAL checkpoint reclamation smoke

This is a correctness-oriented smoke, not a performance baseline. It ran the
release storage benchmark's full `all` suite once with 32 records, 256-byte
values, a 512-byte automatic flush threshold, a 512-byte WAL segment target,
and no warmup. The existing benchmark's complete configured suite emitted 90
scenarios, including FTS and BM25 app/E2E scenarios; all completed successfully.

The new `e2e/wal_checkpoint_reclamation` scenario used public `KV2` writes and
real temporary directories. It measured `.log` filenames/segment IDs and bytes
from the filesystem before and after automatic checkpointing, reopened four
times at eight-record intervals, and checked all previously written values on
each reopen. Each of the 16 automatic checkpoints observed one segment with 411
bytes immediately before checkpoint and one segment with 36 bytes afterward.
The file ID advanced by two each time (0→2, 2→4, ... 30→32); deleted IDs were
not reused. At the four group-boundary reopen observations, the WAL remained at
one segment and 36 bytes. The scenario then verified writes both before and
after another reopen. The observed footprint is this workload's current active
segment only; it does not establish a fixed capacity or guarantee for stalled
checkpoint progress.

The overall single-sample scenario recorded 35 operations, 8,832 configured
logical bytes, and 744,053,554 ns. These numbers are incidental smoke output,
not performance claims: fixture/setup/recovery/verification are mixed into the
scenario lifecycle and one sample is not a performance study. Full suite wall
time including setup was 29.92 seconds.

## Reproduction and source qualification

See `command.txt` for the exact successful command and `raw.json` for the
machine/configuration metadata and all scenario output. Source HEAD was
`cb0b62420c5b519f7cca25472a603ab2a24db2b0`; the worktree was dirty, so this
HEAD alone does not identify the benchmark build. At run time, the benchmark
source/docs and the independent core checkpoint implementation/tests were
dirty. `source.txt` records the source qualification. No tests outside this
benchmark suite are claimed as rerun here; the previously reported core test
context belongs to the parent task.

The scenario uses filesystem-visible counts and logical file lengths through
publicly observable paths; it does not import private WAL internals or parse
the WAL format. Its database lives in a fresh benchmark `TempDir`, removed by
the existing harness after the run. Existing result archives and datasets were
not modified.
