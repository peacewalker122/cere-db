# Checkpoint-based WAL reclamation release smoke

This is a fresh, one-sample correctness-oriented smoke run, not a performance
claim or a full baseline. It ran after the reclamation fixes and is independent
of the earlier `2026-10-25T08-11-20Z-wal-reclamation` archive, which was left
untouched.

## Command and outcome

Exact command:

```sh
/home/peacewalker/.cargo/bin/cargo bench --bench storage -- --run --suite all --records 32 --value-bytes 256 --repetitions 1 --warmup-runs 0 --flush-bytes 512 --wal-segment-bytes 1 --data-dir target/wal-reclamation-final-data --output benches/storage/results/2026-10-25T08-31-34Z-wal-reclamation-final/raw.json
```

Cargo's `bench` profile is optimized (`release`); Cargo 1.97.1 does not accept
`--release` on `cargo bench`. The run completed successfully with 90 distinct
scenarios and one sample each (90 raw samples total). The new
`e2e/wal_checkpoint_reclamation` scenario also completed its reopen and value
checks: 32 primary records across four groups of eight, then two new writes
followed by a reopen and validation, then another write in that reopened engine.
It validated existing values and later writes after reopen. Its raw scenario
timing is lifecycle-only smoke data, not a performance result.

Observed public-API checkpoint lifecycle: before automatic checkpoints, the
scenario observed 411 WAL bytes and the current segment ID; after checkpoint
reclamation it observed 36 bytes in the active segment. IDs advanced
monotonically through 0, 2, 4, …, 32 and were not recycled. Across all four
group reopens, the WAL remained at one active 36-byte segment. These are the
reported on-disk observations for this small fixture, not a general bound on
WAL size; retained history depends on checkpoint progress.

## Source qualification

Git HEAD was `cb0b62420c5b519f7cca25472a603ab2a24db2b0`. The benchmark used HEAD
plus the dirty worktree. This capture predates the latest durability hardening;
its source hashes below qualify the captured run, not the final committed source,
and its measurements are not claims about the latest code. The checkout also had
unrelated dirty batches, TODO, and scaleDB/archive changes, which were not
edited by this task. Relevant source SHA-256 hashes at capture:

| File | SHA-256 |
| --- | --- |
| `benches/storage/e2e.rs` | `3e575fa928561416c9ed9c7f850fc464afe4edc2f744a8dc62f1afc8f8280875` |
| `src/storage/kv2.rs` | `0e53a32903ec1fad9c65023fa7eaaad51dcecbbd9fa3d7d64f9d198123bcb9fe` |
| `src/storage/manifest_codec.rs` | `e98611665ae0fe8d27500ec478e67c5b378767b182a645517c9d04d0dfa2cd16` |
| `src/storage/recovermanager/wal.rs` | `9fba4724a549872241948815ad4fa1f5b32bc198dff93ed46484ee638b33f0c8` |
| `src/storage/writemanager/block.rs` | `687a926a882262b47b708289dd610f19c0c5361e846b5729ad32ce75622a6214` |
| `src/storage/writemanager/write.rs` | `7a58903a1aae733f3e24330064b17fc301ebf470bca418361c1f73007d1763db` |
| `tests/integration/wal_reclamation.rs` | `013f63cbc5bc5e783980398b9a42828950b196ed7986164c0ce80c6edc68c7c9` |
| `tests/integration/wal_recovery.rs` | `8175f93c56d6980a7e0e42809d181ca581b7467a9cbbf71095d30b4a55194d13` |

The machine was Linux x86_64 on Btrfs, with an 11th Gen Intel Core i5-1135G7
and 8 available logical CPUs. Rust was `rustc 1.97.1 (8bab26f4f 2026-07-14)`.
The release build emitted existing compiler warnings (37 library warnings,
including unused imports/variables and async public-trait lint warnings); no
benchmark scenario failed. See `run.log` for full compiler and scenario output
and `raw.json` for captured metadata and measurements.

## Regression tests run separately

After the release smoke, `/home/peacewalker/.cargo/bin/cargo test --lib`
passed 215 tests with 1 ignored, and
`/home/peacewalker/.cargo/bin/cargo test --test integration` passed all 45
integration tests. Together these are 260 passed and 1 ignored. These test
results are separate from the 90 benchmark scenario samples.

## Limits

- One sample is a smoke check, not a stable timing estimate or comparison.
- OS page cache was retained; this was not a cold-cache or process-crash test.
- `--wal-segment-bytes 1` was passed as requested; the application validates
  segment sizes with a 256 KiB minimum in normal storage configuration, so this
  does not mean a one-byte physical segment size.
- The smoke fixture cannot establish a universal WAL byte/file bound or behavior
  under stalled checkpoint progress. The implementation is progress-based, not
  fixed-capacity ring reuse.
- This benchmark did not add or run a separate oversized scenario. The separate
  integration suite did run and pass
  `large_batch_records_survive_checkpoint_and_reopen` (850 records with 5,000-byte
  values) from `tests/integration/wal_reclamation.rs`.
- This benchmark is not the full Rust test suite; regression test results are
  reported separately above. No additional large-dataset engine run or external
  baseline comparison was performed.
