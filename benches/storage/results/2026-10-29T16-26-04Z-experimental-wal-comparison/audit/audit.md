# Artifact audit — matched WAL-as-storage benchmark

Status: **PASS** on every executed check (43 measured checks + 11 pilot checks, `audit_exit=0`).
This is a self-audit by the measuring agent. It is not an independent review and does not make the
result commit-ready.

## What was executed, in order

1. Provenance snapshot before any build: git HEAD, tracked status, tracked diff digest, and a
   61-path sorted SHA256 manifest over every production `src/**/*.rs`, every `tests/**/*.rs`, the
   harness, the contract doc, `Cargo.toml`, and `Cargo.lock`. `.envrc` and other untracked secrets
   were never opened; only paths and hashes of source files were read.
2. One release build: `rtk cargo build --release --example experimental_compare`, exit 0.
3. Source manifest re-hashed after build → identical.
4. Binary hashed, then pilot, then the same binary hash re-checked before the measured run.
5. Fresh dataset root created under `target/bench-data/`, canonicalized, mount resolved from
   `/proc/self/mountinfo` by the harness, and `findmnt -T` recorded before pilot, before measured,
   and after final. Paths identical, filesystem `btrfs` on `/dev/nvme0n1p3[/home]`, never `tmpfs`.
   The run root is claimed with an exclusive `.harness-run-owner` file and every sample directory is
   created exclusively; no pre-existing path was deleted or reused.
6. Release functional pilot (32 records, 4 KiB, batch 8): exit 0, all four `PILOT PASS` lines,
   `pilot/results.json` written.
7. Measured suite (`--profile small-tables --repetitions 3 --warmups 1`) on the same binary.
8. Source manifest re-hashed before measured and after final → identical to pre-build. Binary hash
   identical pre-pilot, pre-measured, post-final.
9. Raw JSON copied into `raw/` byte-identically (`cmp` clean) with its own sha256.
10. Independent recomputation over the raw JSON plus the on-disk sample/warmup directories.

## Independent recomputation (not trusting harness summary fields)

`audit/recompute.py` re-derives, from per-sample records only:

- schema string, `lifecycle_basis`, `git_head` against the provenance value, profile label,
  `repetitions == 3`, `warmups == 1`, seed `0x6A09E667F3BCC909`, mutation percentages 25/10/50;
- seven explicitly checked `small-tables` config fields against the documented profile;
- persistent-filesystem record (`fstype`, mount point, `fresh_run_root`, `existing_paths_deleted`);
- exactly 6 cases in the exact matrix `(64 B,16384,1) (64 B,16384,64) (4096,512,1) (4096,512,64)
  (65536,64,1) (65536,64,16)`, 6 samples each (3 reference + 3 experimental, indices 0/1/2,
  `warmup: false`), totalling 36 measured samples;
- 12 warmup executions by walking the on-disk `warmup-0/{reference,experimental}` directories;
- every closed census breakdown: `wal + sstable + metadata + other == total`, `file_count > 0`;
- workload identities: `live_logical_bytes == live_records*24 + live_value_bytes`,
  `mutations == append+overwrite+delete+reinsert`, and
  `batches == ceil(append/batch) + ceil(mutations_without_append/batch)`;
- every reported `space_amplification` value re-divided from `total_bytes / live key+value bytes`;
- every `lifecycle_phase_sum` re-added from its nine component timers;
- every summary median/min/max re-derived from the three sample values using the harness's
  `sorted[len/2]` convention;
- `clean()` identities: `live_records` and `live_logical_bytes` against the workload,
  `copied_payload_bytes == live_value_bytes`, `new_generation == old_generation + 1`,
  `elapsed >= pause > 0`, `deferred_cleanup_bytes == 0`, and
  `max(before, after) <= observed_peak_bytes <= before + after` (one coexistence snapshot, not a
  continuous maximum);
- cleaned roots hold exactly one generation directory, uncleaned experimental roots hold at least
  one; reference samples carry no clean and no postclean state;
- read operation counts: `point_gets == min(64, live_records)`, `point_hits == point_gets`,
  `scan_entries == min(128, live_records)`;
- FTS: role, `equivalent == true`, 128 documents, 128 documents re-read on the reference preclean
  and on the experimental preclean *and* postclean, exact Boolean AND/OR ID sequences, exact ranked
  AND/OR ID sequences and `f64::to_bits` score equality, non-empty result sets.

`raw/pilot-results.json` was audited separately: pilot schema, `pilot == true`, 32 records / 4096 B,
2 samples with the right engines, postclean state present only for the experimental sample,
FTS 16-document equivalence before and after clean, generation advance, and one remaining generation.

## Reported values re-checked by hand

- `observed_peak_bytes` equals `before + after - 49` in all 18 measured experimental samples across six cases, consistent with a single old+new coexistence sample.
- `copied_payload_bytes` equals the workload `live_value_bytes` exactly in all six cases
  (996,160 / 1,990,656 / 3,997,696).
- Cleaner transaction counts match the documented 256-record / 8 MiB bound:
  61 for 15,565 records, 2 for 486 records, 1 for 61 × 64 KiB records.
- The core cleaner's own `total_root_bytes_before` / `total_root_bytes_after` match the harness's
  closed preclean/postclean censuses byte-for-byte in every case (they cover the same root).
- Phase sums exclude verification and censuses by construction; `clean_pause` is not double-counted
  because only `clean_elapsed` enters the sum.

## Source-freeze and dataset checks

- `git diff --check` clean; four relevant untracked hashed inputs are the harness, engine, integration test and
  contract documentation; the filtered status also includes unrelated documentation; no trailing whitespace in untracked Rust source.
- No production, harness, or docs file was modified during the frozen run: the 61-path manifest is
  byte-identical across all four snapshots, and the binary hash never changed.
- Dataset retained at
  `target/bench-data/experimental-wal-2026-10-29T16-26-04Z` (189,182,153 B); nothing from it was
  copied into the archive and nothing was committed.
- Relevant unrelated dirty work (`README.md`, `src/**`, `tests/integration/mod.rs` and the rest of
  the pre-existing tracked modifications) was left untouched.

## Residual gaps, stated not concealed

1. **Measured-run exit status was not captured as a number.** The suite was launched detached with
   `nohup`, so its `$?` was not written. Success evidence: `measured.stderr` is 0 bytes, the final
   stdout line is `results=<path>` (printed only after the JSON write succeeds), the artifact parses
   and passes all structural checks, and the FTS line reports `equivalence=pass`. These support
   completed measurement; they do not prove normal process exit, whose status remains
   unverified. The pilot's exit status *was* captured (0).
2. Cleanup-failure paths were not exercised: `deferred_cleanup_bytes` is 0 everywhere and fault
   injection is `cfg(test)`-only.
3. Space is logical file size. `os_allocated_bytes` is reported but is not unique physical usage on
   a `compress=zstd:1` Btrfs mount.
4. Read timings are warm-cache, bounded (64 gets, one ≤128-entry scan), and serialize one call at a
   time; no percentiles or concurrency claims.
5. The harness's own `compaction_observed` flags are reported per case rather than assumed: the 64 B
   reference never compacted, and the experimental engine had no SSTable at all at 4 KiB/64 KiB
   preclean. No equal-flush or equal-compaction assumption is made anywhere.
