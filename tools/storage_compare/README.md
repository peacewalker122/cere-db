# Isolated three-engine storage comparison

This harness compares public write/read APIs for normal KV2 inline storage, `ExperimentalEngine` with WAL-backed values, and RocksDB. It is isolated under `tools/storage_compare`; it changes neither production dependencies nor production lockfiles. `src/main.rs` remains the historical two-engine harness. The new executable is `wal_values`.

## Build and run

The benchmark dataset stays under a caller-selected, retained path. Do not use `/tmp` for database stores. The dataset parent must already exist; the dataset itself must be absent or empty. The harness takes an exclusive lock, creates unique per-sample stores, and never deletes them. Existing contents, symlinked path ancestors, reused sample paths, existing output files, and unsupported filesystems are rejected.

Build with the already-cached toolchain and locked isolated manifest:

```sh
export PATH="/home/peacewalker/.cargo/bin:$PATH"
libclang="$PWD/target/rocksdb-compare-toolchain/libclang-18.1.1/libclang-18.1.1.data/platlib/clang/native"
export LIBCLANG_PATH="$libclang"
export LD_LIBRARY_PATH="$libclang${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export BINDGEN_EXTRA_CLANG_ARGS="-I/usr/lib/gcc/x86_64-redhat-linux/16/include"
export CARGO_TARGET_DIR="$PWD/target/rocksdb-compare"
rtk cargo build --manifest-path tools/storage_compare/Cargo.toml --release --locked --bin wal_values
```

Functional pilot, one 20-record case, 4 KiB values, batch size 1, one sample per engine, no warmup. It exercises append, mutations, close/reopen verification, warm reads, and experimental clean/reopen. Pilot timings are not a performance result.

```sh
DATA_DIR="$PWD/target/storage-compare-pilot-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir "$DATA_DIR"
findmnt -T "$DATA_DIR" -o TARGET,FSTYPE,SOURCE,OPTIONS
rtk cargo run --manifest-path tools/storage_compare/Cargo.toml --release --locked --bin wal_values -- --data-dir "$DATA_DIR" --pilot
findmnt -T "$DATA_DIR" -o TARGET,FSTYPE,SOURCE,OPTIONS
```

The full benchmark is gated on independent method review. Use a new dataset path and record `findmnt -T` against that exact path before and after the run:

```sh
DATA_DIR="$PWD/target/storage-compare-full-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir "$DATA_DIR"
findmnt -T "$DATA_DIR" -o TARGET,FSTYPE,SOURCE,OPTIONS
rtk cargo run --manifest-path tools/storage_compare/Cargo.toml --release --locked --bin wal_values -- --data-dir "$DATA_DIR"
findmnt -T "$DATA_DIR" -o TARGET,FSTYPE,SOURCE,OPTIONS
```

`--output PATH` is optional and must name an unused direct child of `--data-dir`; a relative basename resolves inside that directory. The file is created with exclusive-create semantics. Default is `report.json`. The tool independently records the canonical dataset path, resolved mount, filesystem, source, and options before and after the run. It refuses `tmpfs`, `ramfs`, `devtmpfs`, `overlay`, and `fuse.overlayfs`, and fails closed if `findmnt` cannot resolve the path. A changed mount source, target, filesystem, or normalized set of mount options aborts report creation.

## Workload

Default profile has eight explicit cases:

| Values | Initial records | Batch sizes |
| --- | ---: | --- |
| 256 B | 1,024 | 1, 16, 64, 256 |
| 4 KiB | 512 | 1, 64 |
| 64 KiB | 64 | 1, 16 |

Each engine/case has one discarded warmup and three measured executions, for 24 warmups and 72 measured executions. Every execution uses a fresh retained store. Engine order rotates through all three permutations by case and ordinal, so RocksDB is not systematically last.

Keys are deterministic 20-byte values. Payloads use a fixed-seed SplitMix64 stream and are built once per case before timing, then shared byte-for-byte across engines and samples. After appends, each case overwrites 25% of original keys, deletes the nearest even count to 10% in a disjoint key range, then reinserts exactly half of deleted IDs. Even rounding preserves an exact half; counts are reported as executed. The pilot uses 20 records so its ratios are exact: 5 overwrites, 2 deletes, 1 reinsertion.

All cases are checked against safe bounds: at most 1,024 records, 64 KiB values, batch size 256, and 32 MiB total logical append-plus-mutation input per case. Appends and mutations use the same ordered operations and batch boundaries on every engine. Batch size 1 uses public `put`/`delete`; larger sizes use public atomic batches. Input, expected maps, mutation plans, probes, and phase counts are prepared outside timers. Write timers include API-required copying/allocation and batch construction: CereDB clones owned vectors, whereas RocksDB receives borrowed slices and performs its own copying.

## Engine settings and measurement boundaries

Both CereDB engines use one `StorageConfig`: 1 MiB memtable threshold, 1 MiB WAL target, and normal production defaults for all other fields. The isolated lock pins `rocksdb` 0.24.0 and `librocksdb-sys` 0.17.3+10.4.2 (native RocksDB 10.4.2). RocksDB uses a 1 MiB write buffer, WAL enabled, `WriteOptions.sync=true`, and compression disabled. Equal byte targets do not imply equal memory accounting, flush triggers, compaction policy, or scheduling. CereDB uses public async APIs; RocksDB uses synchronous APIs on the benchmark thread.

For every execution, the harness separately times fresh open, append loop, mutation loop, barrier/drain, close, uncleaned reopen, warm point gets, and a bounded warm scan. CereDB barriers call `await_pending_writes`; RocksDB's barrier calls public `flush_wal(true)` after synchronous writes. The close timer starts immediately after the barrier and includes CereDB shutdown/background-worker join or RocksDB drop. RocksDB teardown cancels or unschedules pending work and waits for outstanding jobs; it does not mean all maintenance finished. No explicit memtable flush or manual compaction is done for natural KV2/RocksDB state.

After close and before reopen, the harness performs an untimed census of every file under the closed store root. It records total logical file bytes and root-relative WAL, SSTable, manifest, metadata, and other categories. `st_blocks` is informational only and does not represent unique physical usage on Btrfs. CereDB level counts come from the public manifest snapshot while closed; RocksDB level counts come from public `live_files()` after the timed reopen. Levels are observed, not forced to match.

The uncleaned reopen is timed separately. Outside timers, the harness checks the complete ordered scan against the exact expected map, up to 16 deterministic point gets, one deleted-key probe, and one never-present-key probe. It then times eight or fewer deterministic gets and one scan of up to eight entries, reporting actual calls and entries. No aggregate phase time is converted into p95 or another latency percentile.

Experimental storage additionally runs `clean()` and reports every `CleanStats` field, including total root bytes before/after, sampled coexist peak, write pause, full elapsed time, copied payload bytes, and deferred cleanup bytes. It then times the barrier/close, takes a closed post-clean census and manifest snapshot, times post-clean reopen, repeats exact verification, repeats the identical warm get and scan workload under separate post-clean timers, and times the final close. Natural and cleaned read counts, keys and ranges are identical. The cleaner's `observed_peak_bytes` is a point sample before `CURRENT` publication, not a continuous peak. It is not comparable to unmaintained RocksDB as an equivalent state.

`timings.lifecycle_sum_ns` is the sum of non-overlapping open, append, mutation, barrier, close, reopen, warm-read, cleaner, and extra close/reopen phase timers present for that engine. `CleanStats.write_pause_ns` is nested inside cleaner elapsed and is not added twice. The sum is not continuous wall time. Live key-plus-value bytes are the denominator for `root_bytes_per_live_logical_byte`; the ratio is `null` for an empty store while absolute census bytes remain available.

## JSON report schema

The report is pretty-printed JSON with `schema_version: 2` and these top-level fields:

- `mode`, `performance_result`, fixed `seed_hex`, `key_bytes`
- measured/warmup counts and expected execution totals
- `storage_config`, `rocksdb_config`
- `timing_boundaries`, `caveats`
- `mount_before`, `mount_after`
- `executions`, one row per engine/case/ordinal

Each execution row contains:

- `case`: case ID, record count, key/value byte sizes, batch size
- `store_path`: retained unique database path for audit
- `engine`, `execution_order`, `engine_position`, `warmup`, `sample_ordinal`
- `append` and `mutations`: actual operation, batch-call, logical-byte counts
- overwrite/delete/reinsert counts and final live records/logical bytes
- `timings`: per-stage nanoseconds and the explicit lifecycle sum; cleaner elapsed is `clean.elapsed_ns` and is counted once
- `natural_closed`: all-file census, live-byte denominator/ratio, actual level counts, evidence source
- `verification_uncleaned` and natural read counts in `warm_reads_natural`
- `timings.warm_get_uncleaned_ns` and `timings.warm_scan_uncleaned_ns`, counted once in the lifecycle sum
- Experimental-only `clean`, `post_clean_closed`, `verification_post_clean`, read counts in `warm_reads_post_clean`, and `timings.warm_get_post_clean_ns` / `timings.warm_scan_post_clean_ns`; non-applicable fields serialize as `null`. The two post-clean read phases also enter the lifecycle sum once.

The `performance_result` flag is false for pilot and true for the full matrix. The executable emits only a one-line report path/count summary; the JSON artifact is the result. Keep the dataset and report for audit. The full profile must not be run until the parent method review approves the frozen implementation.

## Interpretation limits

Experimental `WAL` reads validate the complete referenced transaction for each value, up to the engine's 256 MiB validation bound. Full exact scans are retained for correctness, while extra deterministic gets remain bounded; the harness does not add a redundant get-per-key pass. Cleaner validation remains inside cleaner time.

The census includes WAL, SSTables, manifests, logs, options, metadata, every generation, and any remaining files. A root-size ratio is a closed-state footprint, not lifetime write amplification. This harness makes no power-loss-safety, external-service parity, sustained-scalability, continuous-peak, or speedup claim. Historical runs with patterned payloads or different filesystems are context only, not matched baselines.
