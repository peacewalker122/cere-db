# Isolated RocksDB comparison — 2026-10-28T03-38-09Z

## Result

Review correction: the dataset path was `/tmp/ceredb-rocksdb-compare-work`,
which is on **tmpfs**. This archive measures public synchronous-WAL API overhead
on a memory-backed filesystem, not persistent-storage performance. The raw
capture is preserved unchanged; use a separately qualified persistent-filesystem
run for disk comparisons.

The harness built successfully after local runtime-loader/header-path adjustments and a harness-only type correction. All cases completed; JSON output is `measurements.json`. Each engine/batch has three measured samples and one warmup (24 measured samples total). Every sample wrote 1,024 mutations / 282,624 logical key+value bytes. All values were verified after write and after reopen outside the timer.

## Workload and pairing

Public CereDB `KV2` async API versus RocksDB `DB` synchronous local API; WAL enabled, sync writes, compression disabled; 20-byte keys, 256-byte values; batches 1 (individual put), 16, 64, 256 (atomic batch); 1 MiB configured CereDB memtable/flush threshold and RocksDB write buffer. Each measured result covers 1,024 key mutations. One warmup then three samples per engine/batch, alternating engine order. Timers start after DB open and stop after final write call; database setup and all verification are outside the timer.

CereDB's WAL path calls `sync_data` before acknowledging each public put or batch (one transaction/sync per call); RocksDB receives `WriteOptions::set_sync(true)` for every `put_opt` or `write_opt`. CereDB operations are awaited through Tokio; RocksDB calls are synchronous and block the benchmark thread. CereDB's post-commit maintenance hook runs after each call, so any triggered work remains in the timer. For this 282,624-byte logical input, the configured 1 MiB threshold was not reached and the harness issues no explicit flush. This is matching durability intent, not identical implementation or scheduling semantics.

## Results

| Batch size | CereDB median (ms) | RocksDB median (ms) | CereDB / RocksDB | Faster median |
|---:|---:|---:|---:|---|
| 1 | 24.141 | 1.899 | 12.714x | RocksDB |
| 16 | 2.779 | 0.591 | 4.699x | RocksDB |
| 64 | 1.652 | 0.503 | 3.285x | RocksDB |
| 256 | 1.426 | 0.485 | 2.941x | RocksDB |

Ratio is CereDB median elapsed time divided by RocksDB median elapsed time; below 1 means CereDB was faster. The small workload and very short runs make these host-specific observations, not a general performance claim.

## Build and machine caveats

Build duration is separate from measurement duration. Initial attempts exposed (1) the bundled libclang soname being unavailable through the dynamic loader, and (2) bindgen not inheriting GCC 16 internal header paths. Resolved without package installs by adding an ignored local soname symlink in the extracted wheel directory and setting `LD_LIBRARY_PATH` plus `BINDGEN_EXTRA_CLANG_ARGS=-I/usr/lib/gcc/x86_64-redhat-linux/16/include`. The isolated build then completed. Full attempt logs and commands are archived alongside this summary.

Host: Intel Core i5-1135G7, Fedora Linux x86_64, 8 logical CPUs. The datasets were on `/tmp` **tmpfs**; the recorded `/home` Btrfs mount does not describe this run's dataset storage, and its compression setting does not apply here. CPU scaling was reported at 59%. Background activity, cache state, maintenance scheduling and very short measured durations limit interpretation. No explicit cache drop was performed. RocksDB native version 10.4.2; Rust crate 0.24.0. Background maintenance is not isolated. `etcd` is not included because it is a replicated consensus service, not a raw local-LSM comparison. The generated values have only 251 distinct payloads; any subsequent compressed-filesystem run must account for their repeated content.

## Provenance

Build logs: `build*.stdout` / `build*.stderr`; wall times: `wall-times.txt`, result files and start files; run timestamps: `measurement-*.txt`; setup commands: `commands.txt`; host/mount: `environment.txt`; exact binary hash: `binary.sha256`; crate/native versions: `dependency-versions.txt`; isolated lockfile is `../../../../tools/storage_compare/Cargo.lock` in the checkout (its sha256 appears in source hash files); libclang wheel hash: `libclang-wheel.sha256`. `source-files-before.sha256` was captured before the build; complete core Rust module hashes are reconstructed from HEAD in `core-rust-source-hashes-before-build-from-head.sha256`, after confirming all tracked library source except the unrelated, pre-existing dirty `src/main.rs` was unchanged. Harness/source hashes before and after measurement match. The archived pre-build source diff/status records preserve the original `src/main.rs` change and parent `changelog.md` draft untouched.
