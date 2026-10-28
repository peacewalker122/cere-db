# Persistent-filesystem RocksDB comparison — 2026-10-28T03-59-08Z

## Storage and run provenance

The exact benchmark work directory was `/home/peacewalker/project/learn/wasm-kv/target/rocksdb-compare-persistent-2026-10-28T03-59-08Z`. `findmnt -T` before and after the run reported `/home` on `/dev/nvme0n1p3[/home]`, filesystem `btrfs`, options `rw,relatime,seclabel,compress=zstd:1,ssd,discard=async,space_cache=v2,subvolid=257,subvol=/home`. It was not tmpfs, ramfs, or overlay. Mount evidence is saved in `mount-before.txt`, `mount-pre-run.txt`, and `mount-after.txt`.

Run interval UTC: `2026-10-28T03:59:22,442113208+00:00` to `2026-10-28T03:59:51,393101571+00:00`; process exit `0`.

## Results

| Batch | CereDB median (ms) [range] | RocksDB median (ms) [range] | CereDB / RocksDB | Lower median |
|---:|---:|---:|---:|---|
| 1 | 3138.720 [3123.046–3182.603] | 3036.882 [2977.730–3071.907] | 1.034× | RocksDB |
| 16 | 195.973 [195.274–196.681] | 196.613 [189.269–198.458] | 0.997× | CereDB |
| 64 | 50.806 [49.911–103.769] | 53.137 [46.778–53.591] | 0.956× | CereDB |
| 256 | 27.960 [27.503–28.083] | 25.758 [25.525–26.013] | 1.085× | RocksDB |

Three measured samples and one warmup for every engine/batch pair; 24 measured samples total and eight warmups. Each sample executed 1,024 unique keys (20 bytes) with 256-byte values, or 282,624 logical key+value bytes. WAL was enabled and sync writes requested on both. Each database was freshly created for every warmup/sample pair, with engine execution order alternating by pair; key insertion order was identical. All values were checked after writes and again after reopening; checks and store setup are outside the timed interval. The write API calls, cloning of owned CereDB inputs, Tokio scheduling/await, and any in-call maintenance are part of the CereDB timed public API path. RocksDB uses blocking synchronous calls. This compares public APIs and durability behavior, not internal LSM mechanics. No power-loss test was performed.

## Interpretation limits

This is a small local observation, not a general disk benchmark. Values use only 251 distinct 256-byte payload patterns across 1,024 records, so payload repetitiveness plus Btrfs `compress=zstd:1` can make the physical I/O/compression behavior unlike random or high-entropy data. Total logical input (282,624 bytes) is below the configured 1 MiB flush threshold; this does not represent sustained writes or steady-state compaction. OS cache remained enabled; CPU frequency was not controlled. Timing results therefore include host scheduling, cache and maintenance variance. The harness behavior/source was not modified for this run; exact snapshots and binary/source hashes are preserved here. The earlier `/tmp` tmpfs run remains unchanged and is not a disk baseline; differences between it and this run reflect a change in storage environment as well as run variability, not an engine-only optimization.

## Checks

- JSON has 24 measured entries, 3 per each of 8 engine/batch combinations; all entries report 1,024 mutations and 282,624 logical bytes.
- Harness declares 1 warmup and performs 1 warmup + 3 measured iterations for each of 8 engine/batch combinations.
- Deterministic value generator yields 251 distinct 256-byte payload patterns across 1,024 keys.
- SHA-256 hashes of executable, harness source/manifest/lockfile, and production Cargo.lock match from pre-run to post-run (`hashes-before.sha256`, `hashes-after.sha256`).
- Exact command, captured harness and both lockfiles, host environment, source state, logs, raw JSON and mount evidence are archived alongside this summary.
