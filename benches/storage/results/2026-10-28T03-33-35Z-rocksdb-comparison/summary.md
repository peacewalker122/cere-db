# RocksDB comparison attempt — 2026-10-28

## Outcome

No measurements were produced. The isolated benchmark build was attempted once
with two Cargo jobs and the existing Rust toolchain. Compilation stopped at
`clang-sys 1.9.1`: neither `llvm-config` nor any `libclang.so` was installed or
found in `/usr/lib64`, `/usr/local/lib`, or `/nix/store`. The RocksDB Rust crate
uses bindgen at build time; installing system packages or a compiler toolchain
was outside scope. No fallback competitor or synthetic metrics were used.

## Pinned source/build

- RocksDB Rust crate: `rocksdb 0.24.0`
- Bundled native RocksDB: `10.4.2` (`librocksdb-sys 0.17.3+10.4.2`)
- Isolated lockfile: `isolated-Cargo.lock`
- Full build stdout/stderr: `build-and-run.stdout`, `build-and-run.stderr`
- Command: `commands.txt`
- Source HEAD before build: `source-head.txt`; file hashes before and after are saved.
- Workload intended: 1,024 records; 20-byte keys; 256-byte values; 1 MiB configured
  flush/write buffer; WAL enabled; sync writes; compression disabled; batch sizes
  1, 16, 64, 256; one warmup plus three samples per pair, alternating order.

## Host and provenance

See `environment.txt`, `git-status-before.txt`, `source-diff-before.patch`, and
`source-files-{before,after}.sha256`. This host is an Intel Core i5-1135G7,
Fedora Linux x86_64, eight online logical CPUs. The checkout already contained
unrelated dirty/untracked changes, including `src/main.rs`; none were modified
by this benchmark. The parent's untracked `changelog.md` draft was preserved.

## Follow-up

Install/provide a matching LLVM/Clang development toolchain with libclang, then
rerun using the documented isolated manifest. Before presenting any resulting
numbers, inspect the run archive and have the parent independently review them.
