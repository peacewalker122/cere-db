# Isolated local RocksDB comparison

This tool is deliberately outside the production workspace manifest/lockfile.
It compares the current public `KV2` / `AsyncKVEngine` write path with
RocksDB's synchronous local `DB` API. Both engines receive deterministic
20-byte keys and 256-byte values in the same insertion order, with WAL enabled
and synchronous durability. KV2 timings include its normal Tokio async API
overhead; RocksDB's synchronous calls run directly on the benchmark's calling
thread. It is a public-API workload comparison, not an internal LSM comparison.

Run using the isolated manifest and target directory. Pass a disposable
work directory outside the results archive; the tool creates and removes a
fresh database directory there for every engine/sample pair:

```sh
PATH="$HOME/.cargo/bin:$PATH" CARGO_TARGET_DIR="$PWD/target/rocksdb-compare" \
  cargo run --manifest-path tools/storage_compare/Cargo.toml --release --locked -- \
  /tmp/ceredb-rocksdb-compare-work
```

If bindgen cannot locate libclang, set `LIBCLANG_PATH` to the directory
containing `libclang.so`; if the shared library's SONAME is not resolved by the
loader, add that directory to `LD_LIBRARY_PATH` as well. Some Linux toolchains
also need their C compiler's builtin headers passed to bindgen, for example
`BINDGEN_EXTRA_CLANG_ARGS="-I$(gcc -print-file-name=include)"`. These are
build-environment settings only; the isolated lockfile pins Rust dependencies
and no production dependency or system package is added by the benchmark.

The executable uses fresh stores, one warmup and three measured samples per
engine and batch size, alternating engine order. Put/open and correctness checks
before and after reopen are outside the write timer. Write timing excludes
database creation/open but includes the public put/batch calls and their normal
WAL durability behavior. Batch sizes 16/64/256 use
the public atomic batch methods; size 1 uses individual puts. It verifies every
key/value before and after reopening. Reports count mutations and logical bytes,
not batch calls; it does not derive request latency percentiles from batch time.
Only a small 1,024-record profile is implemented, so it is a bounded local
comparison, not a scale benchmark.

KV2's flush trigger floor and RocksDB's write-buffer size are both configured
at 1 MiB, though this does not equate their internal memory accounting or flush
and compaction scheduling. Background maintenance costs are not isolated from
the timed API calls. RocksDB compression is disabled. Record the filesystem
and its mount-level compression; this harness does not control Btrfs settings.
etcd is not a raw local LSM competitor: meaningful comparison requires a
separate matched KV2/Raft and multi-member etcd service experiment, and current
Raft apply semantics must be validated before making an equivalence claim.
