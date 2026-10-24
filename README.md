# CereDB

CereDB is an experimental embedded key-value store written in Rust. Single-node mode persists data with a write-ahead log (WAL) and SSTables. Optional Raft mode adds multi-node replication. The project includes a command-line REPL for trying the store.

## Install and run

Install a Rust toolchain with Rust 2024 edition support, then run:

```bash
git clone https://github.com/peacewalker122/cere-db.git
cd cere-db
cargo run -- --data-dir ./data
```

Cargo builds the project and starts the REPL. The `./data` directory stores database files.

## Use

Enter commands at the prompt:

```text
SET greeting "hello world"
GET greeting
DELETE greeting
BATCH SET greeting "hello again" SET visits 1 DELETE obsolete
EXIT
```

`BATCH` accepts a sequence of `SET <key> <value>` and `DELETE <key>` operations
on one line. Quote keys or values containing whitespace. The full line is
validated before execution, then submitted as one engine batch; it is not
implemented as sequential CLI writes. KV2 applies batches atomically and
durably, preserving operation order (the final operation for a repeated key
wins). Backends without batch support, including the current Raft backend,
return an explicit unsupported-operation error rather than falling back to
individual writes.

Raft mode is available with `--raft-mode`. See `cargo run -- --help` for CLI options. Raft support is experimental and not production-hardened.

## Benchmark

Run the public storage-module and end-to-end benchmarks with:

```bash
cargo bench --bench storage -- --run
```

See [benchmark workloads and measurement boundaries](benches/storage/README.md)
for filtering, filesystem selection, checkpoint/recovery comparisons, and JSON
results.

## Tombstone reclamation

Compaction removes a winning delete record when its key falls outside every SSTable range excluded from that merge. Overlapping ranges retain the tombstone, even if the other table might not contain that exact key. Reclamation runs during the existing compaction process; a lone source file does not trigger compaction.

The output is synced before one checksummed manifest entry replaces all inputs together. A changed manifest aborts publication so concurrent flushes or compactions cannot invalidate the range check. Obsolete input files are deleted after publication. Interrupted cleanup can leave unregistered files; output creation skips occupied filenames.

This reclaims SSTable tombstones, not retained WAL history. The public database API has no retained historical snapshots. Manifest entries interrupted during append still fail closed on reopen; automatic manifest tail repair is not implemented. Databases written with the new compaction entry require a reader that supports that entry.
