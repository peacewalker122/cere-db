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

## Full-text search

Single-node CLI mode also supports explicit document indexing:

```text
DOC PUT article-1 "Rust stores text as UTF-8 bytes"
DOC GET article-1
FTS AND "rust text"
FTS OR "utf-8 bytes"
FTS RANK AND "rust text"
FTS RANK OR "utf-8 bytes"
DOC DELETE article-1
```

Document IDs are non-empty. Text is tokenized into Unicode alphanumeric terms,
lowercased, and deduplicated; Boolean search uses exact AND or OR term matching,
orders results by document ID, and returns at most 100 documents per command.
Ranked search uses the same query normalization and AND/OR matching, with
positive BM25 IDF and defaults `k1=1.2`, `b=0.75`. It materializes and scores
all matching candidates, sorts by descending score with ascending document ID
ties, then applies the 100-result CLI limit; it does not use WAND or top-k
pruning and makes no performance promises. Documents must contain valid UTF-8
for indexing. `SET`, `GET`, `DELETE`, and `BATCH` remain
raw key-value operations and do not update the search index automatically.

The search index and documents persist in a reserved, versioned key namespace
inside the same database and are updated atomically by document commands. Do
not use that reserved namespace for raw keys. The index is managed by one open
`FtsIndex` at a time; separate independently opened writers to the same database
are not coordinated. The current CLI's Raft backend supports raw key-value
commands but reports document and search commands as unsupported.

`AsyncKVEngine::await_pending_writes` is the completion barrier used by FTS
before loading or querying persisted index state. Implementations must wait for
all writes admitted before the barrier to finish publishing their running
state, or return an error when they cannot guarantee that. The default is
unsupported, so custom engines used with `FtsIndex` must implement the barrier;
it does not prevent writes admitted after the barrier from racing.

This is exact-term retrieval, not phrase search or a full GIN-compatible
implementation: it has no stemming, stop-word processing, or compatibility
with PostgreSQL GIN. Corpus statistics are updated in the same atomic batch as
document/index records. The FTS format change requires rebuilding existing
indexes before opening them; there is no automatic migration or data deletion.
The library API allows search limits up to 1000; posting scans and ranked
candidate materialization are not bounded by the result limit.

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

Checkpointing also authorizes WAL reclamation in KV2. After the SSTable and its directory are synced and registered in the manifest, the engine rotates the active WAL and records a trusted recovery checkpoint. On a later open, KV2 validates the complete contents of registered SSTables and every WAL segment at or after the checkpoint's replay floor before removing covered earlier segments. SSTable preflight currently reads each complete table into memory, so startup validation can temporarily require memory proportional to the largest table. Missing required files or invalid contents fail closed; failures deleting covered files are logged and retried at startup. A public flush or a foreign `WriteComponent` does not receive checkpoint ownership merely by writing or publishing a table. Only the engine's trusted checkpoint path can grant cleanup authority. Legacy manifest checkpoint metadata remains informational and does not grant deletion authority.

The checkpoint's committed sequence is an application/MVCC sequence, while WAL record LSNs and segment IDs are physical recovery positions; they are not interchangeable. WAL space is bounded by checkpoint progress and the active/post-checkpoint history, not by a fixed-size ring. Segments are deleted only after they are covered; identifiers are not reused. Standalone WAL managers and the public raw WAL append API do not acquire KV2 checkpoint ownership, so rotating or appending through those APIs alone cannot authorize reclamation. The public `KV2::wal_manager()` handle originates from the engine-owned manager; the standalone `WALManager::new` constructor does not carry that authority.

This reclamation is separate from SSTable tombstone compaction. The public database API has no retained historical snapshots. Manifest entries interrupted during append still fail closed on reopen; automatic manifest tail repair is not implemented. Databases written with the new compaction entry require a reader that supports that entry.
