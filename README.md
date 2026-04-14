# wasm-kv

[![Rust](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml/badge.svg)](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml)

An LSM-tree based key-value store built from scratch in Rust. Built as a learning project to deeply understand how storage engines work — from WAL to compaction — with a future path toward WASM compatibility.

## Overview

`wasm-kv` is a single-node, single-writer embedded key-value store that implements the core components of a Log-Structured Merge Tree (LSM-tree):

- **Write path**: Writes go through a Write-Ahead Log for durability, then into an in-memory SkipMap, which flushes to sorted on-disk SSTables when a size threshold is reached.
- **Read path**: Reads check the in-memory MemTable first, then search through leveled SSTables using bloom filters and sparse indexes to minimize disk I/O.

## Architecture

```
                          WRITE PATH
                          ─────────
    put(key, value)
          │
          ▼
    ┌───────────┐     Append record with
    │    WAL    │◄─── CRC32 checksum for
    │ (durability)│    crash recovery
    └─────┬─────┘
          ▼
    ┌───────────┐     crossbeam SkipMap
    │ MemTable  │◄─── (sorted, concurrent)
    └─────┬─────┘
          │  threshold reached (400KB)
          ▼
    ┌───────────┐     Flush via crossbeam
    │  Flush    │◄─── channel to background
    │ (async)   │     watcher thread
    └─────┬─────┘
          ▼
    ┌───────────────────────────────────┐
    │           SSTable File            │
    │  ┌───────┬───────┬───────┐       │
    │  │Block 0│Block 1│Block N│ 4KB   │
    │  └───────┴───────┴───────┘       │
    │  ┌─────────────────────┐         │
    │  │    Sparse Index     │         │
    │  └─────────────────────┘         │
    │  ┌─────────────────────┐         │
    │  │    Bloom Filter     │         │
    │  └─────────────────────┘         │
    │  ┌─────────────────────┐         │
    │  │      Footer         │         │
    │  └─────────────────────┘         │
    └───────────────────────────────────┘
          │
          ▼
    ┌───────────┐     Tracks SSTable files
    │ Manifest  │◄─── per level
    └───────────┘


                          READ PATH
                          ─────────
    get(key)
      │
      ▼
    MemTable ──found──▶ return value
      │ miss
      ▼
    Bloom Filter ──negative──▶ skip file
      │ maybe
      ▼
    Sparse Index ──locate──▶ target block
      │
      ▼
    Block scan ──found──▶ return value
      │ miss
      ▼
    Next SSTable... ──▶ repeat per level
```

## Features

- **Write-Ahead Log (WAL)** — Append-only log with CRC32 checksums, WAL rotation on flush, and archival for recovery
- **MemTable** — Lock-free concurrent SkipMap (`crossbeam-skiplist`) with sorted key ordering
- **SSTable** — Immutable on-disk sorted tables with:
  - Fixed-size 4KB blocks
  - Sparse index for block-level binary search
  - Bloom filters for fast negative lookups
  - Footer with offset metadata and checksums
- **Leveled Storage** — Multi-level SSTable organization (L0, L1, L2...) with manifest tracking
- **Concurrent Flush** — Channel-based background flush watcher thread decoupled from the write path
- **Tombstone Deletes** — Logical deletes via `RecordType::Delete` markers, cleaned up during compaction
- **Crash Recovery** — WAL replay on startup to rebuild MemTable state
- **CLI REPL** — Interactive command loop with `SET`, `GET`, `DELETE`, `EXIT/QUIT`
- **Command Core Abstraction** — Transport-agnostic command parse/execute layer reusable for future TCP transport
- **CLI** — Configurable via `clap` with log level and data directory options

## Project Structure

```
src/
├── main.rs                 # CLI entry point
├── lib.rs                  # Public crate API
├── command.rs              # Transport-agnostic command parser/executor
├── config.rs               # CLI config (clap)
├── error.rs                # Error types (thiserror)
├── api/
│   ├── mod.rs
│   └── api.rs              # KVEngine trait definition
└── storage/
    ├── mod.rs
    ├── kv.rs               # PersistentKV — core engine implementation
    ├── wal.rs               # WAL header, record encoding/decoding
    ├── log.rs               # Record format, WAL I/O, SSTable search
    ├── record.rs            # RecordType (Put/Delete)
    ├── block.rs             # BlockBuilder — 4KB block encoding
    ├── bloom.rs             # Bloom filter implementation
    ├── sstable.rs           # SSTable encode/decode, flush, k-way merge
    ├── manifest.rs          # Manifest file tracking (level → files)
    ├── levelstore.rs        # Level store data structure
    ├── constant.rs          # Thresholds and magic numbers
    ├── signal.rs            # FlushSignal struct
    ├── skiplist.rs          # SkipList utilities
    └── watcher.rs           # Background flush watcher thread
```

## Getting Started

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (edition 2024)

### Build

```bash
cargo build
```

### Run

```bash
# Default (info logging)
cargo run

# With debug logging
cargo run -- --verbose

# Custom log level and data directory
cargo run -- --log-level trace --data-dir ./mydata
```

When running, the binary starts an interactive REPL:

```text
wasm-kv CLI REPL (SET/GET/DELETE). Type EXIT to quit.
> SET greeting "hello world"
OK
> GET greeting
hello world
> DELETE greeting
OK
> GET greeting
(nil)
```

`SET` supports unquoted and quoted values, including escaped quotes inside quoted values.

### Test

```bash
cargo test --verbose
```

### Heap Profiling

```bash
cargo run --features dhat-heap
```

## API

The core interface is the `KVEngine` trait:

```rust
pub trait KVEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    fn delete(&mut self, key: &[u8]);
}
```

### Usage

```rust
use wasm_kv::PersistentKV;
use wasm_kv::api::api::KVEngine;

let mut kv = PersistentKV::new();

// Put
kv.put(b"hello".to_vec(), b"world".to_vec()).unwrap();

// Get
let value = kv.get(b"hello").unwrap();
assert_eq!(value.unwrap().as_slice(), b"world");

// Delete
kv.delete(b"hello");
assert!(kv.get(b"hello").unwrap().is_none());
```

## Storage Format

### WAL Record

The WAL file begins with a 32-byte header containing a magic number (`WALMGIC\0`), version, and checkpoint metadata:

```
┌─────────────────────────────────────────────────────────┐
│  WAL Header (32 bytes)                                  │
│  ┌──────────┬─────────┬──────────────────┬───────────┐ │
│  │ magic    │ version │ last_checkpoint  │ reserved  │ │
│  │ (8 bytes)│(8 bytes)│   (8 bytes)      │ (8 bytes) │ │
│  └──────────┴─────────┴──────────────────┴───────────┘ │
└─────────────────────────────────────────────────────────┘
```

Each WAL record is appended with the following binary layout:

```
┌────────────┬──────┬──────────┬──────┬────────────┬───────┬──────────┐
│record_type │ lsn  │ key_len  │ key  │ value_len  │ value │ checksum │
│ (1 byte)   │(8 B) │ (8 bytes)│(var) │  (8 bytes) │ (var) │(4 bytes) │
└────────────┴──────┴──────────┴──────┴────────────┴───────┴──────────┘
```

- **record_type**: 1 byte — `1` for Put, `2` for Delete
- **lsn**: 8 bytes — Log Sequence Number for ordering and recovery
- **key_len**: 8 bytes — key length as u64
- **key**: variable length — the key bytes
- **value_len**: 8 bytes — value length as u64
- **value**: variable length — the value bytes
- **checksum**: 4 bytes — CRC32 checksum of the value for integrity verification

### SSTable Layout

Each SSTable file is structured as:

```
┌─────────────────────────────────────────┐
│  Data Blocks (N x 4KB blocks)           │
│  ┌─────────────────────────────────┐    │
│  │ Record: key_len|val_len|key|val │    │
│  │ Record: ...                     │    │
│  └─────────────────────────────────┘    │
├─────────────────────────────────────────┤
│  Sparse Index                           │
│  ┌─────────────────────────────────┐    │
│  │ first_key | block_offset | ...  │    │
│  └─────────────────────────────────┘    │
├─────────────────────────────────────────┤
│  Bloom Filter (serialized bit vector)   │
├─────────────────────────────────────────┤
│  Footer                                 │
│  ┌─────────────────────────────────┐    │
│  │ data_block_start/end            │    │
│  │ index_block_start/end           │    │
│  │ bloom_block_start/end           │    │
│  │ index_checksum | bloom_checksum │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

**Read flow**: Footer is read first (fixed size at end of file) to locate the sparse index and bloom filter offsets. The bloom filter provides fast negative lookups, the sparse index locates the target block, and then the block is scanned linearly.

## Learning Resources

This project follows a phased learning roadmap (see [`PLAN.md`](./PLAN.md)) covering:

- Binary encoding and on-disk record formats
- Write-Ahead Logging and crash recovery
- LSM-tree architecture (MemTable, SSTable, compaction)
- Bloom filters and sparse indexing
- Concurrency patterns (channels, RwLock, atomic operations)

### Further Reading

- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md) — Google's original LSM-tree implementation
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki) — Production-grade LSM with advanced compaction strategies
- [WiscKey: Separating Keys from Values](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) — Key-value separation to reduce write amplification
- [Database Internals by Alex Petrov](https://www.databass.dev/) — Comprehensive guide to storage engine design
