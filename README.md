# CereDB

[![Rust](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml/badge.svg)](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml)
An LSM-tree based key-value store built from scratch in Rust. This project is focused on learning storage-engine internals end-to-end: WAL durability, memtable flushing, SSTable codecs, manifests, and leveled compaction.

## Overview

`ceredb` is a single-node embedded KV store with an async engine (`KV2`) and CLI REPL.

- **Write path**: WAL append → in-memory SkipMap memtable → flush to L0 SSTable when threshold is reached.
- **Read path**: memtable first, then leveled SSTables using bloom filters + sparse index.
- **Compaction path**: background compaction worker merges level-N files and overlapping level-(N+1) files.

## Current Architecture

```
PUT/DELETE
   │
   ▼
WAL (durability, replay)
   │
   ▼
MemTable (SkipMap, latest LSN wins)
   │  threshold reached (MEMTABLE_SIZE_THRESHOLD)
   ▼
Flush -> SSTable (L0)
   │
   ├── Data Blocks (4KB)
   ├── Sparse Index (first_key/last_key + offset)
   ├── Bloom Filter
   └── Footer (offsets + checksums)
   │
   ▼
Manifest (append-only metadata)
  - file_id, level, path, record_count, bloom
  - smallest_key/largest_key cache (derived from sparse index)
   │
   ▼
Compaction Worker
  - Triggered when level file count threshold is exceeded
  - Selects overlap via key ranges
  - Merges level N + overlapping level N+1
  - Writes output into level N+1 and updates manifest
```

## Features

- **WAL durability + recovery**
  - CRC32 integrity checks
  - WAL replay on startup
  - WAL rotation on successful flush
- **MemTable on SkipMap** (`crossbeam-skiplist`)
- **SSTable codec**
  - 4KB blocks
  - sparse index for block targeting
  - bloom filter for negative lookups
  - fixed-size footer with section offsets and checksums
- **Manifest manager (`manifest_codec`)**
  - append-only metadata log
  - snapshot view for reads/compaction
  - per-SSTable key-range cache (`smallest_key`, `largest_key`)
- **Leveled compaction**
  - compacts full source level with overlapping next-level SSTables
  - overlap based on key ranges
  - falls back to sparse-index-derived ranges for legacy entries
- **CLI REPL**
  - `SET`, `GET`, `DELETE`, `EXIT/QUIT`

## Project Structure (current)

```
src/
├── main.rs
├── lib.rs
├── repl.rs
├── command.rs
├── config.rs
├── error.rs
├── api/
│   └── api.rs                      # KVEngine + AsyncKVEngine traits
└── storage/
    ├── kv2.rs                      # Main engine orchestration
    ├── constant.rs
    ├── record.rs
    ├── bloom.rs
    ├── index.rs
    ├── footer.rs
    ├── sstable_codec.rs            # SSTable serialize/deserialize utilities
    ├── manifest_codec.rs           # Active manifest implementation
    ├── writemanager/
    │   ├── write.rs                # Flush + register metadata
    │   └── block.rs
    ├── readmanager/
    │   └── read.rs
    ├── compactionmanager/
    │   └── compaction.rs
    └── recovermanager/
        ├── wal.rs
        └── segment.rs
```

> Some older storage files are still present for learning/history, but the active path is driven by `kv2.rs` + manager modules above.

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
cere CLI REPL (SET/GET/DELETE). Type EXIT to quit.
> SET greeting "hello world"
OK
> GET greeting
hello world
> DELETE greeting
OK
> GET greeting
(nil)
```

### Test

```bash
cargo test --verbose
```

### Heap Profiling

```bash
cargo run --features dhat-heap
```

## API

The async engine implements `AsyncKVEngine`:

```rust
pub trait AsyncKVEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;
}
```

### Usage

```rust
use ceredb::api::api::AsyncKVEngine;
use ceredb::KV2;

#[tokio::main]
async fn main() {
    let mut kv = KV2::open("./data").await.unwrap();

    kv.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
    let value = kv.get(b"hello").await.unwrap();
    assert_eq!(value.unwrap().as_ref(), b"world");

    kv.delete(b"hello".to_vec()).await.unwrap();
    assert!(kv.get(b"hello").await.unwrap().is_none());
}
```

## Storage Format

### WAL Record

WAL begins with a fixed header (`WALMGIC\0`) and appends records as:

```
record_type (1 byte) | lsn (8) | key_len (8) | value_len (8) | key | value | checksum (4)
```

### SSTable Layout

```
Data Blocks -> Sparse Index -> Bloom Filter -> Footer
```

Footer stores offsets/checksums for section lookup. Read path uses footer first, then bloom/index to narrow data block reads.

## Learning Resources

This project follows a phased learning roadmap (see [`PLAN.md`](./PLAN.md)) around:

- On-disk encoding and checksums
- WAL + crash recovery
- Memtable/SSTable/manifest interactions
- Leveled compaction mechanics
- Concurrency and background workers

### Further Reading

- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [Database Internals by Alex Petrov](https://www.databass.dev/)
