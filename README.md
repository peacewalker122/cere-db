[![Rust](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml/badge.svg)](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml)

# CereDB

Single-node embedded key-value store in Rust, built as an architecture-focused LSM implementation.

This repository currently implements an async write/read engine (`KV2`) with:

- WAL-backed durability and restart recovery
- Skiplist memtable with per-key versioning by LSN
- SSTable flush pipeline (block + sparse index + bloom + footer)
- Append-only manifest metadata
- Background leveled compaction trigger path
- CLI REPL for interactive testing

Primary intent of this README: help reviewers evaluate correctness boundaries, coupling, and operational risk in the current design.

---

## What is implemented now

### Write path

`put/delete` in `KV2` delegates to `WriteComponent`:

1. Allocate monotonically increasing LSN.
2. Append WAL record and `sync_data`.
3. Insert `(key, lsn) -> MemtableRecord` into `SkipMap`.
4. Track memtable bytes.
5. If memtable threshold is reached, rotate active memtable and flush to an L0 SSTable.

Flush threshold is defined in `src/storage/constant.rs`:

- `MEMTABLE_SIZE_THRESHOLD = 409600` bytes (comment says 4MB, value is ~400KB)

### Read path

`ReadManager::get` resolves reads in this order:

1. In-memory snapshot read from memtable range `(key, 0..=sequence_number)` and take newest LSN.
2. On miss, scan manifest levels in ascending level order.
3. For each SSTable (newest-first within a level):
   - Load footer/index/bloom sections.
   - Bloom negative-check short-circuit.
   - Use sparse index key range to locate candidate block.
   - Decode block and resolve exact key record.

Deletes are tombstones (`RecordType::Delete`) and return `None`.

### Flush / SSTable construction

Flush currently:

- Folds memtable versions into latest-per-key winners (highest LSN).
- Builds 4KB data blocks (`SSTABLE_BLOCK_SIZE = 4096`).
- Builds sparse index entries with first/last key + block offset + record count.
- Builds bloom filter over surviving keys.
- Serializes SSTable sections in this order:

`data blocks -> sparse index -> bloom filter -> footer`

### Manifest

`ManifestManager` is append-only and checksum-protected per entry.

Entry kinds:

- `ADD` SSTable
- `REMOVE` SSTable
- `CHECKPOINT` (checkpoint LSN + active WAL segment)

Snapshot API provides consistent metadata view to readers and compaction.

### Compaction

Compaction is triggered when level-0 file count reaches threshold:

- `MAXIMUM_LEVEL_FILES = 2`

Flow:

1. Sender emits `CompactLevel { level: 0 }` into bounded channel.
2. Dedicated worker thread receives trigger.
3. Worker runs async compaction with a Tokio runtime inside rayon scope.
4. Compaction merges:
   - all files in level N
   - overlapping files in level N+1 (by key range)
5. Output SSTable is written into level N+1 and manifest is updated (add new, remove stale).

---

## Architecture review notes

### Component boundaries

- `KV2` is the orchestration boundary (open/recover/wire read+write+compaction trigger).
- `WriteComponent` owns mutation path concerns (LSN assignment, WAL append, memtable mutation, flush).
- `ReadManager` owns read resolution order and SSTable probing strategy.
- `ManifestManager` is the metadata source of truth shared across read/write/compaction.
- `WALManager` owns durability log file lifecycle and replay.

This separation is clean enough for iterative experimentation, but consistency guarantees are still composition-based (cross-component ordering discipline), not transactionally unified.

### Correctness-critical invariants

Reviewers should validate these invariants remain true across future changes:

1. **WAL-before-memtable** on writes (durability before visibility).
2. **Monotonic LSN** per process lifetime and after recovery bootstrap.
3. **Latest-LSN-wins** during flush compaction from `(key, lsn)` space.
4. **Manifest update durability** for add/remove/checkpoint entries.
5. **Read precedence**: memtable snapshot first, then persisted levels.
6. **Tombstone semantics** preserved across memtable, SSTable, and compaction.

### Coupling and concurrency model

- Foreground path is async (`tokio`) for open/read/write/flush IO.
- Compaction trigger path bridges `crossbeam_channel` + dedicated thread + rayon scope + nested Tokio runtime.

This mixed concurrency stack works functionally, but increases lifecycle/observability complexity (thread ownership, runtime nesting, backpressure behavior).

### Performance-relevant design choices (current)

- 4KB block granularity for SSTable data path.
- Bloom + sparse index used to reduce negative and wide-scan reads.
- Level-0 compaction trigger at low threshold (`MAXIMUM_LEVEL_FILES=2`) for early behavior exercise.
- WAL `sync_data` per write favors durability clarity over throughput.

### Risk areas for architecture evolution

- Manifest and WAL checkpoint coordination is append-only but not yet optimized for long-term metadata growth.
- Compaction policy is minimal (functional baseline, limited tuning knobs).
- Some recovery/raft-adjacent files are present but outside active runtime path, which can confuse architectural ownership.

### Architecture review checklist

| Invariant / concern | Primary enforcement point(s) | How to verify quickly |
|---|---|---|
| WAL-before-memtable visibility | `WriteComponent::put/delete` (`write.rs`) writes WAL before memtable insert | Inspect call order in code and run crash/restart tests around recent write operations |
| Monotonic LSN assignment | `WriteComponent.sequence_number` + recovery bootstrap in `KV2::open` | Confirm LSN increases on each mutation and that restart resumes from recovered max LSN |
| Latest version wins on flush | `WriteComponent::flush` folds by key, keeps highest LSN | Add/verify test with multiple versions of same key before flush |
| Tombstone correctness | `ReadManager::get` and compaction `flush_winner` skip/delete semantics | Verify delete after put returns `(nil)` before and after flush/compaction |
| Read precedence (memtable over SSTable) | `ReadManager::get` checks memtable before manifest/SSTables | Create key in memtable shadowing older SSTable value and assert newest read |
| Manifest as source of truth | `ManifestManager` append+replay and snapshot usage | Validate restart state reconstructs expected level/file metadata |
| Compaction overlap safety | `compaction.rs` range overlap selection (`smallest_key/largest_key` fallback to index range) | Use fixtures with overlapping/non-overlapping key ranges and assert selected merge set |
| Background compaction lifecycle | `KV2` channel trigger + worker loop + async compaction invocation | Verify trigger emits when L0 threshold reached and manifest changes after worker completion |
| SSTable section integrity | `SSTableFooter` checksums/magic + section decode paths | Corrupt footer/index/bloom bytes in test and assert decode failure |
| Config/runtime consistency | constants + CLI/config defaults (`constant.rs`, `config.rs`) | Ensure README/documented values match code constants and runtime flags |

---

## Current architecture (as coded)

```text
Client PUT/DELETE
   -> WAL append + fsync
   -> Memtable insert (SkipMap key=(user_key, lsn))
   -> Threshold check
      -> rotate memtable
      -> flush winners to L0 SSTable
      -> manifest register + WAL rotate/checkpoint
      -> optional compaction trigger

Client GET
   -> Memtable latest visible LSN lookup
   -> On miss: manifest snapshot -> leveled SSTable search
      bloom -> sparse index -> block decode -> key match
```

---

## Project structure (active paths)

```text
src/
├── main.rs
├── lib.rs
├── config.rs
├── repl.rs
├── command.rs
├── api/
│   └── api.rs                      # KVEngine + AsyncKVEngine traits
└── storage/
    ├── kv2.rs                      # Engine orchestration
    ├── constant.rs                 # Core thresholds/layout constants
    ├── record.rs                   # Memtable record and record type
    ├── sstable_codec.rs            # SSTable section serialization
    ├── footer.rs                   # Footer offsets/checksums/magic
    ├── index.rs                    # Sparse index entry format
    ├── bloom.rs                    # Bloom filter wrapper
    ├── manifest_codec.rs           # Append-only manifest manager
    ├── writemanager/
    │   ├── write.rs                # WAL write + memtable + flush
    │   └── block.rs                # BlockBuilder / Block encoding
    ├── readmanager/
    │   └── read.rs                 # Memtable + SSTable read path
    ├── compactionmanager/
    │   └── compaction.rs           # Leveled compaction
    └── recovermanager/
        ├── wal.rs                  # WAL manager + replay
        ├── segment.rs
        ├── log_store.rs
        └── raft.rs                 # Future strong-consistency path (not active in KV2 runtime yet)
```

---

## Getting started

### Prerequisites

- Rust toolchain (edition 2024)

### Build

```bash
cargo build
```

### Run CLI

```bash
# default log level: info
cargo run

# debug logging
cargo run -- --verbose

# custom log level and data directory
cargo run -- --log-level trace --data-dir ./mydata
```

Startup banner:

```text
ceredb CLI REPL (SET/GET/DELETE). Type EXIT to quit.
```

Example session:

```text
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

### Heap profiling

```bash
cargo run --features dhat-heap
```

---

## API surface

The async engine implements `AsyncKVEngine`:

```rust
pub trait AsyncKVEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;
}
```

Usage:

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

---

## Format details

### WAL

WAL files start with a fixed header (`WALMGIC\0`, version, checkpoint metadata).

Record encoding includes:

- record type
- LSN
- key length + value length
- key bytes + value bytes
- checksum

### SSTable footer

Footer is fixed-size (`64` bytes) and stores:

- data/index/bloom offsets
- index and bloom checksums
- magic number + footer checksum

This enables section-wise reads without full table decode.

---

## Known limitations / in-progress areas

To keep the README aligned with current code, these are explicit:

- Not a distributed/replicated DB; single-node embedded engine.
- Compaction executes via background trigger path, but policy/tuning is still minimal.
- Raft-related modules are intentional forward planning for future strong-consistency/distributed behavior; they are not active in the current `KV2` runtime path yet.
- Constant comments and exact runtime values may diverge in places (e.g., memtable threshold comment vs value).
- Mixed async/thread/rayon runtime model is functional but not yet simplified for operational ergonomics.

---

## Why this repo is useful for storage-engine learning

This codebase captures the hard boundaries between durability, in-memory state, on-disk structures, and background maintenance:

- WAL fsync semantics vs flush throughput
- MVCC-like latest-by-LSN resolution in memtable
- SSTable section layout and lookup accelerators (bloom + sparse index)
- Manifest as durable metadata source of truth
- Level overlap analysis during compaction

If your goal is to understand *how LSM pieces fit together in real Rust code*, this repo is designed for that.
