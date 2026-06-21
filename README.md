[![Rust](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml/badge.svg)](https://github.com/peacewalker122/slow-database/actions/workflows/rust.yml)

# CereDB

Embedded key-value store in Rust with optional Raft consensus — built as an
architecture-focused LSM implementation.

Supports two modes:

- **Single-node** (`KV2`) — WAL-backed durability, no replication.
- **Raft cluster** (`RaftKV2`) — multi-node consensus via openraft, log
  replication, leader election, automatic failover.

This repository implements an async write/read engine with:

- WAL-backed durability and restart recovery
- Skiplist memtable with per-key versioning by LSN
- SSTable flush pipeline (block + sparse index + bloom + footer)
- Append-only manifest metadata
- Background leveled compaction trigger path
- openraft-based Raft consensus layer with HTTP transport
- CLI REPL for interactive testing
- Real-time cluster monitoring dashboard

Primary intent of this README: help reviewers evaluate correctness boundaries,
coupling, and operational risk in the current design.

---

## What is implemented now

### Write path (single-node KV2)

`put/delete` in `KV2` delegates to `WriteComponent`:

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
   │
   ├── Single-node mode (KV2)
   │     -> WAL append + fsync
   │     -> Memtable insert (SkipMap key=(user_key, lsn))
   │     -> Threshold check
   │          -> rotate memtable
   │          -> flush winners to L0 SSTable
   │          -> manifest register + WAL rotate/checkpoint
   │          -> optional compaction trigger
   │
   └── Raft mode (RaftKV2)
         -> Leader checks leadership
         -> ClientWrite proposal to Raft
         -> Raft log replication (leader → followers)
         -> Majority commit
         -> KVStateMachine::apply() → WriteComponent (no WAL write)
         -> Memtable insert
         -> Threshold check (same flush/compaction as above)

Client GET (Raft mode — leader only)
   -> RaftKV2::get verifies leadership
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
├── config.rs                     # CLI flags, StorageConfig + RaftNodeConfig
├── repl.rs
├── command.rs
├── api/
│   └── api.rs                    # KVEngine + AsyncKVEngine traits
├── storage/
│   ├── kv2.rs                    # Engine orchestration (KV2 + RaftKV2)
│   ├── constant.rs               # Core thresholds/layout constants
│   ├── record.rs                 # Memtable record and record type
│   ├── sstable_codec.rs          # SSTable section serialization
│   ├── footer.rs                 # Footer offsets/checksums/magic
│   ├── index.rs                  # Sparse index entry format
│   ├── bloom.rs                  # Bloom filter wrapper
│   ├── manifest_codec.rs         # Append-only manifest manager
│   ├── raft/                     # openraft consensus layer
│   │   ├── mod.rs                # RaftConsensusLayer wrapper
│   │   ├── types.rs              # TypeConfig (D=RPC, NodeId=u64)
│   │   ├── log_store.rs          # Disk-backed RaftLogStorage
│   │   ├── state_machine.rs      # KVStateMachine (applies to WriteComponent)
│   │   ├── network.rs            # HTTP RaftNetwork + RaftNetworkFactory
│   │   ├── server.rs             # Axum HTTP server for Raft RPCs
│   │   └── mod_tests.rs          # Unit tests (single + multi-node)
│   ├── writemanager/
│   │   ├── write.rs              # WAL write + memtable + flush
│   │   └── block.rs              # BlockBuilder / Block encoding
│   ├── readmanager/
│   │   └── read.rs               # Memtable + SSTable read path
│   ├── compactionmanager/
│   │   └── compaction.rs         # Leveled compaction
│   └── recovermanager/
│       ├── wal.rs                # WAL manager + replay
│       ├── segment.rs
│       └── log_store.rs          # LogCommand + LogStore trait

tools/
└── raft-dashboard/
    └── index.html                # Real-time cluster monitoring UI
```

---

## Getting started

### Prerequisites

- Rust toolchain (edition 2024)

### Build

```bash
cargo build
```

### Run CLI — single-node mode

```bash
# default log level: info
cargo run

# debug logging
cargo run -- --verbose

# custom log level and data directory
cargo run -- --log-level trace --data-dir ./mydata
```

### Run CLI — Raft cluster mode

Start three nodes (separate terminals):

```bash
# Node 1
cargo run -- --raft-mode --raft-node-id 1 --raft-http-bind 127.0.0.1:21011 --raft-dir /tmp/raft1

# Node 2
cargo run -- --raft-mode --raft-node-id 2 --raft-http-bind 127.0.0.1:21012 --raft-peers "1:127.0.0.1:21011,3:127.0.0.1:21013" --raft-dir /tmp/raft2

# Node 3
cargo run -- --raft-mode --raft-node-id 3 --raft-http-bind 127.0.0.1:21013 --raft-peers "1:127.0.0.1:21011,2:127.0.0.1:21012" --raft-dir /tmp/raft3
```

Open the dashboard to watch elections and replication:

```bash
open tools/raft-dashboard/index.html
```

Add node URLs (`127.0.0.1:21011`, `127.0.0.1:21012`, `127.0.0.1:21013`) to
the dashboard panel.

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

Both engines implement `AsyncKVEngine`:

```rust
pub trait AsyncKVEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;
}
```

### Single-node (`KV2`)

```rust
use ceredb::KV2;
use ceredb::api::api::AsyncKVEngine;

let mut kv = KV2::open("./data").await.unwrap();
kv.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
```

### Raft cluster (`RaftKV2`)

```rust
use ceredb::storage::kv2::RaftKV2;
use ceredb::storage::raft::RaftNodeConfig;
use ceredb::storage::config::StorageConfig;
use ceredb::api::api::AsyncKVEngine;

let config = RaftNodeConfig {
    node_id: 1,
    peers: vec![(2, "127.0.0.1:21012".into())],
    http_bind: "127.0.0.1:21011".parse().unwrap(),
    raft_dir: "./data/raft".into(),
};
let mut kv = RaftKV2::open("./data", StorageConfig::default(), config)
    .await
    .unwrap();
// Must initialize the cluster once before use
kv.raft_layer().initialize(&[1, 2]).await.unwrap();

kv.put(b"hello".to_vec(), b"world".to_vec()).await.unwrap();
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

## Raft consensus layer

The Raft implementation uses **[openraft](https://github.com/datafuselabs/openraft)**
v0.9.24 (`storage-v2` feature).  HTTP transport (reqwest + axum) replaces the
protobuf/gRPC approach of an earlier `rust-raft` prototype.

### Architecture

```text
RaftKV2 (implements AsyncKVEngine)
  │
  ├── RaftConsensusLayer (wraps openraft::Raft<TypeConfig>)
  │     ├── LogStore       — RaftLogStorage + RaftLogReader
  │     │                    (bincode-serialized entries.dat + vote)
  │     ├── KVStateMachine — RaftStateMachine
  │     │                    (applies committed entries to WriteComponent)
  │     ├── RaftHttpNetwork — RaftNetworkFactory
  │     │                    (maps node IDs to HTTP URLs)
  │     └── axum server    — /raft/{append_entries,vote,install_snapshot,metrics}
  │
  └── WriteComponent (shared via Arc<RwLock<>>)
        └── Memtable + SSTable (same flush/compaction as single-node mode)
```

### Key design points

- **Shared state machine handle** — `WriteComponent` lives behind
  `Arc<RwLock<>>`, shared between `KVStateMachine` (applies committed entries)
  and `RaftKV2` (local reads).  No event consumer channel needed.
- **Raft log serves as WAL** — committed entries are applied to the state machine
  without a separate WAL write (the Raft log IS the durability layer).
- **Leader-only reads** — `get()` and `scan()` check `is_leader()` before
  serving.  Followers return "not the leader" error.  Linearizable reads via
  ReadIndex protocol are not yet implemented.
- **Brutalism-style dashboard** — `tools/raft-dashboard/index.html` is a
  standalone HTML page that polls `/raft/metrics` on each node and displays
  node state, leader status, replication progress bars, and a live event log.

### Cluster lifecycle

1. Start each node with `--raft-mode` and the peer addresses.
2. Call `initialize` on **one** node with all member IDs.
3. That node immediately campaigns and becomes leader.
4. Followers receive the membership config via append_entries replication.
5. All nodes participate in subsequent elections.

### Test coverage

| Test | What it verifies |
|------|-----------------|
| `start_and_shutdown_single_node` | Single-node cluster, init, election |
| `single_node_propose_and_commit` | Propose → commit → apply cycle |
| `reject_proposals_to_follower` | Non-leader rejects writes |
| `three_node_elects_leader` | 3-node cluster elects exactly one leader |
| `three_node_propose_from_leader` | Leader replication to all 3 nodes |
| `leader_failover` | Leader crash → new election → writes continue |

---

## Known limitations / in-progress areas

To keep the README aligned with current code, these are explicit:

- **Raft cluster is functional but unhardened** — basic operations (election,
  replication, failover) work, but membership changes (add/remove nodes) and
  snapshot-based log compaction are still TODO.
- Compaction executes via background trigger path, but policy/tuning is still minimal.
- Constant comments and exact runtime values may diverge in places (e.g., memtable threshold comment vs value).
- Mixed async/thread/rayon runtime model is functional but not yet simplified for operational ergonomics.

---

## Why this repo is useful for storage-engine learning

This codebase captures the hard boundaries between durability, in-memory state,
on-disk structures, background maintenance, and distributed consensus:

- WAL fsync semantics vs flush throughput
- MVCC-like latest-by-LSN resolution in memtable
- SSTable section layout and lookup accelerators (bloom + sparse index)
- Manifest as durable metadata source of truth
- Level overlap analysis during compaction
- Raft consensus log storage, state machine, and HTTP transport integration
- Shared-memory state machine pattern (Arc<RwLock<WriteComponent>>)
- Real-time cluster monitoring with brute-force HTTP polling
