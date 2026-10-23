# Transactional WAL

The file WAL adapts the transaction chains, commit records, frame checksums, and
tail-repair rules from [first-principle/wal](https://github.com/peacewalker122/first-principle/tree/e9bbf81206a02d93e234f2fe1a0510270b5075df/wal).
It uses logical key/value updates for CereDB's LSM storage instead of fixed-size
page updates. The formats are not binary compatible.

## Write and recovery contract

`write_log` and `LogStore::append` are single-update transactions.
`write_transaction(Vec<LogCommand>)` durably commits a nonempty batch. It is a WAL
primitive; it does not publish the batch to an already running engine's memtable.
The existing engine puts and deletes publish only after their WAL call succeeds.

The write graph is:

`validate commands → lock writer → assign physical LSNs → rotate if needed → append updates and commit → sync_data → acknowledge → publish memtable update`

One mutex owns the file, segment number, byte count, and physical LSN allocator.
Rotation cannot race appends. Each transaction syncs once; this is not group
commit across callers. Transactions stay within one segment and may exceed the
configured segment target. Version-two record bodies are limited to 64 MiB.
A failed or cancelled write/rotation poisons the writer; reopen it before reuse.
A failed acknowledgement can still have committed, so callers must treat its
outcome as uncertain.

The recovery graph is:

`scan segments numerically → validate headers, frames, LSNs and transaction chains → identify committed transactions → replay their updates in physical order`

Physical LSNs start at one and count every update and commit. Each transaction ID
is its first physical LSN. `previous_lsn` links that transaction's records.
`LogCommand.lsn` remains the application's MVCC sequence, independently of the
physical WAL sequence, including across restarts.

Uncommitted updates never reach SSTables through the engine write path. Recovery
starts with an empty memtable, so loser undo consists of excluding their updates.
There are no mutable database pages, page LSNs, before-images, dirty-page table,
or compensation log records. This is an ARIES-inspired no-steal LSM adaptation,
not full steal/no-force ARIES page recovery. Raft's consensus log and commit index
remain separate from this local file WAL.

Only an incomplete final frame in the newest segment is truncated on open. A
complete frame header must pass its independent checksum before its body length
can justify truncation. The truncation is synced before appending. Checksum failures, invalid
records, broken transaction chains, duplicate/gapped segment IDs, unsupported
versions, and incomplete sealed segments fail opening. Repeating recovery
returns the same committed commands without modifying complete frames.
Legacy version-one records retain their original autocommit semantics and accept
payloads larger than 64 MiB, bounded by the available file bytes. They are
read in place and new writes start a version-two segment. Migration does not
recover records already deleted by the old implementation. Opening the same WAL
directory with multiple managers/processes is unsupported; the owner must provide
exclusive access.

## Segments and checkpoints

Creating a segment uses exclusive creation and syncs its header and parent
directory before use. If creation is interrupted, opening completes an empty or
partial newest header only when it is an exact prefix of a supported zero-checkpoint
header. Ambiguous prefixes select version two; version one is allowed only before
transactional records. Completion appends the missing suffix and syncs the file
and parent directory before use, so another interruption remains recoverable.
Malformed prefixes, corrupt complete headers, and incomplete sealed headers fail
without repair. Existing complete segments are never overwritten. Rotation
syncs and seals the old segment without deleting or recycling it.
`mark_reserved` validates a sealed segment and marks it in memory; it does not
reclaim its bytes.

Flush persists and syncs the SSTable and its directory before registering it in
the manifest, rotating the WAL, and recording checkpoint metadata. The manifest
checkpoint uses the flushed application's maximum sequence and the active
segment ID, not a physical redo frontier. WAL header checkpoint fields remain
zero. Recovery conservatively replays retained WAL history, including already
flushed commands. WAL disk usage and recovery work therefore grow with history;
there is no checkpoint-based reclamation or fuzzy-checkpoint optimization yet.
Never delete retained segments manually as a routine restart/reset procedure.

The engine flushes a snapshot of the shared active memtable. Failed or cancelled
flushes retain its readable entries; successful flushes remove only snapshot
entries, preserving later writes. Existing SSTable filenames are skipped with
fresh file IDs, so interrupted flushes cannot block retries after restart or
overwrite registered data. Unregistered orphan files remain on disk.

## Version-two encoding

All integers are big endian. The existing 36-byte segment header contains the
8-byte magic, version `2` (`u64`), two zero checkpoint fields (`u64` each), and a
CRC-32/IEEE of the first 32 bytes.

Each frame contains physical LSN (`u64`), body length (`u32`), body CRC-32/IEEE
(`u32`), header CRC-32/IEEE (`u32`), and body. The 20-byte frame header's checksum
covers its first 16 bytes and is validated before trusting the length. The body
checksum covers the first 12 frame bytes followed by the body.
A body starts with kind (`u8`: update `1`, commit `2`), transaction ID (`u64`),
and previous physical LSN (`u64`). An update continues with operation (`u8`: put
`1`, delete `2`), application sequence (`u64`), key length (`u64`), value length
(`u64`), key bytes and value bytes. Deletes require an empty value. A commit has
no additional fields. `WALRecord.checksum` is the source frame checksum for
version two, or the original record checksum for version one.

## Verification

Run `cargo test --lib storage::recovermanager`, `cargo test --lib`, and
`cargo test --test integration`. The WAL tests use real temporary files and cover
all truncated prefixes of a batch and segment header, loser exclusion, restart followed by append,
checksums and chain validation, legacy migration, concurrent rotation, and
preservation of acknowledged writes across segments.
