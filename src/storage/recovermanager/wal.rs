use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64},
    },
};

use crossbeam_skiplist::SkipMap;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use super::{
    log_store::{LogCommand, LogPosition, LogStore},
    segment::SegmentId,
};
use crate::{
    error::DBError,
    storage::{constant::*, manifest_codec::RecoveryCheckpoint, record::RecordType},
};

const LEGACY_WAL_VERSION: u64 = 1;
const MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;
const FRAME_HEADER_SIZE: usize = 20;
const MAX_GROUP_COUNT: usize = 1024;
const MAX_GROUP_ENCODED_SIZE: usize = 256 * 1024 * 1024;
const WAL_VALUE_REF_VERSION: u8 = 1;
pub(crate) const WAL_VALUE_REF_ENCODED_SIZE: usize = 94;
const MIN_WAL_UPDATE_FRAME_SIZE: usize = FRAME_HEADER_SIZE + 42;
const MAX_WAL_UPDATE_FRAME_SIZE: usize = FRAME_HEADER_SIZE + MAX_FRAME_SIZE;

pub struct WALManager {
    state: Mutex<WriterState>,
    max_segment_size: u64,
    wal_dir: PathBuf,
    raw_writes: std::sync::atomic::AtomicBool,
    checkpoint_owner: bool,
    checkpoint_owner_registered: AtomicBool,
    checkpoint_token: OnceLock<Arc<CheckpointOwner>>,
    #[cfg(test)]
    sync_data_count: AtomicU64,
    #[cfg(test)]
    planned_update_count: AtomicU64,
}

pub(crate) struct CheckpointOwner {
    _private: (),
}

struct WriterState {
    file: File,
    segment_id: u64,
    size: u64,
    next_lsn: u64,
    poisoned: bool,
    sealed: BTreeSet<u64>,
    checkpoint: Option<RecoveryCheckpoint>,
}

pub type FileWalStore = WALManager;

#[derive(Debug, Clone)]
pub struct WALRecord {
    pub record_type: u8,
    pub lsn: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub checksum: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WalValueRef {
    pub(crate) segment_id: u64,
    pub(crate) transaction_id: u64,
    pub(crate) transaction_start_offset: u64,
    pub(crate) commit_offset: u64,
    pub(crate) commit_lsn: u64,
    pub(crate) frame_offset: u64,
    pub(crate) frame_length: u32,
    pub(crate) physical_lsn: u64,
    pub(crate) application_sequence: u64,
    pub(crate) record_type: RecordType,
    pub(crate) key_length: u32,
    pub(crate) key_checksum: u32,
    pub(crate) value_length: u64,
    pub(crate) value_checksum: u32,
}

#[derive(Debug)]
pub(crate) struct RecoveredWalUpdate {
    pub(crate) record: WALRecord,
    pub(crate) reference: WalValueRef,
}

#[derive(Debug)]
enum TransactionRecord {
    Update(LogCommand),
    Commit,
}

#[derive(Debug)]
struct Frame {
    lsn: u64,
    transaction_id: u64,
    previous_lsn: u64,
    record: TransactionRecord,
}

#[derive(Default)]
struct Analysis {
    latest_lsn: u64,
    previous_lsns: BTreeMap<u64, u64>,
    committed_transactions: BTreeSet<u64>,
    updates: Vec<LoggedUpdate>,
    legacy: Vec<WALRecord>,
    track_refs: bool,
    transaction_starts: BTreeMap<u64, u64>,
    commit_positions: BTreeMap<u64, CommitPosition>,
}

struct LoggedUpdate {
    transaction_id: u64,
    command: LogCommand,
    checksum: u32,
    position: Option<Box<UpdatePosition>>,
}

#[derive(Clone, Copy)]
struct FramePosition {
    segment_id: u64,
    offset: u64,
    length: u32,
}

struct UpdatePosition {
    segment_id: u64,
    transaction_start_offset: u64,
    frame_offset: u64,
    frame_length: u32,
    physical_lsn: u64,
}

#[derive(Clone, Copy)]
struct CommitPosition {
    frame_offset: u64,
    commit_lsn: u64,
}

struct PlannedUpdate {
    transaction_id: u64,
    transaction_start_offset: u64,
    frame_offset: u64,
    frame_length: u32,
    physical_lsn: u64,
    application_sequence: u64,
    record_type: RecordType,
    key_length: u32,
    key_checksum: u32,
    value_length: u64,
    value_checksum: u32,
}

struct PlannedTransaction {
    updates: Vec<PlannedUpdate>,
    commit_offset: u64,
    commit_lsn: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReceiptCollection {
    Skip,
    Collect,
}

impl ReceiptCollection {
    fn collecting(self) -> bool {
        self == Self::Collect
    }
}

impl Analysis {
    fn accept(
        &mut self,
        frame: Frame,
        checksum: u32,
        position: FramePosition,
    ) -> Result<(), DBError> {
        let previous_lsn = self
            .previous_lsns
            .get(&frame.transaction_id)
            .copied()
            .unwrap_or(0);
        if frame.lsn != self.latest_lsn + 1
            || frame.transaction_id == 0
            || self.committed_transactions.contains(&frame.transaction_id)
            || frame.previous_lsn != previous_lsn
        {
            return Err(DBError::Corrupted("Invalid WAL transaction chain".into()));
        }
        let frame_lsn = frame.lsn;
        match frame.record {
            TransactionRecord::Update(command) => {
                if self.track_refs && previous_lsn == 0 {
                    self.transaction_starts
                        .insert(frame.transaction_id, position.offset);
                }
                let update_position = self.track_refs.then(|| {
                    Box::new(UpdatePosition {
                        segment_id: position.segment_id,
                        transaction_start_offset: self
                            .transaction_starts
                            .get(&frame.transaction_id)
                            .copied()
                            .unwrap_or(position.offset),
                        frame_offset: position.offset,
                        frame_length: position.length,
                        physical_lsn: frame_lsn,
                    })
                });
                self.updates.push(LoggedUpdate {
                    transaction_id: frame.transaction_id,
                    command,
                    checksum,
                    position: update_position,
                });
            }
            TransactionRecord::Commit => {
                if previous_lsn == 0 {
                    return Err(DBError::Corrupted("Commit without update".into()));
                }
                self.committed_transactions.insert(frame.transaction_id);
                if self.track_refs {
                    self.commit_positions.insert(
                        frame.transaction_id,
                        CommitPosition {
                            frame_offset: position.offset,
                            commit_lsn: frame_lsn,
                        },
                    );
                }
            }
        }
        self.latest_lsn = frame_lsn;
        self.previous_lsns.insert(frame.transaction_id, frame_lsn);
        Ok(())
    }

    fn redo_with_refs(self) -> Result<Vec<RecoveredWalUpdate>, DBError> {
        if !self.legacy.is_empty() {
            return Err(DBError::StorageError(
                "Legacy WAL records do not carry value references".into(),
            ));
        }
        let mut updates = Vec::new();
        for update in self.updates {
            if !self.committed_transactions.contains(&update.transaction_id) {
                continue;
            }
            let position = *update.position.ok_or_else(|| {
                DBError::StorageError("WAL recovery is missing an update position".into())
            })?;
            let commit = self
                .commit_positions
                .get(&update.transaction_id)
                .copied()
                .ok_or_else(|| {
                    DBError::StorageError("WAL recovery is missing a commit position".into())
                })?;
            let LogCommand {
                record_type,
                key,
                value,
                lsn,
            } = update.command;
            let reference = WalValueRef {
                segment_id: position.segment_id,
                transaction_id: update.transaction_id,
                transaction_start_offset: position.transaction_start_offset,
                commit_offset: commit.frame_offset,
                commit_lsn: commit.commit_lsn,
                frame_offset: position.frame_offset,
                frame_length: position.frame_length,
                physical_lsn: position.physical_lsn,
                application_sequence: lsn,
                record_type,
                key_length: u32::try_from(key.len())
                    .map_err(|_| DBError::Corrupted("WAL key length overflow".into()))?,
                key_checksum: crc32fast::hash(&key),
                value_length: u64::try_from(value.len())
                    .map_err(|_| DBError::Corrupted("WAL value length overflow".into()))?,
                value_checksum: crc32fast::hash(&value),
            };
            updates.push(RecoveredWalUpdate {
                record: WALRecord {
                    record_type: record_type as u8,
                    lsn,
                    key,
                    value,
                    checksum: update.checksum,
                },
                reference,
            });
        }
        Ok(updates)
    }

    fn redo(self) -> Vec<WALRecord> {
        let mut records = self.legacy;
        records.extend(
            self.updates
                .into_iter()
                .filter(|update| self.committed_transactions.contains(&update.transaction_id))
                .map(|update| WALRecord {
                    record_type: update.command.record_type as u8,
                    lsn: update.command.lsn,
                    key: update.command.key,
                    value: update.command.value,
                    checksum: update.checksum,
                }),
        );
        records
    }
}

impl WALManager {
    pub async fn new(wal_dir: PathBuf, max_segment_size: u64) -> Result<Self, DBError> {
        Self::new_inner(wal_dir, max_segment_size, None, false).await
    }

    pub(crate) async fn new_with_checkpoint(
        wal_dir: PathBuf,
        max_segment_size: u64,
        checkpoint: Option<RecoveryCheckpoint>,
    ) -> Result<Self, DBError> {
        Self::new_inner(wal_dir, max_segment_size, checkpoint, true).await
    }

    async fn new_inner(
        wal_dir: PathBuf,
        max_segment_size: u64,
        checkpoint: Option<RecoveryCheckpoint>,
        checkpoint_owner: bool,
    ) -> Result<Self, DBError> {
        crate::storage::durable_dir::create_dir_all_durable(&wal_dir).await?;
        let paths = segment_paths(
            &wal_dir,
            checkpoint.map(|checkpoint| checkpoint.replay_start_segment),
        )
        .await?;
        let mut analysis = Analysis::default();
        if let Some(checkpoint) = checkpoint {
            analysis.latest_lsn = checkpoint
                .next_physical_lsn
                .checked_sub(1)
                .ok_or_else(|| DBError::Corrupted("invalid checkpoint physical LSN".into()))?;
        }
        let mut latest_version = WAL_VERSION;
        for (index, (_, path)) in paths.iter().enumerate() {
            let tail_handling = if index + 1 == paths.len() {
                TailHandling::Repair
            } else {
                TailHandling::Reject
            };
            latest_version =
                scan_segment_with_tail_handling(path, tail_handling, &mut analysis).await?;
        }
        let mut segment_id = paths.last().map_or(0, |(id, _)| *id);
        let mut sealed: BTreeSet<u64> = paths.iter().map(|(id, _)| *id).collect();
        if latest_version == LEGACY_WAL_VERSION && !paths.is_empty() {
            segment_id = segment_id
                .checked_add(1)
                .ok_or_else(|| DBError::StorageError("WAL segment ID exhausted".into()))?;
        }
        let path = segment_path(&wal_dir, segment_id);
        let file = if paths.is_empty() || latest_version == LEGACY_WAL_VERSION {
            create_segment(&wal_dir, segment_id).await?
        } else {
            OpenOptions::new().append(true).open(path).await?
        };
        sealed.remove(&segment_id);
        let size = file.metadata().await?.len();
        Ok(Self {
            state: Mutex::new(WriterState {
                file,
                segment_id,
                size,
                next_lsn: analysis.latest_lsn + 1,
                poisoned: false,
                sealed,
                checkpoint,
            }),
            max_segment_size,
            wal_dir,
            raw_writes: std::sync::atomic::AtomicBool::new(false),
            checkpoint_owner,
            checkpoint_owner_registered: AtomicBool::new(false),
            checkpoint_token: OnceLock::new(),
            #[cfg(test)]
            sync_data_count: AtomicU64::new(0),
            #[cfg(test)]
            planned_update_count: AtomicU64::new(0),
        })
    }

    pub async fn write_log(
        &self,
        key: &Vec<u8>,
        value: &Vec<u8>,
        lsn: u64,
        record_type: RecordType,
    ) -> Result<u64, DBError> {
        self.write_transaction(vec![LogCommand::new(
            record_type,
            key.clone(),
            value.clone(),
            lsn,
        )])
        .await?;
        Ok(lsn)
    }

    pub async fn write_transaction(&self, commands: Vec<LogCommand>) -> Result<(), DBError> {
        self.raw_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.write_transaction_inner(commands).await
    }

    pub(crate) async fn write_engine_transaction(
        &self,
        commands: Vec<LogCommand>,
        owner: Option<&Arc<CheckpointOwner>>,
    ) -> Result<(), DBError> {
        self.append_engine_transactions(vec![commands], owner, ReceiptCollection::Skip)
            .await
            .map(|_| ())
    }

    pub(crate) async fn write_engine_transactions(
        &self,
        groups: Vec<Vec<LogCommand>>,
        owner: Option<&Arc<CheckpointOwner>>,
    ) -> Result<(), DBError> {
        self.append_engine_transactions(groups, owner, ReceiptCollection::Skip)
            .await
            .map(|_| ())
    }

    pub(crate) async fn append_engine_transactions_with_receipts(
        &self,
        groups: Vec<Vec<LogCommand>>,
        owner: Option<&Arc<CheckpointOwner>>,
    ) -> Result<Vec<Vec<WalValueRef>>, DBError> {
        self.append_engine_transactions(groups, owner, ReceiptCollection::Collect)
            .await
    }

    async fn append_engine_transactions(
        &self,
        groups: Vec<Vec<LogCommand>>,
        owner: Option<&Arc<CheckpointOwner>>,
        receipts: ReceiptCollection,
    ) -> Result<Vec<Vec<WalValueRef>>, DBError> {
        let owns_checkpoint = self
            .checkpoint_token
            .get()
            .is_some_and(|token| owner.is_some_and(|owner| Arc::ptr_eq(token, owner)));
        if !owns_checkpoint {
            self.mark_untracked_mutation();
        }
        self.append_transactions(groups, receipts).await
    }

    #[cfg(test)]
    pub(crate) fn sync_data_count(&self) -> u64 {
        self.sync_data_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(crate) fn planned_update_count(&self) -> u64 {
        self.planned_update_count
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn write_transaction_inner(&self, commands: Vec<LogCommand>) -> Result<(), DBError> {
        self.write_transactions_inner(vec![commands]).await
    }

    async fn write_transactions_inner(&self, groups: Vec<Vec<LogCommand>>) -> Result<(), DBError> {
        self.append_transactions(groups, ReceiptCollection::Skip)
            .await
            .map(|_| ())
    }

    async fn append_transactions(
        &self,
        groups: Vec<Vec<LogCommand>>,
        receipts: ReceiptCollection,
    ) -> Result<Vec<Vec<WalValueRef>>, DBError> {
        let collecting = receipts.collecting();
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        if groups.len() > MAX_GROUP_COUNT {
            return Err(DBError::StorageError(
                "WAL transaction group exceeds count limit".into(),
            ));
        }
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        let mut bytes = Vec::new();
        let mut next_lsn = state.next_lsn;
        let mut planned_transactions =
            Vec::with_capacity(if collecting { groups.len() } else { 0 });
        for commands in groups {
            if commands.is_empty() {
                return Err(DBError::StorageError(
                    "WAL transaction must contain an update".into(),
                ));
            }
            let transaction_id = next_lsn;
            let transaction_start_offset = u64::try_from(bytes.len())
                .map_err(|_| DBError::StorageError("WAL offset overflow".into()))?;
            let mut previous_lsn = 0;
            let mut group_size = 0usize;
            let mut updates = Vec::with_capacity(if collecting { commands.len() } else { 0 });
            for command in commands {
                if command.record_type == RecordType::Delete && !command.value.is_empty() {
                    return Err(DBError::StorageError("Delete contains a value".into()));
                }
                let record_type = command.record_type;
                let application_sequence = command.lsn;
                let key_length = u32::try_from(command.key.len())
                    .map_err(|_| DBError::StorageError("WAL key length overflow".into()))?;
                let value_length = u64::try_from(command.value.len())
                    .map_err(|_| DBError::StorageError("WAL value length overflow".into()))?;
                let (key_checksum, value_checksum) = if collecting {
                    (
                        crc32fast::hash(&command.key),
                        crc32fast::hash(&command.value),
                    )
                } else {
                    (0, 0)
                };
                let frame_offset = u64::try_from(bytes.len())
                    .map_err(|_| DBError::StorageError("WAL offset overflow".into()))?;
                let physical_lsn = next_lsn;
                let frame = encode_frame(&Frame {
                    lsn: next_lsn,
                    transaction_id,
                    previous_lsn,
                    record: TransactionRecord::Update(command),
                })?;
                let frame_length = u32::try_from(frame.len())
                    .map_err(|_| DBError::StorageError("WAL frame length overflow".into()))?;
                if collecting {
                    updates.push(PlannedUpdate {
                        transaction_id,
                        transaction_start_offset,
                        frame_offset,
                        frame_length,
                        physical_lsn,
                        application_sequence,
                        record_type,
                        key_length,
                        key_checksum,
                        value_length,
                        value_checksum,
                    });
                    #[cfg(test)]
                    self.planned_update_count
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                group_size = group_size
                    .checked_add(frame.len())
                    .ok_or_else(|| DBError::StorageError("WAL transaction size overflow".into()))?;
                bytes.extend(frame);
                if group_size > MAX_GROUP_ENCODED_SIZE {
                    return Err(DBError::StorageError(
                        "WAL transaction exceeds group limit".into(),
                    ));
                }
                previous_lsn = next_lsn;
                next_lsn = next_lsn
                    .checked_add(1)
                    .ok_or_else(|| DBError::StorageError("WAL LSN exhausted".into()))?;
            }
            let commit_offset = u64::try_from(bytes.len())
                .map_err(|_| DBError::StorageError("WAL offset overflow".into()))?;
            let commit_lsn = next_lsn;
            let commit = encode_frame(&Frame {
                lsn: next_lsn,
                transaction_id,
                previous_lsn,
                record: TransactionRecord::Commit,
            })?;
            group_size = group_size
                .checked_add(commit.len())
                .ok_or_else(|| DBError::StorageError("WAL transaction size overflow".into()))?;
            if group_size > MAX_GROUP_ENCODED_SIZE {
                return Err(DBError::StorageError(
                    "WAL transaction exceeds group limit".into(),
                ));
            }
            bytes.extend(commit);
            if collecting {
                planned_transactions.push(PlannedTransaction {
                    updates,
                    commit_offset,
                    commit_lsn,
                });
            }
            next_lsn = next_lsn
                .checked_add(1)
                .ok_or_else(|| DBError::StorageError("WAL LSN exhausted".into()))?;
        }
        state.poisoned = true;
        if state.size > WAL_HEADER_SIZE
            && state.size.saturating_add(bytes.len() as u64) > self.max_segment_size
        {
            self.rotate_locked(&mut state).await?;
        }
        let segment_id = state.segment_id;
        let segment_start = state.size;
        let receipts = planned_transactions
            .into_iter()
            .map(|transaction| {
                let commit_offset = segment_start
                    .checked_add(transaction.commit_offset)
                    .ok_or_else(|| DBError::StorageError("WAL offset overflow".into()))?;
                transaction
                    .updates
                    .into_iter()
                    .map(|update| {
                        Ok(WalValueRef {
                            segment_id,
                            transaction_id: update.transaction_id,
                            transaction_start_offset: segment_start
                                .checked_add(update.transaction_start_offset)
                                .ok_or_else(|| {
                                    DBError::StorageError("WAL offset overflow".into())
                                })?,
                            commit_offset,
                            commit_lsn: transaction.commit_lsn,
                            frame_offset: segment_start
                                .checked_add(update.frame_offset)
                                .ok_or_else(|| {
                                    DBError::StorageError("WAL offset overflow".into())
                                })?,
                            frame_length: update.frame_length,
                            physical_lsn: update.physical_lsn,
                            application_sequence: update.application_sequence,
                            record_type: update.record_type,
                            key_length: update.key_length,
                            key_checksum: update.key_checksum,
                            value_length: update.value_length,
                            value_checksum: update.value_checksum,
                        })
                    })
                    .collect::<Result<Vec<_>, DBError>>()
            })
            .collect::<Result<Vec<_>, DBError>>()?;
        state.file.write_all(&bytes).await?;
        state.file.sync_data().await?;
        #[cfg(test)]
        self.sync_data_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        state.size += bytes.len() as u64;
        state.next_lsn = next_lsn;
        state.poisoned = false;
        Ok(receipts)
    }

    pub async fn recover(
        &self,
        memtable: &mut SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    ) -> Result<(), DBError> {
        for record in self.recover_records().await? {
            let record_type = decode_record_type(record.record_type)?;
            memtable.insert(record.key, (record_type, record.value));
        }
        Ok(())
    }

    pub async fn recover_records(&self) -> Result<Vec<WALRecord>, DBError> {
        Ok(self.analyze_committed(false).await?.redo())
    }

    pub(crate) async fn recover_records_with_refs(
        &self,
    ) -> Result<Vec<RecoveredWalUpdate>, DBError> {
        self.analyze_committed(true).await?.redo_with_refs()
    }

    pub(crate) async fn preflight_recovery(wal_dir: &Path) -> Result<(u64, u64), DBError> {
        let paths = segment_paths(wal_dir, None).await?;
        let mut analysis = Analysis {
            track_refs: true,
            ..Analysis::default()
        };
        for (index, (_, path)) in paths.iter().enumerate() {
            let tail_handling = if index + 1 == paths.len() {
                TailHandling::Allow
            } else {
                TailHandling::Reject
            };
            scan_segment_with_tail_handling(path, tail_handling, &mut analysis).await?;
        }
        let next_physical_lsn = analysis
            .latest_lsn
            .checked_add(1)
            .ok_or_else(|| DBError::Corrupted("WAL physical LSN overflow".into()))?;
        let recovered_sequence = analysis
            .redo_with_refs()?
            .iter()
            .map(|update| update.record.lsn)
            .max()
            .unwrap_or(0);
        Ok((recovered_sequence, next_physical_lsn))
    }

    async fn analyze_committed(&self, track_refs: bool) -> Result<Analysis, DBError> {
        let state = self.state.lock().await;
        ensure_healthy(&state)?;
        let mut analysis = Analysis {
            track_refs,
            ..Analysis::default()
        };
        if let Some(checkpoint) = state.checkpoint {
            analysis.latest_lsn = checkpoint
                .next_physical_lsn
                .checked_sub(1)
                .ok_or_else(|| DBError::Corrupted("invalid checkpoint physical LSN".into()))?;
        }
        for (_, path) in segment_paths(
            &self.wal_dir,
            state
                .checkpoint
                .map(|checkpoint| checkpoint.replay_start_segment),
        )
        .await?
        {
            scan_segment(&path, false, &mut analysis).await?;
        }
        Ok(analysis)
    }

    pub(crate) async fn read_value(
        &self,
        reference: &WalValueRef,
        expected_key: &[u8],
        expected_application_sequence: u64,
    ) -> Result<Vec<u8>, DBError> {
        reference.validate()?;
        let path = segment_path(&self.wal_dir, reference.segment_id);
        let mut file = match File::open(&path).await {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(DBError::Corrupted(
                    "WAL value reference segment is missing".into(),
                ));
            }
            Err(error) => return Err(DBError::IO(error)),
        };
        let file_length = file.metadata().await?.len();
        if file_length < WAL_HEADER_SIZE {
            return Err(DBError::Corrupted(
                "WAL segment is shorter than its header".into(),
            ));
        }
        let header = read_exact_at(&mut file, 0, WAL_HEADER_SIZE as usize).await?;
        let header = WALHeader::decode(&header)?;
        if header.version != WAL_VERSION {
            return Err(DBError::StorageError(
                "Legacy WAL segments do not support value references".into(),
            ));
        }
        if header.last_checkpoint != 0 || header.last_checkpoint_offset != 0 {
            return Err(DBError::Corrupted(
                "Unsupported WAL checkpoint metadata".into(),
            ));
        }
        let commit_header_end = reference
            .commit_offset
            .checked_add(FRAME_HEADER_SIZE as u64)
            .ok_or_else(|| DBError::Corrupted("WAL value reference commit overflow".into()))?;
        if commit_header_end > file_length {
            return Err(DBError::Corrupted(
                "WAL value reference commit is outside the segment".into(),
            ));
        }
        let commit_header =
            read_exact_at(&mut file, reference.commit_offset, FRAME_HEADER_SIZE).await?;
        let (commit_lsn, commit_body_length) = decode_frame_header(&commit_header)?;
        if commit_lsn != reference.commit_lsn {
            return Err(DBError::Corrupted(
                "WAL value reference commit LSN mismatch".into(),
            ));
        }
        let commit_body_length = usize::try_from(commit_body_length)
            .map_err(|_| DBError::Corrupted("WAL commit length overflow".into()))?;
        if !(17..=MAX_FRAME_SIZE).contains(&commit_body_length) {
            return Err(DBError::Corrupted("Invalid WAL commit frame size".into()));
        }
        let commit_end = reference
            .commit_offset
            .checked_add(FRAME_HEADER_SIZE as u64)
            .and_then(|end| end.checked_add(commit_body_length as u64))
            .ok_or_else(|| DBError::Corrupted("WAL value reference commit overflow".into()))?;
        if commit_end > file_length {
            return Err(DBError::Corrupted(
                "WAL value reference commit is truncated".into(),
            ));
        }
        let frame_header =
            read_exact_at(&mut file, reference.frame_offset, FRAME_HEADER_SIZE).await?;
        let (frame_lsn, frame_body_length) = decode_frame_header(&frame_header)?;
        let frame_body_length = usize::try_from(frame_body_length)
            .map_err(|_| DBError::Corrupted("WAL frame length overflow".into()))?;
        if !(17..=MAX_FRAME_SIZE).contains(&frame_body_length) {
            return Err(DBError::Corrupted(
                "Invalid WAL value reference frame size".into(),
            ));
        }
        let frame_length = FRAME_HEADER_SIZE
            .checked_add(frame_body_length)
            .ok_or_else(|| DBError::Corrupted("WAL frame length overflow".into()))?;
        if frame_lsn != reference.physical_lsn
            || u32::try_from(frame_length).ok() != Some(reference.frame_length)
        {
            return Err(DBError::Corrupted(
                "WAL value reference frame mismatch".into(),
            ));
        }
        let span = commit_end
            .checked_sub(reference.transaction_start_offset)
            .ok_or_else(|| {
                DBError::Corrupted("WAL value reference transaction order is invalid".into())
            })?;
        if span > MAX_GROUP_ENCODED_SIZE as u64 {
            return Err(DBError::Corrupted(
                "WAL value reference transaction is too large".into(),
            ));
        }
        let span = usize::try_from(span)
            .map_err(|_| DBError::Corrupted("WAL transaction length overflow".into()))?;
        let transaction =
            read_exact_at(&mut file, reference.transaction_start_offset, span).await?;
        let mut cursor = 0usize;
        let mut previous_lsn = 0u64;
        let mut first_lsn = None;
        let mut selected = None;
        while cursor < transaction.len() {
            if transaction.len() - cursor < FRAME_HEADER_SIZE {
                return Err(DBError::Corrupted(
                    "Incomplete WAL transaction frame".into(),
                ));
            }
            let body_start = cursor + FRAME_HEADER_SIZE;
            let frame_header = &transaction[cursor..body_start];
            let (frame_lsn, frame_body_length) = decode_frame_header(frame_header)?;
            let frame_body_length = usize::try_from(frame_body_length)
                .map_err(|_| DBError::Corrupted("WAL frame length overflow".into()))?;
            if !(17..=MAX_FRAME_SIZE).contains(&frame_body_length) {
                return Err(DBError::Corrupted("Invalid WAL frame size".into()));
            }
            let frame_length = FRAME_HEADER_SIZE
                .checked_add(frame_body_length)
                .ok_or_else(|| DBError::Corrupted("WAL frame length overflow".into()))?;
            let frame_end = cursor
                .checked_add(frame_length)
                .ok_or_else(|| DBError::Corrupted("WAL frame length overflow".into()))?;
            if frame_end > transaction.len() {
                return Err(DBError::Corrupted(
                    "Incomplete WAL transaction frame".into(),
                ));
            }
            verify_frame_body(frame_header, &transaction[body_start..frame_end])?;
            let frame = decode_frame(frame_lsn, &transaction[body_start..frame_end])?;
            if frame.transaction_id != reference.transaction_id {
                return Err(DBError::Corrupted(
                    "WAL transaction identity mismatch".into(),
                ));
            }
            if first_lsn.is_some() {
                let expected_lsn = previous_lsn
                    .checked_add(1)
                    .ok_or_else(|| DBError::Corrupted("WAL transaction LSN overflow".into()))?;
                if frame.lsn != expected_lsn || frame.previous_lsn != previous_lsn {
                    return Err(DBError::Corrupted("Invalid WAL transaction chain".into()));
                }
            } else if frame.lsn != reference.transaction_id || frame.previous_lsn != 0 {
                return Err(DBError::Corrupted("Invalid WAL transaction start".into()));
            }
            first_lsn.get_or_insert(frame.lsn);
            let frame_offset = reference
                .transaction_start_offset
                .checked_add(cursor as u64)
                .ok_or_else(|| DBError::Corrupted("WAL offset overflow".into()))?;
            let is_update = matches!(&frame.record, TransactionRecord::Update(_));
            if is_update {
                if frame_end == transaction.len() {
                    return Err(DBError::Corrupted("WAL transaction has no commit".into()));
                }
                if frame_offset == reference.frame_offset {
                    if selected.is_some() {
                        return Err(DBError::Corrupted(
                            "WAL value reference frame is duplicated".into(),
                        ));
                    }
                    if frame.lsn != reference.physical_lsn
                        || u32::try_from(frame_length).ok() != Some(reference.frame_length)
                    {
                        return Err(DBError::Corrupted(
                            "WAL value reference frame mismatch".into(),
                        ));
                    }
                    if let TransactionRecord::Update(command) = frame.record {
                        selected = Some(command);
                    }
                }
            } else if frame_offset != reference.commit_offset
                || frame.lsn != reference.commit_lsn
                || frame_end != transaction.len()
            {
                return Err(DBError::Corrupted(
                    "WAL value reference commit mismatch".into(),
                ));
            }
            previous_lsn = frame.lsn;
            cursor = frame_end;
        }
        if cursor != transaction.len() || first_lsn != Some(reference.transaction_id) {
            return Err(DBError::Corrupted(
                "WAL transaction does not end at its commit".into(),
            ));
        }
        let Some(command) = selected else {
            return Err(DBError::Corrupted(
                "WAL value reference frame is missing".into(),
            ));
        };
        if command.record_type != RecordType::Put || reference.record_type != RecordType::Put {
            return Err(DBError::Corrupted(
                "WAL value reference is not a put".into(),
            ));
        }
        if command.lsn != reference.application_sequence
            || command.lsn != expected_application_sequence
        {
            return Err(DBError::Corrupted(
                "WAL value reference application sequence mismatch".into(),
            ));
        }
        if command.key != expected_key {
            return Err(DBError::Corrupted(
                "WAL value reference key mismatch".into(),
            ));
        }
        if u32::try_from(command.key.len()).ok() != Some(reference.key_length)
            || crc32fast::hash(&command.key) != reference.key_checksum
        {
            return Err(DBError::Corrupted(
                "WAL value reference key metadata mismatch".into(),
            ));
        }
        if u64::try_from(command.value.len()).ok() != Some(reference.value_length)
            || crc32fast::hash(&command.value) != reference.value_checksum
        {
            return Err(DBError::Corrupted(
                "WAL value reference value metadata mismatch".into(),
            ));
        }
        Ok(command.value)
    }

    pub(crate) async fn recovery_position(&self) -> Result<(u64, u64), DBError> {
        let state = self.state.lock().await;
        ensure_healthy(&state)?;
        Ok((state.segment_id, state.next_lsn))
    }

    pub(crate) async fn trusted_recovery_position(&self) -> Result<Option<(u64, u64)>, DBError> {
        let state = self.state.lock().await;
        ensure_healthy(&state)?;
        if self.raw_writes.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(None);
        }
        Ok(Some((state.segment_id, state.next_lsn)))
    }

    pub(crate) fn register_checkpoint_owner(&self) -> Option<Arc<CheckpointOwner>> {
        if !self.checkpoint_owner
            || self
                .checkpoint_owner_registered
                .compare_exchange(
                    false,
                    true,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                )
                .is_err()
        {
            return None;
        }
        let owner = Arc::new(CheckpointOwner { _private: () });
        let _ = self.checkpoint_token.set(Arc::clone(&owner));
        Some(owner)
    }

    pub(crate) fn owns_recovery_checkpoints(&self, owner: Option<&Arc<CheckpointOwner>>) -> bool {
        self.checkpoint_owner
            && self
                .checkpoint_token
                .get()
                .is_some_and(|token| owner.is_some_and(|owner| Arc::ptr_eq(token, owner)))
    }

    pub(crate) fn mark_untracked_mutation(&self) {
        self.raw_writes
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) async fn reclaim_checkpointed_segments(
        &self,
        checkpoint: RecoveryCheckpoint,
    ) -> Result<(), DBError> {
        if !self.checkpoint_owner {
            return Err(DBError::StorageError(
                "WAL manager has no engine checkpoint authority".into(),
            ));
        }
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        if checkpoint.replay_start_segment > state.segment_id
            || checkpoint.next_physical_lsn > state.next_lsn
        {
            return Err(DBError::StorageError(
                "WAL checkpoint is ahead of the writer state".into(),
            ));
        }
        state.checkpoint = Some(checkpoint);
        let mut entries = tokio::fs::read_dir(&self.wal_dir).await?;
        let mut covered = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let Some(id) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_suffix(".log"))
                .and_then(|id| id.parse::<u64>().ok())
            else {
                continue;
            };
            if id < checkpoint.replay_start_segment {
                covered.push((id, entry.path(), entry.file_type().await?));
            }
        }
        covered.sort_by_key(|(id, _, _)| *id);

        let mut removed_any = false;
        let mut cleanup_error = None;
        for (id, path, file_type) in covered {
            if file_type.is_symlink() || (!file_type.is_file() && !file_type.is_dir()) {
                continue;
            }
            if let Err(error) = tokio::fs::remove_file(path).await {
                cleanup_error = Some(DBError::IO(error));
                break;
            }
            state.sealed.remove(&id);
            removed_any = true;
        }
        if removed_any {
            File::open(&self.wal_dir).await?.sync_all().await?;
        }
        cleanup_error.map_or(Ok(()), Err)
    }

    async fn rotate_locked(&self, state: &mut WriterState) -> Result<(), DBError> {
        state.file.sync_all().await?;
        let next_id = state
            .segment_id
            .checked_add(1)
            .ok_or_else(|| DBError::StorageError("WAL segment ID exhausted".into()))?;
        let file = create_segment(&self.wal_dir, next_id).await?;
        state.sealed.insert(state.segment_id);
        state.file = file;
        state.segment_id = next_id;
        state.size = WAL_HEADER_SIZE;
        Ok(())
    }

    pub async fn rotate_wal_file(&self) -> Result<u64, DBError> {
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        state.poisoned = true;
        self.rotate_locked(&mut state).await?;
        state.poisoned = false;
        Ok(state.segment_id)
    }

    pub async fn change_to_reserve(&self, locked_wal: u64) -> Result<SegmentId, DBError> {
        let mut state = self.state.lock().await;
        ensure_healthy(&state)?;
        if !state.sealed.remove(&locked_wal) {
            return Err(DBError::StorageError("WAL segment is not sealed".into()));
        }
        Ok(SegmentId(AtomicU64::new(locked_wal)))
    }

    #[cfg(test)]
    async fn read_committed_file(path: PathBuf) -> Result<Vec<WALRecord>, DBError> {
        let mut analysis = Analysis::default();
        scan_segment(&path, false, &mut analysis).await?;
        Ok(analysis.redo())
    }
}

fn ensure_healthy(state: &WriterState) -> Result<(), DBError> {
    if state.poisoned {
        return Err(DBError::StorageError(
            "WAL write interrupted or failed; reopen before reuse".into(),
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TailHandling {
    Repair,
    Allow,
    Reject,
}

fn segment_path(directory: &Path, id: u64) -> PathBuf {
    directory.join(format!("{id:020}.log"))
}

async fn segment_paths(
    directory: &Path,
    floor: Option<u64>,
) -> Result<Vec<(u64, PathBuf)>, DBError> {
    let mut entries = tokio::fs::read_dir(directory).await?;
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if let Some(id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_suffix(".log"))
            .and_then(|id| id.parse::<u64>().ok())
        {
            if floor.is_some_and(|floor| id < floor) {
                continue;
            }
            if entry.path() != segment_path(directory, id) {
                return Err(DBError::Corrupted(
                    "WAL segment filename is not canonical".into(),
                ));
            }
            if !entry.file_type().await?.is_file() {
                return Err(DBError::Corrupted(
                    "WAL segment is not a regular file".into(),
                ));
            }
            paths.push((id, entry.path()));
        }
    }
    paths.sort_by_key(|(id, _)| *id);
    let first = floor.unwrap_or_else(|| paths.first().map_or(0, |(id, _)| *id));
    paths.retain(|(id, _)| *id >= first);
    if floor.is_some() && paths.first().map(|(id, _)| *id) != floor {
        return Err(DBError::Corrupted("Missing WAL checkpoint segment".into()));
    }
    if paths
        .windows(2)
        .any(|pair| pair[0].0 == pair[1].0 || pair[0].0.checked_add(1) != Some(pair[1].0))
    {
        return Err(DBError::Corrupted(
            "Duplicate or missing WAL segment".into(),
        ));
    }
    Ok(paths)
}

async fn create_segment(directory: &Path, id: u64) -> Result<File, DBError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(segment_path(directory, id))
        .await?;
    file.write_all(&WALHeader::default().encode()).await?;
    file.sync_all().await?;
    File::open(directory).await?.sync_all().await?;
    Ok(file)
}

fn encode_frame(frame: &Frame) -> Result<Vec<u8>, DBError> {
    if let TransactionRecord::Update(command) = &frame.record {
        let size = 42usize
            .checked_add(command.key.len())
            .and_then(|size| size.checked_add(command.value.len()));
        if size.is_none_or(|size| size > MAX_FRAME_SIZE) {
            return Err(DBError::StorageError("WAL frame exceeds 64 MiB".into()));
        }
    }
    let mut body = Vec::new();
    body.push(match frame.record {
        TransactionRecord::Update(_) => 1,
        TransactionRecord::Commit => 2,
    });
    body.extend_from_slice(&frame.transaction_id.to_be_bytes());
    body.extend_from_slice(&frame.previous_lsn.to_be_bytes());
    if let TransactionRecord::Update(command) = &frame.record {
        body.push(command.record_type as u8);
        body.extend_from_slice(&command.lsn.to_be_bytes());
        body.extend_from_slice(&(command.key.len() as u64).to_be_bytes());
        body.extend_from_slice(&(command.value.len() as u64).to_be_bytes());
        body.extend_from_slice(&command.key);
        body.extend_from_slice(&command.value);
    }
    if body.len() > MAX_FRAME_SIZE {
        return Err(DBError::StorageError("WAL frame exceeds 64 MiB".into()));
    }
    let mut bytes = Vec::with_capacity(FRAME_HEADER_SIZE + body.len());
    bytes.extend_from_slice(&frame.lsn.to_be_bytes());
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&bytes);
    hasher.update(&body);
    bytes.extend_from_slice(&hasher.finalize().to_be_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

struct Decoder<'a> {
    remaining: &'a [u8],
}

impl<'a> Decoder<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], DBError> {
        if length > self.remaining.len() {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        let (value, remaining) = self.remaining.split_at(length);
        self.remaining = remaining;
        Ok(value)
    }

    fn number(&mut self) -> Result<u64, DBError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }

    fn payload(&mut self, length: u64) -> Result<Vec<u8>, DBError> {
        let length = usize::try_from(length)
            .map_err(|_| DBError::Corrupted("WAL length overflow".into()))?;
        Ok(self.take(length)?.to_vec())
    }
}

fn decode_record_type(value: u8) -> Result<RecordType, DBError> {
    match value {
        1 => Ok(RecordType::Put),
        2 => Ok(RecordType::Delete),
        _ => Err(DBError::Corrupted("Invalid WAL operation".into())),
    }
}

impl WalValueRef {
    pub(crate) fn encode(&self) -> [u8; WAL_VALUE_REF_ENCODED_SIZE] {
        let mut bytes = [0u8; WAL_VALUE_REF_ENCODED_SIZE];
        bytes[0] = WAL_VALUE_REF_VERSION;
        bytes[1..9].copy_from_slice(&self.segment_id.to_be_bytes());
        bytes[9..17].copy_from_slice(&self.transaction_id.to_be_bytes());
        bytes[17..25].copy_from_slice(&self.transaction_start_offset.to_be_bytes());
        bytes[25..33].copy_from_slice(&self.commit_offset.to_be_bytes());
        bytes[33..41].copy_from_slice(&self.commit_lsn.to_be_bytes());
        bytes[41..49].copy_from_slice(&self.frame_offset.to_be_bytes());
        bytes[49..53].copy_from_slice(&self.frame_length.to_be_bytes());
        bytes[53..61].copy_from_slice(&self.physical_lsn.to_be_bytes());
        bytes[61..69].copy_from_slice(&self.application_sequence.to_be_bytes());
        bytes[69] = self.record_type as u8;
        bytes[70..74].copy_from_slice(&self.key_length.to_be_bytes());
        bytes[74..78].copy_from_slice(&self.key_checksum.to_be_bytes());
        bytes[78..86].copy_from_slice(&self.value_length.to_be_bytes());
        bytes[86..90].copy_from_slice(&self.value_checksum.to_be_bytes());
        let checksum = crc32fast::hash(&bytes[..WAL_VALUE_REF_ENCODED_SIZE - 4]);
        bytes[WAL_VALUE_REF_ENCODED_SIZE - 4..].copy_from_slice(&checksum.to_be_bytes());
        bytes
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        if bytes.len() != WAL_VALUE_REF_ENCODED_SIZE {
            return Err(DBError::Corrupted(
                "Invalid WAL value reference length".into(),
            ));
        }
        if bytes[0] != WAL_VALUE_REF_VERSION {
            return Err(DBError::Corrupted(
                "Unsupported WAL value reference version".into(),
            ));
        }
        let checksum = be_u32(bytes, WAL_VALUE_REF_ENCODED_SIZE - 4)?;
        if crc32fast::hash(&bytes[..WAL_VALUE_REF_ENCODED_SIZE - 4]) != checksum {
            return Err(DBError::Corrupted(
                "WAL value reference checksum mismatch".into(),
            ));
        }
        let reference = Self {
            segment_id: be_u64(bytes, 1)?,
            transaction_id: be_u64(bytes, 9)?,
            transaction_start_offset: be_u64(bytes, 17)?,
            commit_offset: be_u64(bytes, 25)?,
            commit_lsn: be_u64(bytes, 33)?,
            frame_offset: be_u64(bytes, 41)?,
            frame_length: be_u32(bytes, 49)?,
            physical_lsn: be_u64(bytes, 53)?,
            application_sequence: be_u64(bytes, 61)?,
            record_type: decode_record_type(bytes[69])?,
            key_length: be_u32(bytes, 70)?,
            key_checksum: be_u32(bytes, 74)?,
            value_length: be_u64(bytes, 78)?,
            value_checksum: be_u32(bytes, 86)?,
        };
        reference.validate()?;
        Ok(reference)
    }

    fn validate(&self) -> Result<(), DBError> {
        if self.transaction_id == 0
            || self.physical_lsn < self.transaction_id
            || self.commit_lsn <= self.physical_lsn
        {
            return Err(DBError::Corrupted(
                "Invalid WAL value reference LSNs".into(),
            ));
        }
        if self.transaction_start_offset < WAL_HEADER_SIZE
            || self.commit_offset <= self.transaction_start_offset
            || self.frame_offset < self.transaction_start_offset
        {
            return Err(DBError::Corrupted(
                "Invalid WAL value reference offsets".into(),
            ));
        }
        let frame_end = self
            .frame_offset
            .checked_add(u64::from(self.frame_length))
            .ok_or_else(|| DBError::Corrupted("WAL value reference overflow".into()))?;
        if frame_end > self.commit_offset {
            return Err(DBError::Corrupted(
                "WAL value reference frame is not before its commit".into(),
            ));
        }
        let frame_length = usize::try_from(self.frame_length)
            .map_err(|_| DBError::Corrupted("WAL frame length overflow".into()))?;
        let key_length = usize::try_from(self.key_length)
            .map_err(|_| DBError::Corrupted("WAL key length overflow".into()))?;
        let value_length = usize::try_from(self.value_length)
            .map_err(|_| DBError::Corrupted("WAL value length overflow".into()))?;
        let body_length = 42usize
            .checked_add(key_length)
            .and_then(|length| length.checked_add(value_length))
            .ok_or_else(|| DBError::Corrupted("WAL value reference length overflow".into()))?;
        if body_length > MAX_FRAME_SIZE {
            return Err(DBError::Corrupted(
                "WAL value reference payload is too large".into(),
            ));
        }
        if frame_length != FRAME_HEADER_SIZE + body_length
            || !(MIN_WAL_UPDATE_FRAME_SIZE..=MAX_WAL_UPDATE_FRAME_SIZE).contains(&frame_length)
        {
            return Err(DBError::Corrupted(
                "Invalid WAL value reference frame length".into(),
            ));
        }
        if self.record_type == RecordType::Delete && self.value_length != 0 {
            return Err(DBError::Corrupted(
                "Delete WAL value reference has a value".into(),
            ));
        }
        Ok(())
    }
}

fn be_u32(bytes: &[u8], offset: usize) -> Result<u32, DBError> {
    Ok(u32::from_be_bytes(bytes[offset..offset + 4].try_into()?))
}

fn be_u64(bytes: &[u8], offset: usize) -> Result<u64, DBError> {
    Ok(u64::from_be_bytes(bytes[offset..offset + 8].try_into()?))
}

fn decode_frame_header(bytes: &[u8]) -> Result<(u64, u32), DBError> {
    let checksum = be_u32(bytes, 16)?;
    if checksum != crc32fast::hash(&bytes[..16]) {
        return Err(DBError::Corrupted(
            "WAL frame header checksum mismatch".into(),
        ));
    }
    Ok((be_u64(bytes, 0)?, be_u32(bytes, 8)?))
}

fn verify_frame_body(header: &[u8], body: &[u8]) -> Result<u32, DBError> {
    let checksum = be_u32(header, 12)?;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[..12]);
    hasher.update(body);
    if checksum != hasher.finalize() {
        return Err(DBError::Corrupted("WAL frame checksum mismatch".into()));
    }
    Ok(checksum)
}

async fn read_exact_at(file: &mut File, offset: u64, length: usize) -> Result<Vec<u8>, DBError> {
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let mut bytes = vec![0u8; length];
    file.read_exact(&mut bytes).await?;
    Ok(bytes)
}

fn segment_id_from_path(path: &Path) -> Result<u64, DBError> {
    path.file_stem()
        .and_then(|name| name.to_str())
        .and_then(|name| name.parse::<u64>().ok())
        .ok_or_else(|| DBError::Corrupted("Invalid WAL segment filename".into()))
}

fn decode_frame(lsn: u64, body: &[u8]) -> Result<Frame, DBError> {
    let mut decoder = Decoder { remaining: body };
    let kind = decoder.take(1)?[0];
    let transaction_id = decoder.number()?;
    let previous_lsn = decoder.number()?;
    let record = match kind {
        1 => {
            let record_type = decode_record_type(decoder.take(1)?[0])?;
            let command_lsn = decoder.number()?;
            let key_length = decoder.number()?;
            let value_length = decoder.number()?;
            let key = decoder.payload(key_length)?;
            let value = decoder.payload(value_length)?;
            if record_type == RecordType::Delete && !value.is_empty() {
                return Err(DBError::Corrupted("Delete contains a value".into()));
            }
            TransactionRecord::Update(LogCommand::new(record_type, key, value, command_lsn))
        }
        2 => TransactionRecord::Commit,
        _ => return Err(DBError::Corrupted("Invalid WAL transaction record".into())),
    };
    if !decoder.remaining.is_empty() {
        return Err(DBError::Corrupted("Trailing WAL record bytes".into()));
    }
    Ok(Frame {
        lsn,
        transaction_id,
        previous_lsn,
        record,
    })
}

async fn scan_segment(
    path: &Path,
    repair_tail: bool,
    analysis: &mut Analysis,
) -> Result<u64, DBError> {
    let tail_handling = if repair_tail {
        TailHandling::Repair
    } else {
        TailHandling::Reject
    };
    scan_segment_with_tail_handling(path, tail_handling, analysis).await
}

async fn scan_segment_with_tail_handling(
    path: &Path,
    tail_handling: TailHandling,
    analysis: &mut Analysis,
) -> Result<u64, DBError> {
    let mut bytes = tokio::fs::read(path).await?;
    if bytes.len() < WAL_HEADER_SIZE as usize && tail_handling != TailHandling::Reject {
        let header = [WAL_VERSION, LEGACY_WAL_VERSION]
            .into_iter()
            .filter(|version| *version != LEGACY_WAL_VERSION || analysis.latest_lsn == 0)
            .map(|version| {
                WALHeader {
                    version,
                    ..WALHeader::default()
                }
                .encode()
            })
            .find(|header| header.starts_with(&bytes))
            .ok_or_else(|| DBError::Corrupted("Invalid incomplete WAL header".into()))?;
        if tail_handling == TailHandling::Repair {
            let mut file = OpenOptions::new().append(true).open(path).await?;
            file.write_all(&header[bytes.len()..]).await?;
            file.sync_all().await?;
            File::open(path.parent().ok_or_else(|| {
                DBError::StorageError("WAL segment has no parent directory".into())
            })?)
            .await?
            .sync_all()
            .await?;
        }
        bytes = header;
    }
    let header = WALHeader::decode(&bytes)?;
    if header.version == LEGACY_WAL_VERSION && analysis.latest_lsn != 0 {
        return Err(DBError::Corrupted(
            "Legacy WAL after transactional segment".into(),
        ));
    }
    if header.last_checkpoint != 0 || header.last_checkpoint_offset != 0 {
        return Err(DBError::Corrupted(
            "Unsupported WAL checkpoint metadata".into(),
        ));
    }
    let segment_id = analysis
        .track_refs
        .then(|| segment_id_from_path(path))
        .transpose()?;
    let mut offset = WAL_HEADER_SIZE as usize;
    while offset < bytes.len() {
        let start = offset;
        if header.version == LEGACY_WAL_VERSION {
            match decode_legacy(&bytes[offset..]) {
                Ok((record, length)) => {
                    analysis.legacy.push(record);
                    offset += length;
                }
                Err(DBError::IO(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(error) => return Err(error),
            }
        } else {
            if bytes.len() - offset < FRAME_HEADER_SIZE {
                break;
            }
            let frame_header = &bytes[offset..offset + FRAME_HEADER_SIZE];
            let (lsn, length) = decode_frame_header(frame_header)?;
            let length = usize::try_from(length)
                .map_err(|_| DBError::Corrupted("WAL frame length overflow".into()))?;
            if !(17..=MAX_FRAME_SIZE).contains(&length) {
                return Err(DBError::Corrupted("Invalid WAL frame size".into()));
            }
            if bytes.len() - offset - FRAME_HEADER_SIZE < length {
                break;
            }
            let end = offset + FRAME_HEADER_SIZE + length;
            let body = &bytes[offset + FRAME_HEADER_SIZE..end];
            let checksum = verify_frame_body(frame_header, body)?;
            let frame = decode_frame(lsn, body)
                .map_err(|error| DBError::Corrupted(format!("Invalid WAL frame: {error}")))?;
            analysis.accept(
                frame,
                checksum,
                FramePosition {
                    segment_id: segment_id.unwrap_or(0),
                    offset: u64::try_from(offset)
                        .map_err(|_| DBError::Corrupted("WAL offset overflow".into()))?,
                    length: u32::try_from(end - offset)
                        .map_err(|_| DBError::Corrupted("WAL frame length overflow".into()))?,
                },
            )?;
            offset = end;
        }
        if offset == start {
            break;
        }
    }
    if offset != bytes.len() {
        match tail_handling {
            TailHandling::Repair => {
                let file = OpenOptions::new().write(true).open(path).await?;
                file.set_len(offset as u64).await?;
                file.sync_all().await?;
            }
            TailHandling::Allow => {}
            TailHandling::Reject => {
                return Err(DBError::Corrupted(
                    "Incomplete WAL frame in sealed segment".into(),
                ));
            }
        }
    }
    Ok(header.version)
}

fn decode_legacy(bytes: &[u8]) -> Result<(WALRecord, usize), DBError> {
    let mut decoder = Decoder { remaining: bytes };
    let record_type = decoder.take(1)?[0];
    decode_record_type(record_type)?;
    let lsn = decoder.number()?;
    let key_length = decoder.number()?;
    let key = decoder.payload(key_length)?;
    let value_length = decoder.number()?;
    let value = decoder.payload(value_length)?;
    let payload_length = bytes.len() - decoder.remaining.len();
    let checksum = u32::from_be_bytes(decoder.take(4)?.try_into()?);
    if crc32fast::hash(&bytes[..payload_length]) != checksum {
        return Err(DBError::Corrupted("Legacy WAL checksum mismatch".into()));
    }
    Ok((
        WALRecord {
            record_type,
            lsn,
            key,
            value,
            checksum,
        },
        bytes.len() - decoder.remaining.len(),
    ))
}

/// WAL file header structure (32 Bytes + 4 bytes checksum)
#[derive(Debug)]
pub struct WALHeader {
    pub magic: [u8; 8],              // Magic number: "WALMGIC\0"
    pub version: u64,                // WAL format version
    pub last_checkpoint: u64,        // LSN of last checkpoint
    pub last_checkpoint_offset: u64, // Reserved for future use
}

impl WALHeader {
    /// Encode header to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint_offset.to_be_bytes());

        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf);

        buf.extend_from_slice(&hash.finalize().to_be_bytes());

        buf
    }

    /// Decode header from bytes
    pub fn decode(buf: &[u8]) -> Result<Self, DBError> {
        if buf.len() < WAL_HEADER_SIZE as usize {
            return Err(DBError::Corrupted(
                "Buffer too short for WAL header".to_string(),
            ));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        if &magic != WAL_MAGIC {
            return Err(DBError::Corrupted("Invalid WAL magic number".to_string()));
        }

        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[8..16]);
        let version = u64::from_be_bytes(b);
        if version != LEGACY_WAL_VERSION && version != WAL_VERSION {
            return Err(DBError::Corrupted("Unsupported WAL version".into()));
        }

        b.copy_from_slice(&buf[16..24]);
        let last_checkpoint = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[24..32]);
        let last_checkpoint_offset = u64::from_be_bytes(b);

        let mut checksum_data = [0u8; 4];
        checksum_data.copy_from_slice(&buf[32..36]);
        let checksum = u32::from_be_bytes(checksum_data);

        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf[0..32]);

        if checksum != hash.finalize() {
            return Err(DBError::Corrupted(
                "WAL header checksum mismatch".to_string(),
            ));
        }

        Ok(WALHeader {
            magic,
            version,
            last_checkpoint,
            last_checkpoint_offset,
        })
    }
}

impl Default for WALHeader {
    fn default() -> Self {
        WALHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_checkpoint: 0,
            last_checkpoint_offset: 0,
        }
    }
}

#[async_trait::async_trait]
impl LogStore for WALManager {
    async fn append(&self, command: LogCommand) -> Result<LogPosition, DBError> {
        let lsn = command.lsn;
        self.write_transaction(vec![command]).await?;
        Ok(LogPosition { lsn })
    }

    async fn recover_commands(&self) -> Result<Vec<LogCommand>, DBError> {
        self.recover_records()
            .await?
            .into_iter()
            .map(|record| {
                Ok(LogCommand::new(
                    decode_record_type(record.record_type)?,
                    record.key,
                    record.value,
                    record.lsn,
                ))
            })
            .collect()
    }

    async fn rotate(&self) -> Result<u64, DBError> {
        self.rotate_wal_file().await
    }

    async fn mark_reserved(&self, segment_id: u64) -> Result<(), DBError> {
        self.change_to_reserve(segment_id).await?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;
    use tokio::io::AsyncReadExt;

    use super::*;

    #[tokio::test]
    async fn initialize_new_file_segment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string()); // make sure to use a unique temp directory for each test run
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        assert_eq!(wal_manager.state.lock().await.segment_id, 0);
        assert_eq!(
            wal_manager
                .state
                .lock()
                .await
                .file
                .metadata()
                .await
                .unwrap()
                .len(),
            WAL_HEADER_SIZE as u64
        );

        // Clean up
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn checkpoint_replay_ignores_retained_prefix_and_preserves_physical_lsn() {
        let directory = tempfile::tempdir().unwrap();
        let wal_dir = directory.path().to_path_buf();
        let wal = WALManager::new(wal_dir.clone(), 1024 * 1024).await.unwrap();
        wal.write_log(&b"old".to_vec(), &b"prefix".to_vec(), 1, RecordType::Put)
            .await
            .unwrap();
        wal.rotate_wal_file().await.unwrap();
        let (replay_start_segment, next_physical_lsn) = wal.recovery_position().await.unwrap();
        drop(wal);
        tokio::fs::write(segment_path(&wal_dir, 0), b"corrupted retained prefix")
            .await
            .unwrap();

        let checkpoint = RecoveryCheckpoint {
            replay_start_segment,
            next_physical_lsn,
            committed_sequence: 1,
        };
        let wal = WALManager::new_with_checkpoint(wal_dir.clone(), 1024 * 1024, Some(checkpoint))
            .await
            .unwrap();
        assert!(wal.recover_records().await.unwrap().is_empty());
        wal.write_log(
            &b"new".to_vec(),
            &b"after-checkpoint".to_vec(),
            2,
            RecordType::Put,
        )
        .await
        .unwrap();
        assert_eq!(wal.state.lock().await.next_lsn, next_physical_lsn + 2);
        assert_eq!(wal.recover_records().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_replay_rejects_missing_segment_after_floor() {
        let directory = tempfile::tempdir().unwrap();
        let wal_dir = directory.path().to_path_buf();
        let wal = WALManager::new(wal_dir.clone(), 1024 * 1024).await.unwrap();
        wal.write_log(&b"base".to_vec(), &b"value".to_vec(), 1, RecordType::Put)
            .await
            .unwrap();
        wal.rotate_wal_file().await.unwrap();
        let (replay_start_segment, next_physical_lsn) = wal.recovery_position().await.unwrap();
        let checkpoint = RecoveryCheckpoint {
            replay_start_segment,
            next_physical_lsn,
            committed_sequence: 1,
        };
        wal.write_log(&b"one".to_vec(), &b"value".to_vec(), 2, RecordType::Put)
            .await
            .unwrap();
        wal.rotate_wal_file().await.unwrap();
        wal.write_log(&b"two".to_vec(), &b"value".to_vec(), 3, RecordType::Put)
            .await
            .unwrap();
        wal.rotate_wal_file().await.unwrap();
        drop(wal);
        tokio::fs::remove_file(segment_path(&wal_dir, replay_start_segment + 1))
            .await
            .unwrap();
        assert!(
            WALManager::new_with_checkpoint(wal_dir, 1024 * 1024, Some(checkpoint))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn write_log_creates_new_segment() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        println!("Testing WALManager write_log with wal_dir: {:?}", wal_dir);

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write a log entry (no rotation triggered with large segment size)
        let key = b"key".to_vec();
        let value = vec![0u8; 2048];
        let record_type = RecordType::Put;

        let lsn = wal_manager
            .write_log(&key, &value, 1, record_type)
            .await
            .unwrap();
        assert_eq!(lsn, 1);

        // Segment ID should remain at 0 (no rotation with 1MB segment size)
        assert_eq!(wal_manager.state.lock().await.segment_id, 0);

        // check the attached file, is it contains the log entry we just wrote?
        let wal_file_path = wal_dir.join(format!(
            "{:020}.log",
            wal_manager.state.lock().await.segment_id
        ));
        let mut wal_file = File::open(&wal_file_path).await.unwrap();

        // read from the header_last_offset to the end of the file
        let mut buf = Vec::new();
        wal_file.read_to_end(&mut buf).await.unwrap();

        // parse the header first...
        let header = WALHeader::decode(&buf[0..WAL_HEADER_SIZE as usize].to_vec()).unwrap();
        assert_eq!(header.magic, *WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);
        assert_eq!(header.last_checkpoint_offset, 0);

        // verify the log entry content
        let log_entry = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();
        assert_eq!(log_entry.len(), 1);
        assert_eq!(log_entry[0].record_type, record_type as u8);
        assert_eq!(log_entry[0].key, key);
        assert_eq!(log_entry[0].value, value);

        // Clean up
        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_single_record() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        let lsn = wal_manager
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        // Verify LSN starts from 1
        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, value);
        assert_eq!(records[0].record_type, RecordType::Put as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_multiple_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write multiple records
        let mut expected_lsns = vec![];
        for i in 0..5 {
            let key = format!("key_{}", i).into_bytes();
            let value = format!("value_{}", i).into_bytes();
            let lsn = wal_manager
                .write_log(&key, &value, 1, RecordType::Put)
                .await
                .unwrap();
            expected_lsns.push(lsn);
        }

        // Read back and verify all records
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.key, format!("key_{}", i).into_bytes());
            assert_eq!(record.value, format!("value_{}", i).into_bytes());
            assert_eq!(record.lsn, (1) as u64);
        }

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_delete_record() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"delete_me".to_vec();

        let lsn = wal_manager
            .write_log(&key, &vec![], 1, RecordType::Delete)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![] as Vec<u8>);
        assert_eq!(records[0].record_type, RecordType::Delete as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_empty_value() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let key = b"key_with_empty_value".to_vec();
        let value = vec![];

        let lsn = wal_manager
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, key);
        assert_eq!(records[0].value, vec![] as Vec<u8>);
        assert_eq!(records[0].record_type, RecordType::Put as u8);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn write_log_large_key_and_value() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        // Write a record with large key and value
        let key = vec![0u8; 10000]; // 10KB key
        let value = vec![1u8; 50000]; // 50KB value

        let lsn = wal_manager
            .write_log(&key, &value, 1, RecordType::Put)
            .await
            .unwrap();

        assert_eq!(lsn, 1);

        // Read back and verify
        let wal_file_path = wal_dir.join("00000000000000000000.log");
        let records = WALManager::read_committed_file(wal_file_path)
            .await
            .unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key.len(), 10000);
        assert_eq!(records[0].value.len(), 50000);
        assert!(records[0].key.iter().all(|&b| b == 0));
        assert!(records[0].value.iter().all(|&b| b == 1));

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_empty_wal() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Create an empty WAL directory
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();

        let mut memtable = SkipMap::new();
        let result = wal_manager.recover(&mut memtable).await;
        assert!(result.is_ok());
        assert_eq!(memtable.len(), 0);

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_with_put_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write some records first
        {
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            wal_manager
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key2".to_vec(), &b"value2".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            wal_manager
                .write_log(&b"key3".to_vec(), &b"value3".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
        }

        // Recover into a new memtable
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager.recover(&mut memtable).await.unwrap();

        // Verify all records were recovered
        assert_eq!(memtable.len(), 3);

        let binding1 = memtable.get(&b"key1".to_vec()).unwrap();
        let (rt1, val1) = binding1.value();
        assert_eq!(*rt1, RecordType::Put);
        assert_eq!(val1, &b"value1".to_vec());

        let binding2 = memtable.get(&b"key2".to_vec()).unwrap();
        let (rt2, val2) = binding2.value();
        assert_eq!(*rt2, RecordType::Put);
        assert_eq!(val2, &b"value2".to_vec());

        let binding3 = memtable.get(&b"key3".to_vec()).unwrap();
        let (rt3, val3) = binding3.value();
        assert_eq!(*rt3, RecordType::Put);
        assert_eq!(val3, &b"value3".to_vec());

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_with_delete_records() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write put and delete records
        {
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            // Put a key
            wal_manager
                .write_log(&b"key1".to_vec(), &b"value1".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
            // Delete it
            wal_manager
                .write_log(&b"key1".to_vec(), &b"".to_vec(), 1, RecordType::Delete)
                .await
                .unwrap();
        }

        // Recover
        let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
            .await
            .unwrap();
        let mut memtable = SkipMap::new();
        wal_manager.recover(&mut memtable).await.unwrap();

        // The key should exist with a Delete record type (empty value)
        assert_eq!(memtable.len(), 1);
        let binding = memtable.get(&b"key1".to_vec()).unwrap();
        let (rt, val) = binding.value();
        assert_eq!(*rt, RecordType::Delete);
        assert!(val.is_empty());

        std::fs::remove_dir_all(wal_dir).unwrap();
    }

    #[tokio::test]
    async fn recover_checksum_verification() {
        let wal_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&wal_dir).unwrap();

        // Write a valid record
        {
            let wal_manager = super::WALManager::new(wal_dir.clone(), 1024 * 1024)
                .await
                .unwrap();

            wal_manager
                .write_log(&b"key".to_vec(), &b"value".to_vec(), 1, RecordType::Put)
                .await
                .unwrap();
        }

        // Corrupt the WAL file by modifying a byte in the data
        {
            // Use zero-padded segment ID format (20 digits)
            let wal_file_path = wal_dir.join("00000000000000000000.log");
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&wal_file_path)
                .unwrap();
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(65)).unwrap();
            file.write_all(&[0xFF]).unwrap();
        }

        // Recover should fail due to checksum mismatch
        let result = super::WALManager::new(wal_dir.clone(), 1024 * 1024).await;
        assert!(matches!(result, Err(DBError::Corrupted(_))));

        std::fs::remove_dir_all(wal_dir).unwrap();
    }
}

#[cfg(test)]
#[path = "wal_tests.rs"]
mod crash_tests;
