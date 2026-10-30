//! WriteComponent - Handles checkpointing operations
//!
//! Responsible for:
//! 1. Converting memtable to SSTable
//! 2. Writing SSTable to disk
//! 3. Managing checkpoint metadata

use std::{path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;
use tokio::io::AsyncWriteExt;

use super::admission::{AdmissionQueue, MAX_DRAIN_BYTES, MAX_DRAIN_REQUESTS};
use crate::api::api::BatchOperation;
use crate::error::DBError;
use crate::storage::{
    bloom::BloomFilterWrapper,
    config::StorageConfig,
    experimental::{ReferenceValue, RootLock},
    index::SparseIndexEntry,
    manifest_codec::{ManifestManager, RecoveryCheckpoint, SSTableMeta},
    record::{MemtableRecord, RecordType},
    recovermanager::log_store::LogCommand,
    recovermanager::wal::{WALManager, WalValueRef},
    sstable_codec::SSTableCodec,
    writemanager::block::{Block, BlockBuilder, BlockBuilderState},
};

const MAX_WAL_FRAME_BYTES: usize = 64 * 1024 * 1024;
const MAX_WAL_TRANSACTION_BYTES: usize = 256 * 1024 * 1024;

/// Result of a checkpoint operation
#[derive(Debug, Clone)]
pub struct CheckpointResult {
    /// Path to the written SSTable file
    pub sstable_path: String,
    /// Number of records flushed
    pub record_count: usize,
    /// Level at which SSTable was written
    pub level: u32,
    /// File ID used for the SSTable
    pub file_id: u64,

    pub data: Vec<u8>,
}

/// WriteComponent handles checkpoint operations
///
/// Responsibilities:
/// - Convert memtable (SkipMap) to SSTable
/// - Write SSTable to disk
/// - Return checkpoint metadata
/// WriteComponent handles checkpoint operations
///
/// Responsibilities:
/// - Convert memtable (SkipMap) to SSTable
/// - Write SSTable to disk
/// - Return checkpoint metadata
pub struct WriteComponent {
    /// Base directory for SSTable storage
    /// MemTable key is (key, lsn) composite for MVCC support (ADR-0002)
    memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,

    /// Memtable size tracking (shared via Arc for Clone support).
    memtable_size: Arc<std::sync::atomic::AtomicUsize>,
    sequence_number: Arc<std::sync::atomic::AtomicU64>,
    writer_lock: Arc<tokio::sync::Mutex<()>>,
    admission: AdmissionQueue,
    checkpoint_owner: Option<Arc<crate::storage::recovermanager::wal::CheckpointOwner>>,
    payload_mode: PayloadMode,

    sstable_dir: PathBuf,
    wal_manager: Arc<WALManager>,
    manifest_manager: Arc<ManifestManager>,
    config: Arc<StorageConfig>,
}

impl Clone for WriteComponent {
    fn clone(&self) -> Self {
        Self {
            memtable: Arc::clone(&self.memtable),
            memtable_size: Arc::clone(&self.memtable_size),
            sequence_number: Arc::clone(&self.sequence_number),
            writer_lock: Arc::clone(&self.writer_lock),
            admission: self.admission.clone(),
            checkpoint_owner: self.checkpoint_owner.clone(),
            payload_mode: self.payload_mode.clone(),
            sstable_dir: self.sstable_dir.clone(),
            wal_manager: Arc::clone(&self.wal_manager),
            manifest_manager: Arc::clone(&self.manifest_manager),
            config: Arc::clone(&self.config),
        }
    }
}

#[derive(Clone)]
pub(crate) enum PayloadMode {
    Inline,
    WalReference {
        generation: u64,
        #[allow(dead_code)]
        root_lock: Arc<RootLock>,
    },
}

struct MemtableWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    record_type: RecordType,
    lsn: u64,
}

impl WriteComponent {
    fn append_to_memtable(&self, key: Vec<u8>, value: Vec<u8>, record_type: RecordType, lsn: u64) {
        let key_len = key.len();
        let value_len = value.len();

        self.memtable
            .insert((key, lsn), MemtableRecord::new(value, record_type, lsn));

        let added_bytes = if record_type == RecordType::Delete {
            key_len
        } else {
            key_len + value_len
        };

        self.memtable_size
            .fetch_add(added_bytes, std::sync::atomic::Ordering::SeqCst);
    }

    /// Create a new WriteComponent
    pub fn new(
        sstable_dir: PathBuf,
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        sequence_number: u64, // this is from the wal recovery process
        config: Arc<StorageConfig>,
    ) -> Self {
        log::info!(
            "Initializing WriteComponent with sequence_number={}, sstable_dir={}",
            sequence_number,
            sstable_dir.display()
        );
        Self::with_payload_mode(
            sstable_dir,
            wal_manager,
            manifest_manager,
            sequence_number,
            config,
            PayloadMode::Inline,
        )
    }

    /// Create a new WriteComponent with default directory
    pub fn with_default_dir(
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        config: Arc<StorageConfig>,
    ) -> Self {
        Self::with_payload_mode(
            PathBuf::from("data"),
            wal_manager,
            manifest_manager,
            0,
            config,
            PayloadMode::Inline,
        )
    }

    pub(crate) fn new_reference_index(
        sstable_dir: PathBuf,
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        sequence_number: u64,
        config: Arc<StorageConfig>,
        generation: u64,
        root_lock: Arc<RootLock>,
    ) -> Self {
        Self::with_payload_mode(
            sstable_dir,
            wal_manager,
            manifest_manager,
            sequence_number,
            config,
            PayloadMode::WalReference {
                generation,
                root_lock,
            },
        )
    }

    fn with_payload_mode(
        sstable_dir: PathBuf,
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        sequence_number: u64,
        config: Arc<StorageConfig>,
        payload_mode: PayloadMode,
    ) -> Self {
        let checkpoint_owner = wal_manager.register_checkpoint_owner();
        Self {
            memtable: Arc::new(SkipMap::new()),
            memtable_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            sequence_number: Arc::new(std::sync::atomic::AtomicU64::new(sequence_number)),
            writer_lock: Arc::new(tokio::sync::Mutex::new(())),
            admission: AdmissionQueue::new(),
            checkpoint_owner,
            payload_mode,
            sstable_dir,
            wal_manager,
            manifest_manager,
            config,
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        self.write_batch(vec![BatchOperation::Put { key, value }])
            .await
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), DBError> {
        self.write_batch(vec![BatchOperation::Delete { key }]).await
    }

    pub async fn write_batch(&self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
        if operations.is_empty() {
            return Ok(());
        }
        let estimated_bytes = validate_batch(&operations)?;
        let admission = self.admission.enqueue(operations, estimated_bytes).await?;
        if admission.should_start_runner {
            let component = self.clone();
            tokio::spawn(async move { component.run_write_admission().await });
        }
        admission
            .completion
            .await
            .map_err(|_| DBError::StorageError("write runner stopped before completion".into()))?
    }

    pub async fn await_pending_writes(&self) {
        if let Ok(ticket) = self.admission.last_admitted_ticket() {
            let _ = self.admission.await_completed_through(ticket).await;
        }
        let _writer = Arc::clone(&self.writer_lock).lock_owned().await;
    }

    async fn run_write_admission(&self) {
        loop {
            tokio::task::yield_now().await;
            let requests = match self
                .admission
                .drain_ready(MAX_DRAIN_REQUESTS, MAX_DRAIN_BYTES)
            {
                Ok(Some(requests)) => requests,
                Ok(None) | Err(_) => return,
            };
            let _writer = Arc::clone(&self.writer_lock).lock_owned().await;
            let current = self.current_sequence_number();
            let mut next_lsn = current;
            let mut groups = Vec::with_capacity(requests.len());
            let mut build_error = None;
            for request in &requests {
                let mut commands = Vec::with_capacity(request.operations.len());
                for operation in &request.operations {
                    next_lsn = match next_lsn.checked_add(1) {
                        Some(lsn) => lsn,
                        None => {
                            build_error = Some("batch sequence exhausted".to_string());
                            break;
                        }
                    };
                    let command = match operation {
                        BatchOperation::Put { key, value } => {
                            LogCommand::new(RecordType::Put, key.clone(), value.clone(), next_lsn)
                        }
                        BatchOperation::Delete { key } => {
                            LogCommand::new(RecordType::Delete, key.clone(), Vec::new(), next_lsn)
                        }
                    };
                    commands.push(command);
                }
                if build_error.is_some() {
                    break;
                }
                groups.push(commands);
            }
            let result = if let Some(message) = build_error {
                Err(DBError::StorageError(message))
            } else {
                match &self.payload_mode {
                    PayloadMode::Inline => self
                        .wal_manager
                        .write_engine_transactions(groups.clone(), self.checkpoint_owner.as_ref())
                        .await
                        .map(|()| {
                            for commands in groups {
                                for command in commands {
                                    self.append_to_memtable(
                                        command.key,
                                        command.value,
                                        command.record_type,
                                        command.lsn,
                                    );
                                }
                            }
                            self.sequence_number
                                .store(next_lsn, std::sync::atomic::Ordering::SeqCst);
                        }),
                    PayloadMode::WalReference { generation, .. } => self
                        .wal_manager
                        .append_engine_transactions_with_receipts(
                            groups.clone(),
                            self.checkpoint_owner.as_ref(),
                        )
                        .await
                        .and_then(|receipts| {
                            encode_reference_writes(*generation, &groups, receipts)
                        })
                        .map(|writes| {
                            for write in writes {
                                self.append_to_memtable(
                                    write.key,
                                    write.value,
                                    write.record_type,
                                    write.lsn,
                                );
                            }
                            self.sequence_number
                                .store(next_lsn, std::sync::atomic::Ordering::SeqCst);
                        }),
                }
            };
            for request in requests {
                let request_result = result.as_ref().map(|()| ()).map_err(Self::clone_db_error);
                let _ = self.admission.complete(request, request_result);
            }
        }
    }

    fn clone_db_error(error: &DBError) -> DBError {
        match error {
            DBError::IO(error) => DBError::IO(match error.raw_os_error() {
                Some(code) => std::io::Error::from_raw_os_error(code),
                None => std::io::Error::new(error.kind(), error.to_string()),
            }),
            DBError::TryIntoError(_) => {
                DBError::TryIntoError(<[u8; 2]>::try_from(&[0u8][..]).unwrap_err())
            }
            DBError::Corrupted(message) => DBError::Corrupted(message.clone()),
            DBError::MutexPoisoned(message) => DBError::MutexPoisoned(message.clone()),
            DBError::StorageError(message) => DBError::StorageError(message.clone()),
            DBError::NotFound => DBError::NotFound,
        }
    }

    /// Apply a committed replicated command to memtable.
    ///
    /// This path intentionally does not write to WAL, because the entry is
    /// already durable via raft/WAL replication flow.
    pub async fn apply_replicated_command(&self, cmd: LogCommand) {
        if let Ok(ticket) = self.admission.last_admitted_ticket() {
            let _ = self.admission.await_completed_through(ticket).await;
        }
        self.wal_manager.mark_untracked_mutation();
        let _writer = self.writer_lock.lock().await;
        let normalized_value = if cmd.record_type == RecordType::Delete {
            Vec::new()
        } else {
            cmd.value
        };

        self.append_to_memtable(cmd.key, normalized_value, cmd.record_type, cmd.lsn);

        self.sequence_number
            .fetch_max(cmd.lsn, std::sync::atomic::Ordering::SeqCst);
    }

    // the transtition were managed by the calller. WriteComponent only provide the lock_memtable function to return the current memtable for flushing, and create a new memtable for new writes. This design allows the caller to have more control over the transition process, such as when to trigger the flush and how to handle concurrent writes during the transition.
    pub async fn lock_memtable(&mut self) -> Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> {
        self.wal_manager.mark_untracked_mutation();
        // current memtable were full, we need to flush it to disk, so we need to create a new memtable and return the old memtable for flushing.
        let new_memtable = Arc::new(SkipMap::new());

        let old_memtable = std::mem::replace(&mut self.memtable, new_memtable);
        self.memtable_size
            .store(0, std::sync::atomic::Ordering::SeqCst);
        old_memtable
    }

    pub async fn flush_active_memtable(&mut self) -> Result<CheckpointResult, DBError> {
        let ticket = self.admission.last_admitted_ticket()?;
        self.admission.await_completed_through(ticket).await?;
        let writer_lock = Arc::clone(&self.writer_lock);
        let _writer = writer_lock.lock().await;
        let snapshot = Arc::new(SkipMap::new());
        for entry in self.memtable.iter() {
            snapshot.insert(entry.key().clone(), entry.value().clone());
        }
        let sequence = self.current_sequence_number();
        let result = self
            .flush_inner(Arc::clone(&snapshot), Some(sequence))
            .await?;
        let mut removed_bytes = 0;
        for entry in snapshot.iter() {
            if let Some(removed) = self.memtable.remove(entry.key()) {
                removed_bytes += removed.key().0.len() + removed.value().value.len();
            }
        }
        self.memtable_size
            .fetch_sub(removed_bytes, std::sync::atomic::Ordering::SeqCst);
        Ok(result)
    }

    pub fn active_memtable_handle(&self) -> Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> {
        Arc::clone(&self.memtable)
    }

    pub fn current_sequence_number(&self) -> u64 {
        self.sequence_number
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn memtable_size_bytes(&self) -> usize {
        self.memtable_size.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub fn restore_memtable(
        &mut self,
        recovered_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord>,
        recovered_memtable_size: usize,
    ) {
        self.memtable = Arc::new(recovered_memtable);
        self.memtable_size
            .store(recovered_memtable_size, std::sync::atomic::Ordering::SeqCst);
    }

    /// Perform checkpoint - convert memtable to SSTable and write to disk
    ///
    /// This function:
    /// 1. Takes ownership of the memtable
    /// 2. Writes SSTable to disk using flush_memtable
    ///
    /// # Arguments
    /// * `memtable` - The memtable to flush (takes ownership)
    /// * `level` - The level at which to write the SSTable (0 for L0)
    /// * `file_id` - Unique identifier for the SSTable file
    /// * `wal_path` - Path to the WAL file (used for archive naming)
    ///
    /// # Returns
    /// * `Ok(CheckpointResult)` - Information about the written SSTable
    /// * `Err(std::io::Error)` - If flush fails
    pub async fn flush(
        &mut self,
        memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
    ) -> Result<CheckpointResult, DBError> {
        self.flush_inner(memtable, None).await
    }

    async fn flush_inner(
        &mut self,
        memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
        trusted_sequence: Option<u64>,
    ) -> Result<CheckpointResult, DBError> {
        let mut valid_records: std::collections::BTreeMap<Vec<u8>, MemtableRecord> =
            std::collections::BTreeMap::new();
        for entry in memtable.iter() {
            let key = &entry.key().0;
            let record = entry.value();

            valid_records
                .entry(key.clone())
                .and_modify(|existing| {
                    if existing.lsn < record.lsn {
                        *existing = record.clone();
                    }
                })
                .or_insert_with(|| record.clone());
        }

        let mut block_builder = BlockBuilder::new(0);
        let mut block_entries: Vec<Block> = Vec::new();
        let mut sparse_index = Vec::<SparseIndexEntry>::new();
        let mut bloom_filter = BloomFilterWrapper::with_rate(
            valid_records.len() + 10,
            self.config.bloom_false_positive_rate,
        );

        let mut encoded_offset: u64 = 0;

        for (key, record) in valid_records.iter() {
            bloom_filter.insert(key);

            match block_builder.add_record(key, record) {
                BlockBuilderState::EnoughSpace => {
                    // DO NOTHING
                }
                BlockBuilderState::Full(block, _) => {
                    log::debug!(
                        "Block full with first_key={}, last_key={}, record_count={}, data_size={}",
                        String::from_utf8_lossy(&block.first_key),
                        String::from_utf8_lossy(&block.last_key),
                        block.record_count,
                        block.data_size
                    );

                    block_builder.add_record(&key, &record);

                    // TODO: remove the index insertion later.
                    sparse_index.push(SparseIndexEntry {
                        first_key: block.first_key.clone(),
                        block_offset: encoded_offset,
                        last_key: block.last_key.clone(),
                        record_count: block.record_count,
                    });
                    encoded_offset += block.data_size as u64;
                    block_entries.push(block);
                }
            }
        }

        // flush the last block if it has any records
        if let Some((block, _)) = block_builder.build() {
            sparse_index.push(SparseIndexEntry {
                first_key: block.first_key.clone(),
                block_offset: encoded_offset,
                last_key: block.last_key.clone(),
                record_count: block.record_count,
            });
            block_entries.push(block);
        }

        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());
        for entry in sparse_index.iter() {
            index_block.extend_from_slice(&entry.encode());
        }

        let mut serialized_keys = std::collections::BTreeSet::new();
        for block in &block_entries {
            for record in block.data.as_ref().into_iter().flatten() {
                let Some(expected) = valid_records.get(&record.key) else {
                    return Err(DBError::Corrupted(
                        "SSTable block contains a record outside the flush snapshot".into(),
                    ));
                };
                if !serialized_keys.insert(record.key.clone())
                    || expected.lsn != record.lsn
                    || expected.record_type != record.record_type
                    || expected.value != record.value
                {
                    return Err(DBError::Corrupted(
                        "SSTable block records do not cover the flush snapshot".into(),
                    ));
                }
            }
        }
        if serialized_keys.len() != valid_records.len() {
            return Err(DBError::Corrupted(
                "SSTable blocks omit records from the flush snapshot".into(),
            ));
        }

        let codec = SSTableCodec::new(block_entries, sparse_index, bloom_filter.clone());
        let (encoded, footer) = codec.serialize();

        let (smallest_key, largest_key) =
            SSTableCodec::range_from_index(&codec.index).ok_or_else(|| {
                DBError::Corrupted("cannot register SSTable without sparse index range".to_string())
            })?;

        let record_count = valid_records.len();

        crate::storage::durable_dir::create_dir_all_durable(&self.sstable_dir.join("level-0"))
            .await?;
        let (file_id, file_path, mut file) = loop {
            let file_id = self.manifest_manager.allocate_file_id().await?;
            let file_path = self
                .sstable_dir
                .join(format!("level-0/sstable-{}.dat", file_id));
            match tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&file_path)
                .await
            {
                Ok(file) => break (file_id, file_path, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        };
        file.write_all(&encoded).await?;
        file.sync_all().await?;
        tokio::fs::File::open(self.sstable_dir.join("level-0"))
            .await?
            .sync_all()
            .await?;

        self.manifest_manager
            .register_sstable(SSTableMeta {
                file_id,
                level: 0,
                path: file_path.to_string_lossy().to_string(),
                record_count,
                bloom_offset: footer.bloom_block_start,
                bloom_size: footer.bloom_block_end - footer.bloom_block_start,
                smallest_key,
                largest_key,
            })
            .await?;

        let active_segment = self.wal_manager.rotate_wal_file().await?;
        let checkpoint_lsn = valid_records
            .values()
            .map(|record| record.lsn)
            .max()
            .unwrap_or(0);
        self.manifest_manager
            .mark_checkpoint(checkpoint_lsn, active_segment)
            .await?;
        if let Some(committed_sequence) = trusted_sequence.filter(|_| {
            self.wal_manager
                .owns_recovery_checkpoints(self.checkpoint_owner.as_ref())
        }) {
            if let Some((replay_start_segment, next_physical_lsn)) =
                self.wal_manager.trusted_recovery_position().await?
            {
                let checkpoint = RecoveryCheckpoint {
                    replay_start_segment,
                    next_physical_lsn,
                    committed_sequence,
                };
                self.manifest_manager
                    .publish_recovery_checkpoint(checkpoint)
                    .await?;
                if let Err(error) = self
                    .wal_manager
                    .reclaim_checkpointed_segments(checkpoint)
                    .await
                {
                    log::warn!("WAL checkpoint reclamation will retry later: {error}");
                }
            }
        }

        Ok(CheckpointResult {
            sstable_path: file_path.to_string_lossy().to_string(), // Will be set after flush_memtable
            record_count,
            level: 0,
            file_id, // Will be set after flush_memtable
            data: encoded,
        })
    }
}

fn encode_reference_writes(
    generation: u64,
    groups: &[Vec<LogCommand>],
    receipts: Vec<Vec<WalValueRef>>,
) -> Result<Vec<MemtableWrite>, DBError> {
    if groups.len() != receipts.len() {
        return Err(DBError::StorageError(
            "WAL receipts do not match admitted groups".into(),
        ));
    }
    let mut writes = Vec::new();
    for (commands, references) in groups.iter().zip(receipts) {
        if commands.len() != references.len() {
            return Err(DBError::StorageError(
                "WAL receipts do not match admitted commands".into(),
            ));
        }
        for (command, reference) in commands.iter().zip(references) {
            if reference.record_type != command.record_type
                || reference.application_sequence != command.lsn
            {
                return Err(DBError::StorageError(
                    "WAL receipt does not match its command".into(),
                ));
            }
            let value = match command.record_type {
                RecordType::Put => ReferenceValue::new(generation, reference).encode().to_vec(),
                RecordType::Delete => Vec::new(),
            };
            writes.push(MemtableWrite {
                key: command.key.clone(),
                value,
                record_type: command.record_type,
                lsn: command.lsn,
            });
        }
    }
    Ok(writes)
}

fn validate_batch(operations: &[BatchOperation]) -> Result<usize, DBError> {
    let mut transaction_bytes = 37usize;
    for operation in operations {
        let (key, value) = match operation {
            BatchOperation::Put { key, value } => (key, value.as_slice()),
            BatchOperation::Delete { key } => (key, &[][..]),
        };
        let frame_payload = 42usize
            .checked_add(key.len())
            .and_then(|size| size.checked_add(value.len()))
            .ok_or_else(|| DBError::StorageError("WAL frame size overflow".into()))?;
        if frame_payload > MAX_WAL_FRAME_BYTES {
            return Err(DBError::StorageError("WAL frame exceeds 64 MiB".into()));
        }
        let frame_bytes = 20usize
            .checked_add(frame_payload)
            .ok_or_else(|| DBError::StorageError("WAL frame size overflow".into()))?;
        transaction_bytes = transaction_bytes
            .checked_add(frame_bytes)
            .ok_or_else(|| DBError::StorageError("WAL transaction size overflow".into()))?;
        if transaction_bytes > MAX_WAL_TRANSACTION_BYTES {
            return Err(DBError::StorageError(
                "WAL transaction exceeds group limit".into(),
            ));
        }
    }
    Ok(transaction_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::api::AsyncKVEngine;
    use crate::storage::{
        manifest_codec::ManifestManager,
        record::{MemtableRecord, RecordType},
        recovermanager::log_store::LogCommand,
        recovermanager::wal::WALManager,
    };
    use crossbeam_skiplist::SkipMap;

    async fn build_write_component(temp_dir: &std::path::Path) -> WriteComponent {
        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new_with_checkpoint(temp_dir.join("wal"), 1024 * 1024, None)
            .await
            .unwrap();
        let manifest_manager = Arc::new(
            ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
                .await
                .unwrap(),
        );

        WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            manifest_manager,
            0,
            Arc::new(crate::storage::config::StorageConfig::default()),
        )
    }

    #[test]
    fn cloned_db_errors_preserve_variants_and_io_details() {
        let os_error =
            WriteComponent::clone_db_error(&DBError::IO(std::io::Error::from_raw_os_error(5)));
        match os_error {
            DBError::IO(error) => {
                assert_eq!(error.kind(), std::io::Error::from_raw_os_error(5).kind());
                assert_eq!(error.raw_os_error(), Some(5));
            }
            error => panic!("unexpected cloned error: {error:?}"),
        }

        assert!(matches!(
            WriteComponent::clone_db_error(&DBError::NotFound),
            DBError::NotFound
        ));
        assert!(matches!(
            WriteComponent::clone_db_error(&DBError::Corrupted("bad checksum".into())),
            DBError::Corrupted(message) if message == "bad checksum"
        ));
        assert!(matches!(
            WriteComponent::clone_db_error(&DBError::TryIntoError(
                <[u8; 2]>::try_from(&[0u8][..]).unwrap_err()
            )),
            DBError::TryIntoError(_)
        ));
    }

    #[tokio::test]
    async fn active_flush_preserves_cloned_writes_and_shared_accounting() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        component
            .apply_replicated_command(LogCommand::new(
                RecordType::Put,
                b"old".to_vec(),
                b"yes".to_vec(),
                1,
            ))
            .await;
        let clone = component.clone();
        let reader = component.active_memtable_handle();
        component.flush_active_memtable().await.unwrap();
        clone
            .apply_replicated_command(LogCommand::new(
                RecordType::Delete,
                b"new".to_vec(),
                Vec::new(),
                2,
            ))
            .await;
        assert!(Arc::ptr_eq(&reader, &component.active_memtable_handle()));
        assert_eq!(reader.len(), 1);
        assert_eq!(
            reader.iter().next().unwrap().value().record_type,
            RecordType::Delete
        );
        assert_eq!(component.memtable_size_bytes(), 3);
        assert_eq!(clone.memtable_size_bytes(), 3);
        component.flush_active_memtable().await.unwrap();
        assert!(reader.is_empty());
        assert_eq!(clone.memtable_size_bytes(), 0);
    }

    #[tokio::test]
    async fn owner_clone_shares_checkpoint_authority_and_reclaims_wal() {
        let directory = tempfile::tempdir().unwrap();
        let mut owner = build_write_component(directory.path()).await;
        let clone = owner.clone();
        clone
            .put(b"cloned-write".to_vec(), b"durable".to_vec())
            .await
            .unwrap();
        owner.flush_active_memtable().await.unwrap();

        let checkpoint = owner
            .manifest_manager
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        assert_eq!(checkpoint.committed_sequence, 1);
        let segments = std::fs::read_dir(directory.path().join("wal"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".log"))
            .count();
        assert_eq!(segments, 1);
    }

    #[tokio::test]
    async fn active_flush_publishes_trusted_checkpoint_but_public_flush_does_not() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        component
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        let partial = SkipMap::new();
        partial.insert(
            (b"partial".to_vec(), 999),
            MemtableRecord::new(b"supplied".to_vec(), RecordType::Put, 999),
        );
        component.flush(Arc::new(partial)).await.unwrap();
        assert!(
            component
                .manifest_manager
                .snapshot()
                .await
                .recovery_checkpoint
                .is_none()
        );

        component.flush_active_memtable().await.unwrap();
        let checkpoint = component
            .manifest_manager
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        assert_eq!(checkpoint.committed_sequence, 1);
        assert_eq!(checkpoint.replay_start_segment, 2);
        assert_eq!(checkpoint.next_physical_lsn, 3);
    }

    #[tokio::test]
    async fn default_oversized_batch_is_fully_checkpointed_and_recovered() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        let operations = (0..850)
            .map(|index| BatchOperation::Put {
                key: format!("k{index:04}").into_bytes(),
                value: vec![(index % 251) as u8; 5000],
            })
            .collect();
        component.write_batch(operations).await.unwrap();
        assert_eq!(component.current_sequence_number(), 850);
        let result = component.flush_active_memtable().await.unwrap();
        assert_eq!(result.record_count, 850);

        let segments = std::fs::read_dir(directory.path().join("wal"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".log"))
            .count();
        assert_eq!(segments, 1);
        drop(component);

        let database = crate::storage::kv2::KV2::open_with_defaults(directory.path())
            .await
            .unwrap();
        for index in 0..850 {
            assert_eq!(
                database
                    .get(format!("k{index:04}").as_bytes())
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &vec![(index % 251) as u8; 5000]
            );
        }
        assert_eq!(database.scan(..).await.unwrap().len(), 850);
    }

    #[tokio::test]
    async fn oversized_records_at_block_boundaries_are_all_serialized() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        component
            .write_batch(vec![
                BatchOperation::Put {
                    key: b"00-large-first".to_vec(),
                    value: vec![1; 5000],
                },
                BatchOperation::Put {
                    key: b"01-small".to_vec(),
                    value: b"small-a".to_vec(),
                },
                BatchOperation::Put {
                    key: b"02-large-middle".to_vec(),
                    value: vec![2; 5000],
                },
                BatchOperation::Put {
                    key: b"03-small".to_vec(),
                    value: b"small-b".to_vec(),
                },
                BatchOperation::Put {
                    key: b"04-large-last".to_vec(),
                    value: vec![3; 5000],
                },
            ])
            .await
            .unwrap();
        let result = component.flush_active_memtable().await.unwrap();
        assert_eq!(result.record_count, 5);
        drop(component);

        let database = crate::storage::kv2::KV2::open_with_defaults(directory.path())
            .await
            .unwrap();
        for (key, expected) in [
            (b"00-large-first".as_slice(), vec![1; 5000]),
            (b"02-large-middle".as_slice(), vec![2; 5000]),
            (b"04-large-last".as_slice(), vec![3; 5000]),
        ] {
            assert_eq!(
                database.get(key).await.unwrap().unwrap().as_ref(),
                &expected
            );
        }
        assert_eq!(
            database.get(b"01-small").await.unwrap().unwrap().as_ref(),
            b"small-a"
        );
        assert_eq!(
            database.get(b"03-small").await.unwrap().unwrap().as_ref(),
            b"small-b"
        );
    }

    #[tokio::test]
    async fn direct_wal_mutation_prevents_checkpoint_publication() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        component
            .wal_manager
            .write_log(
                &b"external".to_vec(),
                &b"not-in-memtable".to_vec(),
                7,
                RecordType::Put,
            )
            .await
            .unwrap();
        component
            .put(b"engine".to_vec(), b"tracked".to_vec())
            .await
            .unwrap();
        component.flush_active_memtable().await.unwrap();
        assert!(
            component
                .manifest_manager
                .snapshot()
                .await
                .recovery_checkpoint
                .is_none()
        );
    }

    #[tokio::test]
    async fn standalone_wal_does_not_grant_engine_checkpoint_authority() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(directory.path().join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(directory.path().join("wal")).unwrap();
        let wal_manager = Arc::new(
            WALManager::new(directory.path().join("wal"), 1024 * 1024)
                .await
                .unwrap(),
        );
        let manifest_manager = Arc::new(
            ManifestManager::load_or_create(directory.path().join("MANIFEST"))
                .await
                .unwrap(),
        );
        let mut component = WriteComponent::new(
            directory.path().join("sstable"),
            Arc::clone(&wal_manager),
            Arc::clone(&manifest_manager),
            0,
            Arc::new(StorageConfig::default()),
        );
        component
            .put(b"raft-log".to_vec(), b"retained".to_vec())
            .await
            .unwrap();
        component.flush_active_memtable().await.unwrap();

        assert!(
            manifest_manager
                .snapshot()
                .await
                .recovery_checkpoint
                .is_none()
        );
        assert!(
            directory
                .path()
                .join("wal/00000000000000000000.log")
                .exists()
        );
    }

    #[tokio::test]
    async fn cancelled_active_flush_retains_shared_entries_for_retry() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        component
            .put(b"old".to_vec(), b"yes".to_vec())
            .await
            .unwrap();
        component.delete(b"gone".to_vec()).await.unwrap();
        let reader = component.active_memtable_handle();
        let mut flush = Box::pin(component.flush_active_memtable());
        std::future::poll_fn(|context| {
            assert!(std::future::Future::poll(flush.as_mut(), context).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        drop(flush);
        assert!(Arc::ptr_eq(&reader, &component.active_memtable_handle()));
        assert_eq!(reader.len(), 2);
        assert_eq!(component.memtable_size_bytes(), 10);
        component
            .put(b"new".to_vec(), b"yes".to_vec())
            .await
            .unwrap();
        let result = component.flush_active_memtable().await.unwrap();
        assert_eq!(result.record_count, 3);
        assert!(reader.is_empty());
        assert_eq!(component.memtable_size_bytes(), 0);
    }

    #[tokio::test]
    async fn failed_sstable_write_preserves_wal_without_publishing_manifest() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(&directory.path().to_path_buf()).await;
        component
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        tokio::fs::remove_dir(directory.path().join("sstable/level-0"))
            .await
            .unwrap();
        tokio::fs::write(directory.path().join("sstable/level-0"), b"obstruction")
            .await
            .unwrap();
        assert!(component.flush_active_memtable().await.is_err());
        assert_eq!(component.active_memtable_handle().len(), 1);
        assert_eq!(component.memtable_size_bytes(), 8);
        let snapshot = component.manifest_manager.snapshot().await;
        assert!(snapshot.levels.get(&0).is_none_or(|files| files.is_empty()));
        assert_eq!(snapshot.active_wal_segment, 0);
        let records = component.wal_manager.recover_records().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, b"value");
    }

    #[tokio::test]
    async fn flush_persists_memtable_and_updates_manifest() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        memtable.insert(
            (b"key1".to_vec(), 1),
            MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1),
        );
        memtable.insert(
            (b"key2".to_vec(), 2),
            MemtableRecord::new(Vec::new(), RecordType::Delete, 2),
        );

        let result = write_component.flush(Arc::new(memtable)).await.unwrap();

        assert_eq!(result.record_count, 2);
        assert_eq!(result.level, 0);

        let snapshot = write_component.manifest_manager.snapshot().await;
        let level0_files = snapshot.levels.get(&0).unwrap();
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].record_count, 2);
        // bloom_offset and bloom_size are set from the serialized SSTable footer
        assert!(
            level0_files[0].bloom_size > 0,
            "bloom filter should have non-zero size in manifest"
        );
        assert_eq!(
            tokio::fs::read(&result.sstable_path).await.unwrap(),
            result.data
        );
        assert!(!result.data.is_empty(), "flush should return encoded data");

        assert!(snapshot.active_wal_segment > 0);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn flush_persists_bloom_filter_metadata_for_memtable_keys() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        memtable.insert(
            (b"key-z".to_vec(), 1),
            MemtableRecord::new(b"value-z".to_vec(), RecordType::Put, 1),
        );
        memtable.insert(
            (b"key-a".to_vec(), 2),
            MemtableRecord::new(b"value-a".to_vec(), RecordType::Put, 2),
        );
        memtable.insert(
            (b"key-m".to_vec(), 3),
            MemtableRecord::new(b"value-m".to_vec(), RecordType::Put, 3),
        );

        let _ = write_component.flush(Arc::new(memtable)).await.unwrap();

        let snapshot = write_component.manifest_manager.snapshot().await;
        let level0_files = snapshot.levels.get(&0).unwrap();
        assert_eq!(level0_files.len(), 1);
        // Bloom filter metadata is stored as offset+size rather than inline bitmap
        assert!(
            level0_files[0].bloom_size > 0,
            "bloom filter should have non-zero size in manifest metadata"
        );
        assert!(
            level0_files[0].bloom_offset > 0,
            "bloom filter should have non-zero offset in manifest metadata"
        );

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn flush_twice_registers_two_sstables_with_incrementing_ids() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let first_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        first_memtable.insert(
            (b"first-key".to_vec(), 1),
            MemtableRecord::new(b"first-value".to_vec(), RecordType::Put, 1),
        );

        let second_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        second_memtable.insert(
            (b"second-key".to_vec(), 2),
            MemtableRecord::new(b"second-value".to_vec(), RecordType::Put, 2),
        );

        let _ = write_component
            .flush(Arc::new(first_memtable))
            .await
            .unwrap();
        let _ = write_component
            .flush(Arc::new(second_memtable))
            .await
            .unwrap();

        let snapshot = write_component.manifest_manager.snapshot().await;
        let level0_files = snapshot.levels.get(&0).unwrap();
        assert_eq!(level0_files.len(), 2);
        assert_eq!(level0_files[0].file_id, 1);
        assert_eq!(level0_files[1].file_id, 2);
        assert_eq!(snapshot.next_file_id, 3);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn flush_rotates_wal_on_each_successful_flush() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        memtable.insert(
            (b"wal-key".to_vec(), 1),
            MemtableRecord::new(b"wal-value".to_vec(), RecordType::Put, 1),
        );

        let _ = write_component.flush(Arc::new(memtable)).await.unwrap();

        let snapshot_after_first = write_component.manifest_manager.snapshot().await;
        let first_segment = snapshot_after_first.active_wal_segment;
        assert!(first_segment > 0);

        let second_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        second_memtable.insert(
            (b"wal-key-2".to_vec(), 2),
            MemtableRecord::new(b"wal-value-2".to_vec(), RecordType::Put, 2),
        );

        let _ = write_component
            .flush(Arc::new(second_memtable))
            .await
            .unwrap();
        let snapshot_after_second = write_component.manifest_manager.snapshot().await;
        assert!(snapshot_after_second.active_wal_segment > first_segment);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn put_appends_put_record_to_memtable_and_updates_size() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let write_component = build_write_component(&temp_dir).await;

        let key = b"put-key".to_vec();
        let value = b"put-value".to_vec();

        write_component
            .put(key.clone(), value.clone())
            .await
            .unwrap();

        assert_eq!(write_component.memtable.len(), 1);

        let entry = write_component.memtable.iter().next().unwrap();
        assert_eq!(&entry.key().0, &key);
        assert_eq!(entry.key().1, 1);

        let record = entry.value();
        assert_eq!(record.record_type, RecordType::Put);
        assert_eq!(record.value, value);
        assert_eq!(record.lsn, 1);

        assert_eq!(
            write_component
                .memtable_size
                .load(std::sync::atomic::Ordering::SeqCst),
            key.len() + b"put-value".len()
        );

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn delete_appends_tombstone_record_to_memtable_and_updates_size() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let write_component = build_write_component(&temp_dir).await;

        let key = b"delete-key".to_vec();

        write_component.delete(key.clone()).await.unwrap();

        assert_eq!(write_component.memtable.len(), 1);

        let entry = write_component.memtable.iter().next().unwrap();
        assert_eq!(&entry.key().0, &key);
        assert_eq!(entry.key().1, 1);

        let record = entry.value();
        assert_eq!(record.record_type, RecordType::Delete);
        assert!(record.value.is_empty());
        assert_eq!(record.lsn, 1);

        assert_eq!(
            write_component
                .memtable_size
                .load(std::sync::atomic::Ordering::SeqCst),
            key.len()
        );

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn apply_replicated_command_applies_without_wal_write_and_tracks_sequence() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let write_component = build_write_component(&temp_dir).await;

        let cmd = LogCommand::new(
            RecordType::Put,
            b"rep-key".to_vec(),
            b"rep-value".to_vec(),
            7,
        );
        write_component.apply_replicated_command(cmd).await;

        assert_eq!(write_component.memtable.len(), 1);
        let entry = write_component.memtable.iter().next().unwrap();
        assert_eq!(&entry.key().0, &b"rep-key".to_vec());
        assert_eq!(entry.key().1, 7);
        assert_eq!(entry.value().record_type, RecordType::Put);
        assert_eq!(entry.value().value, b"rep-value".to_vec());
        assert_eq!(write_component.current_sequence_number(), 7);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn cancelled_batch_waiting_for_admission_capacity_is_not_admitted() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let held = Arc::clone(&component.writer_lock).lock_owned().await;
        let mut tasks = Vec::new();
        for index in 0..super::super::admission::MAX_ADMITTED_REQUESTS {
            let writer = component.clone();
            tasks.push(tokio::spawn(async move {
                writer
                    .put(format!("key-{index}").into_bytes(), b"value".to_vec())
                    .await
            }));
        }
        while component.admission.last_admitted_ticket().unwrap()
            < u64::from(super::super::admission::MAX_ADMITTED_REQUESTS)
        {
            tokio::task::yield_now().await;
        }
        let writer = component.clone();
        let waiting = tokio::spawn(async move {
            writer
                .put(b"not-admitted".to_vec(), b"value".to_vec())
                .await
        });
        tokio::task::yield_now().await;
        waiting.abort();
        let _ = waiting.await;
        drop(held);
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert_eq!(
            component.admission.last_admitted_ticket().unwrap(),
            u64::from(super::super::admission::MAX_ADMITTED_REQUESTS)
        );
        assert_eq!(
            component.current_sequence_number(),
            u64::from(super::super::admission::MAX_ADMITTED_REQUESTS)
        );
    }

    #[tokio::test]
    async fn cancelled_admitted_batch_finishes_publication() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let held = Arc::clone(&component.writer_lock).lock_owned().await;
        let writer = component.clone();
        let task = tokio::spawn(async move {
            writer
                .write_batch(vec![BatchOperation::Put {
                    key: b"committed".to_vec(),
                    value: b"value".to_vec(),
                }])
                .await
        });
        while component.admission.last_admitted_ticket().unwrap() == 0 {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        drop(held);
        component.await_pending_writes().await;
        assert_eq!(component.current_sequence_number(), 1);
        assert_eq!(component.active_memtable_handle().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_requests_share_wal_sync_and_publish_distinct_sequences() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let start = Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for index in 0..16u8 {
            let writer = component.clone();
            let start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                start.wait().await;
                writer
                    .put(vec![index], vec![index, index.wrapping_add(1)])
                    .await
            }));
        }
        start.wait().await;
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        assert!(component.wal_manager.sync_data_count() < 16);
        assert_eq!(component.current_sequence_number(), 16);
        let mut lsns: Vec<_> = component
            .active_memtable_handle()
            .iter()
            .map(|entry| entry.key().1)
            .collect();
        lsns.sort_unstable();
        assert_eq!(lsns, (1..=16).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn flush_barrier_includes_admitted_writes_before_checkpointing() {
        let directory = tempfile::tempdir().unwrap();
        let mut component = build_write_component(directory.path()).await;
        let held = Arc::clone(&component.writer_lock).lock_owned().await;
        let writer = component.clone();
        let write = tokio::spawn(async move {
            writer
                .put(b"before-flush".to_vec(), b"durable".to_vec())
                .await
        });
        while component.admission.last_admitted_ticket().unwrap() == 0 {
            tokio::task::yield_now().await;
        }
        let manifest = Arc::clone(&component.manifest_manager);
        let flush = tokio::spawn(async move { component.flush_active_memtable().await });
        tokio::task::yield_now().await;
        assert!(!flush.is_finished());
        drop(held);
        write.await.unwrap().unwrap();
        let checkpoint = flush.await.unwrap().unwrap();
        assert_eq!(checkpoint.record_count, 1);
        assert_eq!(
            manifest
                .snapshot()
                .await
                .recovery_checkpoint
                .unwrap()
                .committed_sequence,
            1
        );
    }

    #[tokio::test]
    async fn admission_ticket_order_matches_published_sequence_order() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let mut completions = Vec::new();
        let mut expected = Vec::new();
        for key in 0..12u8 {
            let request = component
                .admission
                .enqueue(
                    vec![BatchOperation::Put {
                        key: vec![key],
                        value: vec![key],
                    }],
                    64,
                )
                .await
                .unwrap();
            expected.push((vec![key], request.ticket));
            completions.push(request.completion);
        }
        let writer = component.clone();
        tokio::spawn(async move { writer.run_write_admission().await });
        for completion in completions {
            completion.await.unwrap().unwrap();
        }
        let memtable = component.active_memtable_handle();
        for (key, ticket) in expected {
            let entry = memtable.iter().find(|entry| entry.key().0 == key).unwrap();
            assert_eq!(entry.key().1, ticket);
        }
        assert_eq!(component.wal_manager.sync_data_count(), 1);
    }

    #[tokio::test]
    async fn batch_limits_are_rejected_before_admission() {
        let too_large = vec![BatchOperation::Put {
            key: vec![0; MAX_WAL_FRAME_BYTES],
            value: Vec::new(),
        }];
        assert!(validate_batch(&too_large).is_err());
        assert_eq!(validate_batch(&[]).unwrap(), 37);
    }

    fn receipt(
        record_type: RecordType,
        application_sequence: u64,
        key: &[u8],
        value: &[u8],
    ) -> WalValueRef {
        WalValueRef {
            segment_id: 1,
            transaction_id: application_sequence,
            transaction_start_offset: 36,
            commit_offset: 200,
            commit_lsn: application_sequence + 2,
            frame_offset: 36,
            frame_length: 20 + 42 + key.len() as u32 + value.len() as u32,
            physical_lsn: application_sequence,
            application_sequence,
            record_type,
            key_length: key.len() as u32,
            key_checksum: crc32fast::hash(key),
            value_length: value.len() as u64,
            value_checksum: crc32fast::hash(value),
        }
    }

    #[test]
    fn reference_writes_encode_puts_and_preserve_delete_tombstones() {
        let commands = vec![
            LogCommand::new(RecordType::Put, b"k".to_vec(), b"v".to_vec(), 1),
            LogCommand::new(RecordType::Delete, b"d".to_vec(), Vec::new(), 2),
        ];
        let receipts = vec![vec![
            receipt(RecordType::Put, 1, b"k", b"v"),
            receipt(RecordType::Delete, 2, b"d", b""),
        ]];
        let writes = encode_reference_writes(5, &[commands], receipts).unwrap();
        assert_eq!(writes.len(), 2);
        assert_eq!(writes[0].record_type, RecordType::Put);
        assert_eq!(writes[0].lsn, 1);
        assert_eq!(
            writes[0].value.len(),
            crate::storage::experimental::REFERENCE_VALUE_ENCODED_SIZE
        );
        assert_eq!(
            ReferenceValue::decode(&writes[0].value).unwrap().generation,
            5
        );
        assert_eq!(writes[1].record_type, RecordType::Delete);
        assert!(writes[1].value.is_empty());
    }

    #[test]
    fn reference_writes_reject_mismatched_receipts() {
        let commands = vec![LogCommand::new(
            RecordType::Put,
            b"k".to_vec(),
            b"v".to_vec(),
            1,
        )];
        assert!(encode_reference_writes(1, &[commands.clone()], vec![vec![]]).is_err());
        assert!(
            encode_reference_writes(
                1,
                &[commands],
                vec![vec![receipt(RecordType::Delete, 1, b"k", b"")]],
            )
            .is_err()
        );
    }
}
