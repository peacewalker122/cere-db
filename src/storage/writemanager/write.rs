//! WriteComponent - Handles checkpointing operations
//!
//! Responsible for:
//! 1. Converting memtable to SSTable
//! 2. Writing SSTable to disk
//! 3. Managing checkpoint metadata

use std::{path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;
use tokio::io::AsyncWriteExt;

use crate::api::api::BatchOperation;
use crate::error::DBError;
use crate::storage::{
    bloom::BloomFilterWrapper,
    config::StorageConfig,
    index::SparseIndexEntry,
    manifest_codec::{ManifestManager, SSTableMeta},
    record::{MemtableRecord, RecordType},
    recovermanager::log_store::LogCommand,
    recovermanager::wal::WALManager,
    sstable_codec::SSTableCodec,
    writemanager::block::{Block, BlockBuilder, BlockBuilderState},
};

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
            sstable_dir: self.sstable_dir.clone(),
            wal_manager: Arc::clone(&self.wal_manager),
            manifest_manager: Arc::clone(&self.manifest_manager),
            config: Arc::clone(&self.config),
        }
    }
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

        Self {
            memtable: Arc::new(SkipMap::new()),
            memtable_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            sequence_number: Arc::new(std::sync::atomic::AtomicU64::new(sequence_number)),
            writer_lock: Arc::new(tokio::sync::Mutex::new(())),
            sstable_dir,
            wal_manager,
            manifest_manager,
            config,
        }
    }

    /// Create a new WriteComponent with default directory
    pub fn with_default_dir(
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        config: Arc<StorageConfig>,
    ) -> Self {
        Self {
            memtable: Arc::new(SkipMap::new()),
            memtable_size: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            sequence_number: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            writer_lock: Arc::new(tokio::sync::Mutex::new(())),
            sstable_dir: PathBuf::from("data"),
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
        let writer = Arc::clone(&self.writer_lock).lock_owned().await;
        let component = self.clone();
        tokio::spawn(async move { component.write_batch_committed(operations, writer).await })
            .await
            .map_err(|error| DBError::StorageError(format!("batch writer task failed: {error}")))?
    }

    async fn write_batch_committed(
        &self,
        operations: Vec<BatchOperation>,
        _writer: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<(), DBError> {
        if operations.is_empty() {
            return Ok(());
        }
        let current = self.current_sequence_number();
        let count = u64::try_from(operations.len())
            .map_err(|_| DBError::StorageError("batch sequence exhausted".into()))?;
        let final_sequence = current
            .checked_add(count)
            .ok_or_else(|| DBError::StorageError("batch sequence exhausted".into()))?;
        let mut commands = Vec::with_capacity(operations.len());
        for (offset, operation) in operations.iter().enumerate() {
            let lsn = current + offset as u64 + 1;
            let command = match operation {
                BatchOperation::Put { key, value } => {
                    LogCommand::new(RecordType::Put, key.clone(), value.clone(), lsn)
                }
                BatchOperation::Delete { key } => {
                    LogCommand::new(RecordType::Delete, key.clone(), Vec::new(), lsn)
                }
            };
            commands.push(command);
        }
        self.wal_manager.write_transaction(commands.clone()).await?;
        for command in commands {
            self.append_to_memtable(command.key, command.value, command.record_type, command.lsn);
        }
        self.sequence_number
            .store(final_sequence, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Apply a committed replicated command to memtable.
    ///
    /// This path intentionally does not write to WAL, because the entry is
    /// already durable via raft/WAL replication flow.
    pub async fn apply_replicated_command(&self, cmd: LogCommand) {
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
        // current memtable were full, we need to flush it to disk, so we need to create a new memtable and return the old memtable for flushing.
        let new_memtable = Arc::new(SkipMap::new());

        let old_memtable = std::mem::replace(&mut self.memtable, new_memtable);
        self.memtable_size
            .store(0, std::sync::atomic::Ordering::SeqCst);
        old_memtable
    }

    pub async fn flush_active_memtable(&mut self) -> Result<CheckpointResult, DBError> {
        let writer_lock = Arc::clone(&self.writer_lock);
        let _writer = writer_lock.lock().await;
        let snapshot = Arc::new(SkipMap::new());
        for entry in self.memtable.iter() {
            snapshot.insert(entry.key().clone(), entry.value().clone());
        }
        let result = self.flush(Arc::clone(&snapshot)).await?;
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

        let codec = SSTableCodec::new(block_entries, sparse_index, bloom_filter.clone());
        let (encoded, footer) = codec.serialize();

        let (smallest_key, largest_key) =
            SSTableCodec::range_from_index(&codec.index).ok_or_else(|| {
                DBError::Corrupted("cannot register SSTable without sparse index range".to_string())
            })?;

        let record_count = valid_records.len();

        tokio::fs::create_dir_all(self.sstable_dir.join("level-0")).await?;
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

        Ok(CheckpointResult {
            sstable_path: file_path.to_string_lossy().to_string(), // Will be set after flush_memtable
            record_count,
            level: 0,
            file_id, // Will be set after flush_memtable
            data: encoded,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
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
    async fn cancelled_batch_waiting_for_writer_lock_is_not_admitted() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let held = Arc::clone(&component.writer_lock).lock_owned().await;
        let writer = component.clone();
        let task = tokio::spawn(async move {
            writer
                .write_batch(vec![BatchOperation::Put {
                    key: b"cancelled".to_vec(),
                    value: b"value".to_vec(),
                }])
                .await
        });
        tokio::task::yield_now().await;
        task.abort();
        drop(held);
        tokio::task::yield_now().await;
        assert_eq!(component.current_sequence_number(), 0);
        assert!(component.active_memtable_handle().is_empty());
    }

    #[tokio::test]
    async fn cancelled_admitted_batch_finishes_publication() {
        let directory = tempfile::tempdir().unwrap();
        let component = build_write_component(directory.path()).await;
        let writer = component.clone();
        let task = tokio::spawn(async move {
            writer
                .write_batch(vec![BatchOperation::Put {
                    key: b"committed".to_vec(),
                    value: b"value".to_vec(),
                }])
                .await
        });
        tokio::task::yield_now().await;
        task.abort();
        let _guard = Arc::clone(&component.writer_lock).lock_owned().await;
        assert_eq!(component.current_sequence_number(), 1);
        assert_eq!(component.active_memtable_handle().len(), 1);
    }
}
