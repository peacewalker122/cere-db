//! WriteComponent - Handles checkpointing operations
//!
//! Responsible for:
//! 1. Converting memtable to SSTable
//! 2. Writing SSTable to disk
//! 3. Managing checkpoint metadata

use std::{path::PathBuf, sync::Arc};

use crossbeam_skiplist::SkipMap;
use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::storage::{
    bloom::BloomFilterWrapper,
    index::SparseIndexEntry,
    manifest_codec::{ManifestManager, SSTableMeta},
    record::{MemtableRecord, RecordType},
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
pub struct WriteComponent {
    /// Base directory for SSTable storage
    /// MemTable key is (key, lsn) composite for MVCC support (ADR-0002)
    memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
    memtable_size: std::sync::atomic::AtomicUsize,

    sequence_number: std::sync::atomic::AtomicU64,

    sstable_dir: PathBuf,
    wal_manager: Arc<WALManager>,
    manifest_manager: Arc<ManifestManager>,
}

impl WriteComponent {
    /// Create a new WriteComponent
    pub fn new(
        sstable_dir: PathBuf,
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
        sequence_number: u64, // this is from the wal recovery process
    ) -> Self {
        log::info!(
            "Initializing WriteComponent with sequence_number={}, sstable_dir={}",
            sequence_number,
            sstable_dir.display()
        );

        Self {
            memtable: Arc::new(SkipMap::new()),
            memtable_size: std::sync::atomic::AtomicUsize::new(0),
            sequence_number: std::sync::atomic::AtomicU64::new(sequence_number),
            sstable_dir,
            wal_manager,
            manifest_manager,
        }
    }

    /// Create a new WriteComponent with default directory
    pub fn with_default_dir(
        wal_manager: Arc<WALManager>,
        manifest_manager: Arc<ManifestManager>,
    ) -> Self {
        Self {
            memtable: Arc::new(SkipMap::new()),
            memtable_size: std::sync::atomic::AtomicUsize::new(0),
            sequence_number: std::sync::atomic::AtomicU64::new(0),
            sstable_dir: PathBuf::from("data"),
            wal_manager,
            manifest_manager,
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), std::io::Error> {
        let lsn = self
            .sequence_number
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.wal_manager
            .write_log(&key, &value, lsn, RecordType::Put)
            .await?;

        let value_len = value.len();
        self.memtable.insert(
            (key.clone(), lsn),
            MemtableRecord::new(value, RecordType::Put, lsn),
        );
        self.memtable_size
            .fetch_add(key.len() + value_len, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<(), std::io::Error> {
        let lsn = self
            .sequence_number
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.wal_manager
            .write_log(&key, &Vec::new(), lsn, RecordType::Delete)
            .await?;

        self.memtable.insert(
            (key.clone(), lsn),
            MemtableRecord::new(Vec::new(), RecordType::Delete, lsn),
        );
        self.memtable_size
            .fetch_add(key.len(), std::sync::atomic::Ordering::SeqCst);

        Ok(())
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
    ) -> Result<CheckpointResult, std::io::Error> {
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
        let mut bloom_filter = BloomFilterWrapper::with_rate(valid_records.len() + 10, 0.0001);

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
        let (encoded, _) = codec.serialize();

        let record_count = valid_records.len();

        let file_id = self.manifest_manager.allocate_file_id().await?;

        let file_path = self
            .sstable_dir
            .join(format!("level-0/sstable-{}.dat", file_id));

        // Write encoded SSTable data to disk
        self.save_buffer(&encoded, &file_path).await?;

        self.manifest_manager
            .register_sstable(SSTableMeta {
                file_id,
                level: 0,
                path: file_path.to_string_lossy().to_string(),
                record_count,
                bloom_bitmap: bloom_filter,
            })
            .await?;

        // wal rotate should be called after the checkpoint is successful, so we can use the wal_path to name the sstable file.
        let lsn = self.wal_manager.rotate_wal_file().await?;
        self.manifest_manager.mark_checkpoint(lsn, lsn).await?;

        Ok(CheckpointResult {
            sstable_path: file_path.to_string_lossy().to_string(), // Will be set after flush_memtable
            record_count,
            level: 0,
            file_id, // Will be set after flush_memtable
            data: encoded,
        })
    }

    pub async fn save_buffer(
        &self,
        buffer: &[u8],
        file_path: &PathBuf,
    ) -> Result<(), std::io::Error> {
        let mut file = tokio::fs::File::create(file_path).await?;
        file.write_all(buffer).await?;
        file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        manifest_codec::ManifestManager,
        record::{MemtableRecord, RecordType},
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
        )
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
        assert!(level0_files[0].bloom_bitmap.contains(b"key1"));
        // Note: flush() encodes data but doesn't write to disk - only registers metadata in manifest
        // result.data contains the encoded SSTable bytes
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
        let bloom = &level0_files[0].bloom_bitmap;
        assert!(bloom.contains(b"key-a"));
        assert!(bloom.contains(b"key-m"));
        assert!(bloom.contains(b"key-z"));

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
}
