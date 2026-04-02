//! WriteComponent - Handles checkpointing operations
//!
//! Responsible for:
//! 1. Converting memtable to SSTable
//! 2. Writing SSTable to disk
//! 3. Managing checkpoint metadata

use std::path::PathBuf;

use crossbeam_skiplist::SkipMap;
use tokio::io::AsyncWriteExt;

use crate::storage::{
    self, bloom,
    record::{Record, RecordType},
    recovermanager::wal::WALManager,
    sstable::{self, SSTable, SparseIndexEntry},
    writemanager::{
        block::BlockBuilder,
        manifest::{ManifestManager, SSTableMeta},
        record::MemtableRecord,
    },
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
}

/// WriteComponent handles checkpoint operations
///
/// Responsibilities:
/// - Convert memtable (SkipMap) to SSTable
/// - Write SSTable to disk
/// - Return checkpoint metadata
pub struct WriteComponent {
    /// Base directory for SSTable storage
    memtable: SkipMap<Vec<u8>, MemtableRecord>,

    sstable_dir: PathBuf,
    wal_manager: Box<WALManager>,
    manifest_manager: ManifestManager,
}

impl WriteComponent {
    /// Create a new WriteComponent
    pub fn new(
        sstable_dir: PathBuf,
        wal_manager: Box<WALManager>,
        manifest_manager: ManifestManager,
    ) -> Self {
        Self {
            memtable: SkipMap::new(),
            sstable_dir,
            wal_manager,
            manifest_manager,
        }
    }

    /// Create a new WriteComponent with default directory
    pub fn with_default_dir(
        wal_manager: Box<WALManager>,
        manifest_manager: ManifestManager,
    ) -> Self {
        Self {
            memtable: SkipMap::new(),
            sstable_dir: PathBuf::from("data"),
            wal_manager,
            manifest_manager,
        }
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
        memtable: SkipMap<Vec<u8>, MemtableRecord>,
    ) -> Result<CheckpointResult, std::io::Error> {
        let record_count = memtable.len();

        // iterate all memtable data and create the blocks with the sparse index.
        let mut sparse_index: Vec<SparseIndexEntry> = Vec::new();
        let mut block_records: Vec<u8> = Vec::new();

        let mut block_builder = BlockBuilder::new(0);
        let mut bloom_filter =
            storage::bloom::BloomFilterWrapper::with_rate(memtable.len(), 0.0001); // Example size and false positive rate

        let mut smallest_key: Vec<u8> = vec![];
        let mut largest_key: Vec<u8> = vec![];

        for entry in memtable.iter() {
            let key = entry.key();
            let record = entry.value();

            if smallest_key.is_empty() {
                smallest_key = entry.key().clone();
            }
            largest_key = entry.key().clone();

            bloom_filter.insert(key);
            match block_builder.add_record(key, record) {
                super::block::BlockBuilderState::EnoughSpace => {
                    // NOTHING
                }
                super::block::BlockBuilderState::Full(block, _) => {
                    // build the spars_index and add the block to the block_records
                    let block_offset = block_records.len() as u64;
                    sparse_index.push(SparseIndexEntry {
                        first_key: block.first_key.clone(),
                        block_offset,
                        last_key: block.last_key.clone(),
                        record_count: block.record_count,
                    });

                    let encode = block.encode();
                    bloom_filter.insert(key);
                    block_records.extend_from_slice(&encode);
                }
            }
        }

        let file_id = self.manifest_manager.allocate_file_id().await?;

        let file_path = self
            .sstable_dir
            .join(format!("level-0/sstable-{}.dat", file_id));
        let mut file = tokio::fs::File::create(&file_path).await?;

        let block_offset = block_records.len() as u64;
        file.write_all(&block_records).await?;

        let mut index_blocks: Vec<u8> = Vec::new();

        index_blocks.extend_from_slice(&(sparse_index.len() as u64).to_be_bytes());
        for entry in sparse_index.iter() {
            index_blocks.append(&mut entry.encode());
        }
        let index_checksum = crc32fast::hash(&index_blocks);

        // Write index blocks
        file.write_all(&index_blocks).await?;

        let mut bloom_blocks = bloom_filter.encode();
        let bloom_checksum = crc32fast::hash(&bloom_blocks);
        // Write bloom filter blocks
        file.write_all(&bloom_blocks).await?;

        let footer = sstable::SSTableFooter {
            data_block_start: 0,
            data_block_end: block_offset,
            index_block_start: block_offset,
            index_block_end: (block_offset + index_blocks.len() as u64),
            index_checksum,
            bloom_block_start: (block_offset + index_blocks.len() as u64),
            bloom_block_end: (block_offset + index_blocks.len() as u64 + bloom_blocks.len() as u64),
            bloom_checksum,
        };
        let footer_bytes = footer.encode();

        file.write_all(&footer_bytes).await?;
        file.sync_all().await?;

        self.manifest_manager
            .register_sstable(SSTableMeta {
                file_id,
                level: 0,
                path: file_path.to_string_lossy().to_string(),
                smallest_key,
                largest_key,
                record_count,
            })
            .await?;

        // wal rotate should be called after the checkpoint is successful, so we can use the wal_path to name the sstable file.
        let lsn = self.wal_manager.rotate_wal_file().await?;
        self.manifest_manager.mark_checkpoint(lsn, lsn).await?;

        Ok(CheckpointResult {
            sstable_path: String::new(), // Will be set after flush_memtable
            record_count,
            level: 0,
            file_id: 0, // Will be set after flush_memtable
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{
        record::RecordType,
        recovermanager::wal::WALManager,
        writemanager::{manifest::ManifestManager, record::MemtableRecord},
    };
    use crossbeam_skiplist::SkipMap;

    async fn build_write_component(temp_dir: &std::path::Path) -> WriteComponent {
        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest_manager = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();

        WriteComponent::new(
            temp_dir.join("sstable"),
            Box::new(wal_manager),
            manifest_manager,
        )
    }

    #[tokio::test]
    async fn flush_persists_memtable_and_updates_manifest() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        memtable.insert(
            b"key1".to_vec(),
            MemtableRecord::new(b"value1".to_vec(), RecordType::Put, 1),
        );
        memtable.insert(
            b"key2".to_vec(),
            MemtableRecord::new(Vec::new(), RecordType::Delete, 2),
        );

        let result = write_component.flush(memtable).await.unwrap();

        assert_eq!(result.record_count, 2);
        assert_eq!(result.level, 0);

        let snapshot = write_component.manifest_manager.snapshot().await;
        let level0_files = snapshot.levels.get(&0).unwrap();
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].record_count, 2);
        assert!(!level0_files[0].smallest_key.is_empty());
        assert!(!level0_files[0].largest_key.is_empty());
        assert!(std::path::Path::new(&level0_files[0].path).exists());

        assert!(snapshot.active_wal_segment > 0);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn flush_sets_smallest_and_largest_keys_from_sorted_memtable() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        memtable.insert(
            b"key-z".to_vec(),
            MemtableRecord::new(b"value-z".to_vec(), RecordType::Put, 1),
        );
        memtable.insert(
            b"key-a".to_vec(),
            MemtableRecord::new(b"value-a".to_vec(), RecordType::Put, 2),
        );
        memtable.insert(
            b"key-m".to_vec(),
            MemtableRecord::new(b"value-m".to_vec(), RecordType::Put, 3),
        );

        let _ = write_component.flush(memtable).await.unwrap();

        let snapshot = write_component.manifest_manager.snapshot().await;
        let level0_files = snapshot.levels.get(&0).unwrap();
        assert_eq!(level0_files.len(), 1);
        assert_eq!(level0_files[0].smallest_key, b"key-a".to_vec());
        assert_eq!(level0_files[0].largest_key, b"key-z".to_vec());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn flush_twice_registers_two_sstables_with_incrementing_ids() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let mut write_component = build_write_component(&temp_dir).await;

        let first_memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        first_memtable.insert(
            b"first-key".to_vec(),
            MemtableRecord::new(b"first-value".to_vec(), RecordType::Put, 1),
        );

        let second_memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        second_memtable.insert(
            b"second-key".to_vec(),
            MemtableRecord::new(b"second-value".to_vec(), RecordType::Put, 2),
        );

        let _ = write_component.flush(first_memtable).await.unwrap();
        let _ = write_component.flush(second_memtable).await.unwrap();

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

        let memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        memtable.insert(
            b"wal-key".to_vec(),
            MemtableRecord::new(b"wal-value".to_vec(), RecordType::Put, 1),
        );

        let _ = write_component.flush(memtable).await.unwrap();

        let snapshot_after_first = write_component.manifest_manager.snapshot().await;
        let first_segment = snapshot_after_first.active_wal_segment;
        assert!(first_segment > 0);

        let second_memtable: SkipMap<Vec<u8>, MemtableRecord> = SkipMap::new();
        second_memtable.insert(
            b"wal-key-2".to_vec(),
            MemtableRecord::new(b"wal-value-2".to_vec(), RecordType::Put, 2),
        );

        let _ = write_component.flush(second_memtable).await.unwrap();
        let snapshot_after_second = write_component.manifest_manager.snapshot().await;
        assert!(snapshot_after_second.active_wal_segment > first_segment);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
