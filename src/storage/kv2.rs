use std::{
    borrow::Cow,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
};

use crossbeam_channel::{Receiver, Sender, bounded};
use crossbeam_skiplist::SkipMap;

use crate::{
    api::api::AsyncKVEngine,
    error::DBError,
    storage::{
        compactionmanager::compaction::compaction,
        constant::{MAXIMUM_LEVEL_FILES, MEMTABLE_SIZE_THRESHOLD},
        manifest_codec::ManifestManager,
        readmanager::read::ReadManager,
        record::{MemtableRecord, RecordType},
        recovermanager::wal::{WALManager, WALRecord},
        writemanager::write::WriteComponent,
    },
};

/// Compaction trigger message sent from write path to compaction worker.
#[derive(Debug, Clone)]
pub enum CompactionTrigger {
    CompactLevel { level: u32 },
}

const DEFAULT_WAL_SEGMENT_SIZE: u64 = 1024 * 1024;

/// KV2 is the frontline orchestrator for storage managers.
///
/// Phase 1 scope:
/// - lifecycle wiring only (open/init)
/// - manager ownership topology
/// - KVEngine trait surface stubbed for later phases
pub struct KV2 {
    pub base_dir: PathBuf,
    pub wal_dir: PathBuf,
    pub sstable_dir: PathBuf,
    pub manifest_path: PathBuf,

    wal_manager: Arc<WALManager>,
    manifest: Arc<ManifestManager>, // Single source of truth (Arc) - shared with all components
    write_component: WriteComponent,
    read_manager: ReadManager,

    // Async compaction trigger channel (Step 1.3)
    pub compaction_sender: Sender<CompactionTrigger>,
    _compaction_receiver: Receiver<CompactionTrigger>,
}

impl KV2 {
    /// Open KV2 from a base directory and initialize all managers.
    ///
    /// Directory layout:
    /// - {base}/wal
    /// - {base}/sstable/level-0
    /// - {base}/MANIFEST
    pub async fn open(base_dir: impl AsRef<Path>) -> Result<Self, DBError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let wal_dir = base_dir.join("wal");
        let sstable_dir = base_dir.join("sstable");
        let level0_dir = sstable_dir.join("level-0");
        let manifest_path = base_dir.join("MANIFEST");

        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&level0_dir).await?;

        let wal_manager =
            Arc::new(WALManager::new(wal_dir.clone(), DEFAULT_WAL_SEGMENT_SIZE).await?);
        let recovered_wal_records = wal_manager.recover_records().await?;

        let (recovered_memtable, recovered_sequence, recovered_memtable_size) =
            Self::build_recovered_memtable(recovered_wal_records);

        // Single ManifestManager as source of truth - shared with all components
        let manifest = Arc::new(ManifestManager::load_or_create(manifest_path.clone()).await?);

        let mut write_component = WriteComponent::new(
            sstable_dir.clone(),
            Arc::clone(&wal_manager),
            Arc::clone(&manifest),
            recovered_sequence,
        );
        write_component.restore_memtable(recovered_memtable, recovered_memtable_size);

        let active_memtable = write_component.active_memtable_handle();
        let read_manager = ReadManager::new(active_memtable, Arc::clone(&manifest));

        // Step 1.4: Create mpsc channel for async compaction triggers
        let (compaction_sender, compaction_receiver) = bounded(16);
        let receiver_clone = compaction_receiver.clone();

        // Spawn rayon worker thread (Step 1.5) - pass shared Arc<ManifestManager>
        let manifest_for_worker = Arc::clone(&manifest);
        let sstable_dir_for_compaction = sstable_dir.clone();
        thread::spawn(move || {
            Self::run_compaction_worker(
                receiver_clone,
                manifest_for_worker,
                sstable_dir_for_compaction,
            );
        });

        Ok(Self {
            base_dir,
            wal_dir,
            sstable_dir,
            manifest_path,
            wal_manager,
            manifest,
            write_component,
            read_manager,
            compaction_sender,
            _compaction_receiver: compaction_receiver,
        })
    }

    /// Blocking convenience wrapper for sync call sites.
    pub fn open_blocking(base_dir: impl AsRef<Path>) -> Result<Self, DBError> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| DBError::StorageError(format!("failed to create tokio runtime: {e}")))?;

        runtime.block_on(Self::open(base_dir))
    }

    fn build_recovered_memtable(
        records: Vec<WALRecord>,
    ) -> (SkipMap<(Vec<u8>, u64), MemtableRecord>, u64, usize) {
        let recovered = SkipMap::new();
        let mut max_lsn = 0u64;
        let mut total_size = 0usize;

        for record in records {
            let record_type = match record.record_type {
                1 => RecordType::Put,
                2 => RecordType::Delete,
                _ => continue,
            };

            max_lsn = max_lsn.max(record.lsn);

            let key = record.key;
            let value = if record_type == RecordType::Delete {
                Vec::new()
            } else {
                record.value
            };

            total_size += key.len() + value.len();
            recovered.insert(
                (key.clone(), record.lsn),
                MemtableRecord::new(value, record_type, record.lsn),
            );
        }

        (recovered, max_lsn, total_size)
    }

    pub fn wal_manager(&self) -> Arc<WALManager> {
        Arc::clone(&self.wal_manager)
    }

    async fn maybe_flush_and_compact(&mut self) -> Result<(), DBError> {
        log::info!(
            "Checking if flush is needed. Current memtable size: {} bytes",
            self.write_component.memtable_size_bytes()
        );
        if self.write_component.memtable_size_bytes() < MEMTABLE_SIZE_THRESHOLD as usize {
            return Ok(());
        }

        let locked_memtable = self.write_component.lock_memtable().await;
        let active_memtable = self.write_component.active_memtable_handle();
        let flush_result = self.write_component.flush(locked_memtable).await?;

        log::debug!(
            "Flushed memtable to SSTable. Flushed size: {} bytes, SSTable path: {}",
            flush_result.data.len(),
            flush_result.sstable_path
        );
        self.read_manager.set_memtable(active_memtable);

        self.write_component
            .save_buffer(
                &flush_result.data,
                &PathBuf::from(&flush_result.sstable_path),
            )
            .await?;

        // Use shared manifest for snapshot check
        let snapshot = self.manifest.snapshot().await;
        let level0_count = snapshot.levels.get(&0).map_or(0, |files| files.len());

        if level0_count >= MAXIMUM_LEVEL_FILES {
            // Send trigger to rayon worker (non-blocking)
            let _ = self
                .compaction_sender
                .send(CompactionTrigger::CompactLevel { level: 0 });
        }

        Ok(())
    }

    /// Rayon worker that processes compaction triggers received via mpsc channel.
    /// Runs in a dedicated thread, using rayon for parallel compaction work.
    fn run_compaction_worker(
        receiver: Receiver<CompactionTrigger>,
        manifest: Arc<ManifestManager>,
        sstable_dir: PathBuf,
    ) {
        // Loop processing triggers from channel
        while let Ok(trigger) = receiver.recv() {
            let CompactionTrigger::CompactLevel { level } = trigger;
            let manifest = Arc::clone(&manifest);
            let sstable_dir = sstable_dir.clone();

            // Spawn parallel work with rayon
            rayon::scope(|s| {
                s.spawn(|_| {
                    // Create tokio runtime for async compaction
                    let rt = tokio::runtime::Runtime::new().ok();
                    if let Some(runtime) = rt {
                        runtime.block_on(async {
                            // Use shared manifest (Arc)
                            let _ = compaction(Arc::clone(&manifest), level).await;
                        });
                    }
                });
            });
        }
    }
}

impl AsyncKVEngine for KV2 {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        let sequence_number = self.write_component.current_sequence_number();
        let record = self.read_manager.get(key.to_vec(), sequence_number).await?;

        match record {
            Some(rec) if rec.record_type == RecordType::Delete => Ok(None),
            Some(rec) => Ok(Some(Cow::Owned(rec.value))),
            None => Ok(None),
        }
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        self.write_component.put(key, value).await?;
        self.maybe_flush_and_compact().await?;

        Ok(())
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        self.write_component.delete(key).await?;
        self.maybe_flush_and_compact().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::KV2;
    use crate::api::api::AsyncKVEngine;

    #[tokio::test]
    async fn open_initializes_storage_directories() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let kv2 = KV2::open(&temp_dir).await.unwrap();

        assert!(kv2.wal_dir.exists());
        assert!(kv2.sstable_dir.join("level-0").exists());
        assert!(kv2.manifest_path.exists());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn kvengine_put_get_delete_roundtrip() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let mut kv2 = KV2::open(&temp_dir).await.unwrap();
        kv2.put(b"k1".to_vec(), b"v1".to_vec()).await.unwrap();

        let value = kv2.get(b"k1").await.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"v1");

        kv2.delete(b"k1".to_vec()).await.unwrap();
        let deleted = kv2.get(b"k1").await.unwrap();
        assert!(deleted.is_none());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn open_recovers_written_value_from_wal_after_restart() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        {
            let mut kv2 = KV2::open(&temp_dir).await.unwrap();
            kv2.put(b"recovery-key".to_vec(), b"recovery-value".to_vec())
                .await
                .unwrap();
        }

        let reopened = KV2::open(&temp_dir).await.unwrap();
        let value = reopened.get(b"recovery-key").await.unwrap();

        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"recovery-value");

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
