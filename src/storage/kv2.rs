use std::{
    borrow::Cow,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use tokio::sync::mpsc::{Receiver, Sender};

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use crate::{
    api::api::{AsyncKVEngine, BatchOperation},
    error::DBError,
    storage::{
        compactionmanager::compaction::compaction,
        config::StorageConfig,
        manifest_codec::ManifestManager,
        raft::{RaftConsensusLayer, RaftNodeConfig},
        readmanager::read::ReadManager,
        record::{MemtableRecord, RecordType},
        recovermanager::{
            log_store::{LogCommand, LogStore},
            wal::{WALManager, WALRecord},
        },
        writemanager::write::WriteComponent,
    },
};

/// Compaction trigger message sent from write path to compaction worker.
#[derive(Debug, Clone)]
pub enum CompactionTrigger {
    CompactLevel { level: u32 },
}

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

    pub config: Arc<StorageConfig>,
    wal_manager: Arc<WALManager>,
    manifest: Arc<ManifestManager>, // Single source of truth (Arc) - shared with all components
    write_component: WriteComponent,
    read_manager: ReadManager,

    // Async compaction trigger channel (Step 1.3)
    pub compaction_sender: Sender<CompactionTrigger>,
    compaction_cancel: tokio_util::sync::CancellationToken,
    worker_stop: tokio_util::sync::CancellationToken,
    compaction_worker: Option<tokio::task::JoinHandle<()>>,
}

impl KV2 {
    pub async fn shutdown(&mut self) -> Result<(), DBError> {
        self.write_component.await_pending_writes().await;
        self.worker_stop.cancel();
        if let Some(worker) = self.compaction_worker.take() {
            worker
                .await
                .map_err(|error| DBError::StorageError(error.to_string()))?;
        }
        Ok(())
    }

    /// Open KV2 from a base directory with custom configuration.
    ///
    /// Directory layout:
    /// - {base}/wal
    /// - {base}/sstable/level-0
    /// - {base}/MANIFEST
    pub async fn open(base_dir: impl AsRef<Path>, config: StorageConfig) -> Result<Self, DBError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let wal_dir = base_dir.join("wal");
        let sstable_dir = base_dir.join("sstable");
        let level0_dir = sstable_dir.join("level-0");
        let manifest_path = base_dir.join("MANIFEST");

        crate::storage::durable_dir::create_dir_all_durable(&base_dir).await?;
        crate::storage::durable_dir::create_dir_all_durable(&wal_dir).await?;
        crate::storage::durable_dir::create_dir_all_durable(&level0_dir).await?;

        let cancel = tokio_util::sync::CancellationToken::new();

        let config = Arc::new(config);

        let manifest = Arc::new(ManifestManager::load_or_create(manifest_path.clone()).await?);
        let manifest_snapshot = manifest.snapshot().await;
        for table in manifest_snapshot.levels.values().flatten() {
            Self::validate_manifest_sstable(table).await?;
        }
        let wal_manager = Arc::new(
            WALManager::new_with_checkpoint(
                wal_dir.clone(),
                config.wal_segment_size,
                manifest_snapshot.recovery_checkpoint,
            )
            .await?,
        );
        let recovered_wal_records = wal_manager.recover_records().await?;
        if let Some(checkpoint) = manifest_snapshot.recovery_checkpoint {
            if let Err(error) = wal_manager.reclaim_checkpointed_segments(checkpoint).await {
                log::warn!("WAL checkpoint reclamation will retry later: {error}");
            }
        }

        let (recovered_memtable, recovered_wal_sequence, recovered_memtable_size) =
            Self::build_recovered_memtable(recovered_wal_records);
        let recovered_sequence = recovered_wal_sequence.max(
            manifest_snapshot
                .recovery_checkpoint
                .map_or(0, |checkpoint| checkpoint.committed_sequence),
        );

        let mut write_component = WriteComponent::new(
            sstable_dir.clone(),
            Arc::clone(&wal_manager),
            Arc::clone(&manifest),
            recovered_sequence,
            Arc::clone(&config),
        );
        write_component.restore_memtable(recovered_memtable, recovered_memtable_size);

        let active_memtable = write_component.active_memtable_handle();
        let read_manager = ReadManager::new(active_memtable, Arc::clone(&manifest));

        // Step 1.4: Create mpsc channel for async compaction triggers
        let (compaction_sender, compaction_receiver) = tokio::sync::mpsc::channel(100);

        // Spawn rayon worker thread (Step 1.5) - pass shared Arc<ManifestManager>
        let manifest_for_worker = Arc::clone(&manifest);
        let sstable_dir_for_compaction = sstable_dir.clone();
        let config_for_compaction = Arc::clone(&config);
        let worker_stop = tokio_util::sync::CancellationToken::new();
        let compaction_worker = Self::run_compaction_worker(
            compaction_receiver,
            manifest_for_worker,
            sstable_dir_for_compaction,
            config_for_compaction,
            cancel.child_token(),
            worker_stop.clone(),
        );

        Ok(Self {
            base_dir,
            wal_dir,
            sstable_dir,
            manifest_path,
            config,
            wal_manager,
            manifest,
            write_component,
            read_manager,
            compaction_sender,
            compaction_cancel: cancel,
            worker_stop,
            compaction_worker: Some(compaction_worker),
        })
    }

    /// Open KV2 with default configuration.
    ///
    /// Convenience wrapper for backward compatibility with existing code.
    pub async fn open_with_defaults(base_dir: impl AsRef<Path>) -> Result<Self, DBError> {
        Self::open(base_dir, StorageConfig::default()).await
    }

    async fn validate_manifest_sstable(
        table: &crate::storage::manifest_codec::SSTableMeta,
    ) -> Result<(), DBError> {
        use tokio::io::{AsyncSeekExt, BufReader};

        let mut file = tokio::fs::File::open(&table.path).await.map_err(|error| {
            DBError::Corrupted(format!(
                "cannot open manifest SSTable {}: {error}",
                table.path
            ))
        })?;
        let file_len = file.metadata().await?.len();
        if file_len < crate::storage::footer::FOOTER_SIZE {
            return Err(DBError::Corrupted(format!(
                "manifest SSTable is shorter than its footer: {}",
                table.path
            )));
        }

        let footer =
            crate::storage::sstable_codec::SSTableCodec::deserialize_footer(&mut file).await?;
        if footer.data_block_start != 0
            || footer.data_block_end != footer.index_block_start
            || footer.index_block_start > footer.index_block_end
            || footer.index_block_end != footer.bloom_block_start
            || footer.bloom_block_start > footer.bloom_block_end
            || footer
                .bloom_block_end
                .checked_add(crate::storage::footer::FOOTER_SIZE)
                != Some(file_len)
        {
            return Err(DBError::Corrupted(format!(
                "invalid SSTable section offsets: {}",
                table.path
            )));
        }

        let preflight_bytes = std::fs::read(&table.path)?;
        crate::storage::sstable_preflight::validate(&preflight_bytes, &footer)?;

        let mut checksum_file = std::fs::File::open(&table.path)?;
        let checked_index =
            crate::storage::footer::read_sstable_sparse_index(&mut checksum_file, &footer)?;
        crate::storage::footer::read_sstable_bloom(&mut checksum_file, &footer)?;

        file.seek(std::io::SeekFrom::Start(0)).await?;
        let mut reader = BufReader::new(file);
        let (decoded_footer, index, _) =
            crate::storage::sstable_codec::SSTableCodec::deserialize_sections(&mut reader).await?;
        let index_records = index
            .iter()
            .try_fold(0usize, |count, entry| {
                count.checked_add(entry.record_count as usize)
            })
            .ok_or_else(|| DBError::Corrupted("SSTable record count overflow".into()))?;
        if index.is_empty()
            || checked_index.len() != index.len()
            || index_records != table.record_count
            || index
                .first()
                .is_none_or(|entry| entry.first_key != table.smallest_key)
            || index
                .last()
                .is_none_or(|entry| entry.last_key != table.largest_key)
        {
            return Err(DBError::Corrupted(format!(
                "SSTable index does not match manifest metadata: {}",
                table.path
            )));
        }
        let blocks = crate::storage::sstable_codec::SSTableCodec::get_all_blocks(
            &mut reader,
            &decoded_footer,
            &index,
        )
        .await?;
        let mut actual_count = 0usize;
        for (entry, block) in index.iter().zip(blocks) {
            let records = block.data.ok_or_else(|| {
                DBError::Corrupted(format!("SSTable block has no records: {}", table.path))
            })?;
            if records.is_empty()
                || records.len() != entry.record_count as usize
                || records
                    .first()
                    .is_none_or(|record| record.key != entry.first_key)
                || records
                    .last()
                    .is_none_or(|record| record.key != entry.last_key)
                || block.first_key != entry.first_key
                || block.last_key != entry.last_key
            {
                return Err(DBError::Corrupted(format!(
                    "SSTable block does not match sparse index: {}",
                    table.path
                )));
            }
            actual_count = actual_count
                .checked_add(records.len())
                .ok_or_else(|| DBError::Corrupted("SSTable record count overflow".into()))?;
        }
        if actual_count != table.record_count {
            return Err(DBError::Corrupted(format!(
                "SSTable record count does not match manifest: {}",
                table.path
            )));
        }
        Ok(())
    }

    /// Blocking convenience wrapper for sync call sites with default config.
    pub fn open_blocking(base_dir: impl AsRef<Path>) -> Result<Self, DBError> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| DBError::StorageError(format!("failed to create tokio runtime: {e}")))?;

        runtime.block_on(Self::open_with_defaults(base_dir))
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
        if self.write_component.memtable_size_bytes() < self.config.memtable_size_threshold as usize
        {
            return Ok(());
        }

        let flush_result = self.write_component.flush_active_memtable().await?;
        let active_memtable = self.write_component.active_memtable_handle();

        log::debug!(
            "Flushed memtable to SSTable. Flushed size: {} bytes, SSTable path: {}",
            flush_result.data.len(),
            flush_result.sstable_path
        );
        self.read_manager.set_memtable(active_memtable);

        // Use shared manifest for snapshot check
        let snapshot = self.manifest.snapshot().await;
        let level0_count = snapshot.levels.get(&0).map_or(0, |files| files.len());

        if level0_count >= self.config.max_level0_files {
            let _ = self
                .compaction_sender
                .send(CompactionTrigger::CompactLevel { level: 0 })
                .await;
        }

        Ok(())
    }

    async fn maintain_after_commit(&mut self) {
        if let Err(error) = self.maybe_flush_and_compact().await {
            log::error!("Post-commit flush or compaction maintenance failed: {error}");
        }
    }

    /// Rayon worker that processes compaction triggers received via mpsc channel.
    /// Runs in a dedicated thread, using rayon for parallel compaction work.
    fn run_compaction_worker(
        mut receiver: Receiver<CompactionTrigger>,
        manifest: Arc<ManifestManager>,
        sstable_dir: PathBuf,
        config: Arc<StorageConfig>,
        cancel: tokio_util::sync::CancellationToken,
        worker_stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(trigger) = receiver.recv() =>  {
                        match trigger {
                            CompactionTrigger::CompactLevel { level } => {
                                log::info!("Received compaction trigger for level {}", level);
                                // Perform compaction in a blocking thread to avoid blocking async runtime
                                let manifest_for_thread = Arc::clone(&manifest);
                                let config_for_thread = Arc::clone(&config);

                                if let Err(e) = compaction(manifest_for_thread, level, config_for_thread, cancel.child_token()).await {
                                    log::error!("Compaction failed for level {}: {}", level, e);
                                } else {
                                    log::info!("Compaction completed for level {}", level);
                                }
                            }
                        }
                    },
                    _ = worker_stop.cancelled() => {
                        log::info!("Compaction worker received shutdown signal");
                        receiver.close();
                        while let Some(CompactionTrigger::CompactLevel { level }) = receiver.recv().await {
                            if let Err(error) = compaction(
                                Arc::clone(&manifest),
                                level,
                                Arc::clone(&config),
                                cancel.child_token(),
                            )
                            .await
                            {
                                log::error!("Compaction failed for level {}: {}", level, error);
                            }
                        }
                        break;
                    }
                    else => break,
                }
            }
        })
    }
}

impl Drop for KV2 {
    fn drop(&mut self) {
        self.compaction_cancel.cancel();
        self.worker_stop.cancel();
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
        self.maintain_after_commit().await;

        Ok(())
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        self.write_component.delete(key).await?;
        self.maintain_after_commit().await;

        Ok(())
    }

    async fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
        if operations.is_empty() {
            return Ok(());
        }
        self.write_component.write_batch(operations).await?;
        self.maintain_after_commit().await;
        Ok(())
    }

    async fn await_pending_writes(&self) -> Result<(), DBError> {
        self.write_component.await_pending_writes().await;
        Ok(())
    }

    async fn scan(
        &self,
        range: impl RangeBounds<Vec<u8>> + Send,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        let sequence_number = self.write_component.current_sequence_number();
        self.read_manager.scan_range(range, sequence_number).await
    }
}

#[cfg(test)]
mod tests {
    use super::KV2;
    use crate::api::api::AsyncKVEngine;
    use crate::storage::record::{MemtableRecord, RecordType};
    use crate::{BatchOperation, Bm25Config, DocumentId, FtsIndex, FtsQuery, StorageConfig};
    use crossbeam_skiplist::SkipMap;
    use std::sync::Arc;

    async fn wal_files(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut entries = tokio::fs::read_dir(directory).await.unwrap();
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(".log"))
            {
                files.push(entry.path());
            }
        }
        files.sort();
        files
    }

    #[tokio::test]
    async fn open_initializes_storage_directories() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();

        assert!(kv2.wal_dir.exists());
        assert!(kv2.sstable_dir.join("level-0").exists());
        assert!(kv2.manifest_path.exists());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn shutdown_joins_idle_compaction_worker_before_reopen() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        for _ in 0..3 {
            let mut engine = KV2::open_with_defaults(&temp_dir).await.unwrap();
            let worker = engine.compaction_worker.as_ref().unwrap();
            assert!(!worker.is_finished());
            engine.shutdown().await.unwrap();
            assert!(engine.compaction_worker.is_none());
            drop(engine);
        }
        let mut reopened = KV2::open_with_defaults(&temp_dir).await.unwrap();
        reopened.shutdown().await.unwrap();
        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn kvengine_put_get_delete_roundtrip() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();
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
            let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();
            kv2.put(b"recovery-key".to_vec(), b"recovery-value".to_vec())
                .await
                .unwrap();
            kv2.shutdown().await.unwrap();
        }

        let reopened = KV2::open_with_defaults(&temp_dir).await.unwrap();
        let value = reopened.get(b"recovery-key").await.unwrap();

        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"recovery-value");

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn repeated_trusted_checkpoints_reclaim_prefix_and_restore_sstable_state() {
        let directory = tempfile::tempdir().unwrap();
        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        engine
            .put(b"keep".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine.wal_manager.rotate_wal_file().await.unwrap();
        engine.wal_manager.rotate_wal_file().await.unwrap();
        let before = wal_files(&engine.wal_dir).await;
        let mut before_bytes = 0;
        for path in &before {
            before_bytes += tokio::fs::metadata(path).await.unwrap().len();
        }
        assert_eq!(before.len(), 3);
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let after_first_checkpoint = wal_files(&engine.wal_dir).await;
        let after_first_bytes: u64 = {
            let mut bytes = 0;
            for path in &after_first_checkpoint {
                bytes += tokio::fs::metadata(path).await.unwrap().len();
            }
            bytes
        };
        assert!(after_first_checkpoint.len() < before.len());
        assert!(after_first_bytes < before_bytes);

        for (key, value) in [
            (b"replace".to_vec(), b"old".to_vec()),
            (b"delete".to_vec(), b"gone".to_vec()),
        ] {
            engine.put(key, value).await.unwrap();
            engine
                .write_component
                .flush_active_memtable()
                .await
                .unwrap();
        }
        assert_eq!(wal_files(&engine.wal_dir).await.len(), 1);
        let retained = wal_files(&engine.wal_dir).await;
        let retained_bytes = tokio::fs::metadata(&retained[0]).await.unwrap().len();
        assert_eq!(
            retained_bytes,
            crate::storage::constant::WAL_HEADER_SIZE as u64
        );
        assert_eq!(
            engine
                .manifest
                .snapshot()
                .await
                .recovery_checkpoint
                .unwrap()
                .replay_start_segment,
            5
        );

        engine
            .put(b"replace".to_vec(), b"new".to_vec())
            .await
            .unwrap();
        engine.delete(b"delete".to_vec()).await.unwrap();
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        assert_eq!(wal_files(&engine.wal_dir).await.len(), 1);
        drop(engine);

        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(engine.write_component.current_sequence_number(), 5);
        assert_eq!(
            engine.get(b"keep").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        assert_eq!(
            engine.get(b"replace").await.unwrap().unwrap().as_ref(),
            b"new"
        );
        assert!(engine.get(b"delete").await.unwrap().is_none());
        engine
            .write_batch(vec![
                BatchOperation::Put {
                    key: b"after-reopen".to_vec(),
                    value: b"next".to_vec(),
                },
                BatchOperation::Put {
                    key: b"batch-second".to_vec(),
                    value: b"second".to_vec(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(engine.write_component.current_sequence_number(), 7);
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        drop(engine);

        let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(engine.write_component.current_sequence_number(), 7);
        assert_eq!(
            engine.get(b"after-reopen").await.unwrap().unwrap().as_ref(),
            b"next"
        );
        assert_eq!(
            engine.get(b"batch-second").await.unwrap().unwrap().as_ref(),
            b"second"
        );
        assert_eq!(wal_files(&directory.path().join("wal")).await.len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_reopen_retries_stale_prefix_cleanup_and_ignores_covered_corruption() {
        let directory = tempfile::tempdir().unwrap();
        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        for sequence in 1..=2 {
            engine
                .put(vec![b'k', sequence], vec![b'v', sequence])
                .await
                .unwrap();
            engine
                .write_component
                .flush_active_memtable()
                .await
                .unwrap();
        }
        let checkpoint = engine
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        assert_eq!(checkpoint.replay_start_segment, 2);
        let old_file = engine.wal_dir.join("00000000000000000000.log");
        let blocked_file = engine.wal_dir.join("00000000000000000001.log");
        tokio::fs::write(&old_file, b"covered corrupt WAL")
            .await
            .unwrap();
        tokio::fs::create_dir(&blocked_file).await.unwrap();
        drop(engine);

        let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(
            engine.get(&[b'k', 1]).await.unwrap().unwrap().as_ref(),
            &[b'v', 1]
        );
        assert!(!old_file.exists());
        assert!(blocked_file.is_dir());
        drop(engine);

        tokio::fs::remove_dir(&blocked_file).await.unwrap();
        let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert!(wal_files(&engine.wal_dir).await.iter().all(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".log"))
                .and_then(|id| id.parse::<u64>().ok())
                .is_none_or(|id| id >= checkpoint.replay_start_segment)
        }));
    }

    #[tokio::test]
    async fn partial_flush_and_untracked_append_preserve_post_checkpoint_wal() {
        let directory = tempfile::tempdir().unwrap();
        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        engine
            .put(b"base".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let checkpoint = engine
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        assert_eq!(checkpoint.replay_start_segment, 1);

        engine
            .wal_manager
            .write_log(
                &b"untracked".to_vec(),
                &b"raw".to_vec(),
                100,
                RecordType::Put,
            )
            .await
            .unwrap();
        engine
            .put(b"engine".to_vec(), b"tracked".to_vec())
            .await
            .unwrap();
        engine
            .write_component
            .flush(Arc::new({
                let partial = SkipMap::new();
                partial.insert(
                    (b"provided".to_vec(), 101),
                    MemtableRecord::new(b"partial".to_vec(), RecordType::Put, 101),
                );
                partial
            }))
            .await
            .unwrap();

        assert!(engine.wal_dir.join("00000000000000000001.log").exists());
        assert_eq!(
            engine
                .manifest
                .snapshot()
                .await
                .recovery_checkpoint
                .unwrap(),
            checkpoint
        );
    }

    #[tokio::test]
    async fn public_partial_flush_does_not_authorize_wal_reclamation() {
        let directory = tempfile::tempdir().unwrap();
        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        engine
            .put(b"engine".to_vec(), b"tracked".to_vec())
            .await
            .unwrap();
        let partial = SkipMap::new();
        partial.insert(
            (b"provided".to_vec(), 100),
            MemtableRecord::new(b"partial".to_vec(), RecordType::Put, 100),
        );
        engine
            .write_component
            .flush(Arc::new(partial))
            .await
            .unwrap();
        assert!(engine.wal_dir.join("00000000000000000000.log").exists());
        assert!(
            engine
                .manifest
                .snapshot()
                .await
                .recovery_checkpoint
                .is_none()
        );

        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        assert_eq!(wal_files(&engine.wal_dir).await.len(), 1);
        assert!(!engine.wal_dir.join("00000000000000000000.log").exists());
    }

    #[tokio::test]
    async fn cancellation_after_checkpoint_publication_leaves_reopenable_cleanup() {
        let directory = tempfile::tempdir().unwrap();
        let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        engine
            .put(b"durable".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let checkpoint = engine
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        let covered = engine
            .wal_dir
            .join(format!("{:020}.log", checkpoint.replay_start_segment - 1));
        tokio::fs::write(&covered, b"leftover covered segment")
            .await
            .unwrap();

        let wal_manager = Arc::clone(&engine.wal_manager);
        let mut cleanup = Box::pin(wal_manager.reclaim_checkpointed_segments(checkpoint));
        let mut cleanup_was_pending = false;
        let mut cleanup_completed = false;
        for _ in 0..3 {
            let result = std::future::poll_fn(|context| {
                match std::future::Future::poll(cleanup.as_mut(), context) {
                    std::task::Poll::Pending => std::task::Poll::Ready(None),
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(Some(result)),
                }
            })
            .await;
            if let Some(result) = result {
                result.unwrap();
                cleanup_completed = true;
                break;
            }
            cleanup_was_pending = true;
        }
        assert!(cleanup_was_pending && !cleanup_completed);
        drop(cleanup);
        drop(engine);

        let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(
            engine.get(b"durable").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        let checkpoint = engine
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        assert!(!covered.exists());
        assert!(wal_files(&engine.wal_dir).await.iter().all(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(".log"))
                .and_then(|id| id.parse::<u64>().ok())
                .is_none_or(|id| id >= checkpoint.replay_start_segment)
        }));
    }

    #[tokio::test]
    async fn reclaimed_wal_reopens_fts_values_and_bm25_from_sstables() {
        let directory = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: usize::MAX as u64,
            ..StorageConfig::default()
        };
        let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
        let mut index = FtsIndex::open(engine).await.unwrap();
        index
            .put_document(DocumentId::new("alpha").unwrap(), b"rare common".to_vec())
            .await
            .unwrap();
        index
            .put_document(
                DocumentId::new("beta").unwrap(),
                b"rare rare common".to_vec(),
            )
            .await
            .unwrap();
        index
            .put_document(DocumentId::new("alpha").unwrap(), b"fresh rare".to_vec())
            .await
            .unwrap();
        index
            .delete_document(&DocumentId::new("beta").unwrap())
            .await
            .unwrap();
        let mut engine = index.into_inner();
        engine
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        assert_eq!(wal_files(&engine.wal_dir).await.len(), 1);
        drop(engine);

        let engine = KV2::open(directory.path(), config).await.unwrap();
        let index = FtsIndex::open(engine).await.unwrap();
        assert_eq!(
            index
                .get_document(&DocumentId::new("alpha").unwrap())
                .await
                .unwrap()
                .unwrap(),
            b"fresh rare"
        );
        assert!(
            index
                .get_document(&DocumentId::new("beta").unwrap())
                .await
                .unwrap()
                .is_none()
        );
        let ranked = index
            .search_ranked(FtsQuery::Or(vec!["rare".into()]), 10, Bm25Config::default())
            .await
            .unwrap();
        assert_eq!(ranked.len(), 1);
        assert_eq!(ranked[0].id.as_str(), "alpha");
        assert!(ranked[0].score.is_finite());
    }

    #[tokio::test]
    async fn reopen_uses_checkpoint_for_disk_values_tombstones_and_sequence() {
        let temp_dir = tempfile::tempdir().unwrap();
        {
            let mut database = KV2::open_with_defaults(temp_dir.path()).await.unwrap();
            database
                .put(b"kept".to_vec(), b"value".to_vec())
                .await
                .unwrap();
            database
                .put(b"gone".to_vec(), b"old".to_vec())
                .await
                .unwrap();
            database.delete(b"gone".to_vec()).await.unwrap();
            database
                .write_component
                .flush_active_memtable()
                .await
                .unwrap();
            let checkpoint = database
                .manifest
                .snapshot()
                .await
                .recovery_checkpoint
                .unwrap();
            assert_eq!(checkpoint.committed_sequence, 3);
        }
        let mut database = KV2::open_with_defaults(temp_dir.path()).await.unwrap();
        assert_eq!(database.write_component.current_sequence_number(), 3);
        assert_eq!(
            database.get(b"kept").await.unwrap().unwrap().as_ref(),
            b"value"
        );
        assert!(database.get(b"gone").await.unwrap().is_none());
        database
            .put(b"next".to_vec(), b"write".to_vec())
            .await
            .unwrap();
        assert_eq!(database.write_component.current_sequence_number(), 4);
        let checkpoint = database
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        let recovered = database.wal_manager.recover_records().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].lsn, 4);
        assert!(
            checkpoint.next_physical_lsn
                < database.wal_manager.recovery_position().await.unwrap().1
        );
    }

    #[tokio::test]
    async fn open_fails_closed_when_checkpoint_sstable_is_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut database = KV2::open_with_defaults(temp_dir.path()).await.unwrap();
        database
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        database
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let path = database.manifest.snapshot().await.levels[&0][0]
            .path
            .clone();
        drop(database);
        tokio::fs::remove_file(path).await.unwrap();
        assert!(KV2::open_with_defaults(temp_dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn invalid_checkpoint_sstable_preserves_restored_covered_wal() {
        let directory = tempfile::tempdir().unwrap();
        let mut database = KV2::open_with_defaults(directory.path()).await.unwrap();
        database
            .put(b"checkpoint-key".to_vec(), b"checkpoint-value".to_vec())
            .await
            .unwrap();
        let covered_wal = wal_files(&database.wal_dir).await[0].clone();
        let covered_bytes = tokio::fs::read(&covered_wal).await.unwrap();
        database.wal_manager.rotate_wal_file().await.unwrap();
        database.wal_manager.rotate_wal_file().await.unwrap();
        database
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let table_path = database.manifest.snapshot().await.levels[&0][0]
            .path
            .clone();
        drop(database);

        tokio::fs::write(&covered_wal, &covered_bytes)
            .await
            .unwrap();
        tokio::fs::write(&table_path, []).await.unwrap();

        assert!(KV2::open_with_defaults(directory.path()).await.is_err());
        assert_eq!(tokio::fs::read(covered_wal).await.unwrap(), covered_bytes);
    }

    #[tokio::test]
    async fn valid_checkpoint_sstable_still_reopens() {
        let directory = tempfile::tempdir().unwrap();
        let mut database = KV2::open_with_defaults(directory.path()).await.unwrap();
        database
            .put(b"checkpoint-key".to_vec(), b"checkpoint-value".to_vec())
            .await
            .unwrap();
        database
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        drop(database);

        let reopened = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(
            reopened
                .get(b"checkpoint-key")
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            b"checkpoint-value"
        );
    }

    #[tokio::test]
    async fn open_fails_closed_when_checkpoint_wal_segment_is_missing() {
        let temp_dir = tempfile::tempdir().unwrap();
        let mut database = KV2::open_with_defaults(temp_dir.path()).await.unwrap();
        database
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        database
            .write_component
            .flush_active_memtable()
            .await
            .unwrap();
        let checkpoint = database
            .manifest
            .snapshot()
            .await
            .recovery_checkpoint
            .unwrap();
        drop(database);
        tokio::fs::remove_file(
            temp_dir
                .path()
                .join(format!("wal/{:020}.log", checkpoint.replay_start_segment)),
        )
        .await
        .unwrap();
        assert!(KV2::open_with_defaults(temp_dir.path()).await.is_err());
    }

    #[tokio::test]
    async fn open_with_custom_config() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        // Create custom config with different parameters
        let custom_config = crate::storage::config::StorageConfig::builder()
            .memtable_size_threshold(8 * 1024 * 1024) // 8 MB instead of 4 MB
            .max_level0_files(4) // 4 instead of 2
            .build()
            .unwrap();

        let kv2 = KV2::open(&temp_dir, custom_config.clone()).await.unwrap();

        // Verify config was set correctly
        assert_eq!(kv2.config.memtable_size_threshold, 8 * 1024 * 1024);
        assert_eq!(kv2.config.max_level0_files, 4);
        assert_eq!(kv2.config.sstable_block_size, 4096); // default

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn scan_returns_all_keys_in_range() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();

        kv2.put(b"alpha".to_vec(), b"value_a".to_vec())
            .await
            .unwrap();
        kv2.put(b"beta".to_vec(), b"value_b".to_vec())
            .await
            .unwrap();
        kv2.put(b"gamma".to_vec(), b"value_c".to_vec())
            .await
            .unwrap();

        // Scan full range
        let results = kv2.scan(..).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"alpha");
        assert_eq!(results[1].0, b"beta");
        assert_eq!(results[2].0, b"gamma");

        // Scan sub-range
        let results = kv2
            .scan(b"beta".to_vec()..=b"gamma".to_vec())
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"beta");
        assert_eq!(results[1].0, b"gamma");

        // Scan empty range
        let results = kv2.scan(b"x".to_vec()..b"z".to_vec()).await.unwrap();
        assert!(results.is_empty());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn scan_filters_tombstones() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();

        kv2.put(b"keep".to_vec(), b"value".to_vec()).await.unwrap();
        kv2.put(b"remove".to_vec(), b"temp".to_vec()).await.unwrap();
        kv2.delete(b"remove".to_vec()).await.unwrap();

        let results = kv2.scan(..).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"keep");

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn scan_returns_newest_version_of_overwritten_keys() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());

        let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();

        kv2.put(b"key".to_vec(), b"old_value".to_vec())
            .await
            .unwrap();
        kv2.put(b"key".to_vec(), b"new_value".to_vec())
            .await
            .unwrap();

        let results = kv2.scan(..).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, b"new_value");

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn scans_on_shared_memtable_observe_complete_batch_generations() {
        use crate::api::api::BatchOperation;
        use std::sync::Arc;
        use tokio::sync::Barrier;

        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        let mut kv2 = KV2::open_with_defaults(&temp_dir).await.unwrap();
        let keys = 32;
        let operations = move |generation: usize| {
            (0..keys)
                .map(|index| BatchOperation::Put {
                    key: format!("key-{index:02}").into_bytes(),
                    value: generation.to_string().into_bytes(),
                })
                .collect::<Vec<_>>()
        };
        kv2.write_component
            .write_batch(operations(0))
            .await
            .unwrap();

        let writer = kv2.write_component.clone();
        let barrier = Arc::new(Barrier::new(2));
        let writer_barrier = Arc::clone(&barrier);
        let writer_task = tokio::spawn(async move {
            let mut sequences = Vec::new();
            for generation in 1..=16 {
                writer_barrier.wait().await;
                writer.write_batch(operations(generation)).await.unwrap();
                sequences.push(writer.current_sequence_number());
                writer_barrier.wait().await;
            }
            sequences
        });

        for _ in 1..=16 {
            let watermark = kv2.write_component.current_sequence_number();
            barrier.wait().await;
            barrier.wait().await;
            let scanned = kv2.read_manager.scan_range(.., watermark).await.unwrap();
            assert_eq!(scanned.len(), keys);
            let expected_generation = (watermark / keys as u64) - 1;
            assert!(
                scanned
                    .iter()
                    .all(|(_, value)| { value == expected_generation.to_string().as_bytes() })
            );
        }

        let sequences = writer_task.await.unwrap();
        assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(sequences.last().copied(), Some((keys * 17) as u64));
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}

// ============================================================================
// RaftKV2 — Raft-backed KV engine
// ============================================================================

/// Raft-backed KV engine.
///
/// Behaviour summary:
///
/// | Operation | Leader | Follower |
/// |-----------|--------|----------|
/// | `put`     | Propose to Raft -> apply on commit | Reject / redirect |
/// | `delete`  | Propose to Raft -> apply on commit | Reject / redirect |
/// | `get`     | Local read (state machine) | Local read (may lag slightly) |
/// | `scan`    | Local read | Local read (may lag slightly) |
pub struct RaftKV2 {
    base_dir: PathBuf,
    config: Arc<StorageConfig>,
    _manifest: Arc<ManifestManager>,
    read_manager: ReadManager,
    compaction_sender: Sender<CompactionTrigger>,
    /// Shared sequence number — updated by both the leader propose path and
    /// the state machine apply path.
    sequence_number: Arc<AtomicU64>,
    raft_layer: RaftConsensusLayer,
    /// Shared handle to the write pipeline — same instance as in the state machine.
    write_handle: Arc<RwLock<WriteComponent>>,
    compaction_cancel: tokio_util::sync::CancellationToken,
    worker_stop: tokio_util::sync::CancellationToken,
    compaction_worker: Option<tokio::task::JoinHandle<()>>,
}

impl RaftKV2 {
    /// Open or create a Raft-backed KV store.
    pub async fn open(
        base_dir: impl AsRef<Path>,
        config: StorageConfig,
        raft_config: RaftNodeConfig,
    ) -> Result<Self, DBError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        let sstable_dir = base_dir.join("sstable");
        let level0_dir = sstable_dir.join("level-0");
        let manifest_path = base_dir.join("MANIFEST");

        tokio::fs::create_dir_all(&level0_dir).await?;

        let config = Arc::new(config);

        // --- Manifest ---
        let manifest = Arc::new(ManifestManager::load_or_create(manifest_path).await?);

        // --- Write component (no WAL needed; Raft log is the WAL) ---
        let wal_dir = base_dir.join("raft-wal");
        tokio::fs::create_dir_all(&wal_dir).await?;
        let dummy_wal = Arc::new(WALManager::new(wal_dir, config.wal_segment_size).await?);

        let write_component = WriteComponent::new(
            sstable_dir,
            dummy_wal,
            Arc::clone(&manifest),
            0,
            Arc::clone(&config),
        );
        let write_handle: Arc<RwLock<WriteComponent>> = Arc::new(RwLock::new(write_component));

        // --- Raft consensus layer (owns LogStore + KVStateMachine) ---
        let raft_layer = RaftConsensusLayer::start(raft_config, Arc::clone(&write_handle)).await?;

        // --- Read manager ---
        let active_memtable = write_handle.read().await.active_memtable_handle();
        let read_manager = ReadManager::new(active_memtable, Arc::clone(&manifest));

        // --- Compaction worker ---
        let (compaction_sender, compaction_receiver) = tokio::sync::mpsc::channel(100);
        let manifest_for_worker = Arc::clone(&manifest);
        let sstable_dir_for_compaction = base_dir.join("sstable");
        let config_for_compaction = Arc::clone(&config);
        let cancel = tokio_util::sync::CancellationToken::new();
        let worker_stop = tokio_util::sync::CancellationToken::new();
        let compaction_worker = Self::run_compaction_worker(
            compaction_receiver,
            manifest_for_worker,
            sstable_dir_for_compaction,
            config_for_compaction,
            cancel.clone(),
            worker_stop.clone(),
        );

        // --- Shared sequence number ---
        let seq_arc = Arc::new(AtomicU64::new(0));

        log::info!("RaftKV2 opened at {:?}", base_dir);

        Ok(Self {
            base_dir,
            config,
            _manifest: manifest,
            read_manager,
            compaction_sender,
            sequence_number: seq_arc,
            raft_layer,
            write_handle,
            compaction_cancel: cancel,
            worker_stop,
            compaction_worker: Some(compaction_worker),
        })
    }

    fn build_recovered_memtable(
        commands: Vec<LogCommand>,
    ) -> (
        crossbeam_skiplist::SkipMap<(Vec<u8>, u64), MemtableRecord>,
        u64,
        usize,
    ) {
        let memtable = crossbeam_skiplist::SkipMap::new();
        let mut max_lsn = 0u64;
        let mut total_size = 0usize;
        for cmd in commands {
            max_lsn = max_lsn.max(cmd.lsn);
            let value = if cmd.record_type == RecordType::Delete {
                Vec::new()
            } else {
                cmd.value
            };
            total_size += cmd.key.len() + value.len();
            memtable.insert(
                (cmd.key.clone(), cmd.lsn),
                MemtableRecord::new(value, cmd.record_type, cmd.lsn),
            );
        }
        (memtable, max_lsn, total_size)
    }

    fn run_compaction_worker(
        mut receiver: Receiver<CompactionTrigger>,
        manifest: Arc<ManifestManager>,
        sstable_dir: PathBuf,
        config: Arc<StorageConfig>,
        cancel: tokio_util::sync::CancellationToken,
        worker_stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(trigger) = receiver.recv() => {
                        if let CompactionTrigger::CompactLevel { level } = trigger {
                            log::info!("RaftKV2 compacting level {level}");
                            if let Err(e) = compaction(
                                Arc::clone(&manifest), level, Arc::clone(&config), cancel.child_token()
                            ).await {
                                log::error!("RaftKV2 compaction failed: {e}");
                            }
                        }
                    }
                    _ = worker_stop.cancelled() => {
                        receiver.close();
                        while let Some(CompactionTrigger::CompactLevel { level }) = receiver.recv().await {
                            if let Err(error) = compaction(
                                Arc::clone(&manifest),
                                level,
                                Arc::clone(&config),
                                cancel.child_token(),
                            )
                            .await
                            {
                                log::error!("RaftKV2 compaction failed: {error}");
                            }
                        }
                        break;
                    }
                    else => break,
                }
            }
        })
    }

    async fn maybe_flush(&mut self) -> Result<(), DBError> {
        {
            let wc = self.write_handle.read().await;
            if wc.memtable_size_bytes() < self.config.memtable_size_threshold as usize {
                return Ok(());
            }
        }
        let new_memtable = {
            let mut wc = self.write_handle.write().await;
            wc.flush_active_memtable().await?;
            wc.active_memtable_handle()
        };
        self.read_manager.set_memtable(new_memtable);

        let snapshot = self._manifest.snapshot().await;
        let l0 = snapshot.levels.get(&0).map_or(0, |f| f.len());
        if l0 >= self.config.max_level0_files {
            let _ = self
                .compaction_sender
                .send(CompactionTrigger::CompactLevel { level: 0 });
        }
        Ok(())
    }

    /// Return a reference to the Raft consensus layer.
    pub fn raft_layer(&self) -> &RaftConsensusLayer {
        &self.raft_layer
    }

    pub async fn shutdown(&mut self) -> Result<(), DBError> {
        self.worker_stop.cancel();
        if let Some(worker) = self.compaction_worker.take() {
            worker
                .await
                .map_err(|error| DBError::StorageError(error.to_string()))?;
        }
        self.raft_layer.shutdown().await;
        Ok(())
    }
}

impl Drop for RaftKV2 {
    fn drop(&mut self) {
        self.compaction_cancel.cancel();
        self.worker_stop.cancel();
    }
}

impl AsyncKVEngine for RaftKV2 {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        if !self.raft_layer.is_leader().await {
            return Err(DBError::StorageError(
                "not the leader; redirect reads to leader node".to_string(),
            ));
        }
        let seq = self.sequence_number.load(Ordering::Acquire);
        let record = self.read_manager.get(key.to_vec(), seq).await?;
        match record {
            Some(rec) if rec.record_type == RecordType::Delete => Ok(None),
            Some(rec) => Ok(Some(Cow::Owned(rec.value))),
            None => Ok(None),
        }
    }

    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        if !self.raft_layer.is_leader().await {
            let leader = self.raft_layer.current_leader().await;
            return Err(DBError::StorageError(format!(
                "not the leader; redirect to leader node {:?}",
                leader
            )));
        }
        let lsn = self.sequence_number.fetch_add(1, Ordering::SeqCst) + 1;
        let cmd = LogCommand::new(RecordType::Put, key, value, lsn);
        let data = cmd.serialize();
        self.raft_layer.propose(data).await?;
        self.write_handle
            .write()
            .await
            .apply_replicated_command(cmd)
            .await;
        self.maybe_flush().await?;
        Ok(())
    }

    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        if !self.raft_layer.is_leader().await {
            let leader = self.raft_layer.current_leader().await;
            return Err(DBError::StorageError(format!(
                "not the leader; redirect to leader node {:?}",
                leader
            )));
        }
        let lsn = self.sequence_number.fetch_add(1, Ordering::SeqCst) + 1;
        let cmd = LogCommand::new(RecordType::Delete, key, Vec::new(), lsn);
        let data = cmd.serialize();
        self.raft_layer.propose(data).await?;
        self.write_handle
            .write()
            .await
            .apply_replicated_command(cmd)
            .await;
        self.maybe_flush().await?;
        Ok(())
    }

    async fn write_batch(&mut self, _operations: Vec<BatchOperation>) -> Result<(), DBError> {
        Err(DBError::StorageError(
            "atomic batches are unsupported by RaftKV2 without multi-command proposals".into(),
        ))
    }

    async fn await_pending_writes(&self) -> Result<(), DBError> {
        Err(DBError::StorageError(
            "pending-write barriers are unsupported by RaftKV2".into(),
        ))
    }

    async fn scan(
        &self,
        range: impl RangeBounds<Vec<u8>> + Send,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        if !self.raft_layer.is_leader().await {
            return Err(DBError::StorageError(
                "not the leader; redirect reads to leader node".to_string(),
            ));
        }
        let seq = self.sequence_number.load(Ordering::Acquire);
        self.read_manager.scan_range(range, seq).await
    }
}

#[cfg(test)]
mod raft_tests {
    use super::*;
    use crate::storage::raft::RaftNodeConfig;

    fn test_config(node_id: u64, dir: &tempfile::TempDir) -> RaftNodeConfig {
        RaftNodeConfig {
            node_id,
            peers: vec![],
            http_bind: "127.0.0.1:0".parse().unwrap(),
            raft_dir: dir.path().join("raft"),
        }
    }

    #[tokio::test]
    async fn failed_flush_keeps_raft_writer_and_reader_on_the_same_table() {
        let directory = tempfile::tempdir().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 1,
            max_level0_files: usize::MAX,
            ..StorageConfig::default()
        };
        let mut kv = RaftKV2::open(directory.path(), config, test_config(1, &directory))
            .await
            .unwrap();
        let level0 = directory.path().join("sstable/level-0");
        tokio::fs::remove_dir(&level0).await.unwrap();
        tokio::fs::write(&level0, b"obstruction").await.unwrap();
        {
            let writer = kv.write_handle.write().await;
            writer
                .apply_replicated_command(LogCommand::new(
                    RecordType::Put,
                    b"old".to_vec(),
                    b"yes".to_vec(),
                    1,
                ))
                .await;
            writer
                .apply_replicated_command(LogCommand::new(
                    RecordType::Delete,
                    b"gone".to_vec(),
                    Vec::new(),
                    2,
                ))
                .await;
        }
        assert!(kv.maybe_flush().await.is_err());
        assert_eq!(
            kv.read_manager
                .get(b"old".to_vec(), 2)
                .await
                .unwrap()
                .unwrap()
                .value,
            b"yes"
        );
        tokio::fs::remove_file(&level0).await.unwrap();
        kv.write_handle
            .write()
            .await
            .apply_replicated_command(LogCommand::new(
                RecordType::Put,
                b"new".to_vec(),
                b"yes".to_vec(),
                3,
            ))
            .await;
        kv.maybe_flush().await.unwrap();
        for key in [b"old", b"new"] {
            assert_eq!(
                kv.read_manager
                    .get(key.to_vec(), 3)
                    .await
                    .unwrap()
                    .unwrap()
                    .value,
                b"yes"
            );
        }
        assert!(
            kv.read_manager
                .get(b"gone".to_vec(), 3)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(kv.write_handle.read().await.memtable_size_bytes(), 0);
        kv.raft_layer.shutdown().await;
    }

    #[tokio::test]
    async fn open_creates_directories() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(1, &dir);
        let kv = RaftKV2::open(dir.path(), StorageConfig::default(), cfg)
            .await
            .unwrap();
        assert!(dir.path().join("sstable/level-0").exists());
        assert!(dir.path().join("MANIFEST").exists());
        kv.raft_layer.shutdown().await;
    }

    #[tokio::test]
    async fn get_returns_none_for_missing_key() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(1, &dir);
        let mut kv = RaftKV2::open(dir.path(), StorageConfig::default(), cfg)
            .await
            .unwrap();
        kv.raft_layer.initialize(&[1]).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(kv.raft_layer.is_leader().await);
        let val = kv.get(b"nonexistent").await.unwrap();
        assert!(val.is_none());
        kv.raft_layer.shutdown().await;
    }

    #[tokio::test]
    async fn put_on_non_leader_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(1, &dir);
        let mut kv = RaftKV2::open(dir.path(), StorageConfig::default(), cfg)
            .await
            .unwrap();
        let result = kv.put(b"k".to_vec(), b"v".to_vec()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not the leader"));
        kv.raft_layer.shutdown().await;
    }

    #[tokio::test]
    async fn delete_on_non_leader_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(1, &dir);
        let mut kv = RaftKV2::open(dir.path(), StorageConfig::default(), cfg)
            .await
            .unwrap();
        let result = kv.delete(b"k".to_vec()).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not the leader"));
        kv.raft_layer.shutdown().await;
    }

    #[tokio::test]
    async fn scan_returns_empty_for_empty_store() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = test_config(1, &dir);
        let kv = RaftKV2::open(dir.path(), StorageConfig::default(), cfg)
            .await
            .unwrap();
        kv.raft_layer.initialize(&[1]).await.unwrap();
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
        assert!(kv.raft_layer.is_leader().await);
        let results = kv.scan(..).await.unwrap();
        assert!(results.is_empty());
        kv.raft_layer.shutdown().await;
    }
}
