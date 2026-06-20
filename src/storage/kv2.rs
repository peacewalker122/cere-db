use std::{
    borrow::Cow,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Receiver, Sender},
};

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use crate::{
    api::api::AsyncKVEngine,
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
}

impl KV2 {
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

        tokio::fs::create_dir_all(&wal_dir).await?;
        tokio::fs::create_dir_all(&level0_dir).await?;

        let cancel = tokio_util::sync::CancellationToken::new();

        let config = Arc::new(config);

        let wal_manager =
            Arc::new(WALManager::new(wal_dir.clone(), config.wal_segment_size).await?);
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
        Self::run_compaction_worker(
            compaction_receiver,
            manifest_for_worker,
            sstable_dir_for_compaction,
            config_for_compaction,
            cancel.child_token(),
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
        })
    }

    /// Open KV2 with default configuration.
    ///
    /// Convenience wrapper for backward compatibility with existing code.
    pub async fn open_with_defaults(base_dir: impl AsRef<Path>) -> Result<Self, DBError> {
        Self::open(base_dir, StorageConfig::default()).await
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
        if self.write_component.memtable_size_bytes() < self.config.memtable_size_threshold as usize {
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

        // Write SSTable to disk (file ownership lives in KV2, not WriteComponent)
        let mut file = tokio::fs::File::create(&flush_result.sstable_path).await?;
        file.write_all(&flush_result.data).await?;
        file.sync_all().await?;

        // Use shared manifest for snapshot check
        let snapshot = self.manifest.snapshot().await;
        let level0_count = snapshot.levels.get(&0).map_or(0, |files| files.len());

        if level0_count >= self.config.max_level0_files {
            // Send trigger to rayon worker (non-blocking)
            let _ = self
                .compaction_sender
                .send(CompactionTrigger::CompactLevel { level: 0 });
        }

        Ok(())
    }

    /// Rayon worker that processes compaction triggers received via mpsc channel.
    /// Runs in a dedicated thread, using rayon for parallel compaction work.
    async fn run_compaction_worker(
        mut receiver: Receiver<CompactionTrigger>,
        manifest: Arc<ManifestManager>,
        sstable_dir: PathBuf,
        config: Arc<StorageConfig>,
        cancel: tokio_util::sync::CancellationToken,
    ) {
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
                    _ = cancel.cancelled() => {
                        log::info!("Compaction worker received shutdown signal");
                        break;
                    }
                }
            }
        });
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
        }

        let reopened = KV2::open_with_defaults(&temp_dir).await.unwrap();
        let value = reopened.get(b"recovery-key").await.unwrap();

        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref(), b"recovery-value");

        std::fs::remove_dir_all(temp_dir).unwrap();
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
        let results = kv2.scan(b"beta".to_vec()..=b"gamma".to_vec()).await.unwrap();
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

        kv2.put(b"keep".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        kv2.put(b"remove".to_vec(), b"temp".to_vec())
            .await
            .unwrap();
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
        let dummy_wal = Arc::new(
            WALManager::new(wal_dir, config.wal_segment_size).await?,
        );

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
        Self::run_compaction_worker(
            compaction_receiver,
            manifest_for_worker,
            sstable_dir_for_compaction,
            config_for_compaction,
            cancel,
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
        })
    }

    fn build_recovered_memtable(
        commands: Vec<LogCommand>,
    ) -> (crossbeam_skiplist::SkipMap<(Vec<u8>, u64), MemtableRecord>, u64, usize) {
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
    ) {
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
                    _ = cancel.cancelled() => break,
                }
            }
        });
    }

    async fn maybe_flush(&mut self) -> Result<(), DBError> {
        {
            let wc = self.write_handle.read().await;
            if wc.memtable_size_bytes() < self.config.memtable_size_threshold as usize {
                return Ok(());
            }
        }
        let (new_memtable, result) = {
            let mut wc = self.write_handle.write().await;
            let locked = wc.lock_memtable().await;
            let new_memtable = wc.active_memtable_handle();
            let result = wc.flush(locked).await?;
            (new_memtable, result)
        };
        let mut file = tokio::fs::File::create(&result.sstable_path).await?;
        file.write_all(&result.data).await?;
        file.sync_all().await?;
        self.read_manager.set_memtable(new_memtable);

        let snapshot = self._manifest.snapshot().await;
        let l0 = snapshot.levels.get(&0).map_or(0, |f| f.len());
        if l0 >= self.config.max_level0_files {
            let _ = self.compaction_sender.send(CompactionTrigger::CompactLevel { level: 0 });
        }
        Ok(())
    }

    /// Return a reference to the Raft consensus layer.
    pub fn raft_layer(&self) -> &RaftConsensusLayer {
        &self.raft_layer
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
        self.write_handle.write().await.apply_replicated_command(cmd);
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
        self.write_handle.write().await.apply_replicated_command(cmd);
        self.maybe_flush().await?;
        Ok(())
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
