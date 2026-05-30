use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

use crate::error::DBError;
use crate::storage::{
    manifest_codec::{ManifestManager, ManifestSnapshot, SSTableMeta},
    record::{MemtableRecord, RecordType},
    sstable_codec::SSTableCodec,
};
use std::collections::HashMap;
use std::ops::RangeBounds;

pub struct ReadManager {
    memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
    manifest: Arc<ManifestManager>,
}

impl ReadManager {
    pub fn new(
        memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
        manifest_manager: Arc<ManifestManager>,
    ) -> Self {
        Self {
            memtable,
            manifest: manifest_manager,
        }
    }

    pub fn set_memtable(&mut self, memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>) {
        self.memtable = memtable;
    }

    /// Scan all keys in the given range, merging memtable + SSTable sources.
    ///
    /// Returns sorted unique key-value pairs, newest version per key wins,
    /// tombstones are filtered out.
    pub async fn scan_range(
        &self,
        range: impl RangeBounds<Vec<u8>>,
        sequence_number: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        // Collect entries from memtable
        let mut merged: HashMap<Vec<u8>, (Vec<u8>, u64)> = HashMap::new();

        // 1. Scan memtable
        // SkipMap::range() returns entries in key order. We iterate the range
        // and keep the highest LSN per key.
        for entry in self.memtable.range(collect_range_bounds(&range)) {
            let (key, lsn) = entry.key();
            let record = entry.value();
            if *lsn > sequence_number {
                continue;
            }
            // Only track Put (non-tombstone) entries
            if record.record_type != RecordType::Delete {
                let is_newer = merged
                    .get(key)
                    .map(|(_, existing_lsn)| *lsn > *existing_lsn)
                    .unwrap_or(true);
                if is_newer {
                    merged.insert(key.clone(), (record.value.clone(), *lsn));
                }
            } else {
                // Tombstone: remove any existing entry
                merged.remove(key);
            }
        }

        // 2. Scan SSTable levels
        let manifest_snapshot = self.manifest.snapshot().await;
        self.scan_sstables(&manifest_snapshot, &range, sequence_number, &mut merged)
            .await?;

        // 3. Produce sorted output
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .map(|(key, (value, _))| (key, value))
            .collect();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
    }

    async fn scan_sstables(
        &self,
        manifest_snapshot: &ManifestSnapshot,
        range: &impl RangeBounds<Vec<u8>>,
        sequence_number: u64,
        merged: &mut HashMap<Vec<u8>, (Vec<u8>, u64)>,
    ) -> Result<(), DBError> {
        let mut levels: Vec<(&u32, &Vec<SSTableMeta>)> =
            manifest_snapshot.levels.iter().collect();
        levels.sort_by_key(|(level, _)| **level);

        for (_level, files) in levels.into_iter() {
            for sstable in files.iter() {
                self.scan_single_sstable(sstable, range, sequence_number, merged)
                    .await?;
            }
        }
        Ok(())
    }

    async fn scan_single_sstable(
        &self,
        sstable: &SSTableMeta,
        range: &impl RangeBounds<Vec<u8>>,
        sequence_number: u64,
        merged: &mut HashMap<Vec<u8>, (Vec<u8>, u64)>,
    ) -> Result<(), DBError> {
        // Quick range check: skip SSTable if its key range doesn't overlap scan range
        if !ranges_overlap(range, &sstable.smallest_key, &sstable.largest_key) {
            return Ok(());
        }

        let template = self.manifest.get_or_open_file(&sstable.path).await?;
        let file = template.try_clone().await?;
        let mut reader = tokio::io::BufReader::new(file);

        let (footer, index, _bloom) =
            SSTableCodec::deserialize_sections(&mut reader).await?;

        // Iterate through blocks that overlap the scan range
        for index_entry in index.iter() {
            // Skip blocks whose entire key range is outside our scan range
            if !ranges_overlap(range, &index_entry.first_key, &index_entry.last_key) {
                continue;
            }

            let block =
                SSTableCodec::get_block(&mut reader, &footer, &index, &index_entry.first_key)
                    .await?;

            if let Some(records) = block.data {
                for record in records {
                    if record.lsn > sequence_number {
                        continue;
                    }
                    // Check if key is within range
                    if !key_in_range(&record.key, range) {
                        continue;
                    }
                    if record.record_type != RecordType::Delete {
                        let key = record.key.clone();
                        let is_newer = merged
                            .get(&key)
                            .map(|(_, existing_lsn)| record.lsn > *existing_lsn)
                            .unwrap_or(true);
                        if is_newer {
                            merged.insert(key, (record.value, record.lsn));
                        }
                    } else {
                        merged.remove(&record.key);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn get(
        &self,
        key: Vec<u8>,
        sequence_number: u64,
    ) -> Result<Option<MemtableRecord>, DBError> {
        let in_mem = self
            .memtable
            .range((key.clone(), 0)..=(key.clone(), sequence_number))
            .next_back()
            .map(|entry| entry.value().clone());

        log::info!(
            "Searching for key {:?} in memtable with sequence number <= {}: Found: {}",
            String::from_utf8_lossy(&key),
            sequence_number,
            in_mem.is_some()
        );
        if in_mem.is_some() {
            return Ok(in_mem);
        }

        let manifest_snapshot = self.manifest.snapshot().await;
        let res = self
            .get_from_sstable(&manifest_snapshot, &key, sequence_number)
            .await?;

        Ok(res)
    }

    async fn get_from_sstable(
        &self,
        manifest_snapshot: &ManifestSnapshot,
        key: &[u8],
        sequence_number: u64,
    ) -> Result<Option<MemtableRecord>, DBError> {
        let mut levels: Vec<(&u32, &Vec<SSTableMeta>)> = manifest_snapshot.levels.iter().collect();
        levels.sort_by_key(|(level, _)| **level);

        log::info!(
            "Searching for key {:?} in SSTables across levels: {:?}",
            String::from_utf8_lossy(key),
            levels
                .iter()
                .map(|(level, files)| {
                    let file_names: Vec<String> =
                        files.iter().map(|f| f.path.to_string()).collect();
                    format!("Level {}: [{}]", level, file_names.join(", "))
                })
                .collect::<Vec<String>>()
        );

        for (level, files) in levels.into_iter() {
            if *level == 0 {
                // Level 0 files can overlap in key range. Must check ALL files and
                // return the version with the highest LSN to avoid stale reads.
                let mut best: Option<MemtableRecord> = None;
                for sstable in files.iter() {
                    if let Some(record) = self
                        .get_from_single_sstable(sstable, key, sequence_number)
                        .await?
                    {
                        let is_newer = best
                            .as_ref()
                            .map(|current| record.lsn > current.lsn)
                            .unwrap_or(true);
                        if is_newer {
                            best = Some(record);
                        }
                    }
                }
                if let Some(record) = best {
                    match record.record_type {
                        RecordType::Delete => return Ok(None),
                        _ => return Ok(Some(record)),
                    }
                }
            } else {
                // Deeper levels are non-overlapping — first match is correct.
                for sstable in files.iter().rev() {
                    if let Some(record) = self
                        .get_from_single_sstable(sstable, key, sequence_number)
                        .await?
                    {
                        match record.record_type {
                            RecordType::Delete => return Ok(None),
                            _ => return Ok(Some(record)),
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    async fn get_from_single_sstable(
        &self,
        sstable: &SSTableMeta,
        key: &[u8],
        sequence_number: u64,
    ) -> Result<Option<MemtableRecord>, DBError> {
        // Use cached file handle for OS page cache reuse.
        // try_clone() gives an independent handle with its own file position.
        let template = self.manifest.get_or_open_file(&sstable.path).await?;
        let file = template.try_clone().await?;

        let mut reader = tokio::io::BufReader::new(file);

        let (footer, index, bloom) = SSTableCodec::deserialize_sections(&mut reader).await?;
        if !bloom.contains(key) {
            return Ok(None);
        }

        log::debug!(
            "Key {:?} passed Bloom filter for SSTable {:?}",
            String::from_utf8_lossy(key),
            sstable.path
        );

        for index_entry in index.iter() {
            if key < index_entry.first_key.as_slice() || key > index_entry.last_key.as_slice() {
                continue;
            }

            let block =
                SSTableCodec::get_block(&mut reader, &footer, &index, &index_entry.first_key)
                    .await?;

            if let Some(records) = block.data {
                for record in records {
                    if record.key == key {
                        return Ok(Some(record));
                    }
                }
            }
        }

        Ok(None)
    }
}

// ── Helper functions for range scan ──────────────────────────────

/// Convert a `RangeBounds<Vec<u8>>` to `(Bound<(Vec<u8>, u64)>, Bound<(Vec<u8>, u64)>)` for SkipMap.
/// Uses (key, 0) as the lower bound and (key, u64::MAX) as the upper bound to
/// capture all LSN versions of each key in the range.
fn collect_range_bounds(
    range: &impl RangeBounds<Vec<u8>>,
) -> (std::ops::Bound<(Vec<u8>, u64)>, std::ops::Bound<(Vec<u8>, u64)>) {
    use std::ops::Bound;

    let start: Bound<(Vec<u8>, u64)> = match range.start_bound() {
        Bound::Included(key) => Bound::Included((key.clone(), 0)),
        Bound::Excluded(key) => Bound::Excluded((key.clone(), u64::MAX)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end: Bound<(Vec<u8>, u64)> = match range.end_bound() {
        Bound::Included(key) => Bound::Included((key.clone(), u64::MAX)),
        Bound::Excluded(key) => Bound::Excluded((key.clone(), 0)),
        Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
}

/// Check if an SSTable's key range overlaps with the scan range.
fn ranges_overlap(range: &impl RangeBounds<Vec<u8>>, sst_start: &[u8], sst_end: &[u8]) -> bool {
    use std::ops::Bound;

    let range_start: Option<&[u8]> = match range.start_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.as_slice()),
        Bound::Unbounded => None,
    };
    let range_end: Option<&[u8]> = match range.end_bound() {
        Bound::Included(key) | Bound::Excluded(key) => Some(key.as_slice()),
        Bound::Unbounded => None,
    };

    // If range has an end and SSTable starts after it → no overlap
    if let Some(end) = range_end {
        if sst_start > end {
            return false;
        }
    }
    // If range has a start and SSTable ends before it → no overlap
    if let Some(start) = range_start {
        if sst_end < start {
            return false;
        }
    }
    true
}

/// Check if a key falls within the given range.
fn key_in_range(key: &[u8], range: &impl RangeBounds<Vec<u8>>) -> bool {
    use std::ops::Bound;

    match range.start_bound() {
        Bound::Included(start) => {
            if key < start.as_slice() {
                return false;
            }
        }
        Bound::Excluded(start) => {
            if key <= start.as_slice() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    match range.end_bound() {
        Bound::Included(end) => {
            if key > end.as_slice() {
                return false;
            }
        }
        Bound::Excluded(end) => {
            if key >= end.as_slice() {
                return false;
            }
        }
        Bound::Unbounded => {}
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{recovermanager::wal::WALManager, writemanager::write::WriteComponent};
    use crate::testing::with_temp_dir;

    #[tokio::test]
    async fn get_returns_latest_visible_version_for_snapshot_sequence_number() {
        let temp_dir = with_temp_dir("readmanager-mvcc", |p| p);

        let memtable = Arc::new(SkipMap::new());
        let key = b"mvcc-key".to_vec();

        memtable.insert(
            (key.clone(), 1),
            MemtableRecord::new(b"value-v1".to_vec(), RecordType::Put, 1),
        );
        memtable.insert(
            (key.clone(), 3),
            MemtableRecord::new(b"value-v3".to_vec(), RecordType::Put, 3),
        );
        memtable.insert(
            (key.clone(), 5),
            MemtableRecord::new(b"value-v5".to_vec(), RecordType::Put, 5),
        );

        let manifest_manager = Arc::new(
            ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
                .await
                .unwrap(),
        );

        let manager = ReadManager::new(memtable, manifest_manager);

        let record = manager.get(key, 4).await.unwrap();

        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.value, b"value-v3".to_vec());
        assert_eq!(record.lsn, 3);
    }

    #[tokio::test]
    async fn get_reads_from_sstable_when_memtable_misses() {
        let temp_dir = with_temp_dir("readmanager-sstable", |p| p);

        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest = Arc::new(
            ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
                .await
                .unwrap(),
        );

        let mut write_component = WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            Arc::clone(&manifest),
            0,
            Arc::new(crate::storage::config::StorageConfig::default()),
        );

        let key = b"sstable-key".to_vec();
        let flush_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        flush_memtable.insert(
            (key.clone(), 7),
            MemtableRecord::new(b"sstable-value".to_vec(), RecordType::Put, 7),
        );
        let flush_result = write_component
            .flush(Arc::new(flush_memtable))
            .await
            .unwrap();

        // Write SSTable to disk (caller owns file I/O, not WriteComponent)
        let mut file = tokio::fs::File::create(&flush_result.sstable_path).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut file, &flush_result.data)
            .await
            .unwrap();
        file.sync_all().await.unwrap();

        let active_memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> =
            Arc::new(SkipMap::new());
        let manager = ReadManager::new(active_memtable, manifest);

        let record = manager.get(key, 7).await.unwrap();
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.value, b"sstable-value".to_vec());
        assert_eq!(record.record_type, RecordType::Put);
        assert_eq!(record.lsn, 7);
    }

    #[tokio::test]
    async fn l0_overlap_returns_highest_lsn() {
        // Regression test for Issue #11: L0 overlapping SSTables should
        // return the Put with the highest LSN, not just the first match.
        let temp_dir = with_temp_dir("readmanager-l0-overlap", |p| p);

        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest = Arc::new(
            ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
                .await
                .unwrap(),
        );

        let mut write_component = WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            Arc::clone(&manifest),
            0,
            Arc::new(crate::storage::config::StorageConfig::default()),
        );

        let key = b"overlap-key".to_vec();

        // Flush 1: older version (LSN=5)
        let mt1: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        mt1.insert(
            (key.clone(), 5),
            MemtableRecord::new(b"old-value".to_vec(), RecordType::Put, 5),
        );
        let r1 = write_component.flush(Arc::new(mt1)).await.unwrap();
        let mut file = tokio::fs::File::create(&r1.sstable_path).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut file, &r1.data)
            .await
            .unwrap();
        file.sync_all().await.unwrap();

        // Flush 2: newer version (LSN=10) — overlapping with flush 1 in L0
        let mt2: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        mt2.insert(
            (key.clone(), 10),
            MemtableRecord::new(b"new-value".to_vec(), RecordType::Put, 10),
        );
        let r2 = write_component.flush(Arc::new(mt2)).await.unwrap();
        let mut file = tokio::fs::File::create(&r2.sstable_path).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut file, &r2.data)
            .await
            .unwrap();
        file.sync_all().await.unwrap();

        // Read via new ReadManager (empty memtable)
        let active_memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> =
            Arc::new(SkipMap::new());
        let manager = ReadManager::new(active_memtable, manifest);

        let record = manager.get(key.clone(), 10).await.unwrap();
        assert!(record.is_some(), "Should find the key in overlapping L0 files");
        let record = record.unwrap();
        assert_eq!(
            record.value, b"new-value",
            "Should return the value with the highest LSN"
        );
        assert_eq!(record.lsn, 10, "Should return LSN=10 (newest) not LSN=5");
    }
}
