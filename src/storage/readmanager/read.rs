use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

use crate::storage::{
    manifest_codec::{ManifestManager, ManifestSnapshot, SSTableMeta},
    record::{MemtableRecord, RecordType},
    sstable_codec::SSTableCodec,
};

pub struct ReadManager {
    memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
    manifest: ManifestManager,
}

impl ReadManager {
    pub fn new(
        memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>,
        manifest_manager: ManifestManager,
    ) -> Self {
        Self {
            memtable,
            manifest: manifest_manager,
        }
    }

    pub fn set_memtable(&mut self, memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>>) {
        self.memtable = memtable;
    }

    pub async fn get(
        &self,
        key: Vec<u8>,
        sequence_number: u64,
    ) -> Result<Option<MemtableRecord>, std::io::Error> {
        let in_mem = self
            .memtable
            .range((key.clone(), 0)..=(key.clone(), sequence_number))
            .next_back()
            .map(|entry| entry.value().clone());

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
    ) -> Result<Option<MemtableRecord>, std::io::Error> {
        let mut levels: Vec<(&u32, &Vec<SSTableMeta>)> = manifest_snapshot.levels.iter().collect();
        levels.sort_by_key(|(level, _)| **level);

        for (_, files) in levels.into_iter() {
            for sstable in files.iter().rev() {
                if let Some(record) = self
                    .get_from_single_sstable(sstable, key, sequence_number)
                    .await?
                {
                    return Ok(Some(record));
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
    ) -> Result<Option<MemtableRecord>, std::io::Error> {
        let file = tokio::fs::File::open(&sstable.path).await?;

        let mut reader = tokio::io::BufReader::new(file);

        let (footer, index, bloom) = SSTableCodec::deserialize_sections(&mut reader).await?;
        if !bloom.contains(key) {
            return Ok(None);
        }

        let mut best_visible: Option<MemtableRecord> = None;
        for index_entry in index.iter() {
            if key < index_entry.first_key.as_slice() || key > index_entry.last_key.as_slice() {
                continue;
            }

            let block =
                SSTableCodec::get_block(&mut reader, &footer, &index, &index_entry.first_key)
                    .await?;

            if let Some(records) = block.data {
                for record in records {
                    if record.key != key {
                        continue;
                    }

                    if record.lsn > sequence_number {
                        continue;
                    }

                    match &best_visible {
                        Some(existing) if existing.lsn >= record.lsn => {}
                        _ => best_visible = Some(record),
                    }
                }
            }
        }

        Ok(best_visible)
    }
}

fn scan_block_payload_for_key(payload: &[u8], target_key: &[u8]) -> Option<MemtableRecord> {
    let mut offset = 0usize;

    while offset < payload.len() {
        if payload.len().checked_sub(offset)? < 8 {
            break;
        }

        let key_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;

        if payload.len().checked_sub(offset)? < key_len + 4 {
            break;
        }

        let key = &payload[offset..offset + key_len];
        offset += key_len;

        let value_len = u32::from_le_bytes(payload[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;

        if payload.len().checked_sub(offset)? < value_len + 1 + 8 {
            break;
        }

        let value = payload[offset..offset + value_len].to_vec();
        offset += value_len;

        let record_type = match payload[offset] {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => return None,
        };
        offset += 1;

        let lsn = u64::from_le_bytes(payload[offset..offset + 8].try_into().ok()?);
        offset += 8;

        if key == target_key {
            return Some(MemtableRecord::new(value, record_type, lsn));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{recovermanager::wal::WALManager, writemanager::write::WriteComponent};
    use std::sync::Arc;

    #[tokio::test]
    async fn get_returns_latest_visible_version_for_snapshot_sequence_number() {
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

        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let manifest_manager = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();

        let manager = ReadManager::new(memtable, manifest_manager);

        let record = manager.get(key, 4).await.unwrap();

        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.value, b"value-v3".to_vec());
        assert_eq!(record.lsn, 3);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn get_reads_from_sstable_when_memtable_misses() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest_for_write = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();

        let mut write_component = WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            manifest_for_write,
            0,
        );

        let key = b"sstable-key".to_vec();
        let flush_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        flush_memtable.insert(
            (key.clone(), 7),
            MemtableRecord::new(b"sstable-value".to_vec(), RecordType::Put, 7),
        );
        write_component.flush(flush_memtable).await.unwrap();

        let manifest_for_read = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();
        let active_memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> =
            Arc::new(SkipMap::new());
        let manager = ReadManager::new(active_memtable, manifest_for_read);

        let record = manager.get(key, 7).await.unwrap();
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.value, b"sstable-value".to_vec());
        assert_eq!(record.record_type, RecordType::Put);
        assert_eq!(record.lsn, 7);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[tokio::test]
    async fn get_from_sstable_hides_newer_records_than_snapshot_sequence() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest_for_write = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();

        let mut write_component = WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            manifest_for_write,
            0,
        );

        let key = b"snapshot-key".to_vec();
        let flush_memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        flush_memtable.insert(
            (key.clone(), 11),
            MemtableRecord::new(b"value-lsn-11".to_vec(), RecordType::Put, 11),
        );
        write_component.flush(flush_memtable).await.unwrap();

        let manifest_for_read = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();
        let active_memtable: Arc<SkipMap<(Vec<u8>, u64), MemtableRecord>> =
            Arc::new(SkipMap::new());
        let manager = ReadManager::new(active_memtable, manifest_for_read);

        let record = manager.get(key, 10).await.unwrap();
        assert!(record.is_none());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
