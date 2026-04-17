use std::{
    collections::{BTreeMap, HashMap},
    io::{Cursor, Read},
    path::PathBuf,
};

use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::Mutex,
};

use crate::storage::bloom::BloomFilterWrapper;

const ENTRY_KIND_ADD: u8 = 1;
const ENTRY_KIND_REMOVE: u8 = 2;
const ENTRY_KIND_CHECKPOINT: u8 = 3;

const MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableMeta {
    pub file_id: u64,
    pub level: u32,
    pub path: String,
    pub record_count: usize,
    pub bloom_bitmap: BloomFilterWrapper,
    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSnapshot {
    pub version: u32,
    pub next_file_id: u64,
    pub active_wal_segment: u64,
    pub levels: HashMap<u32, Vec<SSTableMeta>>,
}

#[derive(Debug)]
struct ManifestState {
    version: u32,
    next_file_id: u64,
    checkpoint_lsn: u64,
    active_wal_segment: u64,
    levels: HashMap<u32, BTreeMap<u64, SSTableMeta>>,
}

impl Default for ManifestState {
    fn default() -> Self {
        Self {
            version: MANIFEST_VERSION,
            next_file_id: 1,
            checkpoint_lsn: 0,
            active_wal_segment: 0,
            levels: HashMap::new(),
        }
    }
}

/// Append-only SSTable manifest manager.
///
/// Owns internal synchronization to keep metadata updates atomic at component level.
pub struct ManifestManager {
    manifest_path: PathBuf,
    state: Mutex<ManifestState>,
}

impl ManifestManager {
    pub async fn load_or_create(manifest_path: PathBuf) -> Result<Self, std::io::Error> {
        if let Some(parent) = manifest_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let bytes = match tokio::fs::read(&manifest_path).await {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::File::create(&manifest_path).await?;
                Vec::new()
            }
            Err(err) => return Err(err),
        };

        let state = replay_manifest(&bytes)?;

        Ok(Self {
            manifest_path,
            state: Mutex::new(state),
        })
    }

    pub async fn allocate_file_id(&self) -> Result<u64, std::io::Error> {
        let mut state = self.state.lock().await;
        let file_id = state.next_file_id;
        state.next_file_id = state
            .next_file_id
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("manifest file id overflow"))?;
        Ok(file_id)
    }

    pub async fn register_sstable(&self, meta: SSTableMeta) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().await;

        let payload = encode_add_payload(&meta)?;
        let entry = encode_entry(ENTRY_KIND_ADD, &payload)?;
        append_manifest_entry(&self.manifest_path, &entry).await?;

        state
            .levels
            .entry(meta.level)
            .or_default()
            .insert(meta.file_id, meta.clone());

        if meta.file_id >= state.next_file_id {
            state.next_file_id = meta
                .file_id
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("manifest file id overflow"))?;
        }

        Ok(())
    }

    pub async fn remove_sstable(&self, level: u32, file_id: u64) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().await;

        let payload = encode_remove_payload(level, file_id);
        let entry = encode_entry(ENTRY_KIND_REMOVE, &payload)?;
        append_manifest_entry(&self.manifest_path, &entry).await?;

        if let Some(files) = state.levels.get_mut(&level) {
            files.remove(&file_id);
            if files.is_empty() {
                state.levels.remove(&level);
            }
        }

        Ok(())
    }

    pub async fn mark_checkpoint(
        &self,
        checkpoint_lsn: u64,
        active_wal_segment: u64,
    ) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().await;

        let payload = encode_checkpoint_payload(checkpoint_lsn, active_wal_segment);
        let entry = encode_entry(ENTRY_KIND_CHECKPOINT, &payload)?;
        append_manifest_entry(&self.manifest_path, &entry).await?;

        state.checkpoint_lsn = checkpoint_lsn;
        state.active_wal_segment = active_wal_segment;

        Ok(())
    }

    // Get a consistent snapshot of the manifest state for external use (e.g. compaction).
    pub async fn snapshot(&self) -> ManifestSnapshot {
        let state = self.state.lock().await;
        ManifestSnapshot {
            version: state.version,
            next_file_id: state.next_file_id,
            active_wal_segment: state.active_wal_segment,
            levels: state
                .levels
                .iter()
                .map(|(level, files)| (*level, files.values().cloned().collect()))
                .collect(),
        }
    }
}

async fn append_manifest_entry(path: &PathBuf, entry: &[u8]) -> Result<(), std::io::Error> {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let mut writer = BufWriter::new(file);
    writer.write_all(entry).await?;
    writer.flush().await?;
    writer.into_inner().sync_data().await?;
    Ok(())
}

fn replay_manifest(data: &[u8]) -> Result<ManifestState, std::io::Error> {
    let mut state = ManifestState::default();
    let mut offset = 0usize;

    while offset < data.len() {
        let (kind, payload, next_offset) = decode_entry(data, offset)?;
        match kind {
            ENTRY_KIND_ADD => {
                let meta = decode_add_payload(payload)?;
                state
                    .levels
                    .entry(meta.level)
                    .or_default()
                    .insert(meta.file_id, meta.clone());

                if meta.file_id >= state.next_file_id {
                    state.next_file_id = meta.file_id + 1;
                }
            }
            ENTRY_KIND_REMOVE => {
                let (level, file_id) = decode_remove_payload(payload)?;
                if let Some(files) = state.levels.get_mut(&level) {
                    files.remove(&file_id);
                    if files.is_empty() {
                        state.levels.remove(&level);
                    }
                }
            }
            ENTRY_KIND_CHECKPOINT => {
                let (checkpoint_lsn, active_wal_segment) = decode_checkpoint_payload(payload)?;
                state.active_wal_segment = active_wal_segment;
            }
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unknown manifest entry kind: {kind}"),
                ));
            }
        }

        offset = next_offset;
    }

    Ok(state)
}

fn encode_entry(kind: u8, payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "manifest entry payload too large",
        )
    })?;

    let mut bytes = Vec::with_capacity(1 + 4 + payload.len() + 4);
    bytes.push(kind);
    bytes.extend_from_slice(&payload_len.to_be_bytes());
    bytes.extend_from_slice(payload);

    let checksum = crc32fast::hash(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());

    Ok(bytes)
}

fn decode_entry(data: &[u8], offset: usize) -> Result<(u8, &[u8], usize), std::io::Error> {
    let header_end = offset.checked_add(5).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest offset overflow")
    })?;
    if header_end > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated manifest entry header",
        ));
    }

    let kind = data[offset];
    let len = u32::from_be_bytes(data[offset + 1..offset + 5].try_into().unwrap()) as usize;
    let payload_end = header_end.checked_add(len).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "manifest payload overflow")
    })?;
    let checksum_end = payload_end.checked_add(4).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "manifest checksum overflow",
        )
    })?;

    if checksum_end > data.len() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "truncated manifest entry payload",
        ));
    }

    let stored_checksum = u32::from_be_bytes(data[payload_end..checksum_end].try_into().unwrap());
    let calculated_checksum = crc32fast::hash(&data[offset..payload_end]);

    if stored_checksum != calculated_checksum {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "manifest entry checksum mismatch: expected 0x{calculated_checksum:08X}, got 0x{stored_checksum:08X}"
            ),
        ));
    }

    Ok((kind, &data[header_end..payload_end], checksum_end))
}

fn encode_add_payload(meta: &SSTableMeta) -> Result<Vec<u8>, std::io::Error> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&meta.level.to_be_bytes());
    payload.extend_from_slice(&meta.file_id.to_be_bytes());
    payload.extend_from_slice(&(meta.record_count as u64).to_be_bytes());

    push_bytes(&mut payload, meta.path.as_bytes())?;
    push_bytes(&mut payload, meta.bloom_bitmap.encode().as_slice())?;
    push_bytes(&mut payload, &meta.smallest_key)?;
    push_bytes(&mut payload, &meta.largest_key)?;

    Ok(payload)
}

fn decode_add_payload(payload: &[u8]) -> Result<SSTableMeta, std::io::Error> {
    let mut cursor = Cursor::new(payload);

    let level = read_u32(&mut cursor)?;
    let file_id = read_u64(&mut cursor)?;
    let record_count = read_u64(&mut cursor)?;

    let path = read_string(&mut cursor)?;
    let bloom_bytes = read_vec(&mut cursor)?;
    let bitmap = BloomFilterWrapper::decode(Cursor::new(&bloom_bytes))?;

    // Backward compatibility: older payloads have no min/max key range.
    let (smallest_key, largest_key) = if (cursor.position() as usize) < payload.len() {
        (read_vec(&mut cursor)?, read_vec(&mut cursor)?)
    } else {
        (Vec::new(), Vec::new())
    };

    Ok(SSTableMeta {
        file_id,
        level,
        path,
        bloom_bitmap: bitmap,
        smallest_key,
        largest_key,
        record_count: usize::try_from(record_count).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "record count does not fit usize",
            )
        })?,
    })
}

fn encode_remove_payload(level: u32, file_id: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(4 + 8);
    payload.extend_from_slice(&level.to_be_bytes());
    payload.extend_from_slice(&file_id.to_be_bytes());
    payload
}

fn decode_remove_payload(payload: &[u8]) -> Result<(u32, u64), std::io::Error> {
    if payload.len() != 12 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "remove payload length must be 12 bytes",
        ));
    }

    let level = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let file_id = u64::from_be_bytes(payload[4..12].try_into().unwrap());
    Ok((level, file_id))
}

fn encode_checkpoint_payload(checkpoint_lsn: u64, active_wal_segment: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(16);
    payload.extend_from_slice(&checkpoint_lsn.to_be_bytes());
    payload.extend_from_slice(&active_wal_segment.to_be_bytes());
    payload
}

fn decode_checkpoint_payload(payload: &[u8]) -> Result<(u64, u64), std::io::Error> {
    if payload.len() != 16 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "checkpoint payload length must be 16 bytes",
        ));
    }

    let checkpoint_lsn = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let active_wal_segment = u64::from_be_bytes(payload[8..16].try_into().unwrap());
    Ok((checkpoint_lsn, active_wal_segment))
}

fn push_bytes(buf: &mut Vec<u8>, value: &[u8]) -> Result<(), std::io::Error> {
    let len = u32::try_from(value.len()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "field length exceeds u32")
    })?;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(value);
    Ok(())
}

fn read_vec(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, std::io::Error> {
    let len = read_u32(cursor)? as usize;
    let mut bytes = vec![0u8; len];
    cursor.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_string(cursor: &mut Cursor<&[u8]>) -> Result<String, std::io::Error> {
    let bytes = read_vec(cursor)?;
    String::from_utf8(bytes).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid UTF-8 in manifest path: {err}"),
        )
    })
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, std::io::Error> {
    let mut b = [0u8; 4];
    cursor.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, std::io::Error> {
    let mut b = [0u8; 8];
    cursor.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_and_decode_entry_roundtrip() {
        let mut bloom = BloomFilterWrapper::with_rate(16, 0.01);
        bloom.insert(b"a");
        bloom.insert(b"z");

        let meta = SSTableMeta {
            file_id: 42,
            level: 0,
            path: "data/level-0/42.sst".to_string(),
            record_count: 101,
            bloom_bitmap: bloom,
            smallest_key: b"a".to_vec(),
            largest_key: b"z".to_vec(),
        };

        let payload = encode_add_payload(&meta).unwrap();
        let entry = encode_entry(ENTRY_KIND_ADD, &payload).unwrap();

        let (kind, decoded_payload, next_offset) = decode_entry(&entry, 0).unwrap();
        assert_eq!(kind, ENTRY_KIND_ADD);
        assert_eq!(next_offset, entry.len());

        let decoded = decode_add_payload(decoded_payload).unwrap();
        assert_eq!(decoded.file_id, meta.file_id);
        assert_eq!(decoded.level, meta.level);
        assert_eq!(decoded.path, meta.path);
        assert_eq!(decoded.record_count, meta.record_count);
        assert_eq!(decoded.bloom_bitmap.encode(), meta.bloom_bitmap.encode());
        assert_eq!(decoded.smallest_key, meta.smallest_key);
        assert_eq!(decoded.largest_key, meta.largest_key);
        assert!(decoded.bloom_bitmap.contains(b"a"));
        assert!(decoded.bloom_bitmap.contains(b"z"));
    }

    #[test]
    fn decode_add_payload_supports_legacy_without_range_fields() {
        let mut bloom = BloomFilterWrapper::with_rate(16, 0.01);
        bloom.insert(b"legacy");

        let mut legacy_payload = Vec::new();
        legacy_payload.extend_from_slice(&0u32.to_be_bytes());
        legacy_payload.extend_from_slice(&7u64.to_be_bytes());
        legacy_payload.extend_from_slice(&1u64.to_be_bytes());
        push_bytes(&mut legacy_payload, b"data/level-0/7.sst").unwrap();
        push_bytes(&mut legacy_payload, bloom.encode().as_slice()).unwrap();

        let decoded = decode_add_payload(&legacy_payload).unwrap();
        assert_eq!(decoded.file_id, 7);
        assert_eq!(decoded.level, 0);
        assert_eq!(decoded.path, "data/level-0/7.sst");
        assert_eq!(decoded.record_count, 1);
        assert!(decoded.bloom_bitmap.contains(b"legacy"));
        assert!(decoded.smallest_key.is_empty());
        assert!(decoded.largest_key.is_empty());
    }

    #[tokio::test]
    async fn manager_replays_entries() {
        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(&temp_dir).unwrap();
        let manifest_path = temp_dir.join("MANIFEST");

        let manager = ManifestManager::load_or_create(manifest_path.clone())
            .await
            .unwrap();

        let file_id_1 = manager.allocate_file_id().await.unwrap();
        manager
            .register_sstable(SSTableMeta {
                file_id: file_id_1,
                level: 0,
                path: format!("data/level-0/{file_id_1}.sst"),
                record_count: 9,
                bloom_bitmap: BloomFilterWrapper::with_rate(16, 0.01),
                smallest_key: b"k1".to_vec(),
                largest_key: b"k9".to_vec(),
            })
            .await
            .unwrap();

        let file_id_2 = manager.allocate_file_id().await.unwrap();
        manager
            .register_sstable(SSTableMeta {
                file_id: file_id_2,
                level: 1,
                path: format!("data/level-1/{file_id_2}.sst"),
                record_count: 2,
                bloom_bitmap: BloomFilterWrapper::with_rate(16, 0.01),
                smallest_key: b"aa".to_vec(),
                largest_key: b"az".to_vec(),
            })
            .await
            .unwrap();

        manager.remove_sstable(0, file_id_1).await.unwrap();
        manager.mark_checkpoint(77, 3).await.unwrap();

        let reopened = ManifestManager::load_or_create(manifest_path)
            .await
            .unwrap();
        let snapshot = reopened.snapshot().await;

        assert_eq!(snapshot.active_wal_segment, 3);
        assert!(snapshot.levels.get(&0).is_none());
        assert_eq!(snapshot.levels.get(&1).unwrap().len(), 1);
        assert_eq!(snapshot.next_file_id, file_id_2 + 1);

        std::fs::remove_dir_all(temp_dir).unwrap();
    }

    #[test]
    fn decode_rejects_checksum_mismatch() {
        let payload = encode_checkpoint_payload(12, 8);
        let mut entry = encode_entry(ENTRY_KIND_CHECKPOINT, &payload).unwrap();

        let last = entry.len() - 1;
        entry[last] ^= 0xFF;

        let result = decode_entry(&entry, 0);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }
}
