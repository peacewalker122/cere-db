//! Range-selected leveled compaction with atomic publication of partitioned outputs.

use std::{io::Cursor, path::PathBuf, sync::Arc};

use tokio::io::AsyncWriteExt;

use crate::error::DBError;
use crate::storage::{
    bloom::{self, BloomFilterWrapper},
    config::StorageConfig,
    index::SparseIndexEntry,
    manifest_codec::{ManifestManager, SSTableMeta},
    record::{MemtableRecord, RecordType},
    sstable_codec::SSTableCodec,
    writemanager::block::{Block, BlockBuilder, BlockBuilderState},
};

/// Coalesce queued L0 triggers and drain byte debt in deeper levels.
pub(crate) async fn compact_levels(
    manifest: Arc<ManifestManager>,
    requested_level: u32,
    config: Arc<StorageConfig>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), DBError> {
    if config.target_sstable_bytes == 0
        || config.level_base_bytes == 0
        || config.level_size_multiplier < 2
        || config.max_level0_files == 0
    {
        return Err(DBError::StorageError("invalid compaction targets".into()));
    }
    let mut requested = Some(requested_level);
    loop {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".into()));
        }
        let snapshot = manifest.snapshot().await;
        let mut pending = requested.take().filter(|level| {
            snapshot.levels.get(level).is_some_and(|files| {
                !files.is_empty() && (*level != 0 || files.len() >= config.max_level0_files)
            })
        });
        // Relieve deeper debt before pushing more data out of L0/L1.
        let mut levels: Vec<_> = snapshot
            .levels
            .keys()
            .copied()
            .filter(|level| *level > 0)
            .collect();
        levels.sort_unstable_by(|a, b| b.cmp(a));
        if pending.is_none() {
            for level in levels {
                let target = config
                    .level_base_bytes
                    .saturating_mul(config.level_size_multiplier.saturating_pow(level - 1));
                let mut bytes = 0u64;
                for file in &snapshot.levels[&level] {
                    bytes = bytes.saturating_add(tokio::fs::metadata(&file.path).await?.len());
                }
                if bytes > target {
                    pending = Some(level);
                    break;
                }
            }
        }
        if pending.is_none()
            && snapshot
                .levels
                .get(&0)
                .is_some_and(|files| files.len() >= config.max_level0_files)
        {
            pending = Some(0);
        }
        let Some(level) = pending else {
            return Ok(());
        };
        compaction(
            manifest.clone(),
            level,
            config.clone(),
            cancel.child_token(),
        )
        .await?;
    }
}

pub async fn compaction(
    manifest: Arc<ManifestManager>,
    level: u32,
    config: Arc<StorageConfig>,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<SSTableCodec, DBError> {
    let _compaction_lease = manifest.compaction_lease().await;
    let snapshot = manifest.snapshot().await;
    if cancel.is_cancelled() {
        return Err(DBError::StorageError("compaction cancelled".to_string()));
    }

    let manifest_level = snapshot.levels.get(&level);

    if manifest_level.is_none() {
        return Err(DBError::StorageError(format!(
            "Level {} not found in manifest",
            level
        )));
    }

    let mut current_level_files = manifest_level.unwrap().clone();
    current_level_files.sort_by_key(|meta| meta.file_id);
    let next_level = level
        .checked_add(1)
        .ok_or_else(|| DBError::StorageError("compaction level overflow".into()))?;
    let next_level_files = snapshot
        .levels
        .get(&next_level)
        .cloned()
        .unwrap_or_default();

    // Oldest files first. Nonzero levels move one range at a time; L0 can
    // coalesce a small group before closing overlaps in both levels.
    let mut merge_candidates = Vec::new();
    let mut input_bytes = 0u64;
    for meta in &current_level_files {
        if !merge_candidates.is_empty()
            && (level != 0 || input_bytes >= config.target_sstable_bytes)
        {
            break;
        }
        input_bytes = input_bytes.saturating_add(tokio::fs::metadata(&meta.path).await?.len());
        merge_candidates.push(meta.clone());
    }
    let mut selected_range = compute_level_range(&merge_candidates, &cancel)
        .await?
        .ok_or_else(|| DBError::Corrupted(format!("Level {} has no valid key range", level)))?;

    // A destination file can widen the range and reach more source files.
    // Repeat to a fixed point so no older L0 record can shadow a newer L1
    // record, and no output overlaps an untouched destination file.
    loop {
        let previous_count = merge_candidates.len();
        for candidate in current_level_files.iter().chain(&next_level_files) {
            if merge_candidates
                .iter()
                .any(|meta| meta.file_id == candidate.file_id)
            {
                continue;
            }
            let range = compute_meta_range(candidate, &cancel)
                .await?
                .ok_or_else(|| DBError::Corrupted("SSTable has no key range".into()))?;
            if ranges_overlap(&selected_range, &range) {
                selected_range.0 = selected_range.0.min(range.0);
                selected_range.1 = selected_range.1.max(range.1);
                merge_candidates.push(candidate.clone());
            }
        }
        if previous_count == merge_candidates.len() {
            break;
        }
    }

    let mut sstables = vec![];
    // provisioning the sstables, we need to read the sstables from the disk, and then we can merge them to 1 file, and then we can update the manifest file with the new level store information

    for level_store in &merge_candidates {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".to_string()));
        }
        let sstable_file = tokio::fs::File::open(&level_store.path).await?;
        let mut read_buffer = tokio::io::BufReader::new(sstable_file);
        let (footer, index, bloom) = SSTableCodec::deserialize_sections(&mut read_buffer).await?;
        let sstable_blocks =
            SSTableCodec::get_all_blocks(&mut read_buffer, &footer, &index).await?;

        sstables.push(SSTableCodec {
            blocks: sstable_blocks,
            index,
            bloom, // TODO: we can read the bloom filter from the disk, but for now we just create a new one, because we will rebuild the bloom filter when we merge the files.
        });
    }

    let mut protected_ranges = Vec::new();
    for meta in snapshot.levels.values().flatten() {
        if merge_candidates
            .iter()
            .any(|input| input.file_id == meta.file_id)
        {
            continue;
        }
        let range = compute_meta_range(meta, &cancel)
            .await?
            .ok_or_else(|| DBError::Corrupted("SSTable has no key range".to_string()))?;
        protected_ranges.push(range);
    }
    let merged_sstable =
        merge_files_with_reclamation(sstables, &config, &cancel, Some(&protected_ranges)).await?;

    if cancel.is_cancelled() {
        return Err(DBError::StorageError("compaction cancelled".to_string()));
    }

    let mut outputs = Vec::new();
    for table in partition_output(&merged_sstable, &config) {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".into()));
        }
        outputs.push(
            write_compaction_output(&manifest, &current_level_files[0].path, next_level, &table)
                .await?,
        );
    }

    let _publication_lease = manifest.publication_lease().await;
    if cancel.is_cancelled() {
        return Err(DBError::StorageError("compaction cancelled".to_string()));
    }
    manifest
        .publish_compaction(&snapshot, &merge_candidates, outputs)
        .await?;
    remove_compacted_files(&manifest, &merge_candidates).await;

    Ok(merged_sstable)
}

fn partition_output(merged: &SSTableCodec, config: &StorageConfig) -> Vec<SSTableCodec> {
    let mut outputs = Vec::new();
    let mut records = Vec::new();
    let mut bytes = 0u64;
    for record in merged
        .blocks
        .iter()
        .flat_map(|block| block.data.iter().flatten())
    {
        let record_bytes = record.record_length(&record.key) as u64;
        if !records.is_empty() && bytes.saturating_add(record_bytes) > config.target_sstable_bytes {
            outputs.push(build_output(std::mem::take(&mut records), config));
            bytes = 0;
        }
        records.push(record.clone());
        bytes = bytes.saturating_add(record_bytes);
    }
    if !records.is_empty() {
        outputs.push(build_output(records, config));
    }
    outputs
}

fn build_output(records: Vec<MemtableRecord>, config: &StorageConfig) -> SSTableCodec {
    let mut builder = BlockBuilder::new(0);
    let mut blocks = Vec::new();
    let mut bloom = BloomFilterWrapper::with_rate(records.len(), config.bloom_false_positive_rate);
    for record in records {
        flush_winner(&mut builder, &mut blocks, &mut bloom, record);
    }
    if let Some((block, _)) = builder.build() {
        blocks.push(block);
    }
    SSTableCodec::new(blocks, vec![], bloom)
}

async fn write_compaction_output(
    manifest: &ManifestManager,
    source_path: &str,
    level: u32,
    table: &SSTableCodec,
) -> Result<SSTableMeta, DBError> {
    let mut file_id = manifest.allocate_file_id().await?;
    let (output_path, mut output_file) = loop {
        let path = build_compaction_output_path(source_path, level, file_id);
        if let Some(parent) = path.parent() {
            crate::storage::durable_dir::create_dir_all_durable(parent).await?;
        }
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(file) => break (path, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                file_id = manifest.allocate_file_id().await?;
            }
            Err(error) => return Err(error.into()),
        }
    };
    let (encoded, footer) = table.serialize();
    output_file.write_all(&encoded).await?;
    output_file.sync_all().await?;
    tokio::fs::File::open(output_path.parent().unwrap())
        .await?
        .sync_all()
        .await?;

    let record_count = table
        .blocks
        .iter()
        .map(|block| block.record_count as usize)
        .sum();

    let (smallest_key, largest_key) = range_from_blocks(&table.blocks)
        .ok_or_else(|| DBError::Corrupted("compaction output has no key range".into()))?;
    Ok(SSTableMeta {
        file_id,
        level,
        path: output_path.to_string_lossy().into_owned(),
        record_count,
        bloom_offset: footer.bloom_block_start,
        bloom_size: footer.bloom_block_end - footer.bloom_block_start,
        smallest_key,
        largest_key,
    })
}

async fn remove_compacted_files(manifest: &ManifestManager, inputs: &[SSTableMeta]) {
    for input in inputs {
        manifest.evict_file(&input.path).await;
        if let Err(error) = tokio::fs::remove_file(&input.path).await {
            log::warn!(
                "Failed to remove compacted SSTable {}: {}",
                input.path,
                error
            );
        }
    }
}

fn build_compaction_output_path(base_file_path: &str, level: u32, file_id: u64) -> PathBuf {
    let source = PathBuf::from(base_file_path);
    let level_dir = source
        .parent()
        .and_then(|parent| {
            parent
                .parent()
                .map(|sstable_root| sstable_root.join(format!("level-{level}")))
        })
        .unwrap_or_else(|| PathBuf::from(format!("level-{level}")));

    level_dir.join(format!("sstable-{file_id}.dat"))
}

fn ranges_overlap(left: &(Vec<u8>, Vec<u8>), right: &(Vec<u8>, Vec<u8>)) -> bool {
    left.0 <= right.1 && right.0 <= left.1
}

fn range_from_blocks(
    blocks: &[crate::storage::writemanager::block::Block],
) -> Option<(Vec<u8>, Vec<u8>)> {
    let first = blocks.first()?;
    let last = blocks.last()?;
    Some((first.first_key.clone(), last.last_key.clone()))
}

async fn compute_meta_range(
    meta: &SSTableMeta,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, DBError> {
    if cancel.is_cancelled() {
        return Err(DBError::StorageError("compaction cancelled".to_string()));
    }

    if !meta.smallest_key.is_empty() && !meta.largest_key.is_empty() {
        return Ok(Some((meta.smallest_key.clone(), meta.largest_key.clone())));
    }

    let sstable_file = tokio::fs::File::open(&meta.path).await?;
    let mut read_buffer = tokio::io::BufReader::new(sstable_file);
    let (_footer, index, _bloom) = SSTableCodec::deserialize_sections(&mut read_buffer).await?;
    Ok(SSTableCodec::range_from_index(&index))
}

async fn compute_level_range(
    metas: &[SSTableMeta],
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Option<(Vec<u8>, Vec<u8>)>, DBError> {
    let mut range: Option<(Vec<u8>, Vec<u8>)> = None;

    for meta in metas {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".to_string()));
        }
        if let Some((smallest, largest)) = compute_meta_range(meta, cancel).await? {
            range = Some(match range {
                Some((cur_min, cur_max)) => (cur_min.min(smallest), cur_max.max(largest)),
                None => (smallest, largest),
            });
        }
    }

    Ok(range)
}

#[cfg(test)]
async fn merge_files(
    sstables: Vec<SSTableCodec>,
    config: &Arc<StorageConfig>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<SSTableCodec, DBError> {
    merge_files_with_reclamation(sstables, config, cancel, None).await
}

async fn merge_files_with_reclamation(
    sstables: Vec<SSTableCodec>,
    config: &Arc<StorageConfig>,
    cancel: &tokio_util::sync::CancellationToken,
    protected_ranges: Option<&[(Vec<u8>, Vec<u8>)]>,
) -> Result<SSTableCodec, DBError> {
    let mut merged_by_key: std::collections::BTreeMap<Vec<u8>, MemtableRecord> =
        std::collections::BTreeMap::new();

    for sstable in &sstables {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".to_string()));
        }
        for block in &sstable.blocks {
            if let Some(records) = &block.data {
                for record in records {
                    let key = record.key.clone();
                    let candidate = record.clone();

                    let winner = match merged_by_key.get(&key) {
                        Some(existing) => choose_winner(existing.clone(), candidate),
                        None => candidate,
                    };

                    merged_by_key.insert(key, winner);
                }
            }
        }
    }

    let mut block_builder = BlockBuilder::new(0);
    let mut block_entries: Vec<Block> = Vec::new();

    // TODO: need to store the actual number of records on the footer of sstable.
    let mut bloom_filter = BloomFilterWrapper::with_rate(1000000, config.bloom_false_positive_rate);
    for winner in merged_by_key.into_values() {
        if cancel.is_cancelled() {
            return Err(DBError::StorageError("compaction cancelled".to_string()));
        }
        if winner.record_type == RecordType::Delete
            && protected_ranges.is_some_and(|ranges| {
                !ranges
                    .iter()
                    .any(|(start, end)| start <= &winner.key && &winner.key <= end)
            })
        {
            continue;
        }
        flush_winner(
            &mut block_builder,
            &mut block_entries,
            &mut bloom_filter,
            winner,
        );
    }

    if let Some((block, _)) = block_builder.build() {
        log::debug!(
            "Final block with first_key={}, last_key={}, record_count={}, data_size={}",
            String::from_utf8_lossy(&block.first_key),
            String::from_utf8_lossy(&block.last_key),
            block.record_count,
            block.data_size
        );
        block_entries.push(block);
    }

    Ok(SSTableCodec {
        blocks: block_entries,
        index: vec![],
        bloom: bloom_filter,
    })
}

fn choose_winner(current: MemtableRecord, candidate: MemtableRecord) -> MemtableRecord {
    if candidate.lsn > current.lsn {
        return candidate;
    }

    if candidate.lsn < current.lsn {
        return current;
    }

    match (current.record_type, candidate.record_type) {
        (RecordType::Delete, _) => current,
        (_, RecordType::Delete) => candidate,
        _ => current,
    }
}

fn flush_winner(
    block_builder: &mut BlockBuilder,
    block_entries: &mut Vec<Block>,
    bloom_filter: &mut BloomFilterWrapper,
    winner: MemtableRecord,
) {
    bloom_filter.insert(&winner.key);

    match block_builder.add_record(&winner.key, &winner) {
        BlockBuilderState::EnoughSpace => {}
        BlockBuilderState::Full(block, _) => {
            block_builder.add_record(&winner.key, &winner);
            log::debug!(
                "Block full with first_key={}, last_key={}, record_count={}, data_size={}",
                String::from_utf8_lossy(&block.first_key),
                String::from_utf8_lossy(&block.last_key),
                block.record_count,
                block.data_size
            );

            block_entries.push(block)
        }
    }
}

// ============================================================================
// Unit Tests for merge_files and compaction functions
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{bloom::BloomFilterWrapper, record::RecordType};

    /// Helper: Create a MemtableRecord with key, value, record_type, and lsn
    fn make_record(key: &[u8], value: &[u8], record_type: RecordType, lsn: u64) -> MemtableRecord {
        MemtableRecord::new(value.to_vec(), record_type, lsn).with_key(key.to_vec())
    }

    /// Helper: Create a Block with given records
    fn make_block(records: Vec<MemtableRecord>, offset: u64) -> Block {
        let first_key = records.first().map(|r| r.key.clone()).unwrap_or_default();
        let last_key = records.last().map(|r| r.key.clone()).unwrap_or_default();
        let data_size = records
            .iter()
            .map(|r| r.record_length(&r.key))
            .sum::<usize>() as u32;

        Block {
            offset,
            first_key,
            last_key,
            record_count: records.len() as u32,
            data_size,
            data: Some(records),
        }
    }

    /// Helper: Create an SSTableCodec with given blocks
    fn make_sstable(blocks: Vec<Block>) -> SSTableCodec {
        SSTableCodec::new(blocks, vec![], BloomFilterWrapper::with_rate(1000, 0.01))
    }

    // ===== Happy Path Tests =====

    #[tokio::test]
    async fn merge_single_sstable_single_block() {
        // Test: Single SSTable with one block containing multiple records → verifies sorted output
        let records = vec![
            make_record(b"key1", b"val1", RecordType::Put, 100),
            make_record(b"key2", b"val2", RecordType::Put, 200),
            make_record(b"key3", b"val3", RecordType::Put, 300),
        ];
        let block = make_block(records, 0);
        let sstables = vec![make_sstable(vec![block])];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify sorted output
        assert_eq!(result.blocks.len(), 1);
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 3);
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[1].key, b"key2");
        assert_eq!(block_data[2].key, b"key3");
    }

    #[tokio::test]
    async fn merge_multiple_sstables_non_overlapping() {
        // Test: 2 SSTables with non-overlapping key ranges → verifies sorted merge
        // SSTable 1: key1, key2
        let sst1_records = vec![
            make_record(b"key1", b"val1", RecordType::Put, 100),
            make_record(b"key2", b"val2", RecordType::Put, 200),
        ];
        // SSTable 2: key3, key4
        let sst2_records = vec![
            make_record(b"key3", b"val3", RecordType::Put, 300),
            make_record(b"key4", b"val4", RecordType::Put, 400),
        ];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify all 4 keys in sorted order
        assert_eq!(result.blocks.len(), 1);
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 4);
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[1].key, b"key2");
        assert_eq!(block_data[2].key, b"key3");
        assert_eq!(block_data[3].key, b"key4");
    }

    #[tokio::test]
    async fn merge_duplicate_keys_timestamps() {
        // Test: Same key in multiple SSTables with different timestamps → verifies newer timestamp wins
        // SSTable 1: key=key1, lsn=100
        let sst1_records = vec![make_record(b"key1", b"old_value", RecordType::Put, 100)];
        // SSTable 2: key=key1, lsn=200 (newer)
        let sst2_records = vec![make_record(b"key1", b"new_value", RecordType::Put, 200)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify only one record exists (deduplicated by key)
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 1);
        // Newer LSN should win (200 > 100)
        assert_eq!(block_data[0].lsn, 200);
        assert_eq!(block_data[0].value, b"new_value");
    }

    // ===== Edge Case / Side Effect Tests =====

    #[tokio::test]
    async fn merge_empty_blocks() {
        // Test: SSTable with data: None → should skip gracefully
        let empty_block = Block {
            offset: 0,
            first_key: vec![],
            last_key: vec![],
            record_count: 0,
            data_size: 0,
            data: None,
        };
        let records = vec![make_record(b"key1", b"val1", RecordType::Put, 100)];
        let normal_block = make_block(records, 0);

        let sstables = vec![
            make_sstable(vec![empty_block]),
            make_sstable(vec![normal_block]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Should have the normal block's record
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 1);
        assert_eq!(block_data[0].key, b"key1");
    }

    #[tokio::test]
    async fn merge_single_record_sstables() {
        // Test: Each SSTable has exactly 1 record → verifies heap behavior
        let sst1_records = vec![make_record(b"aaa", b"val1", RecordType::Put, 100)];
        let sst2_records = vec![make_record(b"bbb", b"val2", RecordType::Put, 200)];
        let sst3_records = vec![make_record(b"ccc", b"val3", RecordType::Put, 300)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
            make_sstable(vec![make_block(sst3_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify all 3 records in sorted order
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 3);
        assert_eq!(block_data[0].key, b"aaa");
        assert_eq!(block_data[1].key, b"bbb");
        assert_eq!(block_data[2].key, b"ccc");
    }

    #[tokio::test]
    #[ignore = "Skipped: memory allocation error - needs investigation"]
    async fn merge_block_full_triggers_new_block() {
        env_logger::builder().is_test(true).init();

        // Test: Many records to trigger block capacity → verifies multiple blocks created
        // Create enough small records to fill multiple blocks
        let small_value = b"x".to_vec();
        let mut sst_records = Vec::new();
        for i in 0..500u32 {
            let key = format!("key{:03}", i);
            sst_records.push(make_record(
                key.as_bytes(),
                &small_value,
                RecordType::Put,
                i as u64,
            ));
        }

        let sstables = vec![make_sstable(vec![make_block(sst_records, 0)])];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify multiple blocks were created due to capacity
        assert!(
            result.blocks.len() > 1,
            "Expected multiple blocks, got {}",
            result.blocks.len()
        );

        // Verify all records are present
        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 500);
    }

    #[tokio::test]
    async fn merge_bloom_filter_contains_all_keys() {
        // Test: After merge, verify bloom filter has all merged keys
        let sst1_records = vec![
            make_record(b"key1", b"val1", RecordType::Put, 100),
            make_record(b"key2", b"val2", RecordType::Put, 200),
        ];
        let sst2_records = vec![
            make_record(b"key3", b"val3", RecordType::Put, 300),
            make_record(b"key4", b"val4", RecordType::Put, 400),
        ];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify bloom filter contains all merged keys
        assert!(result.bloom.contains(b"key1"));
        assert!(result.bloom.contains(b"key2"));
        assert!(result.bloom.contains(b"key3"));
        assert!(result.bloom.contains(b"key4"));
    }

    // ===== Additional Happy Path Tests for compaction =====

    #[tokio::test]
    async fn compaction_with_delete_tombstones() {
        let records = vec![
            make_record(b"key1", b"val1", RecordType::Put, 100),
            make_record(b"key2", b"", RecordType::Delete, 200),
            make_record(b"key3", b"val3", RecordType::Put, 300),
        ];
        let block = make_block(records, 0);
        let sstables = vec![make_sstable(vec![block])];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 3);
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[1].key, b"key2");
        assert_eq!(block_data[1].record_type, RecordType::Delete);
        assert_eq!(block_data[2].key, b"key3");
    }

    #[tokio::test]
    async fn merge_duplicate_keys_equal_lsn_delete_wins() {
        let sst1_records = vec![make_record(b"key1", b"alive", RecordType::Put, 200)];
        let sst2_records = vec![make_record(b"key1", b"", RecordType::Delete, 200)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 1);
        assert_eq!(
            result.blocks[0].data.as_ref().unwrap()[0].record_type,
            RecordType::Delete
        );
        assert!(result.bloom.contains(b"key1"));
    }

    #[tokio::test]
    async fn merge_duplicate_keys_newest_delete_retained() {
        let sst1_records = vec![make_record(b"key1", b"older", RecordType::Put, 100)];
        let sst2_records = vec![make_record(b"key1", b"", RecordType::Delete, 300)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 1);
        assert_eq!(
            result.blocks[0].data.as_ref().unwrap()[0].record_type,
            RecordType::Delete
        );
        assert!(result.bloom.contains(b"key1"));
    }

    #[tokio::test]
    async fn merge_duplicate_keys_older_delete_newer_put_survives() {
        // Test: Older delete and newer put for same key -> newer Put survives.
        let sst1_records = vec![make_record(b"key1", b"", RecordType::Delete, 100)];
        let sst2_records = vec![make_record(b"key1", b"restored", RecordType::Put, 300)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 1);
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[0].value, b"restored");
        assert_eq!(block_data[0].record_type, RecordType::Put);
    }

    #[tokio::test]
    async fn merge_duplicate_key_appears_once_after_compaction() {
        // Test: Multiple versions of same key collapse into one output entry.
        let sst1_records = vec![make_record(b"key1", b"v1", RecordType::Put, 100)];
        let sst2_records = vec![make_record(b"key1", b"v2", RecordType::Put, 200)];
        let sst3_records = vec![make_record(b"key1", b"v3", RecordType::Put, 300)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
            make_sstable(vec![make_block(sst3_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 1);
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[0].value, b"v3");
        assert_eq!(block_data[0].lsn, 300);
    }

    #[tokio::test]
    async fn merge_duplicate_keys_non_contiguous_input_order() {
        // Test: duplicate keys can appear in non-contiguous read order and still deduplicate correctly.
        // This intentionally uses unsorted record order inside an SSTable to validate map-based correctness.
        let sst1_records = vec![
            make_record(b"key2", b"v2", RecordType::Put, 100),
            make_record(b"key1", b"old", RecordType::Put, 100),
        ];
        let sst2_records = vec![
            make_record(b"key3", b"v3", RecordType::Put, 100),
            make_record(b"key1", b"", RecordType::Delete, 200),
        ];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 3);
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[0].record_type, RecordType::Delete);
        assert_eq!(block_data[1].key, b"key2");
        assert_eq!(block_data[2].key, b"key3");
        assert!(result.bloom.contains(b"key1"));
        assert!(result.bloom.contains(b"key2"));
        assert!(result.bloom.contains(b"key3"));
    }

    #[tokio::test]
    async fn compaction_merge_reverse_order() {
        // Test: SSTables in reverse key order → still merges correctly
        let sst1_records = vec![
            make_record(b"y_key", b"val2", RecordType::Put, 200),
            make_record(b"z_key", b"val1", RecordType::Put, 100),
        ];
        let sst2_records = vec![
            make_record(b"a_key", b"val3", RecordType::Put, 300),
            make_record(b"b_key", b"val4", RecordType::Put, 400),
        ];

        // Note: The merge will sort by key during merge
        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let result = merge_files(sstables, &Arc::new(StorageConfig::default()), &cancel)
            .await
            .unwrap();

        // Verify all 4 keys sorted correctly
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 4);
        assert_eq!(block_data[0].key, b"a_key");
        assert_eq!(block_data[1].key, b"b_key");
        assert_eq!(block_data[2].key, b"y_key");
        assert_eq!(block_data[3].key, b"z_key");
    }

    #[test]
    fn ranges_overlap_when_boundaries_touch() {
        let left = (b"a".to_vec(), b"m".to_vec());
        let right = (b"m".to_vec(), b"z".to_vec());
        assert!(ranges_overlap(&left, &right));
    }

    #[test]
    fn ranges_do_not_overlap_when_disjoint() {
        let left = (b"a".to_vec(), b"f".to_vec());
        let right = (b"g".to_vec(), b"z".to_vec());
        assert!(!ranges_overlap(&left, &right));
    }

    #[tokio::test]
    async fn compute_level_range_uses_manifest_range_when_present() {
        let metas = vec![
            SSTableMeta {
                file_id: 1,
                level: 0,
                path: "unused-a".to_string(),
                record_count: 10,
                bloom_offset: 4096,
                bloom_size: 1024,
                smallest_key: b"b".to_vec(),
                largest_key: b"d".to_vec(),
            },
            SSTableMeta {
                file_id: 2,
                level: 0,
                path: "unused-b".to_string(),
                record_count: 10,
                bloom_offset: 8192,
                bloom_size: 1024,
                smallest_key: b"a".to_vec(),
                largest_key: b"z".to_vec(),
            },
        ];

        let cancel = tokio_util::sync::CancellationToken::new();
        let range = compute_level_range(&metas, &cancel).await.unwrap().unwrap();
        assert_eq!(range.0, b"a");
        assert_eq!(range.1, b"z");
    }
}
