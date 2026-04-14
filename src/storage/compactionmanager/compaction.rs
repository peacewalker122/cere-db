// what's the idea behind all of this?
// the compaction is an action that will be happen when an level store is full, and we need to
// merge the data within that level to 1 file, this action would be propagated when the level + 1
// store is also full, and we need to merege the data within that level to 1 file, and so on, until we find a level that is not full, then we can stop the compaction action.
//
// Who would be the caller? The WriteManager... when it flushes and the level store threshold were
// exceeded, it will trigger the compaction action, and the compaction manager will be responsible for managing the compaction action, and the compaction action will be responsible for merging the data within the level store to 1 file, and then it will return the result to the WriteManager, and the WriteManager will update the manifest file with the new level store information.

use std::io::Cursor;

use crate::storage::{
    bloom::{self, BloomFilterWrapper},
    manifest_codec::ManifestManager,
    record::{MemtableRecord, RecordType},
    sstable::SparseIndexEntry,
    sstable_codec::SSTableCodec,
    writemanager::block::{Block, BlockBuilder, BlockBuilderState},
};

pub async fn compaction(
    manifest: ManifestManager,
    level: u32,
) -> Result<SSTableCodec, std::io::Error> {
    // TODO:
    // 1. read the manifest file and get the level store information
    // 2. check if the level store is full, if not, return
    // 3. if the level store is full, merge the data within the level store to 1 file
    // 4. update the manifest file with the new level store information

    let snapshot = manifest.snapshot().await;
    let manifest_level = snapshot.levels.get(&level);

    if manifest_level.is_none() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Level {} not found in manifest", level),
        ));
    }

    let levels = manifest_level.unwrap();
    if levels.len() <= 1 {
        log::warn!("Level {} is not full, no need to compact", level);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("Level {} is not full, no need to compact", level),
        ));
    }

    // need to merge k files to 1 file, and update the manifest file with the new level store information

    let mut sstables = vec![];
    // provisioning the sstables, we need to read the sstables from the disk, and then we can merge them to 1 file, and then we can update the manifest file with the new level store information

    for level_store in levels {
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

    let merged_sstable = merge_files(sstables).await?;

    Ok(merged_sstable)
}

async fn merge_files(sstables: Vec<SSTableCodec>) -> Result<SSTableCodec, std::io::Error> {
    let mut merged_by_key: std::collections::BTreeMap<Vec<u8>, MemtableRecord> =
        std::collections::BTreeMap::new();

    for sstable in &sstables {
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
    let mut bloom_filter = BloomFilterWrapper::with_rate(1000000, 0.0001);
    for winner in merged_by_key.into_values() {
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
    if winner.record_type == RecordType::Delete {
        return;
    }

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
    use crate::storage::record::RecordType;

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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

        // Verify all 3 records in sorted order
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 3);
        assert_eq!(block_data[0].key, b"aaa");
        assert_eq!(block_data[1].key, b"bbb");
        assert_eq!(block_data[2].key, b"ccc");
    }

    #[tokio::test]
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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

        // Verify bloom filter contains all merged keys
        assert!(result.bloom.contains(b"key1"));
        assert!(result.bloom.contains(b"key2"));
        assert!(result.bloom.contains(b"key3"));
        assert!(result.bloom.contains(b"key4"));
    }

    // ===== Additional Happy Path Tests for compaction =====

    #[tokio::test]
    async fn compaction_with_delete_tombstones() {
        // Test: Delete tombstone should be dropped from compacted output
        let records = vec![
            make_record(b"key1", b"val1", RecordType::Put, 100),
            make_record(b"key2", b"", RecordType::Delete, 200),
            make_record(b"key3", b"val3", RecordType::Put, 300),
        ];
        let block = make_block(records, 0);
        let sstables = vec![make_sstable(vec![block])];

        let result = merge_files(sstables).await.unwrap();

        // Verify delete tombstone is removed
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 2);
        assert_eq!(block_data[0].key, b"key1");
        assert_eq!(block_data[1].key, b"key3");
    }

    #[tokio::test]
    async fn merge_duplicate_keys_equal_lsn_delete_wins() {
        // Test: Same key with equal LSN where one is Delete -> Delete wins and key is dropped.
        let sst1_records = vec![make_record(b"key1", b"alive", RecordType::Put, 200)];
        let sst2_records = vec![make_record(b"key1", b"", RecordType::Delete, 200)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let result = merge_files(sstables).await.unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 0);
        assert!(!result.bloom.contains(b"key1"));
    }

    #[tokio::test]
    async fn merge_duplicate_keys_newest_delete_dropped() {
        // Test: Newest version for a duplicated key is Delete -> key absent in final output.
        let sst1_records = vec![make_record(b"key1", b"older", RecordType::Put, 100)];
        let sst2_records = vec![make_record(b"key1", b"", RecordType::Delete, 300)];

        let sstables = vec![
            make_sstable(vec![make_block(sst1_records, 0)]),
            make_sstable(vec![make_block(sst2_records, 0)]),
        ];

        let result = merge_files(sstables).await.unwrap();

        let total_records: usize = result
            .blocks
            .iter()
            .map(|b| b.data.as_ref().map(|d| d.len()).unwrap_or(0))
            .sum();

        assert_eq!(total_records, 0);
        assert!(!result.bloom.contains(b"key1"));
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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

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

        let result = merge_files(sstables).await.unwrap();

        // key1 should be dropped due to newer delete; key2 and key3 should remain.
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 2);
        assert_eq!(block_data[0].key, b"key2");
        assert_eq!(block_data[1].key, b"key3");
        assert!(!result.bloom.contains(b"key1"));
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

        let result = merge_files(sstables).await.unwrap();

        // Verify all 4 keys sorted correctly
        let block_data = result.blocks[0].data.as_ref().unwrap();
        assert_eq!(block_data.len(), 4);
        assert_eq!(block_data[0].key, b"a_key");
        assert_eq!(block_data[1].key, b"b_key");
        assert_eq!(block_data[2].key, b"y_key");
        assert_eq!(block_data[3].key, b"z_key");
    }
}
