//! Integration tests for compaction disk usage stability.
//!
//! Verifies that SSTable files are properly deleted after compaction,
//! ensuring no disk leak across repeated compaction cycles.

use std::sync::Arc;

use ceredb::storage::{
    compactionmanager::compaction::compaction,
    config::StorageConfig,
    constant::MAXIMUM_LEVEL_FILES,
    manifest_codec::ManifestManager,
};
use ceredb::testing::{count_sstable_files, list_sstable_files, total_sstable_files};

/// Verify disk usage stays stable after repeated compaction cycles.
///
/// Arrange: Create a temp directory with KV2 storage layout.
/// Act: Write enough data to trigger multiple flushes, then run compaction.
/// Assert: L0 files drop to 0 after compaction (proper cleanup).
/// Assert: L1 has exactly 1 file (merged output replaces old L1).
/// Repeat 3 cycles to verify stability.
#[tokio::test]
async fn compaction_disk_usage_stable_across_repeated_cycles() {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempfile::Builder::new()
        .prefix("compaction-disk-stability")
        .tempdir()
        .expect("Failed to create temp directory");
    let base_dir = temp_dir.path().to_path_buf();

    let wal_dir = base_dir.join("wal");
    let sstable_dir = base_dir.join("sstable");
    let level0_dir = sstable_dir.join("level-0");
    let manifest_path = base_dir.join("MANIFEST");

    tokio::fs::create_dir_all(&wal_dir).await.unwrap();
    tokio::fs::create_dir_all(&level0_dir).await.unwrap();

    let manifest = Arc::new(ManifestManager::load_or_create(manifest_path).await.unwrap());

    let cancel = tokio_util::sync::CancellationToken::new();

    // Run 3 compaction cycles
    for cycle in 1..=3 {
        // Arrange: Write enough L0 files to trigger compaction
        let _files_before = write_l0_files_until_threshold(&sstable_dir, &manifest, cycle).await;

        let l0_count_before = count_sstable_files(&base_dir, 0);
        assert!(
            l0_count_before >= MAXIMUM_LEVEL_FILES,
            "Cycle {cycle}: Expected >= {MAXIMUM_LEVEL_FILES} L0 files before compaction, got {l0_count_before}"
        );

        // Act: Run compaction on L0
        let config = Arc::new(StorageConfig::default());
        let result = compaction(Arc::clone(&manifest), 0, config, cancel.child_token()).await;

        // Debug: print state after compaction
        let l0_after = count_sstable_files(&base_dir, 0);
        let l1_after = count_sstable_files(&base_dir, 1);
        let total_after = total_sstable_files(&base_dir);
        let all_files = list_sstable_files(&base_dir);
        eprintln!("Cycle {cycle} AFTER compaction: L0={l0_after}, L1={l1_after}, total={total_after}, files={all_files:?}, result={result:?}");

        if let Err(ref e) = result {
            eprintln!("Cycle {cycle} compaction error: {e}");
            let snap = manifest.snapshot().await;
            eprintln!("  Manifest L0 entries: {:?}", snap.levels.get(&0).map(|f| f.iter().map(|m| &m.path).collect::<Vec<_>>()));
            eprintln!("  Manifest L1 entries: {:?}", snap.levels.get(&1).map(|f| f.iter().map(|m| &m.path).collect::<Vec<_>>()));
        }

        // Assert: Compaction succeeded
        assert!(
            result.is_ok(),
            "Cycle {cycle}: Compaction failed: {:?}. L0 files before: {}. All files: {:?}",
            result.err(),
            l0_count_before,
            list_sstable_files(&base_dir)
        );

        // Assert: L0 files fully cleaned up (0 remaining)
        assert_eq!(
            l0_after, 0,
            "Cycle {cycle}: Expected 0 L0 files after compaction, got {l0_after}. Leaked L0 files: {:?}",
            list_sstable_files(&base_dir).iter().filter(|p| p.to_string_lossy().contains("level-0")).collect::<Vec<_>>()
        );

        // Assert: L1 has exactly 1 file (merged output replaces any existing L1)
        assert_eq!(
            l1_after, 1,
            "Cycle {cycle}: Expected exactly 1 L1 file after compaction, got {l1_after}. Files: {:?}",
            all_files
        );

        // Assert: Total files = 1 (no orphaned files from any level)
        assert_eq!(
            total_after, 1,
            "Cycle {cycle}: Expected exactly 1 total file, got {total_after}. Orphaned files: {:?}",
            all_files
        );

        // Assert: File count matches manifest state
        let snapshot = manifest.snapshot().await;
        let manifest_l0 = snapshot.levels.get(&0).map_or(0, |f| f.len());
        let manifest_l1 = snapshot.levels.get(&1).map_or(0, |f| f.len());
        assert_eq!(
            l0_after, manifest_l0,
            "Cycle {cycle}: L0 disk files ({l0_after}) != manifest entries ({manifest_l0})"
        );
        assert_eq!(
            l1_after, manifest_l1,
            "Cycle {cycle}: L1 disk files ({l1_after}) != manifest entries ({manifest_l1})"
        );
    }
}

/// Write synthetic SSTable files to L0 until the compaction threshold is reached.
///
/// Uses overlapping key ranges across cycles so compaction merges into a single L1 file.
async fn write_l0_files_until_threshold(
    sstable_dir: &std::path::Path,
    manifest: &ManifestManager,
    _cycle: u32,
) -> Vec<String> {
    let mut written = Vec::new();

    // Keep writing until we have enough L0 files
    loop {
        let file_id = manifest.allocate_file_id().await.unwrap();
        let level0_dir = sstable_dir.join("level-0");
        let file_path = level0_dir.join(format!("sstable-{file_id}.dat"));

        // Create a minimal valid SSTable with overlapping key ranges across cycles.
        // Using a shared key range [a, z] ensures compaction merges into one L1 file.
        let data = build_minimal_sstable(file_id);
        tokio::fs::write(&file_path, &data).await.unwrap();

        // Register in manifest
        manifest
            .register_sstable(
                ceredb::storage::manifest_codec::SSTableMeta {
                    file_id,
                    level: 0,
                    path: file_path.to_string_lossy().to_string(),
                    record_count: 2,
                    bloom_offset: 4096,
                    bloom_size: 1024,
                    smallest_key: b"a".to_vec(),
                    largest_key: b"z".to_vec(),
                },
            )
            .await
            .unwrap();

        written.push(file_path.to_string_lossy().to_string());

        let l0_count = count_sstable_files(sstable_dir.parent().unwrap(), 0);
        if l0_count >= MAXIMUM_LEVEL_FILES {
            break;
        }
    }

    written
}

/// Build a minimal valid SSTable binary with 2 records.
fn build_minimal_sstable(file_id: u64) -> Vec<u8> {
    use ceredb::storage::{
        bloom::BloomFilterWrapper,
        index::SparseIndexEntry,
        record::{MemtableRecord, RecordType},
        sstable_codec::SSTableCodec,
        writemanager::block::Block,
    };

    let key_a = b"a".to_vec();
    let key_b = b"z".to_vec();

    let records = vec![
        MemtableRecord::new(b"val-a".to_vec(), RecordType::Put, file_id).with_key(key_a.clone()),
        MemtableRecord::new(b"val-b".to_vec(), RecordType::Put, file_id + 1).with_key(key_b.clone()),
    ];

    let data_size: u32 = records.iter().map(|r| r.record_length(&r.key)).sum::<usize>() as u32;

    let block = Block {
        offset: 0,
        first_key: key_a.clone(),
        last_key: key_b.clone(),
        record_count: 2,
        data_size,
        data: Some(records),
    };

    let index = vec![SparseIndexEntry {
        first_key: key_a,
        block_offset: 0,
        last_key: key_b,
        record_count: 2,
    }];

    let mut bloom = BloomFilterWrapper::with_rate(10, 0.01);
    bloom.insert(b"a");
    bloom.insert(b"z");

    let codec = SSTableCodec::new(vec![block], index, bloom);
    codec.serialize().0
}
