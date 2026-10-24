use std::{path::Path, sync::Arc};

use ceredb::storage::{
    bloom::BloomFilterWrapper,
    compactionmanager::compaction::compaction,
    manifest_codec::{ManifestManager, SSTableMeta},
    record::{MemtableRecord, RecordType},
    sstable_codec::SSTableCodec,
    writemanager::block::{BlockBuilder, BlockBuilderState},
};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use ceredb::KV2;
use ceredb::api::api::AsyncKVEngine;
use ceredb::storage::config::StorageConfig;

#[tokio::test]
async fn mixed_memtable_mutations_survive_older_sstable_values_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 30,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();

    database
        .put(b"item-a".to_vec(), vec![b'o'; 16])
        .await
        .unwrap();
    database
        .put(b"item-b".to_vec(), vec![b'b'; 16])
        .await
        .unwrap();
    assert_eq!(
        database.get(b"item-a").await.unwrap().unwrap().as_ref(),
        &[b'o'; 16]
    );

    database.delete(b"item-a".to_vec()).await.unwrap();
    assert_eq!(
        database
            .scan(b"item-".to_vec()..=b"item-z".to_vec())
            .await
            .unwrap()
            .len(),
        1
    );

    database
        .put(b"item-a".to_vec(), b"latest".to_vec())
        .await
        .unwrap();
    let expected = vec![
        (b"item-a".to_vec(), b"latest".to_vec()),
        (b"item-b".to_vec(), vec![b'b'; 16]),
    ];
    assert_eq!(
        database
            .scan(b"item-".to_vec()..=b"item-z".to_vec())
            .await
            .unwrap(),
        expected
    );
    drop(database);

    let reopened = KV2::open(directory.path(), config).await.unwrap();
    assert_eq!(
        reopened.get(b"item-a").await.unwrap().unwrap().as_ref(),
        b"latest"
    );
    assert_eq!(
        reopened
            .scan(b"item-".to_vec()..=b"item-z".to_vec())
            .await
            .unwrap(),
        expected
    );
}

fn fixture_record(key: &[u8], value: &[u8], record_type: RecordType, lsn: u64) -> MemtableRecord {
    MemtableRecord::new(value.to_vec(), record_type, lsn).with_key(key.to_vec())
}

fn fixture_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 64 * 1024 * 1024,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    }
}

async fn initialize_disk_fixture(root: &Path) {
    let database = KV2::open(root, fixture_config()).await.unwrap();
    database
        .wal_manager()
        .write_log(
            &b"!sequence".to_vec(),
            &b"ready".to_vec(),
            1000,
            RecordType::Put,
        )
        .await
        .unwrap();
}

async fn persist_table(
    root: &Path,
    manifest: &ManifestManager,
    level: u32,
    mut records: Vec<MemtableRecord>,
) -> SSTableMeta {
    records.sort_by(|left, right| left.key.cmp(&right.key));
    let smallest_key = records.first().unwrap().key.clone();
    let largest_key = records.last().unwrap().key.clone();
    let record_count = records.len();
    let mut bloom = BloomFilterWrapper::with_rate(record_count, 0.01);
    let mut builder = BlockBuilder::new(0);
    for record in &records {
        bloom.insert(&record.key);
        assert!(matches!(
            builder.add_record(&record.key, record),
            BlockBuilderState::EnoughSpace
        ));
    }
    let (block, _) = builder.build().unwrap();
    let (bytes, footer) = SSTableCodec::new(vec![block], vec![], bloom).serialize();
    let file_id = manifest.allocate_file_id().await.unwrap();
    let directory = root.join("sstable").join(format!("level-{level}"));
    tokio::fs::create_dir_all(&directory).await.unwrap();
    let path = directory.join(format!("sstable-{file_id}.dat"));
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .await
        .unwrap();
    file.write_all(&bytes).await.unwrap();
    file.sync_all().await.unwrap();
    tokio::fs::File::open(directory)
        .await
        .unwrap()
        .sync_all()
        .await
        .unwrap();
    let meta = SSTableMeta {
        file_id,
        level,
        path: path.to_string_lossy().into_owned(),
        record_count,
        smallest_key,
        largest_key,
        bloom_offset: footer.bloom_block_start,
        bloom_size: footer.bloom_block_end - footer.bloom_block_start,
    };
    manifest.register_sstable(meta.clone()).await.unwrap();
    meta
}

async fn assert_target_is_disk_only(database: &KV2) {
    let records = database.wal_manager().recover_records().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].key, b"!sequence");
}

#[tokio::test]
async fn compacted_tombstone_hides_deeper_value_until_persisted_reinsert() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    initialize_disk_fixture(root).await;
    let manifest = Arc::new(
        ManifestManager::load_or_create(root.join("MANIFEST"))
            .await
            .unwrap(),
    );
    let deep = persist_table(
        root,
        &manifest,
        2,
        vec![fixture_record(b"item-a", b"old", RecordType::Put, 1)],
    )
    .await;
    persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"item-a", b"", RecordType::Delete, 3)],
    )
    .await;
    persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"item-z", b"anchor", RecordType::Put, 4)],
    )
    .await;
    let merged = compaction(
        manifest.clone(),
        0,
        Arc::new(fixture_config()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let target = merged
        .blocks
        .iter()
        .flat_map(|block| block.data.iter().flatten())
        .find(|record| record.key == b"item-a")
        .unwrap();
    assert_eq!(target.record_type, RecordType::Delete);
    assert_eq!(target.lsn, 3);
    let snapshot = manifest.snapshot().await;
    assert!(snapshot.levels.get(&0).is_none_or(|files| files.is_empty()));
    assert_eq!(snapshot.levels[&1].len(), 1);
    assert_eq!(snapshot.levels[&2], vec![deep.clone()]);
    assert!(Path::new(&deep.path).exists());
    drop(manifest);

    let database = KV2::open(root, fixture_config()).await.unwrap();
    assert_target_is_disk_only(&database).await;
    assert!(database.get(b"item-a").await.unwrap().is_none());
    assert!(
        database
            .scan(b"item-a".to_vec()..=b"item-a".to_vec())
            .await
            .unwrap()
            .is_empty()
    );
    drop(database);

    let manifest = Arc::new(
        ManifestManager::load_or_create(root.join("MANIFEST"))
            .await
            .unwrap(),
    );
    persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"item-a", b"latest", RecordType::Put, 5)],
    )
    .await;
    persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"item-z", b"anchor", RecordType::Put, 6)],
    )
    .await;
    let merged = compaction(
        manifest.clone(),
        0,
        Arc::new(fixture_config()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let target = merged
        .blocks
        .iter()
        .flat_map(|block| block.data.iter().flatten())
        .find(|record| record.key == b"item-a")
        .unwrap();
    assert_eq!(target.record_type, RecordType::Put);
    assert_eq!(target.value, b"latest");
    assert_eq!(target.lsn, 5);
    assert_eq!(manifest.snapshot().await.levels[&2], vec![deep]);
    drop(manifest);

    let database = KV2::open(root, fixture_config()).await.unwrap();
    assert_target_is_disk_only(&database).await;
    assert_eq!(
        database.get(b"item-a").await.unwrap().unwrap().as_ref(),
        b"latest"
    );
    assert_eq!(
        database
            .scan(b"item-a".to_vec()..=b"item-a".to_vec())
            .await
            .unwrap(),
        vec![(b"item-a".to_vec(), b"latest".to_vec())]
    );
}

#[tokio::test]
async fn disk_only_scans_merge_lsn_order_across_files_and_levels() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    initialize_disk_fixture(root).await;
    let manifest = ManifestManager::load_or_create(root.join("MANIFEST"))
        .await
        .unwrap();
    persist_table(
        root,
        &manifest,
        2,
        vec![
            fixture_record(b"item-a", b"old", RecordType::Put, 1),
            fixture_record(b"item-b", b"", RecordType::Delete, 2),
            fixture_record(b"item-c", b"steady", RecordType::Put, 3),
        ],
    )
    .await;
    persist_table(
        root,
        &manifest,
        0,
        vec![
            fixture_record(b"item-a", b"", RecordType::Delete, 6),
            fixture_record(b"item-b", b"new", RecordType::Put, 7),
        ],
    )
    .await;
    persist_table(
        root,
        &manifest,
        0,
        vec![
            fixture_record(b"item-a", b"older", RecordType::Put, 2),
            fixture_record(b"item-b", b"", RecordType::Delete, 1),
        ],
    )
    .await;
    drop(manifest);
    for _ in 0..2 {
        let database = KV2::open(root, fixture_config()).await.unwrap();
        assert_target_is_disk_only(&database).await;
        assert!(database.get(b"item-a").await.unwrap().is_none());
        assert_eq!(
            database.get(b"item-b").await.unwrap().unwrap().as_ref(),
            b"new"
        );
        assert_eq!(
            database
                .scan(b"item-a".to_vec()..b"item-c".to_vec())
                .await
                .unwrap(),
            vec![(b"item-b".to_vec(), b"new".to_vec())]
        );
        assert_eq!(
            database
                .scan(b"item-b".to_vec()..=b"item-c".to_vec())
                .await
                .unwrap(),
            vec![
                (b"item-b".to_vec(), b"new".to_vec()),
                (b"item-c".to_vec(), b"steady".to_vec()),
            ]
        );
        assert!(
            database
                .scan(b"item-a".to_vec()..=b"item-a".to_vec())
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn reclaim_unprotected_tombstones_but_keep_overlapping_deeper_ranges() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    initialize_disk_fixture(root).await;
    let manifest = Arc::new(
        ManifestManager::load_or_create(root.join("MANIFEST"))
            .await
            .unwrap(),
    );
    let deep = persist_table(
        root,
        &manifest,
        2,
        vec![fixture_record(b"protected", b"old", RecordType::Put, 1)],
    )
    .await;
    let old = persist_table(
        root,
        &manifest,
        1,
        vec![fixture_record(b"free", b"old", RecordType::Put, 1)],
    )
    .await;
    let deletes = persist_table(
        root,
        &manifest,
        0,
        vec![
            fixture_record(b"free", b"", RecordType::Delete, 3),
            fixture_record(b"protected", b"", RecordType::Delete, 4),
        ],
    )
    .await;
    let anchor = persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"z", b"live", RecordType::Put, 5)],
    )
    .await;
    let merged = compaction(
        manifest.clone(),
        0,
        Arc::new(fixture_config()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    let records: Vec<_> = merged
        .blocks
        .iter()
        .flat_map(|block| block.data.iter().flatten())
        .collect();
    assert!(!records.iter().any(|record| record.key == b"free"));
    assert!(
        records
            .iter()
            .any(|record| record.key == b"protected" && record.record_type == RecordType::Delete)
    );
    for input in [old, deletes, anchor] {
        assert!(!Path::new(&input.path).exists());
    }
    assert!(Path::new(&deep.path).exists());
    drop(manifest);
    let mut database = KV2::open(root, fixture_config()).await.unwrap();
    assert_target_is_disk_only(&database).await;
    assert!(database.get(b"free").await.unwrap().is_none());
    assert!(database.get(b"protected").await.unwrap().is_none());
    assert_eq!(
        database
            .scan(b"free".to_vec()..=b"z".to_vec())
            .await
            .unwrap(),
        vec![(b"z".to_vec(), b"live".to_vec())]
    );
    database
        .put(b"free".to_vec(), b"new".to_vec())
        .await
        .unwrap();
    drop(database);
    let database = KV2::open(root, fixture_config()).await.unwrap();
    assert_eq!(
        database.get(b"free").await.unwrap().unwrap().as_ref(),
        b"new"
    );
}

#[tokio::test]
async fn all_deleted_compaction_removes_inputs_without_creating_an_output() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    initialize_disk_fixture(root).await;
    let manifest = Arc::new(
        ManifestManager::load_or_create(root.join("MANIFEST"))
            .await
            .unwrap(),
    );
    let old = persist_table(
        root,
        &manifest,
        1,
        vec![fixture_record(b"key", b"old", RecordType::Put, 1)],
    )
    .await;
    let delete = persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"key", b"", RecordType::Delete, 2)],
    )
    .await;
    let absent = persist_table(
        root,
        &manifest,
        0,
        vec![fixture_record(b"other", b"", RecordType::Delete, 3)],
    )
    .await;
    let merged = compaction(
        manifest.clone(),
        0,
        Arc::new(fixture_config()),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(merged.blocks.is_empty());
    assert!(manifest.snapshot().await.levels.is_empty());
    for input in [old, delete, absent] {
        assert!(!Path::new(&input.path).exists());
    }
    drop(manifest);
    let database = KV2::open(root, fixture_config()).await.unwrap();
    assert_target_is_disk_only(&database).await;
    assert!(database.get(b"key").await.unwrap().is_none());
    assert!(
        database
            .scan(b"key".to_vec()..=b"other".to_vec())
            .await
            .unwrap()
            .is_empty()
    );
}
