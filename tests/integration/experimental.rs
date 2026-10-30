use std::{
    borrow::Cow,
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use ceredb::{
    Bm25Config, DocumentId, ExperimentalEngine, FtsIndex, FtsQuery, KV2, StorageConfig,
    api::api::{AsyncKVEngine, BatchOperation},
    error::DBError,
    storage::{kv2::RaftKV2, manifest_codec::ManifestManager, raft::RaftNodeConfig},
};
use tempfile::TempDir;

const PAYLOAD_MARKER: &[u8] = b"PAYLOAD-MARKER";

fn files_under(directory: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(current) = pending.pop() {
        for entry in fs::read_dir(&current).unwrap() {
            let entry = entry.unwrap();
            let file_type = entry.file_type().unwrap();
            if file_type.is_dir() {
                pending.push(entry.path());
            } else if file_type.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    files
}

fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    files_under(root)
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            (relative, fs::read(&path).unwrap())
        })
        .collect()
}

fn generation_directory(root: &Path) -> PathBuf {
    let mut entries = fs::read_dir(root.join("generations")).unwrap();
    let entry = entries.next().unwrap().unwrap();
    assert!(entries.next().is_none());
    entry.path()
}

fn wal_segments(wal_dir: &Path) -> Vec<PathBuf> {
    files_under(wal_dir)
        .into_iter()
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect()
}

fn level_files(sstable_dir: &Path, level: u32) -> usize {
    match fs::read_dir(sstable_dir.join(format!("level-{level}"))) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_file()))
            .count(),
        Err(_) => 0,
    }
}

async fn wait_for(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if condition() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for storage state"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn manifest_levels(index_dir: &Path) -> BTreeMap<u32, usize> {
    let manifest = ManifestManager::load_or_create(index_dir.join("MANIFEST"))
        .await
        .unwrap();
    manifest
        .snapshot()
        .await
        .levels
        .into_iter()
        .map(|(level, files)| (level, files.len()))
        .collect()
}

fn config(level_base_bytes: u64) -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 1024 * 1024,
        sstable_block_size: 4096,
        max_level0_files: 1,
        target_sstable_bytes: 1024 * 1024,
        level_base_bytes,
        level_size_multiplier: 10,
        wal_segment_size: 256 * 1024,
        bloom_false_positive_rate: 0.01,
        max_sstable_file_handles: 64,
    }
}

fn wide_key(prefix: &str, index: usize) -> Vec<u8> {
    let mut key = format!("{prefix}{index:06}").into_bytes();
    key.resize(800, b'k');
    key
}

fn wide_value(prefix: &str, index: usize) -> Vec<u8> {
    let mut value = format!("PAYLOAD-MARKER-{prefix}-{index:06}").into_bytes();
    value.resize(200, b'v');
    value
}

fn wide_batch(prefix: &str) -> Vec<BatchOperation> {
    (0..1200)
        .map(|index| BatchOperation::Put {
            key: wide_key(prefix, index),
            value: wide_value(prefix, index),
        })
        .collect()
}

#[tokio::test]
async fn foreign_format_guards_preserve_bytes() {
    let experimental_root = TempDir::new().unwrap();
    let mut experimental = ExperimentalEngine::open(experimental_root.path())
        .await
        .unwrap();
    experimental
        .put(b"key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    experimental.shutdown().await.unwrap();
    drop(experimental);

    let before = snapshot_tree(experimental_root.path());
    let index_dir = generation_directory(experimental_root.path()).join("index");
    assert!(
        KV2::open_with_defaults(experimental_root.path())
            .await
            .is_err()
    );
    assert!(KV2::open_with_defaults(&index_dir).await.is_err());
    let raft = RaftKV2::open(
        experimental_root.path(),
        StorageConfig::default(),
        RaftNodeConfig {
            node_id: 1,
            peers: Vec::new(),
            http_bind: "127.0.0.1:0".parse().unwrap(),
            raft_dir: experimental_root.path().join("raft"),
        },
    )
    .await;
    assert!(raft.is_err());
    assert_eq!(snapshot_tree(experimental_root.path()), before);

    let normal_root = TempDir::new().unwrap();
    let mut normal = KV2::open_with_defaults(normal_root.path()).await.unwrap();
    normal
        .put(b"key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    normal.shutdown().await.unwrap();
    drop(normal);

    let before = snapshot_tree(normal_root.path());
    assert!(ExperimentalEngine::open(normal_root.path()).await.is_err());
    assert_eq!(snapshot_tree(normal_root.path()), before);
}

#[tokio::test]
async fn exclusive_root_lock_rejects_second_engine() {
    let directory = TempDir::new().unwrap();
    let first = ExperimentalEngine::open(directory.path()).await.unwrap();
    let second = ExperimentalEngine::open(directory.path()).await;
    assert!(matches!(second, Err(DBError::StorageError(message)) if message.contains("locked")));
    drop(first);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert!(reopened.scan(..).await.unwrap().is_empty());
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn put_overwrite_delete_batch_scan_and_reopen() {
    let directory = TempDir::new().unwrap();
    let segments;
    {
        let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
        engine
            .write_batch(vec![
                BatchOperation::Put {
                    key: b"k1".to_vec(),
                    value: b"v1".to_vec(),
                },
                BatchOperation::Put {
                    key: b"k2".to_vec(),
                    value: b"v2".to_vec(),
                },
                BatchOperation::Put {
                    key: b"k1".to_vec(),
                    value: b"v1-new".to_vec(),
                },
                BatchOperation::Delete {
                    key: b"k2".to_vec(),
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            engine.get(b"k1").await.unwrap().unwrap().as_ref(),
            b"v1-new"
        );
        assert!(engine.get(b"k2").await.unwrap().is_none());

        engine.put(b"k3".to_vec(), b"v3".to_vec()).await.unwrap();
        engine
            .put(b"k3".to_vec(), b"v3-new".to_vec())
            .await
            .unwrap();
        engine.delete(b"k3".to_vec()).await.unwrap();
        engine
            .put(b"k3".to_vec(), b"v3-final".to_vec())
            .await
            .unwrap();

        let value = engine.get(b"k3").await.unwrap().unwrap();
        assert!(matches!(value, Cow::Owned(_)));
        assert_eq!(value.as_ref(), b"v3-final");
        assert_eq!(
            engine.scan(..).await.unwrap(),
            vec![
                (b"k1".to_vec(), b"v1-new".to_vec()),
                (b"k3".to_vec(), b"v3-final".to_vec())
            ]
        );

        engine.await_pending_writes().await.unwrap();
        segments = wal_segments(&generation_directory(directory.path()).join("wal")).len();
        assert!(segments > 0);
        engine.shutdown().await.unwrap();
    }

    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        engine.get(b"k1").await.unwrap().unwrap().as_ref(),
        b"v1-new"
    );
    assert_eq!(
        engine.get(b"k3").await.unwrap().unwrap().as_ref(),
        b"v3-final"
    );
    assert!(engine.get(b"k2").await.unwrap().is_none());
    assert_eq!(
        wal_segments(&generation_directory(directory.path()).join("wal")).len(),
        segments
    );
    engine
        .put(b"after-reopen".to_vec(), b"ok".to_vec())
        .await
        .unwrap();
    assert_eq!(
        engine.get(b"after-reopen").await.unwrap().unwrap().as_ref(),
        b"ok"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn flush_and_compaction_keep_payloads_out_of_index_sstables() {
    let directory = TempDir::new().unwrap();
    {
        let mut engine =
            ExperimentalEngine::open_with_config(directory.path(), config(64 * 1024 * 1024))
                .await
                .unwrap();
        engine.write_batch(wide_batch("a")).await.unwrap();
        let generation = generation_directory(directory.path());
        let sstable_dir = generation.join("index").join("sstable");
        wait_for(|| level_files(&sstable_dir, 1) > 0).await;
        assert_eq!(
            engine
                .get(&wide_key("a", 0))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &wide_value("a", 0)
        );

        let wal_dir = generation.join("wal");
        let segments = wal_segments(&wal_dir);
        assert!(segments.len() >= 2);
        assert!(
            segments.iter().any(|path| fs::read(path)
                .unwrap()
                .windows(PAYLOAD_MARKER.len())
                .any(|window| window == PAYLOAD_MARKER)),
            "the primary WAL must retain payload bytes"
        );
        let sstables = files_under(&sstable_dir);
        assert!(!sstables.is_empty());
        for path in sstables {
            let bytes = fs::read(&path).unwrap();
            assert!(
                !bytes
                    .windows(PAYLOAD_MARKER.len())
                    .any(|window| window == PAYLOAD_MARKER),
                "index SSTable {} must not contain payload bytes",
                path.display()
            );
        }
        engine.shutdown().await.unwrap();
    }
    let levels = manifest_levels(&generation_directory(directory.path()).join("index")).await;
    assert!(levels.get(&1).is_some_and(|files| *files > 0));

    {
        let mut engine =
            ExperimentalEngine::open_with_config(directory.path(), config(1024 * 1024))
                .await
                .unwrap();
        engine.write_batch(wide_batch("b")).await.unwrap();
        let generation = generation_directory(directory.path());
        let sstable_dir = generation.join("index").join("sstable");
        wait_for(|| level_files(&sstable_dir, 2) > 0 && level_files(&sstable_dir, 0) == 0).await;
        assert_eq!(
            engine
                .get(&wide_key("b", 17))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &wide_value("b", 17)
        );
        assert_eq!(
            engine
                .get(&wide_key("a", 1199))
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &wide_value("a", 1199)
        );
        assert_eq!(engine.scan(..).await.unwrap().len(), 2400);
        for path in files_under(&sstable_dir) {
            let bytes = fs::read(&path).unwrap();
            assert!(
                !bytes
                    .windows(PAYLOAD_MARKER.len())
                    .any(|window| window == PAYLOAD_MARKER),
                "compacted SSTable {} must not contain payload bytes",
                path.display()
            );
        }
        engine.shutdown().await.unwrap();
    }
    let index_dir = generation_directory(directory.path()).join("index");
    let levels = manifest_levels(&index_dir).await;
    assert!(levels.get(&2).is_some_and(|files| *files > 0));

    let mut engine = ExperimentalEngine::open_with_config(directory.path(), config(1024 * 1024))
        .await
        .unwrap();
    assert_eq!(
        engine
            .get(&wide_key("a", 7))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &wide_value("a", 7)
    );
    assert_eq!(
        engine
            .get(&wide_key("b", 7))
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        &wide_value("b", 7)
    );
    engine.shutdown().await.unwrap();

    let manifest = ManifestManager::load_or_create(index_dir.join("MANIFEST"))
        .await
        .unwrap();
    let sstable_path = manifest
        .snapshot()
        .await
        .levels
        .values()
        .flatten()
        .next()
        .unwrap()
        .path
        .clone();
    drop(manifest);
    fs::remove_file(&sstable_path).unwrap();
    assert!(
        ExperimentalEngine::open_with_config(directory.path(), config(1024 * 1024))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn fts_documents_and_bm25_survive_experimental_reopen() {
    let directory = TempDir::new().unwrap();
    let expected_score;
    {
        let engine = ExperimentalEngine::open(directory.path()).await.unwrap();
        let mut index = FtsIndex::open(engine).await.unwrap();
        index
            .put_documents(vec![
                (DocumentId::new("a").unwrap(), b"red apple harvest".to_vec()),
                (DocumentId::new("b").unwrap(), b"red berry harvest".to_vec()),
                (
                    DocumentId::new("c").unwrap(),
                    b"green pear harvest".to_vec(),
                ),
            ])
            .await
            .unwrap();
        let hits = index
            .search_ranked(
                FtsQuery::And(vec!["red".into(), "apple".into()]),
                10,
                Bm25Config::default(),
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id.as_str(), "a");
        assert!(hits[0].score.is_finite() && hits[0].score > 0.0);
        expected_score = hits[0].score;
        assert_eq!(
            index
                .search(
                    FtsQuery::Or(vec!["apple".into(), "berry".into(), "pear".into()]),
                    10,
                )
                .await
                .unwrap()
                .len(),
            3
        );
        index.into_inner().shutdown().await.unwrap();
    }

    let engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    let hits = index
        .search_ranked(
            FtsQuery::And(vec!["red".into(), "apple".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id.as_str(), "a");
    assert_eq!(hits[0].score, expected_score);
    assert_eq!(
        index
            .get_document(&DocumentId::new("b").unwrap())
            .await
            .unwrap(),
        Some(b"red berry harvest".to_vec())
    );
    index.into_inner().shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_admitted_write_keeps_barrier_and_writer_healthy() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    let payload = vec![7u8; 4096];
    let mut cancelled = Box::pin(engine.put(b"cancelled".to_vec(), payload.clone()));
    std::future::poll_fn(|context| {
        assert!(std::future::Future::poll(cancelled.as_mut(), context).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(cancelled);

    tokio::time::timeout(Duration::from_secs(30), engine.await_pending_writes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.get(b"cancelled").await.unwrap().unwrap().as_ref(),
        &payload
    );
    engine.put(b"after".to_vec(), b"ok".to_vec()).await.unwrap();
    assert_eq!(engine.get(b"after").await.unwrap().unwrap().as_ref(), b"ok");
    assert_eq!(engine.scan(..).await.unwrap().len(), 2);
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_current_tampered_wal_and_missing_wal_fail_closed() {
    let corrupt_current = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(corrupt_current.path())
            .await
            .unwrap();
        engine
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    let current_path = corrupt_current.path().join("CURRENT");
    let mut current = fs::read(&current_path).unwrap();
    current[0] ^= 0x01;
    fs::write(&current_path, current).unwrap();
    assert!(
        ExperimentalEngine::open(corrupt_current.path())
            .await
            .is_err()
    );

    let tampered_wal = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(tampered_wal.path()).await.unwrap();
    engine
        .put(b"tamper".to_vec(), b"tamper-target-value".to_vec())
        .await
        .unwrap();
    let wal_dir = generation_directory(tampered_wal.path()).join("wal");
    let segment = wal_segments(&wal_dir)
        .into_iter()
        .find(|path| {
            fs::read(path)
                .unwrap()
                .windows(b"tamper-target-value".len())
                .any(|window| window == b"tamper-target-value")
        })
        .unwrap();
    let mut bytes = fs::read(&segment).unwrap();
    let position = bytes
        .windows(b"tamper-target-value".len())
        .position(|window| window == b"tamper-target-value")
        .unwrap();
    bytes[position] ^= 0xff;
    fs::write(&segment, bytes).unwrap();
    assert!(matches!(
        engine.get(b"tamper").await,
        Err(DBError::Corrupted(_))
    ));
    engine.shutdown().await.unwrap();

    let missing_wal = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(missing_wal.path()).await.unwrap();
        engine
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    for segment in wal_segments(&generation_directory(missing_wal.path()).join("wal")) {
        fs::remove_file(segment).unwrap();
    }
    assert!(ExperimentalEngine::open(missing_wal.path()).await.is_err());
}

fn root_bytes(directory: &Path) -> u64 {
    files_under(directory)
        .into_iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum()
}

fn clean_value(round: u8, index: usize, size: usize) -> Vec<u8> {
    let mut value = format!("CLEAN-PAYLOAD-{round}-{index:06}").into_bytes();
    value.resize(size, b'a' + round);
    value
}

fn clean_config() -> StorageConfig {
    let mut settings = config(64 * 1024 * 1024);
    settings.memtable_size_threshold = 8 * 1024 * 1024;
    settings
}

#[tokio::test]
async fn clean_reclaims_overwritten_and_deleted_payloads() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open_with_config(directory.path(), clean_config())
        .await
        .unwrap();
    for round in 0..3u8 {
        let operations = (0..32)
            .map(|index| BatchOperation::Put {
                key: wide_key("clean", index),
                value: clean_value(round, index, 96 * 1024),
            })
            .collect();
        engine.write_batch(operations).await.unwrap();
    }
    for index in 0..8 {
        engine.delete(wide_key("clean", index)).await.unwrap();
    }
    engine.await_pending_writes().await.unwrap();

    let live = engine.scan(..).await.unwrap();
    assert_eq!(live.len(), 24);
    let bytes_before = root_bytes(directory.path());
    let stats = engine.clean().await.unwrap();

    assert_eq!(stats.old_generation, 1);
    assert_eq!(stats.new_generation, 2);
    assert_eq!(stats.live_records, live.len() as u64);
    assert_eq!(stats.index_sstables_written, 1);
    assert!(stats.copied_transactions > 0);
    assert!(stats.copied_payload_bytes > 0);
    assert!(stats.live_logical_bytes >= stats.copied_payload_bytes);
    assert_eq!(stats.total_root_bytes_before, bytes_before);
    assert!(stats.observed_peak_bytes >= bytes_before);
    assert!(
        stats.total_root_bytes_after < stats.total_root_bytes_before,
        "clean must reclaim space: before={} after={}",
        stats.total_root_bytes_before,
        stats.total_root_bytes_after
    );
    assert_eq!(stats.deferred_cleanup_bytes, 0);
    assert_eq!(stats.total_root_bytes_after, root_bytes(directory.path()));
    assert_eq!(
        generation_directory(directory.path()).file_name().unwrap(),
        "00000000000000000002"
    );

    assert_eq!(engine.scan(..).await.unwrap(), live);
    for (key, value) in &live {
        assert_eq!(
            engine.get(key).await.unwrap().unwrap().as_ref(),
            value.as_slice()
        );
    }
    engine
        .put(b"post-clean".to_vec(), b"ok".to_vec())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut reopened = ExperimentalEngine::open_with_config(directory.path(), clean_config())
        .await
        .unwrap();
    assert_eq!(reopened.scan(..).await.unwrap().len(), live.len() + 1);
    for (key, value) in &live {
        assert_eq!(
            reopened.get(key).await.unwrap().unwrap().as_ref(),
            value.as_slice()
        );
    }
    assert_eq!(
        reopened.get(b"post-clean").await.unwrap().unwrap().as_ref(),
        b"ok"
    );
    assert_eq!(
        generation_directory(directory.path()).file_name().unwrap(),
        "00000000000000000002"
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_preserves_documents_and_bm25_scores() {
    let directory = TempDir::new().unwrap();
    let engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    let (mut engine, expected_score) = {
        let mut index = FtsIndex::open(engine).await.unwrap();
        index
            .put_documents(vec![
                (DocumentId::new("a").unwrap(), b"red apple harvest".to_vec()),
                (DocumentId::new("b").unwrap(), b"red berry harvest".to_vec()),
                (
                    DocumentId::new("c").unwrap(),
                    b"green pear harvest".to_vec(),
                ),
            ])
            .await
            .unwrap();
        let hits = index
            .search_ranked(
                FtsQuery::And(vec!["red".into(), "apple".into()]),
                10,
                Bm25Config::default(),
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        (index.into_inner(), hits[0].score)
    };
    let before = engine.scan(..).await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert!(stats.live_records >= 3);
    assert_eq!(stats.deferred_cleanup_bytes, 0);

    let index = FtsIndex::open(engine).await.unwrap();
    let hits = index
        .search_ranked(
            FtsQuery::And(vec!["red".into(), "apple".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id.as_str(), "a");
    assert_eq!(hits[0].score, expected_score);
    assert_eq!(
        index
            .get_document(&DocumentId::new("b").unwrap())
            .await
            .unwrap(),
        Some(b"red berry harvest".to_vec())
    );
    let mut engine = index.into_inner();
    assert_eq!(engine.scan(..).await.unwrap(), before);
    engine
        .put(b"post-clean".to_vec(), b"ok".to_vec())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);

    let engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    let hits = index
        .search_ranked(
            FtsQuery::And(vec!["red".into(), "apple".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].score, expected_score);
    let mut engine = index.into_inner();
    assert_eq!(
        engine.get(b"post-clean").await.unwrap().unwrap().as_ref(),
        b"ok"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_preserves_sequence_floor_above_the_last_delete() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    engine
        .put(b"live".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    engine
        .put(b"doomed".to_vec(), b"gone".to_vec())
        .await
        .unwrap();
    engine.delete(b"doomed".to_vec()).await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert_eq!(stats.live_records, 1);
    assert_eq!(stats.new_generation, 2);
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"value"
    );
    assert!(reopened.get(b"doomed").await.unwrap().is_none());
    reopened
        .put(b"after-reopen".to_vec(), b"ok".to_vec())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get(b"after-reopen")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"ok"
    );
    let stats = reopened.clean().await.unwrap();
    assert_eq!(stats.old_generation, 2);
    assert_eq!(stats.new_generation, 3);
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn empty_store_keeps_sequence_floor_across_clean_and_reopen() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    engine.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
    engine.put(b"b".to_vec(), b"2".to_vec()).await.unwrap();
    engine.delete(b"a".to_vec()).await.unwrap();
    engine.delete(b"b".to_vec()).await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert_eq!(stats.live_records, 0);
    assert_eq!(stats.index_sstables_written, 0);
    assert_eq!(stats.new_generation, 2);
    engine.shutdown().await.unwrap();
    drop(engine);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert!(reopened.scan(..).await.unwrap().is_empty());
    reopened.put(b"c".to_vec(), b"3".to_vec()).await.unwrap();
    assert_eq!(reopened.get(b"c").await.unwrap().unwrap().as_ref(), b"3");
    reopened.delete(b"c".to_vec()).await.unwrap();
    let stats = reopened.clean().await.unwrap();
    assert_eq!(stats.live_records, 0);
    assert_eq!(stats.old_generation, 2);
    assert_eq!(stats.new_generation, 3);
    reopened.shutdown().await.unwrap();
    drop(reopened);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert!(reopened.scan(..).await.unwrap().is_empty());
    reopened.put(b"d".to_vec(), b"4".to_vec()).await.unwrap();
    assert_eq!(reopened.get(b"d").await.unwrap().unwrap().as_ref(), b"4");
    assert_eq!(
        generation_directory(directory.path()).file_name().unwrap(),
        "00000000000000000003"
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupted_current_blocks_generation_cleanup() {
    let directory = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
        engine
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    let orphan = directory
        .path()
        .join("generations")
        .join("00000000000000000009");
    fs::create_dir_all(&orphan).unwrap();
    let current_path = directory.path().join("CURRENT");
    let original_current = fs::read(&current_path).unwrap();

    let mut corrupt = original_current.clone();
    corrupt[0] ^= 0x01;
    fs::write(&current_path, corrupt).unwrap();
    let corrupted_tree = snapshot_tree(directory.path());
    assert!(ExperimentalEngine::open(directory.path()).await.is_err());
    assert_eq!(snapshot_tree(directory.path()), corrupted_tree);
    assert!(orphan.is_dir());

    fs::remove_file(&current_path).unwrap();
    let missing_tree = snapshot_tree(directory.path());
    assert!(ExperimentalEngine::open(directory.path()).await.is_err());
    assert_eq!(snapshot_tree(directory.path()), missing_tree);
    assert!(orphan.is_dir());

    fs::write(&current_path, original_current).unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert!(!orphan.exists());
    assert_eq!(
        engine.get(b"key").await.unwrap().unwrap().as_ref(),
        b"value"
    );
    engine.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_refuses_corrupt_source_payloads() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    let marker = b"UNIQUE-CLEAN-SOURCE-PAYLOAD".to_vec();
    engine
        .put(b"target".to_vec(), marker.clone())
        .await
        .unwrap();
    engine
        .put(b"other".to_vec(), b"other-value".to_vec())
        .await
        .unwrap();
    engine.await_pending_writes().await.unwrap();

    let generations = generation_directory(directory.path());
    let mut corrupted = false;
    for segment in wal_segments(&generations.join("wal")) {
        let mut bytes = fs::read(&segment).unwrap();
        if let Some(position) = bytes
            .windows(marker.len())
            .position(|window| window == marker)
        {
            bytes[position] ^= 0xff;
            fs::write(&segment, bytes).unwrap();
            corrupted = true;
            break;
        }
    }
    assert!(corrupted, "the source payload must be present in the WAL");
    let current_before = fs::read(directory.path().join("CURRENT")).unwrap();
    let generation_before = snapshot_tree(&generations);

    assert!(engine.clean().await.is_err());
    assert_eq!(
        fs::read(directory.path().join("CURRENT")).unwrap(),
        current_before
    );
    assert_eq!(snapshot_tree(&generations), generation_before);
    engine
        .put(b"after-failure".to_vec(), b"ok".to_vec())
        .await
        .unwrap();
    assert_eq!(
        engine
            .get(b"after-failure")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"ok"
    );
    engine.shutdown().await.unwrap();
}

const COMMIT_FRAME_BYTES: u64 = 37;

fn last_wal_segment(root: &Path) -> PathBuf {
    wal_segments(&generation_directory(root).join("wal"))
        .into_iter()
        .last()
        .expect("generation has a WAL segment")
}

fn wal_segment_containing(root: &Path, payload: &[u8]) -> PathBuf {
    wal_segments(&generation_directory(root).join("wal"))
        .into_iter()
        .find(|path| {
            fs::read(path)
                .unwrap()
                .windows(payload.len())
                .any(|window| window == payload)
        })
        .expect("payload must be present in the generation WAL")
}

fn truncate_wal_tail(segment: &Path, bytes: u64) {
    let length = fs::metadata(segment).unwrap().len();
    assert!(
        length > bytes,
        "WAL segment is shorter than the removed tail"
    );
    fs::OpenOptions::new()
        .write(true)
        .open(segment)
        .unwrap()
        .set_len(length - bytes)
        .unwrap();
}

async fn cleaned_engine(root: &Path) -> ExperimentalEngine {
    let mut engine = ExperimentalEngine::open(root).await.unwrap();
    engine
        .put(b"live".to_vec(), b"acknowledged".to_vec())
        .await
        .unwrap();
    engine
        .put(b"doomed".to_vec(), b"gone".to_vec())
        .await
        .unwrap();
    engine.delete(b"doomed".to_vec()).await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert_eq!(stats.live_records, 1);
    assert_eq!(stats.new_generation, 2);
    engine.shutdown().await.unwrap();
    drop(engine);
    ExperimentalEngine::open(root).await.unwrap()
}

#[tokio::test]
async fn clean_seal_accepts_strictly_appended_uncommitted_tail() {
    let directory = TempDir::new().unwrap();
    let mut engine = cleaned_engine(directory.path()).await;
    engine
        .put(b"tail".to_vec(), b"not-committed".to_vec())
        .await
        .unwrap();
    engine.await_pending_writes().await.unwrap();
    drop(engine);

    truncate_wal_tail(&last_wal_segment(directory.path()), COMMIT_FRAME_BYTES);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"acknowledged"
    );
    assert!(reopened.get(b"tail").await.unwrap().is_none());
    assert!(reopened.get(b"doomed").await.unwrap().is_none());
    assert_eq!(
        reopened.scan(..).await.unwrap(),
        vec![(b"live".to_vec(), b"acknowledged".to_vec())]
    );
    reopened
        .put(b"after".to_vec(), b"continued".to_vec())
        .await
        .unwrap();
    reopened.shutdown().await.unwrap();
    drop(reopened);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"acknowledged"
    );
    assert_eq!(
        reopened.get(b"after").await.unwrap().unwrap().as_ref(),
        b"continued"
    );
    assert!(reopened.get(b"tail").await.unwrap().is_none());
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_seal_accepts_partial_update_append() {
    let directory = TempDir::new().unwrap();
    let mut engine = cleaned_engine(directory.path()).await;
    engine
        .put(b"tail".to_vec(), b"not-committed".to_vec())
        .await
        .unwrap();
    engine.await_pending_writes().await.unwrap();
    drop(engine);

    truncate_wal_tail(&last_wal_segment(directory.path()), COMMIT_FRAME_BYTES + 10);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"acknowledged"
    );
    assert!(reopened.get(b"tail").await.unwrap().is_none());
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_seal_accepts_uncommitted_tail_in_rotated_segment() {
    let directory = TempDir::new().unwrap();
    let settings = StorageConfig {
        wal_segment_size: 256 * 1024,
        ..StorageConfig::default()
    };
    let mut engine = ExperimentalEngine::open_with_config(directory.path(), settings.clone())
        .await
        .unwrap();
    engine
        .put(b"live".to_vec(), b"acknowledged".to_vec())
        .await
        .unwrap();
    engine.delete(b"leaf".to_vec()).await.unwrap();
    engine.await_pending_writes().await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert_eq!(stats.new_generation, 2);
    engine
        .put(b"filler".to_vec(), b"committed-after-clean".to_vec())
        .await
        .unwrap();
    engine.await_pending_writes().await.unwrap();
    let mut payload = b"ROTATED-UNCOMMITTED".to_vec();
    payload.resize(300 * 1024, b'r');
    engine.put(b"rotated".to_vec(), payload).await.unwrap();
    engine.await_pending_writes().await.unwrap();
    drop(engine);

    let wal_dir = generation_directory(directory.path()).join("wal");
    assert_eq!(wal_segments(&wal_dir).len(), 3);
    truncate_wal_tail(&last_wal_segment(directory.path()), COMMIT_FRAME_BYTES);

    let mut reopened = ExperimentalEngine::open_with_config(directory.path(), settings)
        .await
        .unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"acknowledged"
    );
    assert_eq!(
        reopened.get(b"filler").await.unwrap().unwrap().as_ref(),
        b"committed-after-clean"
    );
    assert!(reopened.get(b"rotated").await.unwrap().is_none());
    reopened
        .put(b"after".to_vec(), b"continued".to_vec())
        .await
        .unwrap();
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_seal_accepts_uncommitted_tail_in_empty_generation() {
    let directory = TempDir::new().unwrap();
    let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
    engine.put(b"a".to_vec(), b"1".to_vec()).await.unwrap();
    engine.put(b"b".to_vec(), b"2".to_vec()).await.unwrap();
    engine.delete(b"a".to_vec()).await.unwrap();
    engine.delete(b"b".to_vec()).await.unwrap();
    let stats = engine.clean().await.unwrap();
    assert_eq!(stats.live_records, 0);
    assert_eq!(stats.new_generation, 2);
    engine
        .put(b"tail".to_vec(), b"not-committed".to_vec())
        .await
        .unwrap();
    engine.await_pending_writes().await.unwrap();
    drop(engine);

    truncate_wal_tail(&last_wal_segment(directory.path()), COMMIT_FRAME_BYTES);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert!(reopened.scan(..).await.unwrap().is_empty());
    assert!(reopened.get(b"tail").await.unwrap().is_none());
    reopened.put(b"c".to_vec(), b"3".to_vec()).await.unwrap();
    assert_eq!(reopened.get(b"c").await.unwrap().unwrap().as_ref(), b"3");
    reopened.shutdown().await.unwrap();
    drop(reopened);

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.scan(..).await.unwrap(),
        vec![(b"c".to_vec(), b"3".to_vec())]
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn clean_seal_rejects_truncated_and_missing_sealed_segments() {
    let truncated = TempDir::new().unwrap();
    {
        let engine = cleaned_engine(truncated.path()).await;
        drop(engine);
    }
    truncate_wal_tail(
        &wal_segment_containing(truncated.path(), b"acknowledged"),
        10,
    );
    assert!(matches!(
        ExperimentalEngine::open(truncated.path()).await,
        Err(DBError::Corrupted(_))
    ));

    let corrupt = TempDir::new().unwrap();
    {
        let engine = cleaned_engine(corrupt.path()).await;
        drop(engine);
    }
    let segment = wal_segment_containing(corrupt.path(), b"acknowledged");
    let mut bytes = fs::read(&segment).unwrap();
    let position = bytes
        .windows(b"acknowledged".len())
        .position(|window| window == b"acknowledged")
        .unwrap();
    bytes[position] ^= 0xff;
    fs::write(&segment, bytes).unwrap();
    assert!(matches!(
        ExperimentalEngine::open(corrupt.path()).await,
        Err(DBError::Corrupted(_))
    ));

    let missing = TempDir::new().unwrap();
    {
        let engine = cleaned_engine(missing.path()).await;
        drop(engine);
    }
    fs::remove_file(wal_segment_containing(missing.path(), b"acknowledged")).unwrap();
    assert!(matches!(
        ExperimentalEngine::open(missing.path()).await,
        Err(DBError::Corrupted(_))
    ));
}

#[tokio::test]
async fn clean_seal_rejects_acknowledged_tail_removed_after_published_floor() {
    let directory = TempDir::new().unwrap();
    let mut engine = cleaned_engine(directory.path()).await;
    engine
        .put(b"acknowledged-after-clean".to_vec(), b"durable".to_vec())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    drop(engine);

    truncate_wal_tail(&last_wal_segment(directory.path()), COMMIT_FRAME_BYTES);

    assert!(matches!(
        ExperimentalEngine::open(directory.path()).await,
        Err(DBError::Corrupted(_))
    ));
}

async fn assert_normal_engines_reject(root: &Path) {
    let before = snapshot_tree(root);
    assert!(KV2::open_with_defaults(root).await.is_err());
    assert!(raft_engine_open(root, root.join("raft")).await.is_err());
    assert_eq!(snapshot_tree(root), before);
}

async fn raft_engine_open(root: &Path, raft_dir: PathBuf) -> Result<RaftKV2, DBError> {
    RaftKV2::open(
        root,
        StorageConfig::default(),
        RaftNodeConfig {
            node_id: 1,
            peers: Vec::new(),
            http_bind: "127.0.0.1:0".parse().unwrap(),
            raft_dir,
        },
    )
    .await
}

#[tokio::test]
async fn current_without_root_marker_rejects_normal_engines() {
    let directory = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(directory.path()).await.unwrap();
        engine
            .put(b"live".to_vec(), b"acknowledged".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    fs::remove_file(directory.path().join("EXPERIMENTAL-WAL-VALUES")).unwrap();

    assert_normal_engines_reject(directory.path()).await;

    let mut reopened = ExperimentalEngine::open(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"live").await.unwrap().unwrap().as_ref(),
        b"acknowledged"
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_residue_rejects_normal_engines() {
    let current_temp = TempDir::new().unwrap();
    fs::write(current_temp.path().join("CURRENT.tmp"), b"partial").unwrap();
    assert_normal_engines_reject(current_temp.path()).await;

    let marker_temp = TempDir::new().unwrap();
    fs::write(
        marker_temp.path().join("EXPERIMENTAL-WAL-VALUES.tmp"),
        b"partial",
    )
    .unwrap();
    assert_normal_engines_reject(marker_temp.path()).await;

    let reservation = TempDir::new().unwrap();
    fs::write(reservation.path().join("NEXT-GENERATION"), b"partial").unwrap();
    assert_normal_engines_reject(reservation.path()).await;

    let lock = TempDir::new().unwrap();
    fs::write(lock.path().join("LOCK"), b"").unwrap();
    assert_normal_engines_reject(lock.path()).await;

    let generation_only = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(generation_only.path())
            .await
            .unwrap();
        engine
            .put(b"live".to_vec(), b"acknowledged".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    for artifact in [
        "EXPERIMENTAL-WAL-VALUES",
        "CURRENT",
        "NEXT-GENERATION",
        "LOCK",
    ] {
        let path = generation_only.path().join(artifact);
        if path.exists() {
            fs::remove_file(path).unwrap();
        }
    }
    assert_normal_engines_reject(generation_only.path()).await;
    match ExperimentalEngine::open(generation_only.path()).await {
        Ok(mut engine) => {
            assert_eq!(
                engine.get(b"live").await.unwrap().unwrap().as_ref(),
                b"acknowledged"
            );
            engine.shutdown().await.unwrap();
        }
        Err(_) => {}
    }
}

#[tokio::test]
async fn raft_consensus_layer_rejects_experimental_raft_dir() {
    let experimental = TempDir::new().unwrap();
    {
        let mut engine = ExperimentalEngine::open(experimental.path()).await.unwrap();
        engine
            .put(b"key".to_vec(), b"value".to_vec())
            .await
            .unwrap();
        engine.shutdown().await.unwrap();
    }
    let before = snapshot_tree(experimental.path());
    let base = TempDir::new().unwrap();
    let opened = raft_engine_open(base.path(), experimental.path().to_path_buf()).await;
    assert!(opened.is_err());
    assert_eq!(snapshot_tree(experimental.path()), before);
}

#[tokio::test]
async fn normal_root_with_generic_files_still_opens() {
    let directory = TempDir::new().unwrap();
    fs::write(directory.path().join("notes.txt"), b"user data").unwrap();
    let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    engine
        .put(b"key".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    engine.shutdown().await.unwrap();
    assert_eq!(
        fs::read(directory.path().join("notes.txt")).unwrap(),
        b"user data"
    );
}
