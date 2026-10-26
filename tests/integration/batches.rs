use ceredb::{BatchOperation, KV2, api::api::AsyncKVEngine, storage::config::StorageConfig};
use tempfile::TempDir;

async fn open() -> (KV2, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    (engine, directory)
}

#[tokio::test]
async fn batch_applies_mixed_operations_and_repeated_keys_in_order() {
    let (mut engine, _directory) = open().await;
    engine
        .write_batch(vec![
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"first".to_vec(),
            },
            BatchOperation::Put {
                key: b"b".to_vec(),
                value: b"present".to_vec(),
            },
            BatchOperation::Delete { key: b"a".to_vec() },
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"last".to_vec(),
            },
            BatchOperation::Delete { key: b"b".to_vec() },
        ])
        .await
        .unwrap();
    assert_eq!(engine.get(b"a").await.unwrap().unwrap().as_ref(), b"last");
    assert!(engine.get(b"b").await.unwrap().is_none());
    assert_eq!(
        engine.scan(..).await.unwrap(),
        vec![(b"a".to_vec(), b"last".to_vec())]
    );
}

#[tokio::test]
async fn empty_batch_is_a_noop_and_batch_recovers_before_and_after_flush() {
    let (mut engine, directory) = open().await;
    engine.write_batch(Vec::new()).await.unwrap();
    engine
        .write_batch(vec![
            BatchOperation::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            BatchOperation::Delete {
                key: b"gone".to_vec(),
            },
            BatchOperation::Put {
                key: b"gone".to_vec(),
                value: b"back".to_vec(),
            },
        ])
        .await
        .unwrap();
    drop(engine);
    let mut reopened = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(reopened.get(b"k").await.unwrap().unwrap().as_ref(), b"v1");
    assert_eq!(
        reopened.get(b"gone").await.unwrap().unwrap().as_ref(),
        b"back"
    );
    reopened
        .write_batch(vec![BatchOperation::Delete { key: b"k".to_vec() }])
        .await
        .unwrap();
    drop(reopened);
    let reopened = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert!(reopened.get(b"k").await.unwrap().is_none());
}

#[tokio::test]
async fn batch_crossing_flush_threshold_is_fully_recovered() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        ..StorageConfig::default()
    };
    let mut engine = KV2::open(directory.path(), config).await.unwrap();

    engine
        .write_batch(vec![
            BatchOperation::Put {
                key: b"kept".to_vec(),
                value: b"value".to_vec(),
            },
            BatchOperation::Delete {
                key: b"removed".to_vec(),
            },
            BatchOperation::Put {
                key: b"removed".to_vec(),
                value: b"reinserted".to_vec(),
            },
        ])
        .await
        .unwrap();

    assert!(
        !std::fs::read_dir(directory.path().join("sstable/level-0"))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .is_empty()
    );
    drop(engine);

    let reopened = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"kept").await.unwrap().unwrap().as_ref(),
        b"value"
    );
    assert_eq!(
        reopened.get(b"removed").await.unwrap().unwrap().as_ref(),
        b"reinserted"
    );
}

#[tokio::test]
async fn oversized_later_operation_does_not_publish_earlier_operations() {
    let (mut engine, _directory) = open().await;
    let oversized_value = vec![0; 64 * 1024 * 1024 + 1];

    assert!(
        engine
            .write_batch(vec![
                BatchOperation::Put {
                    key: b"must-not-appear".to_vec(),
                    value: b"first".to_vec(),
                },
                BatchOperation::Put {
                    key: b"oversized".to_vec(),
                    value: oversized_value,
                },
            ])
            .await
            .is_err()
    );

    assert!(engine.get(b"must-not-appear").await.unwrap().is_none());
    assert!(engine.scan(..).await.unwrap().is_empty());
}

#[tokio::test]
async fn failed_rotation_does_not_publish_batch_or_damage_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        wal_segment_size: 1,
        ..StorageConfig::default()
    };
    let mut engine = KV2::open(directory.path(), config).await.unwrap();
    engine
        .put(b"prior".to_vec(), b"durable".to_vec())
        .await
        .unwrap();
    let obstruction = directory.path().join("wal/00000000000000000001.log");
    std::fs::create_dir(&obstruction).unwrap();
    assert!(
        engine
            .write_batch(vec![BatchOperation::Put {
                key: b"not-visible".to_vec(),
                value: b"value".to_vec(),
            }])
            .await
            .is_err()
    );
    assert!(engine.get(b"not-visible").await.unwrap().is_none());
    std::fs::remove_dir(obstruction).unwrap();
    drop(engine);
    let reopened = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(
        reopened.get(b"prior").await.unwrap().unwrap().as_ref(),
        b"durable"
    );
    assert!(reopened.get(b"not-visible").await.unwrap().is_none());
}

#[tokio::test]
async fn committed_batch_survives_flush_failure_and_flush_retries_later() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let mut engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let level0 = directory.path().join("sstable/level-0");
    std::fs::remove_dir(&level0).unwrap();
    std::fs::write(&level0, b"obstruction").unwrap();

    engine
        .write_batch(vec![BatchOperation::Put {
            key: b"committed".to_vec(),
            value: b"durable".to_vec(),
        }])
        .await
        .unwrap();
    assert_eq!(
        engine.get(b"committed").await.unwrap().unwrap().as_ref(),
        b"durable"
    );
    std::fs::remove_file(&level0).unwrap();
    drop(engine);

    let mut reopened = KV2::open(directory.path(), config).await.unwrap();
    assert_eq!(
        reopened.get(b"committed").await.unwrap().unwrap().as_ref(),
        b"durable"
    );
    reopened
        .write_batch(vec![BatchOperation::Put {
            key: b"retry-trigger".to_vec(),
            value: b"also-durable".to_vec(),
        }])
        .await
        .unwrap();
    assert!(!std::fs::read_dir(&level0).unwrap().next().is_none());
    drop(reopened);

    let recovered = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(
        recovered.get(b"committed").await.unwrap().unwrap().as_ref(),
        b"durable"
    );
    assert_eq!(
        recovered
            .get(b"retry-trigger")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"also-durable"
    );
}
