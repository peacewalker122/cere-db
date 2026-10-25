use ceredb::{
    KV2,
    api::api::{AsyncKVEngine, BatchOperation},
    storage::{
        config::StorageConfig, manifest_codec::ManifestManager, writemanager::write::WriteComponent,
    },
};
use std::{path::Path, sync::Arc};

async fn wal_segments(directory: &Path) -> Vec<std::path::PathBuf> {
    let mut entries = tokio::fs::read_dir(directory).await.unwrap();
    let mut paths = Vec::new();
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_string_lossy().ends_with(".log") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    paths
}

#[tokio::test]
async fn large_batch_records_survive_checkpoint_and_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let operations = (0..850)
        .map(|index| BatchOperation::Put {
            key: format!("k{index:04}").into_bytes(),
            value: vec![(index % 251) as u8; 5000],
        })
        .collect();
    {
        let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
        database.write_batch(operations).await.unwrap();
        assert_eq!(
            wal_segments(&directory.path().join("wal")).await.len(),
            1,
            "covered WAL segments should be reclaimed after the large batch checkpoint"
        );
        database
            .put(b"after-bulk-flush".to_vec(), b"subsequent".to_vec())
            .await
            .unwrap();
    }

    let database = KV2::open(directory.path(), config).await.unwrap();
    for index in 0..850 {
        assert_eq!(
            database
                .get(format!("k{index:04}").as_bytes())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            &vec![(index % 251) as u8; 5000]
        );
    }
    assert_eq!(
        database
            .get(b"after-bulk-flush")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"subsequent"
    );
    assert_eq!(database.scan(..).await.unwrap().len(), 851);
}

#[tokio::test]
async fn first_checkpoint_in_new_nested_database_reclaims_only_after_durable_layout() {
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("new-parent").join("database");
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    {
        let mut database = KV2::open(&base, config.clone()).await.unwrap();
        database
            .put(b"checkpointed".to_vec(), b"durable".to_vec())
            .await
            .unwrap();
        assert_eq!(wal_segments(&base.join("wal")).await.len(), 1);
        for path in [
            base.clone(),
            base.join("wal"),
            base.join("sstable"),
            base.join("sstable/level-0"),
        ] {
            assert!(path.is_dir(), "missing directory {}", path.display());
        }
        assert!(base.join("MANIFEST").is_file());
    }

    let database = KV2::open(&base, config).await.unwrap();
    assert_eq!(
        database
            .get(b"checkpointed")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"durable"
    );
}

#[tokio::test]
async fn foreign_writer_mutation_withholds_future_reclamation_and_recovers() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
    database
        .put(b"owner-before".to_vec(), b"baseline".to_vec())
        .await
        .unwrap();
    let retained_before = wal_segments(&directory.path().join("wal")).await;
    assert!(retained_before.len() <= 2);

    let wal_manager = database.wal_manager();
    let foreign_manifest = Arc::new(
        ManifestManager::load_or_create(directory.path().join("foreign-MANIFEST"))
            .await
            .unwrap(),
    );
    tokio::fs::create_dir_all(directory.path().join("foreign-sstable/level-0"))
        .await
        .unwrap();
    let foreign = WriteComponent::new(
        directory.path().join("foreign-sstable"),
        wal_manager,
        foreign_manifest,
        0,
        Arc::new(config.clone()),
    );
    let foreign_clone = foreign.clone();
    foreign_clone
        .write_batch(vec![BatchOperation::Put {
            key: b"foreign-after-checkpoint".to_vec(),
            value: b"must-remain-recoverable".to_vec(),
        }])
        .await
        .unwrap();
    database
        .put(b"owner-after".to_vec(), b"also-retained".to_vec())
        .await
        .unwrap();
    assert!(wal_segments(&directory.path().join("wal")).await.len() >= 2);
    drop(foreign);
    drop(database);

    let database = KV2::open(directory.path(), config).await.unwrap();
    assert_eq!(
        database
            .get(b"foreign-after-checkpoint")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"must-remain-recoverable"
    );
    assert_eq!(
        database
            .get(b"owner-after")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"also-retained"
    );
    assert_eq!(
        database
            .get(b"owner-before")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"baseline"
    );
}

#[tokio::test]
async fn foreign_writer_before_first_checkpoint_is_replayed_without_gc() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
    let foreign_manifest = Arc::new(
        ManifestManager::load_or_create(directory.path().join("foreign-MANIFEST"))
            .await
            .unwrap(),
    );
    tokio::fs::create_dir_all(directory.path().join("foreign-sstable/level-0"))
        .await
        .unwrap();
    let foreign = WriteComponent::new(
        directory.path().join("foreign-sstable"),
        database.wal_manager(),
        foreign_manifest,
        0,
        Arc::new(config.clone()),
    );
    foreign
        .write_batch(vec![BatchOperation::Put {
            key: b"foreign-before-checkpoint".to_vec(),
            value: b"survives-first-floor".to_vec(),
        }])
        .await
        .unwrap();
    database
        .put(b"owner-first-checkpoint".to_vec(), b"owner".to_vec())
        .await
        .unwrap();
    assert!(wal_segments(&directory.path().join("wal")).await.len() >= 2);
    drop(foreign);
    drop(database);

    let database = KV2::open(directory.path(), config).await.unwrap();
    assert_eq!(
        database
            .get(b"foreign-before-checkpoint")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"survives-first-floor"
    );
    assert_eq!(
        database
            .get(b"owner-first-checkpoint")
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        b"owner"
    );
}

#[tokio::test]
async fn oversized_delete_key_survives_checkpoint_without_resurrection() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    {
        let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
        let key = vec![b'x'; 5000];
        database.put(key.clone(), b"old".to_vec()).await.unwrap();
        database.delete(key.clone()).await.unwrap();
        assert!(database.get(&key).await.unwrap().is_none());
    }
    let database = KV2::open(directory.path(), config).await.unwrap();
    assert!(database.get(&vec![b'x'; 5000]).await.unwrap().is_none());
}
