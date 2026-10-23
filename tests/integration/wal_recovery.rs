use ceredb::{KV2, api::api::AsyncKVEngine, storage::config::StorageConfig};

#[tokio::test]
async fn flush_and_rotation_preserve_updates_and_tombstones_after_repeated_restarts() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 32,
        wal_segment_size: 180,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    {
        let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
        database.put(b"keep".to_vec(), vec![1; 40]).await.unwrap();
        database.put(b"remove".to_vec(), vec![2; 40]).await.unwrap();
        database.delete(b"remove".to_vec()).await.unwrap();
        database.put(b"keep".to_vec(), vec![3; 40]).await.unwrap();
    }
    for value in 4..=6 {
        let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
        assert_eq!(
            database.get(b"keep").await.unwrap().unwrap().as_ref(),
            &vec![value - 1; 40]
        );
        assert!(database.get(b"remove").await.unwrap().is_none());
        database
            .put(b"keep".to_vec(), vec![value; 40])
            .await
            .unwrap();
    }
    let database = KV2::open(directory.path(), config).await.unwrap();
    assert_eq!(
        database.get(b"keep").await.unwrap().unwrap().as_ref(),
        &vec![6; 40]
    );
    assert!(database.get(b"remove").await.unwrap().is_none());
    assert!(
        directory
            .path()
            .join("wal/00000000000000000000.log")
            .exists()
    );
}

#[tokio::test]
async fn engine_reopens_after_torn_final_commit_and_can_write_again() {
    let directory = tempfile::tempdir().unwrap();
    {
        let mut database = KV2::open_with_defaults(directory.path()).await.unwrap();
        database
            .put(b"key".to_vec(), b"committed".to_vec())
            .await
            .unwrap();
        database
            .put(b"key".to_vec(), b"uncommitted".to_vec())
            .await
            .unwrap();
    }
    let path = directory.path().join("wal/00000000000000000000.log");
    let length = tokio::fs::metadata(&path).await.unwrap().len();
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .unwrap();
    file.set_len(length - 1).await.unwrap();
    file.sync_all().await.unwrap();
    {
        let mut database = KV2::open_with_defaults(directory.path()).await.unwrap();
        assert_eq!(
            database.get(b"key").await.unwrap().unwrap().as_ref(),
            b"committed"
        );
        database
            .put(b"key".to_vec(), b"after-restart".to_vec())
            .await
            .unwrap();
    }
    let database = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert_eq!(
        database.get(b"key").await.unwrap().unwrap().as_ref(),
        b"after-restart"
    );
}

#[tokio::test]
async fn failed_flush_keeps_values_and_tombstones_visible_through_retry_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 32,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
    database
        .put(b"deleted".to_vec(), vec![7; 40])
        .await
        .unwrap();
    let level0 = directory.path().join("sstable/level-0");
    let parked = directory.path().join("parked-level-0");
    tokio::fs::rename(&level0, &parked).await.unwrap();
    tokio::fs::write(&level0, b"obstruction").await.unwrap();
    database
        .put(b"old".to_vec(), b"yes".to_vec())
        .await
        .unwrap();
    assert!(
        database
            .put(b"trigger".to_vec(), vec![1; 40])
            .await
            .is_err()
    );
    assert!(database.delete(b"deleted".to_vec()).await.is_err());
    assert_eq!(
        database.get(b"old").await.unwrap().unwrap().as_ref(),
        b"yes"
    );
    assert!(database.get(b"deleted").await.unwrap().is_none());
    tokio::fs::remove_file(&level0).await.unwrap();
    tokio::fs::rename(&parked, &level0).await.unwrap();
    database
        .put(b"new".to_vec(), b"yes".to_vec())
        .await
        .unwrap();
    for key in [b"old", b"new"] {
        assert_eq!(database.get(key).await.unwrap().unwrap().as_ref(), b"yes");
    }
    assert!(database.get(b"deleted").await.unwrap().is_none());
    database.put(b"next".to_vec(), vec![2; 40]).await.unwrap();
    drop(database);
    let database = KV2::open(directory.path(), config).await.unwrap();
    for key in [b"old", b"new"] {
        assert_eq!(database.get(key).await.unwrap().unwrap().as_ref(), b"yes");
    }
    assert!(database.get(b"deleted").await.unwrap().is_none());
    assert_eq!(
        database.get(b"trigger").await.unwrap().unwrap().as_ref(),
        &vec![1; 40]
    );
    assert_eq!(database.scan(..).await.unwrap().len(), 4);
}

#[tokio::test]
async fn orphan_files_and_directories_are_skipped_after_repeated_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 32,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    drop(KV2::open(directory.path(), config.clone()).await.unwrap());
    let level0 = directory.path().join("sstable/level-0");
    let mut registered_files = Vec::new();
    let mut orphan_files = Vec::new();
    for round in 0..3 {
        let file_id = round * 3 + 1;
        let orphan = level0.join(format!("sstable-{file_id}.dat"));
        tokio::fs::write(&orphan, b"unregistered bytes")
            .await
            .unwrap();
        orphan_files.push(orphan);
        let orphan_directory = level0.join(format!("sstable-{}.dat", file_id + 1));
        tokio::fs::create_dir(&orphan_directory).await.unwrap();
        tokio::fs::write(orphan_directory.join("sentinel"), b"preserved")
            .await
            .unwrap();
        for _ in 0..2 {
            drop(KV2::open(directory.path(), config.clone()).await.unwrap());
        }
        let mut database = KV2::open(directory.path(), config.clone()).await.unwrap();
        database
            .put(format!("key-{round}").into_bytes(), vec![round as u8; 40])
            .await
            .unwrap();
        for previous in 0..=round {
            assert_eq!(
                database
                    .get(format!("key-{previous}").as_bytes())
                    .await
                    .unwrap()
                    .unwrap()
                    .as_ref(),
                &vec![previous as u8; 40]
            );
        }
        let registered = level0.join(format!("sstable-{}.dat", file_id + 2));
        registered_files.push((
            registered.clone(),
            tokio::fs::read(registered).await.unwrap(),
        ));
        for (path, bytes) in &registered_files {
            assert_eq!(&tokio::fs::read(path).await.unwrap(), bytes);
        }
        for path in &orphan_files {
            assert_eq!(tokio::fs::read(path).await.unwrap(), b"unregistered bytes");
        }
        assert_eq!(
            tokio::fs::read(orphan_directory.join("sentinel"))
                .await
                .unwrap(),
            b"preserved"
        );
    }
}
