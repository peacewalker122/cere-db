//! Integration tests for range scan across memtable and SSTable levels.
//!
//! Verifies that `scan()` correctly merges results from the active memtable
//! and persisted SSTables, filters tombstones, and returns newest versions.
//!
//! NOTE: These tests write data that stays primarily in the memtable
//! (to avoid requiring large data sets for flush triggers). The SSTable
//! scan path is tested through ReadManager unit tests.

use ceredb::api::api::AsyncKVEngine;
use ceredb::storage::config::StorageConfig;
use ceredb::KV2;

/// Helper: create a KV2 instance.
async fn create_kv2(label: &str) -> (KV2, tempfile::TempDir) {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempfile::Builder::new()
        .prefix(&format!("scan-integration-{label}"))
        .tempdir()
        .expect("Failed to create temp directory");

    // Use a large memtable threshold so all data stays in memtable
    let config = StorageConfig::builder()
        .memtable_size_threshold(64 * 1024 * 1024) // 64 MB — no flushes
        .build()
        .expect("valid config");

    let kv2 = KV2::open(temp_dir.path(), config)
        .await
        .expect("KV2 should open");
    (kv2, temp_dir)
}

#[tokio::test]
async fn scan_returns_all_keys_sorted() {
    let (mut kv2, _temp_dir) = create_kv2("sorted").await;

    kv2.put(b"delta".to_vec(), b"val_d".to_vec())
        .await
        .unwrap();
    kv2.put(b"alpha".to_vec(), b"val_a".to_vec())
        .await
        .unwrap();
    kv2.put(b"charlie".to_vec(), b"val_c".to_vec())
        .await
        .unwrap();
    kv2.put(b"beta".to_vec(), b"val_b".to_vec())
        .await
        .unwrap();

    let results = kv2.scan(..).await.unwrap();
    assert_eq!(results.len(), 4);

    // Keys must be sorted alphabetically
    assert_eq!(results[0].0, b"alpha");
    assert_eq!(results[1].0, b"beta");
    assert_eq!(results[2].0, b"charlie");
    assert_eq!(results[3].0, b"delta");

    // Values must be correct
    assert_eq!(results[0].1, b"val_a");
    assert_eq!(results[3].1, b"val_d");
}

#[tokio::test]
async fn scan_sub_range() {
    let (mut kv2, _temp_dir) = create_kv2("subrange").await;

    kv2.put(b"apple".to_vec(), b"v1".to_vec())
        .await
        .unwrap();
    kv2.put(b"banana".to_vec(), b"v2".to_vec())
        .await
        .unwrap();
    kv2.put(b"cherry".to_vec(), b"v3".to_vec())
        .await
        .unwrap();
    kv2.put(b"date".to_vec(), b"v4".to_vec())
        .await
        .unwrap();

    // Inclusive range
    let results = kv2.scan(b"banana".to_vec()..=b"cherry".to_vec()).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"banana");
    assert_eq!(results[1].0, b"cherry");

    // Range from start
    let results = kv2.scan(b"cherry".to_vec()..).await.unwrap();
    assert_eq!(results.len(), 2);

    // Range to end (exclusive)
    let results = kv2.scan(..b"cherry".to_vec()).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, b"apple");

    // Empty range
    let results = kv2.scan(b"x".to_vec()..=b"z".to_vec()).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn scan_filters_tombstones() {
    let (mut kv2, _temp_dir) = create_kv2("filter").await;

    kv2.put(b"keep".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    kv2.put(b"remove".to_vec(), b"temp".to_vec())
        .await
        .unwrap();
    kv2.delete(b"remove".to_vec()).await.unwrap();

    let results = kv2.scan(..).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, b"keep");
}

#[tokio::test]
async fn scan_overwritten_key_returns_latest() {
    let (mut kv2, _temp_dir) = create_kv2("overwrite").await;

    kv2.put(b"key".to_vec(), b"old".to_vec()).await.unwrap();
    kv2.put(b"key".to_vec(), b"new".to_vec()).await.unwrap();

    let results = kv2.scan(..).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1, b"new");
}
