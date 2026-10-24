use std::{borrow::Cow, ops::RangeBounds};

use ceredb::{
    DocumentId, FtsIndex, FtsQuery, KV2,
    api::api::{AsyncKVEngine, BatchOperation},
    error::DBError,
    storage::{config::StorageConfig, manifest_codec::ManifestManager},
};
use tempfile::TempDir;
use tokio::sync::oneshot;

async fn open() -> (FtsIndex<KV2>, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    (FtsIndex::open(engine).await.unwrap(), directory)
}

fn id(value: &str) -> DocumentId {
    DocumentId::new(value).unwrap()
}

#[tokio::test]
async fn tokenizes_unicode_deduplicates_and_preserves_original_document() {
    let (mut index, _) = open().await;
    let document = "École, 世界! ÉCOLE x_y 123".as_bytes().to_vec();
    index
        .put_document(id("unicode"), document.clone())
        .await
        .unwrap();
    assert_eq!(
        index.get_document(&id("unicode")).await.unwrap(),
        Some(document.clone())
    );
    let hits = index
        .search(
            FtsQuery::And(vec!["éCOLE".into(), "世界".into(), "École".into()]),
            10,
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].document, document);
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["x_y".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(index.put_document(id("empty"), Vec::new()).await.is_ok());
    assert!(
        index
            .search(FtsQuery::And(vec!["!!!".into()]), 1)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn and_or_sort_limit_overwrite_delete_and_reopen() {
    let (mut index, directory) = open().await;
    index
        .put_document(id("z"), b"red blue".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("a"), b"red green".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("m"), b"blue green".to_vec())
        .await
        .unwrap();
    let and = index
        .search(FtsQuery::And(vec!["red".into(), "blue".into()]), 10)
        .await
        .unwrap();
    assert_eq!(
        and.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["z"]
    );
    let or = index
        .search(
            FtsQuery::Or(vec!["red".into(), "blue".into(), "red".into()]),
            2,
        )
        .await
        .unwrap();
    assert_eq!(
        or.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["a", "m"]
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["red".into()]), 0)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["red".into()]), 1001)
            .await
            .is_err()
    );
    assert!(index.put_document(id("bad"), vec![0xff]).await.is_err());
    assert!(index.get_document(&id("bad")).await.unwrap().is_none());
    index
        .put_document(id("z"), b"amber".to_vec())
        .await
        .unwrap();
    let blue = index
        .search(FtsQuery::And(vec!["blue".into()]), 10)
        .await
        .unwrap();
    assert_eq!(
        blue.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["m"]
    );
    drop(index);
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let mut reopened = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        reopened.get_document(&id("z")).await.unwrap(),
        Some(b"amber".to_vec())
    );
    assert!(reopened.delete_document(&id("a")).await.unwrap());
    assert!(!reopened.delete_document(&id("a")).await.unwrap());
    let green = reopened
        .search(FtsQuery::Or(vec!["green".into()]), 10)
        .await
        .unwrap();
    assert_eq!(
        green.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["m"]
    );
}

#[tokio::test]
async fn raw_operations_reserve_and_hide_namespace_and_validate_whole_batch() {
    let (mut index, _) = open().await;
    index
        .put(b"ordinary".to_vec(), b"value".to_vec())
        .await
        .unwrap();
    let reserved = b"\0ceredb:fts:v1:intrusion".to_vec();
    assert!(
        index
            .write_batch(vec![
                ceredb::BatchOperation::Put {
                    key: b"ordinary-other".to_vec(),
                    value: b"not applied".to_vec()
                },
                ceredb::BatchOperation::Put {
                    key: reserved.clone(),
                    value: Vec::new()
                },
            ])
            .await
            .is_err()
    );
    assert!(index.get(b"ordinary-other").await.unwrap().is_none());
    assert!(index.get(&reserved).await.is_err());
    assert_eq!(
        index.scan(..).await.unwrap(),
        vec![(b"ordinary".to_vec(), b"value".to_vec())]
    );
}

#[tokio::test]
async fn metadata_versions_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    engine
        .put(b"\0ceredb:fts:v1:m".to_vec(), vec![1, 99])
        .await
        .unwrap();
    drop(engine);
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert!(FtsIndex::open(engine).await.is_err());
}

#[tokio::test]
async fn namespace_without_metadata_is_not_overwritten() {
    let directory = tempfile::tempdir().unwrap();
    let mut engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    engine
        .put(b"\0ceredb:fts:v1:d\0\0\0\x01x".to_vec(), b"orphan".to_vec())
        .await
        .unwrap();
    drop(engine);

    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert!(FtsIndex::open(engine).await.is_err());
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    assert!(engine.get(b"\0ceredb:fts:v1:m").await.unwrap().is_none());
}

#[tokio::test]
async fn query_validation_precedes_zero_limit_shortcut() {
    let (mut index, _) = open().await;
    index
        .put_document(id("one"), b"one document".to_vec())
        .await
        .unwrap();

    assert!(index.search(FtsQuery::And(Vec::new()), 0).await.is_err());
    let too_many = (0..33).map(|n| format!("term{n}")).collect();
    assert!(index.search(FtsQuery::Or(too_many), 0).await.is_err());
    assert!(
        index
            .search(FtsQuery::Or(vec!["valid".into()]), 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn low_threshold_flush_update_and_delete_survive_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    index
        .put_document(id("flush"), b"initial searchable".to_vec())
        .await
        .unwrap();
    assert!(!ceredb::testing::list_sstable_files(directory.path()).is_empty());

    index
        .put_document(id("flush"), b"updated searchable".to_vec())
        .await
        .unwrap();
    assert!(
        index
            .search(FtsQuery::And(vec!["initial".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        index
            .search(FtsQuery::And(vec!["updated".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(index.delete_document(&id("flush")).await.unwrap());
    drop(index);

    let engine = KV2::open(directory.path(), config).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert!(index.get_document(&id("flush")).await.unwrap().is_none());
    for term in ["initial", "updated", "searchable"] {
        assert!(
            index
                .search(FtsQuery::Or(vec![term.into()]), 10)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn compacted_fts_records_do_not_resurface_and_remain_mutable() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 1,
        max_level0_files: 6,
        ..StorageConfig::default()
    };
    let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    index
        .put_document(id("target"), b"staleword shared".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("target"), b"freshword shared".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("deleted"), b"goneword shared".to_vec())
        .await
        .unwrap();
    index.delete_document(&id("deleted")).await.unwrap();
    index
        .put_document(id("anchor"), b"anchorword shared".to_vec())
        .await
        .unwrap();
    for _ in 0..8 {
        let hits = index
            .search(FtsQuery::And(vec!["shared".into()]), 10)
            .await
            .unwrap();
        assert_eq!(hits.len(), 2);
    }
    let mut engine = index.into_inner();
    engine.shutdown().await.unwrap();

    let manifest = ManifestManager::load_or_create(directory.path().join("MANIFEST"))
        .await
        .unwrap();
    let manifest_after = manifest.snapshot().await;
    assert!(
        ceredb::testing::list_sstable_files(directory.path())
            .iter()
            .any(|path| path.to_string_lossy().contains("level-1"))
    );
    assert!(!manifest_after.levels[&1].is_empty());

    let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    assert!(
        index
            .search(FtsQuery::Or(vec!["staleword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["goneword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        index
            .search(FtsQuery::And(vec!["freshword".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    index
        .put_document(id("target"), b"reinsertedword shared".to_vec())
        .await
        .unwrap();
    assert!(
        index
            .search(FtsQuery::Or(vec!["freshword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(index.delete_document(&id("anchor")).await.unwrap());
    assert!(
        index
            .search(FtsQuery::Or(vec!["anchorword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    drop(index);
    let engine = KV2::open(directory.path(), config).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["reinsertedword".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["staleword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(index.get_document(&id("anchor")).await.unwrap().is_none());
}

#[tokio::test]
async fn wal_rotation_failure_preserves_fts_state_and_can_retry_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let config = StorageConfig {
        wal_segment_size: 256 * 1024,
        memtable_size_threshold: usize::MAX as u64,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    };
    let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    index
        .put_document(id("stable"), b"oldquery".to_vec())
        .await
        .unwrap();
    let obstruction = directory.path().join("wal/00000000000000000001.log");
    tokio::fs::create_dir(&obstruction).await.unwrap();
    assert!(
        index
            .put_document(id("stable"), vec![b'x'; 300 * 1024])
            .await
            .is_err()
    );
    assert_eq!(
        index.get_document(&id("stable")).await.unwrap().unwrap(),
        b"oldquery"
    );
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["oldquery".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["x".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    drop(index);

    tokio::fs::remove_dir(&obstruction).await.unwrap();
    let engine = KV2::open(directory.path(), config.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index.get_document(&id("stable")).await.unwrap().unwrap(),
        b"oldquery"
    );
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["oldquery".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["x".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    index
        .put_document(id("stable"), b"retriedquery".to_vec())
        .await
        .unwrap();
    drop(index);
    let engine = KV2::open(directory.path(), config).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["retriedquery".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["oldquery".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

struct CommittedBatchGate {
    engine: KV2,
    committed: Option<oneshot::Sender<()>>,
}

impl AsyncKVEngine for CommittedBatchGate {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        self.engine.get(key).await
    }
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        self.engine.put(key, value).await
    }
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        self.engine.delete(key).await
    }
    async fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
        self.engine.write_batch(operations).await?;
        if let Some(committed) = self.committed.take() {
            let _ = committed.send(());
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    async fn await_pending_writes(&self) -> Result<(), DBError> {
        self.engine.await_pending_writes().await
    }
    async fn scan(
        &self,
        range: impl RangeBounds<Vec<u8>> + Send,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        self.engine.scan(range).await
    }
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_admitted_insert_is_settled_before_followup_reads_and_writes() {
    use std::future::poll_fn;
    use std::task::Poll;

    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    let mut canceled = Box::pin(index.put_document(id("a"), b"common".to_vec()));
    poll_fn(|cx| {
        assert!(matches!(canceled.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    drop(canceled);

    index
        .put_document(id("b"), b"common".to_vec())
        .await
        .unwrap();
    let hits = index
        .search_ranked(
            FtsQuery::Or(vec!["common".into()]),
            10,
            ceredb::Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.id.as_str()).collect::<Vec<_>>(),
        vec!["a", "b"]
    );
    let expected = 3.0_f64.ln() - 2.5_f64.ln();
    assert!(hits.iter().all(|hit| (hit.score - expected).abs() < 1e-12));
    assert_eq!(
        index.get_document(&id("a")).await.unwrap(),
        Some(b"common".to_vec())
    );

    let engine = index.into_inner();
    let index = FtsIndex::open(engine).await.unwrap();
    let reopened = index
        .search_ranked(
            FtsQuery::Or(vec!["common".into()]),
            10,
            ceredb::Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(reopened.len(), 2);
    assert!(
        reopened
            .iter()
            .all(|hit| (hit.score - expected).abs() < 1e-12)
    );

    let mut index = index;
    let deleted_id = id("a");
    let mut canceled_delete = Box::pin(index.delete_document(&deleted_id));
    poll_fn(|cx| {
        assert!(matches!(canceled_delete.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    drop(canceled_delete);
    index
        .put_document(id("c"), b"common".to_vec())
        .await
        .unwrap();
    let after_delete = index
        .search_ranked(
            FtsQuery::Or(vec!["common".into()]),
            10,
            ceredb::Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        after_delete
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "c"]
    );
    assert!(
        after_delete
            .iter()
            .all(|hit| (hit.score - expected).abs() < 1e-12)
    );

    let mut canceled_query_write = Box::pin(index.put_document(id("d"), b"common".to_vec()));
    poll_fn(|cx| {
        assert!(matches!(
            canceled_query_write.as_mut().poll(cx),
            Poll::Pending
        ));
        Poll::Ready(())
    })
    .await;
    drop(canceled_query_write);
    let after_query = index
        .search_ranked(
            FtsQuery::Or(vec!["common".into()]),
            10,
            ceredb::Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        after_query
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "c", "d"]
    );
    let expected_after_query = 4.0_f64.ln() - 3.5_f64.ln();
    assert!(
        after_query
            .iter()
            .all(|hit| (hit.score - expected_after_query).abs() < 1e-12)
    );
}

#[tokio::test]
async fn cancellation_after_real_batch_commit_leaves_coherent_state_for_followup_ops() {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    let engine = index.into_inner();
    let (committed, observed) = oneshot::channel();
    let gated = CommittedBatchGate {
        engine,
        committed: Some(committed),
    };
    let mut index = FtsIndex::open(gated).await.unwrap();
    let task = tokio::spawn(async move {
        index
            .put_document(id("cancelled"), b"coherentword originalword".to_vec())
            .await
    });
    observed.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());

    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index.get_document(&id("cancelled")).await.unwrap().unwrap(),
        b"coherentword originalword"
    );
    assert_eq!(
        index
            .search(
                FtsQuery::And(vec!["coherentword".into(), "originalword".into()]),
                10
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        index
            .search_ranked(
                FtsQuery::And(vec!["coherentword".into(), "originalword".into()]),
                10,
                ceredb::Bm25Config::default(),
            )
            .await
            .unwrap()
            .len(),
        1
    );
    index
        .put_document(id("cancelled"), b"nextword".to_vec())
        .await
        .unwrap();
    assert!(
        index
            .search(FtsQuery::Or(vec!["coherentword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(index.delete_document(&id("cancelled")).await.unwrap());
    drop(index);
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert!(
        index
            .get_document(&id("cancelled"))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["nextword".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        index
            .search_ranked(
                FtsQuery::Or(vec!["nextword".into()]),
                10,
                ceredb::Bm25Config::default(),
            )
            .await
            .unwrap()
            .is_empty()
    );
}
