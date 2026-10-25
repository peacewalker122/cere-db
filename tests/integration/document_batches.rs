use ceredb::{
    Bm25Config, DocumentId, FtsIndex, FtsQuery, KV2, MAX_DOCUMENT_BATCH_DOCUMENTS,
    MAX_DOCUMENT_BATCH_INPUT_BYTES,
};
use std::path::Path;
use tempfile::TempDir;

async fn open() -> (FtsIndex<KV2>, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    (FtsIndex::open(engine).await.unwrap(), directory)
}

fn id(value: &str) -> DocumentId {
    DocumentId::new(value).unwrap()
}

async fn wal_bytes(directory: &Path) -> u64 {
    let mut entries = tokio::fs::read_dir(directory.join("wal")).await.unwrap();
    let mut total = 0;
    while let Some(entry) = entries.next_entry().await.unwrap() {
        if entry.file_name().to_string_lossy().ends_with(".log") {
            total += entry.metadata().await.unwrap().len();
        }
    }
    total
}

async fn open_for_batch_boundary() -> (FtsIndex<KV2>, TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open(
        directory.path(),
        ceredb::storage::config::StorageConfig {
            wal_segment_size: 16 * 1024 * 1024,
            memtable_size_threshold: usize::MAX as u64,
            max_level0_files: usize::MAX,
            ..ceredb::storage::config::StorageConfig::default()
        },
    )
    .await
    .unwrap();
    (FtsIndex::open(engine).await.unwrap(), directory)
}

#[tokio::test]
async fn batch_updates_search_statistics_and_duplicate_ids_last_write_wins() {
    let (mut index, directory) = open().await;
    index
        .put_documents(vec![
            (id("a"), b"apple apple shared".to_vec()),
            (id("b"), b"banana shared".to_vec()),
            (id("a"), b"pear shared shared".to_vec()),
        ])
        .await
        .unwrap();

    assert_eq!(
        index.get_document(&id("a")).await.unwrap(),
        Some(b"pear shared shared".to_vec())
    );
    assert_eq!(
        index
            .search(FtsQuery::And(vec!["pear".into(), "shared".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        index
            .search(FtsQuery::Or(vec!["apple".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    let ranked = index
        .search_ranked(
            FtsQuery::Or(vec!["shared".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    let average = 5.0 / 2.0;
    let idf = (2.0f64 + 1.0).ln() - (2.0f64 + 0.5).ln();
    let expected_a = idf * 2.0 * 2.2 / (2.0 + 1.2 * (0.25 + 0.75 * 3.0 / average));
    let expected_b = idf * 1.0 * 2.2 / (1.0 + 1.2 * (0.25 + 0.75 * 2.0 / average));
    assert!(
        (ranked
            .iter()
            .find(|hit| hit.id.as_str() == "a")
            .unwrap()
            .score
            - expected_a)
            .abs()
            < 1e-12
    );
    assert!(
        (ranked
            .iter()
            .find(|hit| hit.id.as_str() == "b")
            .unwrap()
            .score
            - expected_b)
            .abs()
            < 1e-12
    );

    index.delete_document(&id("a")).await.unwrap();
    assert!(
        index
            .search(FtsQuery::Or(vec!["pear".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );
    index
        .put_documents(vec![(id("a"), b"reinserted shared".to_vec())])
        .await
        .unwrap();
    drop(index);
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index
            .search(
                FtsQuery::And(vec!["reinserted".into(), "shared".into()]),
                10
            )
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn empty_invalid_and_oversized_batches_do_not_change_index() {
    let (mut index, _) = open().await;
    index.put_documents(Vec::new()).await.unwrap();
    index
        .put_document(id("existing"), b"stable".to_vec())
        .await
        .unwrap();

    assert!(
        index
            .put_documents(vec![
                (id("valid"), b"valid".to_vec()),
                (id("invalid"), vec![0xff])
            ])
            .await
            .is_err()
    );
    assert!(index.get_document(&id("valid")).await.unwrap().is_none());
    assert!(index.get_document(&id("existing")).await.unwrap().is_some());

    let too_many = (0..=MAX_DOCUMENT_BATCH_DOCUMENTS)
        .map(|n| (id(&format!("many-{n}")), b"value".to_vec()))
        .collect();
    assert!(index.put_documents(too_many).await.is_err());
    let too_large = vec![(id("large"), vec![b'x'; MAX_DOCUMENT_BATCH_INPUT_BYTES + 1])];
    assert!(index.put_documents(too_large).await.is_err());
    assert!(index.get_document(&id("large")).await.unwrap().is_none());
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["stable".into()]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn put_document_keeps_existing_large_document_behavior() {
    let directory = tempfile::tempdir().unwrap();
    let engine = KV2::open(
        directory.path(),
        ceredb::storage::config::StorageConfig {
            wal_segment_size: 2 * 1024 * 1024,
            memtable_size_threshold: usize::MAX as u64,
            max_level0_files: usize::MAX,
            ..ceredb::storage::config::StorageConfig::default()
        },
    )
    .await
    .unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    let document = vec![b'x'; 300 * 1024];
    index
        .put_document(id("large"), document.clone())
        .await
        .unwrap();
    assert_eq!(
        index.get_document(&id("large")).await.unwrap(),
        Some(document)
    );
}

#[tokio::test]
async fn failed_wal_publication_keeps_batch_state_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let storage = ceredb::storage::config::StorageConfig {
        wal_segment_size: 256 * 1024,
        memtable_size_threshold: usize::MAX as u64,
        max_level0_files: usize::MAX,
        ..ceredb::storage::config::StorageConfig::default()
    };
    let engine = KV2::open(directory.path(), storage).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    index
        .put_documents(vec![(id("stable"), b"oldword shared".to_vec())])
        .await
        .unwrap();
    let obstruction = directory.path().join("wal/00000000000000000001.log");
    tokio::fs::create_dir(&obstruction).await.unwrap();
    assert!(
        index
            .put_documents(vec![(id("stable"), vec![b'x'; 300 * 1024])])
            .await
            .is_err()
    );
    assert_eq!(
        index.get_document(&id("stable")).await.unwrap(),
        Some(b"oldword shared".to_vec())
    );
    assert_eq!(
        index
            .search(FtsQuery::Or(vec!["oldword".into()]), 10)
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
}

#[tokio::test]
async fn document_batch_wal_mutations_stay_bounded_and_atomic_at_boundary() {
    const WAL_LIMIT: u64 = 8 * 1024 * 1024;
    const ACCEPTED_BYTES: usize = 2_097_000;

    let (mut index, directory) = open_for_batch_boundary().await;
    index
        .put_documents(vec![(id("seed"), b"seedterm shared".to_vec())])
        .await
        .unwrap();
    let empty_wal = wal_bytes(directory.path()).await;

    let oversized = vec![b'x'; 2_097_090];
    assert!(
        index
            .put_documents(vec![(id("rejected"), oversized)])
            .await
            .is_err()
    );
    assert_eq!(wal_bytes(directory.path()).await, empty_wal);
    assert!(index.get_document(&id("rejected")).await.unwrap().is_none());
    assert!(
        index
            .search(FtsQuery::Or(vec!["x".into()]), 10)
            .await
            .unwrap()
            .is_empty()
    );

    let accepted = vec![b'x'; ACCEPTED_BYTES];
    index
        .put_documents(vec![(id("boundary"), accepted.clone())])
        .await
        .unwrap();
    let accepted_wal = wal_bytes(directory.path()).await;
    let growth = accepted_wal - empty_wal;
    assert!(growth <= WAL_LIMIT, "WAL grew by {growth} bytes");
    assert_eq!(
        index.get_document(&id("boundary")).await.unwrap(),
        Some(accepted.clone())
    );

    let corpus_before_rejection = index
        .search_ranked(
            FtsQuery::Or(vec!["seedterm".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    let over = vec![b'x'; ACCEPTED_BYTES + 24];
    assert!(index.put_documents(vec![(id("over"), over)]).await.is_err());
    assert_eq!(wal_bytes(directory.path()).await, accepted_wal);
    assert!(index.get_document(&id("over")).await.unwrap().is_none());
    assert_eq!(
        index
            .search_ranked(
                FtsQuery::Or(vec!["seedterm".into()]),
                10,
                Bm25Config::default(),
            )
            .await
            .unwrap(),
        corpus_before_rejection
    );

    drop(index);
    let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    assert_eq!(
        index.get_document(&id("boundary")).await.unwrap(),
        Some(accepted.clone())
    );
    assert!(index.get_document(&id("rejected")).await.unwrap().is_none());
    assert!(index.get_document(&id("over")).await.unwrap().is_none());
    let accepted_term = String::from_utf8(accepted.clone()).unwrap();
    let ranked = index
        .search_ranked(
            FtsQuery::Or(vec!["seedterm".into()]),
            10,
            Bm25Config::default(),
        )
        .await
        .unwrap();
    assert_eq!(ranked, corpus_before_rejection);
    assert_eq!(
        index
            .search(FtsQuery::Or(vec![accepted_term]), 10)
            .await
            .unwrap()
            .len(),
        1
    );
}
