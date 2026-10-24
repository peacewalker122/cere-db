use ceredb::api::api::AsyncKVEngine;
use ceredb::{Bm25Config, DocumentId, FtsIndex, FtsQuery, KV2};
use tempfile::TempDir;

async fn index() -> (FtsIndex<KV2>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let engine = KV2::open_with_defaults(dir.path()).await.unwrap();
    (FtsIndex::open(engine).await.unwrap(), dir)
}

fn id(value: &str) -> DocumentId {
    DocumentId::new(value).unwrap()
}

fn expected_score(n: f64, df: f64, tf: f64, dl: f64, average: f64) -> f64 {
    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
    idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * dl / average))
}

#[tokio::test]
async fn computes_bm25_and_sorts_before_limit_with_deterministic_ties() {
    let (mut index, _) = index().await;
    for (document_id, text) in [
        ("z", "rare rare common"),
        ("b", "rare common"),
        ("a", "rare common"),
        ("long", "rare common extra words"),
        ("common-only", "common"),
    ] {
        index
            .put_document(id(document_id), text.as_bytes().to_vec())
            .await
            .unwrap();
    }
    let config = Bm25Config::default();
    let ranked = index
        .search_ranked(FtsQuery::Or(vec!["rare".into(), "rare".into()]), 10, config)
        .await
        .unwrap();
    let expected = expected_score(5.0, 4.0, 2.0, 3.0, 2.4);
    assert!(
        (ranked
            .iter()
            .find(|hit| hit.id.as_str() == "z")
            .unwrap()
            .score
            - expected)
            .abs()
            < 1e-12
    );
    let expected_b = expected_score(5.0, 4.0, 1.0, 2.0, 2.4);
    assert!(
        (ranked
            .iter()
            .find(|hit| hit.id.as_str() == "a")
            .unwrap()
            .score
            - expected_b)
            .abs()
            < 1e-12
    );
    assert_eq!(ranked[0].id.as_str(), "z");
    assert_eq!(ranked[1].id.as_str(), "a");
    assert_eq!(ranked[2].id.as_str(), "b");
    assert_eq!(ranked[3].id.as_str(), "long");
    let common_b = index
        .search_ranked(FtsQuery::And(vec!["common".into()]), 10, config)
        .await
        .unwrap()
        .into_iter()
        .find(|hit| hit.id.as_str() == "b")
        .unwrap();
    assert!(ranked[2].score > common_b.score);
    let limited = index
        .search_ranked(FtsQuery::Or(vec!["rare".into()]), 1, config)
        .await
        .unwrap();
    assert_eq!(limited[0].id.as_str(), "z");
}

#[tokio::test]
async fn bm25_remains_finite_for_extreme_finite_k1_values() {
    let (mut index, _) = index().await;
    index
        .put_document(id("short"), b"common".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("long"), b"common other other other".to_vec())
        .await
        .unwrap();

    let query = FtsQuery::Or(vec!["common".into()]);
    for (k1, expected_long) in [
        (f64::MIN_POSITIVE, 1.2f64.ln()),
        (f64::from_bits(1), 1.2f64.ln()),
        (f64::MAX, 1.2f64.ln() / 1.6),
    ] {
        let hits = index
            .search_ranked(query.clone(), 10, Bm25Config::new(k1, 1.0).unwrap())
            .await
            .unwrap();
        let score = hits
            .iter()
            .find(|hit| hit.id.as_str() == "long")
            .unwrap()
            .score;
        assert!(score.is_finite());
        assert!((score - expected_long).abs() < 1e-12);
    }

    let hits = index
        .search_ranked(query, 10, Bm25Config::new(f64::MAX, 0.0).unwrap())
        .await
        .unwrap();
    let expected = 1.2f64.ln();
    assert!(
        (hits
            .iter()
            .find(|hit| hit.id.as_str() == "long")
            .unwrap()
            .score
            - expected)
            .abs()
            < 1e-12
    );
}

#[tokio::test]
async fn zero_statistics_validate_requested_terms_with_or_without_df_records() {
    for keep_df in [true, false] {
        let (mut index, directory) = index().await;
        index
            .put_document(id("one"), b"present".to_vec())
            .await
            .unwrap();
        assert_eq!(
            index
                .search(FtsQuery::Or(vec!["present".into()]), 10)
                .await
                .unwrap()
                .len(),
            1
        );
        let mut engine = index.into_inner();
        engine
            .put(
                b"\0ceredb:fts:v1:s".to_vec(),
                [0u64.to_be_bytes(), 0u64.to_be_bytes()].concat(),
            )
            .await
            .unwrap();
        if !keep_df {
            engine
                .delete(b"\0ceredb:fts:v1:g\0\0\0\x07present".to_vec())
                .await
                .unwrap();
        }
        drop(engine);

        let engine = KV2::open_with_defaults(directory.path()).await.unwrap();
        let index = FtsIndex::open(engine).await.unwrap();
        assert!(
            index
                .search_ranked(
                    FtsQuery::Or(vec!["present".into()]),
                    10,
                    Bm25Config::default(),
                )
                .await
                .is_err(),
            "corrupt zero statistics must be rejected when keep_df={keep_df}"
        );
    }
}

#[tokio::test]
async fn empty_and_zero_token_corpora_rank_to_no_hits() {
    let (empty_index, _) = index().await;
    assert!(
        empty_index
            .search_ranked(
                FtsQuery::Or(vec!["absent".into()]),
                10,
                Bm25Config::default(),
            )
            .await
            .unwrap()
            .is_empty()
    );

    let (mut index, _) = index().await;
    index
        .put_document(id("punctuation"), b"!!!".to_vec())
        .await
        .unwrap();
    assert!(
        index
            .search_ranked(
                FtsQuery::Or(vec!["absent".into()]),
                10,
                Bm25Config::default(),
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn boolean_modes_empty_documents_updates_delete_and_reopen_keep_statistics_coherent() {
    let (mut index, dir) = index().await;
    index
        .put_document(id("empty"), b"!!!".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("one"), b"apple apple".to_vec())
        .await
        .unwrap();
    index
        .put_document(id("two"), b"apple pear".to_vec())
        .await
        .unwrap();
    let config = Bm25Config::default();
    let apple = FtsQuery::Or(vec!["APPLE".into(), "apple".into()]);
    let initial = index
        .search_ranked(apple.clone(), 10, config)
        .await
        .unwrap();
    assert_eq!(initial.len(), 2);
    index
        .put_document(id("one"), b"apple pear pear".to_vec())
        .await
        .unwrap();
    let and = index
        .search_ranked(FtsQuery::And(vec!["apple pear".into()]), 10, config)
        .await
        .unwrap();
    assert_eq!(and.len(), 2);
    assert!(index.delete_document(&id("two")).await.unwrap());
    drop(index);
    let engine = KV2::open_with_defaults(dir.path()).await.unwrap();
    let index = FtsIndex::open(engine).await.unwrap();
    let after = index.search_ranked(apple, 10, config).await.unwrap();
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id.as_str(), "one");
}

#[tokio::test]
async fn configuration_and_query_limits_are_validated() {
    for (k1, b) in [
        (f64::NAN, 0.5),
        (f64::INFINITY, 0.5),
        (0.0, 0.5),
        (1.2, f64::NAN),
        (1.2, -0.1),
        (1.2, 1.1),
    ] {
        assert!(Bm25Config::new(k1, b).is_err());
    }
    assert!(Bm25Config::new(0.1, 0.0).is_ok());
    assert!(Bm25Config::new(1.0, 1.0).is_ok());
    let (index, _) = index().await;
    assert!(
        index
            .search_ranked(FtsQuery::And(Vec::new()), 0, Bm25Config::default())
            .await
            .is_err()
    );
    assert!(
        index
            .search_ranked(FtsQuery::Or(vec!["ok".into()]), 1001, Bm25Config::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn limit_keeps_lowest_id_when_top_scores_tie() {
    let (mut index, _) = index().await;
    index.put_document(id("z"), b"tied".to_vec()).await.unwrap();
    index.put_document(id("a"), b"tied".to_vec()).await.unwrap();
    let hits = index
        .search_ranked(FtsQuery::Or(vec!["tied".into()]), 1, Bm25Config::default())
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id.as_str(), "a");
}

#[tokio::test]
async fn format_one_is_rejected_without_mutating_its_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let mut engine = KV2::open_with_defaults(dir.path()).await.unwrap();
    let meta = b"\0ceredb:fts:v1:m".to_vec();
    let record = b"\0ceredb:fts:v1:legacy".to_vec();
    engine.put(meta.clone(), vec![1, 1]).await.unwrap();
    engine
        .put(record.clone(), b"preserve".to_vec())
        .await
        .unwrap();
    drop(engine);
    let engine = KV2::open_with_defaults(dir.path()).await.unwrap();
    assert!(FtsIndex::open(engine).await.is_err());
    let engine = KV2::open_with_defaults(dir.path()).await.unwrap();
    assert_eq!(
        engine.get(&meta).await.unwrap().unwrap().as_ref(),
        &vec![1, 1]
    );
    assert_eq!(
        engine.get(&record).await.unwrap().unwrap().as_ref(),
        b"preserve"
    );
}

#[tokio::test]
async fn wal_rotation_failure_preserves_ranked_scores_through_reopen_and_retry() {
    let directory = tempfile::tempdir().unwrap();
    let storage = ceredb::storage::config::StorageConfig {
        wal_segment_size: 256 * 1024,
        memtable_size_threshold: usize::MAX as u64,
        max_level0_files: usize::MAX,
        ..ceredb::storage::config::StorageConfig::default()
    };
    let engine = KV2::open(directory.path(), storage.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    for (document_id, text) in [
        ("a", "rare rare common"),
        ("b", "rare common"),
        ("c", "common filler"),
    ] {
        index
            .put_document(id(document_id), text.as_bytes().to_vec())
            .await
            .unwrap();
    }
    let query = FtsQuery::Or(vec!["rare".into()]);
    let config = Bm25Config::default();
    let before = index
        .search_ranked(query.clone(), 10, config)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| (hit.id, hit.score))
        .collect::<Vec<_>>();
    let expected_a = expected_score(3.0, 2.0, 2.0, 3.0, 7.0 / 3.0);
    let expected_b = expected_score(3.0, 2.0, 1.0, 2.0, 7.0 / 3.0);
    assert_eq!(before[0].0.as_str(), "a");
    assert_eq!(before[1].0.as_str(), "b");
    assert!((before[0].1 - expected_a).abs() < 1e-12);
    assert!((before[1].1 - expected_b).abs() < 1e-12);

    let obstruction = directory.path().join("wal/00000000000000000001.log");
    tokio::fs::create_dir(&obstruction).await.unwrap();
    assert!(
        index
            .put_document(id("a"), vec![b'x'; 300 * 1024])
            .await
            .is_err()
    );
    let after_failure = index
        .search_ranked(query.clone(), 10, config)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| (hit.id, hit.score))
        .collect::<Vec<_>>();
    assert_eq!(after_failure, before);
    drop(index);

    tokio::fs::remove_dir(&obstruction).await.unwrap();
    let engine = KV2::open(directory.path(), storage.clone()).await.unwrap();
    let mut index = FtsIndex::open(engine).await.unwrap();
    let after_reopen = index
        .search_ranked(query.clone(), 10, config)
        .await
        .unwrap()
        .into_iter()
        .map(|hit| (hit.id, hit.score))
        .collect::<Vec<_>>();
    assert_eq!(after_reopen, before);

    index
        .put_document(id("a"), b"rare common common common".to_vec())
        .await
        .unwrap();
    let after_update = index
        .search_ranked(query.clone(), 10, config)
        .await
        .unwrap();
    let updated_a = after_update
        .iter()
        .find(|hit| hit.id.as_str() == "a")
        .unwrap();
    let updated_b = after_update
        .iter()
        .find(|hit| hit.id.as_str() == "b")
        .unwrap();
    assert!((updated_a.score - expected_score(3.0, 2.0, 1.0, 4.0, 8.0 / 3.0)).abs() < 1e-12);
    assert!((updated_b.score - expected_score(3.0, 2.0, 1.0, 2.0, 8.0 / 3.0)).abs() < 1e-12);
    assert!(updated_a.score < updated_b.score);

    assert!(index.delete_document(&id("a")).await.unwrap());
    let after_delete = index
        .search_ranked(query.clone(), 10, config)
        .await
        .unwrap();
    assert_eq!(after_delete.len(), 1);
    assert_eq!(after_delete[0].id.as_str(), "b");
    assert!((after_delete[0].score - expected_score(2.0, 1.0, 1.0, 2.0, 2.0)).abs() < 1e-12);

    index
        .put_document(id("a"), b"rare rare common".to_vec())
        .await
        .unwrap();
    let after_reinsert = index.search_ranked(query, 10, config).await.unwrap();
    assert_eq!(after_reinsert.len(), 2);
    let reinserted_a = after_reinsert
        .iter()
        .find(|hit| hit.id.as_str() == "a")
        .unwrap();
    let reinserted_b = after_reinsert
        .iter()
        .find(|hit| hit.id.as_str() == "b")
        .unwrap();
    assert!((reinserted_a.score - expected_score(3.0, 2.0, 2.0, 3.0, 7.0 / 3.0)).abs() < 1e-12);
    assert!((reinserted_b.score - expected_score(3.0, 2.0, 1.0, 2.0, 7.0 / 3.0)).abs() < 1e-12);
}
