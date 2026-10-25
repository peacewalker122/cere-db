use std::{hint::black_box, path::Path, time::Instant};

use ceredb::{
    Bm25Config, DocumentId, FtsIndex, FtsQuery, KV2, StorageConfig,
    api::api::{AsyncKVEngine, BatchOperation},
    command::{Response, execute_command_async, parse_command},
    storage::manifest_codec::ManifestManager,
};

use crate::support::{BenchConfig, BenchResult, DiskUsage, Measurement};

const BASELINE_SCENARIOS: &[&str] = &[
    "e2e/open_empty",
    "e2e/put_sequential_durable",
    "e2e/get_present_single_pass",
    "e2e/get_missing_single_pass",
    "e2e/overwrite_durable",
    "e2e/delete_durable",
    "e2e/range_scan",
    "e2e/reopen_before_flush",
];
const CHECKPOINT_SCENARIOS: &[&str] = &[
    "e2e/put_automatic_checkpoint",
    "e2e/get_after_flush",
    "e2e/reopen_after_flush",
    "e2e/range_scan_after_flush",
];

type Entries = Vec<(Vec<u8>, Vec<u8>)>;

pub async fn run(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if BASELINE_SCENARIOS.iter().any(|name| config.includes(name)) {
        baseline(config, sample, measurements).await?;
    }
    if CHECKPOINT_SCENARIOS
        .iter()
        .any(|name| config.includes(name))
    {
        checkpoints(config, sample, measurements).await?;
    }
    if config.includes("e2e/wal_checkpoint_reclamation") {
        wal_checkpoint_reclamation(config, sample, measurements).await?;
    }
    if [
        "e2e/range_scan_after_flush_delete",
        "e2e/range_scan_after_flush_reinsert",
        "app/sequential_set_checkpoint",
    ]
    .iter()
    .any(|name| config.includes(name))
    {
        checkpoint_mutations(config, sample, measurements).await?;
    }
    if [
        "e2e/fts_index_put",
        "e2e/fts_reopen_before_flush",
        "e2e/fts_reopen_after_flush",
        "e2e/fts_search_after_flush",
        "e2e/fts_search_common",
        "e2e/fts_search_rare",
        "e2e/fts_search_missing",
        "e2e/fts_search_and",
        "e2e/fts_search_or",
        "e2e/fts_search_ranked_common",
        "e2e/fts_search_ranked_rare",
        "e2e/fts_search_ranked_missing",
        "e2e/fts_search_ranked_and",
        "e2e/fts_search_ranked_or",
        "e2e/fts_ranked_after_flush",
    ]
    .iter()
    .any(|name| config.includes(name))
    {
        fts_lifecycle(config, sample, measurements).await?;
    }
    if [
        "app/doc_put_documents",
        "app/fts_and_search",
        "app/fts_or_search",
        "app/fts_ranked_and_search",
        "app/fts_ranked_or_search",
    ]
    .iter()
    .any(|name| config.includes(name))
    {
        app_fts_workloads(config, sample, measurements).await?;
    }
    if [1, 16, 64, 256].iter().any(|size| {
        config.includes(&format!("e2e/batch_put_{size}"))
            || config.includes(&format!("e2e/batch_mixed_{size}"))
            || config.includes(&format!("app/batch_{size}"))
    }) || config.includes("e2e/batch_sequential_checkpoint")
        || config.includes("app/sequential_set_checkpoint")
    {
        batch_workloads(config, sample, measurements).await?;
    }
    Ok(())
}

fn fts_documents(config: &BenchConfig) -> BenchResult<Vec<(DocumentId, Vec<u8>)>> {
    Ok((0..config.records)
        .map(|number| {
            let id = DocumentId::new(format!("doc-{number:016x}"))?;
            let text = if number == 0 {
                format!("common shared rareterm payload-{number}")
            } else if number + 1 == config.records && config.records > 1 {
                format!("common shared deleteterm payload-{number}")
            } else {
                format!("common shared payload-{number}")
            };
            Ok((id, text.into_bytes()))
        })
        .collect::<Result<_, ceredb::error::DBError>>()?)
}

async fn fts_lifecycle(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let directory = config.temporary_directory()?;
    let mut settings = storage_config(config, u64::MAX);
    settings.max_level0_files = 1;
    let engine = KV2::open(directory.path(), settings.clone()).await?;
    let mut index = FtsIndex::open(engine).await?;
    let documents = fts_documents(config)?;
    let replacement = (config.records > 2)
        .then(|| (documents[1].0.clone(), b"common revised payload-1".to_vec()));
    let deleted = (config.records > 1).then(|| documents.last().unwrap().0.clone());
    let live_documents: Vec<_> = documents
        .iter()
        .filter(|(id, _)| deleted.as_ref() != Some(id))
        .map(|(id, document)| {
            replacement
                .as_ref()
                .filter(|(replace_id, _)| replace_id == id)
                .cloned()
                .unwrap_or_else(|| (id.clone(), document.clone()))
        })
        .collect();

    if config.includes("e2e/fts_index_put") {
        let started = Instant::now();
        for (id, document) in &documents {
            index.put_document(id.clone(), document.clone()).await?;
        }
        if let Some((id, document)) = &replacement {
            index.put_document(id.clone(), document.clone()).await?;
        }
        if let Some(id) = &deleted {
            index.delete_document(id).await?;
        }
        let elapsed = started.elapsed();
        verify_fts_documents(&index, &live_documents).await?;
        if let Some(id) = &deleted {
            if index.get_document(id).await?.is_some() {
                return Err("FTS delete left the document readable".into());
            }
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/fts_index_put",
                sample,
                config.records + u64::from(replacement.is_some()) + u64::from(deleted.is_some()),
                documents
                    .iter()
                    .map(|(_, document)| document.len() as u64)
                    .sum::<u64>()
                    + replacement
                        .as_ref()
                        .map_or(0, |(_, document)| document.len() as u64),
                elapsed,
            ),
            directory.path(),
        )?;
    } else {
        for (id, document) in &documents {
            index.put_document(id.clone(), document.clone()).await?;
        }
        if let Some((id, document)) = &replacement {
            index.put_document(id.clone(), document.clone()).await?;
        }
        if let Some(id) = &deleted {
            index.delete_document(id).await?;
        }
    }

    let persisted = index.into_inner();
    drop(persisted);
    let started = Instant::now();
    let engine = KV2::open(directory.path(), settings.clone()).await?;
    let elapsed = started.elapsed();
    let index = FtsIndex::open(engine).await?;
    verify_fts_documents(&index, &live_documents).await?;
    if let Some(id) = &deleted {
        if index.get_document(id).await?.is_some() {
            return Err("reopened FTS index retained a deleted document".into());
        }
    }
    verify_fts_postings(&index).await?;
    if config.includes("e2e/fts_reopen_before_flush") {
        record(
            config,
            measurements,
            Measurement::new("e2e/fts_reopen_before_flush", sample, 1, 0, elapsed),
            directory.path(),
        )?;
    }
    bench_fts_queries(&index, config, sample, measurements).await?;

    if config.includes("e2e/fts_reopen_after_flush")
        || config.includes("e2e/fts_search_after_flush")
        || config.includes("e2e/fts_ranked_after_flush")
    {
        let mut checkpoint_settings = storage_config(config, config.flush_bytes);
        checkpoint_settings.max_level0_files = 1;
        let engine = index.into_inner();
        drop(engine);
        let mut engine = KV2::open(directory.path(), checkpoint_settings.clone()).await?;
        for (id, document) in &documents {
            engine
                .put(
                    format!("fts-bench-padding-{}", id.as_str()).into_bytes(),
                    document.clone(),
                )
                .await?;
        }
        engine.shutdown().await?;
        let registered_sstable_count = registered_sstable_count(directory.path()).await?;
        if registered_sstable_count == 0 {
            return Err("FTS checkpoint workload produced no manifest-registered SSTable; increase --records or lower --flush-bytes".into());
        }
        drop(engine);
        let started = Instant::now();
        let engine = KV2::open(directory.path(), checkpoint_settings).await?;
        let elapsed = started.elapsed();
        let index = FtsIndex::open(engine).await?;
        verify_fts_documents(&index, &live_documents).await?;
        if let Some(id) = &deleted {
            if index.get_document(id).await?.is_some() {
                return Err("reopened FTS index retained a deleted document".into());
            }
        }
        verify_fts_postings(&index).await?;
        verify_fts_queries(&index, config).await?;
        if config.includes("e2e/fts_search_after_flush") {
            let limit =
                (config.records.saturating_sub(u64::from(config.records > 1)) as usize).min(1000);
            let started = Instant::now();
            let hits = index
                .search(
                    FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
                    limit,
                )
                .await?;
            let elapsed = started.elapsed();
            if hits.len() != limit
                || hits
                    .windows(2)
                    .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
                || hits
                    .first()
                    .is_none_or(|hit| hit.id.as_str() != "doc-0000000000000000")
            {
                return Err("FTS OR search after flush returned incorrect results".into());
            }
            record(
                config,
                measurements,
                Measurement::new("e2e/fts_search_after_flush", sample, 1, 0, elapsed)
                    .with_registered_sstable_count(registered_sstable_count),
                directory.path(),
            )?;
        }
        if config.includes("e2e/fts_ranked_after_flush") {
            let started = Instant::now();
            let hits = index
                .search_ranked(
                    FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
                    100,
                    Bm25Config::default(),
                )
                .await?;
            let elapsed = started.elapsed();
            if hits.len() != live_documents.len().min(100)
                || hits
                    .first()
                    .is_none_or(|hit| hit.id.as_str() != "doc-0000000000000000")
                || hits
                    .iter()
                    .any(|hit| !hit.score.is_finite() || hit.score <= 0.0)
            {
                return Err("ranked FTS search after flush returned incorrect results".into());
            }
            record(
                config,
                measurements,
                Measurement::new("e2e/fts_ranked_after_flush", sample, 1, 0, elapsed)
                    .with_registered_sstable_count(registered_sstable_count),
                directory.path(),
            )?;
        }
        if config.includes("e2e/fts_reopen_after_flush") {
            record(
                config,
                measurements,
                Measurement::new("e2e/fts_reopen_after_flush", sample, 1, 0, elapsed)
                    .with_registered_sstable_count(registered_sstable_count),
                directory.path(),
            )?;
        }
    }
    Ok(())
}

async fn registered_sstable_count(directory: &Path) -> BenchResult<usize> {
    let manifest = ManifestManager::load_or_create(directory.join("MANIFEST")).await?;
    let snapshot = manifest.snapshot().await;
    let tables: Vec<_> = snapshot.levels.values().flatten().collect();
    if tables
        .iter()
        .any(|table| match std::fs::metadata(&table.path) {
            Ok(metadata) => metadata.len() == 0,
            Err(_) => true,
        })
    {
        return Err("manifest references a missing or empty SSTable".into());
    }
    Ok(tables.len())
}

async fn app_fts_workloads(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let directory = config.temporary_directory()?;
    let engine = KV2::open(directory.path(), storage_config(config, u64::MAX)).await?;
    let mut index = FtsIndex::open(engine).await?;
    let documents = fts_documents(config)?;

    if config.includes("app/doc_put_documents") {
        let started = Instant::now();
        for (id, document) in &documents {
            let command = format!(
                "DOC PUT {} \"{}\"",
                id.as_str(),
                String::from_utf8_lossy(document)
            );
            let parsed = parse_command(&command)
                .map_err(|error| format!("benchmark generated invalid DOC PUT: {error:?}"))?;
            if execute_command_async(&mut index, parsed).await? != Response::Ok {
                return Err("DOC PUT returned a non-OK response".into());
            }
        }
        let elapsed = started.elapsed();
        verify_fts_documents(&index, &documents).await?;
        record(
            config,
            measurements,
            Measurement::new(
                "app/doc_put_documents",
                sample,
                config.records,
                documents
                    .iter()
                    .map(|(_, document)| document.len() as u64)
                    .sum(),
                elapsed,
            ),
            directory.path(),
        )?;
    }

    for (scenario, command_text, expected_id) in [
        (
            "app/fts_and_search",
            "FTS AND common rareterm",
            Some("doc-0000000000000000"),
        ),
        (
            "app/fts_or_search",
            "FTS OR common rareterm",
            Some("doc-0000000000000000"),
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        if !config.includes("app/doc_put_documents") {
            for (id, document) in &documents {
                index.put_document(id.clone(), document.clone()).await?;
            }
        }
        let started = Instant::now();
        let command = parse_command(command_text)
            .map_err(|error| format!("benchmark generated invalid FTS command: {error:?}"))?;
        let response = execute_command_async(&mut index, command).await?;
        let elapsed = started.elapsed();
        let Response::SearchResults(hits) = response else {
            return Err(format!("{scenario} returned a non-search response").into());
        };
        let expected_count = if scenario.ends_with("and_search") {
            1
        } else {
            (config.records as usize).min(100)
        };
        if hits.len() != expected_count
            || hits
                .windows(2)
                .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
            || hits.first().map(|hit| hit.id.as_str()) != expected_id
        {
            return Err(format!("{scenario} returned incorrect results").into());
        }
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    for (scenario, command_text, expected_count) in [
        (
            "app/fts_ranked_and_search",
            "FTS RANK AND \"common rareterm\"",
            1,
        ),
        (
            "app/fts_ranked_or_search",
            "FTS RANK OR \"common rareterm\"",
            (config.records as usize).min(100),
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        if !config.includes("app/doc_put_documents") {
            for (id, document) in &documents {
                index.put_document(id.clone(), document.clone()).await?;
            }
        }
        let started = Instant::now();
        let command = parse_command(command_text).map_err(|error| {
            format!("benchmark generated invalid ranked FTS command: {error:?}")
        })?;
        let response = execute_command_async(&mut index, command).await?;
        let elapsed = started.elapsed();
        let Response::RankedSearchResults(hits) = response else {
            return Err(format!("{scenario} returned a non-ranked-search response").into());
        };
        if hits.len() != expected_count
            || hits
                .iter()
                .any(|hit| !hit.score.is_finite() || hit.score <= 0.0)
            || hits.windows(2).any(|pair| {
                pair[0].score < pair[1].score
                    || (pair[0].score == pair[1].score
                        && pair[0].id.as_str() >= pair[1].id.as_str())
            })
        {
            return Err(format!("{scenario} returned incorrect ranked results").into());
        }
        let repeat_command = parse_command(command_text).map_err(|error| {
            format!("benchmark generated invalid ranked FTS command: {error:?}")
        })?;
        let repeat_response = execute_command_async(&mut index, repeat_command).await?;
        let Response::RankedSearchResults(repeated) = repeat_response else {
            return Err(format!("{scenario} repeat returned a non-ranked response").into());
        };
        if hits != repeated {
            return Err(format!("{scenario} returned nondeterministic ranked results").into());
        }
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    Ok(())
}

async fn verify_fts_documents<E: AsyncKVEngine>(
    index: &FtsIndex<E>,
    expected: &[(DocumentId, Vec<u8>)],
) -> BenchResult<()> {
    for (id, document) in expected {
        if index.get_document(id).await?.as_ref() != Some(document) {
            return Err(format!("FTS document mismatch for {}", id.as_str()).into());
        }
    }
    Ok(())
}

async fn verify_fts_queries<E: AsyncKVEngine>(
    index: &FtsIndex<E>,
    config: &BenchConfig,
) -> BenchResult<()> {
    let result_limit =
        (config.records.saturating_sub(u64::from(config.records > 1)) as usize).min(1000);
    let common = index
        .search(FtsQuery::And(vec!["common".into()]), result_limit)
        .await?;
    if common.len() != result_limit
        || common
            .windows(2)
            .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
        || common
            .first()
            .is_none_or(|hit| hit.id.as_str() != "doc-0000000000000000")
    {
        return Err("FTS common-term search returned incorrect count or ordering".into());
    }
    let rare = index
        .search(FtsQuery::And(vec!["rareterm".into()]), 10)
        .await?;
    if rare.len() != 1 || rare[0].id.as_str() != "doc-0000000000000000" {
        return Err("FTS rare-term search returned incorrect hits".into());
    }
    let missing = index
        .search(FtsQuery::And(vec!["notindexed".into()]), 10)
        .await?;
    if !missing.is_empty() {
        return Err("FTS missing-term search returned hits".into());
    }
    let and_hits = index
        .search(FtsQuery::And(vec!["common".into(), "rareterm".into()]), 10)
        .await?;
    if and_hits.len() != 1 || and_hits[0].id.as_str() != "doc-0000000000000000" {
        return Err("FTS AND search returned incorrect hits".into());
    }
    let or_hits = index
        .search(
            FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
            result_limit,
        )
        .await?;
    if or_hits.len() != result_limit
        || or_hits
            .windows(2)
            .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
        || or_hits
            .first()
            .is_none_or(|hit| hit.id.as_str() != "doc-0000000000000000")
    {
        return Err("FTS OR search returned incorrect count or ordering".into());
    }
    verify_fts_postings(index).await?;
    Ok(())
}

async fn verify_fts_postings<E: AsyncKVEngine>(index: &FtsIndex<E>) -> BenchResult<()> {
    if !index
        .search(FtsQuery::And(vec!["revised".into(), "rareterm".into()]), 10)
        .await?
        .is_empty()
    {
        return Err("FTS overwrite retained a stale posting".into());
    }
    if !index
        .search(FtsQuery::And(vec!["deleteterm".into()]), 10)
        .await?
        .is_empty()
    {
        return Err("FTS delete retained a posting".into());
    }
    Ok(())
}

async fn bench_fts_queries<E: AsyncKVEngine>(
    index: &FtsIndex<E>,
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let limit = (config.records.saturating_sub(u64::from(config.records > 1)) as usize).min(1000);
    for (scenario, query, expected_count) in [
        (
            "e2e/fts_search_common",
            FtsQuery::And(vec!["common".into()]),
            limit,
        ),
        (
            "e2e/fts_search_rare",
            FtsQuery::And(vec!["rareterm".into()]),
            1,
        ),
        (
            "e2e/fts_search_missing",
            FtsQuery::And(vec!["notindexed".into()]),
            0,
        ),
        (
            "e2e/fts_search_and",
            FtsQuery::And(vec!["common".into(), "rareterm".into()]),
            1,
        ),
        (
            "e2e/fts_search_or",
            FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
            limit,
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        let started = Instant::now();
        let hits = index.search(query, limit.max(1)).await?;
        let elapsed = started.elapsed();
        if hits.len() != expected_count
            || hits
                .windows(2)
                .any(|pair| pair[0].id.as_str() >= pair[1].id.as_str())
            || hits
                .first()
                .is_some_and(|hit| hit.id.as_str() != "doc-0000000000000000")
        {
            return Err(format!("{scenario} returned incorrect hit count or ordering").into());
        }
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    for (scenario, query, expected_count) in [
        (
            "e2e/fts_search_ranked_common",
            FtsQuery::And(vec!["common".into()]),
            limit,
        ),
        (
            "e2e/fts_search_ranked_rare",
            FtsQuery::And(vec!["rareterm".into()]),
            1,
        ),
        (
            "e2e/fts_search_ranked_missing",
            FtsQuery::And(vec!["notindexed".into()]),
            0,
        ),
        (
            "e2e/fts_search_ranked_and",
            FtsQuery::And(vec!["common".into(), "rareterm".into()]),
            1,
        ),
        (
            "e2e/fts_search_ranked_or",
            FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
            limit,
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        let started = Instant::now();
        let hits = index
            .search_ranked(query, limit.max(1), Bm25Config::default())
            .await?;
        let elapsed = started.elapsed();
        if hits.len() != expected_count
            || hits.windows(2).any(|pair| {
                pair[0].score < pair[1].score
                    || (pair[0].score == pair[1].score
                        && pair[0].id.as_str() >= pair[1].id.as_str())
            })
            || hits
                .iter()
                .any(|hit| !hit.score.is_finite() || hit.score <= 0.0)
        {
            return Err(format!("{scenario} returned incorrect ranked results").into());
        }
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    Ok(())
}

async fn batch_workloads(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if config.includes("e2e/batch_sequential_checkpoint") {
        let directory = config.temporary_directory()?;
        let settings = storage_config(config, config.flush_bytes);
        let mut engine = KV2::open(directory.path(), settings.clone()).await?;
        let expected = entries(config, false);
        let inputs = expected.clone();
        let started = Instant::now();
        for (key, value) in inputs {
            engine.put(key, value).await?;
        }
        let elapsed = started.elapsed();
        if DiskUsage::inspect(directory.path())?.sstable_bytes == 0 {
            return Err("sequential batch baseline produced no automatic checkpoint; increase --records or lower --flush-bytes".into());
        }
        verify(&engine, &expected, false).await?;
        drop(engine);
        let reopened = KV2::open(directory.path(), settings).await?;
        verify(&reopened, &expected, false).await?;
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/batch_sequential_checkpoint",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    for batch_size in [1u64, 16, 64, 256] {
        let app_scenario = match batch_size {
            1 => "app/batch_1",
            16 => "app/batch_16",
            64 => "app/batch_64",
            _ => "app/batch_256",
        };
        if config.includes(app_scenario) {
            let directory = config.temporary_directory()?;
            let mut engine =
                KV2::open(directory.path(), storage_config(config, config.flush_bytes)).await?;
            let expected: Entries = (0..config.records)
                .map(|index| {
                    (
                        format!("key-{index:016x}").into_bytes(),
                        format!("value-{index}").into_bytes(),
                    )
                })
                .collect();
            let lines: Vec<String> = (0..config.records)
                .step_by(batch_size as usize)
                .map(|start| {
                    let end = (start + batch_size).min(config.records);
                    let mut line = String::from("BATCH");
                    for index in start..end {
                        line.push_str(&format!(" SET key-{index:016x} value-{index}"));
                    }
                    line
                })
                .collect();
            let started = Instant::now();
            for line in lines {
                let command = parse_command(&line).map_err(|error| {
                    format!("benchmark generated invalid batch command: {error:?}")
                })?;
                execute_command_async(&mut engine, command).await?;
            }
            let elapsed = started.elapsed();
            if DiskUsage::inspect(directory.path())?.sstable_bytes == 0 {
                return Err("batch workload produced no automatic checkpoint; increase --records or lower --flush-bytes".into());
            }
            verify(&engine, &expected, false).await?;
            drop(engine);
            let reopened =
                KV2::open(directory.path(), storage_config(config, config.flush_bytes)).await?;
            verify(&reopened, &expected, false).await?;
            let logical_bytes = expected
                .iter()
                .map(|(key, value)| (key.len() + value.len()) as u64)
                .sum();
            record(
                config,
                measurements,
                Measurement::new(app_scenario, sample, config.records, logical_bytes, elapsed),
                directory.path(),
            )?;
        }
        for mixed in [false, true] {
            let scenario = if mixed {
                match batch_size {
                    1 => "e2e/batch_mixed_1",
                    16 => "e2e/batch_mixed_16",
                    64 => "e2e/batch_mixed_64",
                    _ => "e2e/batch_mixed_256",
                }
            } else {
                match batch_size {
                    1 => "e2e/batch_put_1",
                    16 => "e2e/batch_put_16",
                    64 => "e2e/batch_put_64",
                    _ => "e2e/batch_put_256",
                }
            };
            if !config.includes(scenario) {
                continue;
            }
            let directory = config.temporary_directory()?;
            let settings = storage_config(config, config.flush_bytes);
            let mut engine = KV2::open(directory.path(), settings.clone()).await?;
            let expected = entries(config, false);
            let mut operations = Vec::new();
            for index in 0..config.records {
                if mixed && index % 2 == 0 {
                    operations.push(BatchOperation::Put {
                        key: config.key(index),
                        value: config.value(index + 1),
                    });
                    operations.push(BatchOperation::Delete {
                        key: config.key(index),
                    });
                    operations.push(BatchOperation::Put {
                        key: config.key(index),
                        value: config.value(index),
                    });
                } else {
                    operations.push(BatchOperation::Put {
                        key: config.key(index),
                        value: config.value(index),
                    });
                }
            }
            let batches: Vec<Vec<BatchOperation>> = operations
                .chunks(batch_size as usize)
                .map(<[BatchOperation]>::to_vec)
                .collect();
            let started = Instant::now();
            for operations in batches {
                engine.write_batch(operations).await?;
            }
            let elapsed = started.elapsed();
            if DiskUsage::inspect(directory.path())?.sstable_bytes == 0 {
                return Err("batch workload produced no automatic checkpoint; increase --records or lower --flush-bytes".into());
            }
            verify(&engine, &expected, false).await?;
            drop(engine);
            let reopened = KV2::open(directory.path(), settings).await?;
            verify(&reopened, &expected, false).await?;
            let mutations = if mixed {
                config.records + 2 * (config.records / 2 + config.records % 2)
            } else {
                config.records
            };
            let logical_bytes = config.logical_bytes()
                + if mixed {
                    (config.records / 2 + config.records % 2) * (40 + config.value_bytes)
                } else {
                    0
                };
            record(
                config,
                measurements,
                Measurement::new(scenario, sample, mutations, logical_bytes, elapsed),
                directory.path(),
            )?;
        }
    }
    Ok(())
}

fn storage_config(config: &BenchConfig, threshold: u64) -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: threshold,
        wal_segment_size: config.wal_segment_bytes,
        max_level0_files: usize::MAX,
        ..StorageConfig::default()
    }
}

fn entries(config: &BenchConfig, overwrite: bool) -> Entries {
    (0..config.records)
        .map(|index| {
            let value_index = if overwrite { index + 1 } else { index };
            (config.key(index), config.value(value_index))
        })
        .collect()
}

fn record(
    config: &BenchConfig,
    measurements: &mut Vec<Measurement>,
    measurement: Measurement,
    path: &Path,
) -> BenchResult<()> {
    if config.includes(measurement.scenario) {
        measurements.push(measurement.with_disk(DiskUsage::inspect(path)?));
    }
    Ok(())
}

async fn verify(engine: &KV2, expected: &Entries, deleted: bool) -> BenchResult<()> {
    for (index, (key, value)) in expected.iter().enumerate() {
        let actual = engine.get(key).await?;
        let expected_value = if deleted && index % 2 == 0 {
            None
        } else {
            Some(value)
        };
        if actual.as_deref() != expected_value {
            return Err(format!("e2e get verification failed for key index {index}").into());
        }
    }
    Ok(())
}

async fn baseline(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let directory = config.temporary_directory()?;
    let settings = storage_config(config, u64::MAX);
    let started = Instant::now();
    let mut engine = KV2::open(directory.path(), settings.clone()).await?;
    let elapsed = started.elapsed();
    if engine.get(&config.key(0)).await?.is_some() {
        return Err("fresh e2e database was not empty".into());
    }
    record(
        config,
        measurements,
        Measurement::new("e2e/open_empty", sample, 1, 0, elapsed),
        directory.path(),
    )?;

    let expected = entries(config, false);
    let inputs = expected.clone();
    let started = Instant::now();
    for (key, value) in inputs {
        engine.put(key, value).await?;
    }
    let elapsed = started.elapsed();
    verify(&engine, &expected, false).await?;
    record(
        config,
        measurements,
        Measurement::new(
            "e2e/put_sequential_durable",
            sample,
            config.records,
            config.logical_bytes(),
            elapsed,
        ),
        directory.path(),
    )?;

    if config.includes("e2e/reopen_before_flush") {
        drop(engine);
        let started = Instant::now();
        engine = KV2::open(directory.path(), settings).await?;
        let elapsed = started.elapsed();
        verify(&engine, &expected, false).await?;
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/reopen_before_flush",
                sample,
                1,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }

    if config.includes("e2e/get_present_single_pass") {
        let mut results = Vec::with_capacity(expected.len());
        let started = Instant::now();
        for (key, _) in &expected {
            results.push(black_box(engine.get(key).await?));
        }
        let elapsed = started.elapsed();
        for (index, (actual, (_, value))) in results.iter().zip(&expected).enumerate() {
            if actual.as_deref() != Some(value) {
                return Err(format!("e2e present read mismatch at {index}").into());
            }
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/get_present_single_pass",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    if config.includes("e2e/get_missing_single_pass") {
        let keys: Vec<_> = (config.records
            ..config
                .records
                .checked_mul(2)
                .ok_or("record count overflow")?)
            .map(|index| config.key(index))
            .collect();
        let mut results = Vec::with_capacity(keys.len());
        let started = Instant::now();
        for key in &keys {
            results.push(black_box(engine.get(key).await?));
        }
        let elapsed = started.elapsed();
        if results.iter().any(Option::is_some) {
            return Err("e2e missing key unexpectedly present".into());
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/get_missing_single_pass",
                sample,
                config.records,
                config.records * 20,
                elapsed,
            ),
            directory.path(),
        )?;
    }

    let needs_mutations = [
        "e2e/overwrite_durable",
        "e2e/delete_durable",
        "e2e/range_scan",
    ]
    .iter()
    .any(|name| config.includes(name));
    if !needs_mutations {
        return Ok(());
    }
    let expected = entries(config, true);
    let inputs = expected.clone();
    let started = Instant::now();
    for (key, value) in inputs {
        engine.put(key, value).await?;
    }
    let elapsed = started.elapsed();
    verify(&engine, &expected, false).await?;
    record(
        config,
        measurements,
        Measurement::new(
            "e2e/overwrite_durable",
            sample,
            config.records,
            config.logical_bytes(),
            elapsed,
        ),
        directory.path(),
    )?;

    if config.includes("e2e/delete_durable") || config.includes("e2e/range_scan") {
        let keys: Vec<_> = expected
            .iter()
            .step_by(2)
            .map(|(key, _)| key.clone())
            .collect();
        let operations = keys.len() as u64;
        let started = Instant::now();
        for key in keys {
            engine.delete(key).await?;
        }
        let elapsed = started.elapsed();
        verify(&engine, &expected, true).await?;
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/delete_durable",
                sample,
                operations,
                operations * 20,
                elapsed,
            ),
            directory.path(),
        )?;
    }
    if config.includes("e2e/range_scan") {
        let lower = config.records / 4;
        let upper = config.records - config.records / 4;
        let range = config.key(lower)..config.key(upper);
        let wanted: Entries = expected
            .into_iter()
            .enumerate()
            .filter(|(index, _)| {
                *index as u64 >= lower && (*index as u64) < upper && index % 2 != 0
            })
            .map(|(_, entry)| entry)
            .collect();
        let logical_bytes = wanted.len() as u64 * (20 + config.value_bytes);
        let started = Instant::now();
        let actual = black_box(engine.scan(range).await?);
        let elapsed = started.elapsed();
        if actual != wanted {
            return Err(format!("e2e range scan mismatch after overwrite/delete: range {lower}..{upper}, expected {} live entries, got {} entries", wanted.len(), actual.len()).into());
        }
        record(
            config,
            measurements,
            Measurement::new("e2e/range_scan", sample, 1, logical_bytes, elapsed),
            directory.path(),
        )?;
    }
    Ok(())
}

async fn checkpoints(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let directory = config.temporary_directory()?;
    let settings = storage_config(config, config.flush_bytes);
    let mut engine = KV2::open(directory.path(), settings.clone()).await?;
    let before = DiskUsage::inspect(directory.path())?;
    let expected = entries(config, false);
    let inputs = expected.clone();
    let started = Instant::now();
    for (key, value) in inputs {
        engine.put(key, value).await?;
    }
    let elapsed = started.elapsed();
    verify(&engine, &expected, false).await?;
    let after = DiskUsage::inspect(directory.path())?;
    if after.sstable_bytes <= before.sstable_bytes {
        return Err(
            "automatic checkpoint produced no SSTable; increase --records or lower --flush-bytes"
                .into(),
        );
    }
    if after.wal_bytes == 0 {
        return Err("automatic checkpoint unexpectedly retained no WAL bytes".into());
    }
    eprintln!(
        "e2e checkpoint sample={sample}: before={before:?}, after={after:?}; compaction disabled; page cache uncontrolled"
    );
    record(
        config,
        measurements,
        Measurement::new(
            "e2e/put_automatic_checkpoint",
            sample,
            config.records,
            config.logical_bytes(),
            elapsed,
        ),
        directory.path(),
    )?;
    if config.includes("e2e/get_after_flush") {
        let mut results = Vec::with_capacity(expected.len());
        let started = Instant::now();
        for (key, _) in &expected {
            results.push(black_box(engine.get(key).await?));
        }
        let elapsed = started.elapsed();
        if results
            .iter()
            .zip(&expected)
            .any(|(actual, (_, value))| actual.as_deref() != Some(value))
        {
            return Err("e2e post-flush get verification failed".into());
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/get_after_flush",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    if config.includes("e2e/range_scan_after_flush") {
        let range = config.key(0)..config.key(config.records);
        let started = Instant::now();
        let actual = black_box(engine.scan(range).await?);
        let elapsed = started.elapsed();
        if actual != expected {
            return Err(format!(
                "e2e range scan after flush mismatch: expected {} entries, got {} entries",
                expected.len(),
                actual.len()
            )
            .into());
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/range_scan_after_flush",
                sample,
                1,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    if config.includes("e2e/reopen_after_flush") {
        drop(engine);
        let started = Instant::now();
        engine = KV2::open(directory.path(), settings).await?;
        let elapsed = started.elapsed();
        verify(&engine, &expected, false).await?;
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/reopen_after_flush",
                sample,
                1,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    Ok(())
}

async fn wal_checkpoint_reclamation(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let directory = config.temporary_directory()?;
    let settings = storage_config(config, config.flush_bytes);
    let mut engine = KV2::open(directory.path(), settings.clone()).await?;
    let records_per_cycle = 8_u64;
    let cycles = config.records.div_ceil(records_per_cycle);
    let started = Instant::now();
    let mut expected = Vec::new();

    for cycle in 0..cycles {
        let first = cycle * records_per_cycle;
        let end = (first + records_per_cycle).min(config.records);
        for index in first..end {
            let key = config.key(index);
            let value = config.value(index);
            let before_put = DiskUsage::inspect(directory.path())?;
            let before_segments = wal_segment_ids(&directory.path().join("wal"))?;
            engine.put(key.clone(), value.clone()).await?;
            let after_put = DiskUsage::inspect(directory.path())?;
            let after_segments = wal_segment_ids(&directory.path().join("wal"))?;
            if after_put.sstable_bytes > before_put.sstable_bytes {
                eprintln!(
                    "e2e WAL checkpoint sample={sample} record={} before_segment_ids={before_segments:?} before_wal_bytes={} after_segment_ids={after_segments:?} after_wal_bytes={}",
                    index + 1,
                    before_put.wal_bytes,
                    after_put.wal_bytes,
                );
            }
            expected.push((key, value));
        }
        verify(&engine, &expected, false).await?;
        let before_reopen = DiskUsage::inspect(directory.path())?;
        let before_segments = wal_segment_ids(&directory.path().join("wal"))?;
        drop(engine);
        engine = KV2::open(directory.path(), settings.clone()).await?;
        verify(&engine, &expected, false).await?;
        let after_reopen = DiskUsage::inspect(directory.path())?;
        let after_segments = wal_segment_ids(&directory.path().join("wal"))?;
        eprintln!(
            "e2e WAL reclamation sample={sample} cycle={}/{} records={} before_segment_ids={before_segments:?} before_wal_bytes={} after_segment_ids={after_segments:?} after_wal_bytes={}",
            cycle + 1,
            cycles,
            expected.len(),
            before_reopen.wal_bytes,
            after_reopen.wal_bytes,
        );
    }

    let elapsed = started.elapsed();
    for index in 0..2 {
        let key = config.key(config.records + index);
        let value = config.value(config.records + index);
        engine.put(key.clone(), value.clone()).await?;
        expected.push((key, value));
    }
    verify(&engine, &expected, false).await?;
    drop(engine);
    let mut engine = KV2::open(directory.path(), settings).await?;
    verify(&engine, &expected, false).await?;
    engine
        .put(
            config.key(config.records + 2),
            config.value(config.records + 2),
        )
        .await?;
    measurements.push(Measurement::new(
        "e2e/wal_checkpoint_reclamation",
        sample,
        config.records + 3,
        config.logical_bytes(),
        elapsed,
    ));
    Ok(())
}

fn wal_segment_ids(directory: &Path) -> BenchResult<Vec<u64>> {
    let mut ids: Vec<_> = std::fs::read_dir(directory)?
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .path()
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.parse().ok())
        })
        .collect();
    ids.sort_unstable();
    Ok(ids)
}

async fn checkpoint_mutations(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if config.includes("app/sequential_set_checkpoint") {
        let directory = config.temporary_directory()?;
        let settings = storage_config(config, config.flush_bytes);
        let mut engine = KV2::open(directory.path(), settings.clone()).await?;
        let expected: Entries = (0..config.records)
            .map(|index| {
                (
                    format!("key-{index:016x}").into_bytes(),
                    format!("value-{index}").into_bytes(),
                )
            })
            .collect();
        let commands: Vec<String> = expected
            .iter()
            .map(|(key, value)| {
                format!(
                    "SET {} {}",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(value)
                )
            })
            .collect();
        let started = Instant::now();
        for line in commands {
            let command = parse_command(&line)
                .map_err(|error| format!("benchmark generated invalid SET command: {error:?}"))?;
            execute_command_async(&mut engine, command).await?;
        }
        let elapsed = started.elapsed();
        if DiskUsage::inspect(directory.path())?.sstable_bytes == 0 {
            return Err("app sequential workload produced no automatic checkpoint; increase --records or lower --flush-bytes".into());
        }
        verify(&engine, &expected, false).await?;
        drop(engine);
        let reopened = KV2::open(directory.path(), settings).await?;
        verify(&reopened, &expected, false).await?;
        let logical_bytes = expected
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();
        record(
            config,
            measurements,
            Measurement::new(
                "app/sequential_set_checkpoint",
                sample,
                config.records,
                logical_bytes,
                elapsed,
            ),
            directory.path(),
        )?;
    }
    let directory = config.temporary_directory()?;
    let settings = storage_config(config, config.flush_bytes);
    let mut engine = KV2::open(directory.path(), settings.clone()).await?;
    let mut expected = entries(config, false);
    for (key, value) in expected.clone() {
        engine.put(key, value).await?;
    }
    if DiskUsage::inspect(directory.path())?.sstable_bytes == 0 {
        return Err(
            "mutation checkpoint produced no SSTable; increase --records or lower --flush-bytes"
                .into(),
        );
    }
    for index in (0..expected.len()).step_by(2) {
        engine.delete(expected[index].0.clone()).await?;
    }
    verify(&engine, &expected, true).await?;
    let surviving: Entries = expected
        .iter()
        .enumerate()
        .filter(|(index, _)| index % 2 != 0)
        .map(|(_, entry)| entry.clone())
        .collect();
    if config.includes("e2e/range_scan_after_flush_delete") {
        let range = config.key(0)..config.key(config.records);
        let started = Instant::now();
        let actual = black_box(engine.scan(range).await?);
        let elapsed = started.elapsed();
        if actual != surviving {
            return Err(
                "post-flush scan resurrected deleted entries or lost surviving values".into(),
            );
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/range_scan_after_flush_delete",
                sample,
                1,
                surviving.len() as u64 * (20 + config.value_bytes),
                elapsed,
            ),
            directory.path(),
        )?;
    }
    if config.includes("e2e/range_scan_after_flush_reinsert") {
        for index in (0..expected.len()).step_by(2) {
            let value = config.value(index as u64 + 2);
            engine.put(expected[index].0.clone(), value.clone()).await?;
            expected[index].1 = value;
        }
        verify(&engine, &expected, false).await?;
        let range = config.key(0)..config.key(config.records);
        let started = Instant::now();
        let actual = black_box(engine.scan(range).await?);
        let elapsed = started.elapsed();
        if actual != expected {
            return Err("post-flush scan lost reinserted values or returned older versions".into());
        }
        record(
            config,
            measurements,
            Measurement::new(
                "e2e/range_scan_after_flush_reinsert",
                sample,
                1,
                config.logical_bytes(),
                elapsed,
            ),
            directory.path(),
        )?;
        drop(engine);
        let engine = KV2::open(directory.path(), settings).await?;
        verify(&engine, &expected, false).await?;
        if engine
            .scan(config.key(0)..config.key(config.records))
            .await?
            != expected
        {
            return Err("reopened mutation workload returned inconsistent scan results".into());
        }
    }
    Ok(())
}
