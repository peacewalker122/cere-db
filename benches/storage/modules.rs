use std::{hint::black_box, io::Cursor, path::Path, sync::Arc, time::Instant};

use ceredb::{
    BatchOperation, Bm25Config, DocumentId, FtsIndex, FtsQuery, KV2, StorageConfig,
    storage::{
        bloom::BloomFilterWrapper,
        checksum::{calculate_crc32, verify_crc32},
        compactionmanager::compaction::compaction,
        footer::SSTableFooter,
        index::SparseIndexEntry,
        manifest_codec::{ManifestManager, SSTableMeta},
        record::{MemtableRecord, Record, RecordType},
        sstable_codec::SSTableCodec,
        writemanager::{block::Block, write::WriteComponent},
    },
};
use tokio_util::sync::CancellationToken;

use crate::support::{BenchConfig, BenchResult, DiskUsage, Measurement};

pub async fn run(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    cpu(config, sample, measurements).await?;
    wal(config, sample, measurements).await?;
    manifest(config, sample, measurements).await?;
    writer(config, sample, measurements).await?;
    compact(config, sample, measurements).await?;
    fts_put_workloads(config, sample, measurements).await?;
    fts(config, sample, measurements).await?;
    Ok(())
}

async fn fts_put_workloads(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let sizes = [1usize, 16, 64, 256];
    if !sizes.iter().any(|size| {
        config.includes(&format!("fts/put_sequential_{size}"))
            || config.includes(&format!("fts/put_batch_{size}"))
    }) {
        return Ok(());
    }
    if config.records > 65_536 {
        return Err("FTS matched-put scenarios are capped at 65,536 documents".into());
    }
    let documents: Vec<_> = (0..config.records)
        .map(|number| {
            let id = DocumentId::new(format!("doc-{number:016x}"))?;
            let text = if number == 0 {
                format!("common shared rareterm payload-{number}")
            } else {
                format!("common shared payload-{number}")
            };
            Ok((id, text.into_bytes()))
        })
        .collect::<Result<_, ceredb::error::DBError>>()?;
    let input_bytes = documents
        .iter()
        .map(|(_, text)| text.len() as u64)
        .sum::<u64>();

    for size in sizes {
        for batch in [false, true] {
            let scenario = match (batch, size) {
                (false, 1) => "fts/put_sequential_1",
                (false, 16) => "fts/put_sequential_16",
                (false, 64) => "fts/put_sequential_64",
                (false, _) => "fts/put_sequential_256",
                (true, 1) => "fts/put_batch_1",
                (true, 16) => "fts/put_batch_16",
                (true, 64) => "fts/put_batch_64",
                (true, _) => "fts/put_batch_256",
            };
            if !config.includes(scenario) {
                continue;
            }
            let directory = config.temporary_directory()?;
            let engine = KV2::open(directory.path(), storage_config(config)).await?;
            let mut index = FtsIndex::open(engine).await?;
            let batch_inputs = batch.then(|| {
                documents
                    .chunks(size)
                    .map(<[(DocumentId, Vec<u8>)]>::to_vec)
                    .collect::<Vec<_>>()
            });
            let sequential_inputs = (!batch).then(|| {
                documents
                    .iter()
                    .map(|(id, document)| (id.clone(), document.clone()))
                    .collect::<Vec<_>>()
            });
            let started = Instant::now();
            if let Some(groups) = batch_inputs {
                for group in groups {
                    index.put_documents(group).await?;
                }
            } else if let Some(inputs) = sequential_inputs {
                for (id, document) in inputs {
                    index.put_document(id, document).await?;
                }
            }
            let elapsed = started.elapsed();
            for (id, document) in &documents {
                require(
                    index.get_document(id).await?.as_ref() == Some(document),
                    "FTS put benchmark document mismatch",
                )?;
            }
            let expected = documents.len().min(1000);
            let hits = index
                .search(FtsQuery::And(vec!["common".into()]), expected)
                .await?;
            require(
                hits.len() == expected,
                "FTS put benchmark search count mismatch",
            )?;
            require(
                hits.first()
                    .is_none_or(|hit| hit.id.as_str() == "doc-0000000000000000"),
                "FTS put benchmark search ordering mismatch",
            )?;
            let ranked = index
                .search_ranked(
                    FtsQuery::And(vec!["common".into()]),
                    expected,
                    Bm25Config::default(),
                )
                .await?;
            require(
                ranked.len() == expected
                    && ranked
                        .iter()
                        .all(|hit| hit.score.is_finite() && hit.score > 0.0)
                    && ranked.windows(2).all(|pair| {
                        pair[0].score > pair[1].score
                            || (pair[0].score == pair[1].score
                                && pair[0].id.as_str() < pair[1].id.as_str())
                    }),
                "FTS put benchmark ranked search mismatch",
            )?;
            measurements.push(Measurement::new(
                scenario,
                sample,
                documents.len() as u64,
                input_bytes,
                elapsed,
            ));
        }
    }
    Ok(())
}

async fn fts(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let scenarios = [
        "fts/index_put_documents",
        "fts/search_common",
        "fts/search_rare",
        "fts/search_missing",
        "fts/search_and",
        "fts/search_or",
        "fts/search_ranked_common",
        "fts/search_ranked_rare",
        "fts/search_ranked_missing",
        "fts/search_ranked_and",
        "fts/search_ranked_or",
        "fts/search_ranked_limit_1",
        "fts/search_ranked_limit_10",
        "fts/search_ranked_limit_100",
    ];
    if !scenarios.iter().any(|name| config.includes(name)) {
        return Ok(());
    }

    let directory = config.temporary_directory()?;
    let settings = storage_config(config);
    let engine = KV2::open(directory.path(), settings).await?;
    let mut index = FtsIndex::open(engine).await?;
    let documents: Vec<_> = (0..config.records)
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
        .collect::<Result<_, ceredb::error::DBError>>()?;
    let replacement = (config.records > 2)
        .then(|| (documents[1].0.clone(), b"common revised payload-1".to_vec()));
    let deleted = (config.records > 1).then(|| documents.last().unwrap().0.clone());
    let live_documents: Vec<_> = documents
        .iter()
        .filter(|(id, _)| deleted.as_ref() != Some(id))
        .map(|(id, text)| {
            replacement
                .as_ref()
                .filter(|(replace_id, _)| replace_id == id)
                .cloned()
                .unwrap_or_else(|| (id.clone(), text.clone()))
        })
        .collect();

    if config.includes("fts/index_put_documents") {
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
        for (id, document) in &live_documents {
            require(
                index.get_document(id).await?.as_ref() == Some(document),
                "FTS indexed document did not round-trip",
            )?;
        }
        if let Some(id) = &deleted {
            require(
                index.get_document(id).await?.is_none(),
                "FTS deleted document remained present",
            )?;
        }
        let mutations =
            config.records + u64::from(replacement.is_some()) + u64::from(deleted.is_some());
        measurements.push(Measurement::new(
            "fts/index_put_documents",
            sample,
            mutations,
            documents
                .iter()
                .map(|(_, document)| document.len() as u64)
                .sum::<u64>()
                + replacement
                    .as_ref()
                    .map_or(0, |(_, document)| document.len() as u64),
            elapsed,
        ));
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

    for (scenario, query, expected_ids) in [
        (
            "fts/search_common",
            FtsQuery::And(vec!["common".into()]),
            live_documents.len().min(1000),
        ),
        ("fts/search_rare", FtsQuery::And(vec!["rareterm".into()]), 1),
        (
            "fts/search_missing",
            FtsQuery::And(vec!["notindexed".into()]),
            0,
        ),
        (
            "fts/search_and",
            FtsQuery::And(vec!["common".into(), "rareterm".into()]),
            1,
        ),
        (
            "fts/search_or",
            FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
            live_documents.len().min(1000),
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        let started = Instant::now();
        let hits = black_box(index.search(query, expected_ids).await?);
        let elapsed = started.elapsed();
        require(
            hits.len() == expected_ids,
            "FTS query returned unexpected hit count",
        )?;
        require(
            hits.windows(2)
                .all(|pair| pair[0].id.as_str() < pair[1].id.as_str()),
            "FTS results are not in ascending ID order",
        )?;
        require(
            hits.first()
                .is_none_or(|hit| hit.id.as_str() == "doc-0000000000000000"),
            "FTS results do not begin with the expected document ID",
        )?;
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    for (scenario, query, expected_count) in [
        (
            "fts/search_ranked_common",
            FtsQuery::And(vec!["common".into()]),
            live_documents.len().min(1000),
        ),
        (
            "fts/search_ranked_rare",
            FtsQuery::And(vec!["rareterm".into()]),
            1,
        ),
        (
            "fts/search_ranked_missing",
            FtsQuery::And(vec!["notindexed".into()]),
            0,
        ),
        (
            "fts/search_ranked_and",
            FtsQuery::And(vec!["common".into(), "rareterm".into()]),
            1,
        ),
        (
            "fts/search_ranked_or",
            FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
            live_documents.len().min(1000),
        ),
    ] {
        if !config.includes(scenario) {
            continue;
        }
        let started = Instant::now();
        let hits = black_box(
            index
                .search_ranked(query, 1000, Bm25Config::default())
                .await?,
        );
        let elapsed = started.elapsed();
        require(
            hits.len() == expected_count,
            "ranked FTS query returned unexpected hit count",
        )?;
        require(
            hits.iter()
                .all(|hit| hit.score.is_finite() && hit.score > 0.0),
            "ranked FTS query returned an invalid score",
        )?;
        require(
            hits.windows(2).all(|pair| {
                pair[0].score > pair[1].score
                    || (pair[0].score == pair[1].score && pair[0].id.as_str() < pair[1].id.as_str())
            }),
            "ranked FTS results are not ordered by score and ID",
        )?;
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    for limit in [1, 10, 100] {
        let scenario = match limit {
            1 => "fts/search_ranked_limit_1",
            10 => "fts/search_ranked_limit_10",
            _ => "fts/search_ranked_limit_100",
        };
        if !config.includes(scenario) {
            continue;
        }
        let started = Instant::now();
        let hits = black_box(
            index
                .search_ranked(
                    FtsQuery::Or(vec!["common".into(), "rareterm".into()]),
                    limit,
                    Bm25Config::default(),
                )
                .await?,
        );
        let elapsed = started.elapsed();
        require(
            hits.len() == live_documents.len().min(limit),
            "ranked FTS limit was not applied after scoring",
        )?;
        require(
            hits.first()
                .is_none_or(|hit| hit.id.as_str() == "doc-0000000000000000"),
            "ranked FTS limit omitted the expected top result",
        )?;
        measurements.push(Measurement::new(scenario, sample, 1, 0, elapsed));
    }
    let stale_hits = index
        .search(FtsQuery::And(vec!["revised".into(), "rareterm".into()]), 10)
        .await?;
    require(
        stale_hits.is_empty(),
        "FTS overwrite retained a stale posting",
    )?;
    let deleted_hits = index
        .search(FtsQuery::And(vec!["deleteterm".into()]), 10)
        .await?;
    require(deleted_hits.is_empty(), "FTS delete retained a posting")?;
    Ok(())
}

fn require(valid: bool, message: &str) -> BenchResult<()> {
    if !valid {
        return Err(message.into());
    }
    Ok(())
}

fn storage_config(config: &BenchConfig) -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: config.flush_bytes,
        wal_segment_size: config.wal_segment_bytes,
        ..StorageConfig::default()
    }
}

fn records(config: &BenchConfig) -> Vec<Record> {
    (0..config.records)
        .map(|i| Record::new(config.key(i), config.value(i), RecordType::Put, i + 1))
        .collect()
}

async fn cpu(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    let names = [
        "record/encode",
        "record/decode",
        "block/encode",
        "block/decode",
        "checksum/calculate",
        "checksum/verify",
        "bloom/insert",
        "bloom/contains",
        "index/encode",
        "index/decode",
        "footer/encode",
        "footer/decode",
        "sstable/serialize",
        "sstable/deserialize_metadata",
    ];
    if !names.iter().any(|name| config.includes(name)) {
        return Ok(());
    }
    let records = records(config);
    let mut encoded = Vec::with_capacity(records.len());
    let start = Instant::now();
    for record in &records {
        encoded.push(black_box(record).encode());
    }
    let elapsed = start.elapsed();
    for (record, bytes) in records.iter().zip(&encoded) {
        require(
            Record::decode(&mut Cursor::new(bytes))? == *record,
            "record encode mismatch",
        )?;
    }
    if config.includes("record/encode") {
        measurements.push(Measurement::new(
            "record/encode",
            sample,
            config.records,
            config.logical_bytes(),
            elapsed,
        ));
    }
    if config.includes("record/decode") {
        let mut decoded = Vec::with_capacity(records.len());
        let start = Instant::now();
        for bytes in &encoded {
            decoded.push(Record::decode(&mut Cursor::new(black_box(bytes)))?);
        }
        let elapsed = start.elapsed();
        require(decoded == records, "record decode mismatch")?;
        measurements.push(Measurement::new(
            "record/decode",
            sample,
            config.records,
            config.logical_bytes(),
            elapsed,
        ));
    }
    let mut bloom = BloomFilterWrapper::with_rate(records.len(), 0.01);
    let start = Instant::now();
    for record in &records {
        bloom.insert(black_box(&record.key));
    }
    let elapsed = start.elapsed();
    require(
        records.iter().all(|r| bloom.contains(&r.key)),
        "bloom false negative",
    )?;
    if config.includes("bloom/insert") {
        measurements.push(Measurement::new(
            "bloom/insert",
            sample,
            config.records,
            config.records * 20,
            elapsed,
        ));
    }
    if config.includes("bloom/contains") {
        let mut found = Vec::with_capacity(records.len());
        let start = Instant::now();
        for record in &records {
            found.push(black_box(bloom.contains(black_box(&record.key))));
        }
        let elapsed = start.elapsed();
        require(
            found.into_iter().all(|found| found),
            "bloom contains mismatch",
        )?;
        measurements.push(Measurement::new(
            "bloom/contains",
            sample,
            config.records,
            config.records * 20,
            elapsed,
        ));
    }
    let indexes: Vec<_> = records
        .iter()
        .enumerate()
        .map(|(i, r)| SparseIndexEntry {
            first_key: r.key.clone(),
            last_key: r.key.clone(),
            block_offset: i as u64 * 4096,
            record_count: 1,
        })
        .collect();
    let mut encoded_index = Vec::with_capacity(indexes.len());
    let start = Instant::now();
    for index in &indexes {
        encoded_index.push(black_box(index).encode());
    }
    let elapsed = start.elapsed();
    for (index, bytes) in indexes.iter().zip(&encoded_index) {
        require(
            SparseIndexEntry::decode(Cursor::new(bytes))?.encode() == index.encode(),
            "index encode mismatch",
        )?;
    }
    if config.includes("index/encode") {
        measurements.push(Measurement::new(
            "index/encode",
            sample,
            config.records,
            config.records * 40,
            elapsed,
        ));
    }
    if config.includes("index/decode") {
        let mut decoded = Vec::with_capacity(indexes.len());
        let start = Instant::now();
        for bytes in &encoded_index {
            decoded.push(SparseIndexEntry::decode(Cursor::new(black_box(bytes)))?);
        }
        let elapsed = start.elapsed();
        for (decoded, expected) in decoded.iter().zip(&indexes) {
            require(
                decoded.encode() == expected.encode(),
                "index decode mismatch",
            )?;
        }
        measurements.push(Measurement::new(
            "index/decode",
            sample,
            config.records,
            config.records * 40,
            elapsed,
        ));
    }
    let blocks: Vec<_> = records
        .iter()
        .map(|r| {
            let record = MemtableRecord::new(r.value.clone(), RecordType::Put, r.timestamp)
                .with_key(r.key.clone());
            Block {
                offset: 0,
                first_key: r.key.clone(),
                last_key: r.key.clone(),
                record_count: 1,
                data_size: record.record_length(&r.key) as u32,
                data: Some(vec![record]),
            }
        })
        .collect();
    if config.includes("block/encode") || config.includes("block/decode") {
        let mut encoded_blocks = Vec::with_capacity(blocks.len());
        let started = Instant::now();
        for block in &blocks {
            encoded_blocks.push(black_box(block).encode());
        }
        let elapsed = started.elapsed();
        let mut decoded = Vec::with_capacity(blocks.len());
        for bytes in &encoded_blocks {
            decoded.push(Block::decode(&mut Cursor::new(bytes), 0)?);
        }
        validate_blocks(&decoded, config, 1)?;
        if config.includes("block/encode") {
            measurements.push(Measurement::new(
                "block/encode",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            ));
        }
        if config.includes("block/decode") {
            let mut decoded = Vec::with_capacity(blocks.len());
            let started = Instant::now();
            for bytes in &encoded_blocks {
                decoded.push(Block::decode(&mut Cursor::new(black_box(bytes)), 0)?);
            }
            let elapsed = started.elapsed();
            validate_blocks(&decoded, config, 1)?;
            measurements.push(Measurement::new(
                "block/decode",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            ));
        }
    }
    if config.includes("checksum/calculate") || config.includes("checksum/verify") {
        let mut checksums = Vec::with_capacity(records.len());
        let started = Instant::now();
        for record in &records {
            checksums.push(black_box(calculate_crc32(black_box(&record.value))));
        }
        let elapsed = started.elapsed();
        for (record, checksum) in records.iter().zip(&checksums) {
            verify_crc32(&record.value, *checksum)?;
        }
        if config.includes("checksum/calculate") {
            measurements.push(Measurement::new(
                "checksum/calculate",
                sample,
                config.records,
                config.records * config.value_bytes,
                elapsed,
            ));
        }
        if config.includes("checksum/verify") {
            let started = Instant::now();
            for (record, checksum) in records.iter().zip(&checksums) {
                black_box(verify_crc32(black_box(&record.value), black_box(*checksum)))?;
            }
            measurements.push(Measurement::new(
                "checksum/verify",
                sample,
                config.records,
                config.records * config.value_bytes,
                started.elapsed(),
            ));
        }
    }
    let codec = SSTableCodec::new(blocks, Vec::new(), bloom);
    let start = Instant::now();
    let (bytes, footer) = black_box(&codec).serialize();
    let elapsed = start.elapsed();
    let (decoded_footer, decoded_index, decoded_bloom) =
        SSTableCodec::deserialize_sections(&mut Cursor::new(&bytes)).await?;
    let decoded_blocks =
        SSTableCodec::get_all_blocks(&mut Cursor::new(&bytes), &decoded_footer, &decoded_index)
            .await?;
    validate_blocks(&decoded_blocks, config, 1)?;
    require(decoded_bloom == codec.bloom, "sstable bloom mismatch")?;
    if config.includes("sstable/serialize") {
        measurements.push(Measurement::new(
            "sstable/serialize",
            sample,
            1,
            config.logical_bytes(),
            elapsed,
        ));
    }
    if config.includes("sstable/deserialize_metadata") {
        let mut cursor = Cursor::new(&bytes);
        let start = Instant::now();
        let decoded = SSTableCodec::deserialize(&mut cursor).await?;
        let elapsed = start.elapsed();
        require(
            decoded.index.len() == codec.blocks.len() && decoded.bloom == codec.bloom,
            "sstable metadata mismatch",
        )?;
        for (actual, expected) in decoded.index.iter().zip(&decoded_index) {
            require(
                actual.encode() == expected.encode(),
                "sstable index mismatch",
            )?;
        }
        measurements.push(Measurement::new(
            "sstable/deserialize_metadata",
            sample,
            1,
            footer.bloom_block_end - footer.index_block_start + 64,
            elapsed,
        ));
    }
    let expected_footer = footer.encode();
    if config.includes("footer/encode") {
        let mut results = Vec::with_capacity(records.len());
        let start = Instant::now();
        for _ in 0..config.records {
            results.push(black_box(&footer).encode());
        }
        let elapsed = start.elapsed();
        require(
            results.iter().all(|bytes| *bytes == expected_footer),
            "footer encode mismatch",
        )?;
        measurements.push(Measurement::new(
            "footer/encode",
            sample,
            config.records,
            config.records * 64,
            elapsed,
        ));
    }
    if config.includes("footer/decode") {
        let mut results = Vec::with_capacity(records.len());
        let start = Instant::now();
        for _ in 0..config.records {
            results.push(SSTableFooter::decode(Cursor::new(black_box(
                &expected_footer,
            )))?);
        }
        let elapsed = start.elapsed();
        require(
            results
                .iter()
                .all(|footer| footer.encode() == expected_footer),
            "footer decode mismatch",
        )?;
        measurements.push(Measurement::new(
            "footer/decode",
            sample,
            config.records,
            config.records * 64,
            elapsed,
        ));
    }
    Ok(())
}

async fn wal(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if !["wal/durable_append", "wal/scan", "wal/rotate"]
        .iter()
        .any(|name| config.includes(name))
    {
        return Ok(());
    }
    let directory = config.temporary_directory()?;
    let db = KV2::open(directory.path(), storage_config(config)).await?;
    let wal = db.wal_manager();
    drop(db);
    let records = records(config);
    let mut lsns = Vec::with_capacity(records.len());
    let start = Instant::now();
    for r in &records {
        lsns.push(
            wal.write_log(&r.key, &r.value, r.timestamp, RecordType::Put)
                .await?,
        );
    }
    let elapsed = start.elapsed();
    require(
        lsns.iter().copied().eq(1..=config.records),
        "WAL append LSN mismatch",
    )?;
    let start_scan = Instant::now();
    let recovered = wal.recover_records().await?;
    let scan_elapsed = start_scan.elapsed();
    require(
        recovered.len() == records.len(),
        "WAL recovery count mismatch",
    )?;
    for (actual, expected) in recovered.iter().zip(&records) {
        require(
            actual.key == expected.key
                && actual.value == expected.value
                && actual.lsn == expected.timestamp
                && actual.record_type == RecordType::Put as u8,
            "WAL recovered data mismatch",
        )?;
    }
    if config.includes("wal/durable_append") {
        measurements.push(
            Measurement::new(
                "wal/durable_append",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            )
            .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    if config.includes("wal/scan") {
        measurements.push(
            Measurement::new("wal/scan", sample, 1, config.logical_bytes(), scan_elapsed)
                .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    if config.includes("wal/rotate") {
        let mut segments = Vec::with_capacity(records.len());
        let start = Instant::now();
        for _ in 0..config.records {
            segments.push(wal.rotate_wal_file().await?);
        }
        let elapsed = start.elapsed();
        require(
            segments.windows(2).all(|s| s[1] == s[0] + 1),
            "WAL rotation sequence mismatch",
        )?;
        let recovered = wal.recover_records().await?;
        require(
            recovered.len() == records.len(),
            "WAL rotation lost records",
        )?;
        for (actual, expected) in recovered.iter().zip(&records) {
            require(
                actual.key == expected.key
                    && actual.value == expected.value
                    && actual.lsn == expected.timestamp,
                "WAL rotation data mismatch",
            )?;
        }
        measurements.push(
            Measurement::new("wal/rotate", sample, config.records, 0, elapsed)
                .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    Ok(())
}

async fn manifest(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if !["manifest/register", "manifest/reopen"]
        .iter()
        .any(|name| config.includes(name))
    {
        return Ok(());
    }
    let directory = config.temporary_directory()?;
    let path = directory.path().join("MANIFEST");
    let manifest = ManifestManager::load_or_create(path.clone()).await?;
    let entries: Vec<_> = (0..config.records)
        .map(|i| SSTableMeta {
            file_id: i + 1,
            level: 0,
            path: directory
                .path()
                .join(format!("sstable/level-0/sstable-{}.dat", i + 1))
                .to_string_lossy()
                .into_owned(),
            record_count: 1,
            smallest_key: config.key(i),
            largest_key: config.key(i),
            bloom_offset: 0,
            bloom_size: 0,
        })
        .collect();
    let inputs = entries.clone();
    let start = Instant::now();
    for entry in inputs {
        manifest.register_sstable(entry).await?;
    }
    let elapsed = start.elapsed();
    let snapshot = manifest.snapshot().await;
    require(
        snapshot.levels.get(&0) == Some(&entries),
        "manifest registration mismatch",
    )?;
    if config.includes("manifest/register") {
        measurements.push(
            Measurement::new("manifest/register", sample, config.records, 0, elapsed)
                .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    drop(manifest);
    let start = Instant::now();
    let reopened = ManifestManager::load_or_create(path).await?;
    let elapsed = start.elapsed();
    require(
        reopened.snapshot().await == snapshot,
        "manifest reopen mismatch",
    )?;
    if config.includes("manifest/reopen") {
        measurements.push(
            Measurement::new("manifest/reopen", sample, 1, 0, elapsed)
                .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    Ok(())
}

async fn make_writer(
    path: &Path,
    config: StorageConfig,
) -> BenchResult<(WriteComponent, Arc<ManifestManager>)> {
    let db = KV2::open(path, config.clone()).await?;
    let wal = db.wal_manager();
    drop(db);
    let manifest = Arc::new(ManifestManager::load_or_create(path.join("MANIFEST")).await?);
    Ok((
        WriteComponent::new(
            path.join("sstable"),
            wal,
            manifest.clone(),
            0,
            Arc::new(config),
        ),
        manifest,
    ))
}

async fn read_blocks(path: &str) -> BenchResult<Vec<Block>> {
    let file = tokio::fs::File::open(path).await?;
    let mut reader = tokio::io::BufReader::new(file);
    let (footer, index, _) = SSTableCodec::deserialize_sections(&mut reader).await?;
    Ok(SSTableCodec::get_all_blocks(&mut reader, &footer, &index).await?)
}

fn validate_blocks(blocks: &[Block], config: &BenchConfig, first_lsn: u64) -> BenchResult<()> {
    let records: Vec<_> = blocks
        .iter()
        .flat_map(|b| b.data.iter().flatten())
        .collect();
    require(
        records.len() as u64 == config.records,
        "SSTable record count mismatch",
    )?;
    for (i, record) in records.iter().enumerate() {
        require(
            record.key == config.key(i as u64)
                && record.value == config.value(i as u64)
                && record.record_type == RecordType::Put
                && record.lsn == first_lsn + i as u64,
            "SSTable data mismatch",
        )?;
    }
    Ok(())
}

async fn writer(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if ![
        "writer/put",
        "writer/flush",
        "sstable/read_blocks",
        "writer/batch_1",
        "writer/batch_16",
        "writer/batch_64",
        "writer/batch_256",
        "writer/concurrent_1",
        "writer/concurrent_4",
        "writer/concurrent_16",
    ]
    .iter()
    .any(|name| config.includes(name))
    {
        return Ok(());
    }
    for writers in [1usize, 4, 16] {
        let scenario = match writers {
            1 => "writer/concurrent_1",
            4 => "writer/concurrent_4",
            _ => "writer/concurrent_16",
        };
        if !config.includes(scenario) {
            continue;
        }
        if config.logical_bytes() > 256 * 1024 * 1024 {
            return Err("concurrent writer fixture exceeds the 256 MiB input budget".into());
        }
        let directory = config.temporary_directory()?;
        let (writer, _) = make_writer(directory.path(), storage_config(config)).await?;
        let worker_inputs: Vec<Vec<_>> = (0..writers)
            .map(|worker| {
                let first = config.records * worker as u64 / writers as u64;
                let end = config.records * (worker + 1) as u64 / writers as u64;
                (first..end)
                    .map(|index| (config.key(index), config.value(index)))
                    .collect()
            })
            .collect();
        let operations = config.records;
        let started = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for work in worker_inputs {
            let writer = writer.clone();
            tasks.spawn(async move {
                for (key, value) in work {
                    writer.put(key, value).await?;
                }
                Ok::<_, ceredb::error::DBError>(())
            });
        }
        while let Some(result) = tasks.join_next().await {
            result??;
        }
        let elapsed = started.elapsed();
        let memtable = writer.active_memtable_handle();
        require(
            memtable.len() == operations as usize,
            "concurrent writer memtable length mismatch",
        )?;
        for index in 0..operations {
            let key = config.key(index);
            let actual = memtable
                .range((key.clone(), 0)..=(key, u64::MAX))
                .next()
                .ok_or("concurrent writer record missing")?;
            require(
                actual.value().value == config.value(index),
                "concurrent writer value mismatch",
            )?;
        }
        measurements.push(
            Measurement::new(
                scenario,
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            )
            .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    for batch_size in [1u64, 16, 64, 256] {
        let scenario = match batch_size {
            1 => "writer/batch_1",
            16 => "writer/batch_16",
            64 => "writer/batch_64",
            _ => "writer/batch_256",
        };
        if !config.includes(scenario) {
            continue;
        }
        let batch_dir = config.temporary_directory()?;
        let (batch_writer, _) = make_writer(batch_dir.path(), storage_config(config)).await?;
        let started = Instant::now();
        for start in (0..config.records).step_by(batch_size as usize) {
            let end = (start + batch_size).min(config.records);
            let operations = (start..end)
                .map(|index| BatchOperation::Put {
                    key: config.key(index),
                    value: config.value(index),
                })
                .collect();
            batch_writer.write_batch(operations).await?;
        }
        let elapsed = started.elapsed();
        let memtable = batch_writer.active_memtable_handle();
        require(
            memtable.len() == config.records as usize,
            "writer batch memtable length mismatch",
        )?;
        for index in 0..config.records {
            let key = config.key(index);
            let record = memtable
                .get(&(key, index + 1))
                .ok_or("writer batch record missing")?;
            require(
                record.value().value == config.value(index),
                "writer batch value mismatch",
            )?;
        }
        measurements.push(
            Measurement::new(
                scenario,
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            )
            .with_disk(DiskUsage::inspect(batch_dir.path())?),
        );
    }
    if !["writer/put", "writer/flush", "sstable/read_blocks"]
        .iter()
        .any(|name| config.includes(name))
    {
        return Ok(());
    }
    let directory = config.temporary_directory()?;
    let (mut writer, manifest) = make_writer(directory.path(), storage_config(config)).await?;
    let records = records(config);
    let inputs = records.clone();
    let start = Instant::now();
    for record in inputs {
        writer.put(record.key, record.value).await?;
    }
    let elapsed = start.elapsed();
    let memtable = writer.active_memtable_handle();
    require(
        memtable.len() == records.len(),
        "writer memtable length mismatch",
    )?;
    for (actual, expected) in memtable.iter().zip(&records) {
        require(
            actual.key().0 == expected.key
                && actual.key().1 == expected.timestamp
                && actual.value().value == expected.value
                && actual.value().record_type == RecordType::Put,
            "writer put mismatch",
        )?;
    }
    if config.includes("writer/put") {
        measurements.push(
            Measurement::new(
                "writer/put",
                sample,
                config.records,
                config.logical_bytes(),
                elapsed,
            )
            .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    let start = Instant::now();
    let flushed = writer.flush_active_memtable().await?;
    let elapsed = start.elapsed();
    require(
        flushed.record_count as u64 == config.records
            && writer.memtable_size_bytes() == 0
            && writer.active_memtable_handle().is_empty(),
        "writer flush mismatch",
    )?;
    require(
        manifest
            .snapshot()
            .await
            .levels
            .get(&0)
            .is_some_and(|files| files.len() == 1 && files[0].file_id == flushed.file_id),
        "flush manifest mismatch",
    )?;
    validate_blocks(&read_blocks(&flushed.sstable_path).await?, config, 1)?;
    if config.includes("writer/flush") {
        measurements.push(
            Measurement::new("writer/flush", sample, 1, config.logical_bytes(), elapsed)
                .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    if config.includes("sstable/read_blocks") {
        let file = tokio::fs::File::open(&flushed.sstable_path).await?;
        let mut reader = tokio::io::BufReader::new(file);
        let (footer, index, _) = SSTableCodec::deserialize_sections(&mut reader).await?;
        let start = Instant::now();
        let blocks = SSTableCodec::get_all_blocks(&mut reader, &footer, &index).await?;
        let elapsed = start.elapsed();
        validate_blocks(&blocks, config, 1)?;
        measurements.push(
            Measurement::new(
                "sstable/read_blocks",
                sample,
                1,
                config.logical_bytes(),
                elapsed,
            )
            .with_disk(DiskUsage::inspect(directory.path())?),
        );
    }
    Ok(())
}

async fn compact(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult<()> {
    if !config.includes("compaction/merge_l0") {
        return Ok(());
    }
    let directory = config.temporary_directory()?;
    let storage = StorageConfig {
        memtable_size_threshold: config.flush_bytes.clamp(1024 * 1024, 64 * 1024 * 1024),
        wal_segment_size: config.wal_segment_bytes.clamp(256 * 1024, 10 * 1024 * 1024),
        ..StorageConfig::default()
    };
    storage.validate()?;
    let (mut writer, manifest) = make_writer(directory.path(), storage.clone()).await?;
    for _ in 0..2 {
        for i in 0..config.records {
            writer.put(config.key(i), config.value(i)).await?;
        }
        let flushed = writer.flush_active_memtable().await?;
        require(
            flushed.record_count as u64 == config.records,
            "compaction setup flush mismatch",
        )?;
    }
    require(
        manifest
            .snapshot()
            .await
            .levels
            .get(&0)
            .is_some_and(|files| files.len() == 2),
        "compaction requires two L0 files",
    )?;
    let cancel = CancellationToken::new();
    let storage = Arc::new(storage);
    let manifest_input = manifest.clone();
    let start = Instant::now();
    let merged = compaction(manifest_input, 0, storage, cancel).await?;
    let elapsed = start.elapsed();
    validate_blocks(&merged.blocks, config, config.records + 1)?;
    let snapshot = manifest.snapshot().await;
    require(
        snapshot.levels.get(&0).is_none_or(|files| files.is_empty()),
        "compaction left L0 files",
    )?;
    let files = snapshot.levels.get(&1).ok_or("compaction missing L1")?;
    require(files.len() == 1, "compaction L1 count mismatch")?;
    validate_blocks(
        &read_blocks(&files[0].path).await?,
        config,
        config.records + 1,
    )?;
    measurements.push(
        Measurement::new(
            "compaction/merge_l0",
            sample,
            1,
            config.logical_bytes().saturating_mul(2),
            elapsed,
        )
        .with_disk(DiskUsage::inspect(directory.path())?),
    );
    Ok(())
}
