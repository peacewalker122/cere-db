use std::{hint::black_box, io::Cursor, path::Path, sync::Arc, time::Instant};

use ceredb::{
    KV2, StorageConfig,
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
