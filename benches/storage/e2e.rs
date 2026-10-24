use std::{hint::black_box, path::Path, time::Instant};

use ceredb::{KV2, StorageConfig, api::api::AsyncKVEngine};

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
