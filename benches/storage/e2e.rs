use std::{hint::black_box, path::Path, time::Instant};

use ceredb::{
    KV2, StorageConfig,
    api::api::{AsyncKVEngine, BatchOperation},
    command::{execute_command_async, parse_command},
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
