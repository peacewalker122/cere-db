use ceredb::{
    KV2, StorageConfig,
    api::api::{AsyncKVEngine, BatchOperation},
};
use rocksdb::{DB, Options, WriteBatch, WriteOptions};
use serde::Serialize;
use std::{
    env, fs,
    path::Path,
    time::{Duration, Instant},
};
use tempfile::TempDir;

const VALUE_BYTES: usize = 256;
const RECORDS: usize = 1024;
const SAMPLES: usize = 3;
const WARMUPS: usize = 1;
const FLUSH_BYTES: u64 = 1024 * 1024;
const BATCHES: [usize; 4] = [1, 16, 64, 256];

#[derive(Serialize)]
struct Sample {
    engine: &'static str,
    batch_size: usize,
    ordinal: usize,
    mutations: usize,
    logical_bytes: usize,
    elapsed_ns: u128,
    ops_per_second: f64,
}

#[derive(Serialize)]
struct Output {
    records: usize,
    key_bytes: usize,
    value_bytes: usize,
    samples: usize,
    warmups: usize,
    wal: &'static str,
    sync: bool,
    compression: &'static str,
    flush_threshold_bytes: u64,
    results: Vec<Sample>,
}

fn key(i: usize) -> Vec<u8> {
    format!("key-{i:016}").into_bytes()
}
fn value(i: usize) -> Vec<u8> {
    (0..VALUE_BYTES)
        .map(|offset| ((i * 31 + offset * 17) % 251) as u8)
        .collect()
}
fn entries() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..RECORDS).map(|i| (key(i), value(i))).collect()
}

fn verify_rocks(db: &DB, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
    for (key, expected) in data {
        anyhow::ensure!(
            db.get(key)?.as_deref() == Some(expected.as_slice()),
            "RocksDB mismatch at {:?}",
            key
        );
    }
    Ok(())
}

async fn verify_kv(db: &KV2, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<()> {
    for (key, expected) in data {
        anyhow::ensure!(
            db.get(key).await?.as_deref() == Some(expected),
            "KV2 mismatch at {:?}",
            key
        );
    }
    Ok(())
}

fn record(
    results: &mut Vec<Sample>,
    engine: &'static str,
    batch_size: usize,
    ordinal: usize,
    elapsed: Duration,
) {
    let logical_bytes = RECORDS * (20 + VALUE_BYTES);
    results.push(Sample {
        engine,
        batch_size,
        ordinal,
        mutations: RECORDS,
        logical_bytes,
        elapsed_ns: elapsed.as_nanos(),
        ops_per_second: RECORDS as f64 / elapsed.as_secs_f64(),
    });
}

async fn run_kv(
    path: &Path,
    batch: usize,
    data: &[(Vec<u8>, Vec<u8>)],
) -> anyhow::Result<Duration> {
    let mut config = StorageConfig::default();
    config.memtable_size_threshold = FLUSH_BYTES;
    config.wal_segment_size = FLUSH_BYTES;
    let mut db = KV2::open(path, config).await?;
    let start = Instant::now();
    for group in data.chunks(batch) {
        if batch == 1 {
            let (key, value) = &group[0];
            db.put(key.clone(), value.clone()).await?;
        } else {
            db.write_batch(
                group
                    .iter()
                    .map(|(key, value)| BatchOperation::Put {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect(),
            )
            .await?;
        }
    }
    let elapsed = start.elapsed();
    verify_kv(&db, data).await?;
    drop(db);
    let db = KV2::open(path, {
        let mut c = StorageConfig::default();
        c.memtable_size_threshold = FLUSH_BYTES;
        c.wal_segment_size = FLUSH_BYTES;
        c
    })
    .await?;
    verify_kv(&db, data).await?;
    drop(db);
    Ok(elapsed)
}

fn run_rocks(path: &Path, batch: usize, data: &[(Vec<u8>, Vec<u8>)]) -> anyhow::Result<Duration> {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_compression_type(rocksdb::DBCompressionType::None);
    options.set_write_buffer_size(FLUSH_BYTES as usize);
    let db = DB::open(&options, path)?;
    let mut write_options = WriteOptions::default();
    write_options.set_sync(true);
    write_options.disable_wal(false);
    let start = Instant::now();
    for group in data.chunks(batch) {
        if batch == 1 {
            db.put_opt(&group[0].0, &group[0].1, &write_options)?;
        } else {
            let mut writes = WriteBatch::default();
            for (key, value) in group {
                writes.put(key, value);
            }
            db.write_opt(writes, &write_options)?;
        }
    }
    let elapsed = start.elapsed();
    verify_rocks(&db, data)?;
    drop(db);
    let db = DB::open(&options, path)?;
    verify_rocks(&db, data)?;
    Ok(elapsed)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let root = env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .ok_or_else(|| anyhow::anyhow!("usage: compare <archive-work-dir>"))?;
    fs::create_dir_all(&root)?;
    let data = entries();
    let mut results = Vec::new();
    // Paired, alternating engine order by batch size and ordinal to limit drift.
    for batch in BATCHES {
        for ordinal in 0..(SAMPLES + WARMUPS) {
            let order = if (batch + ordinal) % 2 == 0 {
                ["CereDB", "RocksDB"]
            } else {
                ["RocksDB", "CereDB"]
            };
            for engine in order {
                let temp = TempDir::new_in(&root)?;
                let elapsed = if engine == "CereDB" {
                    run_kv(&temp.path().join("db"), batch, &data).await?
                } else {
                    run_rocks(&temp.path().join("db"), batch, &data)?
                };
                if ordinal >= WARMUPS {
                    record(&mut results, engine, batch, ordinal - WARMUPS + 1, elapsed);
                }
            }
        }
    }
    let output = Output {
        records: RECORDS,
        key_bytes: 20,
        value_bytes: VALUE_BYTES,
        samples: SAMPLES,
        warmups: WARMUPS,
        wal: "enabled",
        sync: true,
        compression: "disabled on both (CereDB has no compression)",
        flush_threshold_bytes: FLUSH_BYTES,
        results,
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
