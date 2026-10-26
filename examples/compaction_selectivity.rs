use ceredb::storage::{
    compactionmanager::compaction::compaction, manifest_codec::ManifestManager,
    sstable_codec::SSTableCodec, writemanager::write::WriteComponent,
};
use ceredb::{BatchOperation, KV2, StorageConfig};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

const KEYS: usize = 2048;
const VALUE_BYTES: usize = 256;
const TARGET_SSTABLE_BYTES: u64 = 32 * 1024;
const PATCH_START: usize = 960;
const PATCH_END: usize = 992;

struct Fixture {
    _writer: WriteComponent,
    manifest: Arc<ManifestManager>,
    config: Arc<StorageConfig>,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    deletes: Vec<Vec<u8>>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let root = PathBuf::from(format!("target/bench-data/compaction-selectivity-{run_id}"));
    fs::create_dir_all(&root)?;
    let mut samples = Vec::new();
    for sample in 0..4 {
        let fixture = create_fixture(root.join(format!("sample-{sample}"))).await?;
        let baseline = fixture.manifest.snapshot().await;
        let old_level1 = baseline
            .levels
            .get(&1)
            .ok_or("baseline L1 missing")?
            .clone();
        require(
            old_level1.len() >= 8,
            "baseline L1 has fewer than eight partitions",
        )?;
        let before = file_fingerprints(old_level1.iter()).await?;
        let old_l0 = baseline.levels.get(&0).ok_or("baseline L0 missing")?;
        let old_l0_bytes = file_fingerprints(old_l0.iter()).await?;
        let candidate_bytes = old_l0_bytes
            .values()
            .chain(before.values())
            .map(|(_, bytes, _)| *bytes)
            .sum::<u64>();
        let started = Instant::now();
        compaction(
            fixture.manifest.clone(),
            0,
            fixture.config.clone(),
            CancellationToken::new(),
        )
        .await?;
        let elapsed = started.elapsed();
        let after = fixture.manifest.snapshot().await;
        let old_ids: Vec<_> = old_level1.iter().map(|file| file.file_id).collect();
        let current_l1 = after.levels.get(&1).ok_or("compaction missing L1")?;
        let removed: Vec<_> = old_level1
            .iter()
            .filter(|file| !current_l1.iter().any(|new| new.file_id == file.file_id))
            .collect();
        let untouched: Vec<_> = old_level1
            .iter()
            .filter(|file| current_l1.iter().any(|new| new.file_id == file.file_id))
            .collect();
        require(!removed.is_empty(), "compaction selected no prior L1 files")?;
        require(
            removed.len() < old_level1.len(),
            "compaction removed all prior L1 files",
        )?;
        require(!untouched.is_empty(), "no untouched L1 files remain")?;
        let untouched_bytes = file_fingerprints(untouched.iter().copied()).await?;
        for file in &untouched {
            require(
                before.get(&file.file_id) == untouched_bytes.get(&file.file_id),
                "untouched L1 file path, length, or contents changed",
            )?;
        }
        let removed_l0: Vec<_> = old_l0
            .iter()
            .filter(|file| {
                !after
                    .levels
                    .get(&0)
                    .is_some_and(|files| files.iter().any(|new| new.file_id == file.file_id))
            })
            .collect();
        require(
            removed_l0.len() == old_l0.len(),
            "expected L0 inputs remain after compaction",
        )?;
        let removed_ids: std::collections::HashSet<_> = removed
            .iter()
            .chain(removed_l0.iter())
            .map(|file| file.file_id)
            .collect();
        let added_outputs: Vec<_> = after
            .levels
            .get(&1)
            .into_iter()
            .flatten()
            .filter(|file| !old_ids.contains(&file.file_id))
            .collect();
        require(
            !added_outputs.is_empty(),
            "manifest contains no new L1 output",
        )?;
        let mut ordered_l1: Vec<_> = current_l1.iter().collect();
        ordered_l1.sort_by(|left, right| left.smallest_key.cmp(&right.smallest_key));
        require(
            ordered_l1
                .windows(2)
                .all(|pair| pair[0].largest_key < pair[1].smallest_key),
            "L1 output ranges overlap",
        )?;
        verify_values(&after.levels, &fixture.expected, &fixture.deletes).await?;
        let selected_input_bytes = removed_l0
            .iter()
            .map(|file| old_l0_bytes[&file.file_id].1)
            .sum::<u64>()
            + removed
                .iter()
                .map(|file| before[&file.file_id].1)
                .sum::<u64>();
        let output_bytes = file_bytes(added_outputs.iter().map(|file| file.path.as_str())).await?;
        let untouched_total = file_bytes(untouched.iter().map(|file| file.path.as_str())).await?;
        samples.push(json!({
            "sample": sample,
            "kind": if sample == 0 { "warmup" } else { "measured" },
            "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
            "baseline_l0_files": old_l0.len(),
            "baseline_l1_files": old_level1.len(),
            "removed_l0_files": removed_l0.len(),
            "removed_l1_files": removed.len(),
            "untouched_l1_files": untouched.len(),
            "selected_input_bytes": selected_input_bytes,
            "output_bytes": output_bytes,
            "untouched_l1_bytes": untouched_total,
            "all_candidate_bytes": candidate_bytes,
            "selected_fraction_of_candidate_bytes": selected_input_bytes as f64 / candidate_bytes as f64,
            "untouched_files_byte_identical": true,
            "removed_input_file_ids": removed_ids,
            "new_output_file_ids": added_outputs.iter().map(|file| file.file_id).collect::<Vec<_>>(),
        }));
    }
    let mut times: Vec<f64> = samples
        .iter()
        .skip(1)
        .map(|sample| sample["elapsed_ms"].as_f64().unwrap())
        .collect();
    times.sort_by(f64::total_cmp);
    let result = json!({
        "benchmark": "selective_l0_to_l1_compaction",
        "status": "COMPLETE",
        "fixture": {"keys": KEYS, "value_bytes": VALUE_BYTES, "target_sstable_bytes": TARGET_SSTABLE_BYTES, "overwrite_range": [PATCH_START, PATCH_END], "deletes": 8},
        "runs": 4,
        "warmups": 1,
        "measured_samples": 3,
        "median_elapsed_ms": times[1],
        "counterfactual_note": "selected_fraction_of_candidate_bytes is a whole-level byte-fraction comparison, not a measured speedup",
        "amplification_note": "no lifetime write-amplification claim is made",
        "samples": samples,
    });
    fs::write(
        root.join("results.json"),
        serde_json::to_vec_pretty(&result)?,
    )?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn create_fixture(path: PathBuf) -> Result<Fixture, Box<dyn std::error::Error>> {
    fs::create_dir_all(&path)?;
    let config = Arc::new(StorageConfig {
        memtable_size_threshold: 16 * 1024 * 1024,
        target_sstable_bytes: TARGET_SSTABLE_BYTES,
        level_base_bytes: 1024 * 1024 * 1024,
        max_level0_files: 10,
        ..StorageConfig::default()
    });
    config.validate()?;
    let db = KV2::open(&path, (*config).clone()).await?;
    let wal = db.wal_manager();
    drop(db);
    let manifest = Arc::new(ManifestManager::load_or_create(path.join("MANIFEST")).await?);
    let mut writer = WriteComponent::new(
        path.join("sstable"),
        wal,
        manifest.clone(),
        0,
        config.clone(),
    );
    let mut expected = BTreeMap::new();
    let mut batch = Vec::new();
    for number in 0..KEYS {
        let key = key(number);
        let value = baseline_value(number);
        expected.insert(key.clone(), value.clone());
        batch.push(BatchOperation::Put { key, value });
        if batch.len() == 128 {
            writer.write_batch(std::mem::take(&mut batch)).await?;
        }
    }
    if !batch.is_empty() {
        writer.write_batch(batch).await?;
    }
    writer.flush_active_memtable().await?;
    compaction(
        manifest.clone(),
        0,
        config.clone(),
        CancellationToken::new(),
    )
    .await?;
    let setup = manifest.snapshot().await;
    require(
        setup.levels.get(&1).is_some_and(|files| files.len() >= 8),
        "setup did not create at least eight L1 partitions",
    )?;
    let mut mutations = Vec::new();
    let mut deletes = Vec::new();
    for number in PATCH_START..PATCH_END {
        let key = key(number);
        if number % 4 == 0 {
            mutations.push(BatchOperation::Delete { key: key.clone() });
            expected.remove(&key);
            deletes.push(key);
        } else {
            let value = vec![0xD7; VALUE_BYTES];
            mutations.push(BatchOperation::Put {
                key: key.clone(),
                value: value.clone(),
            });
            expected.insert(key, value);
        }
    }
    writer.write_batch(mutations).await?;
    writer.flush_active_memtable().await?;
    Ok(Fixture {
        _writer: writer,
        manifest,
        config,
        expected,
        deletes,
    })
}

async fn verify_values(
    levels: &HashMap<u32, Vec<ceredb::storage::manifest_codec::SSTableMeta>>,
    expected: &BTreeMap<Vec<u8>, Vec<u8>>,
    deletes: &[Vec<u8>],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut latest = BTreeMap::<Vec<u8>, (u64, Option<Vec<u8>>)>::new();
    for file in levels.values().flatten() {
        let input = tokio::fs::File::open(&file.path).await?;
        let mut reader = tokio::io::BufReader::new(input);
        let (footer, index, _) = SSTableCodec::deserialize_sections(&mut reader).await?;
        let blocks = SSTableCodec::get_all_blocks(&mut reader, &footer, &index).await?;
        for record in blocks.iter().flat_map(|block| block.data.iter().flatten()) {
            let value = (record.record_type != ceredb::storage::record::RecordType::Delete)
                .then(|| record.value.clone());
            if latest
                .get(&record.key)
                .is_none_or(|(lsn, _)| record.lsn > *lsn)
            {
                latest.insert(record.key.clone(), (record.lsn, value));
            }
        }
    }
    let actual: BTreeMap<_, _> = latest
        .into_iter()
        .filter_map(|(key, (_, value))| value.map(|value| (key, value)))
        .collect();
    require(
        &actual == expected,
        "decoded SSTable values differ from expected live key map",
    )?;
    for key in deletes {
        require(!actual.contains_key(key), "deleted key is still live")?;
    }
    Ok(())
}

async fn file_fingerprints<'a>(
    files: impl Iterator<Item = &'a ceredb::storage::manifest_codec::SSTableMeta>,
) -> Result<HashMap<u64, (String, u64, u64)>, Box<dyn std::error::Error>> {
    let mut result = HashMap::new();
    for file in files {
        let contents = tokio::fs::read(&file.path).await?;
        let hash = contents.iter().fold(0xcbf29ce484222325u64, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
        });
        result.insert(
            file.file_id,
            (file.path.clone(), contents.len() as u64, hash),
        );
    }
    Ok(result)
}

async fn file_bytes<'a>(
    paths: impl Iterator<Item = &'a str>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let mut total = 0;
    for path in paths {
        total += tokio::fs::metadata(path).await?.len();
    }
    Ok(total)
}

fn key(number: usize) -> Vec<u8> {
    format!("key-{number:08}").into_bytes()
}

fn baseline_value(number: usize) -> Vec<u8> {
    vec![(number % 251) as u8; VALUE_BYTES]
}

fn require(condition: bool, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
