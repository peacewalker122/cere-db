use ceredb::api::api::AsyncKVEngine;
use ceredb::error::DBError;
use ceredb::storage::kv2::KV2;
use ceredb::storage::manifest_codec::ManifestManager;
use ceredb::{
    BatchOperation, Bm25Config, CleanStats, DocumentId, ExperimentalEngine, FtsIndex, FtsQuery,
    StorageConfig,
};
use clap::{Parser, ValueEnum};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const KEY_BYTES: usize = 24;
const MAX_RECORDS: usize = 32_768;
const MAX_INPUT_BYTES: u64 = 32 * 1024 * 1024;
const DEFAULT_SEED: u64 = 0x6A09_E667_F3BC_C909;
const POINT_READ_LIMIT: usize = 64;
const VERIFY_POINT_PROBES: usize = 16;
const SCAN_READ_LIMIT: usize = 128;
const FTS_DOCUMENTS: usize = 128;
const FTS_DOCUMENT_CHUNK: usize = 64;
const PILOT_RECORDS: usize = 32;
const PILOT_VALUE_BYTES: usize = 4096;
const PILOT_BATCH_SIZE: usize = 8;
const PILOT_FTS_DOCUMENTS: usize = 16;
const PILOT_FTS_DOCUMENT_CHUNK: usize = 8;
const METADATA_FILES: [&str; 9] = [
    "MANIFEST",
    "CURRENT",
    "CURRENT.tmp",
    "LOCK",
    "SEAL",
    "SEAL.tmp",
    "NEXT-GENERATION",
    "NEXT-GENERATION.tmp",
    "EXPERIMENTAL-WAL-VALUES",
];
const TRANSIENT_FILESYSTEMS: [&str; 3] = ["tmpfs", "ramfs", "devtmpfs"];
const FTS_VOCABULARY: [&str; 16] = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
    "lambda", "mu", "nu", "xi", "omicron", "pi",
];
const FTS_COMMON_TERM: &str = "common";
const FTS_RARE_TERMS: [&str; 2] = ["raretoken", "violet"];
const RUN_OWNER_FILE: &str = ".harness-run-owner";

#[derive(Debug, thiserror::Error)]
enum HarnessError {
    #[error("storage error: {0}")]
    Storage(#[from] DBError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("configuration error: {0}")]
    Configuration(String),
    #[error("verification failed: {0}")]
    Verification(String),
    #[error("pilot check failed: {0}")]
    Pilot(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ProfileArg {
    SmallTables,
    DefaultBuffer,
}

impl ProfileArg {
    fn label(self) -> &'static str {
        match self {
            Self::SmallTables => "small-tables",
            Self::DefaultBuffer => "default-buffer",
        }
    }

    fn config(self) -> Result<StorageConfig, HarnessError> {
        match self {
            Self::SmallTables => Ok(StorageConfig::builder()
                .memtable_size_threshold(1024 * 1024)
                .sstable_block_size(4096)
                .max_level0_files(2)
                .target_sstable_bytes(16 * 1024)
                .level_base_bytes(64 * 1024)
                .level_size_multiplier(4)
                .wal_segment_size(256 * 1024)
                .build()?),
            Self::DefaultBuffer => Ok(StorageConfig::default()),
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    about = "Matched KV2 versus experimental WAL-value storage comparison",
    long_about = "Measures the current KV2 LSM storage and the experimental WAL-value engine with a deterministic append, overwrite, and delete workload on the same storage configuration."
)]
struct Cli {
    #[arg(long, value_enum, default_value = "small-tables")]
    profile: ProfileArg,
    #[arg(long, default_value_t = 3)]
    repetitions: usize,
    #[arg(long, default_value_t = 1)]
    warmups: usize,
    #[arg(long, default_value_t = DEFAULT_SEED)]
    seed: u64,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, value_parser = parse_records)]
    records: Option<usize>,
    #[arg(long, value_parser = parse_usize_list)]
    value_bytes: Option<Vec<usize>>,
    #[arg(long, value_parser = parse_usize_list)]
    batch_sizes: Option<Vec<usize>>,
    #[arg(long, default_value_t = 25)]
    overwrite_percent: usize,
    #[arg(long, default_value_t = 10)]
    delete_percent: usize,
    #[arg(long, default_value_t = 50)]
    reinsert_percent: usize,
    #[arg(long)]
    skip_reads: bool,
    #[arg(long)]
    skip_fts: bool,
    #[arg(long)]
    pilot: bool,
}

fn parse_records(input: &str) -> Result<usize, String> {
    let records = input
        .parse::<usize>()
        .map_err(|error| format!("invalid record count: {error}"))?;
    if records == 0 || records > MAX_RECORDS {
        return Err(format!("records must be in 1..={MAX_RECORDS}"));
    }
    Ok(records)
}

fn parse_usize_list(input: &str) -> Result<Vec<usize>, String> {
    let values = input
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<usize>()
                .map_err(|error| format!("invalid number '{item}': {error}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err("at least one number is required".into());
    }
    Ok(values)
}

#[derive(Debug, Clone)]
struct CaseSpec {
    name: String,
    value_bytes: usize,
    records: usize,
    batch_sizes: Vec<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EngineKind {
    Reference,
    Experimental,
}

impl EngineKind {
    fn label(self) -> &'static str {
        match self {
            Self::Reference => "reference-kv2",
            Self::Experimental => "experimental-wal",
        }
    }
}

struct Prng {
    state: u64,
}

impl Prng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn fill(&mut self, buffer: &mut [u8]) {
        for chunk in buffer.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

fn key_for(index: usize, seed: u64) -> Vec<u8> {
    let mut key = vec![0u8; KEY_BYTES];
    key[..8].copy_from_slice(&(index as u64).to_be_bytes());
    Prng::new(seed ^ (index as u64).wrapping_mul(0xD6E8_FEB8_6659_FD93)).fill(&mut key[8..]);
    key
}

fn value_for(index: usize, round: u64, value_bytes: usize, seed: u64) -> Vec<u8> {
    let mut value = vec![0u8; value_bytes];
    Prng::new(seed ^ (index as u64).wrapping_mul(0xA076_1D64_78BD_642F) ^ round.rotate_left(32))
        .fill(&mut value);
    value
}

fn shuffled_indices(count: usize, seed: u64) -> Vec<usize> {
    let mut indices: Vec<usize> = (0..count).collect();
    let mut prng = Prng::new(seed ^ 0x51ED_270F_1A2B_3C4D);
    for position in (1..indices.len()).rev() {
        let swap_with = (prng.next_u64() % (position as u64 + 1)) as usize;
        indices.swap(position, swap_with);
    }
    indices
}

fn estimate_input_bytes(
    records: usize,
    value_bytes: usize,
    overwrite_percent: usize,
    delete_percent: usize,
    reinsert_percent: usize,
) -> u64 {
    let key = KEY_BYTES as u128;
    let value = value_bytes as u128;
    let records = records as u128;
    let overwrites = records * overwrite_percent as u128 / 100;
    let deletes = records * delete_percent as u128 / 100;
    let reinserts = deletes * reinsert_percent as u128 / 100;
    let total = records * (key + value)
        + overwrites * (key + value)
        + deletes * key
        + reinserts * (key + value);
    u64::try_from(total).unwrap_or(u64::MAX)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MutationSelection {
    overwrite_count: usize,
    delete_count: usize,
    reinsert_count: usize,
}

fn percent_of(records: usize, percent: usize) -> Option<usize> {
    records.checked_mul(percent)?.checked_div(100)
}

fn mutation_selection(
    records: usize,
    overwrite_percent: usize,
    delete_percent: usize,
    reinsert_percent: usize,
) -> Result<MutationSelection, HarnessError> {
    if overwrite_percent > 100 || delete_percent > 100 || reinsert_percent > 100 {
        return Err(HarnessError::Configuration(
            "mutation percentages must be in 0..=100".into(),
        ));
    }
    let overflow =
        || HarnessError::Configuration("mutation selection overflows the record count".into());
    let overwrite_count = percent_of(records, overwrite_percent).ok_or_else(overflow)?;
    let delete_count = percent_of(records, delete_percent).ok_or_else(overflow)?;
    let selected = overwrite_count
        .checked_add(delete_count)
        .ok_or_else(overflow)?;
    if selected > records {
        return Err(HarnessError::Configuration(format!(
            "overwrite and delete selections need {selected} records but only {records} exist; overwrite percent plus delete percent must not exceed 100"
        )));
    }
    let reinsert_count = percent_of(delete_count, reinsert_percent).ok_or_else(overflow)?;
    Ok(MutationSelection {
        overwrite_count,
        delete_count,
        reinsert_count,
    })
}

fn chunk_ops(operations: Vec<BatchOperation>, batch_size: usize) -> Vec<Vec<BatchOperation>> {
    let mut batches = Vec::new();
    let mut current = Vec::with_capacity(batch_size);
    for operation in operations {
        current.push(operation);
        if current.len() == batch_size {
            batches.push(std::mem::take(&mut current));
            current.reserve(batch_size);
        }
    }
    if !current.is_empty() {
        batches.push(current);
    }
    batches
}

struct WorkloadParams {
    records: usize,
    value_bytes: usize,
    batch_size: usize,
    seed: u64,
    overwrite_percent: usize,
    delete_percent: usize,
    reinsert_percent: usize,
}

#[derive(Debug, Clone)]
struct WorkloadSummary {
    mutations: usize,
    batches: usize,
    append_mutations: usize,
    overwrite_mutations: usize,
    delete_mutations: usize,
    reinsert_mutations: usize,
    logical_input_bytes: u64,
    live_records: usize,
    live_logical_bytes: u64,
    live_value_bytes: u64,
}

struct Workload {
    append: Vec<Vec<BatchOperation>>,
    mutate: Vec<Vec<BatchOperation>>,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    deleted_probe: Option<Vec<u8>>,
    absent_probe: Vec<u8>,
    summary: WorkloadSummary,
}

impl Workload {
    fn build(params: &WorkloadParams) -> Result<Self, HarnessError> {
        if params.records == 0 || params.records > MAX_RECORDS {
            return Err(HarnessError::Configuration(format!(
                "records must be in 1..={MAX_RECORDS}"
            )));
        }
        if params.value_bytes == 0 || params.batch_size == 0 {
            return Err(HarnessError::Configuration(
                "value bytes and batch size must be positive".into(),
            ));
        }
        let selection = mutation_selection(
            params.records,
            params.overwrite_percent,
            params.delete_percent,
            params.reinsert_percent,
        )?;
        let estimated = estimate_input_bytes(
            params.records,
            params.value_bytes,
            params.overwrite_percent,
            params.delete_percent,
            params.reinsert_percent,
        );
        if estimated > MAX_INPUT_BYTES {
            return Err(HarnessError::Configuration(format!(
                "estimated logical input of {estimated} bytes exceeds the {MAX_INPUT_BYTES} byte bound"
            )));
        }

        let mut expected = BTreeMap::new();
        let mut append = Vec::with_capacity(params.records);
        for index in 0..params.records {
            let key = key_for(index, params.seed);
            let value = value_for(index, 0, params.value_bytes, params.seed);
            append.push(BatchOperation::Put {
                key: key.clone(),
                value: value.clone(),
            });
            expected.insert(key, value);
        }

        let order = shuffled_indices(params.records, params.seed);
        let mut mutate = Vec::new();

        for &index in &order[..selection.overwrite_count] {
            let key = key_for(index, params.seed);
            let value = value_for(index, 1, params.value_bytes, params.seed);
            mutate.push(BatchOperation::Put {
                key: key.clone(),
                value: value.clone(),
            });
            expected.insert(key, value);
        }

        let deleted_keys =
            &order[selection.overwrite_count..selection.overwrite_count + selection.delete_count];
        for &index in deleted_keys {
            let key = key_for(index, params.seed);
            mutate.push(BatchOperation::Delete { key: key.clone() });
            expected.remove(&key);
        }

        for &index in &deleted_keys[..selection.reinsert_count] {
            let key = key_for(index, params.seed);
            let value = value_for(index, 2, params.value_bytes, params.seed);
            mutate.push(BatchOperation::Put {
                key: key.clone(),
                value: value.clone(),
            });
            expected.insert(key, value);
        }

        let mut logical_input_bytes = 0u64;
        for operation in append.iter().chain(mutate.iter()) {
            logical_input_bytes += match operation {
                BatchOperation::Put { key, value } => (key.len() + value.len()) as u64,
                BatchOperation::Delete { key } => key.len() as u64,
            };
        }
        if logical_input_bytes > MAX_INPUT_BYTES {
            return Err(HarnessError::Configuration(format!(
                "logical input of {logical_input_bytes} bytes exceeds the {MAX_INPUT_BYTES} byte bound"
            )));
        }

        let absent_probe = key_for(params.records + 12_345, params.seed);

        let live_logical_bytes = expected
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum::<u64>();
        let live_value_bytes = expected.values().map(|value| value.len() as u64).sum();
        let append_batches = chunk_ops(append, params.batch_size);
        let mutate_batches = chunk_ops(mutate, params.batch_size);
        let batches = append_batches.len() + mutate_batches.len();
        let append_mutations = params.records;
        let summary = WorkloadSummary {
            mutations: append_mutations
                + selection.overwrite_count
                + selection.delete_count
                + selection.reinsert_count,
            batches,
            append_mutations,
            overwrite_mutations: selection.overwrite_count,
            delete_mutations: selection.delete_count,
            reinsert_mutations: selection.reinsert_count,
            logical_input_bytes,
            live_records: expected.len(),
            live_logical_bytes,
            live_value_bytes,
        };
        let deleted_probe = if selection.delete_count > selection.reinsert_count {
            Some(key_for(
                order[selection.overwrite_count + selection.reinsert_count],
                params.seed,
            ))
        } else {
            None
        };

        Ok(Self {
            append: append_batches,
            mutate: mutate_batches,
            expected,
            deleted_probe,
            absent_probe,
            summary,
        })
    }
}

fn clone_batches(batches: &[Vec<BatchOperation>]) -> Vec<Vec<BatchOperation>> {
    batches.iter().map(|batch| batch.clone()).collect()
}

async fn phase_ingest<E: AsyncKVEngine>(
    engine: &mut E,
    batches: Vec<Vec<BatchOperation>>,
) -> Result<Duration, HarnessError> {
    let started = Instant::now();
    for batch in batches {
        engine.write_batch(batch).await?;
    }
    Ok(started.elapsed())
}

async fn phase_settle<E: AsyncKVEngine>(engine: &E) -> Result<Duration, HarnessError> {
    let started = Instant::now();
    engine.await_pending_writes().await?;
    Ok(started.elapsed())
}

fn sample_probe_positions(len: usize, limit: usize) -> Vec<usize> {
    if len == 0 || limit == 0 {
        return Vec::new();
    }
    if len <= limit {
        return (0..len).collect();
    }
    if limit == 1 {
        return vec![0];
    }
    let last = len - 1;
    let slots = limit - 1;
    let mut positions = Vec::with_capacity(limit);
    positions.push(0);
    for slot in 1..slots {
        positions.push(slot * last / slots);
    }
    positions.push(last);
    positions.sort_unstable();
    positions.dedup();
    positions
}

async fn verify_engine<E: AsyncKVEngine>(
    engine: &E,
    workload: &Workload,
    stage: &str,
) -> Result<(), HarnessError> {
    let expected: Vec<(Vec<u8>, Vec<u8>)> = workload
        .expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let scanned = engine.scan(..).await?;
    if scanned != expected {
        return Err(HarnessError::Verification(format!(
            "{stage}: scan mismatch: {}",
            first_scan_mismatch(&scanned, &expected)
        )));
    }
    for position in sample_probe_positions(expected.len(), VERIFY_POINT_PROBES) {
        let (key, value) = &expected[position];
        match engine.get(key).await? {
            Some(actual) if actual.as_slice() == value.as_slice() => {}
            Some(actual) => {
                return Err(HarnessError::Verification(format!(
                    "{stage}: get({}) returned {} bytes, expected {} bytes",
                    key_label(key),
                    actual.len(),
                    value.len()
                )));
            }
            None => {
                return Err(HarnessError::Verification(format!(
                    "{stage}: get({}) is missing a live key",
                    key_label(key)
                )));
            }
        }
    }
    if let Some(deleted) = &workload.deleted_probe {
        if engine.get(deleted).await?.is_some() {
            return Err(HarnessError::Verification(format!(
                "{stage}: deleted key {} is still readable",
                key_label(deleted)
            )));
        }
    }
    if engine.get(&workload.absent_probe).await?.is_some() {
        return Err(HarnessError::Verification(format!(
            "{stage}: absent key {} is readable",
            key_label(&workload.absent_probe)
        )));
    }
    Ok(())
}

fn first_scan_mismatch(actual: &[(Vec<u8>, Vec<u8>)], expected: &[(Vec<u8>, Vec<u8>)]) -> String {
    if actual.len() != expected.len() {
        return format!(
            "scan returned {} entries, expected {}",
            actual.len(),
            expected.len()
        );
    }
    for (position, (got, want)) in actual.iter().zip(expected.iter()).enumerate() {
        if got.0 != want.0 {
            return format!(
                "entry {position}: key {} differs from expected {}",
                key_label(&got.0),
                key_label(&want.0)
            );
        }
        if got.1 != want.1 {
            return format!(
                "entry {position}: value for {} has {} bytes, expected {} bytes",
                key_label(&want.0),
                got.1.len(),
                want.1.len()
            );
        }
    }
    "scan equals expected but comparison reported a mismatch".into()
}

fn key_label(key: &[u8]) -> String {
    let prefix = key
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("{prefix}({}B)", key.len())
}

#[derive(Debug, Clone)]
struct ReadTiming {
    point_gets: usize,
    point_hits: usize,
    get: Duration,
    scan_entries: usize,
    scan: Duration,
}

async fn read_benchmark<E: AsyncKVEngine>(
    engine: &E,
    workload: &Workload,
) -> Result<ReadTiming, HarnessError> {
    let ordered_keys: Vec<Vec<u8>> = workload.expected.keys().cloned().collect();
    let probe_keys: Vec<Vec<u8>> = sample_probe_positions(ordered_keys.len(), POINT_READ_LIMIT)
        .into_iter()
        .map(|position| ordered_keys[position].clone())
        .collect();
    let started = Instant::now();
    let mut hits = 0usize;
    for key in &probe_keys {
        if engine.get(key).await?.is_some() {
            hits += 1;
        }
    }
    let get = started.elapsed();
    let scan_limit = ordered_keys.len().min(SCAN_READ_LIMIT);
    let (scan, scan_entries) = if scan_limit == 0 {
        (Duration::ZERO, 0usize)
    } else {
        let from = ordered_keys[0].clone();
        let to = ordered_keys[scan_limit - 1].clone();
        let started = Instant::now();
        let rows = engine.scan(from..=to).await?;
        (started.elapsed(), rows.len())
    };
    Ok(ReadTiming {
        point_gets: probe_keys.len(),
        point_hits: hits,
        get,
        scan_entries,
        scan,
    })
}

#[derive(Debug, Clone)]
struct Census {
    total_bytes: u64,
    wal_bytes: u64,
    sstable_bytes: u64,
    metadata_bytes: u64,
    other_bytes: u64,
    file_count: u64,
    allocated_bytes: u64,
}

fn census(root: &Path) -> Result<Census, HarnessError> {
    let mut census = Census {
        total_bytes: 0,
        wal_bytes: 0,
        sstable_bytes: 0,
        metadata_bytes: 0,
        other_bytes: 0,
        file_count: 0,
        allocated_bytes: 0,
    };
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(HarnessError::Io(error)),
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(HarnessError::Configuration(format!(
                    "refusing to follow symbolic link {}",
                    path.display()
                )));
            }
            if file_type.is_dir() {
                pending.push(path);
                continue;
            }
            if !file_type.is_file() {
                return Err(HarnessError::Configuration(format!(
                    "unexpected filesystem entry {}",
                    path.display()
                )));
            }
            let metadata = entry.metadata()?;
            let bytes = metadata.len();
            census.total_bytes += bytes;
            census.file_count += 1;
            census.allocated_bytes += metadata.blocks() * 512;
            let relative_path = path.strip_prefix(root).map_err(|error| {
                HarnessError::Configuration(format!(
                    "cannot classify {} under {}: {error}",
                    path.display(),
                    root.display()
                ))
            })?;
            if relative_path
                .components()
                .any(|part| part.as_os_str() == "wal")
            {
                census.wal_bytes += bytes;
            } else if relative_path
                .components()
                .any(|part| part.as_os_str() == "sstable")
            {
                census.sstable_bytes += bytes;
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| METADATA_FILES.contains(&name))
            {
                census.metadata_bytes += bytes;
            } else {
                census.other_bytes += bytes;
            }
        }
    }
    Ok(census)
}

#[derive(Debug, Clone)]
struct LevelCount {
    level: u32,
    tables: usize,
}

#[derive(Debug, Clone)]
struct LevelReport {
    levels: Vec<LevelCount>,
    total_tables: usize,
    total_records: usize,
    compaction_observed: bool,
}

async fn read_levels(manifest_path: PathBuf) -> Result<LevelReport, HarnessError> {
    let manager = ManifestManager::load_or_create(manifest_path).await?;
    let snapshot = manager.snapshot().await;
    let mut levels: Vec<LevelCount> = snapshot
        .levels
        .iter()
        .map(|(level, tables)| LevelCount {
            level: *level,
            tables: tables.len(),
        })
        .collect();
    levels.sort_by_key(|entry| entry.level);
    let total_tables = levels.iter().map(|entry| entry.tables).sum();
    let total_records = snapshot
        .levels
        .values()
        .flatten()
        .map(|table| table.record_count)
        .sum();
    let compaction_observed = levels
        .iter()
        .any(|entry| entry.level > 0 && entry.tables > 0);
    Ok(LevelReport {
        levels,
        total_tables,
        total_records,
        compaction_observed,
    })
}

fn generation_ids(root: &Path) -> Result<Vec<u64>, HarnessError> {
    let directory = root.join("generations");
    let entries = match fs::read_dir(&directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(HarnessError::Io(error)),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let id = name.parse::<u64>().map_err(|_| {
            HarnessError::Configuration(format!("unexpected generation entry {name}"))
        })?;
        if !entry.file_type()?.is_dir() {
            return Err(HarnessError::Configuration(format!(
                "generation entry {} is not a directory",
                entry.path().display()
            )));
        }
        ids.push(id);
    }
    ids.sort_unstable();
    Ok(ids)
}

fn experimental_manifest_path(root: &Path, generations: &[u64]) -> Result<PathBuf, HarnessError> {
    let generation = generations.last().ok_or_else(|| {
        HarnessError::Configuration(format!(
            "experimental storage at {} has no generation directory",
            root.display()
        ))
    })?;
    Ok(root
        .join("generations")
        .join(format!("{generation:020}"))
        .join("index")
        .join("MANIFEST"))
}

async fn level_report(kind: EngineKind, root: &Path) -> Result<LevelReport, HarnessError> {
    match kind {
        EngineKind::Reference => read_levels(root.join("MANIFEST")).await,
        EngineKind::Experimental => {
            let generations = generation_ids(root)?;
            read_levels(experimental_manifest_path(root, &generations)?).await
        }
    }
}

#[derive(Debug, Clone)]
struct ClosedObservation {
    census: Census,
    levels: LevelReport,
    generations: Vec<u64>,
}

#[derive(Debug, Clone)]
struct RecoveryTimings {
    preclean_shutdown: Duration,
    preclean_reopen: Duration,
    postclean_shutdown: Option<Duration>,
    postclean_reopen: Option<Duration>,
    final_shutdown: Duration,
}

#[derive(Debug, Clone)]
struct EngineSample {
    ingest: Duration,
    mutate: Duration,
    settle: Duration,
    clean: Option<CleanStats>,
    recovery: RecoveryTimings,
    preclean: ClosedObservation,
    postclean: Option<ClosedObservation>,
    final_census: Census,
    read: Option<ReadTiming>,
}

async fn observe_closed(kind: EngineKind, dir: &Path) -> Result<ClosedObservation, HarnessError> {
    Ok(ClosedObservation {
        census: census(dir)?,
        levels: level_report(kind, dir).await?,
        generations: generation_ids(dir)?,
    })
}

async fn timed<F, T>(future: F) -> Result<(T, Duration), HarnessError>
where
    F: Future<Output = Result<T, DBError>>,
{
    let started = Instant::now();
    let value = future.await?;
    Ok((value, started.elapsed()))
}

async fn measure_reference(
    dir: &Path,
    config: &StorageConfig,
    workload: &Workload,
    reads: bool,
) -> Result<EngineSample, HarnessError> {
    let mut engine = KV2::open(dir, config.clone()).await?;
    let ingest = phase_ingest(&mut engine, clone_batches(&workload.append)).await?;
    let mutate = phase_ingest(&mut engine, clone_batches(&workload.mutate)).await?;
    let settle = phase_settle(&engine).await?;
    let (_, preclean_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);

    let preclean = observe_closed(EngineKind::Reference, dir).await?;

    let (mut reopened, preclean_reopen) = timed(KV2::open(dir, config.clone())).await?;
    verify_engine(&reopened, workload, "reference reopened").await?;
    let read = if reads {
        Some(read_benchmark(&reopened, workload).await?)
    } else {
        None
    };
    let (_, final_shutdown) = timed(reopened.shutdown()).await?;
    drop(reopened);
    let final_census = census(dir)?;

    Ok(EngineSample {
        ingest,
        mutate,
        settle,
        clean: None,
        recovery: RecoveryTimings {
            preclean_shutdown,
            preclean_reopen,
            postclean_shutdown: None,
            postclean_reopen: None,
            final_shutdown,
        },
        preclean,
        postclean: None,
        final_census,
        read,
    })
}

async fn measure_experimental(
    dir: &Path,
    config: &StorageConfig,
    workload: &Workload,
    reads: bool,
) -> Result<EngineSample, HarnessError> {
    let mut engine = ExperimentalEngine::open_with_config(dir, config.clone()).await?;
    let ingest = phase_ingest(&mut engine, clone_batches(&workload.append)).await?;
    let mutate = phase_ingest(&mut engine, clone_batches(&workload.mutate)).await?;
    let settle = phase_settle(&engine).await?;
    let (_, preclean_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);

    let preclean = observe_closed(EngineKind::Experimental, dir).await?;

    let (mut reopened, preclean_reopen) =
        timed(ExperimentalEngine::open_with_config(dir, config.clone())).await?;
    verify_engine(&reopened, workload, "experimental preclean reopen").await?;

    let clean = reopened.clean().await?;
    let (_, postclean_shutdown) = timed(reopened.shutdown()).await?;
    drop(reopened);

    let postclean = observe_closed(EngineKind::Experimental, dir).await?;

    let (mut cleaned, postclean_reopen) =
        timed(ExperimentalEngine::open_with_config(dir, config.clone())).await?;
    verify_engine(&cleaned, workload, "experimental cleaned reopen").await?;
    let read = if reads {
        Some(read_benchmark(&cleaned, workload).await?)
    } else {
        None
    };
    let (_, final_shutdown) = timed(cleaned.shutdown()).await?;
    drop(cleaned);
    let final_census = census(dir)?;

    Ok(EngineSample {
        ingest,
        mutate,
        settle,
        clean: Some(clean),
        recovery: RecoveryTimings {
            preclean_shutdown,
            preclean_reopen,
            postclean_shutdown: Some(postclean_shutdown),
            postclean_reopen: Some(postclean_reopen),
            final_shutdown,
        },
        preclean,
        postclean: Some(postclean),
        final_census,
        read,
    })
}

struct SampleSpec<'a> {
    case: &'a CaseSpec,
    batch_size: usize,
    sample_index: usize,
    warmup: bool,
    reads: bool,
    seed: u64,
    overwrite_percent: usize,
    delete_percent: usize,
    reinsert_percent: usize,
}

struct SampleRow {
    case: String,
    value_bytes: usize,
    records: usize,
    batch_size: usize,
    sample_index: usize,
    warmup: bool,
    seed: u64,
    engine: EngineKind,
    workload: WorkloadSummary,
    sample: EngineSample,
}

fn reject_symlinked_ancestors(path: &Path) -> Result<(), HarnessError> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(HarnessError::Configuration(format!(
                    "refusing path {}: {} is a symbolic link",
                    path.display(),
                    current.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(HarnessError::Io(error)),
        }
    }
    Ok(())
}

fn prepare_run_root(base: &Path) -> Result<(), HarnessError> {
    reject_symlinked_ancestors(base)?;
    match fs::metadata(base) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                return Err(HarnessError::Configuration(format!(
                    "run directory {} exists and is not a directory",
                    base.display()
                )));
            }
            if fs::read_dir(base)?.next().is_some() {
                return Err(HarnessError::Configuration(format!(
                    "run directory {} is not empty; the harness needs a fresh run directory and never deletes existing content",
                    base.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = base
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            fs::create_dir(base)?;
        }
        Err(error) => return Err(HarnessError::Io(error)),
    }
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(base.join(RUN_OWNER_FILE))?;
    Ok(())
}

fn create_fresh_dir(dir: &Path) -> Result<(), HarnessError> {
    reject_symlinked_ancestors(dir)?;
    if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    match fs::create_dir(dir) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(HarnessError::Configuration(format!(
                "scratch directory {} already exists; the harness never deletes or reuses existing paths",
                dir.display()
            )))
        }
        Err(error) => Err(HarnessError::Io(error)),
    }
}

async fn run_sample(
    kind: EngineKind,
    spec: &SampleSpec<'_>,
    config: &StorageConfig,
    dir: &Path,
) -> Result<SampleRow, HarnessError> {
    create_fresh_dir(dir)?;
    let workload = Workload::build(&WorkloadParams {
        records: spec.case.records,
        value_bytes: spec.case.value_bytes,
        batch_size: spec.batch_size,
        seed: spec.seed,
        overwrite_percent: spec.overwrite_percent,
        delete_percent: spec.delete_percent,
        reinsert_percent: spec.reinsert_percent,
    })?;
    let sample = match kind {
        EngineKind::Reference => measure_reference(dir, config, &workload, spec.reads).await?,
        EngineKind::Experimental => {
            measure_experimental(dir, config, &workload, spec.reads).await?
        }
    };
    Ok(SampleRow {
        case: spec.case.name.clone(),
        value_bytes: spec.case.value_bytes,
        records: spec.case.records,
        batch_size: spec.batch_size,
        sample_index: spec.sample_index,
        warmup: spec.warmup,
        seed: spec.seed,
        engine: kind,
        workload: workload.summary,
        sample,
    })
}

fn size_label(value_bytes: usize) -> String {
    if value_bytes % (64 * 1024) == 0 {
        format!("raw{}KiB", value_bytes / (64 * 1024) * 64)
    } else if value_bytes % 1024 == 0 {
        format!("raw{}KiB", value_bytes / 1024)
    } else {
        format!("raw{value_bytes}B")
    }
}

fn default_records(value_bytes: usize) -> usize {
    if value_bytes >= 64 * 1024 {
        64
    } else if value_bytes >= 4096 {
        512
    } else {
        16_384
    }
}

fn default_batch_sizes(value_bytes: usize) -> Vec<usize> {
    if value_bytes >= 64 * 1024 {
        vec![1, 16]
    } else {
        vec![1, 64]
    }
}

fn case_specs(cli: &Cli) -> Result<Vec<CaseSpec>, HarnessError> {
    let value_sizes = cli
        .value_bytes
        .clone()
        .unwrap_or_else(|| vec![64, 4096, 65536]);
    let mut cases = Vec::new();
    for value_bytes in value_sizes {
        if value_bytes < 8 || value_bytes > 1024 * 1024 {
            return Err(HarnessError::Configuration(
                "value bytes must be in 8..=1048576".into(),
            ));
        }
        let records = cli.records.unwrap_or_else(|| default_records(value_bytes));
        if records == 0 || records > MAX_RECORDS {
            return Err(HarnessError::Configuration(format!(
                "records must be in 1..={MAX_RECORDS}"
            )));
        }
        mutation_selection(
            records,
            cli.overwrite_percent,
            cli.delete_percent,
            cli.reinsert_percent,
        )?;
        let estimated = estimate_input_bytes(
            records,
            value_bytes,
            cli.overwrite_percent,
            cli.delete_percent,
            cli.reinsert_percent,
        );
        if estimated > MAX_INPUT_BYTES {
            return Err(HarnessError::Configuration(format!(
                "case {} would write an estimated {estimated} logical bytes, above the {MAX_INPUT_BYTES} byte bound; lower --records",
                size_label(value_bytes)
            )));
        }
        let batch_sizes = match &cli.batch_sizes {
            Some(sizes) => {
                for size in sizes {
                    if *size == 0 || *size > 1024 {
                        return Err(HarnessError::Configuration(
                            "batch sizes must be in 1..=1024".into(),
                        ));
                    }
                }
                sizes.clone()
            }
            None => default_batch_sizes(value_bytes),
        };
        cases.push(CaseSpec {
            name: size_label(value_bytes),
            value_bytes,
            records,
            batch_sizes,
        });
    }
    Ok(cases)
}

fn ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn phase_sum_ns(sample: &EngineSample) -> u64 {
    let recovery = &sample.recovery;
    [
        ns(sample.ingest),
        ns(sample.mutate),
        ns(sample.settle),
        ns(recovery.preclean_shutdown),
        ns(recovery.preclean_reopen),
        recovery.postclean_shutdown.map_or(0, ns),
        recovery.postclean_reopen.map_or(0, ns),
        sample.clean.as_ref().map_or(0, |clean| ns(clean.elapsed)),
        ns(recovery.final_shutdown),
    ]
    .into_iter()
    .fold(0u64, u64::saturating_add)
}

fn space_amplification(census: &Census, live_logical_bytes: u64) -> Option<f64> {
    if live_logical_bytes == 0 {
        None
    } else {
        Some(census.total_bytes as f64 / live_logical_bytes as f64)
    }
}

fn clean_json(clean: &CleanStats) -> Value {
    json!({
        "old_generation": clean.old_generation,
        "new_generation": clean.new_generation,
        "live_records": clean.live_records,
        "live_logical_bytes": clean.live_logical_bytes,
        "copied_payload_bytes": clean.copied_payload_bytes,
        "copied_transactions": clean.copied_transactions,
        "index_sstables_written": clean.index_sstables_written,
        "total_root_bytes_before": clean.total_root_bytes_before,
        "observed_peak_bytes": clean.observed_peak_bytes,
        "total_root_bytes_after": clean.total_root_bytes_after,
        "deferred_cleanup_bytes": clean.deferred_cleanup_bytes,
        "root_bytes_sampling": "core_clean_snapshot_while_engine_live",
        "write_pause_ns": ns(clean.write_pause),
        "elapsed_ns": ns(clean.elapsed),
    })
}

fn census_json(census: &Census) -> Value {
    json!({
        "total_bytes": census.total_bytes,
        "wal_bytes": census.wal_bytes,
        "sstable_bytes": census.sstable_bytes,
        "metadata_bytes": census.metadata_bytes,
        "other_bytes": census.other_bytes,
        "file_count": census.file_count,
        "os_allocated_bytes": census.allocated_bytes,
    })
}

fn level_counts_json(levels: &[LevelCount]) -> Value {
    Value::Array(
        levels
            .iter()
            .map(|entry| {
                json!({
                    "level": entry.level,
                    "tables": entry.tables,
                })
            })
            .collect(),
    )
}

fn levels_json(levels: &LevelReport) -> Value {
    json!({
        "levels": level_counts_json(&levels.levels),
        "total_tables": levels.total_tables,
        "total_records": levels.total_records,
        "compaction_observed": levels.compaction_observed,
    })
}

fn closed_json(observation: &ClosedObservation) -> Value {
    json!({
        "root": census_json(&observation.census),
        "levels": levels_json(&observation.levels),
        "generations": observation.generations,
    })
}

fn read_json(read: &ReadTiming) -> Value {
    json!({
        "point_gets": read.point_gets,
        "point_hits": read.point_hits,
        "point_get_elapsed_ns": ns(read.get),
        "scan_entries": read.scan_entries,
        "scan_elapsed_ns": ns(read.scan),
    })
}

fn sample_json(row: &SampleRow) -> Value {
    let sample = &row.sample;
    json!({
        "engine": row.engine.label(),
        "case": row.case,
        "value_bytes": row.value_bytes,
        "records": row.records,
        "batch_size": row.batch_size,
        "sample_index": row.sample_index,
        "warmup": row.warmup,
        "seed": row.seed,
        "workload": {
            "mutations": row.workload.mutations,
            "batches": row.workload.batches,
            "append_mutations": row.workload.append_mutations,
            "overwrite_mutations": row.workload.overwrite_mutations,
            "delete_mutations": row.workload.delete_mutations,
            "reinsert_mutations": row.workload.reinsert_mutations,
            "logical_input_bytes": row.workload.logical_input_bytes,
            "live_records": row.workload.live_records,
            "live_logical_bytes": row.workload.live_logical_bytes,
            "live_value_bytes": row.workload.live_value_bytes,
        },
        "elapsed_ns": {
            "ingest": ns(sample.ingest),
            "mutate": ns(sample.mutate),
            "settle": ns(sample.settle),
            "clean_pause": sample.clean.as_ref().map(|clean| ns(clean.write_pause)),
            "clean_elapsed": sample.clean.as_ref().map(|clean| ns(clean.elapsed)),
            "shutdown_preclean": ns(sample.recovery.preclean_shutdown),
            "reopen_preclean": ns(sample.recovery.preclean_reopen),
            "shutdown_postclean": sample.recovery.postclean_shutdown.map(ns),
            "reopen_postclean": sample.recovery.postclean_reopen.map(ns),
            "point_get": sample.read.as_ref().map(|read| ns(read.get)),
            "scan": sample.read.as_ref().map(|read| ns(read.scan)),
            "shutdown_final": ns(sample.recovery.final_shutdown),
            "lifecycle_phase_sum": phase_sum_ns(sample),
        },
        "lifecycle_basis": "explicit_phase_sum",
        "reads": sample.read.as_ref().map(read_json),
        "clean": sample.clean.as_ref().map(clean_json),
        "closed": {
            "preclean": closed_json(&sample.preclean),
            "postclean": sample.postclean.as_ref().map(closed_json),
        },
        "root": {
            "preclean": census_json(&sample.preclean.census),
            "postclean": sample.postclean.as_ref().map(|observation| census_json(&observation.census)),
            "final": census_json(&sample.final_census),
            "space_amplification": {
                "preclean": space_amplification(&sample.preclean.census, row.workload.live_logical_bytes),
                "postclean": sample.postclean.as_ref().and_then(|observation| space_amplification(&observation.census, row.workload.live_logical_bytes)),
                "final": space_amplification(&sample.final_census, row.workload.live_logical_bytes),
            },
        },
    })
}

fn stats_json(mut values: Vec<u64>) -> Value {
    let count = values.len();
    if values.is_empty() {
        return json!({"count": 0, "min_ns": null, "median_ns": null, "max_ns": null});
    }
    values.sort_unstable();
    json!({
        "count": count,
        "min_ns": values[0],
        "median_ns": values[values.len() / 2],
        "max_ns": values[values.len() - 1],
    })
}

fn stats_f64_json(mut values: Vec<f64>) -> Value {
    let count = values.len();
    if values.is_empty() {
        return json!({"count": 0, "min": null, "median": null, "max": null});
    }
    values.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "count": count,
        "min": values[0],
        "median": values[values.len() / 2],
        "max": values[values.len() - 1],
    })
}

fn collect_ns<F>(rows: &[&SampleRow], select: F) -> Vec<u64>
where
    F: Fn(&EngineSample) -> Option<u64>,
{
    rows.iter().filter_map(|row| select(&row.sample)).collect()
}

fn engine_summary(rows: &[&SampleRow], kind: EngineKind) -> Value {
    let mine: Vec<&SampleRow> = rows
        .iter()
        .copied()
        .filter(|row| row.engine == kind)
        .collect();
    let live_logical_bytes = mine
        .first()
        .map_or(0, |row| row.workload.live_logical_bytes);
    json!({
        "engine": kind.label(),
        "samples": mine.len(),
        "elapsed_ns": {
            "ingest": stats_json(collect_ns(&mine, |sample| Some(ns(sample.ingest)))),
            "mutate": stats_json(collect_ns(&mine, |sample| Some(ns(sample.mutate)))),
            "settle": stats_json(collect_ns(&mine, |sample| Some(ns(sample.settle)))),
            "clean_pause": stats_json(collect_ns(&mine, |sample| sample.clean.as_ref().map(|clean| ns(clean.write_pause)))),
            "clean_elapsed": stats_json(collect_ns(&mine, |sample| sample.clean.as_ref().map(|clean| ns(clean.elapsed)))),
            "shutdown_preclean": stats_json(collect_ns(&mine, |sample| Some(ns(sample.recovery.preclean_shutdown)))),
            "reopen_preclean": stats_json(collect_ns(&mine, |sample| Some(ns(sample.recovery.preclean_reopen)))),
            "shutdown_postclean": stats_json(collect_ns(&mine, |sample| sample.recovery.postclean_shutdown.map(ns))),
            "reopen_postclean": stats_json(collect_ns(&mine, |sample| sample.recovery.postclean_reopen.map(ns))),
            "point_get": stats_json(collect_ns(&mine, |sample| sample.read.as_ref().map(|read| ns(read.get)))),
            "scan": stats_json(collect_ns(&mine, |sample| sample.read.as_ref().map(|read| ns(read.scan)))),
            "shutdown_final": stats_json(collect_ns(&mine, |sample| Some(ns(sample.recovery.final_shutdown)))),
            "lifecycle_phase_sum": stats_json(collect_ns(&mine, |sample| Some(phase_sum_ns(sample)))),
        },
        "lifecycle_basis": "explicit_phase_sum",
        "recovery_phases": {
            "reference": ["preclean_shutdown", "preclean_reopen", "final_shutdown"],
            "experimental": ["preclean_shutdown", "preclean_reopen", "postclean_shutdown", "postclean_reopen", "final_shutdown"],
        },
        "rewrite_bytes": {
            "copied_payload": stats_json(collect_ns(&mine, |sample| sample.clean.as_ref().map(|clean| clean.copied_payload_bytes))),
            "logical_input": stats_json(mine.iter().map(|row| row.workload.logical_input_bytes).collect()),
        },
        "space": {
            "live_logical_bytes": live_logical_bytes,
            "preclean_total_bytes": stats_json(collect_ns(&mine, |sample| Some(sample.preclean.census.total_bytes))),
            "postclean_total_bytes": stats_json(collect_ns(&mine, |sample| sample.postclean.as_ref().map(|observation| observation.census.total_bytes))),
            "final_total_bytes": stats_json(collect_ns(&mine, |sample| Some(sample.final_census.total_bytes))),
            "space_amplification_preclean": stats_f64_json(
                mine.iter()
                    .filter_map(|row| space_amplification(&row.sample.preclean.census, row.workload.live_logical_bytes))
                    .collect()
            ),
            "space_amplification_postclean": stats_f64_json(
                mine.iter()
                    .filter_map(|row| row.sample.postclean.as_ref().and_then(|observation| space_amplification(&observation.census, row.workload.live_logical_bytes)))
                    .collect()
            ),
            "space_amplification_final": stats_f64_json(
                mine.iter()
                    .filter_map(|row| space_amplification(&row.sample.final_census, row.workload.live_logical_bytes))
                    .collect()
            ),
        },
        "level_evidence": {
            "preclean_compaction_observed": mine.iter().any(|row| row.sample.preclean.levels.compaction_observed),
            "postclean_compaction_observed": mine.iter().any(|row| row.sample.postclean.as_ref().is_some_and(|observation| observation.levels.compaction_observed)),
            "preclean_max_level": mine.iter().map(|row| row.sample.preclean.levels.levels.iter().map(|entry| entry.level).max().unwrap_or(0)).max().unwrap_or(0),
            "postclean_max_level": mine.iter().filter_map(|row| row.sample.postclean.as_ref()).map(|observation| observation.levels.levels.iter().map(|entry| entry.level).max().unwrap_or(0)).max().unwrap_or(0),
            "preclean_table_counts_last_sample": mine.last().map(|row| level_counts_json(&row.sample.preclean.levels.levels)),
            "postclean_table_counts_last_sample": mine.last().and_then(|row| row.sample.postclean.as_ref()).map(|observation| level_counts_json(&observation.levels.levels)),
            "preclean_total_tables_last_sample": mine.last().map(|row| row.sample.preclean.levels.total_tables),
            "postclean_total_tables_last_sample": mine.last().and_then(|row| row.sample.postclean.as_ref()).map(|observation| observation.levels.total_tables),
        },
    })
}

fn config_json(config: &StorageConfig) -> Value {
    json!({
        "memtable_size_threshold": config.memtable_size_threshold,
        "sstable_block_size": config.sstable_block_size,
        "max_level0_files": config.max_level0_files,
        "target_sstable_bytes": config.target_sstable_bytes,
        "level_base_bytes": config.level_base_bytes,
        "level_size_multiplier": config.level_size_multiplier,
        "wal_segment_size": config.wal_segment_size,
        "bloom_false_positive_rate": config.bloom_false_positive_rate,
        "max_sstable_file_handles": config.max_sstable_file_handles,
    })
}

#[derive(Debug, Clone)]
struct FtsDocument {
    id: DocumentId,
    text: Vec<u8>,
}

fn build_fts_documents(count: usize, seed: u64) -> Result<Vec<FtsDocument>, HarnessError> {
    if count == 0 || count > 256 {
        return Err(HarnessError::Configuration(
            "FTS document count must be in 1..=256".into(),
        ));
    }
    let mut documents = Vec::with_capacity(count);
    for index in 0..count {
        let mut words = Vec::with_capacity(48);
        let mut prng = Prng::new(seed ^ (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        for _ in 0..48 {
            let word = FTS_VOCABULARY[(prng.next_u64() % FTS_VOCABULARY.len() as u64) as usize];
            words.push(word);
        }
        let mut text = String::from(FTS_COMMON_TERM);
        if index % 13 == 0 {
            text.push(' ');
            text.push_str(FTS_RARE_TERMS[0]);
        }
        if index % 11 == 0 {
            text.push(' ');
            text.push_str(FTS_RARE_TERMS[1]);
        }
        for word in words {
            text.push(' ');
            text.push_str(word);
        }
        documents.push(FtsDocument {
            id: DocumentId::new(format!("doc-{index:04}"))?,
            text: text.into_bytes(),
        });
    }
    Ok(documents)
}

#[derive(Debug, Clone, PartialEq)]
struct FtsObservation {
    query_and_ids: Vec<String>,
    query_or_ids: Vec<String>,
    ranked_and: Vec<(String, u64)>,
    ranked_or: Vec<(String, u64)>,
    documents_checked: usize,
}

fn fts_queries() -> (FtsQuery, FtsQuery) {
    (
        FtsQuery::And(vec![FTS_COMMON_TERM.into(), FTS_RARE_TERMS[0].into()]),
        FtsQuery::Or(vec![FTS_RARE_TERMS[0].into(), FTS_RARE_TERMS[1].into()]),
    )
}

async fn observe_fts<E: AsyncKVEngine>(
    index: &FtsIndex<E>,
    documents: &[FtsDocument],
) -> Result<(FtsObservation, Duration), HarnessError> {
    let started = Instant::now();
    let (query_and, query_or) = fts_queries();
    let and_hits = index.search(query_and.clone(), 1000).await?;
    let or_hits = index.search(query_or.clone(), 1000).await?;
    let ranked_and = index
        .search_ranked(query_and, 1000, Bm25Config::default())
        .await?;
    let ranked_or = index
        .search_ranked(query_or, 1000, Bm25Config::default())
        .await?;
    let mut documents_checked = 0usize;
    for document in documents {
        match index.get_document(&document.id).await? {
            Some(bytes) if bytes == document.text => documents_checked += 1,
            Some(_) => {
                return Err(HarnessError::Verification(format!(
                    "FTS document {} does not match the indexed input",
                    document.id.as_str()
                )));
            }
            None => {
                return Err(HarnessError::Verification(format!(
                    "FTS document {} is missing",
                    document.id.as_str()
                )));
            }
        }
    }
    let elapsed = started.elapsed();
    Ok((
        FtsObservation {
            query_and_ids: and_hits
                .into_iter()
                .map(|hit| hit.id.as_str().to_string())
                .collect(),
            query_or_ids: or_hits
                .into_iter()
                .map(|hit| hit.id.as_str().to_string())
                .collect(),
            ranked_and: ranked_and
                .into_iter()
                .map(|hit| (hit.id.as_str().to_string(), hit.score.to_bits()))
                .collect(),
            ranked_or: ranked_or
                .into_iter()
                .map(|hit| (hit.id.as_str().to_string(), hit.score.to_bits()))
                .collect(),
            documents_checked,
        },
        elapsed,
    ))
}

struct FtsObservationTiming {
    observation: FtsObservation,
    elapsed: Duration,
}

struct FtsRun {
    engine: EngineKind,
    documents: usize,
    chunk: usize,
    ingest: Duration,
    settle: Duration,
    clean: Option<CleanStats>,
    recovery: RecoveryTimings,
    preclean_observation: FtsObservationTiming,
    postclean_observation: Option<FtsObservationTiming>,
    preclean: ClosedObservation,
    postclean: Option<ClosedObservation>,
    final_census: Census,
}

async fn fts_reference(
    dir: &Path,
    config: &StorageConfig,
    documents: &[FtsDocument],
    chunk: usize,
) -> Result<FtsRun, HarnessError> {
    let engine = KV2::open(dir, config.clone()).await?;
    let mut index = FtsIndex::open(engine).await?;
    let started = Instant::now();
    for group in documents.chunks(chunk) {
        index
            .put_documents(
                group
                    .iter()
                    .map(|document| (document.id.clone(), document.text.clone()))
                    .collect(),
            )
            .await?;
    }
    let ingest = started.elapsed();
    let mut engine = index.into_inner();
    let settle = phase_settle(&engine).await?;
    let (_, preclean_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);

    let preclean = observe_closed(EngineKind::Reference, dir).await?;

    let (engine, preclean_reopen) = timed(KV2::open(dir, config.clone())).await?;
    let index = FtsIndex::open(engine).await?;
    let (observation, elapsed) = observe_fts(&index, documents).await?;
    let mut engine = index.into_inner();
    let (_, final_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);
    let final_census = census(dir)?;

    Ok(FtsRun {
        engine: EngineKind::Reference,
        documents: documents.len(),
        chunk,
        ingest,
        settle,
        clean: None,
        recovery: RecoveryTimings {
            preclean_shutdown,
            preclean_reopen,
            postclean_shutdown: None,
            postclean_reopen: None,
            final_shutdown,
        },
        preclean_observation: FtsObservationTiming {
            observation,
            elapsed,
        },
        postclean_observation: None,
        preclean,
        postclean: None,
        final_census,
    })
}

async fn fts_experimental(
    dir: &Path,
    config: &StorageConfig,
    documents: &[FtsDocument],
    chunk: usize,
) -> Result<FtsRun, HarnessError> {
    let engine = ExperimentalEngine::open_with_config(dir, config.clone()).await?;
    let mut index = FtsIndex::open(engine).await?;
    let started = Instant::now();
    for group in documents.chunks(chunk) {
        index
            .put_documents(
                group
                    .iter()
                    .map(|document| (document.id.clone(), document.text.clone()))
                    .collect(),
            )
            .await?;
    }
    let ingest = started.elapsed();
    let mut engine = index.into_inner();
    let settle = phase_settle(&engine).await?;
    let (_, preclean_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);

    let preclean = observe_closed(EngineKind::Experimental, dir).await?;

    let (engine, preclean_reopen) =
        timed(ExperimentalEngine::open_with_config(dir, config.clone())).await?;
    let index = FtsIndex::open(engine).await?;
    let (observation, elapsed) = observe_fts(&index, documents).await?;
    let preclean_observation = FtsObservationTiming {
        observation,
        elapsed,
    };
    let mut engine = index.into_inner();
    let clean = engine.clean().await?;
    let (_, postclean_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);

    let postclean = observe_closed(EngineKind::Experimental, dir).await?;

    let (engine, postclean_reopen) =
        timed(ExperimentalEngine::open_with_config(dir, config.clone())).await?;
    let index = FtsIndex::open(engine).await?;
    let (observation, elapsed) = observe_fts(&index, documents).await?;
    let postclean_observation = FtsObservationTiming {
        observation,
        elapsed,
    };
    let mut engine = index.into_inner();
    let (_, final_shutdown) = timed(engine.shutdown()).await?;
    drop(engine);
    let final_census = census(dir)?;

    Ok(FtsRun {
        engine: EngineKind::Experimental,
        documents: documents.len(),
        chunk,
        ingest,
        settle,
        clean: Some(clean),
        recovery: RecoveryTimings {
            preclean_shutdown,
            preclean_reopen,
            postclean_shutdown: Some(postclean_shutdown),
            postclean_reopen: Some(postclean_reopen),
            final_shutdown,
        },
        preclean_observation,
        postclean_observation: Some(postclean_observation),
        preclean,
        postclean: Some(postclean),
        final_census,
    })
}

fn compare_fts_observation(
    baseline: &FtsRun,
    candidate: &FtsObservation,
    candidate_documents: usize,
) -> Result<(), String> {
    let reference = &baseline.preclean_observation.observation;
    if reference.documents_checked != baseline.documents
        || candidate.documents_checked != candidate_documents
    {
        return Err(format!(
            "documents checked reference={} experimental={} expected={}",
            reference.documents_checked, candidate.documents_checked, baseline.documents
        ));
    }
    if reference.query_and_ids != candidate.query_and_ids {
        return Err("Boolean AND results differ".into());
    }
    if reference.query_or_ids != candidate.query_or_ids {
        return Err("Boolean OR results differ".into());
    }
    if reference.ranked_and != candidate.ranked_and {
        return Err("ranked AND results differ".into());
    }
    if reference.ranked_or != candidate.ranked_or {
        return Err("ranked OR results differ".into());
    }
    Ok(())
}

fn compare_fts(baseline: &FtsRun, experimental: &FtsRun) -> (bool, String) {
    let mut candidates: Vec<(&str, &FtsObservation)> =
        vec![("preclean", &experimental.preclean_observation.observation)];
    if let Some(postclean) = &experimental.postclean_observation {
        candidates.push(("postclean", &postclean.observation));
    }
    let mut checked = Vec::new();
    for (label, candidate) in candidates {
        if let Err(detail) = compare_fts_observation(baseline, candidate, experimental.documents) {
            return (false, format!("{label}: {detail}"));
        }
        checked.push(label);
    }
    (
        true,
        format!(
            "Boolean and ranked results match the reference engine after {}",
            checked.join(" and ")
        ),
    )
}

fn fts_observation_json(observation: &FtsObservation) -> Value {
    json!({
        "documents_checked": observation.documents_checked,
        "boolean_and_ids": observation.query_and_ids,
        "boolean_or_ids": observation.query_or_ids,
        "ranked_and": observation.ranked_and.iter().map(|(id, score)| json!({"id": id, "score_bits": score})).collect::<Vec<_>>(),
        "ranked_or": observation.ranked_or.iter().map(|(id, score)| json!({"id": id, "score_bits": score})).collect::<Vec<_>>(),
    })
}

fn fts_timing_json(timing: &FtsObservationTiming) -> Value {
    json!({
        "elapsed_ns": ns(timing.elapsed),
        "observation": fts_observation_json(&timing.observation),
    })
}

fn fts_run_json(run: &FtsRun) -> Value {
    json!({
        "engine": run.engine.label(),
        "documents": run.documents,
        "put_documents_chunk": run.chunk,
        "ingest_elapsed_ns": ns(run.ingest),
        "settle_elapsed_ns": ns(run.settle),
        "clean_pause_ns": run.clean.as_ref().map(|clean| ns(clean.write_pause)),
        "clean_elapsed_ns": run.clean.as_ref().map(|clean| ns(clean.elapsed)),
        "shutdown_preclean_ns": ns(run.recovery.preclean_shutdown),
        "reopen_preclean_ns": ns(run.recovery.preclean_reopen),
        "shutdown_postclean_ns": run.recovery.postclean_shutdown.map(ns),
        "reopen_postclean_ns": run.recovery.postclean_reopen.map(ns),
        "shutdown_final_ns": ns(run.recovery.final_shutdown),
        "preclean_observation": fts_timing_json(&run.preclean_observation),
        "postclean_observation": run.postclean_observation.as_ref().map(fts_timing_json),
        "closed": {
            "preclean": closed_json(&run.preclean),
            "postclean": run.postclean.as_ref().map(closed_json),
        },
        "root": {
            "preclean": census_json(&run.preclean.census),
            "postclean": run.postclean.as_ref().map(|observation| census_json(&observation.census)),
            "final": census_json(&run.final_census),
        },
    })
}

fn require(condition: bool, message: impl Into<String>) -> Result<(), HarnessError> {
    if condition {
        Ok(())
    } else {
        Err(HarnessError::Pilot(message.into()))
    }
}

async fn expect_closed(engine: &mut ExperimentalEngine) -> Result<(), HarnessError> {
    if engine.get(b"closed-probe").await.is_ok() {
        return Err(HarnessError::Pilot("get succeeded after shutdown".into()));
    }
    if engine.scan(..).await.is_ok() {
        return Err(HarnessError::Pilot("scan succeeded after shutdown".into()));
    }
    if engine
        .put(b"closed-probe".to_vec(), b"value".to_vec())
        .await
        .is_ok()
    {
        return Err(HarnessError::Pilot("put succeeded after shutdown".into()));
    }
    if engine.delete(b"closed-probe".to_vec()).await.is_ok() {
        return Err(HarnessError::Pilot(
            "delete succeeded after shutdown".into(),
        ));
    }
    if engine
        .write_batch(vec![BatchOperation::Put {
            key: b"closed-probe".to_vec(),
            value: b"value".to_vec(),
        }])
        .await
        .is_ok()
    {
        return Err(HarnessError::Pilot(
            "write_batch succeeded after shutdown".into(),
        ));
    }
    if engine.await_pending_writes().await.is_ok() {
        return Err(HarnessError::Pilot(
            "await_pending_writes succeeded after shutdown".into(),
        ));
    }
    if engine.clean().await.is_ok() {
        return Err(HarnessError::Pilot("clean succeeded after shutdown".into()));
    }
    if engine.shutdown().await.is_ok() {
        return Err(HarnessError::Pilot("shutdown succeeded twice".into()));
    }
    Ok(())
}

async fn pilot_mode_guards(dir: &Path, config: &StorageConfig) -> Result<(), HarnessError> {
    create_fresh_dir(dir)?;
    let mut engine = ExperimentalEngine::open_with_config(dir, config.clone()).await?;
    engine.put(b"guard".to_vec(), b"one".to_vec()).await?;
    engine.await_pending_writes().await?;
    let first = engine.clean().await?;
    require(
        first.live_records == 1,
        "first guard clean lost the live record",
    )?;
    let value = engine
        .get(b"guard")
        .await?
        .ok_or_else(|| HarnessError::Pilot("guard key is missing after the first clean".into()))?;
    require(
        value.as_slice() == b"one",
        "guard key changed after the first clean",
    )?;
    engine.put(b"guard".to_vec(), b"two".to_vec()).await?;
    engine.await_pending_writes().await?;
    let second = engine.clean().await?;
    require(
        second.old_generation == first.new_generation,
        "second clean did not start from the first clean generation",
    )?;
    require(
        second.live_records == 1,
        "second guard clean lost the live record",
    )?;
    let value = engine
        .get(b"guard")
        .await?
        .ok_or_else(|| HarnessError::Pilot("guard key is missing after the second clean".into()))?;
    require(
        value.as_slice() == b"two",
        "guard key did not keep the latest value after the second clean",
    )?;
    engine.shutdown().await?;
    expect_closed(&mut engine).await?;
    drop(engine);
    let mut reopened = ExperimentalEngine::open_with_config(dir, config.clone()).await?;
    let value = reopened.get(b"guard").await?.ok_or_else(|| {
        HarnessError::Pilot("guard key is missing after mode-guard reopen".into())
    })?;
    require(
        value.as_slice() == b"two",
        "guard key value changed after mode-guard reopen",
    )?;
    reopened.shutdown().await?;
    Ok(())
}

fn pilot_sample_paths(base: &Path) -> (PathBuf, PathBuf, PathBuf, PathBuf, PathBuf) {
    (
        base.join("pilot/reference"),
        base.join("pilot/experimental"),
        base.join("pilot/mode-guards"),
        base.join("pilot/fts-baseline"),
        base.join("pilot/fts-experimental"),
    )
}

async fn run_pilot(
    base: &Path,
    config: &StorageConfig,
    filesystem: &Value,
) -> Result<(), HarnessError> {
    let case = CaseSpec {
        name: size_label(PILOT_VALUE_BYTES),
        value_bytes: PILOT_VALUE_BYTES,
        records: PILOT_RECORDS,
        batch_sizes: vec![PILOT_BATCH_SIZE],
    };
    let spec = SampleSpec {
        case: &case,
        batch_size: PILOT_BATCH_SIZE,
        sample_index: 0,
        warmup: false,
        reads: true,
        seed: DEFAULT_SEED,
        overwrite_percent: 25,
        delete_percent: 10,
        reinsert_percent: 50,
    };
    let (reference_dir, experimental_dir, guard_dir, fts_reference_dir, fts_experimental_dir) =
        pilot_sample_paths(base);
    let reference = run_sample(EngineKind::Reference, &spec, config, &reference_dir).await?;
    require(
        reference.workload.overwrite_mutations > 0
            && reference.workload.delete_mutations > 0
            && reference.workload.reinsert_mutations > 0,
        "pilot workload has no overwrite, delete, or reinsert mutations",
    )?;
    println!(
        "PILOT PASS reference writes/overwrites/deletes, full scan/get, shutdown, reopen, and bounded reads"
    );

    let experimental =
        run_sample(EngineKind::Experimental, &spec, config, &experimental_dir).await?;
    let clean =
        experimental.sample.clean.as_ref().ok_or_else(|| {
            HarnessError::Pilot("experimental sample has no clean statistics".into())
        })?;
    require(
        clean.live_records == experimental.workload.live_records as u64,
        format!(
            "clean copied {} live records, workload has {}",
            clean.live_records, experimental.workload.live_records
        ),
    )?;
    require(
        clean.live_logical_bytes == experimental.workload.live_logical_bytes,
        "clean live logical bytes do not match the workload",
    )?;
    require(
        clean.copied_payload_bytes == experimental.workload.live_value_bytes,
        "clean copied payload bytes do not match the live value bytes",
    )?;
    require(
        clean.index_sstables_written == 1,
        "clean did not write exactly one index SSTable",
    )?;
    require(
        clean.new_generation == clean.old_generation + 1,
        "clean did not advance the generation",
    )?;
    require(
        clean.total_root_bytes_after > 0
            && clean.observed_peak_bytes >= clean.total_root_bytes_before,
        "clean reported inconsistent root byte counts",
    )?;
    require(
        clean.deferred_cleanup_bytes == 0,
        "clean deferred cleanup on a quiet filesystem",
    )?;
    require(
        clean.write_pause > Duration::ZERO && clean.elapsed >= clean.write_pause,
        "clean pause and elapsed times are inconsistent",
    )?;
    let postclean =
        experimental.sample.postclean.as_ref().ok_or_else(|| {
            HarnessError::Pilot("experimental sample has no postclean census".into())
        })?;
    require(
        postclean.generations.len() == 1,
        "experimental root kept abandoned generations after clean",
    )?;
    require(
        postclean.levels.total_tables == 1
            && postclean.levels.total_records == experimental.workload.live_records,
        "clean index SSTable does not cover the live records",
    )?;
    require(
        experimental.sample.recovery.preclean_reopen > Duration::ZERO
            && experimental
                .sample
                .recovery
                .postclean_reopen
                .is_some_and(|duration| duration > Duration::ZERO)
            && experimental.sample.recovery.postclean_shutdown.is_some(),
        "experimental sample is missing a closed preclean or postclean phase",
    )?;
    println!(
        "PILOT PASS experimental writes/overwrites/deletes, closed preclean recovery, clean, closed postclean recovery, and bounded reads"
    );

    pilot_mode_guards(&guard_dir, config).await?;
    println!("PILOT PASS experimental mode guards and repeated clean");

    let documents = build_fts_documents(PILOT_FTS_DOCUMENTS, DEFAULT_SEED ^ 0xA5A5_5A5A_1234_5678)?;
    create_fresh_dir(&fts_reference_dir)?;
    create_fresh_dir(&fts_experimental_dir)?;
    let fts_baseline = fts_reference(
        &fts_reference_dir,
        config,
        &documents,
        PILOT_FTS_DOCUMENT_CHUNK,
    )
    .await?;
    let fts_experimental = fts_experimental(
        &fts_experimental_dir,
        config,
        &documents,
        PILOT_FTS_DOCUMENT_CHUNK,
    )
    .await?;
    let (equivalent, detail) = compare_fts(&fts_baseline, &fts_experimental);
    require(equivalent, format!("FTS equivalence failed: {detail}"))?;
    require(
        fts_baseline
            .preclean_observation
            .observation
            .documents_checked
            == documents.len(),
        "baseline FTS document count check failed",
    )?;
    require(
        fts_experimental
            .preclean_observation
            .observation
            .documents_checked
            == documents.len(),
        "experimental FTS document count check failed before clean",
    )?;
    require(
        fts_experimental
            .postclean_observation
            .as_ref()
            .is_some_and(|timing| timing.observation.documents_checked == documents.len()),
        "experimental FTS document count check failed after clean and reopen",
    )?;
    println!(
        "PILOT PASS FTS put_documents, exact Boolean and ranked equivalence before and after experimental clean"
    );

    let rows = vec![&reference, &experimental];
    let pilot_json = json!({
        "schema": "ceredb-experimental-wal-comparison-pilot/2",
        "lifecycle_basis": "explicit_phase_sum",
        "pilot": true,
        "generated_unix_secs": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs()),
        "git_head": git_head(),
        "config": config_json(config),
        "filesystem": filesystem,
        "records": PILOT_RECORDS,
        "value_bytes": PILOT_VALUE_BYTES,
        "samples": rows.iter().map(|row| sample_json(row)).collect::<Vec<_>>(),
        "summary": {
            "reference": engine_summary(&rows, EngineKind::Reference),
            "experimental": engine_summary(&rows, EngineKind::Experimental),
        },
        "fts": {
            "role": "functional_equivalence_only",
            "samples": 1,
            "documents": documents.len(),
            "put_documents_chunk": PILOT_FTS_DOCUMENT_CHUNK,
            "equivalent": equivalent,
            "detail": detail,
            "baseline": fts_run_json(&fts_baseline),
            "experimental": fts_run_json(&fts_experimental),
        },
        "limitations": [
            "pilot output validates the harness and correctness checks only; it is not a performance result",
            "one sample, no warmup, 32 records, and 4 KiB values",
            "the FTS case is functional equivalence only and is not part of the timed sample set"
        ],
    });
    let pilot_output = base.join("pilot/results.json");
    if let Some(parent) = pilot_output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&pilot_output, serde_json::to_vec_pretty(&pilot_json)?)?;
    println!("PILOT JSON {}", pilot_output.display());
    println!("PILOT OK");
    Ok(())
}

fn mount_info_for(path: &Path) -> Result<(String, String, String), HarnessError> {
    let mountinfo = fs::read_to_string("/proc/self/mountinfo")?;
    let target = path.to_string_lossy();
    let mut best: Option<(usize, String, String, String)> = None;
    for line in mountinfo.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let left_fields: Vec<&str> = left.split(' ').collect();
        let right_fields: Vec<&str> = right.split(' ').collect();
        let (Some(raw_mount_point), Some(fstype)) = (left_fields.get(4), right_fields.first())
        else {
            continue;
        };
        let mount_point = unescape_mount_field(raw_mount_point);
        let matches = target == mount_point
            || mount_point == "/"
            || (target.starts_with(&mount_point)
                && target.as_bytes().get(mount_point.len()) == Some(&b'/'));
        if !matches {
            continue;
        }
        let source = right_fields
            .get(1)
            .copied()
            .unwrap_or("unknown")
            .to_string();
        let length = mount_point.len();
        if best
            .as_ref()
            .is_none_or(|(best_length, _, _, _)| length >= *best_length)
        {
            best = Some((length, mount_point, fstype.to_string(), source));
        }
    }
    best.map(|(_, mount_point, fstype, source)| (mount_point, fstype, source))
        .ok_or_else(|| {
            HarnessError::Configuration(format!(
                "could not resolve the mount for {}",
                path.display()
            ))
        })
}

fn unescape_mount_field(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' && index + 3 < bytes.len() {
            let octal = &value[index + 1..index + 4];
            if let Ok(byte) = u8::from_str_radix(octal, 8) {
                output.push(byte);
                index += 4;
                continue;
            }
        }
        output.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&output).into_owned()
}

fn require_persistent_datadir(base: &Path) -> Result<Value, HarnessError> {
    prepare_run_root(base)?;
    let canonical = fs::canonicalize(base)?;
    let (mount_point, fstype, source) = mount_info_for(&canonical)?;
    if TRANSIENT_FILESYSTEMS.contains(&fstype.as_str()) {
        return Err(HarnessError::Configuration(format!(
            "refusing to measure in {} on {fstype} mounted at {mount_point}; choose a persistent filesystem",
            canonical.display()
        )));
    }
    Ok(json!({
        "path": canonical.display().to_string(),
        "mount_point": mount_point,
        "fstype": fstype,
        "source": source,
        "fresh_run_root": true,
        "owner_file": RUN_OWNER_FILE,
        "existing_paths_deleted": false,
        "transient_rejected": TRANSIENT_FILESYSTEMS,
    }))
}

fn git_head() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn run_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("{secs}-{}", std::process::id())
}

fn format_amplification(census: &Census, live_logical_bytes: u64) -> String {
    space_amplification(census, live_logical_bytes)
        .map_or_else(|| "n/a".to_string(), |value| format!("{value:.3}"))
}

fn level_pairs(levels: &LevelReport) -> Vec<(u32, usize)> {
    levels
        .levels
        .iter()
        .map(|entry| (entry.level, entry.tables))
        .collect()
}

fn optional_ms(value: Option<Duration>) -> String {
    value.map_or_else(
        || "n/a".to_string(),
        |duration| (ns(duration) / 1_000_000).to_string(),
    )
}

fn print_sample(row: &SampleRow) {
    let sample = &row.sample;
    let postclean_root = sample.postclean.as_ref().map_or_else(
        || "n/a".to_string(),
        |observation| {
            format!(
                "{}B amp={}",
                observation.census.total_bytes,
                format_amplification(&observation.census, row.workload.live_logical_bytes)
            )
        },
    );
    println!(
        "{} {} batch={} sample={} ingest={}ms mutate={}ms settle={}ms clean={}ms shutdown_pre={}ms reopen_pre={}ms shutdown_post={}ms reopen_post={}ms shutdown_final={}ms root_pre={}B amp_pre={} root_post={} levels_pre={:?} levels_post={:?} phase_sum={}ms",
        row.case,
        row.engine.label(),
        row.batch_size,
        row.sample_index,
        ns(sample.ingest) / 1_000_000,
        ns(sample.mutate) / 1_000_000,
        ns(sample.settle) / 1_000_000,
        sample
            .clean
            .as_ref()
            .map_or(0, |clean| ns(clean.elapsed) / 1_000_000),
        ns(sample.recovery.preclean_shutdown) / 1_000_000,
        ns(sample.recovery.preclean_reopen) / 1_000_000,
        optional_ms(sample.recovery.postclean_shutdown),
        optional_ms(sample.recovery.postclean_reopen),
        ns(sample.recovery.final_shutdown) / 1_000_000,
        sample.preclean.census.total_bytes,
        format_amplification(&sample.preclean.census, row.workload.live_logical_bytes),
        postclean_root,
        level_pairs(&sample.preclean.levels),
        sample
            .postclean
            .as_ref()
            .map(|observation| level_pairs(&observation.levels))
            .map_or_else(|| "n/a".to_string(), |pairs| format!("{pairs:?}")),
        phase_sum_ns(sample) / 1_000_000
    );
}

async fn run_benchmark(
    cli: &Cli,
    config: &StorageConfig,
    cases: &[CaseSpec],
    base: &Path,
    filesystem: &Value,
) -> Result<Value, HarnessError> {
    let mut case_results = Vec::new();
    for case in cases {
        for &batch_size in &case.batch_sizes {
            let mut rows: Vec<SampleRow> = Vec::new();
            for warmup_index in 0..cli.warmups {
                let spec = SampleSpec {
                    case,
                    batch_size,
                    sample_index: warmup_index,
                    warmup: true,
                    reads: !cli.skip_reads,
                    seed: cli.seed,
                    overwrite_percent: cli.overwrite_percent,
                    delete_percent: cli.delete_percent,
                    reinsert_percent: cli.reinsert_percent,
                };
                run_sample(
                    EngineKind::Reference,
                    &spec,
                    config,
                    &base.join(format!(
                        "{}-{batch_size}/warmup-{warmup_index}/reference",
                        case.name
                    )),
                )
                .await?;
                run_sample(
                    EngineKind::Experimental,
                    &spec,
                    config,
                    &base.join(format!(
                        "{}-{batch_size}/warmup-{warmup_index}/experimental",
                        case.name
                    )),
                )
                .await?;
            }
            for sample_index in 0..cli.repetitions {
                let order = if sample_index % 2 == 0 {
                    [EngineKind::Reference, EngineKind::Experimental]
                } else {
                    [EngineKind::Experimental, EngineKind::Reference]
                };
                for kind in order {
                    let spec = SampleSpec {
                        case,
                        batch_size,
                        sample_index,
                        warmup: false,
                        reads: !cli.skip_reads,
                        seed: cli.seed,
                        overwrite_percent: cli.overwrite_percent,
                        delete_percent: cli.delete_percent,
                        reinsert_percent: cli.reinsert_percent,
                    };
                    let directory = base.join(format!(
                        "{}-{batch_size}/sample-{sample_index}/{}",
                        case.name,
                        kind.label()
                    ));
                    let row = run_sample(kind, &spec, config, &directory).await?;
                    print_sample(&row);
                    rows.push(row);
                }
            }
            let reference_compaction = rows.iter().any(|row| {
                row.engine == EngineKind::Reference
                    && row.sample.preclean.levels.compaction_observed
            });
            let experimental_preclean_compaction = rows.iter().any(|row| {
                row.engine == EngineKind::Experimental
                    && row.sample.preclean.levels.compaction_observed
            });
            let experimental_postclean_compaction = rows.iter().any(|row| {
                row.engine == EngineKind::Experimental
                    && row
                        .sample
                        .postclean
                        .as_ref()
                        .is_some_and(|observation| observation.levels.compaction_observed)
            });
            println!(
                "level-evidence {} batch={} reference_preclean_compaction_observed={} experimental_preclean_compaction_observed={} experimental_postclean_compaction_observed={}",
                case.name,
                batch_size,
                reference_compaction,
                experimental_preclean_compaction,
                experimental_postclean_compaction
            );
            let refs: Vec<&SampleRow> = rows.iter().collect();
            case_results.push(json!({
                "case": case.name,
                "value_bytes": case.value_bytes,
                "records": case.records,
                "batch_size": batch_size,
                "profile": cli.profile.label(),
                "samples": rows.iter().map(sample_json).collect::<Vec<_>>(),
                "summary": {
                    "reference": engine_summary(&refs, EngineKind::Reference),
                    "experimental": engine_summary(&refs, EngineKind::Experimental),
                },
            }));
        }
    }

    let fts = if cli.skip_fts {
        Value::Null
    } else {
        let documents = build_fts_documents(FTS_DOCUMENTS, cli.seed ^ 0xA5A5_5A5A_1234_5678)?;
        let baseline_dir = base.join("fts/baseline");
        let experimental_dir = base.join("fts/experimental");
        create_fresh_dir(&baseline_dir)?;
        create_fresh_dir(&experimental_dir)?;
        let baseline = fts_reference(&baseline_dir, config, &documents, FTS_DOCUMENT_CHUNK).await?;
        let experimental =
            fts_experimental(&experimental_dir, config, &documents, FTS_DOCUMENT_CHUNK).await?;
        let (equivalent, detail) = compare_fts(&baseline, &experimental);
        if !equivalent {
            return Err(HarnessError::Verification(format!(
                "FTS equivalence failed after experimental clean and reopen: {detail}"
            )));
        }
        println!("fts matched documents={} equivalence=pass", documents.len());
        json!({
            "role": "functional_equivalence_only",
            "samples": 1,
            "documents": documents.len(),
            "put_documents_chunk": FTS_DOCUMENT_CHUNK,
            "equivalent": equivalent,
            "detail": detail,
            "baseline": fts_run_json(&baseline),
            "experimental": fts_run_json(&experimental),
        })
    };

    Ok(json!({
        "schema": "ceredb-experimental-wal-comparison/2",
        "lifecycle_basis": "explicit_phase_sum",
        "generated_unix_secs": SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |duration| duration.as_secs()),
        "git_head": git_head(),
        "profile": cli.profile.label(),
        "config": config_json(config),
        "seed": cli.seed,
        "repetitions": cli.repetitions,
        "warmups": cli.warmups,
        "overwrite_percent": cli.overwrite_percent,
        "delete_percent": cli.delete_percent,
        "reinsert_percent": cli.reinsert_percent,
        "filesystem": filesystem,
        "data_dir": base.display().to_string(),
        "cases": case_results,
        "fts": fts,
        "limitations": [
            "timings come from batched calls and are not per-request percentiles",
            "lifecycle_phase_sum is an explicit sum of the listed phase timers, not continuous wall time; verification, censuses, and reporting gaps are excluded",
            "root bytes are logical file sizes, not unique physical blocks; Btrfs compression and reflinks are not resolved",
            "clean root byte samples are taken by the core clean implementation while the engine is live; observed_peak_bytes is one snapshot, not a continuous maximum",
            "read timings follow full correctness verification and are warm-cache bounded",
            "full exact scans at each closed boundary re-read referenced WAL transactions; that verification cost is excluded from every timer",
            "the FTS case is functional equivalence only and is not a performance sample",
            "default record bounds are harness guards, not proof that a default 64 KiB case is feasible on every machine",
            "no lifetime write amplification claim; space amplification and clean rewrite bytes are reported separately"
        ],
    }))
}

fn pilot_profile(cli: &mut Cli) {
    cli.pilot = true;
    cli.profile = ProfileArg::SmallTables;
    cli.repetitions = 1;
    cli.warmups = 0;
    cli.records = Some(PILOT_RECORDS);
    cli.value_bytes = Some(vec![PILOT_VALUE_BYTES]);
    cli.batch_sizes = Some(vec![PILOT_BATCH_SIZE]);
    cli.overwrite_percent = 25;
    cli.delete_percent = 10;
    cli.reinsert_percent = 50;
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), HarnessError> {
    let mut cli = Cli::parse();
    if cli.pilot {
        pilot_profile(&mut cli);
    } else {
        if cli.repetitions < 3 {
            return Err(HarnessError::Configuration(
                "benchmark mode requires at least 3 timed samples per engine".into(),
            ));
        }
        if cli.warmups < 1 {
            return Err(HarnessError::Configuration(
                "benchmark mode requires at least one warmup sample".into(),
            ));
        }
    }
    let config = cli.profile.config()?;
    let cases = if cli.pilot {
        mutation_selection(
            PILOT_RECORDS,
            cli.overwrite_percent,
            cli.delete_percent,
            cli.reinsert_percent,
        )?;
        None
    } else {
        Some(case_specs(&cli)?)
    };
    let base = cli.data_dir.clone().unwrap_or_else(|| {
        PathBuf::from(format!("target/bench-data/experimental-wal-{}", run_id()))
    });
    let filesystem = require_persistent_datadir(&base)?;
    println!(
        "data-dir={} fstype={} mount={} profile={}",
        filesystem["path"].as_str().unwrap_or_default(),
        filesystem["fstype"].as_str().unwrap_or_default(),
        filesystem["mount_point"].as_str().unwrap_or_default(),
        cli.profile.label()
    );

    if cli.pilot {
        run_pilot(&base, &config, &filesystem).await?;
        return Ok(());
    }

    let cases = cases
        .ok_or_else(|| HarnessError::Configuration("benchmark cases were not prepared".into()))?;
    let results = run_benchmark(&cli, &config, &cases, &base, &filesystem).await?;
    let output = cli
        .output
        .clone()
        .unwrap_or_else(|| base.join("results.json"));
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&output, serde_json::to_vec_pretty(&results)?)?;
    println!("results={}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn census_classifies_paths_relative_to_the_store_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("wal/sstable/store");
        fs::create_dir_all(root.join("wal")).unwrap();
        fs::create_dir_all(root.join("sstable/level-0")).unwrap();
        fs::write(root.join("wal/0.log"), [1; 3]).unwrap();
        fs::write(root.join("sstable/level-0/0.dat"), [2; 16]).unwrap();
        fs::write(root.join("MANIFEST"), [3; 7]).unwrap();
        fs::write(root.join("other"), [4; 5]).unwrap();

        let observed = census(&root).unwrap();
        assert_eq!(observed.wal_bytes, 3);
        assert_eq!(observed.sstable_bytes, 16);
        assert_eq!(observed.metadata_bytes, 7);
        assert_eq!(observed.other_bytes, 5);
        assert_eq!(observed.total_bytes, 31);
        assert_eq!(observed.file_count, 4);
    }

    #[test]
    fn parse_records_rejects_zero_and_over_the_limit() {
        assert!(parse_records("0").is_err());
        assert!(parse_records(&(MAX_RECORDS + 1).to_string()).is_err());
        assert_eq!(parse_records("1").unwrap(), 1);
    }

    #[test]
    fn mutation_selection_rejects_overlapping_full_slices() {
        let error = mutation_selection(10, 100, 10, 50).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("overwrite percent plus delete percent")
        );
        assert!(mutation_selection(100, 95, 10, 50).is_err());
        assert!(mutation_selection(100, 100, 1, 50).is_err());
    }

    #[test]
    fn mutation_selection_accepts_disjoint_boundaries() {
        assert_eq!(
            mutation_selection(10, 0, 100, 50).unwrap(),
            MutationSelection {
                overwrite_count: 0,
                delete_count: 10,
                reinsert_count: 5,
            }
        );
        let exact = mutation_selection(100, 90, 10, 50).unwrap();
        assert_eq!(exact.overwrite_count, 90);
        assert_eq!(exact.delete_count, 10);
        assert_eq!(exact.reinsert_count, 5);
        assert!(mutation_selection(100, 100, 0, 50).is_ok());
        assert!(mutation_selection(100, 0, 0, 0).is_ok());
    }

    #[test]
    fn mutation_selection_checks_arithmetic_overflow() {
        assert!(mutation_selection(usize::MAX, 100, 0, 0).is_err());
    }

    #[test]
    fn probe_positions_are_bounded_and_include_boundaries() {
        assert!(sample_probe_positions(0, VERIFY_POINT_PROBES).is_empty());
        assert_eq!(sample_probe_positions(10, 1), vec![0]);
        assert_eq!(
            sample_probe_positions(3, VERIFY_POINT_PROBES),
            vec![0, 1, 2]
        );
        let positions = sample_probe_positions(1000, VERIFY_POINT_PROBES);
        assert_eq!(positions.len(), VERIFY_POINT_PROBES);
        assert_eq!(positions.first(), Some(&0));
        assert_eq!(positions.last(), Some(&999));
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
        let slightly_over = sample_probe_positions(VERIFY_POINT_PROBES + 1, VERIFY_POINT_PROBES);
        assert_eq!(slightly_over.len(), VERIFY_POINT_PROBES);
        assert_eq!(slightly_over.last(), Some(&VERIFY_POINT_PROBES));
    }

    #[test]
    fn prepare_run_root_refuses_existing_nonempty_directory() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("run");
        let nested = base.join("sample/nested");
        fs::create_dir_all(&nested).unwrap();
        let database_file = nested.join("db-file");
        let original = vec![7u8; 4096];
        fs::write(&database_file, &original).unwrap();

        assert!(prepare_run_root(&base).is_err());
        assert_eq!(fs::read(&database_file).unwrap(), original);
        assert!(!base.join(RUN_OWNER_FILE).exists());
    }

    #[test]
    fn prepare_run_root_claims_empty_directory_once() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("fresh");
        fs::create_dir(&base).unwrap();
        prepare_run_root(&base).unwrap();
        assert!(base.join(RUN_OWNER_FILE).exists());
        assert!(prepare_run_root(&base).is_err());
    }

    #[test]
    fn prepare_run_root_rejects_symlinked_ancestor_without_touching_target() {
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("target");
        fs::create_dir(&target).unwrap();
        let preserved = target.join("keep.bin");
        let original = b"preserve-me".to_vec();
        fs::write(&preserved, &original).unwrap();
        let link = temp.path().join("link");
        symlink(&target, &link).unwrap();

        let error = prepare_run_root(&link.join("run")).unwrap_err();
        assert!(error.to_string().contains("symbolic link"));
        assert!(!target.join("run").exists());
        assert_eq!(fs::read(&preserved).unwrap(), original);
    }

    #[test]
    fn create_fresh_dir_refuses_existing_nested_database() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("run");
        prepare_run_root(&base).unwrap();
        let sample = base.join("sample");
        let nested = sample.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let database_file = nested.join("db-file");
        let original = b"nested-database-bytes".to_vec();
        fs::write(&database_file, &original).unwrap();

        assert!(create_fresh_dir(&sample).is_err());
        assert_eq!(fs::read(&database_file).unwrap(), original);
    }

    #[test]
    fn create_fresh_dir_creates_once() {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("run");
        prepare_run_root(&base).unwrap();
        let sample = base.join("case/sample-0/reference");
        create_fresh_dir(&sample).unwrap();
        assert!(sample.is_dir());
        assert!(create_fresh_dir(&sample).is_err());
    }
}
