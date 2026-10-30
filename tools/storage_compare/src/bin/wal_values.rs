use anyhow::{Context, Result, ensure};
use ceredb::{
    ExperimentalEngine, KV2, StorageConfig,
    api::api::{AsyncKVEngine, BatchOperation},
    storage::manifest_codec::ManifestManager,
};
use rocksdb::{DB, DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    fs::{File, Metadata, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};
const SEED: u64 = 0x6c75_6e61_7761_6c76;
const KEY_BYTES: usize = 20;
const MAX_RECORDS: usize = 1024;
const MAX_VALUE_BYTES: usize = 64 * 1024;
const MAX_BATCH_SIZE: usize = 256;
const MAX_LOGICAL_INPUT_BYTES: usize = 32 * 1024 * 1024;
const IO_BUFFER_BYTES: u64 = 1024 * 1024;
const MEASURED_SAMPLES: usize = 3;
const WARMUPS: usize = 1;
const MAX_READ_PROBES: usize = 16;
const WARM_GETS: usize = 8;
const WARM_SCAN_ROWS: usize = 8;
const ROCKSDB_VERSION: &str = "0.24.0";
const ROCKSDB_NATIVE_VERSION: &str = "10.4.2";

const USAGE: &str = "usage: wal_values --data-dir PATH [--output PATH] [--pilot]\n\
--data-dir must be absent or empty and reside on a supported local filesystem.\n\
--output must be an unused file directly inside --data-dir.\n\
--pilot runs 20 records, 4 KiB values, batch size 1, one sample, no warmup.\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Engine {
    Kv2Inline,
    ExperimentalWalValues,
    RocksDb,
}

impl Engine {
    const ALL: [Self; 3] = [Self::Kv2Inline, Self::ExperimentalWalValues, Self::RocksDb];

    fn directory_name(self) -> &'static str {
        match self {
            Self::Kv2Inline => "kv2_inline",
            Self::ExperimentalWalValues => "experimental_wal_values",
            Self::RocksDb => "rocksdb",
        }
    }
}

#[derive(Debug, Clone)]
struct Args {
    data_dir: PathBuf,
    output: Option<PathBuf>,
    pilot: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CaseInfo {
    id: String,
    records: usize,
    key_bytes: usize,
    value_bytes: usize,
    batch_size: usize,
}

#[derive(Debug, Clone)]
struct CaseSpec {
    info: CaseInfo,
}

#[derive(Debug, Clone)]
struct Record {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug, Clone)]
enum Mutation {
    Put { record_index: usize, value: Vec<u8> },
    Delete { record_index: usize },
}

#[derive(Debug, Clone)]
struct ExpectedEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

#[derive(Debug, Clone)]
struct Workload {
    records: Vec<Record>,
    mutations: Vec<Mutation>,
    expected: BTreeMap<Vec<u8>, Vec<u8>>,
    expected_rows: Vec<(Vec<u8>, Vec<u8>)>,
    point_checks: Vec<ExpectedEntry>,
    deleted_probe: Vec<u8>,
    never_present_probe: Vec<u8>,
    warm_gets: Vec<ExpectedEntry>,
    warm_scan_rows: Vec<(Vec<u8>, Vec<u8>)>,
    overwrite_count: usize,
    delete_count: usize,
    reinsert_count: usize,
    append_metrics: OperationMetrics,
    mutation_metrics: OperationMetrics,
    live_logical_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct OperationMetrics {
    operations: u64,
    batch_calls: u64,
    logical_bytes: u64,
}

#[derive(Debug, Serialize)]
struct ExecutionReport {
    case: CaseInfo,
    store_path: String,
    engine: Engine,
    execution_order: Vec<Engine>,
    engine_position: usize,
    warmup: bool,
    sample_ordinal: usize,
    append: OperationMetrics,
    mutations: OperationMetrics,
    overwrite_operations: usize,
    delete_operations: usize,
    reinsert_operations: usize,
    live_records: usize,
    live_key_value_logical_bytes: u64,
    timings: Timings,
    natural_closed: StateSnapshot,
    clean: Option<CleanReport>,
    post_clean_closed: Option<StateSnapshot>,
    verification_uncleaned: Verification,
    verification_post_clean: Option<Verification>,
    warm_reads_natural: WarmReads,
    warm_reads_post_clean: Option<WarmReads>,
}

#[derive(Debug, Default, Serialize)]
struct Timings {
    fresh_open_ns: u64,
    append_ns: u64,
    mutations_ns: u64,
    barrier_drain_ns: u64,
    close_ns: u64,
    reopen_uncleaned_ns: u64,
    warm_get_uncleaned_ns: u64,
    warm_scan_uncleaned_ns: u64,
    warm_get_post_clean_ns: Option<u64>,
    warm_scan_post_clean_ns: Option<u64>,
    barrier_after_clean_ns: Option<u64>,
    close_after_clean_ns: Option<u64>,
    reopen_post_clean_ns: Option<u64>,
    close_after_verification_ns: Option<u64>,
    close_post_clean_verification_ns: Option<u64>,
    lifecycle_sum_ns: u64,
}

#[derive(Debug, Serialize)]
struct StateSnapshot {
    census: Census,
    live_key_value_logical_bytes: u64,
    root_bytes_per_live_logical_byte: Option<f64>,
    levels: BTreeMap<u32, u64>,
    level_evidence: &'static str,
}

#[derive(Debug, Default, Serialize)]
struct Census {
    total_file_logical_bytes: u64,
    file_count: u64,
    st_blocks_bytes_informational: Option<u64>,
    categories: BTreeMap<String, CategoryTotal>,
}

#[derive(Debug, Default, Serialize)]
struct CategoryTotal {
    file_count: u64,
    logical_bytes: u64,
}

#[derive(Debug, Serialize)]
struct CleanReport {
    old_generation: u64,
    new_generation: u64,
    live_records: u64,
    live_logical_bytes: u64,
    copied_payload_bytes: u64,
    copied_transactions: u64,
    index_sstables_written: u64,
    total_root_bytes_before: u64,
    observed_peak_bytes: u64,
    total_root_bytes_after: u64,
    deferred_cleanup_bytes: u64,
    write_pause_ns: u64,
    elapsed_ns: u64,
    observed_peak_basis: &'static str,
}

#[derive(Debug, Serialize)]
struct Verification {
    full_scan_entries: usize,
    deterministic_point_gets: usize,
    deleted_probe_missing: bool,
    never_present_probe_missing: bool,
}

#[derive(Debug, Serialize)]
struct WarmReads {
    get_calls: usize,
    get_entries: usize,
    scan_calls: usize,
    scan_entries: usize,
}

struct TimedWarmReads {
    counts: WarmReads,
    get_ns: u64,
    scan_ns: u64,
}

#[derive(Debug, Clone, Serialize)]
struct MountEvidence {
    dataset_path: String,
    target: String,
    fstype: String,
    source: String,
    options: String,
}

#[derive(Debug, Deserialize)]
struct FindmntDocument {
    filesystems: Vec<FindmntRow>,
}

#[derive(Debug, Deserialize)]
struct FindmntRow {
    target: String,
    fstype: String,
    source: String,
    options: String,
}

#[derive(Debug, Serialize)]
struct Report {
    schema_version: u32,
    mode: &'static str,
    performance_result: bool,
    seed_hex: String,
    key_bytes: usize,
    measured_samples_per_engine_case: usize,
    warmups_per_engine_case: usize,
    expected_measured_executions: usize,
    expected_warmup_executions: usize,
    storage_config: StorageConfigReport,
    rocksdb_config: RocksDbConfigReport,
    timing_boundaries: Vec<&'static str>,
    caveats: Vec<&'static str>,
    mount_before: MountEvidence,
    mount_after: MountEvidence,
    executions: Vec<ExecutionReport>,
}

#[derive(Debug, Serialize)]
struct StorageConfigReport {
    memtable_size_threshold_bytes: u64,
    wal_segment_size_bytes: u64,
    other_settings: &'static str,
}

#[derive(Debug, Serialize)]
struct RocksDbConfigReport {
    crate_version: &'static str,
    native_version: &'static str,
    write_buffer_size_bytes: u64,
    wal_enabled: bool,
    synchronous_writes: bool,
    compression: &'static str,
}

enum OpenStore {
    Kv2(KV2),
    Experimental(ExperimentalEngine),
    Rocks(DB),
}

fn main_args() -> Result<Option<Args>> {
    let mut args = env::args_os().skip(1);
    let mut data_dir = None;
    let mut output = None;
    let mut pilot = false;

    while let Some(argument) = args.next() {
        match argument.to_str().context("CLI flags must be valid UTF-8")? {
            "--help" | "-h" => return Ok(None),
            "--pilot" => pilot = true,
            "--data-dir" => {
                ensure!(data_dir.is_none(), "--data-dir may be given only once");
                data_dir = Some(PathBuf::from(
                    args.next().context("--data-dir requires a path")?,
                ));
            }
            "--output" => {
                ensure!(output.is_none(), "--output may be given only once");
                output = Some(PathBuf::from(
                    args.next().context("--output requires a path")?,
                ));
            }
            other => anyhow::bail!("unknown argument {other:?}\n{USAGE}"),
        }
    }

    Ok(Some(Args {
        data_dir: data_dir.context("--data-dir is required")?,
        output,
        pilot,
    }))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let Some(args) = main_args()? else {
        print!("{USAGE}");
        return Ok(());
    };

    let specs = if args.pilot {
        pilot_specs()
    } else {
        benchmark_specs()
    };
    let workloads = specs
        .iter()
        .map(prepare_workload)
        .collect::<Result<Vec<_>>>()?;
    let (data_dir, _lock, mount_before) = claim_data_dir(&args.data_dir)?;
    let output = report_path(args.output.as_deref(), &data_dir)?;
    let samples = if args.pilot { 1 } else { MEASURED_SAMPLES };
    let warmups = if args.pilot { 0 } else { WARMUPS };
    let executions = run_matrix(&data_dir, &specs, &workloads, samples, warmups).await?;
    ensure!(
        executions.len() == specs.len() * Engine::ALL.len() * (samples + warmups),
        "execution count does not match the selected profile"
    );
    let mount_after = find_mount(&data_dir)?;
    ensure_same_mount(&mount_before, &mount_after)?;

    let report = Report {
        schema_version: 2,
        mode: if args.pilot {
            "functional_pilot"
        } else {
            "benchmark_matrix"
        },
        performance_result: !args.pilot,
        seed_hex: format!("0x{SEED:016x}"),
        key_bytes: KEY_BYTES,
        measured_samples_per_engine_case: samples,
        warmups_per_engine_case: warmups,
        expected_measured_executions: specs.len() * Engine::ALL.len() * samples,
        expected_warmup_executions: specs.len() * Engine::ALL.len() * warmups,
        storage_config: StorageConfigReport {
            memtable_size_threshold_bytes: IO_BUFFER_BYTES,
            wal_segment_size_bytes: IO_BUFFER_BYTES,
            other_settings: "StorageConfig::default() for remaining CereDB settings",
        },
        rocksdb_config: RocksDbConfigReport {
            crate_version: ROCKSDB_VERSION,
            native_version: ROCKSDB_NATIVE_VERSION,
            write_buffer_size_bytes: IO_BUFFER_BYTES,
            wal_enabled: true,
            synchronous_writes: true,
            compression: "disabled",
        },
        timing_boundaries: vec![
            "Input values, keys, mutation plan, exact expected map, probes, and per-phase counts are built before timers.",
            "Fresh open is timed separately and excluded from append and mutation timers.",
            "Append and mutation timers include API-required copying/allocation and atomic batch construction; CereDB clones owned vectors while RocksDB receives borrowed slices.",
            "CereDB barrier times await_pending_writes; RocksDB barrier times flush_wal(true) after sync writes.",
            "Close timer includes CereDB shutdown or RocksDB drop; it starts immediately after the barrier.",
            "Closed census and correctness verification are untimed; reopen is timed separately.",
            "Separate warm get and scan timers include API calls and equality checks; identical workloads run before and after experimental cleaning, with calls and returned entries counted.",
            "Lifecycle sum adds non-overlapping phase timers; CleanStats.write_pause is nested in cleaner elapsed and is not added twice.",
        ],
        caveats: vec![
            "Pilot timings are functional evidence only, not a performance result.",
            "Equal 1 MiB targets do not equalize memory accounting, flush triggers, compaction policy, or scheduling.",
            "CereDB paths are public async APIs; RocksDB paths are synchronous APIs on the benchmark thread.",
            "Experimental WAL reads validate the complete referenced transaction on each value read; exact scans are retained and extra point probes are capped at 16.",
            "Census includes every file under each closed store root; st_blocks is informational and is not unique Btrfs physical usage.",
            "Clean observed_peak_bytes is a sample before CURRENT publication, not a continuous peak; copied bytes are not lifetime write amplification.",
            "Natural KV2 and RocksDB states are not force-flushed or compacted; level counts are observations, not equalized maintenance states.",
            "RocksDB close cancels or unschedules pending jobs and waits for outstanding jobs; it does not establish that all maintenance finished.",
            "No power-loss, external-service parity, or sustained-scalability claim is made.",
        ],
        mount_before,
        mount_after,
        executions,
    };
    let bytes = serde_json::to_vec_pretty(&report)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .with_context(|| format!("refusing to overwrite output {}", output.display()))?;
    file.write_all(&bytes)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    println!(
        "{}: {} cases, {} measured executions, {} warmups; report {}",
        report.mode,
        specs.len(),
        report.expected_measured_executions,
        report.expected_warmup_executions,
        output.display()
    );
    Ok(())
}

fn benchmark_specs() -> Vec<CaseSpec> {
    let mut specs = Vec::with_capacity(8);
    for batch_size in [1, 16, 64, 256] {
        specs.push(case_spec(1024, 256, batch_size));
    }
    for batch_size in [1, 64] {
        specs.push(case_spec(512, 4 * 1024, batch_size));
    }
    for batch_size in [1, 16] {
        specs.push(case_spec(64, 64 * 1024, batch_size));
    }
    specs
}

fn pilot_specs() -> Vec<CaseSpec> {
    vec![case_spec(20, 4 * 1024, 1)]
}

fn case_spec(records: usize, value_bytes: usize, batch_size: usize) -> CaseSpec {
    CaseSpec {
        info: CaseInfo {
            id: format!("v{value_bytes}_n{records}_b{batch_size}"),
            records,
            key_bytes: KEY_BYTES,
            value_bytes,
            batch_size,
        },
    }
}

fn prepare_workload(spec: &CaseSpec) -> Result<Workload> {
    validate_case(&spec.info)?;
    let records: Vec<_> = (0..spec.info.records)
        .map(|index| Record {
            key: key(index),
            value: seeded_value(index, 0, spec.info.value_bytes),
        })
        .collect();
    let overwrite_count = spec.info.records / 4;
    let approximate_deletes = (spec.info.records + 5) / 10;
    let delete_count = if approximate_deletes % 2 == 0 {
        approximate_deletes
    } else {
        approximate_deletes + 1
    };
    let delete_start = overwrite_count;
    ensure!(
        delete_start + delete_count <= records.len(),
        "mutation ranges exceed original records"
    );
    let reinsert_count = delete_count / 2;
    let mut mutations = Vec::with_capacity(overwrite_count + delete_count + reinsert_count);
    for record_index in 0..overwrite_count {
        mutations.push(Mutation::Put {
            record_index,
            value: seeded_value(record_index, 1, spec.info.value_bytes),
        });
    }
    for record_index in delete_start..(delete_start + delete_count) {
        mutations.push(Mutation::Delete { record_index });
    }
    for record_index in delete_start..(delete_start + reinsert_count) {
        mutations.push(Mutation::Put {
            record_index,
            value: seeded_value(record_index, 2, spec.info.value_bytes),
        });
    }
    let deleted_index = delete_start + reinsert_count;
    ensure!(
        deleted_index < delete_start + delete_count,
        "workload must retain at least one deleted key"
    );

    let mut expected: BTreeMap<_, _> = records
        .iter()
        .map(|record| (record.key.clone(), record.value.clone()))
        .collect();
    for mutation in &mutations {
        match mutation {
            Mutation::Put {
                record_index,
                value,
            } => {
                expected.insert(records[*record_index].key.clone(), value.clone());
            }
            Mutation::Delete { record_index } => {
                expected.remove(&records[*record_index].key);
            }
        }
    }
    let expected_rows: Vec<_> = expected
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let point_checks = deterministic_checks(&expected_rows, MAX_READ_PROBES);
    let warm_gets = deterministic_checks(&expected_rows, WARM_GETS);
    let warm_scan_rows = expected_rows.iter().take(WARM_SCAN_ROWS).cloned().collect();
    let append_metrics = OperationMetrics {
        operations: records.len() as u64,
        batch_calls: records.len().div_ceil(spec.info.batch_size) as u64,
        logical_bytes: records
            .iter()
            .map(|record| (record.key.len() + record.value.len()) as u64)
            .sum(),
    };
    let mutation_metrics = OperationMetrics {
        operations: mutations.len() as u64,
        batch_calls: mutations.len().div_ceil(spec.info.batch_size) as u64,
        logical_bytes: mutations
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put {
                    record_index,
                    value,
                } => (records[*record_index].key.len() + value.len()) as u64,
                Mutation::Delete { record_index } => records[*record_index].key.len() as u64,
            })
            .sum(),
    };
    let live_logical_bytes = expected_rows
        .iter()
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum();
    let deleted_probe = records[deleted_index].key.clone();
    ensure!(
        append_metrics.logical_bytes + mutation_metrics.logical_bytes
            <= MAX_LOGICAL_INPUT_BYTES as u64,
        "logical input exceeds {} bytes",
        MAX_LOGICAL_INPUT_BYTES
    );

    Ok(Workload {
        records,
        mutations,
        expected,
        expected_rows,
        point_checks,
        deleted_probe,
        never_present_probe: format!("missing-{:012}", spec.info.records + 1).into_bytes(),
        warm_gets,
        warm_scan_rows,
        overwrite_count,
        delete_count,
        reinsert_count,
        append_metrics,
        mutation_metrics,
        live_logical_bytes,
    })
}

fn validate_case(info: &CaseInfo) -> Result<()> {
    ensure!(
        info.records > 0 && info.records <= MAX_RECORDS,
        "record count out of bounds"
    );
    ensure!(
        info.key_bytes == KEY_BYTES,
        "key length must remain {KEY_BYTES} bytes"
    );
    ensure!(
        info.value_bytes > 0 && info.value_bytes <= MAX_VALUE_BYTES,
        "value size out of bounds"
    );
    ensure!(
        info.batch_size > 0 && info.batch_size <= MAX_BATCH_SIZE,
        "batch size out of bounds"
    );
    let logical_bytes = info
        .records
        .checked_mul(info.key_bytes + info.value_bytes)
        .context("logical input byte count overflow")?;
    ensure!(
        logical_bytes <= MAX_LOGICAL_INPUT_BYTES,
        "append input exceeds {} bytes",
        MAX_LOGICAL_INPUT_BYTES
    );
    Ok(())
}

fn seeded_value(record_index: usize, phase: u64, value_bytes: usize) -> Vec<u8> {
    let mut state = SEED
        ^ (record_index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ phase.wrapping_mul(0xd1b5_4a32_d192_ed03)
        ^ (value_bytes as u64).rotate_left(23);
    let mut value = vec![0; value_bytes];
    for chunk in value.chunks_mut(8) {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut word = state;
        word = (word ^ (word >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        word = (word ^ (word >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        word ^= word >> 31;
        let bytes = word.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    value
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:016}").into_bytes()
}

fn deterministic_checks(rows: &[(Vec<u8>, Vec<u8>)], max_checks: usize) -> Vec<ExpectedEntry> {
    let count = rows.len().min(max_checks);
    if count == 0 {
        return Vec::new();
    }
    (0..count)
        .map(|index| {
            let row_index = if count == 1 {
                0
            } else {
                index * (rows.len() - 1) / (count - 1)
            };
            ExpectedEntry {
                key: rows[row_index].0.clone(),
                value: rows[row_index].1.clone(),
            }
        })
        .collect()
}

async fn run_matrix(
    data_dir: &Path,
    specs: &[CaseSpec],
    workloads: &[Workload],
    samples: usize,
    warmups: usize,
) -> Result<Vec<ExecutionReport>> {
    ensure!(
        specs.len() == workloads.len(),
        "workload/spec count mismatch"
    );
    fs::create_dir(data_dir.join("samples"))?;
    let mut executions = Vec::with_capacity(specs.len() * Engine::ALL.len() * (samples + warmups));
    for (case_index, (spec, workload)) in specs.iter().zip(workloads).enumerate() {
        let case_dir = data_dir.join("samples").join(&spec.info.id);
        fs::create_dir(&case_dir)
            .with_context(|| format!("sample case path already exists: {}", case_dir.display()))?;
        for ordinal in 0..(samples + warmups) {
            let execution_order = engine_order(case_index, ordinal);
            for (engine_position, engine) in execution_order.iter().copied().enumerate() {
                let phase_name = if ordinal < warmups {
                    format!("warmup-{}", ordinal + 1)
                } else {
                    format!("sample-{}", ordinal - warmups + 1)
                };
                let store_path = case_dir.join(format!("{}-{phase_name}", engine.directory_name()));
                fs::create_dir(&store_path).with_context(|| {
                    format!("sample path already exists: {}", store_path.display())
                })?;
                executions.push(
                    run_one_engine(
                        &spec.info,
                        workload,
                        engine,
                        execution_order.to_vec(),
                        engine_position,
                        ordinal < warmups,
                        if ordinal < warmups {
                            0
                        } else {
                            ordinal - warmups + 1
                        },
                        &store_path,
                    )
                    .await?,
                );
            }
        }
    }
    Ok(executions)
}

fn engine_order(case_index: usize, ordinal: usize) -> [Engine; 3] {
    match (case_index + ordinal) % 3 {
        0 => [
            Engine::Kv2Inline,
            Engine::ExperimentalWalValues,
            Engine::RocksDb,
        ],
        1 => [
            Engine::ExperimentalWalValues,
            Engine::RocksDb,
            Engine::Kv2Inline,
        ],
        _ => [
            Engine::RocksDb,
            Engine::Kv2Inline,
            Engine::ExperimentalWalValues,
        ],
    }
}

async fn run_one_engine(
    info: &CaseInfo,
    workload: &Workload,
    engine: Engine,
    execution_order: Vec<Engine>,
    engine_position: usize,
    warmup: bool,
    sample_ordinal: usize,
    store_path: &Path,
) -> Result<ExecutionReport> {
    let fresh_open_started = Instant::now();
    let mut store = open_store(engine, store_path).await?;
    let fresh_open_ns = elapsed_ns(fresh_open_started.elapsed());

    let append_started = Instant::now();
    write_appends(&mut store, workload, info.batch_size).await?;
    let append_ns = elapsed_ns(append_started.elapsed());

    let mutations_started = Instant::now();
    write_mutations(&mut store, workload, info.batch_size).await?;
    let mutations_ns = elapsed_ns(mutations_started.elapsed());

    let barrier_started = Instant::now();
    barrier(&store).await?;
    let barrier_drain_ns = elapsed_ns(barrier_started.elapsed());

    let close_started = Instant::now();
    shutdown(&mut store).await?;
    drop(store);
    let close_ns = elapsed_ns(close_started.elapsed());

    let natural_census = census(store_path)?;
    let closed_levels = if engine == Engine::RocksDb {
        None
    } else {
        Some(cere_manifest_levels(engine, store_path).await?)
    };
    let reopen_started = Instant::now();
    let mut reopened = open_store(engine, store_path).await?;
    let reopen_uncleaned_ns = elapsed_ns(reopen_started.elapsed());
    let levels = match (&reopened, closed_levels) {
        (OpenStore::Rocks(db), None) => rocks_level_counts(db)?,
        (_, Some(levels)) => levels,
        _ => anyhow::bail!("engine/store mismatch while reading closed levels"),
    };
    let natural_closed = state_snapshot(
        natural_census,
        workload.live_logical_bytes,
        levels,
        match engine {
            Engine::RocksDb => "rocksdb_live_files_after_reopen",
            _ => "ceredb_public_manifest_snapshot_while_closed",
        },
    );
    let verification_uncleaned = verify(&reopened, workload).await?;
    let natural_reads = warm_reads(&reopened, workload).await?;

    let mut clean_report = None;
    let mut post_clean_closed = None;
    let mut verification_post_clean = None;
    let mut barrier_after_clean_ns = None;
    let mut close_after_clean_ns = None;
    let mut reopen_post_clean_ns = None;
    let mut close_after_verification_ns = None;
    let mut close_post_clean_verification_ns = None;
    let mut post_clean_reads = None;

    if let OpenStore::Experimental(experimental) = &mut reopened {
        let clean_stats = experimental.clean().await?;
        let clean_elapsed_ns = elapsed_ns(clean_stats.elapsed);
        clean_report = Some(CleanReport {
            old_generation: clean_stats.old_generation,
            new_generation: clean_stats.new_generation,
            live_records: clean_stats.live_records,
            live_logical_bytes: clean_stats.live_logical_bytes,
            copied_payload_bytes: clean_stats.copied_payload_bytes,
            copied_transactions: clean_stats.copied_transactions,
            index_sstables_written: clean_stats.index_sstables_written,
            total_root_bytes_before: clean_stats.total_root_bytes_before,
            observed_peak_bytes: clean_stats.observed_peak_bytes,
            total_root_bytes_after: clean_stats.total_root_bytes_after,
            deferred_cleanup_bytes: clean_stats.deferred_cleanup_bytes,
            write_pause_ns: elapsed_ns(clean_stats.write_pause),
            elapsed_ns: clean_elapsed_ns,
            observed_peak_basis: "sampled before CURRENT publication; not a continuous peak",
        });
        let barrier_started = Instant::now();
        barrier(&reopened).await?;
        barrier_after_clean_ns = Some(elapsed_ns(barrier_started.elapsed()));
        let close_started = Instant::now();
        shutdown(&mut reopened).await?;
        drop(reopened);
        close_after_clean_ns = Some(elapsed_ns(close_started.elapsed()));

        let post_clean_census = census(store_path)?;
        let post_clean_levels = cere_manifest_levels(engine, store_path).await?;
        let reopen_started = Instant::now();
        let post_clean = open_store(engine, store_path).await?;
        reopen_post_clean_ns = Some(elapsed_ns(reopen_started.elapsed()));
        post_clean_closed = Some(state_snapshot(
            post_clean_census,
            workload.live_logical_bytes,
            post_clean_levels,
            "ceredb_public_manifest_snapshot_while_closed",
        ));
        verification_post_clean = Some(verify(&post_clean, workload).await?);
        post_clean_reads = Some(warm_reads(&post_clean, workload).await?);
        let close_started = Instant::now();
        shutdown_and_drop(post_clean).await?;
        close_post_clean_verification_ns = Some(elapsed_ns(close_started.elapsed()));
    } else {
        let close_started = Instant::now();
        shutdown_and_drop(reopened).await?;
        close_after_verification_ns = Some(elapsed_ns(close_started.elapsed()));
    }

    let mut timings = Timings {
        fresh_open_ns,
        append_ns,
        mutations_ns,
        barrier_drain_ns,
        close_ns,
        reopen_uncleaned_ns,
        warm_get_uncleaned_ns: natural_reads.get_ns,
        warm_scan_uncleaned_ns: natural_reads.scan_ns,
        warm_get_post_clean_ns: post_clean_reads.as_ref().map(|reads| reads.get_ns),
        warm_scan_post_clean_ns: post_clean_reads.as_ref().map(|reads| reads.scan_ns),
        barrier_after_clean_ns,
        close_after_clean_ns,
        reopen_post_clean_ns,
        close_after_verification_ns,
        close_post_clean_verification_ns,
        lifecycle_sum_ns: 0,
    };
    timings.lifecycle_sum_ns = lifecycle_sum(
        &timings,
        clean_report.as_ref().map(|clean| clean.elapsed_ns),
    );

    Ok(ExecutionReport {
        case: info.clone(),
        store_path: store_path.to_string_lossy().into_owned(),
        engine,
        execution_order,
        engine_position,
        warmup,
        sample_ordinal,
        append: workload.append_metrics.clone(),
        mutations: workload.mutation_metrics.clone(),
        overwrite_operations: workload.overwrite_count,
        delete_operations: workload.delete_count,
        reinsert_operations: workload.reinsert_count,
        live_records: workload.expected.len(),
        live_key_value_logical_bytes: workload.live_logical_bytes,
        timings,
        natural_closed,
        clean: clean_report,
        post_clean_closed,
        verification_uncleaned,
        verification_post_clean,
        warm_reads_natural: natural_reads.counts,
        warm_reads_post_clean: post_clean_reads.map(|reads| reads.counts),
    })
}

async fn open_store(engine: Engine, path: &Path) -> Result<OpenStore> {
    let config = storage_config();
    match engine {
        Engine::Kv2Inline => Ok(OpenStore::Kv2(KV2::open(path, config).await?)),
        Engine::ExperimentalWalValues => Ok(OpenStore::Experimental(
            ExperimentalEngine::open_with_config(path, config).await?,
        )),
        Engine::RocksDb => {
            let mut options = Options::default();
            options.create_if_missing(true);
            options.set_write_buffer_size(IO_BUFFER_BYTES as usize);
            options.set_compression_type(DBCompressionType::None);
            Ok(OpenStore::Rocks(DB::open(&options, path)?))
        }
    }
}

fn storage_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: IO_BUFFER_BYTES,
        wal_segment_size: IO_BUFFER_BYTES,
        ..StorageConfig::default()
    }
}

async fn write_appends(
    store: &mut OpenStore,
    workload: &Workload,
    batch_size: usize,
) -> Result<()> {
    match store {
        OpenStore::Kv2(engine) => write_cere_appends(engine, workload, batch_size).await,
        OpenStore::Experimental(engine) => write_cere_appends(engine, workload, batch_size).await,
        OpenStore::Rocks(engine) => write_rocks_appends(engine, workload, batch_size),
    }
}

async fn write_cere_appends<E: AsyncKVEngine>(
    engine: &mut E,
    workload: &Workload,
    batch_size: usize,
) -> Result<()> {
    for group in workload.records.chunks(batch_size) {
        if batch_size == 1 {
            let record = &group[0];
            engine.put(record.key.clone(), record.value.clone()).await?;
        } else {
            engine
                .write_batch(
                    group
                        .iter()
                        .map(|record| BatchOperation::Put {
                            key: record.key.clone(),
                            value: record.value.clone(),
                        })
                        .collect(),
                )
                .await?;
        }
    }
    Ok(())
}

fn write_rocks_appends(db: &DB, workload: &Workload, batch_size: usize) -> Result<()> {
    let options = rocks_write_options();
    for group in workload.records.chunks(batch_size) {
        if batch_size == 1 {
            let record = &group[0];
            db.put_opt(&record.key, &record.value, &options)?;
        } else {
            let mut batch = WriteBatch::default();
            for record in group {
                batch.put(&record.key, &record.value);
            }
            db.write_opt(batch, &options)?;
        }
    }
    Ok(())
}

async fn write_mutations(
    store: &mut OpenStore,
    workload: &Workload,
    batch_size: usize,
) -> Result<()> {
    match store {
        OpenStore::Kv2(engine) => write_cere_mutations(engine, workload, batch_size).await,
        OpenStore::Experimental(engine) => write_cere_mutations(engine, workload, batch_size).await,
        OpenStore::Rocks(engine) => write_rocks_mutations(engine, workload, batch_size),
    }
}

async fn write_cere_mutations<E: AsyncKVEngine>(
    engine: &mut E,
    workload: &Workload,
    batch_size: usize,
) -> Result<()> {
    for group in workload.mutations.chunks(batch_size) {
        if batch_size == 1 {
            match &group[0] {
                Mutation::Put {
                    record_index,
                    value,
                } => {
                    engine
                        .put(workload.records[*record_index].key.clone(), value.clone())
                        .await?;
                }
                Mutation::Delete { record_index } => {
                    engine
                        .delete(workload.records[*record_index].key.clone())
                        .await?;
                }
            }
        } else {
            engine
                .write_batch(
                    group
                        .iter()
                        .map(|mutation| mutation_batch_operation(mutation, workload))
                        .collect(),
                )
                .await?;
        }
    }
    Ok(())
}

fn write_rocks_mutations(db: &DB, workload: &Workload, batch_size: usize) -> Result<()> {
    let options = rocks_write_options();
    for group in workload.mutations.chunks(batch_size) {
        if batch_size == 1 {
            match &group[0] {
                Mutation::Put {
                    record_index,
                    value,
                } => db.put_opt(&workload.records[*record_index].key, value, &options)?,
                Mutation::Delete { record_index } => {
                    db.delete_opt(&workload.records[*record_index].key, &options)?
                }
            }
        } else {
            let mut batch = WriteBatch::default();
            for mutation in group {
                match mutation {
                    Mutation::Put {
                        record_index,
                        value,
                    } => batch.put(&workload.records[*record_index].key, value),
                    Mutation::Delete { record_index } => {
                        batch.delete(&workload.records[*record_index].key)
                    }
                }
            }
            db.write_opt(batch, &options)?;
        }
    }
    Ok(())
}

fn mutation_batch_operation(mutation: &Mutation, workload: &Workload) -> BatchOperation {
    match mutation {
        Mutation::Put {
            record_index,
            value,
        } => BatchOperation::Put {
            key: workload.records[*record_index].key.clone(),
            value: value.clone(),
        },
        Mutation::Delete { record_index } => BatchOperation::Delete {
            key: workload.records[*record_index].key.clone(),
        },
    }
}

fn rocks_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    options
}

async fn barrier(store: &OpenStore) -> Result<()> {
    match store {
        OpenStore::Kv2(engine) => engine.await_pending_writes().await?,
        OpenStore::Experimental(engine) => engine.await_pending_writes().await?,
        OpenStore::Rocks(engine) => engine.flush_wal(true)?,
    }
    Ok(())
}

async fn shutdown(store: &mut OpenStore) -> Result<()> {
    match store {
        OpenStore::Kv2(engine) => engine.shutdown().await?,
        OpenStore::Experimental(engine) => engine.shutdown().await?,
        OpenStore::Rocks(_) => {}
    }
    Ok(())
}

async fn shutdown_and_drop(mut store: OpenStore) -> Result<()> {
    shutdown(&mut store).await?;
    drop(store);
    Ok(())
}

async fn verify(store: &OpenStore, workload: &Workload) -> Result<Verification> {
    match store {
        OpenStore::Kv2(engine) => verify_cere(engine, workload).await,
        OpenStore::Experimental(engine) => verify_cere(engine, workload).await,
        OpenStore::Rocks(engine) => verify_rocks(engine, workload),
    }
}

async fn verify_cere<E: AsyncKVEngine>(engine: &E, workload: &Workload) -> Result<Verification> {
    let actual = engine.scan(..).await?;
    ensure!(
        actual == workload.expected_rows,
        "full CereDB scan mismatch"
    );
    for expected in &workload.point_checks {
        let actual = engine.get(&expected.key).await?;
        ensure!(
            actual.as_deref().map(Vec::as_slice) == Some(expected.value.as_slice()),
            "CereDB point mismatch at {:?}",
            expected.key
        );
    }
    ensure!(
        engine.get(&workload.deleted_probe).await?.is_none(),
        "deleted CereDB key unexpectedly present"
    );
    ensure!(
        engine.get(&workload.never_present_probe).await?.is_none(),
        "never-present CereDB key unexpectedly present"
    );
    Ok(Verification {
        full_scan_entries: actual.len(),
        deterministic_point_gets: workload.point_checks.len(),
        deleted_probe_missing: true,
        never_present_probe_missing: true,
    })
}

fn verify_rocks(db: &DB, workload: &Workload) -> Result<Verification> {
    let actual = rocks_scan(db, IteratorMode::Start, usize::MAX)?;
    ensure!(
        actual == workload.expected_rows,
        "full RocksDB scan mismatch"
    );
    for expected in &workload.point_checks {
        ensure!(
            db.get(&expected.key)?.as_deref() == Some(expected.value.as_slice()),
            "RocksDB point mismatch at {:?}",
            expected.key
        );
    }
    ensure!(
        db.get(&workload.deleted_probe)?.is_none(),
        "deleted RocksDB key unexpectedly present"
    );
    ensure!(
        db.get(&workload.never_present_probe)?.is_none(),
        "never-present RocksDB key unexpectedly present"
    );
    Ok(Verification {
        full_scan_entries: actual.len(),
        deterministic_point_gets: workload.point_checks.len(),
        deleted_probe_missing: true,
        never_present_probe_missing: true,
    })
}

fn rocks_scan(db: &DB, mode: IteratorMode, limit: usize) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    db.iterator(mode)
        .take(limit)
        .map(|row| {
            let (key, value) = row?;
            Ok((key.to_vec(), value.to_vec()))
        })
        .collect::<std::result::Result<Vec<_>, rocksdb::Error>>()
        .map_err(Into::into)
}

async fn warm_reads(store: &OpenStore, workload: &Workload) -> Result<TimedWarmReads> {
    let mut get_entries = 0;
    let mut scan_entries = 0;
    let get_started = Instant::now();
    match store {
        OpenStore::Kv2(engine) => {
            for expected in &workload.warm_gets {
                let actual = engine.get(&expected.key).await?;
                ensure!(
                    actual.as_deref().map(Vec::as_slice) == Some(expected.value.as_slice()),
                    "warm CereDB get mismatch"
                );
                get_entries += usize::from(actual.is_some());
            }
        }
        OpenStore::Experimental(engine) => {
            for expected in &workload.warm_gets {
                let actual = engine.get(&expected.key).await?;
                ensure!(
                    actual.as_deref().map(Vec::as_slice) == Some(expected.value.as_slice()),
                    "warm experimental get mismatch"
                );
                get_entries += usize::from(actual.is_some());
            }
        }
        OpenStore::Rocks(engine) => {
            for expected in &workload.warm_gets {
                let actual = engine.get(&expected.key)?;
                ensure!(
                    actual.as_deref() == Some(expected.value.as_slice()),
                    "warm RocksDB get mismatch"
                );
                get_entries += usize::from(actual.is_some());
            }
        }
    }
    let get_ns = elapsed_ns(get_started.elapsed());
    let scan_started = Instant::now();
    if let (Some(first), Some(last)) = (
        workload.warm_scan_rows.first(),
        workload.warm_scan_rows.last(),
    ) {
        let rows = match store {
            OpenStore::Kv2(engine) => engine.scan(first.0.clone()..=last.0.clone()).await?,
            OpenStore::Experimental(engine) => {
                engine.scan(first.0.clone()..=last.0.clone()).await?
            }
            OpenStore::Rocks(engine) => rocks_scan(
                engine,
                IteratorMode::From(first.0.as_slice(), Direction::Forward),
                workload.warm_scan_rows.len(),
            )?,
        };
        ensure!(rows == workload.warm_scan_rows, "warm scan mismatch");
        scan_entries = rows.len();
    }
    let scan_ns = elapsed_ns(scan_started.elapsed());
    Ok(TimedWarmReads {
        counts: WarmReads {
            get_calls: workload.warm_gets.len(),
            get_entries,
            scan_calls: usize::from(!workload.warm_scan_rows.is_empty()),
            scan_entries,
        },
        get_ns,
        scan_ns,
    })
}

async fn cere_manifest_levels(engine: Engine, path: &Path) -> Result<BTreeMap<u32, u64>> {
    let manifest_path = match engine {
        Engine::Kv2Inline => path.join("MANIFEST"),
        Engine::ExperimentalWalValues => {
            let current = fs::read(path.join("CURRENT"))?;
            ensure!(current.len() >= 9, "experimental CURRENT file is truncated");
            let generation = u64::from_be_bytes(current[1..9].try_into()?);
            path.join("generations")
                .join(format!("{generation:020}"))
                .join("index")
                .join("MANIFEST")
        }
        Engine::RocksDb => anyhow::bail!("RocksDB levels use live_files"),
    };
    ensure!(
        manifest_path.is_file(),
        "missing public manifest at {}",
        manifest_path.display()
    );
    let manifest = ManifestManager::load_or_create(manifest_path).await?;
    Ok(manifest
        .snapshot()
        .await
        .levels
        .into_iter()
        .map(|(level, files)| (level, files.len() as u64))
        .collect())
}

fn rocks_level_counts(db: &DB) -> Result<BTreeMap<u32, u64>> {
    let mut levels = BTreeMap::new();
    for file in db.live_files()? {
        ensure!(file.level >= 0, "RocksDB returned a negative level");
        *levels.entry(file.level as u32).or_insert(0) += 1;
    }
    Ok(levels)
}

fn state_snapshot(
    census: Census,
    live_key_value_logical_bytes: u64,
    levels: BTreeMap<u32, u64>,
    level_evidence: &'static str,
) -> StateSnapshot {
    let root_bytes_per_live_logical_byte = (live_key_value_logical_bytes > 0)
        .then(|| census.total_file_logical_bytes as f64 / live_key_value_logical_bytes as f64);
    StateSnapshot {
        census,
        live_key_value_logical_bytes,
        root_bytes_per_live_logical_byte,
        levels,
        level_evidence,
    }
}

fn census(root: &Path) -> Result<Census> {
    let mut result = Census {
        st_blocks_bytes_informational: Some(0),
        ..Census::default()
    };
    let mut pending = vec![root.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_dir() {
                pending.push(path);
                continue;
            }
            ensure!(
                metadata.is_file() || metadata.file_type().is_symlink(),
                "unexpected non-file store entry {}",
                path.display()
            );
            let bytes = metadata.len();
            result.file_count += 1;
            result.total_file_logical_bytes += bytes;
            if let Some(allocated) = allocated_bytes(&metadata) {
                if let Some(total) = &mut result.st_blocks_bytes_informational {
                    *total += allocated;
                }
            } else {
                result.st_blocks_bytes_informational = None;
            }
            let category = classify_file(root, &path)?;
            let total = result.categories.entry(category.to_string()).or_default();
            total.file_count += 1;
            total.logical_bytes += bytes;
        }
    }
    Ok(result)
}

#[cfg(unix)]
fn allocated_bytes(metadata: &Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.blocks() * 512)
}

#[cfg(not(unix))]
fn allocated_bytes(_: &Metadata) -> Option<u64> {
    None
}

fn classify_file(root: &Path, path: &Path) -> Result<&'static str> {
    let relative = path.strip_prefix(root)?;
    let components = relative
        .components()
        .filter_map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();
    let name = components.last().copied().unwrap_or_default();
    if components.contains(&"wal") || is_rocks_wal_file(name) {
        Ok("wal")
    } else if components.contains(&"sstable") || name.ends_with(".sst") || name.ends_with(".ldb") {
        Ok("sstable")
    } else if name == "MANIFEST" || name.starts_with("MANIFEST-") {
        Ok("manifest")
    } else if name == "LOG"
        || name.starts_with("LOG.")
        || name == "OPTIONS"
        || name.starts_with("OPTIONS-")
        || matches!(name, "CURRENT" | "LOCK" | "IDENTITY")
        || name.starts_with("SEAL")
        || name.starts_with("NEXT-GENERATION")
        || name == "EXPERIMENTAL-WAL-VALUES"
    {
        Ok("metadata")
    } else {
        Ok("other")
    }
}

fn is_rocks_wal_file(name: &str) -> bool {
    name.strip_suffix(".log")
        .is_some_and(|stem| !stem.is_empty() && stem.bytes().all(|byte| byte.is_ascii_digit()))
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn lifecycle_sum(timings: &Timings, clean_elapsed_ns: Option<u64>) -> u64 {
    [
        Some(timings.fresh_open_ns),
        Some(timings.append_ns),
        Some(timings.mutations_ns),
        Some(timings.barrier_drain_ns),
        Some(timings.close_ns),
        Some(timings.reopen_uncleaned_ns),
        Some(timings.warm_get_uncleaned_ns),
        Some(timings.warm_scan_uncleaned_ns),
        timings.warm_get_post_clean_ns,
        timings.warm_scan_post_clean_ns,
        clean_elapsed_ns,
        timings.barrier_after_clean_ns,
        timings.close_after_clean_ns,
        timings.reopen_post_clean_ns,
        timings.close_after_verification_ns,
        timings.close_post_clean_verification_ns,
    ]
    .into_iter()
    .flatten()
    .fold(0u64, u64::saturating_add)
}

fn report_path(requested: Option<&Path>, data_dir: &Path) -> Result<PathBuf> {
    let requested = requested
        .map(|path| {
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                data_dir.join(path)
            }
        })
        .unwrap_or_else(|| data_dir.join("report.json"));
    let output = absolute_normalized(&requested)?;
    ensure!(
        output.parent() == Some(data_dir),
        "--output must be a direct child of --data-dir"
    );
    ensure!(
        !output.exists(),
        "refusing to overwrite output {}",
        output.display()
    );
    let name = output.file_name().context("--output must name a file")?;
    ensure!(
        name != ".storage-compare.lock" && name != "samples",
        "--output path is reserved"
    );
    Ok(output)
}

fn claim_data_dir(path: &Path) -> Result<(PathBuf, File, MountEvidence)> {
    let data_dir = absolute_normalized(path)?;
    reject_symlink_ancestors(&data_dir)?;
    let parent = data_dir.parent().context("--data-dir must have a parent")?;
    ensure!(parent.is_dir(), "--data-dir parent must already exist");
    let preflight_path = if data_dir.exists() {
        data_dir.clone()
    } else {
        parent.canonicalize()?
    };
    let preflight_mount = find_mount(&preflight_path)?;
    reject_unsupported_filesystem(&preflight_mount)?;
    let (canonical, lock) = claim_owned_data_dir(&data_dir)?;
    let mount_before = find_mount(&canonical)?;
    reject_unsupported_filesystem(&mount_before)?;
    ensure_same_mount(&preflight_mount, &mount_before)?;
    Ok((canonical, lock, mount_before))
}

fn claim_owned_data_dir(data_dir: &Path) -> Result<(PathBuf, File)> {
    reject_symlink_ancestors(data_dir)?;
    let parent = data_dir.parent().context("--data-dir must have a parent")?;
    ensure!(parent.is_dir(), "--data-dir parent must already exist");
    if data_dir.exists() {
        ensure!(data_dir.is_dir(), "--data-dir must be a directory");
        ensure!(
            fs::read_dir(data_dir)?.next().is_none(),
            "--data-dir must be absent or empty; refusing to reuse {}",
            data_dir.display()
        );
    } else {
        fs::create_dir(data_dir)
            .with_context(|| format!("creating dataset root {}", data_dir.display()))?;
    }
    reject_symlink_ancestors(data_dir)?;
    let canonical = data_dir.canonicalize()?;
    let lock_path = canonical.join(".storage-compare.lock");
    let lock = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
        .with_context(|| {
            format!(
                "dataset root is already owned or not empty: {}",
                canonical.display()
            )
        })?;
    lock.try_lock()
        .context("could not lock dataset root exclusively")?;
    Ok((canonical, lock))
}

fn absolute_normalized(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "invalid parent component in path {}",
                    path.display()
                );
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component);
            }
        }
    }
    Ok(normalized)
}

fn reject_symlink_ancestors(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    let components = path.components().collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                ensure!(
                    !metadata.file_type().is_symlink(),
                    "symlinked path component rejected: {}",
                    current.display()
                );
                if index + 1 < components.len() {
                    ensure!(
                        metadata.is_dir(),
                        "non-directory ancestor rejected: {}",
                        current.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn find_mount(path: &Path) -> Result<MountEvidence> {
    let output = Command::new("findmnt")
        .args(["--json", "--target"])
        .arg(path)
        .args(["--output", "TARGET,FSTYPE,SOURCE,OPTIONS"])
        .output()
        .context("findmnt is required for dataset filesystem safety")?;
    ensure!(
        output.status.success(),
        "findmnt failed for {}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr).trim()
    );
    let document: FindmntDocument = serde_json::from_slice(&output.stdout)?;
    let row = document
        .filesystems
        .into_iter()
        .next()
        .context("findmnt returned no filesystem for dataset path")?;
    Ok(MountEvidence {
        dataset_path: path.to_string_lossy().into_owned(),
        target: row.target,
        fstype: row.fstype,
        source: row.source,
        options: row.options,
    })
}

fn reject_unsupported_filesystem(mount: &MountEvidence) -> Result<()> {
    let filesystem = mount.fstype.to_ascii_lowercase();
    ensure!(
        !matches!(
            filesystem.as_str(),
            "tmpfs" | "ramfs" | "devtmpfs" | "overlay" | "fuse.overlayfs"
        ),
        "unsupported benchmark filesystem {} at {}",
        mount.fstype,
        mount.target
    );
    Ok(())
}

fn ensure_same_mount(before: &MountEvidence, after: &MountEvidence) -> Result<()> {
    ensure!(
        before.target == after.target
            && before.fstype == after.fstype
            && before.source == after.source
            && normalized_mount_options(&before.options)
                == normalized_mount_options(&after.options),
        "dataset mount changed during run: before={:?}, after={:?}",
        before,
        after
    );
    Ok(())
}

fn normalized_mount_options(options: &str) -> BTreeSet<&str> {
    options
        .split(',')
        .map(str::trim)
        .filter(|option| !option.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn mount_stability_rejects_option_changes_and_accepts_reordering() {
        let before = MountEvidence {
            dataset_path: "/home/run".into(),
            target: "/home".into(),
            fstype: "btrfs".into(),
            source: "/dev/test".into(),
            options: "rw,compress=zstd:1,ssd".into(),
        };
        let mut after = before.clone();
        after.options = "ssd, rw,compress=zstd:1".into();
        assert!(ensure_same_mount(&before, &after).is_ok());
        after.options = "rw,compress=zstd:3,ssd".into();
        assert!(ensure_same_mount(&before, &after).is_err());
        after.options = "ro,compress=zstd:1,ssd".into();
        assert!(ensure_same_mount(&before, &after).is_err());
    }

    #[test]
    fn lifecycle_counts_natural_and_cleaned_reads_once() {
        let timings = Timings {
            warm_get_uncleaned_ns: 11,
            warm_scan_uncleaned_ns: 13,
            warm_get_post_clean_ns: Some(17),
            warm_scan_post_clean_ns: Some(19),
            ..Timings::default()
        };
        assert_eq!(lifecycle_sum(&timings, Some(23)), 83);
    }

    #[test]
    fn mutation_plan_has_disjoint_ranges_and_exact_half_reinsert() {
        let spec = case_spec(20, 4 * 1024, 1);
        let workload = prepare_workload(&spec).unwrap();
        assert_eq!(workload.overwrite_count, 5);
        assert_eq!(workload.delete_count, 2);
        assert_eq!(workload.reinsert_count, 1);
        assert_eq!(workload.expected.len(), 19);
        assert_eq!(workload.append_metrics.operations, 20);
        assert_eq!(workload.mutation_metrics.operations, 8);
        assert_eq!(workload.append_metrics.batch_calls, 20);
        assert_eq!(workload.mutation_metrics.batch_calls, 8);
        assert_eq!(workload.deleted_probe, key(6));
        assert!(workload.expected.get(&key(6)).is_none());
        assert!(workload.expected.get(&key(5)).is_some());
        assert!(
            workload
                .expected
                .get(&key(0))
                .is_some_and(|value| value == &seeded_value(0, 1, 4 * 1024))
        );
    }

    #[test]
    fn census_categories_are_relative_to_the_store_root() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("wal").join("dataset");
        fs::create_dir_all(root.join("wal")).unwrap();
        fs::create_dir_all(root.join("sstable/level-0")).unwrap();
        fs::write(root.join("wal/000001.wal"), b"wal").unwrap();
        fs::write(root.join("sstable/level-0/000001.sst"), b"sst").unwrap();
        fs::write(root.join("MANIFEST"), b"m").unwrap();
        fs::write(root.join("unknown"), b"o").unwrap();

        let result = census(&root).unwrap();
        assert_eq!(result.total_file_logical_bytes, 8);
        assert_eq!(result.file_count, 4);
        assert_eq!(result.categories["wal"].logical_bytes, 3);
        assert_eq!(result.categories["sstable"].logical_bytes, 3);
        assert_eq!(result.categories["manifest"].logical_bytes, 1);
        assert_eq!(result.categories["other"].logical_bytes, 1);
    }

    #[test]
    fn dataset_root_requires_empty_exclusive_unlinked_path() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("empty");
        fs::create_dir(&root).unwrap();
        let (_claimed, lock) = claim_owned_data_dir(&root).unwrap();
        assert!(claim_owned_data_dir(&root).is_err());
        drop(lock);

        let occupied = temp.path().join("occupied");
        fs::create_dir(&occupied).unwrap();
        fs::write(occupied.join("keep"), b"do not delete").unwrap();
        assert!(claim_owned_data_dir(&occupied).is_err());
        assert_eq!(fs::read(occupied.join("keep")).unwrap(), b"do not delete");

        #[cfg(unix)]
        {
            let target = temp.path().join("target");
            fs::create_dir(&target).unwrap();
            let link = temp.path().join("link");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            assert!(claim_owned_data_dir(&link.join("dataset")).is_err());
        }
    }

    #[test]
    fn case_bounds_reject_oversized_inputs_and_accept_matrix_ceiling() {
        assert_eq!(benchmark_specs().len(), 8);
        assert_eq!(pilot_specs().len(), 1);
        let mut spec = case_spec(20, 4 * 1024, 1);
        assert!(validate_case(&spec.info).is_ok());
        spec.info.records = MAX_RECORDS + 1;
        assert!(validate_case(&spec.info).is_err());
        spec.info.records = MAX_RECORDS;
        spec.info.value_bytes = MAX_VALUE_BYTES + 1;
        assert!(validate_case(&spec.info).is_err());
        spec.info.value_bytes = MAX_VALUE_BYTES;
        spec.info.batch_size = MAX_BATCH_SIZE + 1;
        assert!(validate_case(&spec.info).is_err());
        spec.info.records = 512;
        spec.info.batch_size = 1;
        assert!(validate_case(&spec.info).is_err());
    }
}
