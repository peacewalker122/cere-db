mod e2e;
mod modules;
mod support;

use std::{
    collections::BTreeMap,
    process::Command,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use clap::{CommandFactory, Parser};
use serde_json::{Value, json};
use support::{BenchConfig, BenchResult, Measurement, Suite};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> BenchResult {
    if !std::env::args_os().any(|argument| argument == "--run") {
        BenchConfig::command().print_help()?;
        println!();
        return Ok(());
    }
    let config = BenchConfig::parse();
    std::fs::create_dir_all(&config.data_dir)?;
    let started = Instant::now();
    for sample in 0..config.warmup_runs {
        run_sample(&config, sample as usize, &mut Vec::new()).await?;
    }
    let mut measurements = Vec::new();
    for sample in 0..config.repetitions {
        eprintln!("sample {}/{}", sample + 1, config.repetitions);
        run_sample(&config, sample as usize, &mut measurements).await?;
    }
    if measurements.is_empty() {
        return Err("no benchmark scenarios matched the selected suite/filter".into());
    }
    let report = report(&config, &measurements, started.elapsed().as_secs_f64())?;
    let rendered = serde_json::to_string_pretty(&report)?;
    if let Some(path) = &config.output {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, format!("{rendered}\n"))?;
        eprintln!("saved {}", path.display());
    }
    println!("{rendered}");
    Ok(())
}

async fn run_sample(
    config: &BenchConfig,
    sample: usize,
    measurements: &mut Vec<Measurement>,
) -> BenchResult {
    if matches!(config.suite, Suite::All | Suite::Modules) {
        modules::run(config, sample, measurements).await?;
    }
    if matches!(config.suite, Suite::All | Suite::E2e) {
        e2e::run(config, sample, measurements).await?;
    }
    Ok(())
}

fn report(
    config: &BenchConfig,
    measurements: &[Measurement],
    total_seconds: f64,
) -> BenchResult<Value> {
    let mut groups: BTreeMap<&str, Vec<&Measurement>> = BTreeMap::new();
    for measurement in measurements {
        if measurement.operations == 0 || measurement.elapsed.is_zero() {
            return Err(format!("invalid measurement for {}", measurement.scenario).into());
        }
        groups
            .entry(measurement.scenario)
            .or_default()
            .push(measurement);
    }
    let summaries: Vec<Value> = groups
        .into_iter()
        .map(|(scenario, samples)| {
            let mut elapsed: Vec<f64> = samples
                .iter()
                .map(|sample| sample.elapsed.as_secs_f64() * 1000.0)
                .collect();
            elapsed.sort_by(f64::total_cmp);
            let operations: u64 = samples.iter().map(|sample| sample.operations).sum();
            let logical_bytes: u64 = samples.iter().map(|sample| sample.logical_bytes).sum();
            let seconds: f64 = samples
                .iter()
                .map(|sample| sample.elapsed.as_secs_f64())
                .sum();
            let middle = elapsed.len() / 2;
            let median = if elapsed.len() % 2 == 0 {
                (elapsed[middle - 1] + elapsed[middle]) / 2.0
            } else {
                elapsed[middle]
            };
            json!({
                "scenario": scenario,
                "samples": samples.len(),
                "min_run_ms": elapsed[0],
                "median_run_ms": median,
                "max_run_ms": elapsed[elapsed.len() - 1],
                "operations_per_second": operations as f64 / seconds,
                "amortized_us_per_operation": seconds * 1_000_000.0 / operations as f64,
                "logical_mib_per_second": logical_bytes as f64 / seconds / 1_048_576.0,
            })
        })
        .collect();
    let samples: Vec<Value> = measurements
        .iter()
        .map(|sample| {
            json!({
                "scenario": sample.scenario,
                "sample": sample.sample,
                "operations": sample.operations,
                "logical_bytes": sample.logical_bytes,
                "elapsed_ns": sample.elapsed.as_nanos(),
                "disk": sample.disk.map(|disk| json!({
                    "wal_bytes": disk.wal_bytes,
                    "sstable_bytes": disk.sstable_bytes,
                    "manifest_bytes": disk.manifest_bytes,
                })),
            })
        })
        .collect();
    Ok(json!({
        "schema_version": 1,
        "metadata": {
            "unix_time_seconds": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "git_revision": command_output("git", &["rev-parse", "HEAD"]),
            "tracked_changes": command_output("git", &["status", "--porcelain", "--untracked-files=no"]),
            "rustc": command_output("rustc", &["--version"]),
            "os": std::env::consts::OS,
            "architecture": std::env::consts::ARCH,
            "available_parallelism": std::thread::available_parallelism()?.get(),
            "runtime_workers": 2,
            "debug_assertions": cfg!(debug_assertions),
            "data_directory": std::fs::canonicalize(&config.data_dir)?,
            "total_wall_seconds_including_setup_and_warmup": total_seconds,
            "cache_policy": "OS page cache is retained; no cold-cache claim",
            "latency_policy": "run timings and amortized costs; no request latency percentiles",
        },
        "configuration": {
            "suite": format!("{:?}", config.suite).to_lowercase(),
            "filter": config.filter,
            "records": config.records,
            "key_bytes": 20,
            "value_bytes": config.value_bytes,
            "repetitions": config.repetitions,
            "warmup_runs": config.warmup_runs,
            "flush_bytes": config.flush_bytes,
            "wal_segment_bytes": config.wal_segment_bytes,
        },
        "summaries": summaries,
        "samples": samples,
    }))
}

fn command_output(program: &str, arguments: &[&str]) -> Option<String> {
    let output = Command::new(program).args(arguments).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
