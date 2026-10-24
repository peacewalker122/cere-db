use std::{
    error::Error,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Parser, ValueEnum};

pub type BenchResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum Suite {
    All,
    Modules,
    E2e,
}

#[derive(Debug, Parser)]
pub struct BenchConfig {
    #[arg(long)]
    pub run: bool,
    #[arg(long, hide = true)]
    pub bench: bool,
    #[arg(long, hide = true)]
    pub test: bool,
    #[arg(long, value_enum, default_value_t = Suite::All)]
    pub suite: Suite,
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..))]
    pub records: u64,
    #[arg(long, default_value_t = 256, value_parser = clap::value_parser!(u64).range(1..=1048576))]
    pub value_bytes: u64,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(1..))]
    pub repetitions: u32,
    #[arg(long, default_value_t = 1)]
    pub warmup_runs: u32,
    #[arg(long, default_value_t = 65536, value_parser = clap::value_parser!(u64).range(1..))]
    pub flush_bytes: u64,
    #[arg(long, default_value_t = 1048576, value_parser = clap::value_parser!(u64).range(1..))]
    pub wal_segment_bytes: u64,
    #[arg(long, default_value = "target/benchmark-data")]
    pub data_dir: PathBuf,
    #[arg(long)]
    pub output: Option<PathBuf>,
    #[arg(long)]
    pub filter: Option<String>,
}

impl BenchConfig {
    pub fn includes(&self, name: &str) -> bool {
        self.filter.as_ref().is_none_or(|filter| {
            if filter.contains('/') {
                name == filter
            } else {
                name.contains(filter)
            }
        })
    }

    pub fn temporary_directory(&self) -> BenchResult<tempfile::TempDir> {
        Ok(tempfile::Builder::new()
            .prefix("ceredb-bench-")
            .tempdir_in(&self.data_dir)?)
    }

    pub fn key(&self, index: u64) -> Vec<u8> {
        format!("key-{index:016x}").into_bytes()
    }

    pub fn value(&self, index: u64) -> Vec<u8> {
        vec![(index % 251) as u8; self.value_bytes as usize]
    }

    pub fn logical_bytes(&self) -> u64 {
        self.records.saturating_mul(20 + self.value_bytes)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DiskUsage {
    pub wal_bytes: u64,
    pub sstable_bytes: u64,
    pub manifest_bytes: u64,
}

impl DiskUsage {
    pub fn inspect(base: &Path) -> BenchResult<Self> {
        Ok(Self {
            wal_bytes: directory_bytes(&base.join("wal"))?,
            sstable_bytes: directory_bytes(&base.join("sstable"))?,
            manifest_bytes: file_bytes(&base.join("MANIFEST"))?,
        })
    }
}

fn file_bytes(path: &Path) -> BenchResult<u64> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn directory_bytes(path: &Path) -> BenchResult<u64> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(error.into()),
    };
    let mut bytes = 0;
    for entry in entries {
        let entry = entry?;
        let metadata = entry.metadata()?;
        bytes += if metadata.is_dir() {
            directory_bytes(&entry.path())?
        } else {
            metadata.len()
        };
    }
    Ok(bytes)
}

#[derive(Debug)]
pub struct Measurement {
    pub scenario: &'static str,
    pub sample: usize,
    pub operations: u64,
    pub logical_bytes: u64,
    pub elapsed: Duration,
    pub disk: Option<DiskUsage>,
    pub registered_sstable_count: Option<usize>,
}

impl Measurement {
    pub fn new(
        scenario: &'static str,
        sample: usize,
        operations: u64,
        logical_bytes: u64,
        elapsed: Duration,
    ) -> Self {
        Self {
            scenario,
            sample,
            operations,
            logical_bytes,
            elapsed,
            disk: None,
            registered_sstable_count: None,
        }
    }

    pub fn with_disk(mut self, disk: DiskUsage) -> Self {
        self.disk = Some(disk);
        self
    }

    pub fn with_registered_sstable_count(mut self, count: usize) -> Self {
        self.registered_sstable_count = Some(count);
        self
    }
}
