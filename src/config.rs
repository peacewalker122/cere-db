use clap::Parser;
use log::LevelFilter;

use crate::storage::config::StorageConfig;

/// Configuration for the KV store
#[derive(Parser, Debug, Clone)]
#[command(name = "ceredb")]
#[command(about = "A persistent key-value store with WAL and SSTable", long_about = None)]
pub struct Config {
    /// Set the logging level (off, error, warn, info, debug, trace)
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Enable verbose logging (equivalent to --log-level debug)
    #[arg(short, long)]
    pub verbose: bool,

    /// Data directory for storing database files
    #[arg(short, long, default_value = ".")]
    pub data_dir: String,

    /// Memtable size threshold in MB before triggering flush (default: 4)
    #[arg(long, default_value = "4")]
    pub memtable_size_mb: u64,

    /// SSTable data block size in KB (default: 4)
    #[arg(long, default_value = "4")]
    pub block_size_kb: usize,

    /// Maximum number of Level 0 files before compaction trigger (default: 2)
    #[arg(long, default_value = "2")]
    pub max_level0_files: usize,

    /// WAL segment size in MB before rotation (default: 1)
    #[arg(long, default_value = "1")]
    pub wal_segment_size_mb: u64,

    /// Bloom filter false positive rate as decimal (default: 0.01)
    #[arg(long, default_value = "0.01")]
    pub bloom_fp_rate: f64,
}

impl Config {
    pub fn get_log_level(&self) -> LevelFilter {
        if self.verbose {
            return LevelFilter::Debug;
        }

        match self.log_level.to_lowercase().as_str() {
            "off" => LevelFilter::Off,
            "error" => LevelFilter::Error,
            "warn" => LevelFilter::Warn,
            "info" => LevelFilter::Info,
            "debug" => LevelFilter::Debug,
            "trace" => LevelFilter::Trace,
            _ => {
                eprintln!("Invalid log level '{}', using 'info'", self.log_level);
                LevelFilter::Info
            }
        }
    }

    pub fn init_logger(&self) {
        env_logger::Builder::from_default_env()
            .filter_level(self.get_log_level())
            .format_timestamp_millis()
            .init();
    }

    /// Convert CLI config to a StorageConfig for the storage engine.
    pub fn to_storage_config(&self) -> Result<StorageConfig, String> {
        let config = StorageConfig::builder()
            .memtable_size_threshold(self.memtable_size_mb * 1024 * 1024)
            .sstable_block_size(self.block_size_kb * 1024)
            .max_level0_files(self.max_level0_files)
            .wal_segment_size(self.wal_segment_size_mb * 1024 * 1024)
            .bloom_false_positive_rate(self.bloom_fp_rate)
            .build()
            .map_err(|e| format!("invalid storage config: {e}"))?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Config {
        Config {
            log_level: "info".to_string(),
            verbose: false,
            data_dir: ".".to_string(),
            memtable_size_mb: 4,
            block_size_kb: 4,
            max_level0_files: 2,
            wal_segment_size_mb: 1,
            bloom_fp_rate: 0.01,
        }
    }

    #[test]
    fn test_log_level_parsing() {
        let config = Config {
            log_level: "debug".to_string(),
            ..test_config()
        };
        assert_eq!(config.get_log_level(), LevelFilter::Debug);
    }

    #[test]
    fn test_verbose_flag() {
        let config = Config {
            log_level: "info".to_string(),
            verbose: true,
            ..test_config()
        };
        assert_eq!(config.get_log_level(), LevelFilter::Debug);
    }

    #[test]
    fn test_to_storage_config_defaults() {
        let config = test_config();
        let storage_config = config.to_storage_config().unwrap();
        assert_eq!(storage_config.memtable_size_threshold, 4 * 1024 * 1024);
        assert_eq!(storage_config.sstable_block_size, 4096);
        assert_eq!(storage_config.max_level0_files, 2);
        assert_eq!(storage_config.wal_segment_size, 1024 * 1024);
        assert!((storage_config.bloom_false_positive_rate - 0.01).abs() < 1e-6);
    }

    #[test]
    fn test_to_storage_config_custom() {
        let config = Config {
            memtable_size_mb: 8,
            block_size_kb: 8,
            max_level0_files: 4,
            wal_segment_size_mb: 2,
            bloom_fp_rate: 0.001,
            ..test_config()
        };
        let storage_config = config.to_storage_config().unwrap();
        assert_eq!(storage_config.memtable_size_threshold, 8 * 1024 * 1024);
        assert_eq!(storage_config.sstable_block_size, 8 * 1024);
        assert_eq!(storage_config.max_level0_files, 4);
        assert_eq!(storage_config.wal_segment_size, 2 * 1024 * 1024);
        assert!((storage_config.bloom_false_positive_rate - 0.001).abs() < 1e-6);
    }

    #[test]
    fn test_to_storage_config_invalid_bloom_fp() {
        let config = Config {
            bloom_fp_rate: 1.0,
            ..test_config()
        };
        assert!(config.to_storage_config().is_err());
    }
}
