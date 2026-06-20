use clap::Parser;
use log::LevelFilter;

use crate::storage::config::StorageConfig;

use crate::storage::raft::RaftNodeConfig;

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

    // --- Raft consensus mode ---

    /// Enable Raft consensus mode (multi-node replication)
    #[arg(long, default_value = "false")]
    pub raft_mode: bool,

    /// Raft node identifier (numeric, used in raft-mode)
    #[arg(long, default_value = "1")]
    pub raft_node_id: u64,

    /// Comma-separated list of peer id:host:port (used in raft-mode, e.g. "2:127.0.0.1:21002,3:127.0.0.1:21003")
    #[arg(long, default_value = "")]
    pub raft_peers: String,

    /// HTTP bind address for Raft inter-node RPCs
    #[arg(long, default_value = "127.0.0.1:21001")]
    pub raft_http_bind: String,
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

    /// Build a RaftNodeConfig from CLI flags.
    ///
    /// Returns `None` if `--raft-mode` is not set.
    pub fn to_raft_node_config(&self) -> Option<RaftNodeConfig> {
        if !self.raft_mode {
            return None;
        }
        let peers: Vec<(u64, String)> = self
            .raft_peers
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                let parts: Vec<&str> = s.trim().split(':').collect();
                if parts.len() == 3 {
                    let id = parts[0].parse().ok()?;
                    let addr = format!("{}:{}", parts[1], parts[2]);
                    Some((id, addr))
                } else {
                    eprintln!("WARN: skipping invalid peer spec '{}' (expected id:host:port)", s);
                    None
                }
            })
            .collect();
        let http_bind = self
            .raft_http_bind
            .parse()
            .unwrap_or_else(|_| panic!("invalid --raft-http-bind: {}", self.raft_http_bind));
        let raft_dir = std::path::PathBuf::from(&self.data_dir).join("raft");
        Some(RaftNodeConfig {
            node_id: self.raft_node_id,
            peers,
            http_bind,
            raft_dir,
        })
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
            raft_mode: false,
            raft_node_id: 1,
            raft_peers: String::new(),
            raft_http_bind: "127.0.0.1:21001".to_string(),
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

    #[test]
    fn test_raft_config_returns_none_when_not_raft_mode() {
        let config = test_config();
        assert!(config.to_raft_node_config().is_none());
    }

    #[test]
    fn test_raft_config_parses_peers() {
        let config = Config {
            raft_mode: true,
            raft_node_id: 2,
            raft_peers: "3:127.0.0.1:21002,4:127.0.0.1:21003".to_string(),
            raft_http_bind: "127.0.0.1:21004".to_string(),
            ..test_config()
        };
        let raft_cfg = config.to_raft_node_config().expect("should return Some");
        assert_eq!(raft_cfg.node_id, 2);
        assert_eq!(raft_cfg.peers, vec![(3, "127.0.0.1:21002".to_string()), (4, "127.0.0.1:21003".to_string())]);
        assert_eq!(raft_cfg.http_bind.port(), 21004);
    }
}
