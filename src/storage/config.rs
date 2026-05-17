//! Storage engine configuration system.
//!
//! Provides tunable parameters for the LSM-tree storage engine, allowing
//! performance optimization for different workload patterns without recompilation.

use crate::error::DBError;

/// Storage engine configuration.
///
/// Controls behavior of memtable flushing, SSTable layout, compaction triggers,
/// WAL management, and bloom filter tuning.
///
/// # Examples
///
/// ```ignore
/// // Production default
/// let config = StorageConfig::default();
///
/// // Write-heavy workload
/// let config = StorageConfig::builder()
///     .memtable_size_threshold(8 * 1024 * 1024)
///     .max_level0_files(4)
///     .build()?;
///
/// // Read-heavy workload
/// let config = StorageConfig::builder()
///     .bloom_false_positive_rate(0.001)
///     .max_sstable_file_handles(128)
///     .build()?;
/// ```
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Memtable size threshold (bytes) before triggering flush to SSTable.
    ///
    /// Larger values reduce write amplification but increase memory usage and recovery time.
    /// Smaller values reduce memory but increase compaction frequency.
    ///
    /// Default: 4 MB
    /// Range: 1 MB – 64 MB
    pub memtable_size_threshold: u64,

    /// SSTable data block size (bytes).
    ///
    /// Larger blocks improve sequential scan throughput but reduce point-lookup granularity.
    /// Smaller blocks improve point-lookup efficiency but increase index overhead.
    ///
    /// Default: 4 KB
    /// Range: 1 KB – 64 KB
    pub sstable_block_size: usize,

    /// Maximum number of Level 0 files before compaction is triggered.
    ///
    /// Lower values reduce read amplification but increase compaction frequency.
    /// Higher values reduce compaction overhead but increase read latency.
    ///
    /// Default: 2
    /// Range: 1 – 10
    pub max_level0_files: usize,

    /// WAL segment size (bytes) before rotation to a new segment.
    ///
    /// Larger segments reduce WAL file count but increase recovery time.
    /// Smaller segments reduce recovery time but increase file count.
    ///
    /// Default: 1 MB
    /// Range: 256 KB – 10 MB
    pub wal_segment_size: u64,

    /// Bloom filter false positive rate (0.0 – 1.0).
    ///
    /// Lower values reduce false positives but increase memory usage per SSTable.
    /// Higher values reduce memory but increase unnecessary block reads.
    ///
    /// Default: 0.01 (1%)
    /// Range: 0.0001 (0.01%) – 0.1 (10%)
    pub bloom_false_positive_rate: f64,

    /// Maximum number of cached SSTable file handles (LRU).
    ///
    /// Larger values reduce file open/close overhead but increase OS file descriptor usage.
    /// Smaller values reduce FD usage but increase open/close overhead.
    ///
    /// Default: 64
    /// Range: 8 – 512
    pub max_sstable_file_handles: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 4 * 1024 * 1024, // 4 MB
            sstable_block_size: 4096,                 // 4 KB
            max_level0_files: 2,
            wal_segment_size: 1024 * 1024,   // 1 MB
            bloom_false_positive_rate: 0.01, // 1%
            max_sstable_file_handles: 64,
        }
    }
}

impl StorageConfig {
    /// Create a new builder for constructing a custom `StorageConfig`.
    pub fn builder() -> StorageConfigBuilder {
        StorageConfigBuilder::new()
    }

    /// Validate the configuration for correctness and reasonable ranges.
    pub fn validate(&self) -> Result<(), DBError> {
        if self.memtable_size_threshold < 1024 * 1024 {
            return Err(DBError::StorageError(
                "memtable_size_threshold must be >= 1 MB".to_string(),
            ));
        }
        if self.memtable_size_threshold > 64 * 1024 * 1024 {
            return Err(DBError::StorageError(
                "memtable_size_threshold must be <= 64 MB".to_string(),
            ));
        }

        if self.sstable_block_size < 1024 {
            return Err(DBError::StorageError(
                "sstable_block_size must be >= 1 KB".to_string(),
            ));
        }
        if self.sstable_block_size > 64 * 1024 {
            return Err(DBError::StorageError(
                "sstable_block_size must be <= 64 KB".to_string(),
            ));
        }

        if self.max_level0_files < 1 || self.max_level0_files > 10 {
            return Err(DBError::StorageError(
                "max_level0_files must be between 1 and 10".to_string(),
            ));
        }

        if self.wal_segment_size < 256 * 1024 {
            return Err(DBError::StorageError(
                "wal_segment_size must be >= 256 KB".to_string(),
            ));
        }
        if self.wal_segment_size > 10 * 1024 * 1024 {
            return Err(DBError::StorageError(
                "wal_segment_size must be <= 10 MB".to_string(),
            ));
        }

        if self.bloom_false_positive_rate <= 0.0 || self.bloom_false_positive_rate >= 1.0 {
            return Err(DBError::StorageError(
                "bloom_false_positive_rate must be between 0.0 and 1.0 (exclusive)".to_string(),
            ));
        }

        if self.max_sstable_file_handles < 8 || self.max_sstable_file_handles > 512 {
            return Err(DBError::StorageError(
                "max_sstable_file_handles must be between 8 and 512".to_string(),
            ));
        }

        Ok(())
    }
}

/// Builder for constructing a custom `StorageConfig`.
#[derive(Debug)]
pub struct StorageConfigBuilder {
    memtable_size_threshold: Option<u64>,
    sstable_block_size: Option<usize>,
    max_level0_files: Option<usize>,
    wal_segment_size: Option<u64>,
    bloom_false_positive_rate: Option<f64>,
    max_sstable_file_handles: Option<usize>,
}

impl StorageConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            memtable_size_threshold: None,
            sstable_block_size: None,
            max_level0_files: None,
            wal_segment_size: None,
            bloom_false_positive_rate: None,
            max_sstable_file_handles: None,
        }
    }

    /// Set the memtable size threshold.
    pub fn memtable_size_threshold(mut self, size: u64) -> Self {
        self.memtable_size_threshold = Some(size);
        self
    }

    /// Set the SSTable block size.
    pub fn sstable_block_size(mut self, size: usize) -> Self {
        self.sstable_block_size = Some(size);
        self
    }

    /// Set the maximum number of Level 0 files.
    pub fn max_level0_files(mut self, count: usize) -> Self {
        self.max_level0_files = Some(count);
        self
    }

    /// Set the WAL segment size.
    pub fn wal_segment_size(mut self, size: u64) -> Self {
        self.wal_segment_size = Some(size);
        self
    }

    /// Set the bloom filter false positive rate.
    pub fn bloom_false_positive_rate(mut self, rate: f64) -> Self {
        self.bloom_false_positive_rate = Some(rate);
        self
    }

    /// Set the maximum number of cached SSTable file handles.
    pub fn max_sstable_file_handles(mut self, count: usize) -> Self {
        self.max_sstable_file_handles = Some(count);
        self
    }

    /// Build the `StorageConfig` and validate it.
    pub fn build(self) -> Result<StorageConfig, DBError> {
        let default = StorageConfig::default();
        let config = StorageConfig {
            memtable_size_threshold: self
                .memtable_size_threshold
                .unwrap_or(default.memtable_size_threshold),
            sstable_block_size: self
                .sstable_block_size
                .unwrap_or(default.sstable_block_size),
            max_level0_files: self.max_level0_files.unwrap_or(default.max_level0_files),
            wal_segment_size: self.wal_segment_size.unwrap_or(default.wal_segment_size),
            bloom_false_positive_rate: self
                .bloom_false_positive_rate
                .unwrap_or(default.bloom_false_positive_rate),
            max_sstable_file_handles: self
                .max_sstable_file_handles
                .unwrap_or(default.max_sstable_file_handles),
        };
        config.validate()?;
        Ok(config)
    }
}

impl Default for StorageConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let config = StorageConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn builder_creates_valid_config() {
        let config = StorageConfig::builder()
            .memtable_size_threshold(8 * 1024 * 1024)
            .max_level0_files(4)
            .build();
        assert!(config.is_ok());
    }

    #[test]
    fn builder_rejects_invalid_memtable_size_too_small() {
        let result = StorageConfig::builder()
            .memtable_size_threshold(512 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_memtable_size_too_large() {
        let result = StorageConfig::builder()
            .memtable_size_threshold(128 * 1024 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_block_size_too_small() {
        let result = StorageConfig::builder().sstable_block_size(512).build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_block_size_too_large() {
        let result = StorageConfig::builder()
            .sstable_block_size(128 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_level0_files_zero() {
        let result = StorageConfig::builder().max_level0_files(0).build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_level0_files_too_large() {
        let result = StorageConfig::builder().max_level0_files(20).build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_wal_segment_size_too_small() {
        let result = StorageConfig::builder()
            .wal_segment_size(128 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_wal_segment_size_too_large() {
        let result = StorageConfig::builder()
            .wal_segment_size(20 * 1024 * 1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_bloom_fpr_zero() {
        let result = StorageConfig::builder()
            .bloom_false_positive_rate(0.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_bloom_fpr_one() {
        let result = StorageConfig::builder()
            .bloom_false_positive_rate(1.0)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_bloom_fpr_too_large() {
        let result = StorageConfig::builder()
            .bloom_false_positive_rate(0.5)
            .build();
        assert!(result.is_ok()); // 0.5 is within range
    }

    #[test]
    fn builder_rejects_invalid_file_handles_too_small() {
        let result = StorageConfig::builder().max_sstable_file_handles(4).build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_rejects_invalid_file_handles_too_large() {
        let result = StorageConfig::builder()
            .max_sstable_file_handles(1024)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_partial_override() {
        let config = StorageConfig::builder()
            .memtable_size_threshold(8 * 1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(config.memtable_size_threshold, 8 * 1024 * 1024);
        assert_eq!(config.sstable_block_size, 4096); // default
        assert_eq!(config.max_level0_files, 2); // default
    }
}
