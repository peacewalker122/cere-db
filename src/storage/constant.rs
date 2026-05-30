//! Hardcoded constants for storage layer.
//!
//! NOTE: These constants should eventually be moved to StorageConfig for full tunability.
//! Currently kept for backward compatibility and to minimize refactoring scope.

pub const MEMTABLE_SIZE_THRESHOLD: u64 = 4096 * 1024 * 1024; // 4MB - DEPRECATED: use StorageConfig::memtable_size_threshold
pub const SSTABLE_BLOCK_SIZE: usize = 4096; // 4KB - DEPRECATED: use StorageConfig::sstable_block_size

// WAL (Write-Ahead Log) constants
pub const WAL_HEADER_SIZE: u64 = 36; // Header size in bytes
pub const WAL_MAGIC: &[u8; 8] = b"WALMGIC\0"; // Magic number for WAL file identification
pub const WAL_VERSION: u64 = 1; // WAL format version

pub const MAXIMUM_LEVEL_FILES: usize = 2; // DEPRECATED: use StorageConfig::max_level0_files - Maximum number of files allowed in each level before triggering compaction
