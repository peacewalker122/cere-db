pub const MEMTABLE_SIZE_THRESHOLD: u64 = 409600; // 4MB
pub const SSTABLE_BLOCK_SIZE: usize = 4096; // 4KB

// WAL (Write-Ahead Log) constants
pub const WAL_HEADER_SIZE: u64 = 32; // Header size in bytes
pub const WAL_MAGIC: &[u8; 8] = b"WALMGIC\0"; // Magic number for WAL file identification
pub const WAL_VERSION: u64 = 1; // WAL format version

pub const MAXIMUM_LEVEL_FILES: usize = 2; // Maximum number of files allowed in each level before triggering compaction
