use crossbeam_skiplist::SkipMap;

use crate::storage::log::RecordType;

pub struct FlushSignal {
    pub value: SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    pub wal_path: String,
    pub file_id: u64,
}

pub struct CompactionSignal {
    pub files_to_compact: Vec<std::path::PathBuf>,
    pub compaction_level: u32,
    pub file_id: u64,
}
