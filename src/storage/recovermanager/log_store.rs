use crate::storage::record::RecordType;

#[derive(Debug, Clone, PartialEq)]
pub struct LogCommand {
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lsn: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogPosition {
    pub lsn: u64,
}

#[async_trait::async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, cmd: LogCommand) -> Result<LogPosition, std::io::Error>;
    async fn recover_commands(&self) -> Result<Vec<LogCommand>, std::io::Error>;
    async fn rotate(&self) -> Result<u64, std::io::Error>;
    async fn mark_reserved(&self, segment_id: u64) -> Result<(), std::io::Error>;
}
