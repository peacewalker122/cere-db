use crate::storage::record::RecordType;

#[derive(Debug, Clone, PartialEq)]
pub struct LogCommand {
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub lsn: u64,
}

impl LogCommand {
    pub fn new(record_type: RecordType, key: Vec<u8>, value: Vec<u8>, lsn: u64) -> Self {
        Self {
            record_type,
            key,
            value,
            lsn,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(self.record_type as u8);

        data.extend_from_slice(&self.key.len().to_le_bytes());
        data.extend_from_slice(&self.key);
        data.extend_from_slice(&self.value.len().to_le_bytes());
        data.extend_from_slice(&self.value);
        data.extend_from_slice(&self.lsn.to_le_bytes());
        data
    }

    pub fn deserialize(data: &[u8]) -> Result<Self, std::io::Error> {
        let type_log_buf = data[0];
        let record_type = match type_log_buf {
            0 => RecordType::Put,
            1 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let key_len = u64::from_le_bytes(data[1..9].try_into().unwrap()) as usize;
        let key = data[9..9 + key_len].to_vec();

        let value_len_start = 9 + key_len;
        let value_len = u64::from_le_bytes(
            data[value_len_start..value_len_start + 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let value = data[value_len_start + 8..value_len_start + 8 + value_len].to_vec();

        let lsn_start = value_len_start + 8 + value_len;
        let lsn = u64::from_le_bytes(data[lsn_start..lsn_start + 8].try_into().unwrap());

        Ok(Self {
            record_type,
            key,
            value,
            lsn,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogPosition {
    pub lsn: u64,
}

#[async_trait::async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, cmd: LogCommand) -> Result<LogPosition, std::io::Error>;

    /// Follower replication path:
    /// Append leader entries durably, then stage them for commit-gated memtable apply.
    async fn append_entries_from_leader(
        &self,
        entries: Vec<LogCommand>,
        leader_commit_index: u64,
    ) -> Result<(), std::io::Error> {
        let _ = leader_commit_index;
        for entry in entries {
            self.append(entry).await?;
        }
        Ok(())
    }

    /// Returns newly committed entries that are ready to be applied into memtable.
    ///
    /// Default no-op keeps existing stores backward-compatible.
    async fn sync_committed_entries_to_memtable(&self) -> Result<Vec<LogCommand>, std::io::Error> {
        Ok(Vec::new())
    }

    async fn recover_commands(&self) -> Result<Vec<LogCommand>, std::io::Error>;
    async fn rotate(&self) -> Result<u64, std::io::Error>;
    async fn mark_reserved(&self, segment_id: u64) -> Result<(), std::io::Error>;
}
