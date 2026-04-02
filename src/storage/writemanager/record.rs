use crate::storage::record::RecordType;

pub struct MemtableRecord {
    pub value: Vec<u8>,
    pub record_type: RecordType,
    pub lsn: u64,
}

impl MemtableRecord {
    pub fn new(value: Vec<u8>, record_type: RecordType, lsn: u64) -> Self {
        Self {
            value,
            record_type,
            lsn,
        }
    }

    pub fn encode(&self, key: &Vec<u8>) -> Vec<u8> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&(key.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&key);
        encoded.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&self.value);
        encoded.push(self.record_type as u8);
        encoded.extend_from_slice(&self.lsn.to_le_bytes());

        encoded
    }

    pub fn record_length(&self, key: &Vec<u8>) -> usize {
        4 + key.len() + 4 + self.value.len() + 1 + 8
    }
}
