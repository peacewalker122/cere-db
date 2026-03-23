use std::{
    io::{BufReader, Cursor, Read, Seek, Write},
    sync::atomic::{self, AtomicU64},
};

use crate::storage::log::{RecordType, WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

#[derive(Debug)]
pub struct WAL {
    pub header: WALHeader,
    pub records: Vec<WALRecord>,
}

impl WAL {
    pub fn new() -> Self {
        WAL {
            header: WALHeader::new(),
            records: Vec::new(),
        }
    }

    pub fn decode<T: Read + Seek>(data: T) -> Result<Self, std::io::Error> {
        let mut reader = BufReader::new(data);
        let mut header_buf = vec![0u8; WAL_HEADER_SIZE as usize];
        reader.read_exact(&mut header_buf)?;
        let header = WALHeader::decode(&header_buf)?;

        let mut records = Vec::new();
        loop {
            match WALRecord::decode(&mut reader) {
                Ok((record, _)) => records.push(record),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        Ok(WAL { header, records })
    }
}

/// WAL file header structure
#[derive(Debug)]
pub struct WALHeader {
    pub magic: [u8; 8],              // Magic number: "WALMGIC\0"
    pub version: u64,                // WAL format version
    pub last_checkpoint: u64,        // LSN of last checkpoint
    pub last_checkpoint_offset: u64, // Reserved for future use
}

impl WALHeader {
    /// Create a new WAL header with default values
    pub fn new() -> Self {
        WALHeader {
            magic: *WAL_MAGIC,
            version: WAL_VERSION,
            last_checkpoint: 0,
            last_checkpoint_offset: 0,
        }
    }

    /// Encode header to bytes
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WAL_HEADER_SIZE as usize);
        buf.extend_from_slice(&self.magic);
        buf.extend_from_slice(&self.version.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint.to_be_bytes());
        buf.extend_from_slice(&self.last_checkpoint_offset.to_be_bytes());
        buf
    }

    /// Decode header from bytes
    pub fn decode(buf: &Vec<u8>) -> Result<Self, std::io::Error> {
        if buf.len() < WAL_HEADER_SIZE as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too short for WAL header",
            ));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid WAL magic number",
            ));
        }

        let mut b = [0u8; 8];
        b.copy_from_slice(&buf[8..16]);
        let version = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[16..24]);
        let last_checkpoint = u64::from_be_bytes(b);

        b.copy_from_slice(&buf[24..32]);
        let last_checkpoint_offset = u64::from_be_bytes(b);

        Ok(WALHeader {
            magic,
            version,
            last_checkpoint,
            last_checkpoint_offset,
        })
    }
}

impl Default for WALHeader {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct WALRecord {
    lsn: Option<AtomicU64>,

    pub lsn_val: u64,
    pub record_type: RecordType,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WALRecord {
    pub fn new() -> Self {
        WALRecord {
            lsn: Some(AtomicU64::new(1)),
            key: Vec::new(),
            value: Vec::new(),
            record_type: RecordType::Put,
            lsn_val: 0,
        }
    }

    pub fn encode<T: Write>(
        &self,
        writer: &mut T,
        record_type: RecordType,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), std::io::Error> {
        let lsn = match &self.lsn {
            Some(atomic_lsn) => atomic_lsn.fetch_add(1, atomic::Ordering::SeqCst),
            None => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "LSN not initialized",
            ))?,
        };

        writer.write_all(&(record_type as u8).to_be_bytes())?;
        writer.write_all(&lsn.to_be_bytes())?;
        writer.write_all(&(key.len() as u64).to_be_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(value.len() as u64).to_be_bytes())?;
        writer.write_all(value)?;

        let checksum = crc32fast::hash(value);
        writer.write_all(&checksum.to_be_bytes())?;

        log::info!(
            "Encoded record with LSN {}: key_len={},key={}, value_len={}, value={}, checksum={}",
            lsn,
            key.len(),
            String::from_utf8_lossy(key),
            value.len(),
            String::from_utf8_lossy(value),
            checksum
        );

        Ok(())
    }

    pub fn decode<T: Read + Seek>(data: T) -> Result<(Self, usize), std::io::Error> {
        let mut buf = BufReader::new(data);

        let mut record_type_buf = [0u8; 1];
        buf.read_exact(&mut record_type_buf)?;

        let record_type = match record_type_buf[0] {
            1 => RecordType::Put,
            2 => RecordType::Delete,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid record type",
                ));
            }
        };

        let mut len_buf = [0u8; 8];
        buf.read_exact(&mut len_buf)?;
        let lsn = u64::from_be_bytes(len_buf);

        buf.read_exact(&mut len_buf)?;
        let key_len = u64::from_be_bytes(len_buf) as usize;

        let mut key_bytes = vec![0u8; key_len];
        buf.read_exact(&mut key_bytes)?;

        buf.read_exact(&mut len_buf)?;
        let value_len = u64::from_be_bytes(len_buf) as usize;

        let mut value_bytes = vec![0u8; value_len];
        buf.read_exact(&mut value_bytes)?;

        let mut crc_buf = [0u8; 4];
        buf.read_exact(&mut crc_buf)?;
        let checksum = u32::from_be_bytes(crc_buf);

        log::info!(
            "Decoded record with LSN {}: key_len={}, value_len={}, checksum={}",
            lsn,
            key_len,
            value_len,
            checksum
        );

        let crc = crc32fast::hash(&value_bytes);

        if crc != checksum {
            log::error!(
                "Checksum mismatch for record with LSN {}: expected {}, got {}",
                lsn,
                checksum,
                crc32fast::hash(&value_bytes)
            );
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch",
            ));
        }

        Ok((
            WALRecord {
                lsn: None,
                lsn_val: lsn,
                key: key_bytes.to_vec(),
                value: value_bytes.to_vec(),
                record_type,
            },
            1 + 8 + 8 + key_len + 8 + value_len + 4,
        ))
    }

    pub fn get_lsn(&self) -> u64 {
        if let Some(atomic_lsn) = &self.lsn {
            atomic_lsn.load(std::sync::atomic::Ordering::SeqCst)
        } else {
            self.lsn_val
        }
    }
}

pub fn recover(data: &Vec<u8>) -> Result<Vec<WALRecord>, std::io::Error> {
    let mut records = Vec::new();
    let header = WALHeader::decode(&data)?;

    if header.magic != *WAL_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Invalid WAL magic number",
        ));
    }

    let mut start_offset = header.last_checkpoint_offset as usize;
    if start_offset == 0 {
        start_offset = WAL_HEADER_SIZE as usize;
    }

    let mut current_offset = start_offset;
    while current_offset < data.len() {
        match WALRecord::decode(Cursor::new(&data[current_offset..])) {
            Ok((record, consumed)) => {
                records.push(record);
                current_offset += consumed;
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
    }

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_header_encode_decode() {
        let header = WALHeader::new();
        let encoded = header.encode();

        let decoded = WALHeader::decode(&encoded).unwrap();
        assert_eq!(header.magic, decoded.magic);
        assert_eq!(header.version, decoded.version);
        assert_eq!(header.last_checkpoint, decoded.last_checkpoint);
        assert_eq!(
            header.last_checkpoint_offset,
            decoded.last_checkpoint_offset
        );
    }

    #[test]
    fn test_wal_record_encode_decode() {
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let record_type = RecordType::Put;
        let expected_lsn = 1;

        let mut record = WALRecord::new();
        record.key = key.clone();
        record.value = value.clone();
        record.record_type = record_type;

        let mut buf = Vec::new();
        record.encode(&mut buf, record_type, &key, &value).unwrap();

        let (decoded, _) = WALRecord::decode(Cursor::new(&buf)).unwrap();

        assert_eq!(decoded.key, key);
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.record_type, record_type);
        assert_eq!(decoded.lsn_val, expected_lsn);
    }

    #[test]
    fn test_recover() {
        let mut buf = Vec::new();

        // Write header
        let header = WALHeader::new();
        buf.extend_from_slice(&header.encode());

        // Write records
        let key1 = b"key1".to_vec();
        let val1 = b"val1".to_vec();
        let mut record1 = WALRecord::new();
        record1.key = key1.clone();
        record1.value = val1.clone();
        record1.record_type = RecordType::Put;
        record1
            .encode(&mut buf, RecordType::Put, &key1, &val1)
            .unwrap();

        let key2 = b"key2".to_vec();
        let val2 = b"val2".to_vec();
        let mut record2 = WALRecord::new();
        record2.key = key2.clone();
        record2.value = val2.clone();
        record2.record_type = RecordType::Delete;
        record2
            .encode(&mut buf, RecordType::Delete, &key2, &val2)
            .unwrap();

        let records = recover(&buf).unwrap();

        assert_eq!(records.len(), 2);

        assert_eq!(records[0].key, b"key1");
        assert_eq!(records[0].value, b"val1");
        assert_eq!(records[0].record_type, RecordType::Put);
        assert_eq!(records[0].lsn_val, 1);

        assert_eq!(records[1].key, b"key2");
        assert_eq!(records[1].value, b"val2");
        assert_eq!(records[1].record_type, RecordType::Delete);
        assert_eq!(records[1].lsn_val, 1);
    }
}
