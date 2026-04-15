use std::{
    fs::File,
    io::{BufReader, Cursor, Read, Seek, SeekFrom, Write},
    sync::atomic::{self, AtomicU64},
};

use crate::storage::{
    constant::{WAL_HEADER_SIZE, WAL_MAGIC, WAL_VERSION},
    record::RecordType,
};

#[derive(Debug)]
pub struct WAL {
    pub header: WALHeader,
    pub records: Vec<WALRecord>,
}

pub trait WALManager {
    fn store_log(
        &mut self,
        writer: Box<dyn Write>,
        key: Vec<u8>,
        value: Vec<u8>,
        record_type: RecordType,
    ) -> Result<(u64, u64), std::io::Error>;
    fn recover(&self, data: &Vec<u8>) -> Result<Vec<WALRecord>, std::io::Error>;
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

impl WALManager for WAL {
    fn store_log(
        &mut self,
        mut writer: Box<dyn Write>,
        key: Vec<u8>,
        value: Vec<u8>,
        record_type: RecordType,
    ) -> Result<(u64, u64), std::io::Error> {
        let record = WALRecord::new();
        record.encode(&mut writer, record_type, &key, &value)?;
        let lsn = record.get_lsn();
        self.records.push(record);
        Ok((lsn, 0)) // Placeholder for offset
    }

    fn recover(&self, data: &Vec<u8>) -> Result<Vec<WALRecord>, std::io::Error> {
        recover(data)
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

        let mut hash = crc32fast::Hasher::new();
        hash.update(&buf);
        buf.extend_from_slice(&hash.finalize().to_be_bytes());

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

/// Write WAL header to a file
fn write_wal_header<W: Write>(mut writer: W) -> Result<(), std::io::Error> {
    let header = WALHeader::new();
    writer.write_all(&header.encode())?;
    writer.flush()?;
    Ok(())
}

/// Read WAL header from a file
pub fn read_wal_header<R: Read>(mut reader: R) -> Result<WALHeader, std::io::Error> {
    let mut buf = vec![0u8; WAL_HEADER_SIZE as usize];
    reader.read_exact(&mut buf)?;
    WALHeader::decode(&buf)
}

/// Store a log entry to the Write-Ahead Log (WAL)
/// Returns the offset where the record was written and its LSN
pub fn store_log(
    mut file: &mut File,
    key: &Vec<u8>,
    value: &Vec<u8>,
    record_type: RecordType,
    wal: &WALRecord,
) -> Result<(u64, u64), std::io::Error> {
    // If file is new (empty), write header
    if file.metadata()?.len() == 0 {
        write_wal_header(&mut file)?;
    }

    // Get current position for offset (should be at end after calculate_next_lsn)
    let offset = file.seek(SeekFrom::End(0))?;
    let lsn = wal.get_lsn();

    // Write and sync to ensure durability
    wal.encode(&mut file, record_type, key, value)?;
    file.sync_data()?;

    Ok((offset, lsn))
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, sync::Arc, thread};

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

    #[test]
    fn test_store_and_decode_log() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_store_and_decode_log.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store a record
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Put;
        let (offset, lsn) = store_log(&mut file, &key, &value, RecordType::Put, &mut wal).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE); // First record is right after header
        assert_eq!(lsn, 1); // Current implementation returns constant LSN

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = WAL::decode(file).unwrap();

        for record in result.records {
            if record.key == b"test_key" {
                assert_eq!(record.key, b"test_key");
                assert_eq!(record.value, b"test_value");
                assert_eq!(record.record_type, RecordType::Put);
                assert_eq!(record.lsn_val, 1);
            }
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_store_tombstone() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_store_tombstone.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store a tombstone
        let key = b"deleted_key".to_vec();
        let value = Vec::new();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Delete;
        let (offset, lsn) =
            store_log(&mut file, &key, &value, RecordType::Delete, &mut wal).unwrap();
        assert_eq!(offset, WAL_HEADER_SIZE);
        assert_eq!(lsn, 1);

        // Read it back
        let file = std::fs::File::open(test_file).unwrap();
        let result = WAL::decode(file).unwrap(); // this is doesn't
        // directly read the wal, it read the header first, hence we need an approach that // can skip the header

        for record in result.records {
            if record.key == b"deleted_key" {
                assert_eq!(record.record_type, RecordType::Delete);
                assert_eq!(record.value.len(), 0);
                assert_eq!(record.lsn_val, 1);
            }
        }

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_wal_header() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_wal_header.log");
        let test_file = test_file_path.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Create a new WAL file (header will be written automatically)
        let key = b"key1".to_vec();
        let value = b"value1".to_vec();
        let mut wal = WALRecord::new();
        wal.key = key.clone();
        wal.value = value.clone();
        wal.record_type = RecordType::Put;
        store_log(&mut file, &key, &value, RecordType::Put, &mut wal).unwrap();

        // Read and validate header
        let file = std::fs::File::open(test_file).unwrap();
        let header = read_wal_header(file).unwrap();

        assert_eq!(&header.magic, WAL_MAGIC);
        assert_eq!(header.version, WAL_VERSION);
        assert_eq!(header.last_checkpoint, 0);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_lsn_increment() {
        let test_file = std::env::temp_dir().join("wasm-kv-test_wal_lsn.log");
        let test_file = test_file.to_str().unwrap();

        // Clean up any existing test file
        let _ = std::fs::remove_file(test_file);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        // Store multiple records
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let mut wal1 = WALRecord::new();
        wal1.key = key1.clone();
        wal1.value = value1.clone();
        wal1.record_type = RecordType::Put;
        let (_offset1, lsn1) =
            store_log(&mut file, &key1, &value1, RecordType::Put, &mut wal1).unwrap();

        let key2 = b"key2".to_vec();
        let value2 = b"value2".to_vec();
        let (_offset2, lsn2) =
            store_log(&mut file, &key2, &value2, RecordType::Put, &mut wal1).unwrap();

        let key3 = b"key3".to_vec();
        let value3 = b"value3".to_vec();
        let (_offset3, lsn3) =
            store_log(&mut file, &key3, &value3, RecordType::Put, &mut wal1).unwrap();

        // LSN should increment
        assert_eq!(lsn1, 1);
        assert_eq!(lsn2, 2);
        assert_eq!(lsn3, 3);

        // Clean up
        std::fs::remove_file(test_file).unwrap();
    }

    #[test]
    fn test_lsn_increment_concurrent() {
        let test_file_path = std::env::temp_dir().join("wasm-kv-test_wal_lsn_concurrent.log");
        let test_file = test_file_path.to_str().unwrap();
        let _ = std::fs::remove_file(test_file);

        const THREADS: usize = 8;

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(test_file)
            .unwrap();

        let mut handles = Vec::new();
        let wal = Arc::new(WALRecord::new());

        let (tx, rx) = std::sync::mpsc::channel::<(Vec<u8>, Vec<u8>, Arc<WALRecord>)>();

        handles.push(thread::spawn(move || {
            let (key, value, wal) = rx.recv().unwrap();

            let (_offset, lsn) = store_log(&mut file, &key, &value, RecordType::Put, &wal)
                .expect("store_log failed");

            lsn
        }));

        for i in 0..THREADS {
            let tx = tx.clone();
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            let wal = wal.clone();

            tx.send((key, value, wal)).unwrap();
        }

        // Collect LSNs
        let mut lsns: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        println!("LSNs: {:?}", lsns);

        // Sort so we can reason about ordering
        lsns.sort_unstable();

        // Expect a perfect sequence: 1..=THREADS
        for (i, lsn) in lsns.iter().enumerate() {
            assert_eq!(*lsn, (i + 1) as u64);
        }

        std::fs::remove_file(test_file).unwrap();
    }
}
