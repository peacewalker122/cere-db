use std::io::{Cursor, SeekFrom};

use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

use crate::storage::{
    bloom::BloomFilterWrapper,
    sstable::{SSTableFooter, SparseIndexEntry},
    writemanager::block::Block,
};

/// Stateless SSTable representation for section-wise serialization.
///
/// Layout on disk: blocks -> sparse index -> bloom -> footer
#[derive(Debug)]
pub struct SSTableCodec {
    pub blocks: Vec<Block>,
    pub index: Vec<SparseIndexEntry>,
    pub bloom: BloomFilterWrapper,
}

impl SSTableCodec {
    pub fn new(
        blocks: Vec<Block>,
        index: Vec<SparseIndexEntry>,
        bloom: BloomFilterWrapper,
    ) -> Self {
        Self {
            blocks,
            index,
            bloom,
        }
    }

    /// Serialize in fixed order: blocks -> sparse index -> bloom -> footer.
    pub fn serialize(&self) -> (Vec<u8>, SSTableFooter) {
        let mut out = Vec::new();

        // debugging
        log::debug!(
            "Serializing SSTable with {} blocks, from {}",
            self.blocks.len(),
            out.len()
        );

        let mut indexes = Vec::new();

        // blocks
        for block in self.blocks.iter() {
            log::debug!(
                "Serializing block with first key {:?}, last key {:?}, record count {}, data size {}",
                String::from_utf8_lossy(&block.first_key),
                String::from_utf8_lossy(&block.last_key),
                block.record_count,
                block.data_size
            );

            indexes.push(SparseIndexEntry {
                first_key: block.first_key.clone(),
                last_key: block.last_key.clone(),
                block_offset: out.len() as u64,
                record_count: block.record_count,
            });

            out.extend_from_slice(&block.encode());
        }
        let block_offset = out.len() as u64;

        log::debug!(
            "Data blocks serialized, total size so far: {} bytes",
            out.len()
        );
        // sparse index
        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(indexes.len() as u64).to_be_bytes());
        for entry in indexes.iter() {
            index_block.extend_from_slice(&entry.encode());
        }
        out.extend_from_slice(&index_block);
        log::debug!(
            "Sparse index serialized, total size so far: {} bytes",
            out.len()
        );
        let index_offset = out.len() as u64;

        // bloom
        let bloom_block = self.bloom.encode();
        log::debug!(
            "Bloom filter serialized, total size so far: {} bytes",
            out.len() + bloom_block.len()
        );

        out.extend_from_slice(&bloom_block);
        let bloom_offset = out.len() as u64;

        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: block_offset,
            index_block_start: block_offset,
            index_block_end: index_offset,
            bloom_block_start: index_offset,
            bloom_block_end: bloom_offset,
            index_checksum: crc32fast::hash(&index_block),
            bloom_checksum: crc32fast::hash(&bloom_block),
        };

        // footer
        out.extend_from_slice(&footer.encode());

        (out, footer)
    }

    /// Deserialize footer + sparse index + bloom sections together.
    pub async fn deserialize_sections<R: AsyncRead + AsyncSeek + Unpin>(
        mut reader: &mut R,
    ) -> Result<(SSTableFooter, Vec<SparseIndexEntry>, BloomFilterWrapper), std::io::Error> {
        // get the footer first to know where the index and bloom sections are

        // reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64))).await?;
        let footer = SSTableFooter::async_decode(reader).await?;

        // debug the footer offsets and lengths
        log::debug!("Footer: {:?}", footer);

        // get the sparse index count first
        let mut index_len_buf = [0u8; 8];

        reader.seek(SeekFrom::Start(footer.data_block_end)).await?;
        reader.read_exact(&mut index_len_buf).await?;
        let index_len = u64::from_be_bytes(index_len_buf) as usize;
        log::debug!(
            "Index count from sstable: {}, {}",
            index_len,
            footer.index_block_end - footer.index_block_start
        );

        let mut indexes: Vec<SparseIndexEntry> = vec![];

        for _ in 0..index_len {
            let entry = SparseIndexEntry::async_decode(&mut reader).await?;
            indexes.push(entry);
        }

        // read and decode the bloom section
        reader
            .seek(SeekFrom::Start(footer.bloom_block_start))
            .await?;
        let bloom = BloomFilterWrapper::async_decode(reader).await?;

        Ok((footer, indexes, bloom))
    }

    /// Deserialize from buffered reader + seek stream.
    ///
    /// Uses footer offsets to fetch sparse index and bloom sections only,
    /// keeping block payload out of memory.
    pub async fn deserialize<R: AsyncBufRead + AsyncSeek + Unpin>(
        reader: &mut R,
    ) -> Result<Self, std::io::Error> {
        let (footer, index, bloom) = Self::deserialize_sections(reader).await?;

        Ok(Self {
            blocks: Vec::new(),
            index,
            bloom,
        })
    }

    pub fn bloom(&self) -> &BloomFilterWrapper {
        &self.bloom
    }

    pub async fn get_block<R: AsyncRead + AsyncSeek + Unpin>(
        reader: &mut R,
        footer: &SSTableFooter,
        index: &[SparseIndexEntry],
        key: &[u8],
    ) -> Result<Block, std::io::Error> {
        // find the right block using the sparse index
        let index_position = match index.binary_search_by(|entry| {
            if key < entry.first_key.as_slice() {
                std::cmp::Ordering::Greater
            } else if key > entry.last_key.as_slice() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        }) {
            Ok(pos) => pos,
            Err(pos) => {
                if pos == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "key is smaller than the first key in the index",
                    ));
                }
                pos - 1
            }
        };

        let start = index[index_position].block_offset;
        let end = if index_position + 1 < index.len() {
            index[index_position + 1].block_offset
        } else {
            footer.data_block_end
        };

        if end < start {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid block range in sparse index",
            ));
        }

        log::debug!(
            "Fetching block for key {:?} at index position {}, block offset range: {} - {}",
            String::from_utf8_lossy(key),
            index_position,
            start,
            end
        );

        reader.seek(SeekFrom::Start(start)).await?;
        let result = Block::async_decode(reader).await?;
        Ok(result)
    }
}

fn decode_sparse_index(index_block: &[u8]) -> Result<Vec<SparseIndexEntry>, std::io::Error> {
    if index_block.len() < 8 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "Index block too small",
        ));
    }

    let mut cursor = Cursor::new(index_block);
    let mut count_buf = [0u8; 8];
    std::io::Read::read_exact(&mut cursor, &mut count_buf)?;
    let count = u64::from_be_bytes(count_buf) as usize;

    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(SparseIndexEntry::decode(&mut cursor)?);
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::storage::{
        manifest_codec::ManifestManager,
        record::{MemtableRecord, RecordType},
        recovermanager::wal::WALManager,
        writemanager::write::WriteComponent,
    };
    use crossbeam_skiplist::SkipMap;

    fn build_codec_fixture() -> (SSTableCodec, Vec<u8>) {
        let block_a = Block {
            offset: 0,
            first_key: b"alpha".to_vec(),
            last_key: b"kappa".to_vec(),
            record_count: 2,
            data_size: 64,
            data: Some(vec![
                // add memtable record
                MemtableRecord {
                    key: b"alpha".to_vec(),
                    value: b"value1".to_vec(),
                    record_type: RecordType::Put,
                    lsn: 1,
                },
                MemtableRecord {
                    key: b"lambda".to_vec(),
                    value: b"value2".to_vec(),
                    record_type: RecordType::Put,
                    lsn: 2,
                },
            ]),
        };
        let block_b = Block {
            offset: block_a.encode().len() as u64,
            first_key: b"lambda".to_vec(),
            last_key: b"omega".to_vec(),
            record_count: 1,
            data_size: 48,
            data: Some(vec![
                MemtableRecord {
                    key: b"omega".to_vec(),
                    value: b"value3".to_vec(),
                    record_type: RecordType::Put,
                    lsn: 3,
                },
                MemtableRecord {
                    key: b"psi".to_vec(),
                    value: b"value4".to_vec(),
                    record_type: RecordType::Put,
                    lsn: 4,
                },
            ]),
        };

        let blocks = vec![block_a.clone(), block_b.clone()];

        let index = vec![
            SparseIndexEntry {
                first_key: block_a.first_key.clone(),
                block_offset: block_a.offset,
                last_key: block_a.last_key.clone(),
                record_count: block_a.record_count,
            },
            SparseIndexEntry {
                first_key: block_b.first_key.clone(),
                block_offset: block_b.offset,
                last_key: block_b.last_key.clone(),
                record_count: block_b.record_count,
            },
        ];

        let mut bloom = BloomFilterWrapper::with_rate(8, 0.01);
        bloom.insert(b"alpha");
        bloom.insert(b"omega");

        let data_block_end = (blocks
            .iter()
            .map(|block| block.encode().len())
            .sum::<usize>()) as u64;

        let mut index_block = Vec::new();
        index_block.extend_from_slice(&(index.len() as u64).to_be_bytes());
        for entry in index.iter() {
            index_block.extend_from_slice(&entry.encode());
        }

        let bloom_block = bloom.encode();
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end,
            index_block_start: data_block_end,
            index_block_end: data_block_end + index_block.len() as u64,
            index_checksum: crc32fast::hash(&index_block),
            bloom_block_start: data_block_end + index_block.len() as u64,
            bloom_block_end: data_block_end + index_block.len() as u64 + bloom_block.len() as u64,
            bloom_checksum: crc32fast::hash(&bloom_block),
        };

        let codec = SSTableCodec::new(blocks, index, bloom);
        let raw = codec.serialize();
        (codec, raw.0)
    }

    #[tokio::test]
    async fn deserialize_reads_footer_and_bloom() {
        let bloom = BloomFilterWrapper::with_rate(16, 0.01);
        let index = Vec::<SparseIndexEntry>::new();

        let index_block = {
            let mut buf = Vec::new();
            buf.extend_from_slice(&0u64.to_be_bytes());
            buf
        };
        let bloom_block = bloom.encode();
        // let footer = SSTableFooter {
        //     data_block_start: 0,
        //     data_block_end: 0,
        //     index_block_start: 0,
        //     index_block_end: index_block.len() as u64,
        //     index_checksum: crc32fast::hash(&index_block),
        //     bloom_block_start: index_block.len() as u64,
        //     bloom_block_end: index_block.len() as u64 + bloom_block.len() as u64,
        //     bloom_checksum: crc32fast::hash(&bloom_block),
        // };

        let codec = SSTableCodec::new(Vec::new(), index, bloom);
        let (raw, footer) = codec.serialize();
        let mut cursor = Cursor::new(raw);
        let decoded = SSTableCodec::deserialize(&mut cursor).await.unwrap();

        assert_eq!(footer.index_block_start, 0);
        assert_eq!(footer.index_block_end, index_block.len() as u64);
    }

    // #[tokio::test]
    // async fn serialize_and_deserialize_sections_roundtrip_preserves_footer_index_and_bloom() {
    //     let (codec, raw) = build_codec_fixture();
    //     let mut cursor = Cursor::new(raw);
    //
    //     let (footer, indexes, bloom) = SSTableCodec::deserialize_sections(&mut cursor)
    //         .await
    //         .unwrap();
    //
    //     assert_eq!(footer.data_block_start, codec.footer.data_block_start);
    //     assert_eq!(footer.data_block_end, codec.footer.data_block_end);
    //     assert_eq!(footer.index_block_start, codec.footer.index_block_start);
    //     assert_eq!(footer.index_block_end, codec.footer.index_block_end);
    //     assert_eq!(footer.index_checksum, codec.footer.index_checksum);
    //     assert_eq!(footer.bloom_block_start, codec.footer.bloom_block_start);
    //     assert_eq!(footer.bloom_block_end, codec.footer.bloom_block_end);
    //     assert_eq!(footer.bloom_checksum, codec.footer.bloom_checksum);
    //
    //     assert_eq!(indexes.len(), codec.index.len());
    //     for (decoded, expected) in indexes.iter().zip(codec.index.iter()) {
    //         assert_eq!(decoded.first_key, expected.first_key);
    //         assert_eq!(decoded.last_key, expected.last_key);
    //         assert_eq!(decoded.block_offset, expected.block_offset);
    //         assert_eq!(decoded.record_count, expected.record_count);
    //     }
    //
    //     assert!(bloom.contains(b"alpha"));
    //     assert!(bloom.contains(b"omega"));
    // }

    #[tokio::test]
    async fn deserialize_returns_section_metadata_without_loading_blocks() {
        let (codec, raw) = build_codec_fixture();
        let mut cursor = Cursor::new(raw);

        let decoded = SSTableCodec::deserialize(&mut cursor).await.unwrap();

        assert!(decoded.blocks.is_empty());
        assert_eq!(decoded.index.len(), codec.index.len());
        assert!(decoded.bloom().contains(b"alpha"));
        assert!(decoded.bloom().contains(b"omega"));
    }

    #[tokio::test]
    async fn deserialize_reads_sstable_written_by_flush_serialize_path() {
        env_logger::builder().is_test(true).init();

        let temp_dir = std::env::temp_dir().join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(temp_dir.join("sstable/level-0")).unwrap();
        std::fs::create_dir_all(temp_dir.join("wal")).unwrap();

        let wal_manager = WALManager::new(temp_dir.join("wal"), 1024 * 1024)
            .await
            .unwrap();
        let manifest_manager = ManifestManager::load_or_create(temp_dir.join("MANIFEST"))
            .await
            .unwrap();

        let mut write_component = WriteComponent::new(
            temp_dir.join("sstable"),
            Arc::new(wal_manager),
            manifest_manager,
        );

        let memtable: SkipMap<(Vec<u8>, u64), MemtableRecord> = SkipMap::new();
        for i in 0..1000 {
            // expected to create 1 block that will put an index entry, and a bloom filter with 10k keys
            memtable.insert(
                (format!("flush-key-{i}").as_bytes().to_vec(), i),
                MemtableRecord::new(b"flush-val-a".to_vec(), RecordType::Put, i),
            );
        }

        let flush_result = write_component.flush(memtable).await.unwrap();
        // let raw = tokio::fs::read(&flush_result.sstable_path).await.unwrap();
        let raw = flush_result.data;

        log::debug!("Raw SSTable size: {} bytes", raw.len());
        let mut cursor = Cursor::new(raw.clone());
        let (footer, index, bloom) = SSTableCodec::deserialize_sections(&mut cursor)
            .await
            .unwrap();

        // assert!(decoded.blocks.is_empty());
        assert!(!index.is_empty());
        assert!(bloom.contains(b"flush-key-1"));
        assert!(bloom.contains(b"flush-key-3"));

        // let footer = decoded.footer();
        // assert_eq!(footer.bloom_block_end + FOOTER_SIZE, raw.len() as u64);
        //
        let index_start = footer.index_block_start as usize;
        let index_end = footer.index_block_end as usize;
        let bloom_start = footer.bloom_block_start as usize;
        let bloom_end = footer.bloom_block_end as usize;

        assert_eq!(
            crc32fast::hash(&raw[index_start..index_end]),
            footer.index_checksum
        );
        assert_eq!(
            crc32fast::hash(&raw[bloom_start..bloom_end]),
            footer.bloom_checksum
        );

        // search the given blocks
        let Block = SSTableCodec::get_block(&mut cursor, &footer, &index, "flush-key-3".as_bytes())
            .await
            .unwrap();

        // assert_eq!(Block.first_key, b"flush-key-0".to_vec());
        assert!(Block.data.is_some());
        assert_ne!(Block.data.as_ref().unwrap().len(), 0);

        // check the record
        let record = Block.data.as_ref().unwrap();
        log::debug!("Decoded block record count: {}", record.len());

        for r in record.iter() {
            log::debug!(
                "Record key: {:?}, value: {:?}, type: {:?}, lsn: {}",
                String::from_utf8_lossy(&r.key),
                String::from_utf8_lossy(&r.value),
                r.record_type,
                r.lsn
            );
        }

        let found = record.iter().find(|r| r.key == b"flush-key-3".to_vec());
        assert!(found.is_some());

        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}
