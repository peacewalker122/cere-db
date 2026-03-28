use crossbeam_skiplist::SkipMap;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;

use crate::storage::block::Block;
use crate::storage::record::Record;
use crate::{error::DBError, storage::record::RecordType};

pub trait SortedRecordSource {
    fn next_record(&mut self) -> Result<Option<Record>, DBError>;
}

type MemtableIter<'a> = crossbeam_skiplist::map::Iter<'a, Vec<u8>, (RecordType, Vec<u8>)>;

struct MemtableSource<'a> {
    iter: MemtableIter<'a>,
}

impl<'a> MemtableSource<'a> {
    fn new(memtable: &'a SkipMap<Vec<u8>, (RecordType, Vec<u8>)>) -> Self {
        Self {
            iter: memtable.iter(),
        }
    }
}

impl<'a> SortedRecordSource for MemtableSource<'a> {
    fn next_record(&mut self) -> Result<Option<Record>, DBError> {
        let entry = match self.iter.next() {
            Some(entry) => entry,
            None => return Ok(None),
        };

        let (record_type, value) = entry.value();
        Ok(Some(Record::new(
            entry.key().to_owned(),
            value.to_owned(),
            *record_type,
            crate::storage::record::current_timestamp_millis(),
        )))
    }
}

pub struct SSTableSource {
    blocks: Vec<Block>,
    current_block_idx: usize,
    current_block_record_idx: usize,
}

impl SSTableSource {
    pub fn new(blocks: Vec<Block>) -> Self {
        let mut source = Self {
            blocks,
            current_block_idx: 0,
            current_block_record_idx: 0,
        };
        source.load_next_block();
        source
    }

    fn load_next_block(&mut self) {
        if self.current_block_idx >= self.blocks.len() {
            return;
        }

        let _block = &self.blocks[self.current_block_idx];
        self.current_block_record_idx = 0;
        self.current_block_idx += 1;
    }
}

impl SortedRecordSource for SSTableSource {
    fn next_record(&mut self) -> Result<Option<Record>, DBError> {
        loop {
            if self.current_block_idx == 0 || self.current_block_idx > self.blocks.len() {
                return Ok(None);
            }

            let block_idx = self.current_block_idx - 1;
            let should_advance = match self.blocks[block_idx].data.as_ref() {
                Some(records) if self.current_block_record_idx < records.len() => {
                    let rec = records[self.current_block_record_idx].clone();
                    self.current_block_record_idx += 1;
                    return Ok(Some(rec));
                }
                _ => true,
            };

            if should_advance {
                if self.current_block_idx >= self.blocks.len() {
                    return Ok(None);
                }
                self.load_next_block();
            }
        }
    }
}

#[derive(Debug)]
struct HeapItem {
    record: Record,
    source_id: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.record == other.record && self.source_id == other.source_id
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.record.cmp(&other.record) {
            Ordering::Equal => self.source_id.cmp(&other.source_id),
            ordering => ordering,
        }
    }
}

fn build_merge_sources<'a>(
    memtable: &'a SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    sstables: &'a [Block],
) -> Vec<Box<dyn SortedRecordSource + 'a>> {
    let mut sources: Vec<Box<dyn SortedRecordSource + 'a>> = Vec::new();
    sources.push(Box::new(MemtableSource::new(memtable)));
    sources.push(Box::new(SSTableSource::new(sstables.to_vec())));
    sources
}

pub fn merge_record_sources<'a>(
    mut sources: Vec<Box<dyn SortedRecordSource + 'a>>,
) -> Result<Vec<Record>, DBError> {
    let mut minheap = BinaryHeap::with_capacity(sources.len());

    for (source_id, source) in sources.iter_mut().enumerate() {
        if let Some(record) = source.next_record()? {
            minheap.push(Reverse(HeapItem { record, source_id }));
        }
    }

    let mut merged_records = Vec::new();
    while let Some(Reverse(item)) = minheap.pop() {
        let mut current = item.record;
        let key = current.key.clone();

        if let Some(next) = sources[item.source_id].next_record()? {
            minheap.push(Reverse(HeapItem {
                record: next,
                source_id: item.source_id,
            }));
        }

        while let Some(Reverse(peek)) = minheap.peek() {
            if peek.record.key != key {
                break;
            }

            let Reverse(duplicate) = minheap.pop().expect("peeked entry exists");
            if duplicate.record.timestamp > current.timestamp {
                current = duplicate.record;
            }

            if let Some(next) = sources[duplicate.source_id].next_record()? {
                minheap.push(Reverse(HeapItem {
                    record: next,
                    source_id: duplicate.source_id,
                }));
            }
        }

        merged_records.push(current);
    }

    Ok(merged_records)
}

// k-way streaming merge: O(K) heap where K = number of sources
pub fn merge_sstables(
    memtable: &SkipMap<Vec<u8>, (RecordType, Vec<u8>)>,
    sstables: &Vec<Block>,
) -> Result<Vec<Record>, DBError> {
    let sources = build_merge_sources(memtable, sstables);
    merge_record_sources(sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_sstables_empty() {
        // Test merging empty memtable with empty SSTables
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        let sstables: Vec<Block> = vec![];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(
            result.len(),
            0,
            "Merging empty inputs should return empty result"
        );
    }

    #[test]
    fn test_merge_sstables_memtable_only() {
        // Test merging memtable with no SSTables - should return sorted memtable records
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
        memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));
        memtable.insert(b"bird".to_vec(), (RecordType::Put, b"tweet".to_vec()));

        let sstables: Vec<Block> = vec![];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 3, "Should have 3 records from memtable");

        // Verify sorted order (bird, cat, dog)
        assert_eq!(result[0].key, b"bird", "First record should be 'bird'");
        assert_eq!(result[0].value, b"tweet");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"cat", "Second record should be 'cat'");
        assert_eq!(result[1].value, b"meow");

        assert_eq!(result[2].key, b"dog", "Third record should be 'dog'");
        assert_eq!(result[2].value, b"woof");
    }

    #[test]
    fn test_merge_sstables_sstable_only() {
        // Test merging empty memtable with SSTable data - should return sorted SSTable records
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();

        // Create a block with data
        let block_data = vec![
            Record::new(b"apple".to_vec(), b"red".to_vec(), RecordType::Put, 1000),
            Record::new(
                b"banana".to_vec(),
                b"yellow".to_vec(),
                RecordType::Put,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"banana".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 2, "Should have 2 records from SSTable");

        // Verify sorted order (apple, banana)
        assert_eq!(result[0].key, b"apple", "First record should be 'apple'");
        assert_eq!(result[0].value, b"red");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"banana", "Second record should be 'banana'");
        assert_eq!(result[1].value, b"yellow");
    }

    #[test]
    fn test_merge_sstables_memtable_and_sstable() {
        // Test merging memtable and SSTable with non-overlapping keys
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"dog".to_vec(), (RecordType::Put, b"woof".to_vec()));
        memtable.insert(b"cat".to_vec(), (RecordType::Put, b"meow".to_vec()));

        // Create SSTable block
        let block_data = vec![
            Record::new(b"apple".to_vec(), b"red".to_vec(), RecordType::Put, 1000),
            Record::new(
                b"banana".to_vec(),
                b"yellow".to_vec(),
                RecordType::Put,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"banana".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 4, "Should have 4 merged records");

        // Verify sorted order: apple, banana, cat, dog
        assert_eq!(result[0].key, b"apple");
        assert_eq!(result[1].key, b"banana");
        assert_eq!(result[2].key, b"cat");
        assert_eq!(result[3].key, b"dog");
    }

    #[test]
    fn test_merge_sstables_duplicate_keys_memtable_wins() {
        // Test merging records with duplicate keys from memtable and SSTable
        // Current implementation returns all records sorted (no deduplication yet)
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"new_value".to_vec()));

        // Create SSTable block with same key
        let block_data = vec![Record::new(
            b"key1".to_vec(),
            b"old_value".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block = Block {
            offset: 0,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Current implementation returns both records sorted by key, then by timestamp (desc)
        assert_eq!(result.len(), 1, "Should have 1 records");

        // Both records should have key "key1", sorted by timestamp (newer first)
        assert_eq!(result[0].key, b"key1");

        assert_eq!(
            result[0].value, b"new_value",
            "First should be memtable record"
        );

        assert!(
            result[0].timestamp != 0,
            "First should have memtable timestamp (0)"
        );
    }

    #[test]
    fn test_merge_sstables_duplicate_keys_sstable_wins() {
        // Test merging records with duplicate keys from multiple SSTables
        // Current implementation returns all records sorted (no deduplication yet)
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();

        // Create two SSTable blocks with same key but different timestamps
        let block_data1 = vec![Record::new(
            b"key1".to_vec(),
            b"old_value".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block1 = Block {
            offset: 0,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data1),
        };

        let block_data2 = vec![Record::new(
            b"key1".to_vec(),
            b"new_value".to_vec(),
            RecordType::Put,
            2000,
        )];

        let block2 = Block {
            offset: 100,
            first_key: b"key1".to_vec(),
            last_key: b"key1".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data2),
        };

        let sstables = vec![block1, block2];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Current implementation returns both records sorted by key, then by timestamp (desc)
        assert_eq!(
            result.len(),
            1,
            "Should have 1 records (no deduplication yet)"
        );

        // Both records should have key "key1", sorted by timestamp (newer first)
        assert_eq!(result[0].key, b"key1");

        // Verify newer record comes first due to Record::Ord impl (descending timestamp)
        assert_eq!(
            result[0].timestamp, 2000,
            "First should have newer timestamp"
        );
        assert_eq!(
            result[0].value, b"new_value",
            "First should be newer record"
        );
    }

    #[test]
    fn test_merge_sstables_multiple_sstables() {
        // Test merging multiple SSTables with non-overlapping keys
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"zebra".to_vec(), (RecordType::Put, b"stripes".to_vec()));

        // SSTable 1
        let block_data1 = vec![Record::new(
            b"apple".to_vec(),
            b"red".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block1 = Block {
            offset: 0,
            first_key: b"apple".to_vec(),
            last_key: b"apple".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data1),
        };

        // SSTable 2
        let block_data2 = vec![Record::new(
            b"mango".to_vec(),
            b"orange".to_vec(),
            RecordType::Put,
            1000,
        )];

        let block2 = Block {
            offset: 100,
            first_key: b"mango".to_vec(),
            last_key: b"mango".to_vec(),
            record_count: 1,
            data_size: 50,
            data: Some(block_data2),
        };

        let sstables = vec![block1, block2];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 3, "Should have 3 records from all sources");

        // Verify sorted order: apple, mango, zebra
        assert_eq!(result[0].key, b"apple");
        assert_eq!(result[1].key, b"mango");
        assert_eq!(result[2].key, b"zebra");
    }

    #[test]
    fn test_merge_sstables_preserves_record_types() {
        // Test that both Put and Delete record types are preserved
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"deleted_key".to_vec(), (RecordType::Delete, b"".to_vec()));
        memtable.insert(b"active_key".to_vec(), (RecordType::Put, b"value".to_vec()));

        // Create SSTable block with mixed types
        let block_data = vec![
            Record::new(
                b"another_active".to_vec(),
                b"data".to_vec(),
                RecordType::Put,
                1000,
            ),
            Record::new(
                b"another_deleted".to_vec(),
                b"".to_vec(),
                RecordType::Delete,
                1000,
            ),
        ];

        let block = Block {
            offset: 0,
            first_key: b"another_active".to_vec(),
            last_key: b"another_deleted".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 4, "Should have 4 records");

        // Verify record types are preserved
        // Sorted order: active_key, another_active, another_deleted, deleted_key
        assert_eq!(result[0].key, b"active_key");
        assert_eq!(result[0].record_type, RecordType::Put);

        assert_eq!(result[1].key, b"another_active");
        assert_eq!(result[1].record_type, RecordType::Put);

        assert_eq!(result[2].key, b"another_deleted");
        assert_eq!(result[2].record_type, RecordType::Delete);

        assert_eq!(result[3].key, b"deleted_key");
        assert_eq!(result[3].record_type, RecordType::Delete);
    }

    #[test]
    fn test_merge_sstables_sorted_output() {
        // Test that output is correctly sorted by key regardless of input order
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        // Insert in random order
        memtable.insert(b"z_last".to_vec(), (RecordType::Put, b"zzz".to_vec()));
        memtable.insert(b"a_first".to_vec(), (RecordType::Put, b"aaa".to_vec()));
        memtable.insert(b"m_middle".to_vec(), (RecordType::Put, b"mmm".to_vec()));

        // Create SSTable block with random order keys
        let block_data = vec![
            Record::new(b"b_two".to_vec(), b"bbb".to_vec(), RecordType::Put, 1000),
            Record::new(b"d_four".to_vec(), b"ddd".to_vec(), RecordType::Put, 1000),
        ];

        let block = Block {
            offset: 0,
            first_key: b"b_two".to_vec(),
            last_key: b"d_four".to_vec(),
            record_count: 2,
            data_size: 100,
            data: Some(block_data),
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        assert_eq!(result.len(), 5, "Should have 5 records");

        // Verify strictly ascending order
        assert_eq!(result[0].key, b"a_first");
        assert_eq!(result[1].key, b"b_two");
        assert_eq!(result[2].key, b"d_four");
        assert_eq!(result[3].key, b"m_middle");
        assert_eq!(result[4].key, b"z_last");

        // Verify each subsequent key is greater than the previous
        for i in 1..result.len() {
            assert!(
                result[i].key > result[i - 1].key,
                "Records should be in ascending order by key"
            );
        }
    }

    #[test]
    fn test_merge_sstables_empty_block_data() {
        // Test that blocks with None data field are handled correctly
        let memtable: SkipMap<Vec<u8>, (RecordType, Vec<u8>)> = SkipMap::new();
        memtable.insert(b"key1".to_vec(), (RecordType::Put, b"value1".to_vec()));

        // Create block with None data
        let block = Block {
            offset: 0,
            first_key: b"key2".to_vec(),
            last_key: b"key2".to_vec(),
            record_count: 0,
            data_size: 0,
            data: None, // No data
        };

        let sstables = vec![block];

        let result = merge_sstables(&memtable, &sstables).unwrap();

        // Should only have memtable record since block has no data
        assert_eq!(result.len(), 1, "Should have 1 record from memtable");
        assert_eq!(result[0].key, b"key1");
    }
}
