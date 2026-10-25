use crate::error::DBError;

use super::footer::SSTableFooter;

fn corrupted() -> DBError {
    DBError::Corrupted("invalid SSTable serialized lengths".into())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], DBError> {
        let end = self.position.checked_add(length).ok_or_else(corrupted)?;
        let value = self.bytes.get(self.position..end).ok_or_else(corrupted)?;
        self.position = end;
        Ok(value)
    }

    fn u32_be(&mut self) -> Result<u32, DBError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| corrupted())?,
        ))
    }

    fn u64_be(&mut self) -> Result<u64, DBError> {
        Ok(u64::from_be_bytes(
            self.take(8)?.try_into().map_err(|_| corrupted())?,
        ))
    }

    fn u32_le(&mut self) -> Result<u32, DBError> {
        Ok(u32::from_le_bytes(
            self.take(4)?.try_into().map_err(|_| corrupted())?,
        ))
    }

    fn finish(&self) -> Result<(), DBError> {
        (self.position == self.bytes.len())
            .then_some(())
            .ok_or_else(corrupted)
    }
}

struct IndexEntry {
    offset: usize,
    record_count: u32,
}

pub(super) fn validate(bytes: &[u8], footer: &SSTableFooter) -> Result<(), DBError> {
    if footer.data_block_start != 0
        || footer.data_block_end != footer.index_block_start
        || footer.index_block_start > footer.index_block_end
        || footer.index_block_end != footer.bloom_block_start
        || footer.bloom_block_start > footer.bloom_block_end
        || footer
            .bloom_block_end
            .checked_add(crate::storage::footer::FOOTER_SIZE)
            != u64::try_from(bytes.len()).ok()
    {
        return Err(corrupted());
    }

    let section = |start: u64, end: u64| -> Result<&[u8], DBError> {
        let start = usize::try_from(start).map_err(|_| corrupted())?;
        let end = usize::try_from(end).map_err(|_| corrupted())?;
        bytes.get(start..end).ok_or_else(corrupted)
    };
    let index_bytes = section(footer.index_block_start, footer.index_block_end)?;
    let mut index_cursor = Cursor::new(index_bytes);
    let count = usize::try_from(index_cursor.u64_be()?).map_err(|_| corrupted())?;
    let remaining = index_bytes.len().checked_sub(8).ok_or_else(corrupted)?;
    if count > remaining / 28 {
        return Err(corrupted());
    }
    let mut entries = Vec::with_capacity(count);
    let mut previous_offset = None;
    for _ in 0..count {
        let first_len = usize::try_from(index_cursor.u64_be()?).map_err(|_| corrupted())?;
        index_cursor.take(first_len)?;
        let offset = usize::try_from(index_cursor.u64_be()?).map_err(|_| corrupted())?;
        let last_len = usize::try_from(index_cursor.u64_be()?).map_err(|_| corrupted())?;
        index_cursor.take(last_len)?;
        let record_count = index_cursor.u32_be()?;
        if offset >= usize::try_from(footer.data_block_end).map_err(|_| corrupted())?
            || previous_offset.is_some_and(|previous| offset <= previous)
        {
            return Err(corrupted());
        }
        previous_offset = Some(offset);
        entries.push(IndexEntry {
            offset,
            record_count,
        });
    }
    index_cursor.finish()?;

    let bloom = section(footer.bloom_block_start, footer.bloom_block_end)?;
    let mut bloom_cursor = Cursor::new(bloom);
    bloom_cursor.take(16)?;
    let bit_count = usize::try_from(bloom_cursor.u64_be()?).map_err(|_| corrupted())?;
    let hashes = bloom_cursor.u32_be()?;
    let words = usize::try_from(bloom_cursor.u64_be()?).map_err(|_| corrupted())?;
    let bitmap_bytes = words.checked_mul(8).ok_or_else(corrupted)?;
    if hashes == 0
        || usize::try_from(hashes).map_err(|_| corrupted())? > bit_count
        || words == 0
        || bit_count == 0
        || bit_count.checked_add(63).ok_or_else(corrupted)? / 64 != words
        || bloom.len().checked_sub(36) != Some(bitmap_bytes)
    {
        return Err(corrupted());
    }
    bloom_cursor.take(bitmap_bytes)?;
    bloom_cursor.finish()?;

    for (position, entry) in entries.iter().enumerate() {
        let end = entries.get(position + 1).map_or_else(
            || usize::try_from(footer.data_block_end).map_err(|_| corrupted()),
            |next| Ok(next.offset),
        )?;
        let block = bytes.get(entry.offset..end).ok_or_else(corrupted)?;
        let mut cursor = Cursor::new(block);
        let first_len = cursor.u32_be()? as usize;
        cursor.take(first_len)?;
        let last_len = cursor.u32_be()? as usize;
        cursor.take(last_len)?;
        let block_count = cursor.u32_be()?;
        let data_size = usize::try_from(cursor.u32_be()?).map_err(|_| corrupted())?;
        if block_count != entry.record_count || block_count as usize > data_size / 17 {
            return Err(corrupted());
        }
        let records = cursor.take(data_size)?;
        cursor.take(4)?;
        cursor.finish()?;

        let mut record_cursor = Cursor::new(records);
        for _ in 0..block_count {
            let key_len = record_cursor.u32_le()? as usize;
            record_cursor.take(key_len)?;
            let value_len = record_cursor.u32_le()? as usize;
            record_cursor.take(value_len)?;
            let kind = record_cursor.take(1)?[0];
            if kind != 1 && kind != 2 {
                return Err(corrupted());
            }
            record_cursor.take(8)?;
        }
        record_cursor.finish()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::footer::FOOTER_SIZE;

    fn fixture(value_len: usize) -> (Vec<u8>, SSTableFooter) {
        let key = b"k";
        let value = vec![b'v'; value_len];
        let mut record = Vec::new();
        record.extend_from_slice(&(key.len() as u32).to_le_bytes());
        record.extend_from_slice(key);
        record.extend_from_slice(&(value.len() as u32).to_le_bytes());
        record.extend_from_slice(&value);
        record.push(1);
        record.extend_from_slice(&1u64.to_le_bytes());

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&(record.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&record);
        bytes.extend_from_slice(&0u32.to_be_bytes());
        let data_end = bytes.len();

        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&0u64.to_be_bytes());
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&1u32.to_be_bytes());
        let index_end = bytes.len();

        bytes.extend_from_slice(&0u128.to_be_bytes());
        bytes.extend_from_slice(&64u64.to_be_bytes());
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&1u64.to_be_bytes());
        bytes.extend_from_slice(&0u64.to_be_bytes());
        let bloom_end = bytes.len();
        let footer = SSTableFooter {
            data_block_start: 0,
            data_block_end: data_end as u64,
            index_block_start: data_end as u64,
            index_block_end: index_end as u64,
            index_checksum: 0,
            bloom_block_start: index_end as u64,
            bloom_block_end: bloom_end as u64,
            bloom_checksum: 0,
        };
        bytes.resize(bloom_end + FOOTER_SIZE as usize, 0);
        (bytes, footer)
    }

    fn set_u32(bytes: &mut [u8], offset: usize, value: u32) {
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    fn set_u64(bytes: &mut [u8], offset: usize, value: u64) {
        bytes[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
    }

    #[test]
    fn accepts_valid_and_large_record_blocks() {
        for size in [1, 8 * 1024] {
            let (bytes, footer) = fixture(size);
            validate(&bytes, &footer).unwrap();
        }
    }

    #[test]
    fn rejects_untrusted_index_and_bloom_lengths() {
        let (mut bytes, footer) = fixture(1);
        set_u64(&mut bytes, footer.index_block_start as usize, u64::MAX);
        assert!(validate(&bytes, &footer).is_err());

        let (mut bytes, footer) = fixture(1);
        let first_key_len = footer.index_block_start as usize + 8;
        set_u64(&mut bytes, first_key_len, u64::MAX);
        assert!(validate(&bytes, &footer).is_err());

        let (mut bytes, footer) = fixture(1);
        set_u64(&mut bytes, footer.bloom_block_start as usize + 28, u64::MAX);
        assert!(validate(&bytes, &footer).is_err());
    }

    #[test]
    fn rejects_gaps_between_serialized_sections() {
        let (bytes, footer) = fixture(1);

        let mut gap = footer.clone();
        gap.index_block_start += 1;
        assert!(validate(&bytes, &gap).is_err());

        let mut gap = footer.clone();
        gap.bloom_block_start += 1;
        assert!(validate(&bytes, &gap).is_err());

        let mut gap = footer;
        gap.bloom_block_end -= 1;
        assert!(validate(&bytes, &gap).is_err());
    }

    #[test]
    fn rejects_untrusted_block_and_record_lengths_and_counts() {
        let (original, footer) = fixture(1);
        for (offset, value) in [
            (0, u32::MAX),
            (5, u32::MAX),
            (10, u32::MAX),
            (14, u32::MAX),
            (18, u32::MAX),
            (23, u32::MAX),
        ] {
            let mut bytes = original.clone();
            set_u32(&mut bytes, offset, value);
            assert!(validate(&bytes, &footer).is_err(), "offset {offset}");
        }
    }
}
