use fastbloom::BloomFilter;
use std::io::Read;

// Wrapper around fastbloom's BloomFilter for efficient membership testing in SSTable blocks
pub struct BloomFilterWrapper {
    filter: BloomFilter,
}

impl std::fmt::Debug for BloomFilterWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomFilterWrapper")
            .field("num_bits", &self.filter.num_bits())
            .field("num_hashes", &self.filter.num_hashes())
            .finish()
    }
}

impl BloomFilterWrapper {
    /// Create a new Bloom filter with specified number of bits and expected items
    pub fn new(num_bits: usize, expected_items: usize) -> Self {
        BloomFilterWrapper {
            filter: BloomFilter::with_num_bits(num_bits).expected_items(expected_items),
        }
    }

    /// Create a Bloom filter optimized for expected number of items
    pub fn with_rate(expected_items: usize, false_positive_rate: f64) -> Self {
        BloomFilterWrapper {
            filter: BloomFilter::with_false_pos(false_positive_rate).expected_items(expected_items),
        }
    }

    /// Insert a key into the Bloom filter
    pub fn insert(&mut self, key: &[u8]) {
        self.filter.insert(key);
    }

    /// Check if a key might be in the set
    pub fn contains(&self, key: &[u8]) -> bool {
        self.filter.contains(key)
    }

    /// Encode the Bloom filter to bytes
    /// Stores: [num_bits: u64][num_hashes: u32][bitmap_u64_words: u64][bitmap_bytes: ...]
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Write num_bits
        let num_bits = self.filter.num_bits() as u64;
        buf.extend_from_slice(&num_bits.to_be_bytes());

        // Write num_hashes
        let num_hashes = self.filter.num_hashes();
        buf.extend_from_slice(&num_hashes.to_be_bytes());

        // Write bitmap as u64 words (fastbloom uses u64 internally)
        let bitmap_slice = self.filter.as_slice();
        let num_words = bitmap_slice.len() as u64;
        buf.extend_from_slice(&num_words.to_be_bytes());

        // Write each u64 word as bytes
        for &word in bitmap_slice {
            buf.extend_from_slice(&word.to_be_bytes());
        }

        buf
    }

    /// Decode a Bloom filter from bytes
    pub fn decode<R: Read>(mut reader: R) -> Result<Self, std::io::Error> {
        let mut buf = [0u8; 8];

        // Read num_bits
        reader.read_exact(&mut buf)?;
        let num_bits = u64::from_be_bytes(buf) as usize;

        // Read num_hashes
        let mut num_hashes_buf = [0u8; 4];
        reader.read_exact(&mut num_hashes_buf)?;
        let num_hashes = u32::from_be_bytes(num_hashes_buf);

        // Read number of u64 words
        reader.read_exact(&mut buf)?;
        let num_words = u64::from_be_bytes(buf) as usize;

        // Read bitmap words
        let mut bitmap_words = Vec::with_capacity(num_words);
        for _ in 0..num_words {
            reader.read_exact(&mut buf)?;
            bitmap_words.push(u64::from_be_bytes(buf));
        }

        // Reconstruct filter from bitmap words
        // from_vec creates a builder with bits sized to the vector, then we set hashes
        let filter = BloomFilter::from_vec(bitmap_words).hashes(num_hashes);

        Ok(BloomFilterWrapper { filter })
    }

    /// Clear all bits in the filter
    pub fn clear(&mut self) {
        self.filter.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bloom_filter_basic() {
        let mut filter = BloomFilterWrapper::with_rate(100, 0.01);

        // Insert some keys
        filter.insert(b"key1");
        filter.insert(b"key2");
        filter.insert(b"key3");

        // Should contain inserted keys
        assert!(filter.contains(b"key1"));
        assert!(filter.contains(b"key2"));
        assert!(filter.contains(b"key3"));

        // Should not contain non-inserted keys (with high probability)
        assert!(!filter.contains(b"key4"));
        assert!(!filter.contains(b"nonexistent"));
    }

    #[test]
    fn test_bloom_filter_encode_decode() {
        let mut filter = BloomFilterWrapper::with_rate(100, 0.01);

        filter.insert(b"apple");
        filter.insert(b"banana");
        filter.insert(b"cherry");

        // Encode
        let encoded = filter.encode();
        log::info!("Encoded size: {} bytes", encoded.len());

        // Decode
        let decoded = BloomFilterWrapper::decode(Cursor::new(&encoded)).unwrap();

        // Verify decoded filter works correctly
        assert!(decoded.contains(b"apple"));
        assert!(decoded.contains(b"banana"));
        assert!(decoded.contains(b"cherry"));
        assert!(!decoded.contains(b"dragonfruit"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut filter = BloomFilterWrapper::with_rate(1000, 0.01);

        // Insert 1000 keys
        for i in 0..1000 {
            let key = format!("key{}", i);
            filter.insert(key.as_bytes());
        }

        // Check false positive rate
        let mut false_positives = 0;
        let test_count = 10000;

        for i in 1000..1000 + test_count {
            let key = format!("key{}", i);
            if filter.contains(key.as_bytes()) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / test_count as f64;
        log::info!("False positive rate: {:.4} (expected ~0.01)", fpr);

        // FPR should be reasonably close to target (within 3x for small sample)
        assert!(fpr < 0.03, "False positive rate {} too high", fpr);
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut filter = BloomFilterWrapper::with_rate(100, 0.01);

        let keys = vec![b"test1", b"test2", b"test3", b"test4", b"test5"];

        // Insert all keys
        for key in &keys {
            filter.insert(*key);
        }

        // All inserted keys must be found (no false negatives)
        for key in &keys {
            assert!(
                filter.contains(*key),
                "False negative for key: {:?}",
                String::from_utf8_lossy(*key)
            );
        }
    }

    #[test]
    fn test_bloom_filter_clear() {
        let mut filter = BloomFilterWrapper::with_rate(100, 0.01);

        filter.insert(b"key1");
        filter.insert(b"key2");

        assert!(filter.contains(b"key1"));

        filter.clear();

        // After clear, should not contain anything
        assert!(!filter.contains(b"key1"));
        assert!(!filter.contains(b"key2"));
    }
}
