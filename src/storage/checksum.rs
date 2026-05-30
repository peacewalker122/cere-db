//! Checksum module for data block integrity verification
//!
//! Provides CRC32-IEEE checksum calculation and verification for SSTable blocks.
//! All functions are pure (side-effect free) and designed for composition.

use crate::error::DBError;
use crc32fast::Hasher;

/// Calculate CRC32-IEEE checksum for the given data.
///
/// This is a pure function: same input always produces same output.
/// Handles empty data gracefully by computing CRC32 of empty slice.
///
/// # Arguments
/// * `data` - Byte slice to checksum (can be empty)
///
/// # Returns
/// The 32-bit CRC32-IEEE checksum value
///
/// # Examples
/// ```
/// use ceredb::storage::checksum::calculate_crc32;
/// let checksum = calculate_crc32(b"hello");
/// assert_eq!(checksum, 0x3610a686); // CRC32-IEEE("hello")
/// ```
pub fn calculate_crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify that data matches the expected CRC32 checksum.
///
/// This is a pure function that performs validation without side effects.
/// Used during block decode to detect corruption.
///
/// # Arguments
/// * `data` - Byte slice to verify
/// * `expected` - Expected CRC32 value
///
/// # Returns
/// * `Ok(())` if checksum matches
/// * `Err(DBError::Corrupted)` with detailed message if mismatch
///
/// # Examples
/// ```
/// use ceredb::storage::checksum::{calculate_crc32, verify_crc32};
/// let data = b"hello";
/// let checksum = calculate_crc32(data);
/// assert!(verify_crc32(data, checksum).is_ok());
///
/// // Different data fails verification
/// assert!(verify_crc32(b"world", checksum).is_err());
/// ```
pub fn verify_crc32(data: &[u8], expected: u32) -> Result<(), DBError> {
    let actual = calculate_crc32(data);
    if actual == expected {
        Ok(())
    } else {
        Err(DBError::Corrupted(format!(
            "CRC32 mismatch: expected 0x{:08x}, got 0x{:08x}",
            expected, actual
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_crc32_empty_data() {
        // CRC32 of empty slice should be stable and well-defined
        let checksum = calculate_crc32(b"");
        // CRC32-IEEE of empty is 0
        assert_eq!(checksum, 0);
    }

    #[test]
    fn test_calculate_crc32_simple_data() {
        // Test with known data
        let data = b"hello";
        let checksum1 = calculate_crc32(data);
        let checksum2 = calculate_crc32(data);

        // Pure function: same input = same output
        assert_eq!(checksum1, checksum2);
        // Non-zero for non-empty data
        assert_ne!(checksum1, 0);
    }

    #[test]
    fn test_calculate_crc32_different_data() {
        // Different data should produce different checksums
        let checksum1 = calculate_crc32(b"hello");
        let checksum2 = calculate_crc32(b"world");
        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_calculate_crc32_binary_data() {
        // Test with binary data containing all byte values
        let data: Vec<u8> = (0..=255).collect();
        let checksum = calculate_crc32(&data);
        assert_ne!(checksum, 0);
    }

    #[test]
    fn test_verify_crc32_valid() {
        // Happy path: matching checksum
        let data = b"test data";
        let checksum = calculate_crc32(data);
        assert!(verify_crc32(data, checksum).is_ok());
    }

    #[test]
    fn test_verify_crc32_invalid() {
        // Error case: mismatched checksum
        let data = b"test data";
        let wrong_checksum = 0xdeadbeef;
        let result = verify_crc32(data, wrong_checksum);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            DBError::Corrupted(msg) => {
                assert!(msg.contains("CRC32 mismatch"));
                assert!(msg.contains("0xdeadbeef"));
            }
            _ => panic!("Expected DBError::Corrupted"),
        }
    }

    #[test]
    fn test_verify_crc32_empty_data() {
        // Edge case: verify empty data
        let data = b"";
        let checksum = calculate_crc32(data);
        assert!(verify_crc32(data, checksum).is_ok());
    }

    #[test]
    fn test_verify_crc32_data_modification() {
        // Corruption detection: single byte change should fail
        let mut data = b"important data".to_vec();
        let original_checksum = calculate_crc32(&data);

        // Corrupt one byte
        data[0] ^= 0xFF;
        let result = verify_crc32(&data, original_checksum);
        assert!(result.is_err());
    }

    #[test]
    fn test_checksum_deterministic() {
        // Verify function is truly pure (deterministic)
        let data = b"deterministic";
        let checksums: Vec<u32> = (0..10).map(|_| calculate_crc32(data)).collect();

        // All checksums should be identical
        for (i, &checksum) in checksums.iter().enumerate().skip(1) {
            assert_eq!(
                checksums[0], checksum,
                "Checksum mismatch at iteration {}",
                i
            );
        }
    }
}
