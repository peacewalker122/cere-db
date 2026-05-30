use crate::error::DBError;
use std::borrow::Cow;
use std::ops::RangeBounds;

pub trait KVEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;
}

pub trait AsyncKVEngine {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;

    /// Scan keys in the given range and return all matching key-value pairs.
    ///
    /// Supports inclusive/exclusive boundaries. Returns results sorted by key,
    /// with the newest version per key (highest LSN wins). Tombstones (deleted keys)
    /// are excluded from the result set.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Scan all keys from "a" inclusive to "z" inclusive
    /// let results = engine.scan("a"..="z").await?;
    ///
    /// // Scan all keys starting from "m"
    /// let results = engine.scan("m"..).await?;
    ///
    /// // Scan all keys up to (but not including) "n"
    /// let results = engine.scan(.."n").await?;
    ///
    /// // Scan all keys in the store
    /// let results = engine.scan(..).await?;
    /// ```
    async fn scan(&self, range: impl RangeBounds<Vec<u8>> + Send) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError>;
}
