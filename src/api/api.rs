use crate::error::DBError;
use std::borrow::Cow;

pub trait KVEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError>;
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError>;
    fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError>;
}
