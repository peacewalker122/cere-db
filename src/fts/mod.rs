use crate::api::api::{AsyncKVEngine, BatchOperation};
use crate::error::DBError;
use std::borrow::Cow;
use std::ops::RangeBounds;

const NAMESPACE: &[u8] = b"\0ceredb:fts:v1:";
const META_KEY: &[u8] = b"\0ceredb:fts:v1:m";
const DOCUMENT_MARKER: u8 = b'd';
const FORWARD_MARKER: u8 = b'f';
const POSTING_MARKER: u8 = b'p';
const FORMAT_VERSION: u8 = 1;
const TOKENIZER_VERSION: u8 = 1;
const MAX_QUERY_TERMS: usize = 32;
const MAX_LIMIT: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DocumentId(String);

impl DocumentId {
    pub fn new(id: impl Into<String>) -> Result<Self, DBError> {
        let id = id.into();
        if id.is_empty() {
            return Err(DBError::StorageError(
                "document ID must not be empty".into(),
            ));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FtsQuery {
    And(Vec<String>),
    Or(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchHit {
    pub id: DocumentId,
    pub document: Vec<u8>,
}

pub struct FtsIndex<E: AsyncKVEngine> {
    engine: E,
}

impl<E: AsyncKVEngine> FtsIndex<E> {
    pub async fn open(mut engine: E) -> Result<Self, DBError> {
        let metadata = [FORMAT_VERSION, TOKENIZER_VERSION];
        match engine.get(META_KEY).await? {
            Some(value) if value.as_ref().as_slice() == metadata => {}
            Some(_) => return Err(corrupt("unknown or malformed FTS metadata")),
            None => {
                if !engine.scan(namespace_range()).await?.is_empty() {
                    return Err(corrupt("FTS records exist without metadata"));
                }
                engine.put(META_KEY.to_vec(), metadata.to_vec()).await?;
            }
        }
        Ok(Self { engine })
    }

    pub fn into_inner(self) -> E {
        self.engine
    }

    pub async fn put_document(&mut self, id: DocumentId, document: Vec<u8>) -> Result<(), DBError> {
        let text = std::str::from_utf8(&document).map_err(|error| {
            DBError::StorageError(format!("document is not valid UTF-8: {error}"))
        })?;
        let terms = tokenize(text);
        let old = self.load_document_state(&id).await?;
        let mut operations = Vec::new();
        if let Some((_, old_terms)) = old {
            for term in old_terms {
                if !terms.contains(&term) {
                    operations.push(BatchOperation::Delete {
                        key: posting_key(&term, &id)?,
                    });
                }
            }
        }
        for term in &terms {
            operations.push(BatchOperation::Put {
                key: posting_key(term, &id)?,
                value: Vec::new(),
            });
        }
        operations.push(BatchOperation::Put {
            key: document_key(&id)?,
            value: document,
        });
        operations.push(BatchOperation::Put {
            key: forward_key(&id)?,
            value: encode_terms(&terms)?,
        });
        self.engine.write_batch(operations).await
    }

    pub async fn delete_document(&mut self, id: &DocumentId) -> Result<bool, DBError> {
        let Some((_, terms)) = self.load_document_state(id).await? else {
            return Ok(false);
        };
        let mut operations = terms
            .into_iter()
            .map(|term| {
                Ok(BatchOperation::Delete {
                    key: posting_key(&term, id)?,
                })
            })
            .collect::<Result<Vec<_>, DBError>>()?;
        operations.push(BatchOperation::Delete {
            key: document_key(id)?,
        });
        operations.push(BatchOperation::Delete {
            key: forward_key(id)?,
        });
        self.engine.write_batch(operations).await?;
        Ok(true)
    }

    pub async fn get_document(&self, id: &DocumentId) -> Result<Option<Vec<u8>>, DBError> {
        Ok(self
            .load_document_state(id)
            .await?
            .map(|(document, _)| document))
    }

    pub async fn search(&self, query: FtsQuery, limit: usize) -> Result<Vec<SearchHit>, DBError> {
        if limit > MAX_LIMIT {
            return Err(DBError::StorageError(
                "FTS result limit exceeds 1000".into(),
            ));
        }
        let is_and = matches!(&query, FtsQuery::And(_));
        let input = match query {
            FtsQuery::And(terms) | FtsQuery::Or(terms) => terms,
        };
        let terms = normalize_terms(input);
        if terms.is_empty() {
            return Err(DBError::StorageError(
                "FTS query contains no searchable terms".into(),
            ));
        }
        if terms.len() > MAX_QUERY_TERMS {
            return Err(DBError::StorageError(
                "FTS query exceeds 32 distinct terms".into(),
            ));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut matches: Option<BTreeSet<DocumentId>> = None;
        let mut posting_sets = Vec::new();
        for term in &terms {
            let prefix = posting_prefix(term)?;
            let rows = self.engine.scan(prefix_range(&prefix)).await?;
            let mut ids = BTreeSet::new();
            for (key, value) in rows {
                let id = parse_posting(&prefix, &key, &value)?;
                self.validate_posting(&id, term).await?;
                ids.insert(id);
            }
            posting_sets.push(ids);
        }
        if is_and {
            posting_sets.sort_by_key(BTreeSet::len);
            for ids in posting_sets {
                matches = Some(match matches {
                    None => ids,
                    Some(current) => current.intersection(&ids).cloned().collect(),
                });
            }
        } else {
            matches = Some(posting_sets.into_iter().flatten().collect());
        }
        let mut hits = Vec::new();
        for id in matches.unwrap_or_default().into_iter().take(limit) {
            let document = self
                .engine
                .get(&document_key(&id)?)
                .await?
                .ok_or_else(|| corrupt("posting references a missing document"))?;
            hits.push(SearchHit {
                id,
                document: document.as_ref().clone(),
            });
        }
        Ok(hits)
    }

    async fn load_document_state(
        &self,
        id: &DocumentId,
    ) -> Result<Option<(Vec<u8>, Vec<String>)>, DBError> {
        let document = self.engine.get(&document_key(id)?).await?;
        let forward = self.engine.get(&forward_key(id)?).await?;
        match (document, forward) {
            (None, None) => Ok(None),
            (Some(document), Some(forward)) => {
                let text = std::str::from_utf8(document.as_ref())
                    .map_err(|_| corrupt("stored document is not UTF-8"))?;
                let terms = decode_terms(forward.as_ref())?;
                if terms != tokenize(text) {
                    return Err(corrupt("document and forward terms disagree"));
                }
                for term in &terms {
                    match self.engine.get(&posting_key(term, id)?).await? {
                        Some(value) if value.as_ref().is_empty() => {}
                        _ => return Err(corrupt("forward term and posting disagree")),
                    }
                }
                Ok(Some((document.as_ref().clone(), terms)))
            }
            _ => Err(corrupt("document and forward index disagree")),
        }
    }

    async fn validate_posting(&self, id: &DocumentId, term: &str) -> Result<(), DBError> {
        let Some((_, terms)) = self.load_document_state(id).await? else {
            return Err(corrupt("posting references a missing document"));
        };
        if !terms.iter().any(|candidate| candidate == term) {
            return Err(corrupt("posting is absent from forward index"));
        }
        Ok(())
    }
}

impl<E: AsyncKVEngine> AsyncKVEngine for FtsIndex<E> {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        if reserved(key) {
            return Err(reserved_error());
        }
        self.engine.get(key).await
    }
    async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
        if reserved(&key) {
            return Err(reserved_error());
        }
        self.engine.put(key, value).await
    }
    async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
        if reserved(&key) {
            return Err(reserved_error());
        }
        self.engine.delete(key).await
    }
    async fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
        for operation in &operations {
            let key = match operation {
                BatchOperation::Put { key, .. } | BatchOperation::Delete { key } => key,
            };
            if reserved(key) {
                return Err(reserved_error());
            }
        }
        self.engine.write_batch(operations).await
    }
    async fn scan(
        &self,
        range: impl RangeBounds<Vec<u8>> + Send,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
        Ok(self
            .engine
            .scan(range)
            .await?
            .into_iter()
            .filter(|(key, _)| !reserved(key))
            .collect())
    }
}

use std::collections::BTreeSet;

fn tokenize(text: &str) -> Vec<String> {
    let mut terms = BTreeSet::new();
    let mut current = String::new();
    for character in text.chars() {
        if character.is_alphanumeric() {
            current.extend(character.to_lowercase());
        } else if !current.is_empty() {
            terms.insert(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        terms.insert(current);
    }
    terms.into_iter().collect()
}

fn normalize_terms(input: Vec<String>) -> Vec<String> {
    input
        .into_iter()
        .flat_map(|term| tokenize(&term))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn encode_component(value: &[u8], output: &mut Vec<u8>) -> Result<(), DBError> {
    let length = u32::try_from(value.len())
        .map_err(|_| DBError::StorageError("FTS component too long".into()))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn record_key(marker: u8, id: &DocumentId) -> Result<Vec<u8>, DBError> {
    let mut key = NAMESPACE.to_vec();
    key.push(marker);
    encode_component(id.as_str().as_bytes(), &mut key)?;
    Ok(key)
}
fn document_key(id: &DocumentId) -> Result<Vec<u8>, DBError> {
    record_key(DOCUMENT_MARKER, id)
}
fn forward_key(id: &DocumentId) -> Result<Vec<u8>, DBError> {
    record_key(FORWARD_MARKER, id)
}
fn posting_prefix(term: &str) -> Result<Vec<u8>, DBError> {
    let mut key = NAMESPACE.to_vec();
    key.push(POSTING_MARKER);
    encode_component(term.as_bytes(), &mut key)?;
    Ok(key)
}
fn posting_key(term: &str, id: &DocumentId) -> Result<Vec<u8>, DBError> {
    let mut key = posting_prefix(term)?;
    encode_component(id.as_str().as_bytes(), &mut key)?;
    Ok(key)
}

fn encode_terms(terms: &[String]) -> Result<Vec<u8>, DBError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(terms.len())
            .map_err(|_| DBError::StorageError("too many FTS terms".into()))?
            .to_be_bytes(),
    );
    for term in terms {
        encode_component(term.as_bytes(), &mut bytes)?;
    }
    Ok(bytes)
}

fn decode_terms(bytes: &[u8]) -> Result<Vec<String>, DBError> {
    let mut cursor = 0;
    let count = read_u32(bytes, &mut cursor)? as usize;
    if count > bytes.len().saturating_sub(cursor) / 4 {
        return Err(corrupt("impossible forward term count"));
    }
    let mut terms = Vec::with_capacity(count);
    for _ in 0..count {
        let term = String::from_utf8(read_component(bytes, &mut cursor)?.to_vec())
            .map_err(|_| corrupt("invalid UTF-8 term"))?;
        if term.is_empty() || term.to_lowercase() != term {
            return Err(corrupt("non-normalized forward term"));
        }
        terms.push(term);
    }
    if cursor != bytes.len() || terms.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(corrupt("malformed forward record"));
    }
    Ok(terms)
}

fn parse_posting(prefix: &[u8], key: &[u8], value: &[u8]) -> Result<DocumentId, DBError> {
    if !value.is_empty() || !key.starts_with(prefix) {
        return Err(corrupt("malformed posting record"));
    }
    let mut cursor = prefix.len();
    let id = std::str::from_utf8(read_component(key, &mut cursor)?)
        .map_err(|_| corrupt("invalid UTF-8 posting ID"))?
        .to_owned();
    if cursor != key.len() {
        return Err(corrupt("trailing posting key bytes"));
    }
    DocumentId::new(id).map_err(|_| corrupt("empty posting ID"))
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, DBError> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| corrupt("length overflow"))?;
    let number = u32::from_be_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| corrupt("truncated length"))?
            .try_into()?,
    );
    *cursor = end;
    Ok(number)
}
fn read_component<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], DBError> {
    let length = read_u32(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| corrupt("length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corrupt("truncated component"))?;
    *cursor = end;
    Ok(value)
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    for index in (0..upper.len()).rev() {
        if upper[index] != u8::MAX {
            upper[index] += 1;
            upper.truncate(index + 1);
            return Some(upper);
        }
    }
    None
}
fn prefix_range(prefix: &[u8]) -> (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) {
    (
        std::ops::Bound::Included(prefix.to_vec()),
        prefix_successor(prefix).map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
    )
}
fn namespace_range() -> (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>) {
    prefix_range(NAMESPACE)
}
fn reserved(key: &[u8]) -> bool {
    key.starts_with(NAMESPACE)
}
fn reserved_error() -> DBError {
    DBError::StorageError("key belongs to the reserved FTS namespace".into())
}
fn corrupt(message: &str) -> DBError {
    DBError::Corrupted(message.into())
}
