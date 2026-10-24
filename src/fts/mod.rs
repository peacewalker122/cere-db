use crate::api::api::{AsyncKVEngine, BatchOperation};
use crate::error::DBError;
use std::borrow::Cow;
use std::ops::RangeBounds;

const NAMESPACE: &[u8] = b"\0ceredb:fts:v1:";
const META_KEY: &[u8] = b"\0ceredb:fts:v1:m";
const DOCUMENT_MARKER: u8 = b'd';
const FORWARD_MARKER: u8 = b'f';
const POSTING_MARKER: u8 = b'p';
const DF_MARKER: u8 = b'g';
const STATS_KEY: &[u8] = b"\0ceredb:fts:v1:s";
const FORMAT_VERSION: u8 = 2;
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Config {
    k1: f64,
    b: f64,
}

impl Bm25Config {
    pub fn new(k1: f64, b: f64) -> Result<Self, DBError> {
        if !k1.is_finite() || k1 <= 0.0 || !b.is_finite() || !(0.0..=1.0).contains(&b) {
            return Err(DBError::StorageError("invalid BM25 configuration".into()));
        }
        Ok(Self { k1, b })
    }
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RankedSearchHit {
    pub id: DocumentId,
    pub document: Vec<u8>,
    pub score: f64,
}

pub struct FtsIndex<E: AsyncKVEngine> {
    engine: E,
}

impl<E: AsyncKVEngine> FtsIndex<E> {
    pub async fn open(mut engine: E) -> Result<Self, DBError> {
        engine.await_pending_writes().await?;
        let metadata = [FORMAT_VERSION, TOKENIZER_VERSION];
        match engine.get(META_KEY).await? {
            Some(value) if value.as_ref().as_slice() == metadata => {}
            Some(value) if value.as_ref().as_slice() == [1, TOKENIZER_VERSION] => {
                return Err(corrupt("FTS format 1 requires rebuilding the index"));
            }
            Some(_) => return Err(corrupt("unknown or malformed FTS metadata")),
            None => {
                if !engine.scan(namespace_range()).await?.is_empty() {
                    return Err(corrupt("FTS records exist without metadata"));
                }
                engine
                    .write_batch(vec![
                        BatchOperation::Put {
                            key: META_KEY.to_vec(),
                            value: metadata.to_vec(),
                        },
                        BatchOperation::Put {
                            key: STATS_KEY.to_vec(),
                            value: encode_stats(0, 0),
                        },
                    ])
                    .await?;
            }
        }
        Ok(Self { engine })
    }

    pub fn into_inner(self) -> E {
        self.engine
    }

    pub async fn put_document(&mut self, id: DocumentId, document: Vec<u8>) -> Result<(), DBError> {
        self.engine.await_pending_writes().await?;
        let text = std::str::from_utf8(&document).map_err(|error| {
            DBError::StorageError(format!("document is not valid UTF-8: {error}"))
        })?;
        let frequencies = tokenize(text);
        let old = self.load_document_state(&id).await?;
        let (mut count, mut total) = self.load_stats().await?;
        let old_frequency = old
            .as_ref()
            .map(|(_, terms, _)| terms.clone())
            .unwrap_or_default();
        let old_length = old.as_ref().map_or(0, |(_, _, length)| *length);
        let new_length = frequencies.values().try_fold(0u64, |sum, tf| {
            sum.checked_add(*tf)
                .ok_or_else(|| corrupt("document length overflow"))
        })?;
        if old.is_none() {
            count = count
                .checked_add(1)
                .ok_or_else(|| corrupt("document count overflow"))?;
        }
        total = total
            .checked_sub(old_length)
            .and_then(|value| value.checked_add(new_length))
            .ok_or_else(|| corrupt("corpus length overflow"))?;
        let mut operations = Vec::new();
        for term in old_frequency
            .keys()
            .chain(frequencies.keys())
            .collect::<BTreeSet<_>>()
        {
            let previous = old_frequency.get(term).copied().unwrap_or(0);
            let next = frequencies.get(term).copied().unwrap_or(0);
            if previous != next {
                let df = self.load_df(term).await?;
                let updated = match (previous == 0, next == 0) {
                    (true, false) => df
                        .checked_add(1)
                        .ok_or_else(|| corrupt("document frequency overflow"))?,
                    (false, true) => df
                        .checked_sub(1)
                        .ok_or_else(|| corrupt("document frequency underflow"))?,
                    _ => df,
                };
                if updated == 0 {
                    operations.push(BatchOperation::Delete { key: df_key(term)? });
                } else {
                    operations.push(BatchOperation::Put {
                        key: df_key(term)?,
                        value: updated.to_be_bytes().to_vec(),
                    });
                }
                if next == 0 {
                    operations.push(BatchOperation::Delete {
                        key: posting_key(term, &id)?,
                    });
                } else {
                    operations.push(BatchOperation::Put {
                        key: posting_key(term, &id)?,
                        value: next.to_be_bytes().to_vec(),
                    });
                }
            }
        }
        operations.push(BatchOperation::Put {
            key: document_key(&id)?,
            value: document,
        });
        operations.push(BatchOperation::Put {
            key: forward_key(&id)?,
            value: encode_terms(&frequencies)?,
        });
        operations.push(BatchOperation::Put {
            key: STATS_KEY.to_vec(),
            value: encode_stats(count, total),
        });
        self.engine.write_batch(operations).await
    }

    pub async fn delete_document(&mut self, id: &DocumentId) -> Result<bool, DBError> {
        self.engine.await_pending_writes().await?;
        let Some((_, terms, length)) = self.load_document_state(id).await? else {
            return Ok(false);
        };
        let (count, total) = self.load_stats().await?;
        let count = count
            .checked_sub(1)
            .ok_or_else(|| corrupt("document count underflow"))?;
        let total = total
            .checked_sub(length)
            .ok_or_else(|| corrupt("corpus length underflow"))?;
        let mut operations = Vec::new();
        for term in terms.keys() {
            let df = self.load_df(term).await?;
            let df = df
                .checked_sub(1)
                .ok_or_else(|| corrupt("document frequency underflow"))?;
            if df == 0 {
                operations.push(BatchOperation::Delete { key: df_key(term)? });
            } else {
                operations.push(BatchOperation::Put {
                    key: df_key(term)?,
                    value: df.to_be_bytes().to_vec(),
                });
            }
            operations.push(BatchOperation::Delete {
                key: posting_key(term, id)?,
            });
        }
        operations.push(BatchOperation::Delete {
            key: document_key(id)?,
        });
        operations.push(BatchOperation::Delete {
            key: forward_key(id)?,
        });
        operations.push(BatchOperation::Put {
            key: STATS_KEY.to_vec(),
            value: encode_stats(count, total),
        });
        self.engine.write_batch(operations).await?;
        Ok(true)
    }

    pub async fn get_document(&self, id: &DocumentId) -> Result<Option<Vec<u8>>, DBError> {
        self.engine.await_pending_writes().await?;
        Ok(self
            .load_document_state(id)
            .await?
            .map(|(document, _, _)| document))
    }

    pub async fn search(&self, query: FtsQuery, limit: usize) -> Result<Vec<SearchHit>, DBError> {
        self.engine.await_pending_writes().await?;
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
                let (id, tf) = parse_posting(&prefix, &key, &value)?;
                if tf == 0 {
                    return Err(corrupt("zero posting frequency"));
                }
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

    pub async fn search_ranked(
        &self,
        query: FtsQuery,
        limit: usize,
        config: Bm25Config,
    ) -> Result<Vec<RankedSearchHit>, DBError> {
        self.engine.await_pending_writes().await?;
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
        let (count, total) = self.load_stats().await?;
        if count == 0 || total == 0 {
            if count == 0 && total != 0 {
                return Err(corrupt("invalid FTS corpus statistics"));
            }
            for term in &terms {
                if self.load_df(term).await? != 0
                    || !self
                        .engine
                        .scan(prefix_range(&posting_prefix(term)?))
                        .await?
                        .is_empty()
                {
                    return Err(corrupt("zero corpus length has indexed terms"));
                }
            }
            return Ok(Vec::new());
        }
        let average = total as f64 / count as f64;
        if !average.is_finite() || average <= 0.0 {
            return Err(corrupt("invalid average document length"));
        }
        let mut per_term = Vec::new();
        for term in &terms {
            let df = self.load_df(term).await?;
            if df > count {
                return Err(corrupt("document frequency exceeds document count"));
            }
            let prefix = posting_prefix(term)?;
            let mut frequencies = BTreeMap::new();
            for (key, value) in self.engine.scan(prefix_range(&prefix)).await? {
                let (id, tf) = parse_posting(&prefix, &key, &value)?;
                if tf == 0 {
                    return Err(corrupt("zero posting frequency"));
                }
                self.validate_posting(&id, term).await?;
                frequencies.insert(id, tf);
            }
            if frequencies.len() as u64 != df {
                return Err(corrupt("document frequency disagrees with postings"));
            }
            let idf = (count as f64 + 1.0).ln() - (df as f64 + 0.5).ln();
            if !idf.is_finite() || idf < 0.0 {
                return Err(corrupt("invalid BM25 inverse document frequency"));
            }
            per_term.push((term.clone(), idf, frequencies));
        }
        let mut candidates = BTreeSet::new();
        for (_, _, frequencies) in &per_term {
            candidates.extend(frequencies.keys().cloned());
        }
        if is_and {
            candidates.retain(|id| {
                per_term
                    .iter()
                    .all(|(_, _, postings)| postings.contains_key(id))
            });
        }
        let mut ranked = Vec::with_capacity(candidates.len());
        for id in candidates {
            let (document, forward, length) = self
                .load_document_state(&id)
                .await?
                .ok_or_else(|| corrupt("posting references a missing document"))?;
            let mut score = 0.0;
            for (term, idf, postings) in &per_term {
                if let Some(tf) = postings.get(&id) {
                    let tf = *tf as f64;
                    let dl_ratio = length as f64 / average;
                    let length_factor = (1.0 - config.b) + config.b * dl_ratio;
                    let contribution = if config.k1 <= 1.0 {
                        idf * (tf * (1.0 + config.k1)) / (tf + config.k1 * length_factor)
                    } else {
                        let k1_inverse = 1.0 / config.k1;
                        idf * (tf * (1.0 + k1_inverse)) / (tf * k1_inverse + length_factor)
                    };
                    if !contribution.is_finite()
                        || length_factor <= 0.0
                        || !forward.contains_key(term)
                    {
                        return Err(corrupt("invalid BM25 score state"));
                    }
                    score += contribution;
                }
            }
            if !score.is_finite() {
                return Err(corrupt("BM25 score overflow"));
            }
            ranked.push(RankedSearchHit {
                id,
                document,
                score,
            });
        }
        ranked.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.id.cmp(&right.id))
        });
        ranked.truncate(limit);
        Ok(ranked)
    }

    async fn load_stats(&self) -> Result<(u64, u64), DBError> {
        let value = self
            .engine
            .get(STATS_KEY)
            .await?
            .ok_or_else(|| corrupt("missing FTS corpus statistics"))?;
        decode_stats(value.as_ref())
    }

    async fn load_df(&self, term: &str) -> Result<u64, DBError> {
        match self.engine.get(&df_key(term)?).await? {
            None => Ok(0),
            Some(value) => {
                let df = decode_u64(value.as_ref())?;
                if df == 0 {
                    Err(corrupt("zero document frequency"))
                } else {
                    Ok(df)
                }
            }
        }
    }

    async fn load_document_state(
        &self,
        id: &DocumentId,
    ) -> Result<Option<(Vec<u8>, BTreeMap<String, u64>, u64)>, DBError> {
        let document = self.engine.get(&document_key(id)?).await?;
        let forward = self.engine.get(&forward_key(id)?).await?;
        match (document, forward) {
            (None, None) => Ok(None),
            (Some(document), Some(forward)) => {
                let text = std::str::from_utf8(document.as_ref())
                    .map_err(|_| corrupt("stored document is not UTF-8"))?;
                let (terms, length) = decode_terms(forward.as_ref())?;
                if terms != tokenize(text) {
                    return Err(corrupt("document and forward terms disagree"));
                }
                for (term, tf) in &terms {
                    match self.engine.get(&posting_key(term, id)?).await? {
                        Some(value) if decode_u64(value.as_ref())? == *tf => {}
                        _ => return Err(corrupt("forward term and posting disagree")),
                    }
                }
                Ok(Some((document.as_ref().clone(), terms, length)))
            }
            _ => Err(corrupt("document and forward index disagree")),
        }
    }

    async fn validate_posting(&self, id: &DocumentId, term: &str) -> Result<(), DBError> {
        let Some((_, terms, _)) = self.load_document_state(id).await? else {
            return Err(corrupt("posting references a missing document"));
        };
        if !terms.contains_key(term) {
            return Err(corrupt("posting is absent from forward index"));
        }
        Ok(())
    }
}

impl<E: AsyncKVEngine> AsyncKVEngine for FtsIndex<E> {
    async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
        self.engine.await_pending_writes().await?;
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
        self.engine.await_pending_writes().await?;
        Ok(self
            .engine
            .scan(range)
            .await?
            .into_iter()
            .filter(|(key, _)| !reserved(key))
            .collect())
    }

    async fn await_pending_writes(&self) -> Result<(), DBError> {
        self.engine.await_pending_writes().await
    }
}

use std::collections::{BTreeMap, BTreeSet};

fn tokenize(text: &str) -> BTreeMap<String, u64> {
    let mut terms = BTreeMap::new();
    let mut current = String::new();
    for character in text.chars() {
        if character.is_alphanumeric() {
            current.extend(character.to_lowercase());
        } else if !current.is_empty() {
            *terms.entry(std::mem::take(&mut current)).or_insert(0) += 1;
        }
    }
    if !current.is_empty() {
        *terms.entry(current).or_insert(0) += 1;
    }
    terms.into_iter().collect()
}

fn normalize_terms(input: Vec<String>) -> Vec<String> {
    input
        .into_iter()
        .flat_map(|term| tokenize(&term).into_keys())
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

fn df_key(term: &str) -> Result<Vec<u8>, DBError> {
    let mut key = NAMESPACE.to_vec();
    key.push(DF_MARKER);
    encode_component(term.as_bytes(), &mut key)?;
    Ok(key)
}

fn encode_terms(terms: &BTreeMap<String, u64>) -> Result<Vec<u8>, DBError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u64::try_from(terms.len())
            .map_err(|_| DBError::StorageError("too many FTS terms".into()))?
            .to_be_bytes(),
    );
    let length = terms.values().try_fold(0u64, |sum, tf| {
        sum.checked_add(*tf)
            .ok_or_else(|| DBError::StorageError("FTS document length overflow".into()))
    })?;
    bytes.extend_from_slice(&length.to_be_bytes());
    for (term, tf) in terms {
        encode_component(term.as_bytes(), &mut bytes)?;
        bytes.extend_from_slice(&tf.to_be_bytes());
    }
    Ok(bytes)
}

fn decode_terms(bytes: &[u8]) -> Result<(BTreeMap<String, u64>, u64), DBError> {
    let mut cursor = 0;
    let count = usize::try_from(read_u64(bytes, &mut cursor)?)
        .map_err(|_| corrupt("impossible forward term count"))?;
    let length = read_u64(bytes, &mut cursor)?;
    if count > bytes.len().saturating_sub(cursor) / 12 {
        return Err(corrupt("impossible forward term count"));
    }
    let mut terms = BTreeMap::new();
    let mut observed_length = 0u64;
    let mut previous: Option<String> = None;
    for _ in 0..count {
        let term = String::from_utf8(read_component(bytes, &mut cursor)?.to_vec())
            .map_err(|_| corrupt("invalid UTF-8 term"))?;
        let tf = read_u64(bytes, &mut cursor)?;
        if term.is_empty() || term.to_lowercase() != term {
            return Err(corrupt("non-normalized forward term"));
        }
        if tf == 0 || previous.as_ref().is_some_and(|last| last >= &term) {
            return Err(corrupt("invalid forward term frequency"));
        }
        previous = Some(term.clone());
        terms.insert(term, tf);
        observed_length = observed_length
            .checked_add(tf)
            .ok_or_else(|| corrupt("document length overflow"))?;
    }
    if cursor != bytes.len() || observed_length != length {
        return Err(corrupt("malformed forward record"));
    }
    Ok((terms, length))
}

fn parse_posting(prefix: &[u8], key: &[u8], value: &[u8]) -> Result<(DocumentId, u64), DBError> {
    if !key.starts_with(prefix) {
        return Err(corrupt("malformed posting record"));
    }
    let mut cursor = prefix.len();
    let id = std::str::from_utf8(read_component(key, &mut cursor)?)
        .map_err(|_| corrupt("invalid UTF-8 posting ID"))?
        .to_owned();
    if cursor != key.len() {
        return Err(corrupt("trailing posting key bytes"));
    }
    Ok((
        DocumentId::new(id).map_err(|_| corrupt("empty posting ID"))?,
        decode_u64(value)?,
    ))
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
fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, DBError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| corrupt("length overflow"))?;
    let number = u64::from_be_bytes(
        bytes
            .get(*cursor..end)
            .ok_or_else(|| corrupt("truncated integer"))?
            .try_into()?,
    );
    *cursor = end;
    Ok(number)
}
fn decode_u64(bytes: &[u8]) -> Result<u64, DBError> {
    let mut cursor = 0;
    let value = read_u64(bytes, &mut cursor)?;
    if cursor != bytes.len() {
        return Err(corrupt("malformed integer"));
    }
    Ok(value)
}
fn encode_stats(count: u64, total: u64) -> Vec<u8> {
    [count.to_be_bytes(), total.to_be_bytes()].concat()
}
fn decode_stats(bytes: &[u8]) -> Result<(u64, u64), DBError> {
    if bytes.len() != 16 {
        return Err(corrupt("malformed FTS corpus statistics"));
    }
    let count = decode_u64(&bytes[..8])?;
    let total = decode_u64(&bytes[8..])?;
    if count == 0 && total != 0 {
        return Err(corrupt("invalid FTS corpus statistics"));
    }
    Ok((count, total))
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
