use crate::{
    api::api::{AsyncKVEngine, BatchOperation, KVEngine},
    error::DBError,
    fts::{DocumentId, FtsIndex, FtsQuery, SearchHit},
};

pub trait FtsCommandEngine: AsyncKVEngine {
    async fn put_document(&mut self, _id: DocumentId, _document: Vec<u8>) -> Result<(), DBError> {
        Err(unsupported_fts())
    }

    async fn get_document(&self, _id: &DocumentId) -> Result<Option<Vec<u8>>, DBError> {
        Err(unsupported_fts())
    }

    async fn delete_document(&mut self, _id: &DocumentId) -> Result<bool, DBError> {
        Err(unsupported_fts())
    }

    async fn search(&self, _query: FtsQuery, _limit: usize) -> Result<Vec<SearchHit>, DBError> {
        Err(unsupported_fts())
    }
}

impl<E: AsyncKVEngine> FtsCommandEngine for FtsIndex<E> {
    async fn put_document(&mut self, id: DocumentId, document: Vec<u8>) -> Result<(), DBError> {
        FtsIndex::put_document(self, id, document).await
    }

    async fn get_document(&self, id: &DocumentId) -> Result<Option<Vec<u8>>, DBError> {
        FtsIndex::get_document(self, id).await
    }

    async fn delete_document(&mut self, id: &DocumentId) -> Result<bool, DBError> {
        FtsIndex::delete_document(self, id).await
    }

    async fn search(&self, query: FtsQuery, limit: usize) -> Result<Vec<SearchHit>, DBError> {
        FtsIndex::search(self, query, limit).await
    }
}

impl FtsCommandEngine for crate::storage::kv2::KV2 {}
impl FtsCommandEngine for crate::storage::kv2::RaftKV2 {}

fn unsupported_fts() -> DBError {
    DBError::StorageError(
        "document and full-text search commands are unsupported by this engine".into(),
    )
}

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    Batch { operations: Vec<BatchOperation> },
    DocumentPut { id: String, text: String },
    DocumentGet { id: String },
    DocumentDelete { id: String },
    Search { query: FtsQuery },
    Exit,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Ok,
    Value(String),
    Nil,
    Document(Option<Vec<u8>>),
    SearchResults(Vec<SearchHit>),
    Bye,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    Empty,
    UnterminatedQuotedValue,
    UnknownCommand(String),
    InvalidArity {
        command: String,
        expected: &'static str,
    },
}

pub fn parse_command(input: &str) -> Result<Command, ParseError> {
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Err(ParseError::Empty);
    }

    let keyword = tokens[0].to_uppercase();
    match keyword.as_str() {
        "SET" => {
            if tokens.len() != 3 {
                return Err(ParseError::InvalidArity {
                    command: "SET".to_string(),
                    expected: "SET <key> <value>",
                });
            }
            Ok(Command::Set {
                key: tokens[1].clone(),
                value: tokens[2].clone(),
            })
        }
        "GET" => {
            if tokens.len() != 2 {
                return Err(ParseError::InvalidArity {
                    command: "GET".to_string(),
                    expected: "GET <key>",
                });
            }
            Ok(Command::Get {
                key: tokens[1].clone(),
            })
        }
        "DELETE" | "DEL" => {
            if tokens.len() != 2 {
                return Err(ParseError::InvalidArity {
                    command: keyword,
                    expected: "DELETE <key> (or DEL <key>)",
                });
            }
            Ok(Command::Delete {
                key: tokens[1].clone(),
            })
        }
        "BATCH" => {
            if tokens.len() < 3 {
                return Err(ParseError::InvalidArity {
                    command: "BATCH".to_string(),
                    expected: "BATCH SET <key> <value> [DELETE <key> ...]",
                });
            }
            let mut operations = Vec::new();
            let mut index = 1;
            while index < tokens.len() {
                match tokens[index].to_uppercase().as_str() {
                    "SET" if index + 2 < tokens.len() => {
                        operations.push(BatchOperation::Put {
                            key: tokens[index + 1].as_bytes().to_vec(),
                            value: tokens[index + 2].as_bytes().to_vec(),
                        });
                        index += 3;
                    }
                    "DELETE" if index + 1 < tokens.len() => {
                        operations.push(BatchOperation::Delete {
                            key: tokens[index + 1].as_bytes().to_vec(),
                        });
                        index += 2;
                    }
                    "SET" | "DELETE" => {
                        return Err(ParseError::InvalidArity {
                            command: "BATCH".to_string(),
                            expected: "BATCH SET <key> <value> [DELETE <key> ...]",
                        });
                    }
                    _ => {
                        return Err(ParseError::InvalidArity {
                            command: "BATCH".to_string(),
                            expected: "BATCH SET <key> <value> [DELETE <key> ...]",
                        });
                    }
                }
            }
            if operations.is_empty() {
                return Err(ParseError::InvalidArity {
                    command: "BATCH".to_string(),
                    expected: "BATCH SET <key> <value> [DELETE <key> ...]",
                });
            }
            Ok(Command::Batch { operations })
        }
        "DOC" => match tokens.get(1).map(|token| token.to_uppercase()).as_deref() {
            Some("PUT") if tokens.len() == 4 => Ok(Command::DocumentPut {
                id: tokens[2].clone(),
                text: tokens[3].clone(),
            }),
            Some("GET") if tokens.len() == 3 => Ok(Command::DocumentGet {
                id: tokens[2].clone(),
            }),
            Some("DELETE") if tokens.len() == 3 => Ok(Command::DocumentDelete {
                id: tokens[2].clone(),
            }),
            Some("PUT") => Err(ParseError::InvalidArity {
                command: "DOC PUT".into(),
                expected: "DOC PUT <id> <text>",
            }),
            Some("GET") => Err(ParseError::InvalidArity {
                command: "DOC GET".into(),
                expected: "DOC GET <id>",
            }),
            Some("DELETE") => Err(ParseError::InvalidArity {
                command: "DOC DELETE".into(),
                expected: "DOC DELETE <id>",
            }),
            _ => Err(ParseError::InvalidArity {
                command: "DOC".into(),
                expected: "DOC PUT <id> <text>, DOC GET <id>, or DOC DELETE <id>",
            }),
        },
        "FTS" => {
            let query_kind = tokens.get(1).map(|token| token.to_uppercase());
            if tokens.len() < 3 {
                return Err(ParseError::InvalidArity {
                    command: "FTS".into(),
                    expected: "FTS AND <query> or FTS OR <query>",
                });
            }
            let text = tokens[2..].join(" ");
            match query_kind.as_deref() {
                Some("AND") => Ok(Command::Search {
                    query: FtsQuery::And(vec![text]),
                }),
                Some("OR") => Ok(Command::Search {
                    query: FtsQuery::Or(vec![text]),
                }),
                _ => Err(ParseError::InvalidArity {
                    command: "FTS".into(),
                    expected: "FTS AND <query> or FTS OR <query>",
                }),
            }
        }
        "EXIT" | "QUIT" => Ok(Command::Exit),
        _ => Err(ParseError::UnknownCommand(tokens[0].clone())),
    }
}

pub fn execute_command<E: KVEngine>(engine: &mut E, command: Command) -> Result<Response, DBError> {
    match command {
        Command::Set { key, value } => {
            engine.put(key.into_bytes(), value.into_bytes())?;
            Ok(Response::Ok)
        }
        Command::Get { key } => match engine.get(key.as_bytes())? {
            Some(value) => Ok(Response::Value(String::from_utf8_lossy(&value).to_string())),
            None => Ok(Response::Nil),
        },
        Command::Delete { key } => {
            engine.delete(key.into_bytes())?;
            Ok(Response::Ok)
        }
        Command::Batch { operations } => {
            engine.write_batch(operations)?;
            Ok(Response::Ok)
        }
        Command::DocumentPut { .. }
        | Command::DocumentGet { .. }
        | Command::DocumentDelete { .. }
        | Command::Search { .. } => Err(unsupported_fts()),
        Command::Exit => Ok(Response::Bye),
    }
}

pub async fn execute_command_async<E: FtsCommandEngine>(
    engine: &mut E,
    command: Command,
) -> Result<Response, DBError> {
    match command {
        Command::Set { key, value } => {
            engine.put(key.into_bytes(), value.into_bytes()).await?;
            Ok(Response::Ok)
        }
        Command::Get { key } => match engine.get(key.as_bytes()).await? {
            Some(value) => Ok(Response::Value(String::from_utf8_lossy(&value).to_string())),
            None => Ok(Response::Nil),
        },
        Command::Delete { key } => {
            engine.delete(key.into_bytes()).await?;
            Ok(Response::Ok)
        }
        Command::Batch { operations } => {
            engine.write_batch(operations).await?;
            Ok(Response::Ok)
        }
        Command::DocumentPut { id, text } => {
            engine
                .put_document(DocumentId::new(id)?, text.into_bytes())
                .await?;
            Ok(Response::Ok)
        }
        Command::DocumentGet { id } => Ok(Response::Document(
            engine.get_document(&DocumentId::new(id)?).await?,
        )),
        Command::DocumentDelete { id } => {
            engine.delete_document(&DocumentId::new(id)?).await?;
            Ok(Response::Ok)
        }
        Command::Search { query } => Ok(Response::SearchResults(engine.search(query, 100).await?)),
        Command::Exit => Ok(Response::Bye),
    }
}

fn tokenize(input: &str) -> Result<Vec<String>, ParseError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut quote_char: Option<char> = None;
    let mut escape = false;
    let mut started_quote = false;

    for ch in input.trim().chars() {
        if let Some(active_quote) = quote_char {
            if escape {
                current.push(ch);
                escape = false;
                continue;
            }

            match ch {
                '\\' => escape = true,
                c if c == active_quote => {
                    quote_char = None;
                    tokens.push(current.clone());
                    current.clear();
                    started_quote = false;
                }
                _ => current.push(ch),
            }
            continue;
        }

        match ch {
            '"' | '\'' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                quote_char = Some(ch);
                started_quote = true;
            }
            c if c.is_whitespace() => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if quote_char.is_some() || escape || started_quote {
        return Err(ParseError::UnterminatedQuotedValue);
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::DBError;
    use std::{borrow::Cow, collections::HashMap, ops::RangeBounds};

    #[derive(Default)]
    struct MockKV {
        data: HashMap<Vec<u8>, Vec<u8>>,
        puts: usize,
        deletes: usize,
        batches: Vec<Vec<BatchOperation>>,
        fail_batch: bool,
    }

    impl KVEngine for MockKV {
        fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
            Ok(self.data.get(key).map(Cow::Borrowed))
        }

        fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
            self.puts += 1;
            self.data.insert(key, value);
            Ok(())
        }

        fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
            self.deletes += 1;
            self.data.remove(&key);
            Ok(())
        }

        fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
            if self.fail_batch {
                return Err(DBError::StorageError("batch failed".into()));
            }
            self.batches.push(operations);
            Ok(())
        }
    }

    impl AsyncKVEngine for MockKV {
        async fn get(&self, key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
            Ok(self.data.get(key).map(Cow::Borrowed))
        }

        async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), DBError> {
            self.puts += 1;
            self.data.insert(key, value);
            Ok(())
        }

        async fn delete(&mut self, key: Vec<u8>) -> Result<(), DBError> {
            self.deletes += 1;
            self.data.remove(&key);
            Ok(())
        }

        async fn write_batch(&mut self, operations: Vec<BatchOperation>) -> Result<(), DBError> {
            if self.fail_batch {
                return Err(DBError::StorageError("batch failed".into()));
            }
            self.batches.push(operations);
            Ok(())
        }

        async fn scan(
            &self,
            range: impl RangeBounds<Vec<u8>> + Send,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
            use std::ops::Bound;
            let mut results = Vec::new();
            for (key, value) in &self.data {
                let in_range = match range.start_bound() {
                    Bound::Included(start) => key.as_slice() >= start.as_slice(),
                    Bound::Excluded(start) => key.as_slice() > start.as_slice(),
                    Bound::Unbounded => true,
                } && match range.end_bound() {
                    Bound::Included(end) => key.as_slice() <= end.as_slice(),
                    Bound::Excluded(end) => key.as_slice() < end.as_slice(),
                    Bound::Unbounded => true,
                };
                if in_range {
                    results.push((key.clone(), value.clone()));
                }
            }
            results.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(results)
        }
    }

    impl FtsCommandEngine for MockKV {}

    struct DefaultBatchKV;

    impl KVEngine for DefaultBatchKV {
        fn get(&self, _key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
            Ok(None)
        }

        fn put(&mut self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), DBError> {
            Ok(())
        }

        fn delete(&mut self, _key: Vec<u8>) -> Result<(), DBError> {
            Ok(())
        }
    }

    impl AsyncKVEngine for DefaultBatchKV {
        async fn get(&self, _key: &[u8]) -> Result<Option<Cow<'_, Vec<u8>>>, DBError> {
            Ok(None)
        }

        async fn put(&mut self, _key: Vec<u8>, _value: Vec<u8>) -> Result<(), DBError> {
            Ok(())
        }

        async fn delete(&mut self, _key: Vec<u8>) -> Result<(), DBError> {
            Ok(())
        }

        async fn scan(
            &self,
            _range: impl RangeBounds<Vec<u8>> + Send,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, DBError> {
            Ok(Vec::new())
        }
    }

    impl FtsCommandEngine for DefaultBatchKV {}

    #[test]
    fn parse_set_command() {
        let parsed = parse_command("SET key value");
        assert_eq!(
            parsed,
            Ok(Command::Set {
                key: "key".to_string(),
                value: "value".to_string()
            })
        );
    }

    #[test]
    fn parse_set_with_quoted_value() {
        let parsed = parse_command("SET key \"value with spaces\"");
        assert_eq!(
            parsed,
            Ok(Command::Set {
                key: "key".to_string(),
                value: "value with spaces".to_string()
            })
        );
    }

    #[test]
    fn parse_set_with_single_quoted_value() {
        let parsed = parse_command("SET key 'value with spaces'");
        assert_eq!(
            parsed,
            Ok(Command::Set {
                key: "key".to_string(),
                value: "value with spaces".to_string()
            })
        );
    }

    #[test]
    fn parse_set_with_empty_quoted_value() {
        let parsed = parse_command("SET key \"\"");
        assert_eq!(
            parsed,
            Ok(Command::Set {
                key: "key".to_string(),
                value: "".to_string()
            })
        );
    }

    #[test]
    fn parse_set_with_escaped_quote_in_value() {
        let parsed = parse_command("SET key \"value \\\"quoted\\\"\"");
        assert_eq!(
            parsed,
            Ok(Command::Set {
                key: "key".to_string(),
                value: "value \"quoted\"".to_string()
            })
        );
    }

    #[test]
    fn parse_unterminated_quote() {
        let parsed = parse_command("SET key \"unterminated");
        assert_eq!(parsed, Err(ParseError::UnterminatedQuotedValue));
    }

    #[test]
    fn parse_get_command() {
        let parsed = parse_command("GET key");
        assert_eq!(
            parsed,
            Ok(Command::Get {
                key: "key".to_string()
            })
        );
    }

    #[test]
    fn parse_delete_command() {
        let parsed = parse_command("DELETE key");
        assert_eq!(
            parsed,
            Ok(Command::Delete {
                key: "key".to_string()
            })
        );
    }

    #[test]
    fn parse_batch_preserves_order_and_quotes() {
        assert_eq!(
            parse_command("BATCH SET \"key one\" \"value one\" DELETE old SET x y"),
            Ok(Command::Batch {
                operations: vec![
                    BatchOperation::Put {
                        key: b"key one".to_vec(),
                        value: b"value one".to_vec()
                    },
                    BatchOperation::Delete {
                        key: b"old".to_vec()
                    },
                    BatchOperation::Put {
                        key: b"x".to_vec(),
                        value: b"y".to_vec()
                    },
                ],
            })
        );
    }

    #[test]
    fn parse_batch_rejects_malformed_tail() {
        assert!(matches!(
            parse_command("BATCH SET valid value DELETE"),
            Err(ParseError::InvalidArity { command, .. }) if command == "BATCH"
        ));
    }

    #[test]
    fn malformed_batch_tail_is_rejected_before_dispatch() {
        let kv = MockKV::default();
        let parsed = parse_command("BATCH SET valid value DELETE");
        assert!(parsed.is_err());
        assert!(kv.batches.is_empty());
        assert!(kv.data.is_empty());
        assert_eq!((kv.puts, kv.deletes), (0, 0));
    }

    #[test]
    fn parse_batch_accepts_single_delete() {
        assert_eq!(
            parse_command("BATCH DELETE old"),
            Ok(Command::Batch {
                operations: vec![BatchOperation::Delete {
                    key: b"old".to_vec(),
                }],
            })
        );
    }

    #[test]
    fn parse_del_shorthand_command() {
        let parsed = parse_command("del key");
        assert_eq!(
            parsed,
            Ok(Command::Delete {
                key: "key".to_string()
            })
        );
    }

    #[test]
    fn parse_document_and_search_commands_with_quoted_text() {
        assert_eq!(
            parse_command("DOC PUT doc-1 \"a text document\""),
            Ok(Command::DocumentPut {
                id: "doc-1".into(),
                text: "a text document".into()
            })
        );
        assert_eq!(
            parse_command("DOC GET doc-1"),
            Ok(Command::DocumentGet { id: "doc-1".into() })
        );
        assert_eq!(
            parse_command("DOC DELETE doc-1"),
            Ok(Command::DocumentDelete { id: "doc-1".into() })
        );
        assert_eq!(
            parse_command("FTS AND \"a text\""),
            Ok(Command::Search {
                query: FtsQuery::And(vec!["a text".into()])
            })
        );
        assert_eq!(
            parse_command("FTS OR word another"),
            Ok(Command::Search {
                query: FtsQuery::Or(vec!["word another".into()])
            })
        );
        assert!(matches!(
            parse_command("DOC PUT only-id"),
            Err(ParseError::InvalidArity { .. })
        ));
        assert!(matches!(
            parse_command("FTS XOR term"),
            Err(ParseError::InvalidArity { .. })
        ));
    }

    #[tokio::test]
    async fn unsupported_fts_command_does_not_mutate_raw_engine() {
        let mut kv = MockKV::default();
        let result = execute_command_async(
            &mut kv,
            Command::DocumentPut {
                id: "doc".into(),
                text: "text".into(),
            },
        )
        .await;
        assert!(
            matches!(result, Err(DBError::StorageError(message)) if message.contains("unsupported"))
        );
        assert!(kv.data.is_empty());
        assert_eq!((kv.puts, kv.deletes), (0, 0));
        assert!(kv.batches.is_empty());
    }

    #[test]
    fn parse_lowercase_commands() {
        assert_eq!(
            parse_command("set key value"),
            Ok(Command::Set {
                key: "key".to_string(),
                value: "value".to_string()
            })
        );

        assert_eq!(
            parse_command("get key"),
            Ok(Command::Get {
                key: "key".to_string()
            })
        );

        assert_eq!(
            parse_command("delete key"),
            Ok(Command::Delete {
                key: "key".to_string()
            })
        );
    }

    #[test]
    fn parse_invalid_arity() {
        let parsed = parse_command("SET only_key");
        assert_eq!(
            parsed,
            Err(ParseError::InvalidArity {
                command: "SET".to_string(),
                expected: "SET <key> <value>"
            })
        );
    }

    #[test]
    fn execute_set_get_delete_flow() {
        let mut kv = MockKV::default();

        let set_result = execute_command(
            &mut kv,
            Command::Set {
                key: "k".to_string(),
                value: "v with spaces".to_string(),
            },
        );
        assert!(matches!(set_result, Ok(Response::Ok)));

        let get_result = execute_command(
            &mut kv,
            Command::Get {
                key: "k".to_string(),
            },
        );
        assert!(matches!(get_result, Ok(Response::Value(ref v)) if v == "v with spaces"));

        let delete_result = execute_command(
            &mut kv,
            Command::Delete {
                key: "k".to_string(),
            },
        );
        assert!(matches!(delete_result, Ok(Response::Ok)));

        let missing = execute_command(
            &mut kv,
            Command::Get {
                key: "k".to_string(),
            },
        );
        assert!(matches!(missing, Ok(Response::Nil)));
    }

    #[tokio::test]
    async fn execute_set_get_delete_flow_async() {
        let mut kv = MockKV::default();

        let set_result = execute_command_async(
            &mut kv,
            Command::Set {
                key: "k".to_string(),
                value: "v with spaces".to_string(),
            },
        )
        .await;
        assert!(matches!(set_result, Ok(Response::Ok)));

        let get_result = execute_command_async(
            &mut kv,
            Command::Get {
                key: "k".to_string(),
            },
        )
        .await;
        assert!(matches!(get_result, Ok(Response::Value(ref v)) if v == "v with spaces"));

        let delete_result = execute_command_async(
            &mut kv,
            Command::Delete {
                key: "k".to_string(),
            },
        )
        .await;
        assert!(matches!(delete_result, Ok(Response::Ok)));

        let missing = execute_command_async(
            &mut kv,
            Command::Get {
                key: "k".to_string(),
            },
        )
        .await;
        assert!(matches!(missing, Ok(Response::Nil)));
    }

    #[test]
    fn execute_batch_dispatches_once_in_order_and_propagates_error() {
        let operations = vec![
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            BatchOperation::Delete { key: b"b".to_vec() },
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"2".to_vec(),
            },
        ];
        let mut kv = MockKV::default();
        assert!(matches!(
            execute_command(
                &mut kv,
                Command::Batch {
                    operations: operations.clone()
                }
            ),
            Ok(Response::Ok)
        ));
        assert_eq!(kv.batches, vec![operations]);
        assert_eq!((kv.puts, kv.deletes), (0, 0));

        kv.fail_batch = true;
        assert!(matches!(
            execute_command(&mut kv, Command::Batch { operations: vec![] }),
            Err(DBError::StorageError(message)) if message == "batch failed"
        ));
    }

    #[test]
    fn batch_default_is_explicitly_unsupported() {
        let mut engine = DefaultBatchKV;
        assert!(matches!(
            execute_command(
                &mut engine,
                Command::Batch {
                    operations: vec![BatchOperation::Delete { key: b"key".to_vec() }]
                }
            ),
            Err(DBError::StorageError(message)) if message == "atomic batches are unsupported by this engine"
        ));
    }

    #[tokio::test]
    async fn async_batch_default_is_explicitly_unsupported() {
        let mut engine = DefaultBatchKV;
        assert!(matches!(
            execute_command_async(
                &mut engine,
                Command::Batch {
                    operations: vec![BatchOperation::Delete { key: b"key".to_vec() }]
                }
            )
            .await,
            Err(DBError::StorageError(message)) if message == "atomic batches are unsupported by this engine"
        ));
    }

    #[tokio::test]
    async fn execute_batch_dispatches_once_in_order_and_propagates_error_async() {
        let operations = vec![
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            BatchOperation::Delete { key: b"b".to_vec() },
            BatchOperation::Put {
                key: b"a".to_vec(),
                value: b"2".to_vec(),
            },
        ];
        let mut kv = MockKV::default();
        assert!(matches!(
            execute_command_async(
                &mut kv,
                Command::Batch {
                    operations: operations.clone()
                }
            )
            .await,
            Ok(Response::Ok)
        ));
        assert_eq!(kv.batches, vec![operations]);
        assert_eq!((kv.puts, kv.deletes), (0, 0));

        kv.fail_batch = true;
        assert!(matches!(
            execute_command_async(&mut kv, Command::Batch { operations: vec![] }).await,
            Err(DBError::StorageError(message)) if message == "batch failed"
        ));
    }
}
