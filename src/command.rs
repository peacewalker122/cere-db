use crate::{
    api::api::{AsyncKVEngine, BatchOperation, KVEngine},
    error::DBError,
};
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
    Batch { operations: Vec<BatchOperation> },
    Exit,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Ok,
    Value(String),
    Nil,
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
        Command::Exit => Ok(Response::Bye),
    }
}

pub async fn execute_command_async<E: AsyncKVEngine>(
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
