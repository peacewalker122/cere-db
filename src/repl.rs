use crate::{
    api::api::{AsyncKVEngine, KVEngine},
    command::{ParseError, Response, execute_command, execute_command_async, parse_command},
};
use std::io::{self, BufRead, Write};

fn format_response(response: &Response) -> &'static str {
    match response {
        Response::Ok => "OK",
        Response::Nil => "(nil)",
        Response::Bye => "bye",
        Response::Value(_) => "",
    }
}

pub fn run_repl<E: KVEngine>(engine: &mut E) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdin_lock = stdin.lock();
    let mut stdout = io::stdout();
    let mut line = String::new();

    writeln!(
        stdout,
        "wasm-kv CLI REPL (SET/GET/DELETE). Type EXIT to quit."
    )?;

    loop {
        write!(stdout, "> ")?;
        stdout.flush()?;

        line.clear();
        let bytes_read = stdin_lock.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        match parse_command(&line) {
            Ok(command) => match execute_command(engine, command) {
                Ok(Response::Value(value)) => writeln!(stdout, "{value}")?,
                Ok(response) => {
                    writeln!(stdout, "{}", format_response(&response))?;
                    if response == Response::Bye {
                        break;
                    }
                }
                Err(err) => writeln!(stdout, "ERR {err}")?,
            },
            Err(ParseError::Empty) => continue,
            Err(ParseError::UnknownCommand(command)) => {
                writeln!(stdout, "ERR unknown command: {command}")?
            }
            Err(ParseError::InvalidArity { expected, .. }) => {
                writeln!(stdout, "ERR usage: {expected}")?
            }
            Err(ParseError::UnterminatedQuotedValue) => {
                writeln!(stdout, "ERR unterminated quoted value")?
            }
        }
    }

    Ok(())
}

pub async fn run_repl_async<E: AsyncKVEngine>(engine: &mut E) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdin_lock = stdin.lock();
    let mut stdout = io::stdout();
    let mut line = String::new();

    writeln!(
        stdout,
        "wasm-kv CLI REPL (SET/GET/DELETE). Type EXIT to quit."
    )?;

    loop {
        write!(stdout, "> ")?;
        stdout.flush()?;

        line.clear();
        let bytes_read = stdin_lock.read_line(&mut line)?;
        if bytes_read == 0 {
            break;
        }

        match parse_command(&line) {
            Ok(command) => match execute_command_async(engine, command).await {
                Ok(Response::Value(value)) => writeln!(stdout, "{value}")?,
                Ok(response) => {
                    writeln!(stdout, "{}", format_response(&response))?;
                    if response == Response::Bye {
                        break;
                    }
                }
                Err(err) => writeln!(stdout, "ERR {err}")?,
            },
            Err(ParseError::Empty) => continue,
            Err(ParseError::UnknownCommand(command)) => {
                writeln!(stdout, "ERR unknown command: {command}")?
            }
            Err(ParseError::InvalidArity { expected, .. }) => {
                writeln!(stdout, "ERR usage: {expected}")?
            }
            Err(ParseError::UnterminatedQuotedValue) => {
                writeln!(stdout, "ERR unterminated quoted value")?
            }
        }
    }

    Ok(())
}
