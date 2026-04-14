use clap::Parser;
use log::info;
use wasm_kv::repl::run_repl_async;
use wasm_kv::{Config, KV2};

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logger
    config.init_logger();

    info!("Starting wasm-kv key-value store CLI");
    info!("Data directory: {}", config.data_dir);
    info!("Log level: {:?}", config.get_log_level());

    let mut kv = match KV2::open(&config.data_dir).await {
        Ok(engine) => engine,
        Err(err) => {
            eprintln!("Failed to initialize KV2 engine: {err}");
            return;
        }
    };

    if let Err(err) = run_repl_async(&mut kv).await {
        eprintln!("REPL error: {err}");
    }
}
