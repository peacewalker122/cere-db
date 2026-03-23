use clap::Parser;
use log::info;
use wasm_kv::repl::run_repl;
use wasm_kv::{Config, PersistentKV};

#[tokio::main]
async fn main() {
    // Parse command line arguments
    let config = Config::parse();

    // Initialize logger
    config.init_logger();

    info!("Starting wasm-kv key-value store CLI");
    info!("Data directory: {}", config.data_dir);
    info!("Log level: {:?}", config.get_log_level());

    let mut kv = PersistentKV::new();

    if let Err(err) = run_repl(&mut kv) {
        eprintln!("REPL error: {err}");
    }
}
