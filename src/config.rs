use clap::Parser;
use log::LevelFilter;

/// Configuration for the KV store
#[derive(Parser, Debug, Clone)]
#[command(name = "ceredb")]
#[command(about = "A persistent key-value store with WAL and SSTable", long_about = None)]
pub struct Config {
    /// Set the logging level (off, error, warn, info, debug, trace)
    #[arg(short, long, default_value = "info")]
    pub log_level: String,

    /// Enable verbose logging (equivalent to --log-level debug)
    #[arg(short, long)]
    pub verbose: bool,

    /// Data directory for storing database files
    #[arg(short, long, default_value = ".")]
    pub data_dir: String,
}

impl Config {
    pub fn get_log_level(&self) -> LevelFilter {
        if self.verbose {
            return LevelFilter::Debug;
        }

        match self.log_level.to_lowercase().as_str() {
            "off" => LevelFilter::Off,
            "error" => LevelFilter::Error,
            "warn" => LevelFilter::Warn,
            "info" => LevelFilter::Info,
            "debug" => LevelFilter::Debug,
            "trace" => LevelFilter::Trace,
            _ => {
                eprintln!("Invalid log level '{}', using 'info'", self.log_level);
                LevelFilter::Info
            }
        }
    }

    pub fn init_logger(&self) {
        env_logger::Builder::from_default_env()
            .filter_level(self.get_log_level())
            .format_timestamp_millis()
            .init();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_parsing() {
        let config = Config {
            log_level: "debug".to_string(),
            verbose: false,
            data_dir: ".".to_string(),
        };
        assert_eq!(config.get_log_level(), LevelFilter::Debug);
    }

    #[test]
    fn test_verbose_flag() {
        let config = Config {
            log_level: "info".to_string(),
            verbose: true,
            data_dir: ".".to_string(),
        };
        assert_eq!(config.get_log_level(), LevelFilter::Debug);
    }
}
