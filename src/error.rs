use thiserror::Error;

#[derive(Debug, Error)]
pub enum DBError {
    #[error("io error occurred: {0}")]
    IO(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    TryIntoError(#[from] std::array::TryFromSliceError),

    #[error("Corrupted page: {0}")]
    Corrupted(String),

    #[error("mutex poisoned")]
    MutexPoisoned(String),

    #[error("storage error: {0}")]
    StorageError(String),

    #[error("key not found")]
    NotFound,
}
