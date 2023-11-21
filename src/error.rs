use thiserror::Error;

#[derive(Debug, Error)]
pub enum AutoCacheError {
    #[error("no such key")]
    NoSuchKey,
    #[error("base acache error")]
    Unknown,
}
