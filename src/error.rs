use thiserror::Error;

#[derive(Debug, Error)]
pub enum AutoCacheError {
    #[error("unsupported behaviour")]
    Unsupported,
    #[allow(dead_code)]
    #[error("no such key")]
    NoSuchKey,
    #[allow(dead_code)]
    #[error("base acache error")]
    Unknown,
}
