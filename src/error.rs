use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AutoCacheError {
    #[error("unsupported behaviour")]
    Unsupported,
    #[error("singleflight error")]
    SingleFlight,
    #[allow(dead_code)]
    #[error("no such key")]
    NoSuchKey,
    #[allow(dead_code)]
    #[error("base acache error")]
    Unknown,
}
