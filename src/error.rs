use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AutoCacheError {
    #[error("cache is required")]
    MissingCache,
    #[error("loader is required")]
    MissingLoader,
    #[error("max_batch_size must be greater than zero")]
    InvalidMaxBatchSize,
    #[error("max_concurrent_async_cache_writes must be greater than zero")]
    InvalidMaxConcurrentAsyncCacheWrites,
    #[error("expiration duration is out of range")]
    InvalidExpirationDuration,
    #[error("a Tokio runtime is required for background tasks")]
    RuntimeUnavailable,
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
