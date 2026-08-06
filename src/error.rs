use std::{error::Error as StdError, fmt, sync::Arc};

use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct ErrorSource(Arc<anyhow::Error>);

impl ErrorSource {
    pub fn from_anyhow(error: anyhow::Error) -> Self {
        Self(Arc::new(error))
    }

    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: StdError + Send + Sync + 'static,
    {
        self.0.downcast_ref()
    }
}

impl fmt::Debug for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl fmt::Display for ErrorSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

impl StdError for ErrorSource {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref().as_ref())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CacheOperation {
    Read,
    Write,
    Delete,
}

impl fmt::Display for CacheOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Delete => "delete",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LoaderKind {
    Single,
    Batch,
}

impl fmt::Display for LoaderKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Single => "single",
            Self::Batch => "batch",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SerializationOperation {
    Encode,
    Decode,
}

impl fmt::Display for SerializationOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Encode => "encode",
            Self::Decode => "decode",
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum ConfigurationError {
    #[error("cache is required")]
    MissingCache,
    #[error("loader is required")]
    MissingLoader,
    #[error("max_batch_size must be greater than zero")]
    InvalidMaxBatchSize,
    #[error("max_concurrent_async_cache_writes must be greater than zero")]
    InvalidMaxConcurrentAsyncCacheWrites,
    #[error("async_refresh_queue_capacity must be greater than zero")]
    InvalidAsyncRefreshQueueCapacity,
    #[error("expiration duration is out of range")]
    InvalidExpirationDuration,
    #[error("expire listener is already set")]
    ExpireListenerAlreadySet,
}

#[derive(Clone, Debug, Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),

    #[error("{cache} cache {operation} failed: {source}")]
    Cache {
        operation: CacheOperation,
        cache: &'static str,
        #[source]
        source: ErrorSource,
    },

    #[error("{kind} loader failed: {source}")]
    Loader {
        kind: LoaderKind,
        #[source]
        source: ErrorSource,
    },

    #[error("failed to {operation} with {codec}: {source}")]
    Serialization {
        operation: SerializationOperation,
        codec: &'static str,
        #[source]
        source: ErrorSource,
    },

    #[error("a Tokio runtime is required for background tasks")]
    RuntimeUnavailable,

    #[error("the refresh worker is unavailable")]
    RefreshUnavailable,

    #[error("singleflight failed: {message}")]
    SingleFlight { message: Arc<str> },
}

impl Error {
    pub fn cache(
        operation: CacheOperation,
        cache: &'static str,
        source: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Cache {
            operation,
            cache,
            source: ErrorSource::from_anyhow(source.into()),
        }
    }

    pub fn loader(kind: LoaderKind, source: anyhow::Error) -> Self {
        Self::Loader {
            kind,
            source: ErrorSource::from_anyhow(source),
        }
    }

    pub fn serialization(
        operation: SerializationOperation,
        codec: &'static str,
        source: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Serialization {
            operation,
            codec,
            source: ErrorSource::from_anyhow(source.into()),
        }
    }

    pub(crate) fn from_cache(
        operation: CacheOperation,
        cache: &'static str,
        source: anyhow::Error,
    ) -> Self {
        match source.downcast::<Self>() {
            Ok(error) => error,
            Err(source) => Self::cache(operation, cache, source),
        }
    }
}

#[cfg(test)]
mod tests {
    use thiserror::Error;

    use super::ErrorSource;

    #[derive(Debug, Error)]
    #[error("source marker")]
    struct SourceMarker;

    #[test]
    fn error_source_preserves_downcasting() {
        let source = ErrorSource::from_anyhow(SourceMarker.into());

        assert!(source.downcast_ref::<SourceMarker>().is_some());
    }
}
