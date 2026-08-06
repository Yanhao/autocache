#![doc = include_str!("../README.md")]

mod autocache;
mod builder;
mod cache;
#[cfg(feature = "serialize")]
mod codec;
mod entry;
mod error;
mod loader;
#[cfg(feature = "localcache")]
pub mod local_cache;
mod options;
#[cfg(feature = "rediscache")]
pub mod redis_cache;
mod singleflight;
#[cfg(feature = "ttlcache")]
pub mod ttl_cache;
#[cfg(feature = "twolevelcache")]
pub mod twolevel_cache;

#[cfg(test)]
mod test;

pub use autocache::AutoCache;
pub use builder::AutoCacheBuilder;
pub use cache::Cache;
#[cfg(feature = "serialize")]
pub use codec::Codec;
#[cfg(feature = "serialize")]
pub use entry::SerializableEntryTrait;
pub use entry::{Entry, EntryTrait};
pub use error::{
    CacheOperation, ConfigurationError, Error, ErrorSource, LoaderKind, Result,
    SerializationOperation,
};
pub use options::Options;
