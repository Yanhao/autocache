#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]

mod autocache;
mod builder;
mod cache;
#[cfg(feature = "serilize")]
mod codec;
mod entry;
mod error;
mod loader;
#[cfg(feature = "localcache")]
pub mod local_cache;
mod options;
#[cfg(feature = "rediscache")]
pub mod redis_cache;
#[cfg(feature = "ttlcache")]
pub mod ttl_cache;
#[cfg(feature = "twolevelcache")]
pub mod twolevel_cache;

#[cfg(test)]
mod test;

pub use autocache::AutoCache;
pub use builder::AutoCacheBuilder;
pub use cache::Cache;
#[cfg(feature = "serilize")]
pub use codec::Codec;
#[cfg(feature = "serilize")]
pub use entry::SerilizableEntryTrait;
pub use entry::{Entry, EntryTrait};
pub use options::Options;
