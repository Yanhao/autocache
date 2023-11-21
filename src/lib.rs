#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(result_option_inspect)]

mod autocache;
mod builder;
mod cache;
mod codec;
mod entry;
mod error;
mod loader;
pub mod local_cache;
pub mod redis_cache;
pub mod ttl_cache;
pub mod twolevel_cache;

#[cfg(test)]
mod test;

pub use autocache::AutoCache;
pub use cache::Cache;
pub use codec::Codec;
pub use entry::{EntryTrait, SerilizableEntryTrait};
