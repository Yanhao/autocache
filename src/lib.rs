#![feature(impl_trait_in_assoc_type)]
#![feature(async_closure)]
#![feature(type_alias_impl_trait)]
#![feature(associated_type_defaults)]
#![feature(result_option_inspect)]

pub mod autocache;
pub mod builder;
pub mod cache;
pub mod codec;
pub mod entry;
pub mod error;
pub mod loader;
pub mod local_cache;
pub mod redis_cache;
pub mod ttl_cache;
pub mod twolevel_cache;

#[cfg(test)]
mod test;
