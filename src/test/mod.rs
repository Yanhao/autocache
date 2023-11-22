#[cfg(feature = "localcache")]
mod local_cache_test;
#[cfg(feature = "rediscache")]
mod redis_cache_test;
#[cfg(feature = "ttlcache")]
mod ttl_cache_test;
#[cfg(feature = "twolevelcache")]
mod twolevel_cache_test;
