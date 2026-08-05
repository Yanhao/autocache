use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;
use futures::future::BoxFuture;

use crate::{
    autocache::AutoCache, cache::Cache, entry::Entry, error::AutoCacheError, loader::Loader,
    singleflight::Group,
};

const DEFAULT_MAX_CONCURRENT_ASYNC_CACHE_WRITES: usize = 64;

pub struct AutoCacheBuilder<K, V, C, E>
where
    K: Clone,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache: Option<C>,
    pub(crate) loader: Option<Loader<K, V, E>>,

    pub(crate) cache_none: bool,
    pub(crate) expire_time: std::time::Duration,
    pub(crate) none_value_expire_time: std::time::Duration,
    pub(crate) source_first: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) async_set_cache: bool,
    pub(crate) max_concurrent_async_cache_writes: usize,
    pub(crate) manually_refresh: bool,

    pub(crate) use_expired_data: bool,
    pub(crate) namespace: Option<String>,

    pub(crate) on_metrics:
        Option<fn(method: &str, is_error: bool, ns: &str, from: &str, cache_name: &str)>,
}

impl<K, V, C, E> AutoCacheBuilder<K, V, C, E>
where
    K: Clone + Debug + Eq + Hash + Sync + Send + 'static,
    V: Clone + Debug + Sync + Send + 'static,
    C: Cache<Key = K, Value = Entry<K, V>> + Sync + Send + 'static,
    E: Clone + Debug + Sync + Send + 'static,
{
    pub fn new() -> Self {
        Self {
            loader: None,
            cache: None,
            expire_time: std::time::Duration::from_secs(60),
            none_value_expire_time: std::time::Duration::from_secs(60),
            max_batch_size: 100,
            async_set_cache: false,
            max_concurrent_async_cache_writes: DEFAULT_MAX_CONCURRENT_ASYNC_CACHE_WRITES,
            cache_none: false,

            source_first: false,
            use_expired_data: false,
            manually_refresh: false,

            namespace: None,
            on_metrics: None,
        }
    }

    pub fn cache(mut self, c: C) -> Self {
        self.cache = Some(c);
        self
    }

    /// Sets a loader for individual cache keys.
    ///
    /// `Ok(None)` is an authoritative not-found result. In source-first mode,
    /// AutoCache will not fall back to a previously cached value for that key.
    /// Use `Err` when the source could not determine whether the key exists.
    ///
    /// For a given `K`, the loader result must not vary based on `E`. Any input
    /// that changes the cached value must be represented in `K` itself.
    pub fn single_loader(
        mut self,
        l: impl Fn(K, E) -> BoxFuture<'static, Result<Option<V>>> + 'static + Send + Sync,
    ) -> Self {
        self.loader = Some(Loader::SingleLoader(Box::new(l)));
        self
    }

    /// Sets a loader for batches of cache keys.
    ///
    /// Omitting a requested key from the returned vector is an authoritative
    /// not-found result for that key. In source-first mode, AutoCache will not
    /// fall back to a previously cached value. Return `Err` when the batch
    /// result is incomplete or otherwise cannot be trusted.
    ///
    /// For a given `K`, the loader result must not vary based on `E`. Any input
    /// that changes the cached value must be represented in `K` itself.
    pub fn multi_loader(
        mut self,
        l: impl Fn(Vec<(K, E)>) -> BoxFuture<'static, Result<Vec<(K, V)>>> + 'static + Send + Sync,
    ) -> Self {
        self.loader = Some(Loader::MultiLoader(Box::new(l)));
        self
    }

    pub fn namespace(mut self, ns: String) -> Self {
        self.namespace = Some(ns);
        self
    }

    pub fn source_first(mut self, t: bool) -> Self {
        self.source_first = t;
        self
    }

    pub fn max_batch_size(mut self, sz: usize) -> Self {
        self.max_batch_size = sz;
        self
    }

    pub fn use_expired_data(mut self, t: bool) -> Self {
        self.use_expired_data = t;
        self
    }

    pub fn manually_refresh(mut self, t: bool) -> Self {
        self.manually_refresh = t;
        self
    }

    /// Enables best-effort asynchronous cache fills.
    ///
    /// Async fills are unordered and limited by
    /// [`Self::max_concurrent_async_cache_writes`]. A fill is skipped when the
    /// limit is reached. If no Tokio runtime is available, the fill runs
    /// synchronously instead.
    pub fn async_set_cache(mut self, t: bool) -> Self {
        self.async_set_cache = t;
        self
    }

    /// Sets the maximum number of concurrent best-effort async cache writes.
    ///
    /// The default is 64. Additional fills are skipped while the limit is
    /// reached. The value must be greater than zero.
    pub fn max_concurrent_async_cache_writes(mut self, limit: usize) -> Self {
        self.max_concurrent_async_cache_writes = limit;
        self
    }

    /// Controls whether authoritative not-found results are cached.
    ///
    /// This setting affects only negative-cache storage, not the value returned
    /// to the caller. When disabled, an authoritative not-found result removes
    /// any existing positive cache entry without storing a negative entry.
    pub fn cache_none(mut self, t: bool) -> Self {
        self.cache_none = t;
        self
    }

    pub fn expire_time(mut self, time: std::time::Duration) -> Self {
        self.expire_time = time;
        self
    }

    pub fn none_value_expire_time(mut self, time: std::time::Duration) -> Self {
        self.none_value_expire_time = time;
        self
    }

    /// Registers a callback for cache operation metrics.
    ///
    /// Source reads use method `mget`. Failed or skipped automatic cache fills
    /// use method `mset` with `is_error=true` and `from="source"`.
    pub fn on_metrics(
        mut self,
        func: fn(method: &str, is_error: bool, ns: &str, from: &str, cache_name: &str),
    ) -> Self {
        self.on_metrics = Some(func);
        self
    }

    pub fn build(self) -> Result<AutoCache<K, V, C, E>> {
        let cache = self.cache.ok_or(AutoCacheError::MissingCache)?;
        let loader = self.loader.ok_or(AutoCacheError::MissingLoader)?;
        if self.max_batch_size == 0 {
            return Err(AutoCacheError::InvalidMaxBatchSize.into());
        }
        if self.max_concurrent_async_cache_writes == 0 {
            return Err(AutoCacheError::InvalidMaxConcurrentAsyncCacheWrites.into());
        }

        let mut ac = AutoCache::<K, V, C, E> {
            cache_store: Arc::new(cache),
            loader: Arc::new(loader),
            namespace: self.namespace.clone(),
            cache_none: self.cache_none,
            expire_time: self.expire_time,
            none_value_expire_time: self.none_value_expire_time,
            source_first: self.source_first,
            max_batch_size: self.max_batch_size,
            async_set_cache: self.async_set_cache,
            async_cache_write_permits: Arc::new(tokio::sync::Semaphore::new(
                self.max_concurrent_async_cache_writes,
            )),
            use_expired_data: self.use_expired_data,
            manually_refresh: self.manually_refresh,

            sfg: Arc::new(Group::new()),
            mfg: Arc::new(Group::new()),

            async_refresh_channel: None.into(),
            pending_refresh_keys: Arc::new(parking_lot::Mutex::new(
                std::collections::HashSet::new(),
            )),
            stop_ch: None,

            on_metrics: self.on_metrics,
        };

        if let Some(ns) = self.namespace.as_ref() {
            ac.cache_store.set_ns(ns.clone());
        }

        if ac.use_expired_data || ac.manually_refresh {
            ac.start()?;
        }

        Ok(ac)
    }
}
