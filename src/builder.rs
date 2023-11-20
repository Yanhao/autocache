use anyhow::Result;
use futures::future::BoxFuture;

use crate::{autocache::AutoCache, cache::Cache, codec::Codec, entry::Entry, loader::Loader};

pub struct AutoCacheBuilder<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec,
    V: Clone + Codec,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache: Option<C>,
    pub(crate) loader: Option<Loader<K, V>>,

    pub(crate) cache_none: bool,
    pub(crate) expire_time: std::time::Duration,
    pub(crate) none_value_expire_time: std::time::Duration,
    pub(crate) source_first: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) async_set_cache: bool,

    pub(crate) use_expired_data: bool,
    pub(crate) namespace: Option<String>,
}

impl<K, V, C> AutoCacheBuilder<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec,
    V: Clone + Codec,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub fn new() -> Self {
        Self {
            loader: None,
            cache: None,
            expire_time: std::time::Duration::from_secs(60),
            none_value_expire_time: std::time::Duration::from_secs(60),
            max_batch_size: 100,
            async_set_cache: true,

            source_first: false,
            use_expired_data: false,
            cache_none: false,
            namespace: None,
        }
    }

    pub fn cache(mut self, c: C) -> Self {
        self.cache = Some(c);
        self
    }

    pub fn single_loader(
        mut self,
        l: impl Fn(K) -> BoxFuture<'static, Result<Option<V>>> + 'static,
    ) -> Self {
        self.loader = Some(Loader::SingleLoader(Box::new(l)));
        self
    }

    pub fn multi_loader(
        mut self,
        l: impl Fn(&[K]) -> BoxFuture<'static, Result<Vec<(K, V)>>> + 'static,
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

    pub fn async_set_cache(mut self, t: bool) -> Self {
        self.async_set_cache = t;
        self
    }

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

    pub fn build(self) -> AutoCache<K, V, C> {
        AutoCache::<K, V, C> {
            cache_store: self.cache.unwrap(),
            loader: self.loader.unwrap(),
            namespace: self.namespace,
            cache_none: self.cache_none,
            expire_time: self.expire_time,
            none_value_expire_time: self.none_value_expire_time,
            source_first: self.source_first,
            max_batch_size: self.max_batch_size,
            async_set_cache: self.async_set_cache,
            use_expired_data: self.use_expired_data,

            sfg: async_singleflight::Group::new(),
            mfg: async_singleflight::Group::new(),
        }
    }
}
