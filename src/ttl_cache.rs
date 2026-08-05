use std::sync::Arc;

use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use futures::future::BoxFuture;

use crate::{cache::Cache, entry::EntryTrait, error::AutoCacheError};

type ExpireListener<K, V> = Box<dyn Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync>;

struct CleanupWorker {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CleanupWorker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Clone)]
struct CacheItem<V> {
    time_to_remove_ms: Option<i64>,
    value: V,
}

impl<V> CacheItem<V> {
    fn need_to_remove(&self) -> bool {
        self.time_to_remove_ms
            .is_some_and(|expires_at| expires_at <= Utc::now().timestamp_millis())
    }
}

pub struct TtlCache<K, V> {
    data: Arc<parking_lot::RwLock<im::OrdMap<K, CacheItem<V>>>>,

    ttl: Option<std::time::Duration>,
    expire_listener: ArcSwapOption<ExpireListener<K, V>>,

    cleanup_worker: parking_lot::Mutex<Option<CleanupWorker>>,
}

impl<K, V> TtlCache<K, V> {
    pub fn new(ttl: Option<std::time::Duration>) -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            ttl,
            expire_listener: None.into(),

            cleanup_worker: parking_lot::Mutex::new(None),
        }
    }

    pub fn new_with_expire_listener(
        ttl: Option<std::time::Duration>,
        listener: impl Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            ttl,
            expire_listener: ArcSwapOption::new(Some(Arc::new(Box::new(listener)))),

            cleanup_worker: parking_lot::Mutex::new(None),
        }
    }

    pub fn set_expire_listener(
        &self,
        listener: impl Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Result<()> {
        let cleanup_worker = self.cleanup_worker.lock();
        if cleanup_worker.is_some() {
            bail!("expire listener already set");
        }

        self.expire_listener
            .store(Some(Arc::new(Box::new(listener))));
        drop(cleanup_worker);

        Ok(())
    }

    fn stop_cleanup_worker(&self) {
        self.cleanup_worker.lock().take();
    }
}

impl<K, V> Drop for TtlCache<K, V> {
    fn drop(&mut self) {
        self.stop_cleanup_worker();
    }
}

impl<K, V> Cache for TtlCache<K, V>
where
    K: Ord + Sync + Send + Clone,
    V: Clone + Sync + Send,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Self::Value>> {
        if self.ttl.is_none() {
            let data = self.data.read();
            return Ok(keys
                .iter()
                .filter_map(|key| data.get(key).cloned().map(|item| item.value))
                .collect());
        }

        let mut data = self.data.write();
        Ok(keys
            .iter()
            .filter_map(|key| match data.get(key).cloned() {
                Some(item) if item.need_to_remove() => {
                    data.remove(key);
                    None
                }
                Some(item) => Some(item.value),
                None => None,
            })
            .collect())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        let time_to_remove_ms = self
            .ttl
            .map(|ttl| {
                let ttl_ms = i64::try_from(ttl.as_millis())?;
                Utc::now()
                    .timestamp_millis()
                    .checked_add(ttl_ms)
                    .ok_or_else(|| anyhow::anyhow!("TTL expiration timestamp overflow"))
            })
            .transpose()?;

        let mut data = self.data.write();
        for kv in kvs {
            data.insert(
                kv.0.clone(),
                CacheItem {
                    time_to_remove_ms,
                    value: kv.1.clone(),
                },
            );
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        let mut data = self.data.write();
        keys.iter().for_each(|key| {
            data.remove(key);
        });

        Ok(())
    }

    fn name(&self) -> &'static str {
        "ttlcache"
    }
}

impl<K, V> TtlCache<K, V>
where
    K: Ord + Sync + Send + Clone + 'static,
    V: Clone + Sync + Send + EntryTrait<K> + 'static,
{
    async fn check_expires(
        cache: Arc<parking_lot::RwLock<im::OrdMap<K, CacheItem<V>>>>,
        expire_listener: Arc<ExpireListener<K, V>>,
    ) {
        let cache_snap = cache.read().clone();

        let mut expires = Vec::with_capacity(128);

        for (key, ci) in cache_snap.iter() {
            if ci.value.is_expired() {
                expires.push((key.clone(), ci.value.clone()));

                if expires.len() == 100 {
                    expire_listener(expires.clone()).await;
                    expires.clear();
                }
            }
        }

        if !expires.is_empty() {
            expire_listener(expires.clone()).await;
        }
    }

    fn cleanup_ttl(cache: Arc<parking_lot::RwLock<im::OrdMap<K, CacheItem<V>>>>) {
        let mut cache = cache.write();
        let keys_to_remove = cache
            .iter()
            .filter_map(|(key, ci)| {
                if ci.need_to_remove() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        for key in keys_to_remove.iter() {
            cache.remove(key);
        }
    }

    pub fn start(&self) -> Result<()> {
        let mut cleanup_worker = self.cleanup_worker.lock();
        if cleanup_worker
            .as_ref()
            .is_some_and(|worker| !worker.task.is_finished())
        {
            return Ok(());
        }

        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| AutoCacheError::RuntimeUnavailable)?;
        let listener = self.expire_listener.load_full();

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let cache = self.data.clone();
        let task = runtime.spawn(async move {
            loop {
                ticker.tick().await;
                Self::cleanup_ttl(cache.clone());
                if let Some(listener) = listener.as_ref() {
                    Self::check_expires(cache.clone(), listener.clone()).await;
                }
            }
        });
        *cleanup_worker = Some(CleanupWorker { task });

        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        self.stop_cleanup_worker();
        Ok(())
    }
}
