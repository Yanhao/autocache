use std::sync::Arc;

use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use futures::future::BoxFuture;

use crate::{cache::Cache, entry::EntryTrait};

#[derive(Clone)]
struct CacheItem<V> {
    time_to_remove_ms: Option<i64>,
    value: V,
}

impl<V> CacheItem<V> {
    fn need_to_remove(&self) -> bool {
        if self.time_to_remove_ms.is_none() {
            return false;
        }

        Utc.timestamp_millis_opt(self.time_to_remove_ms.unwrap())
            .unwrap()
            < chrono::Utc::now()
    }
}

pub struct TtlCache<K, V> {
    data: Arc<parking_lot::RwLock<im::OrdMap<K, CacheItem<V>>>>,

    ttl: Option<std::time::Duration>,
    expire_listener:
        ArcSwapOption<Box<dyn Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync>>,

    stop_notifier: ArcSwapOption<tokio::sync::Notify>,
}

impl<K, V> TtlCache<K, V> {
    pub fn new(ttl: Option<std::time::Duration>) -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            ttl,
            expire_listener: None.into(),

            stop_notifier: None.into(),
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

            stop_notifier: None.into(),
        }
    }

    pub fn set_expire_listener(
        &self,
        listener: impl Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Result<()> {
        if self.stop_notifier.load().is_some() {
            bail!("expire listener already set");
        }

        self.expire_listener
            .store(Some(Arc::new(Box::new(listener))));

        Ok(())
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
        Ok(keys
            .iter()
            .filter_map(|key| self.data.read().get(key).cloned().map(|c| c.value))
            .collect::<Vec<_>>())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        for kv in kvs.into_iter() {
            self.data.write().insert(
                kv.0.clone(),
                CacheItem {
                    time_to_remove_ms: self.ttl.map(|ttl| {
                        (chrono::Utc::now() + chrono::Duration::from_std(ttl).unwrap())
                            .timestamp_millis()
                    }),
                    value: kv.1.clone(),
                },
            );
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        keys.iter().for_each(|key| {
            self.data.write().remove(key);
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
        expire_listener: Arc<Box<dyn Fn(Vec<(K, V)>) -> BoxFuture<'static, ()> + Send + Sync>>,
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
        let cache_snap = cache.read().clone();

        let keys_to_remove = cache_snap
            .iter()
            .filter_map(|(key, ci)| {
                if ci.need_to_remove() {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .take(100)
            .collect::<Vec<_>>();

        for key in keys_to_remove.iter() {
            cache.write().remove(key);
        }
    }

    pub fn start(&self) -> Result<()> {
        if self.stop_notifier.load().is_some() {
            return Ok(());
        }

        let Some(listener) = self.expire_listener.load().clone() else {
            bail!("expire listener is none");
        };

        let notifier = Arc::new(tokio::sync::Notify::new());
        self.stop_notifier.store(Some(notifier.clone()));

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let cache = self.data.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ticker.tick()=> {
                        Self::cleanup_ttl(cache.clone());
                        Self::check_expires(cache.clone(), listener.clone()).await;
                    }

                    _ = notifier.notified() => {
                        return;
                    }
                }
            }
        });

        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_notifier.load().as_ref() {
            s.notify_one();
        }

        Ok(())
    }
}
