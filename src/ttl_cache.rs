use std::sync::Arc;

use anyhow::Result;

use crate::{cache::Cache, entry::EntryTrait};

pub struct TtlCache<K, V>
where
    K: Ord + Sync + Send + Clone,
    V: Clone + Sync + Send,
{
    data: Arc<parking_lot::RwLock<im::OrdMap<K, V>>>,
    stop_notifier: Option<Arc<tokio::sync::Notify>>,
}

impl<K, V> TtlCache<K, V>
where
    K: Ord + Sync + Send + Clone,
    V: Clone + Sync + Send,
{
    pub fn new() -> Self {
        Self {
            data: Arc::new(parking_lot::RwLock::new(im::OrdMap::new())),
            stop_notifier: None,
        }
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
            .filter_map(|key| self.data.read().get(key).cloned())
            .collect::<Vec<_>>())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        for kv in kvs.into_iter() {
            self.data.write().insert(kv.0.clone(), kv.1.clone());
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        keys.iter().for_each(|key| {
            self.data.write().remove(key);
        });

        Ok(())
    }
}

impl<K, V> TtlCache<K, V>
where
    K: Ord + Sync + Send + Clone + 'static,
    V: Clone + Sync + Send + EntryTrait<K> + 'static,
{
    fn cleanup_expiration(cache: Arc<parking_lot::RwLock<im::OrdMap<K, V>>>) {
        let cache_snap = cache.read().clone();

        let keys_to_remove = cache_snap
            .iter()
            .filter_map(|(key, migration)| {
                if migration.is_outdated() {
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

    pub fn start(&mut self) -> Result<()> {
        if self.stop_notifier.is_some() {
            return Ok(());
        }

        let notifier = Arc::new(tokio::sync::Notify::new());
        self.stop_notifier.replace(notifier.clone());

        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let cache = self.data.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = ticker.tick()=> {
                        Self::cleanup_expiration(cache.clone());
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
        if let Some(s) = self.stop_notifier.as_ref() {
            s.notify_one();
        }

        Ok(())
    }
}
