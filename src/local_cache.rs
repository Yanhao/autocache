use std::{fmt::Debug, hash::Hash};

use anyhow::Result;
use derivative::Derivative;
use moka::sync::SegmentedCache;
use tracing::debug;

use crate::cache::Cache;

pub struct LocalCache<K, V>
where
    K: Hash + Eq + Sync + Send + Clone,
    V: Clone + Sync + Send,
{
    data: SegmentedCache<K, V>,
}

#[derive(Derivative)]
#[derivative(Default)]
pub struct LocalCacheOption {
    #[derivative(Default(value = "8"))]
    pub segments: usize,
    #[derivative(Default(value = "std::time::Duration::from_secs( 5 * 60)"))]
    pub ttl: std::time::Duration,
    #[derivative(Default(value = "1024"))]
    pub max_capacity: u64,
}

impl<K, V> LocalCache<K, V>
where
    K: Hash + Eq + Sync + Send + Clone + 'static,
    V: Clone + Sync + Send + 'static,
{
    pub fn new(opts: LocalCacheOption) -> Self {
        let data = SegmentedCache::builder(opts.segments)
            .time_to_live(opts.ttl)
            .max_capacity(opts.max_capacity)
            .build();

        Self { data }
    }
}

impl<K, V> Cache for LocalCache<K, V>
where
    K: Hash + Eq + Sync + Send + Clone + 'static + Debug,
    V: Clone + Sync + Send + 'static,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Self::Value>> {
        debug!("autocache: localcache: mget keys: {keys:?}");

        Ok(keys
            .iter()
            .filter_map(|key| self.data.get(key).clone())
            .collect::<Vec<_>>())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        debug!(
            "autocache: localcache: mset keys: {:?}",
            kvs.iter().map(|(k, _)| k).collect::<Vec<_>>()
        );

        for kv in kvs.into_iter() {
            self.data.insert(kv.0.clone(), kv.1.clone());
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        debug!("autocache: localcache: mdel keys: {keys:?}");

        keys.iter().for_each(|key| {
            self.data.remove(key);
        });

        Ok(())
    }
}
