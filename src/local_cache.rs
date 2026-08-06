use std::{fmt::Debug, hash::Hash};

use anyhow::Result;
use derivative::Derivative;
use moka::sync::SegmentedCache;
use tracing::{debug, warn};

use crate::{cache::Cache, Entry};

pub struct LocalCache<K, V> {
    data: SegmentedCache<K, Entry<K, V>>,
}

#[derive(Derivative)]
#[derivative(Default)]
pub struct LocalCacheOption {
    #[derivative(Default(value = "8"))]
    pub segments: usize,
    #[derivative(Default(value = "std::time::Duration::from_secs(5 * 60)"))]
    pub ttl: std::time::Duration,
    #[derivative(Default(value = "1024"))]
    pub max_capacity: u64,
}

impl<K, V> LocalCache<K, V>
where
    K: Hash + Eq + Sync + Send + Clone + 'static,
    V: Sync + Send + Clone + 'static,
{
    pub fn new(opts: LocalCacheOption) -> Self {
        let segments = if opts.segments == 0 {
            warn!("autocache: LocalCache segments must be greater than zero; using one segment");
            1
        } else {
            opts.segments
        };
        let data = SegmentedCache::builder(segments)
            .time_to_live(opts.ttl)
            .max_capacity(opts.max_capacity)
            .build();

        Self { data }
    }
}

impl<K, V> Cache for LocalCache<K, V>
where
    K: Hash + Eq + Sync + Send + Clone + Debug + 'static,
    V: Sync + Send + Clone + 'static,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Entry<Self::Key, Self::Value>>> {
        debug!("autocache: localcache: mget keys: {keys:?}");

        Ok(keys
            .iter()
            .filter_map(|key| self.data.get(key).clone())
            .collect::<Vec<_>>())
    }

    async fn mset(&self, entries: &[Entry<Self::Key, Self::Value>]) -> Result<()> {
        debug!(
            "autocache: localcache: mset keys: {:?}",
            entries.iter().map(|entry| &entry.key).collect::<Vec<_>>()
        );

        for entry in entries {
            self.data.insert(entry.key.clone(), entry.clone());
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

    fn name(&self) -> &'static str {
        "localcache"
    }
}
