use std::collections::BTreeSet;

use crate::{cache::Cache, entry::EntryTrait};

pub struct TwoLevelCache<K, V, L1, L2> {
    local_cache: L1,
    redis_cache: L2,

    _m1: std::marker::PhantomData<K>,
    _m2: std::marker::PhantomData<V>,
}

impl<K, V, L1, L2> TwoLevelCache<K, V, L1, L2> {
    pub fn new(l1: L1, l2: L2) -> Self {
        Self {
            local_cache: l1,
            redis_cache: l2,

            _m1: std::marker::PhantomData,
            _m2: std::marker::PhantomData,
        }
    }
}

impl<K, V, L1, L2> Cache for TwoLevelCache<K, V, L1, L2>
where
    K: Clone + Ord + Sync,
    V: Clone + Sync + EntryTrait<K>,
    L1: Cache<Key = K, Value = V> + Sync,
    L2: Cache<Key = K, Value = V> + Sync,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        let mut l1_entries = self.local_cache.mget(keys).await?;
        let l1_hit_keys = l1_entries
            .iter()
            .filter(|entry| !entry.is_expired())
            .map(EntryTrait::get_key)
            .collect::<BTreeSet<_>>();
        let l1_missed_keys = keys
            .iter()
            .filter(|key| !l1_hit_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();

        if l1_missed_keys.is_empty() {
            l1_entries.retain(|entry| !entry.is_expired());
            return Ok(l1_entries);
        }

        let mut l2_entries = self.redis_cache.mget(&l1_missed_keys).await?;
        let fresh_l2_entries = l2_entries
            .iter()
            .filter(|entry| !entry.is_expired())
            .cloned()
            .map(|entry| (entry.get_key(), entry))
            .collect::<Vec<_>>();
        if !fresh_l2_entries.is_empty() {
            self.local_cache.mset(&fresh_l2_entries).await?;
        }

        let l2_hit_keys = l2_entries
            .iter()
            .map(EntryTrait::get_key)
            .collect::<BTreeSet<_>>();
        l1_entries.retain(|entry| !entry.is_expired() || !l2_hit_keys.contains(&entry.get_key()));

        l1_entries.append(&mut l2_entries);

        Ok(l1_entries)
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        self.redis_cache.mset(kvs).await?;
        self.local_cache.mset(kvs).await?;

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> anyhow::Result<()> {
        self.redis_cache.mdel(keys).await?;
        self.local_cache.mdel(keys).await?;

        Ok(())
    }

    fn name(&self) -> &'static str {
        "twolevelcache"
    }

    fn set_ns(&self, ns: String) {
        self.redis_cache.set_ns(ns);
    }
}
