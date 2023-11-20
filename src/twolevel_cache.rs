use crate::{cache::Cache, codec::Codec, entry::EntryTrait};

pub struct TwoLevelCache<K, V, L1, L2>
where
    K: Ord + Sync + Send + AsRef<str> + Codec + Clone,
    V: Clone + Sync + Send + Codec,
    L1: Cache<Key = K, Value = V>,
    L2: Cache<Key = K, Value = V>,
{
    local_cache: L1,
    redis_cache: L2,

    _m1: std::marker::PhantomData<K>,
    _m2: std::marker::PhantomData<V>,
}

impl<K, V, L1, L2> Cache for TwoLevelCache<K, V, L1, L2>
where
    K: Ord + Sync + Send + AsRef<str> + Codec + Clone,
    V: Clone + Sync + Send + Codec + EntryTrait<K>,
    L1: Cache<Key = K, Value = V>,
    L2: Cache<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        let mut l1_entries = self.local_cache.mget(keys).await?;
        let l1_missed_keys = l1_entries
            .iter()
            .filter_map(|e| {
                let k = e.get_key();
                if keys.contains(&k) {
                    None
                } else {
                    Some(k)
                }
            })
            .collect::<Vec<_>>();

        let mut l2_entries = self.redis_cache.mget(&l1_missed_keys).await?;
        self.local_cache
            .mset(
                &l2_entries
                    .clone()
                    .into_iter()
                    .map(|e| (e.get_key(), e))
                    .collect::<Vec<_>>(),
            )
            .await?;

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
}
