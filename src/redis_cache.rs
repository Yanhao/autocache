use std::sync::Arc;

use anyhow::Result;
use arc_swap::{access::Access, ArcSwapOption};
use bytes::Bytes;
use redis::AsyncCommands;

use crate::{cache::Cache, SerilizableEntryTrait};

pub struct RedisCache<K, V> {
    namespace: ArcSwapOption<String>,

    redis_cli: redis::Client,

    _m1: std::marker::PhantomData<K>,
    _m2: std::marker::PhantomData<V>,
}

impl<K, V> RedisCache<K, V> {
    pub fn new(cli: redis::Client) -> Self {
        Self {
            namespace: None.into(),

            redis_cli: cli,
            _m1: std::marker::PhantomData,
            _m2: std::marker::PhantomData,
        }
    }
}

impl<K, V> RedisCache<K, V>
where
    K: Sync + AsRef<str>,
{
    fn generate_redis_key(&self, k: K) -> String {
        if self.namespace.load().is_none() {
            return k.as_ref().to_string();
        }

        if let Some(ns) = self.namespace.load().as_ref() {
            if ns == "" {
                return k.as_ref().to_string();
            }

            let mut key = String::new();
            key.push_str(ns);
            key.push_str(":");

            key.push_str(k.as_ref());

            return key;
        }
    }
}

impl<K, V> Cache for RedisCache<K, V>
where
    K: Sync + AsRef<str>,
    V: Sync + SerilizableEntryTrait,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Self::Value>> {
        let mut conn = self.redis_cli.get_async_connection().await?;

        if keys.len() == 1 {
            let key = keys.get(0).unwrap();
            let data: bytes::Bytes = conn.get(&self.generate_redis_key(key.as_ref())).await?;

            let value: V = V::decode(data)?;

            return Ok(vec![value]);
        }

        let res: Vec<Option<Bytes>> = conn
            .mget(
                keys.iter()
                    .map(|k| &self.generate_redis_key(key.as_ref()))
                    .collect::<Vec<_>>(),
            )
            .await?;

        Ok(res
            .into_iter()
            .filter_map(|data| data.map(|data| V::decode(data).unwrap()))
            .collect::<Vec<_>>())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        let mut conn = self.redis_cli.get_async_connection().await?;

        if kvs.len() == 1 {
            let kv = kvs.get(0).unwrap();
            conn.set(
                &self.generate_redis_key(kv.0.as_ref()),
                kv.1.encode().unwrap().to_vec(),
            )
            .await?;

            return Ok(());
        }

        conn.mset(
            &kvs.iter()
                .map(|(key, value)| {
                    (
                        &self.generate_redis_key(key.as_ref()),
                        value.encode().unwrap().to_vec(),
                    )
                })
                .collect::<Vec<_>>(),
        )
        .await?;

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        let mut conn = self.redis_cli.get_async_connection().await?;
        conn.del(
            keys.iter()
                .map(|key| &self.generate_redis_key(key.as_ref()))
                .collect::<Vec<_>>(),
        )
        .await?;

        Ok(())
    }

    fn name(&self) -> &'static str {
        "rediscache"
    }

    fn set_ns(&self, ns: String) {
        self.namespace.store(Some(Arc::new(ns)));
    }
}
