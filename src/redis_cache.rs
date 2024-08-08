use std::sync::Arc;

use anyhow::Result;
use arc_swap::ArcSwapOption;
use bytes::Bytes;
use redis::AsyncCommands;

use crate::{cache::Cache, SerilizableEntryTrait};

pub struct RedisCache<K, V> {
    namespace: ArcSwapOption<String>,

    ttl_sec: usize,
    redis_cli: redis::Client,

    _m1: std::marker::PhantomData<K>,
    _m2: std::marker::PhantomData<V>,
}

impl<K, V> RedisCache<K, V> {
    pub fn new(cli: redis::Client) -> Self {
        Self::new_with_ttl(cli, 0)
    }
    pub fn new_with_ttl(cli: redis::Client, ttl_sec: usize) -> Self {
        Self {
            namespace: None.into(),
            ttl_sec,

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
        if let Some(ns) = self.namespace.load().as_ref() {
            if ns.as_ref() == "" {
                return k.as_ref().to_string();
            }

            let mut key = String::new();
            key.push_str(ns);
            key.push_str(":");

            key.push_str(k.as_ref());

            return key;
        } else {
            return k.as_ref().to_string();
        }
    }
}

impl<K, V> Cache for RedisCache<K, V>
where
    K: Clone + Sync + AsRef<str>,
    V: Sync + SerilizableEntryTrait,
{
    type Key = K;
    type Value = V;

    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Self::Value>> {
        let mut conn = self.redis_cli.get_async_connection().await?;

        if keys.len() == 1 {
            let key = keys.get(0).unwrap();
            let data: bytes::Bytes = conn.get(&self.generate_redis_key(key.clone())).await?;

            let value: V = V::decode(data)?;

            return Ok(vec![value]);
        }

        let res: Vec<Option<Bytes>> = conn
            .mget(
                keys.iter()
                    .map(|k| self.generate_redis_key(k.clone()))
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
            if self.ttl_sec == 0 {
                conn.set(
                    &self.generate_redis_key(kv.0.clone()),
                    kv.1.encode().unwrap().to_vec(),
                )
                .await?;
            } else {
                conn.set_ex(
                    &self.generate_redis_key(kv.0.clone()),
                    kv.1.encode().unwrap().to_vec(),
                    self.ttl_sec,
                )
                .await?;
            }
            return Ok(());
        }

        if self.ttl_sec == 0 {
            conn.mset(
                &kvs.iter()
                    .map(|(key, value)| {
                        (
                            self.generate_redis_key(key.clone()),
                            value.encode().unwrap().to_vec(),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await?;
        } else {
            let mut pipe = redis::Pipeline::new();
            pipe.mset(
                &kvs.iter()
                    .map(|(key, value)| {
                        (
                            self.generate_redis_key(key.clone()),
                            value.encode().unwrap().to_vec(),
                        )
                    })
                    .collect::<Vec<_>>(),
            );
            for (key, _) in kvs {
                pipe.expire(&self.generate_redis_key(key.clone()), self.ttl_sec);
            }

            pipe.query_async(&mut conn).await?;
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        let mut conn = self.redis_cli.get_async_connection().await?;
        conn.del(
            keys.iter()
                .map(|key| self.generate_redis_key(key.clone()))
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
