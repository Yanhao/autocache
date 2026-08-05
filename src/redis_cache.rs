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
            if ns.is_empty() {
                return k.as_ref().to_string();
            }

            let mut key = String::new();
            key.push_str(ns);
            key.push(':');

            key.push_str(k.as_ref());

            key
        } else {
            k.as_ref().to_string()
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
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut conn = self.redis_cli.get_multiplexed_async_connection().await?;

        if let [key] = keys {
            let data: Option<Bytes> = conn.get(self.generate_redis_key(key.clone())).await?;

            return match data {
                Some(data) => Ok(vec![V::decode(data)?]),
                None => Ok(vec![]),
            };
        }

        let res: Vec<Option<Bytes>> = conn
            .mget(
                keys.iter()
                    .map(|k| self.generate_redis_key(k.clone()))
                    .collect::<Vec<_>>(),
            )
            .await?;

        res.into_iter().flatten().map(V::decode).collect()
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        let encoded_kvs = kvs
            .iter()
            .map(|(key, value)| {
                Ok((
                    self.generate_redis_key(key.clone()),
                    value.encode()?.to_vec(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut conn = self.redis_cli.get_multiplexed_async_connection().await?;

        if let [(key, value)] = encoded_kvs.as_slice() {
            if self.ttl_sec == 0 {
                conn.set::<_, _, ()>(key, value).await?;
            } else {
                conn.set_ex::<_, _, ()>(key, value, self.ttl_sec.try_into()?)
                    .await?;
            }
            return Ok(());
        }

        if self.ttl_sec == 0 {
            conn.mset::<_, _, ()>(&encoded_kvs).await?;
        } else {
            let mut pipe = redis::Pipeline::new();
            pipe.mset(&encoded_kvs);
            let ttl_sec = self.ttl_sec.try_into()?;
            for (key, _) in &encoded_kvs {
                pipe.expire(key, ttl_sec);
            }

            pipe.query_async::<()>(&mut conn).await?;
        }

        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut conn = self.redis_cli.get_multiplexed_async_connection().await?;
        conn.del::<_, ()>(
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
