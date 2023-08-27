use std::fmt::Debug;

use anyhow::Result;
use chrono::{prelude::*, Duration};

use crate::{
    builder::AcacheBuilder,
    cache::Cache,
    codec::Codec,
    entry::{Entry, EntryTrait},
    loader::Loader,
};

pub struct Acache<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec,
    V: Clone + Codec,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache_store: C,
    pub(crate) loader: Loader<K, V>,

    pub(crate) sfg: async_singleflight::Group<V, anyhow::Error>,
    pub(crate) mfg: async_singleflight::Group<Vec<(K, V)>, anyhow::Error>,

    pub(crate) namespace: Option<String>,
    pub(crate) expire_time: std::time::Duration,
    pub(crate) cache_none: bool,
    pub(crate) none_value_expire_time: std::time::Duration,
    pub(crate) source_first: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) async_set_cache: bool,
    pub(crate) use_expired_data: bool, // means async source
}

impl<K, V, C> Acache<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec + Debug,
    V: Clone + Codec + Debug,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub fn builder() -> AcacheBuilder<K, V, C> {
        AcacheBuilder::new()
    }

    async fn mget_by_sloader(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        let mut entries = self.cache_store.mget(keys).await?;

        let missed_keys = keys
            .iter()
            .filter_map(|key| {
                for ent in entries.iter() {
                    if &ent.key == key {
                        return None;
                    }
                }

                return Some(key.clone());
            })
            .collect::<Vec<_>>();

        let Loader::<K, V>::SingleLoader(ref sloader) = self.loader else {
            unreachable!();
        };

        let mut missed_entries = Vec::with_capacity(missed_keys.len());
        for key in missed_keys.iter() {
            let (value, err, _owner) = self
                .sfg
                .work(key.as_ref(), (|| async { (sloader)(key.clone()).await })())
                .await;

            if err.is_some() {
                anyhow::bail!("single flight error");
            }

            let entry = Entry {
                key: key.clone(),
                value: Some(value.unwrap()),
                expire_at_ms: (Utc::now() + Duration::from_std(self.expire_time).unwrap())
                    .timestamp_millis(),
            };

            missed_entries.push(entry.clone());

            self.cache_store.mset(&[(key.clone(), entry)]).await?;
        }

        entries.append(&mut missed_entries);

        Ok(entries
            .into_iter()
            .map(|entry| (entry.key, entry.value.unwrap()))
            .collect())
    }

    async fn mget_by_mloader(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        let mut entries = self.cache_store.mget(keys).await?;

        let missed_keys = entries
            .iter()
            .filter_map(|entry| {
                if keys.contains(&entry.key) && !entry.is_outdated() {
                    None
                } else {
                    Some(entry.key.clone())
                }
            })
            .collect::<Vec<_>>();

        let Loader::<K, V>::MultiLoader(ref mloader) = self.loader else {
            unreachable!();
        };

        let sfg_key = {
            let mut a = missed_keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>();
            a.sort();
            a.join(",")
        };

        let (kvs, err, _owner) = self
            .mfg
            .work(&sfg_key, (|| async { (mloader)(&missed_keys).await })())
            .await;

        if err.is_some() {
            anyhow::bail!("single flight error");
        }

        let kvs = kvs.unwrap();

        let missed_key_entries = kvs
            .into_iter()
            .map(|kv| {
                (
                    kv.0.clone(),
                    Entry {
                        key: kv.0.clone(),
                        value: Some(kv.1.clone()),
                        expire_at_ms: (Utc::now() + Duration::from_std(self.expire_time).unwrap())
                            .timestamp_millis(),
                    },
                )
            })
            .collect::<Vec<_>>();
        self.cache_store.mset(&missed_key_entries).await?;

        let mut missed_entries = missed_key_entries
            .into_iter()
            .map(|(_, e)| e)
            .collect::<Vec<_>>();

        entries.append(&mut missed_entries);

        Ok(entries
            .into_iter()
            .map(|entry| (entry.key, entry.value.unwrap()))
            .collect())
    }

    pub async fn mget(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        match self.loader {
            Loader::SingleLoader(_) => self.mget_by_sloader(keys).await,
            Loader::MultiLoader(_) => self.mget_by_mloader(keys).await,
        }
    }

    pub async fn mset(&mut self, kvs: &[(K, V)]) -> Result<()> {
        let kvs = kvs
            .iter()
            .map(|kv| {
                (
                    kv.0.clone(),
                    Entry {
                        key: kv.0.clone(),
                        value: Some(kv.1.clone()),
                        expire_at_ms: (Utc::now() + Duration::from_std(self.expire_time).unwrap())
                            .timestamp_millis(),
                    },
                )
            })
            .collect::<Vec<_>>();

        self.cache_store.mset(&kvs).await?;

        Ok(())
    }

    pub async fn mdel(&mut self, keys: &[K]) -> Result<()> {
        self.cache_store.mdel(keys).await
    }
}
