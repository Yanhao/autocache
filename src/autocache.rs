use std::{fmt::Debug, sync::Arc};

use anyhow::Result;
use arc_swap::ArcSwapOption;
use chrono::{prelude::*, Duration};
use tracing::error;

use crate::{
    builder::AutoCacheBuilder,
    cache::Cache,
    codec::Codec,
    entry::{Entry, EntryTrait},
    loader::Loader,
};

pub struct AutoCache<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec,
    V: Clone + Codec,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache_store: Arc<C>,
    pub(crate) loader: Arc<Loader<K, V>>,

    pub(crate) sfg: Arc<async_singleflight::Group<Option<V>, anyhow::Error>>,
    pub(crate) mfg: Arc<async_singleflight::Group<Vec<(K, V)>, anyhow::Error>>,

    pub(crate) namespace: Option<String>,
    pub(crate) expire_time: std::time::Duration,
    pub(crate) cache_none: bool,
    pub(crate) none_value_expire_time: std::time::Duration,
    pub(crate) source_first: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) async_set_cache: bool,
    pub(crate) use_expired_data: bool, // means async source

    pub(crate) input: ArcSwapOption<tokio::sync::mpsc::Sender<AsyncSourceTask<K>>>,
    pub(crate) stop_ch: Option<tokio::sync::mpsc::Sender<()>>,
}

impl<K, V, C> AutoCache<K, V, C>
where
    K: Clone + std::cmp::PartialEq + AsRef<str> + Codec + Debug + Send + 'static + Sync,
    V: Clone + Codec + Debug + Send + Sync + 'static,
    C: Cache<Key = K, Value = Entry<K, V>> + Send + Sync + 'static,
{
    pub fn builder() -> AutoCacheBuilder<K, V, C> {
        AutoCacheBuilder::new()
    }

    pub(crate) fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.stop_ch.replace(tx);

        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(512);
        self.input.store(Some(Arc::new(input_tx)));

        let loader = self.loader.clone();
        let cache = self.cache_store.clone();
        let cache_none = self.cache_none;
        let expire_time = self.expire_time;
        let none_value_expire_time = self.none_value_expire_time;
        let sfg = self.sfg.clone();
        let mfg = self.mfg.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    t = input_rx.recv() => {
                        let Some(t) = t else {
                            continue;
                        };

                        let _ = match *loader {
                            Loader::SingleLoader(_) => {
                                Self::source_by_sloader(
                                    &t.keys,
                                    loader.clone(),
                                    sfg.clone(),
                                    cache.clone(),
                                    cache_none,
                                    expire_time,
                                    none_value_expire_time,
                                    false,
                                )
                                .await.inspect_err(|e| error!("async source by sloader failed, error {e}"))
                            }
                            Loader::MultiLoader(_) => {
                                Self::source_by_mloader(
                                    &t.keys,
                                    loader.clone(),
                                    mfg.clone(),
                                    cache.clone(),
                                    cache_none,
                                    expire_time,
                                    none_value_expire_time,
                                    false,
                                )
                                .await.inspect_err(|e| error!("async source by mloader failed, error {e}"))
                            }
                        };
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.send(()).await?;
        }

        Ok(())
    }

    async fn filter_missed_key_and_unexpired_entry(
        &self,
        keys: &[K],
        entries: Vec<Entry<K, V>>,
    ) -> (Vec<K>, Vec<Entry<K, V>>) {
        let missed_keys = keys
            .iter()
            .filter_map(|key| {
                for ent in entries.iter() {
                    if &ent.key == key && !ent.is_outdated() && !self.use_expired_data {
                        return None;
                    }
                }

                Some(key.clone())
            })
            .collect::<Vec<_>>();

        if self.use_expired_data {
            let expired_keys = entries
                .iter()
                .filter_map(|e| e.is_outdated().then(|| e.key.clone()))
                .collect::<Vec<_>>();

            let keys_vector = expired_keys.chunks(self.max_batch_size).collect::<Vec<_>>();

            for keys in keys_vector.into_iter() {
                if let Some(input) = self.input.load().as_ref() {
                    let _ = input
                        .send(AsyncSourceTask {
                            _crate_time: Utc::now(),
                            keys: keys.to_vec(),
                        })
                        .await
                        .inspect_err(|e| error!("send async source task failed!, error: {e}"));
                }
            }
        }

        let entries = entries
            .into_iter()
            .filter_map(|x| {
                for key in missed_keys.iter() {
                    if &x.key == key {
                        return None;
                    }
                }
                Some(x)
            })
            .collect::<Vec<_>>();

        (missed_keys, entries)
    }

    async fn source_by_sloader(
        keys: &[K],
        loader: Arc<Loader<K, V>>,
        sfg: Arc<async_singleflight::Group<Option<V>, anyhow::Error>>,
        cache: Arc<C>,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        async_set_cache: bool,
    ) -> Result<Vec<Entry<K, V>>> {
        let Loader::<K, V>::SingleLoader(ref sloader) = *loader else {
            unreachable!();
        };

        let mut ret = Vec::with_capacity(keys.len());

        for key in keys.iter() {
            let (value, err, _owner) = sfg
                .work(key.as_ref(), (|| async { (sloader)(key.clone()).await })())
                .await;

            if err.is_some() {
                anyhow::bail!("single flight error");
            }
            if value.is_none() {} // FIXME:
            let value = value.unwrap();

            if value.is_none() && !cache_none {
                continue;
            }

            let expire_time = if value.is_none() {
                none_value_expire_time
            } else {
                expire_time
            };
            let entry = Entry {
                key: key.clone(),
                value,
                expire_at_ms: (Utc::now() + Duration::from_std(expire_time).unwrap())
                    .timestamp_millis(),
            };

            ret.push(entry.clone());

            if async_set_cache {
                let key = key.clone();
                let entry = entry.clone();
                let cache = cache.clone();
                tokio::spawn(async move {
                    let _ = cache
                        .mset(&[(key.clone(), entry)])
                        .await
                        .inspect_err(|e| error!("mset cache failed, error: {e}"));
                });
            } else {
                cache.mset(&[(key.clone(), entry)]).await?;
            }
        }

        Ok(ret)
    }

    async fn source_by_mloader(
        keys: &[K],
        loader: Arc<Loader<K, V>>,
        mfg: Arc<async_singleflight::Group<Vec<(K, V)>, anyhow::Error>>,
        cache: Arc<C>,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        async_set_cache: bool,
    ) -> Result<Vec<Entry<K, V>>> {
        let Loader::<K, V>::MultiLoader(ref mloader) = *loader else {
            unreachable!();
        };

        let sfg_key = {
            let mut a = keys.iter().map(|k| k.as_ref()).collect::<Vec<_>>();
            a.sort();
            a.join(",")
        };

        let (kvs, err, _owner) = mfg
            .work(&sfg_key, (|| async { (mloader)(&keys).await })())
            .await;

        if err.is_some() {
            anyhow::bail!("single flight error");
        }

        let kvs = kvs.unwrap();

        let key_entries = keys
            .iter()
            .filter_map(|k| {
                for kv in kvs.iter() {
                    if &kv.0 == k {
                        return Some((
                            kv.0.clone(),
                            Entry {
                                key: kv.0.clone(),
                                value: Some(kv.1.clone()),
                                expire_at_ms: (Utc::now()
                                    + Duration::from_std(expire_time).unwrap())
                                .timestamp_millis(),
                            },
                        ));
                    }
                }
                cache_none.then(|| {
                    (
                        k.clone(),
                        Entry {
                            key: k.clone(),
                            value: None,
                            expire_at_ms: (Utc::now()
                                + Duration::from_std(none_value_expire_time).unwrap())
                            .timestamp_millis(),
                        },
                    )
                })
            })
            .collect::<Vec<_>>();

        if async_set_cache {
            let key_entries = key_entries.clone();
            tokio::spawn(async move {
                let _ = cache
                    .mset(&key_entries)
                    .await
                    .inspect_err(|e| error!("mset cache failed, error: {e}"));
            });
        } else {
            cache.mset(&key_entries).await?;
        }

        Ok(key_entries.into_iter().map(|(_, e)| e).collect::<Vec<_>>())
    }

    pub async fn mget(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        if self.source_first {
            return self.mget_with_source_first(keys).await;
        }

        let entries = self.cache_store.mget(keys).await?;
        let (missed_keys, mut entries) = self
            .filter_missed_key_and_unexpired_entry(keys, entries)
            .await;

        let mut missed_entries = match *self.loader {
            Loader::SingleLoader(_) => {
                Self::source_by_sloader(
                    &missed_keys,
                    self.loader.clone(),
                    self.sfg.clone(),
                    self.cache_store.clone(),
                    self.cache_none,
                    self.expire_time,
                    self.none_value_expire_time,
                    self.async_set_cache,
                )
                .await?
            }
            Loader::MultiLoader(_) => {
                let missed_key_vector = missed_keys.chunks(self.max_batch_size).collect::<Vec<_>>();

                let mut entries: Vec<Entry<K, V>> = Vec::with_capacity(keys.len());
                for keys in missed_key_vector.into_iter() {
                    entries.append(
                        &mut Self::source_by_mloader(
                            keys,
                            self.loader.clone(),
                            self.mfg.clone(),
                            self.cache_store.clone(),
                            self.cache_none,
                            self.expire_time,
                            self.none_value_expire_time,
                            self.async_set_cache,
                        )
                        .await?,
                    );
                }

                entries
            }
        };

        entries.append(&mut missed_entries);

        Ok(entries
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect())
    }

    pub async fn mget_with_source_first(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        let mut entries = match *self.loader {
            Loader::SingleLoader(_) => {
                Self::source_by_sloader(
                    &keys,
                    self.loader.clone(),
                    self.sfg.clone(),
                    self.cache_store.clone(),
                    self.cache_none,
                    self.expire_time,
                    self.none_value_expire_time,
                    self.async_set_cache,
                )
                .await?
            }
            Loader::MultiLoader(_) => {
                let missed_key_vector = keys.chunks(self.max_batch_size).collect::<Vec<_>>();

                let mut entries: Vec<Entry<K, V>> = Vec::with_capacity(keys.len());
                for keys in missed_key_vector.into_iter() {
                    entries.append(
                        &mut Self::source_by_mloader(
                            keys,
                            self.loader.clone(),
                            self.mfg.clone(),
                            self.cache_store.clone(),
                            self.cache_none,
                            self.expire_time,
                            self.none_value_expire_time,
                            self.async_set_cache,
                        )
                        .await?,
                    );
                }

                entries
            }
        };

        let missed_keys = keys
            .iter()
            .filter_map(|k| {
                for e in entries.iter() {
                    if &e.key == k {
                        return None;
                    }
                }
                return Some(k.clone());
            })
            .collect::<Vec<_>>();

        let mut missed_entries = self.cache_store.mget(&missed_keys).await?;

        entries.append(&mut missed_entries);

        Ok(entries
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect())
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

pub(crate) struct AsyncSourceTask<T> {
    _crate_time: chrono::DateTime<Utc>,
    keys: Vec<T>,
}
