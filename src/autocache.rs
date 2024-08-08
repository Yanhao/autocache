use std::{fmt::Debug, sync::Arc};

use anyhow::{bail, Result};
use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use futures::future::BoxFuture;
use tracing::{debug, error};

use crate::{
    builder::AutoCacheBuilder,
    cache::Cache,
    entry::{Entry, EntryTrait},
    error::AutoCacheError,
    loader::Loader,
    Options,
};

pub struct AutoCache<K, V, C, E>
where
    K: Clone,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache_store: Arc<C>,
    pub(crate) loader: Arc<Loader<K, V, E>>,

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
    pub(crate) manually_refresh: bool,

    pub(crate) async_refresh_channel:
        ArcSwapOption<tokio::sync::mpsc::Sender<AsyncSourceTask<K, E>>>,
    pub(crate) stop_ch: Option<tokio::sync::mpsc::Sender<()>>,

    pub(crate) on_metrics:
        Option<fn(method: &str, is_error: bool, ns: &str, from: &str, cache_name: &str)>,
}

impl<K, V, C, E> AutoCache<K, V, C, E>
where
    K: Clone + Debug + PartialEq + AsRef<str> + Sync + Send + 'static,
    V: Clone + Debug + Sync + Send + 'static,
    C: Cache<Key = K, Value = Entry<K, V>> + Sync + Send + 'static,
    E: Clone + Debug + Sync + Send + 'static,
{
    pub fn builder() -> AutoCacheBuilder<K, V, C, E> {
        AutoCacheBuilder::new()
    }

    pub(crate) fn start(&mut self) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.stop_ch.replace(tx);

        let (input_tx, mut input_rx) = tokio::sync::mpsc::channel(512);
        self.async_refresh_channel.store(Some(Arc::new(input_tx)));

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
                                    t.keys,
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

    async fn source_by_sloader(
        keys: &[(K, E)],
        loader: Arc<Loader<K, V, E>>,
        sfg: Arc<async_singleflight::Group<Option<V>, anyhow::Error>>,
        cache: Arc<C>,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        async_set_cache: bool,
    ) -> Result<Vec<Entry<K, V>>> {
        let Loader::<K, V, E>::SingleLoader(ref sloader) = *loader else {
            unreachable!();
        };

        let mut ret = Vec::with_capacity(keys.len());

        for (key, extra) in keys.iter() {
            let (value, err, _owner) = sfg
                .work(
                    key.as_ref(),
                    (|| async { (sloader)(key.clone(), extra.clone()).await })(),
                )
                .await;

            if err.is_some() {
                error!(msg = "autocache: single source failed", error = ?err, key = ?key);
                anyhow::bail!(AutoCacheError::SingleFlight);
            }
            if value.is_none() {
                error!(msg = "autocache: single source failed (not sf leader)", key = ?key);
                anyhow::bail!(AutoCacheError::SingleFlight);
            }
            let value = value.unwrap();

            if value.is_none() {
                debug!(msg = "autocache: source value is none", key = ?key);

                if !cache_none {
                    continue;
                }
            }
            debug!(msg = "autocache: source value from single loader", key = ?key);

            let expire_time = if value.is_none() {
                none_value_expire_time
            } else {
                expire_time
            };
            let entry = Entry {
                key: key.clone(),
                value,
                expire_at_ms: Some(
                    (Utc::now() + chrono::Duration::from_std(expire_time).unwrap())
                        .timestamp_millis(),
                ),
            };

            ret.push(entry.clone());

            if async_set_cache {
                let key = key.clone();
                let entry = entry.clone();
                let cache = cache.clone();
                tokio::spawn(async move {
                    debug!(msg = "autocache: async set cache", key = ?key);
                    let _ = cache
                        .mset(&[(key.clone(), entry)])
                        .await
                        .inspect_err(|e| error!("mset cache failed, error: {e}"));
                });
            } else {
                debug!(msg = "autocache: sync set cache", key = ?key);
                cache.mset(&[(key.clone(), entry)]).await?;
            }
        }

        Ok(ret)
    }

    async fn source_by_mloader(
        keys: Vec<(K, E)>,
        loader: Arc<Loader<K, V, E>>,
        mfg: Arc<async_singleflight::Group<Vec<(K, V)>, anyhow::Error>>,
        cache: Arc<C>,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        async_set_cache: bool,
    ) -> Result<Vec<Entry<K, V>>> {
        let Loader::<K, V, E>::MultiLoader(ref mloader) = *loader else {
            unreachable!();
        };

        let sfg_key = {
            let mut a = keys.iter().map(|k| k.0.as_ref()).collect::<Vec<_>>();
            a.sort();
            a.join(",")
        };

        let (kvs, err, _owner) = mfg
            .work(&sfg_key, (|| async { (mloader)(keys.clone()).await })())
            .await;

        if err.is_some() {
            anyhow::bail!(AutoCacheError::SingleFlight);
        }
        if kvs.is_none() {
            anyhow::bail!(AutoCacheError::SingleFlight);
        }

        let kvs = kvs.unwrap();

        let key_entries = keys
            .iter()
            .filter_map(|k| {
                for kv in kvs.iter() {
                    if &kv.0 == &k.0 {
                        return Some((
                            kv.0.clone(),
                            Entry {
                                key: kv.0.clone(),
                                value: Some(kv.1.clone()),
                                expire_at_ms: Some(
                                    (Utc::now() + chrono::Duration::from_std(expire_time).unwrap())
                                        .timestamp_millis(),
                                ),
                            },
                        ));
                    }
                }
                cache_none.then(|| {
                    (
                        k.0.clone(),
                        Entry {
                            key: k.0.clone(),
                            value: None,
                            expire_at_ms: Some(
                                (Utc::now()
                                    + chrono::Duration::from_std(none_value_expire_time).unwrap())
                                .timestamp_millis(),
                            ),
                        },
                    )
                })
            })
            .collect::<Vec<_>>();

        if !key_entries.is_empty() {
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
        }

        Ok(key_entries.into_iter().map(|(_, e)| e).collect::<Vec<_>>())
    }

    async fn filter_sync_source_keys(
        &self,
        keys: &[(K, E)],
        entries: &[Entry<K, V>],
        use_expired_data: bool,
    ) -> Vec<(K, E)> {
        let sync_source_keys = keys
            .iter()
            .filter_map(|key| {
                for ent in entries.iter() {
                    if &ent.key == &key.0 {
                        if !ent.is_expired() {
                            return None;
                        }

                        if use_expired_data || self.manually_refresh {
                            return None;
                        }
                    }
                }

                Some(key.clone())
            })
            .collect::<Vec<_>>();
        debug!(msg = "autocache: sync_source_keys", keys = ?sync_source_keys);

        sync_source_keys
    }

    async fn check_and_async_source(
        &self,
        entries: &[Entry<K, V>],
        keys: &[(K, E)],
        use_expired_data: bool,
    ) {
        if use_expired_data && !self.manually_refresh {
            let expired_keys = entries
                .iter()
                .filter_map(|e| e.is_expired().then(|| e.key.clone()))
                .collect::<Vec<_>>();

            let _ = self
                .refresh(
                    &keys
                        .iter()
                        .filter_map(|k| {
                            if expired_keys.contains(&k.0) {
                                Some(k.clone())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .await;
        }
    }

    async fn filter_unexpired_entry(
        &self,
        sync_source_keys: &[(K, E)],
        entries: Vec<Entry<K, V>>,
        use_expired_data: bool,
    ) -> Vec<Entry<K, V>> {
        entries
            .into_iter()
            .filter_map(|ent| {
                if ent.is_expired() && !use_expired_data {
                    return None;
                }

                for key in sync_source_keys.iter() {
                    if &ent.key == &key.0 {
                        return None;
                    }
                }

                Some(ent)
            })
            .collect::<Vec<_>>()
    }

    pub async fn mget(&self, keys: &[(K, E)]) -> Result<Vec<(K, V)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        self.mget_with_option(keys, Options::default()).await
    }

    pub async fn mget_with_option(&self, keys: &[(K, E)], options: Options) -> Result<Vec<(K, V)>> {
        if options.source_first == Some(true) {
            return self.mget_with_source_first(keys).await;
        }
        if self.source_first && options.source_first != Some(false) {
            return self.mget_with_source_first(keys).await;
        }

        let mut from = "-";
        let entries = self
            .cache_store
            .mget(
                &keys
                    .into_iter()
                    .map(|key| key.0.clone())
                    .collect::<Vec<_>>(),
            )
            .await?;
        debug!(msg = "autocache: mget from cache before filter", keys = ?keys, ret = ?{
            entries.iter().map(|e| e.key.clone()).collect::<Vec<_>>()
        });

        let sync_source_keys = self
            .filter_sync_source_keys(
                keys,
                &entries,
                match options.use_expired_data {
                    Some(u) => u,
                    None => self.use_expired_data,
                },
            )
            .await;
        self.check_and_async_source(
            &entries,
            keys,
            match options.use_expired_data {
                Some(u) => u,
                None => self.use_expired_data,
            },
        )
        .await;

        let mut entries = self
            .filter_unexpired_entry(
                &sync_source_keys,
                entries,
                match options.use_expired_data {
                    Some(u) => u,
                    None => self.use_expired_data,
                },
            )
            .await;
        if !entries.is_empty() {
            from = "cache";
        }

        debug!(msg = "autocache: mget from cache", keys = ?keys, ret = ?{
            entries.iter().map(|e| e.key.clone()).collect::<Vec<_>>()
        });

        if !sync_source_keys.is_empty() {
            let mut missed_entries = match *self.loader {
                Loader::SingleLoader(_) => Self::source_by_sloader(
                    &sync_source_keys,
                    self.loader.clone(),
                    self.sfg.clone(),
                    self.cache_store.clone(),
                    match options.cache_none {
                        Some(b) => b,
                        None => self.cache_none,
                    },
                    match options.expire_time {
                        Some(e) => e,
                        None => self.expire_time,
                    },
                    match options.none_value_expire_time {
                        Some(e) => e,
                        None => self.none_value_expire_time,
                    },
                    match options.async_set_cache {
                        Some(a) => a,
                        None => self.async_set_cache,
                    },
                )
                .await
                .inspect_err(|_| {
                    if let Some(metrics) = self.on_metrics {
                        metrics(
                            "mget",
                            true,
                            self.namespace.as_ref().unwrap_or(&"".to_string()),
                            "source",
                            self.cache_store.name(),
                        );
                    }
                })?,
                Loader::MultiLoader(_) => {
                    let missed_key_vector = sync_source_keys
                        .chunks(self.max_batch_size)
                        .collect::<Vec<_>>();

                    let mut entries: Vec<Entry<K, V>> = Vec::with_capacity(keys.len());
                    for keys in missed_key_vector.into_iter() {
                        entries.append(
                            &mut Self::source_by_mloader(
                                keys.to_vec(),
                                self.loader.clone(),
                                self.mfg.clone(),
                                self.cache_store.clone(),
                                match options.cache_none {
                                    Some(b) => b,
                                    None => self.cache_none,
                                },
                                match options.expire_time {
                                    Some(e) => e,
                                    None => self.expire_time,
                                },
                                match options.none_value_expire_time {
                                    Some(e) => e,
                                    None => self.none_value_expire_time,
                                },
                                match options.async_set_cache {
                                    Some(a) => a,
                                    None => self.async_set_cache,
                                },
                            )
                            .await?,
                        );
                    }

                    entries
                }
            };
            if !missed_entries.is_empty() {
                if from == "cache" {
                    from = "both";
                } else {
                    from = "source";
                }
            }

            entries.append(&mut missed_entries);
        }

        if let Some(metrics) = self.on_metrics {
            metrics(
                "mget",
                false,
                self.namespace.as_ref().unwrap_or(&"".to_string()),
                from,
                self.cache_store.name(),
            );
        }
        Ok(entries
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect())
    }
    async fn mget_with_source_first(&self, keys: &[(K, E)]) -> Result<Vec<(K, V)>> {
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
                            keys.to_vec(),
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
                    if &e.key == &k.0 {
                        return None;
                    }
                }
                Some(k.0.clone())
            })
            .collect::<Vec<_>>();

        if !missed_keys.is_empty() {
            let mut missed_entries = self.cache_store.mget(&missed_keys).await?;
            entries.append(&mut missed_entries);
        }

        Ok(entries
            .into_iter()
            .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
            .collect())
    }

    pub async fn mset(&self, kvs: &[(K, V)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        let kvs = kvs
            .iter()
            .map(|kv| {
                (
                    kv.0.clone(),
                    Entry {
                        key: kv.0.clone(),
                        value: Some(kv.1.clone()),
                        expire_at_ms: Some(
                            (Utc::now() + chrono::Duration::from_std(self.expire_time).unwrap())
                                .timestamp_millis(),
                        ),
                    },
                )
            })
            .collect::<Vec<_>>();

        self.cache_store.mset(&kvs).await?;

        Ok(())
    }

    pub async fn mdel(&self, keys: &[K]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.cache_store.mdel(keys).await
    }

    pub async fn refresh(&self, keys: &[(K, E)]) -> Result<()> {
        if self.async_refresh_channel.load().is_none() {
            bail!(AutoCacheError::Unsupported);
        }

        let keys_vector = keys.chunks(self.max_batch_size).collect::<Vec<_>>();
        for keys in keys_vector.into_iter() {
            let _ = self
                .async_refresh_channel
                .load()
                .as_ref()
                .unwrap()
                .send(AsyncSourceTask {
                    _crate_time: Utc::now(),
                    keys: keys.to_vec(),
                })
                .await
                .inspect_err(|e| error!("autocache: send async source task failed!, error: {e}"));
        }

        Ok(())
    }

    pub async fn with_cache<T>(
        &self,
        op: impl FnOnce(Arc<C>) -> BoxFuture<'static, Result<T>> + Send,
    ) -> Result<T> {
        op(self.cache_store.clone()).await
    }
}

impl<K, V, C, E> Drop for AutoCache<K, V, C, E>
where
    K: Clone,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl<K, V, C, E> AutoCache<K, V, C, E>
where
    K: Clone,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn stop(&self) -> Result<()> {
        if let Some(s) = self.stop_ch.as_ref() {
            s.try_send(())?;
        }

        Ok(())
    }
}
pub(crate) struct AsyncSourceTask<K, E> {
    _crate_time: chrono::DateTime<Utc>,
    keys: Vec<(K, E)>,
}
