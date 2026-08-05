use std::{collections::HashSet, fmt::Debug, hash::Hash, sync::Arc};

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
    singleflight::Group,
    Options,
};

pub struct AutoCache<K, V, C, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache_store: Arc<C>,
    pub(crate) loader: Arc<Loader<K, V, E>>,

    pub(crate) sfg: Arc<Group<K, Option<Entry<K, V>>>>,
    pub(crate) mfg: Arc<Group<BatchKey<K>, Vec<Entry<K, V>>>>,

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
    pub(crate) pending_refresh_keys: Arc<parking_lot::Mutex<HashSet<K>>>,
    pub(crate) stop_ch: Option<tokio::sync::mpsc::Sender<()>>,

    pub(crate) on_metrics:
        Option<fn(method: &str, is_error: bool, ns: &str, from: &str, cache_name: &str)>,
}

impl<K, V, C, E> AutoCache<K, V, C, E>
where
    K: Clone + Debug + Eq + Hash + Sync + Send + 'static,
    V: Clone + Debug + Sync + Send + 'static,
    C: Cache<Key = K, Value = Entry<K, V>> + Sync + Send + 'static,
    E: Clone + Debug + Sync + Send + 'static,
{
    pub fn builder() -> AutoCacheBuilder<K, V, C, E> {
        AutoCacheBuilder::new()
    }

    pub(crate) fn start(&mut self) -> Result<()> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| AutoCacheError::RuntimeUnavailable)?;

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
        let pending_refresh_keys = self.pending_refresh_keys.clone();

        runtime.spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.recv() => {
                        break;
                    }
                    t = input_rx.recv() => {
                        let Some(t) = t else {
                            continue;
                        };
                        let task_keys = t.keys.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();

                        let result = match *loader {
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
                        Self::remove_pending_refresh_keys(&pending_refresh_keys, &task_keys);
                        let _ = result;
                    }
                }
            }
        });

        Ok(())
    }

    fn remove_pending_refresh_keys(
        pending_refresh_keys: &parking_lot::Mutex<HashSet<K>>,
        keys: &[K],
    ) {
        let mut pending_refresh_keys = pending_refresh_keys.lock();
        for key in keys {
            pending_refresh_keys.remove(key);
        }
    }

    async fn source_by_sloader(
        keys: &[(K, E)],
        loader: Arc<Loader<K, V, E>>,
        sfg: Arc<Group<K, Option<Entry<K, V>>>>,
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
            let loader_key = key.clone();
            let loader_extra = extra.clone();
            let loader_cache = cache.clone();
            let entry = sfg
                .work(key.clone(), async move {
                    let value = (sloader)(loader_key.clone(), loader_extra).await?;

                    if value.is_none() {
                        debug!(msg = "autocache: source value is none", key = ?loader_key);

                        if !cache_none {
                            return Ok(None);
                        }
                    }
                    debug!(msg = "autocache: source value from single loader", key = ?loader_key);

                    let entry_expire_time = if value.is_none() {
                        none_value_expire_time
                    } else {
                        expire_time
                    };
                    let entry = Entry {
                        key: loader_key.clone(),
                        value,
                        expire_at_ms: Some(
                            (Utc::now() + chrono::Duration::from_std(entry_expire_time).unwrap())
                                .timestamp_millis(),
                        ),
                    };

                    if async_set_cache {
                        let key = loader_key.clone();
                        let cache_entry = entry.clone();
                        tokio::spawn(async move {
                            debug!(msg = "autocache: async set cache", key = ?key);
                            let _ = loader_cache
                                .mset(&[(key.clone(), cache_entry)])
                                .await
                                .inspect_err(|e| error!("mset cache failed, error: {e}"));
                        });
                    } else {
                        debug!(msg = "autocache: sync set cache", key = ?loader_key);
                        let _ = loader_cache
                            .mset(&[(loader_key.clone(), entry.clone())])
                            .await
                            .inspect_err(|e| error!("mset cache failed, error: {e}"));
                    }

                    Ok(Some(entry))
                })
                .await?;

            if let Some(entry) = entry {
                ret.push(entry);
            }
        }

        Ok(ret)
    }

    async fn source_by_mloader(
        keys: Vec<(K, E)>,
        loader: Arc<Loader<K, V, E>>,
        mfg: Arc<Group<BatchKey<K>, Vec<Entry<K, V>>>>,
        cache: Arc<C>,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        async_set_cache: bool,
    ) -> Result<Vec<Entry<K, V>>> {
        let Loader::<K, V, E>::MultiLoader(ref mloader) = *loader else {
            unreachable!();
        };

        let batch_key = BatchKey(keys.iter().map(|(key, _)| key.clone()).collect());
        let loader_keys = keys.clone();

        mfg.work(batch_key, async move {
            let kvs = (mloader)(loader_keys.clone()).await?;
            let key_entries = loader_keys
                .iter()
                .filter_map(|k| {
                    for kv in kvs.iter() {
                        if kv.0 == k.0 {
                            return Some((
                                kv.0.clone(),
                                Entry {
                                    key: kv.0.clone(),
                                    value: Some(kv.1.clone()),
                                    expire_at_ms: Some(
                                        (Utc::now()
                                            + chrono::Duration::from_std(expire_time).unwrap())
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
                                        + chrono::Duration::from_std(none_value_expire_time)
                                            .unwrap())
                                    .timestamp_millis(),
                                ),
                            },
                        )
                    })
                })
                .collect::<Vec<_>>();

            if !key_entries.is_empty() {
                if async_set_cache {
                    let cache_entries = key_entries.clone();
                    tokio::spawn(async move {
                        let _ = cache
                            .mset(&cache_entries)
                            .await
                            .inspect_err(|e| error!("mset cache failed, error: {e}"));
                    });
                } else {
                    let _ = cache
                        .mset(&key_entries)
                        .await
                        .inspect_err(|e| error!("mset cache failed, error: {e}"));
                }
            }

            Ok(key_entries.into_iter().map(|(_, entry)| entry).collect())
        })
        .await
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
    ) -> Result<()> {
        if use_expired_data && !self.manually_refresh {
            let expired_keys = entries
                .iter()
                .filter_map(|e| e.is_expired().then(|| e.key.clone()))
                .collect::<Vec<_>>();

            self.refresh(
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
            .await?;
        }

        Ok(())
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
            return self.mget_with_source_first(keys, &options).await;
        }
        if self.source_first && options.source_first != Some(false) {
            return self.mget_with_source_first(keys, &options).await;
        }

        let requested_use_expired_data = options.use_expired_data.unwrap_or(self.use_expired_data);
        let refresh_worker_available = self
            .async_refresh_channel
            .load()
            .as_ref()
            .is_some_and(|sender| !sender.is_closed());
        let use_expired_data = requested_use_expired_data && refresh_worker_available;

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
            .filter_sync_source_keys(keys, &entries, use_expired_data)
            .await;
        self.check_and_async_source(&entries, keys, use_expired_data)
            .await?;

        let mut entries = self
            .filter_unexpired_entry(&sync_source_keys, entries, use_expired_data)
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
    async fn mget_with_source_first(
        &self,
        keys: &[(K, E)],
        options: &Options,
    ) -> Result<Vec<(K, V)>> {
        let cache_none = options.cache_none.unwrap_or(self.cache_none);
        let expire_time = options.expire_time.unwrap_or(self.expire_time);
        let none_value_expire_time = options
            .none_value_expire_time
            .unwrap_or(self.none_value_expire_time);
        let async_set_cache = options.async_set_cache.unwrap_or(self.async_set_cache);

        let entries = match *self.loader {
            Loader::SingleLoader(_) => {
                Self::source_by_sloader(
                    keys,
                    self.loader.clone(),
                    self.sfg.clone(),
                    self.cache_store.clone(),
                    cache_none,
                    expire_time,
                    none_value_expire_time,
                    async_set_cache,
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
                            cache_none,
                            expire_time,
                            none_value_expire_time,
                            async_set_cache,
                        )
                        .await?,
                    );
                }

                entries
            }
        };

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
        let Some(sender) = self.async_refresh_channel.load_full() else {
            bail!(AutoCacheError::Unsupported);
        };

        for keys in keys.chunks(self.max_batch_size) {
            let all_keys_pending = {
                let pending_refresh_keys = self.pending_refresh_keys.lock();
                keys.iter()
                    .all(|(key, _)| pending_refresh_keys.contains(key))
            };
            if all_keys_pending {
                continue;
            }

            let permit = sender.reserve().await.map_err(|e| {
                error!("autocache: reserve async source task failed!, error: {e}");
                anyhow::anyhow!("reserve async source task failed: {e}")
            })?;
            let task_keys = {
                let mut pending_refresh_keys = self.pending_refresh_keys.lock();
                keys.iter()
                    .filter_map(|(key, extra)| {
                        if pending_refresh_keys.insert(key.clone()) {
                            Some((key.clone(), extra.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            };
            if task_keys.is_empty() {
                continue;
            }

            permit.send(AsyncSourceTask {
                _crate_time: Utc::now(),
                keys: task_keys,
            });
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
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

impl<K, V, C, E> AutoCache<K, V, C, E>
where
    K: Clone + Eq + Hash,
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BatchKey<K>(Vec<K>);

pub(crate) struct AsyncSourceTask<K, E> {
    _crate_time: chrono::DateTime<Utc>,
    keys: Vec<(K, E)>,
}
