use std::{collections::HashSet, fmt::Debug, hash::Hash, sync::Arc};

use arc_swap::ArcSwapOption;
use chrono::prelude::*;
use futures::future::BoxFuture;
use tracing::{debug, error, warn};

use crate::{
    builder::AutoCacheBuilder,
    cache::Cache,
    entry::{Entry, EntryTrait},
    error::ConfigurationError,
    loader::Loader,
    singleflight::Group,
    CacheOperation, Error, LoaderKind, Options, Result,
};

pub(crate) type MetricsCallback =
    fn(method: &str, is_error: bool, ns: &str, from: &str, cache_name: &str);
type SingleFlight<K, V> = Arc<Group<K, Option<Entry<K, V>>>>;
type MultiFlight<K, V> = Arc<Group<BatchKey<K>, Vec<Entry<K, V>>>>;

enum RefreshEnqueueMode {
    WaitForCapacity,
    BestEffort,
}

#[derive(Clone)]
struct MetricsContext {
    callback: MetricsCallback,
    namespace: String,
}

impl MetricsContext {
    fn record(&self, method: &str, is_error: bool, from: &str, cache_name: &str) {
        (self.callback)(method, is_error, &self.namespace, from, cache_name);
    }
}

#[derive(Clone)]
struct CacheFillConfig {
    asynchronous: bool,
    permits: Arc<tokio::sync::Semaphore>,
    metrics: Option<MetricsContext>,
}

struct SourceLoadContext<K, V, C, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    loader: Arc<Loader<K, V, E>>,
    cache: Arc<C>,
    cache_none: bool,
    expire_time: std::time::Duration,
    none_value_expire_time: std::time::Duration,
    cache_fill_config: CacheFillConfig,
}

impl<K, V, C, E> Clone for SourceLoadContext<K, V, C, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn clone(&self) -> Self {
        Self {
            loader: self.loader.clone(),
            cache: self.cache.clone(),
            cache_none: self.cache_none,
            expire_time: self.expire_time,
            none_value_expire_time: self.none_value_expire_time,
            cache_fill_config: self.cache_fill_config.clone(),
        }
    }
}

pub struct AutoCache<K, V, C, E = ()>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    pub(crate) cache_store: Arc<C>,
    pub(crate) loader: Arc<Loader<K, V, E>>,

    pub(crate) sfg: SingleFlight<K, V>,
    pub(crate) mfg: MultiFlight<K, V>,

    pub(crate) namespace: Option<String>,
    pub(crate) expire_time: std::time::Duration,
    pub(crate) cache_none: bool,
    pub(crate) none_value_expire_time: std::time::Duration,
    pub(crate) source_first: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) async_set_cache: bool,
    pub(crate) async_cache_write_permits: Arc<tokio::sync::Semaphore>,
    pub(crate) async_refresh_queue_capacity: usize,
    pub(crate) use_expired_data: bool, // means async source
    pub(crate) manually_refresh: bool,

    pub(crate) async_refresh_channel:
        ArcSwapOption<tokio::sync::mpsc::Sender<AsyncSourceTask<K, E>>>,
    pub(crate) pending_refresh_keys: Arc<parking_lot::Mutex<HashSet<K>>>,
    pub(crate) stop_ch: Option<tokio::sync::mpsc::Sender<()>>,

    pub(crate) on_metrics: Option<MetricsCallback>,
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
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| Error::RuntimeUnavailable)?;

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        self.stop_ch.replace(tx);

        let (input_tx, mut input_rx) =
            tokio::sync::mpsc::channel(self.async_refresh_queue_capacity);
        self.async_refresh_channel.store(Some(Arc::new(input_tx)));

        let loader = self.loader.clone();
        let cache = self.cache_store.clone();
        let cache_none = self.cache_none;
        let expire_time = self.expire_time;
        let none_value_expire_time = self.none_value_expire_time;
        let sfg = self.sfg.clone();
        let mfg = self.mfg.clone();
        let pending_refresh_keys = self.pending_refresh_keys.clone();
        let cache_fill_config = self.cache_fill_config(false);
        let refresh_metrics = cache_fill_config.metrics.clone();
        let source_load_context = SourceLoadContext {
            loader: loader.clone(),
            cache: cache.clone(),
            cache_none,
            expire_time,
            none_value_expire_time,
            cache_fill_config,
        };

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
                                    sfg.clone(),
                                    source_load_context.clone(),
                                )
                                .await
                            }
                            Loader::MultiLoader(_) => {
                                Self::source_by_mloader(
                                    t.keys,
                                    mfg.clone(),
                                    source_load_context.clone(),
                                )
                                .await
                            }
                        };
                        if let Err(error) = result {
                            error!(%error, "autocache: async source refresh failed");
                            if let Some(metrics) = refresh_metrics.as_ref() {
                                metrics.record("refresh", true, "source", cache.name());
                            }
                        }
                        Self::remove_pending_refresh_keys(&pending_refresh_keys, &task_keys);
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

    fn expiration_timestamp(duration: std::time::Duration) -> Result<i64> {
        let duration = chrono::Duration::from_std(duration)
            .map_err(|_| ConfigurationError::InvalidExpirationDuration)?;
        Utc::now()
            .checked_add_signed(duration)
            .map(|expires_at| expires_at.timestamp_millis())
            .ok_or_else(|| ConfigurationError::InvalidExpirationDuration.into())
    }

    fn cache_fill_config(&self, asynchronous: bool) -> CacheFillConfig {
        CacheFillConfig {
            asynchronous,
            permits: self.async_cache_write_permits.clone(),
            metrics: self.on_metrics.map(|callback| MetricsContext {
                callback,
                namespace: self.namespace.clone().unwrap_or_default(),
            }),
        }
    }

    fn source_load_context(
        &self,
        cache_none: bool,
        expire_time: std::time::Duration,
        none_value_expire_time: std::time::Duration,
        cache_fill_config: CacheFillConfig,
    ) -> SourceLoadContext<K, V, C, E> {
        SourceLoadContext {
            loader: self.loader.clone(),
            cache: self.cache_store.clone(),
            cache_none,
            expire_time,
            none_value_expire_time,
            cache_fill_config,
        }
    }

    async fn write_cache_entries(
        cache: Arc<C>,
        entries: Vec<(K, Entry<K, V>)>,
        metrics: Option<MetricsContext>,
    ) {
        if let Err(error) = cache.mset(&entries).await {
            error!("mset cache failed, error: {error}");
            if let Some(metrics) = metrics {
                metrics.record("mset", true, "source", cache.name());
            }
        }
    }

    async fn set_cache_entries(
        cache: Arc<C>,
        entries: Vec<(K, Entry<K, V>)>,
        config: CacheFillConfig,
    ) {
        if config.asynchronous {
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => {
                    match config.permits.clone().try_acquire_owned() {
                        Ok(permit) => {
                            runtime.spawn(async move {
                                let _permit = permit;
                                Self::write_cache_entries(cache, entries, config.metrics).await;
                            });
                        }
                        Err(error) => {
                            warn!(
                                cache = cache.name(),
                                entry_count = entries.len(),
                                error = %error,
                                "autocache: async cache write limit reached; skipping cache fill"
                            );
                            if let Some(metrics) = config.metrics {
                                metrics.record("mset", true, "source", cache.name());
                            }
                        }
                    }
                    return;
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        "autocache: Tokio runtime unavailable; setting cache synchronously"
                    );
                }
            }
        }

        Self::write_cache_entries(cache, entries, config.metrics).await;
    }

    async fn invalidate_cache_keys(cache: &C, keys: &[K], metrics: Option<MetricsContext>) {
        if let Err(error) = cache.mdel(keys).await {
            error!(
                key_count = keys.len(),
                error = %error,
                "autocache: failed to invalidate cache after source returned not found"
            );
            if let Some(metrics) = metrics {
                metrics.record("mdel", true, "source", cache.name());
            }
        }
    }

    async fn source_by_sloader(
        keys: &[(K, E)],
        sfg: SingleFlight<K, V>,
        context: SourceLoadContext<K, V, C, E>,
    ) -> Result<Vec<Entry<K, V>>> {
        let SourceLoadContext {
            loader,
            cache,
            cache_none,
            expire_time,
            none_value_expire_time,
            cache_fill_config,
        } = context;
        let Loader::<K, V, E>::SingleLoader(ref sloader) = *loader else {
            unreachable!();
        };

        let mut ret = Vec::with_capacity(keys.len());

        for (key, extra) in keys.iter() {
            let loader_key = key.clone();
            let loader_extra = extra.clone();
            let loader_cache = cache.clone();
            let loader_cache_fill_config = cache_fill_config.clone();
            let entry = sfg
                .work(key.clone(), async move {
                    let value = (sloader)(loader_key.clone(), loader_extra)
                        .await
                        .map_err(|error| Error::loader(LoaderKind::Single, error))?;

                    if value.is_none() {
                        debug!(msg = "autocache: source value is none", key = ?loader_key);

                        if !cache_none {
                            Self::invalidate_cache_keys(
                                loader_cache.as_ref(),
                                std::slice::from_ref(&loader_key),
                                loader_cache_fill_config.metrics.clone(),
                            )
                            .await;
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
                        expire_at_ms: Some(Self::expiration_timestamp(entry_expire_time)?),
                    };

                    debug!(
                        msg = "autocache: set cache",
                        key = ?loader_key,
                        asynchronous = loader_cache_fill_config.asynchronous
                    );
                    Self::set_cache_entries(
                        loader_cache,
                        vec![(loader_key.clone(), entry.clone())],
                        loader_cache_fill_config,
                    )
                    .await;

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
        mfg: MultiFlight<K, V>,
        context: SourceLoadContext<K, V, C, E>,
    ) -> Result<Vec<Entry<K, V>>> {
        let SourceLoadContext {
            loader,
            cache,
            cache_none,
            expire_time,
            none_value_expire_time,
            cache_fill_config,
        } = context;
        let Loader::<K, V, E>::MultiLoader(ref mloader) = *loader else {
            unreachable!();
        };

        let batch_key = BatchKey(keys.iter().map(|(key, _)| key.clone()).collect());
        let loader_keys = keys.clone();

        mfg.work(batch_key, async move {
            let kvs = (mloader)(loader_keys.clone())
                .await
                .map_err(|error| Error::loader(LoaderKind::Batch, error))?;
            let mut key_entries = Vec::with_capacity(loader_keys.len());
            let mut missing_keys = Vec::new();

            for key in &loader_keys {
                if let Some((loaded_key, value)) = kvs.iter().find(|kv| kv.0 == key.0) {
                    key_entries.push((
                        loaded_key.clone(),
                        Entry {
                            key: loaded_key.clone(),
                            value: Some(value.clone()),
                            expire_at_ms: Some(Self::expiration_timestamp(expire_time)?),
                        },
                    ));
                } else if cache_none {
                    key_entries.push((
                        key.0.clone(),
                        Entry {
                            key: key.0.clone(),
                            value: None,
                            expire_at_ms: Some(Self::expiration_timestamp(none_value_expire_time)?),
                        },
                    ));
                } else {
                    missing_keys.push(key.0.clone());
                }
            }

            if !missing_keys.is_empty() {
                Self::invalidate_cache_keys(
                    cache.as_ref(),
                    &missing_keys,
                    cache_fill_config.metrics.clone(),
                )
                .await;
            }

            if !key_entries.is_empty() {
                Self::set_cache_entries(cache, key_entries.clone(), cache_fill_config).await;
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
                    if ent.key == key.0 {
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
                .filter(|e| e.is_expired())
                .map(|e| e.key.clone())
                .collect::<Vec<_>>();

            self.enqueue_refresh(
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
                RefreshEnqueueMode::BestEffort,
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
                    if ent.key == key.0 {
                        return None;
                    }
                }

                Some(ent)
            })
            .collect::<Vec<_>>()
    }

    pub async fn mget_with_context(&self, keys: &[(K, E)]) -> Result<Vec<(K, V)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        self.mget_with_context_and_option(keys, Options::default())
            .await
    }

    pub async fn mget_with_context_and_option(
        &self,
        keys: &[(K, E)],
        options: Options,
    ) -> Result<Vec<(K, V)>> {
        let source_first = options.source_first == Some(true)
            || (self.source_first && options.source_first != Some(false));
        if source_first {
            let result = self.mget_with_source_first(keys, &options).await;
            if let Some(metrics) = self.on_metrics {
                metrics(
                    "mget",
                    result.is_err(),
                    self.namespace.as_deref().unwrap_or(""),
                    "source",
                    self.cache_store.name(),
                );
            }
            return result;
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
            .mget(&keys.iter().map(|key| key.0.clone()).collect::<Vec<_>>())
            .await
            .map_err(|error| {
                Error::from_cache(CacheOperation::Read, self.cache_store.name(), error)
            })?;
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
            let cache_none = options.cache_none.unwrap_or(self.cache_none);
            let expire_time = options.expire_time.unwrap_or(self.expire_time);
            let none_value_expire_time = options
                .none_value_expire_time
                .unwrap_or(self.none_value_expire_time);
            let cache_fill_config =
                self.cache_fill_config(options.async_set_cache.unwrap_or(self.async_set_cache));
            let source_load_context = self.source_load_context(
                cache_none,
                expire_time,
                none_value_expire_time,
                cache_fill_config,
            );
            let missed_entries = async {
                match *self.loader {
                    Loader::SingleLoader(_) => {
                        Self::source_by_sloader(
                            &sync_source_keys,
                            self.sfg.clone(),
                            source_load_context,
                        )
                        .await
                    }
                    Loader::MultiLoader(_) => {
                        let mut entries = Vec::with_capacity(keys.len());
                        for keys in sync_source_keys.chunks(self.max_batch_size) {
                            entries.append(
                                &mut Self::source_by_mloader(
                                    keys.to_vec(),
                                    self.mfg.clone(),
                                    source_load_context.clone(),
                                )
                                .await?,
                            );
                        }
                        Ok(entries)
                    }
                }
            }
            .await
            .inspect_err(|_| {
                if let Some(metrics) = self.on_metrics {
                    metrics(
                        "mget",
                        true,
                        self.namespace.as_deref().unwrap_or(""),
                        "source",
                        self.cache_store.name(),
                    );
                }
            })?;

            from = if from == "cache" { "both" } else { "source" };
            entries.extend(missed_entries);
        }

        if let Some(metrics) = self.on_metrics {
            metrics(
                "mget",
                false,
                self.namespace.as_deref().unwrap_or(""),
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
        let cache_fill_config =
            self.cache_fill_config(options.async_set_cache.unwrap_or(self.async_set_cache));
        let source_load_context = self.source_load_context(
            cache_none,
            expire_time,
            none_value_expire_time,
            cache_fill_config,
        );

        let entries = match *self.loader {
            Loader::SingleLoader(_) => {
                Self::source_by_sloader(keys, self.sfg.clone(), source_load_context).await?
            }
            Loader::MultiLoader(_) => {
                let missed_key_vector = keys.chunks(self.max_batch_size).collect::<Vec<_>>();

                let mut entries: Vec<Entry<K, V>> = Vec::with_capacity(keys.len());
                for keys in missed_key_vector.into_iter() {
                    entries.append(
                        &mut Self::source_by_mloader(
                            keys.to_vec(),
                            self.mfg.clone(),
                            source_load_context.clone(),
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
                Ok((
                    kv.0.clone(),
                    Entry {
                        key: kv.0.clone(),
                        value: Some(kv.1.clone()),
                        expire_at_ms: Some(Self::expiration_timestamp(self.expire_time)?),
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        self.cache_store.mset(&kvs).await.map_err(|error| {
            Error::from_cache(CacheOperation::Write, self.cache_store.name(), error)
        })?;

        Ok(())
    }

    pub async fn mdel(&self, keys: &[K]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        self.cache_store.mdel(keys).await.map_err(|error| {
            Error::from_cache(CacheOperation::Delete, self.cache_store.name(), error)
        })
    }

    async fn enqueue_refresh(&self, keys: &[(K, E)], mode: RefreshEnqueueMode) -> Result<()> {
        let Some(sender) = self.async_refresh_channel.load_full() else {
            return Err(Error::RefreshUnavailable);
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

            let permit = match mode {
                RefreshEnqueueMode::WaitForCapacity => sender.reserve().await.map_err(|error| {
                    error!(%error, "autocache: reserve async source task failed");
                    Error::RefreshUnavailable
                })?,
                RefreshEnqueueMode::BestEffort => match sender.try_reserve() {
                    Ok(permit) => permit,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        debug!(
                            key_count = keys.len(),
                            "autocache: async refresh queue is full; skipping automatic refresh"
                        );
                        if let Some(metrics) = self.on_metrics {
                            metrics(
                                "refresh",
                                true,
                                self.namespace.as_deref().unwrap_or(""),
                                "source",
                                self.cache_store.name(),
                            );
                        }
                        return Ok(());
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        warn!("autocache: async refresh worker is unavailable; skipping automatic refresh");
                        if let Some(metrics) = self.on_metrics {
                            metrics(
                                "refresh",
                                true,
                                self.namespace.as_deref().unwrap_or(""),
                                "source",
                                self.cache_store.name(),
                            );
                        }
                        return Ok(());
                    }
                },
            };
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

    pub async fn refresh_with_context(&self, keys: &[(K, E)]) -> Result<()> {
        self.enqueue_refresh(keys, RefreshEnqueueMode::WaitForCapacity)
            .await
    }

    pub async fn with_cache<T>(
        &self,
        op: impl FnOnce(Arc<C>) -> BoxFuture<'static, Result<T>> + Send,
    ) -> Result<T> {
        op(self.cache_store.clone()).await
    }
}

impl<K, V, C> AutoCache<K, V, C, ()>
where
    K: Clone + Debug + Eq + Hash + Sync + Send + 'static,
    V: Clone + Debug + Sync + Send + 'static,
    C: Cache<Key = K, Value = Entry<K, V>> + Sync + Send + 'static,
{
    /// Returns the value associated with a single key.
    pub async fn get(&self, key: &K) -> Result<Option<V>> {
        Ok(self
            .mget(std::slice::from_ref(key))
            .await?
            .into_iter()
            .find(|(result_key, _)| result_key == key)
            .map(|(_, value)| value))
    }

    /// Returns all values found for the requested keys.
    pub async fn mget(&self, keys: &[K]) -> Result<Vec<(K, V)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        self.mget_with_option(keys, Options::default()).await
    }

    /// Returns all values found for the requested keys using per-request options.
    pub async fn mget_with_option(&self, keys: &[K], options: Options) -> Result<Vec<(K, V)>> {
        let keys = keys
            .iter()
            .cloned()
            .map(|key| (key, ()))
            .collect::<Vec<_>>();
        self.mget_with_context_and_option(&keys, options).await
    }

    /// Queues an explicit refresh for the requested keys.
    pub async fn refresh(&self, keys: &[K]) -> Result<()> {
        let keys = keys
            .iter()
            .cloned()
            .map(|key| (key, ()))
            .collect::<Vec<_>>();
        self.refresh_with_context(&keys).await
    }
}

impl<K, V, C, E> Drop for AutoCache<K, V, C, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn drop(&mut self) {
        self.stop();
    }
}

impl<K, V, C, E> AutoCache<K, V, C, E>
where
    K: Clone + Eq + Hash,
    V: Clone,
    C: Cache<Key = K, Value = Entry<K, V>>,
{
    fn stop(&self) {
        if let Some(s) = self.stop_ch.as_ref() {
            let _ = s.try_send(());
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct BatchKey<K>(Vec<K>);

pub(crate) struct AsyncSourceTask<K, E> {
    _crate_time: chrono::DateTime<Utc>,
    keys: Vec<(K, E)>,
}
