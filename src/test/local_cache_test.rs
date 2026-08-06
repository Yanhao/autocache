use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::FutureExt;

use crate::{
    autocache::AutoCache,
    local_cache::{LocalCache, LocalCacheOption},
    Cache, CacheOperation, ConfigurationError, Entry, EntryTrait, Error, LoaderKind, Options,
};

fn on_metrics(_method: &str, _is_error: bool, _ns: &str, _from: &str, _cache_name: &str) {}

static SOURCE_FIRST_METRIC_CALLS: AtomicUsize = AtomicUsize::new(0);
static SOURCE_FIRST_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);
static SOURCE_FIRST_METRIC_SOURCE: AtomicUsize = AtomicUsize::new(0);
static CACHE_FIRST_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);
static CACHE_FIRST_METRIC_SOURCE: AtomicUsize = AtomicUsize::new(0);
static ASYNC_CACHE_WRITE_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);
static ASYNC_REFRESH_QUEUE_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);
static ASYNC_SOURCE_REFRESH_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);
static CACHE_INVALIDATION_METRIC_ERRORS: AtomicUsize = AtomicUsize::new(0);

fn source_first_metrics(_method: &str, is_error: bool, _ns: &str, from: &str, _cache_name: &str) {
    SOURCE_FIRST_METRIC_CALLS.fetch_add(1, Ordering::SeqCst);
    if is_error {
        SOURCE_FIRST_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
    if from == "source" {
        SOURCE_FIRST_METRIC_SOURCE.fetch_add(1, Ordering::SeqCst);
    }
}

fn cache_first_metrics(method: &str, is_error: bool, _ns: &str, from: &str, _cache_name: &str) {
    if method == "mget" && is_error {
        CACHE_FIRST_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
    if method == "mget" && !is_error && from == "source" {
        CACHE_FIRST_METRIC_SOURCE.fetch_add(1, Ordering::SeqCst);
    }
}

fn async_cache_write_metrics(
    method: &str,
    is_error: bool,
    _ns: &str,
    _from: &str,
    _cache_name: &str,
) {
    if method == "mset" && is_error {
        ASYNC_CACHE_WRITE_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
}

fn async_refresh_queue_metrics(
    method: &str,
    is_error: bool,
    _ns: &str,
    _from: &str,
    _cache_name: &str,
) {
    if method == "refresh" && is_error {
        ASYNC_REFRESH_QUEUE_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
}

fn async_source_refresh_metrics(
    method: &str,
    is_error: bool,
    _ns: &str,
    _from: &str,
    _cache_name: &str,
) {
    if method == "refresh" && is_error {
        ASYNC_SOURCE_REFRESH_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
}

fn cache_invalidation_metrics(
    method: &str,
    is_error: bool,
    _ns: &str,
    _from: &str,
    _cache_name: &str,
) {
    if method == "mdel" && is_error {
        CACHE_INVALIDATION_METRIC_ERRORS.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Clone)]
struct Item {
    count: u32,
    message: String,
}

#[derive(Clone)]
struct BlockingWriteCache {
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Semaphore>,
}

impl BlockingWriteCache {
    fn new() -> Self {
        Self {
            started: Arc::new(AtomicUsize::new(0)),
            completed: Arc::new(AtomicUsize::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }
}

impl Cache for BlockingWriteCache {
    type Key = String;
    type Value = Entry<String, String>;

    async fn mget(&self, _keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        Ok(Vec::new())
    }

    async fn mset(&self, _kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release.acquire().await?.forget();
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn mdel(&self, _keys: &[Self::Key]) -> anyhow::Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "blocking-write-cache"
    }
}

#[derive(Clone)]
struct FailingDeleteCache;

impl Cache for FailingDeleteCache {
    type Key = String;
    type Value = Entry<String, String>;

    async fn mget(&self, _keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        Ok(Vec::new())
    }

    async fn mset(&self, _kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn mdel(&self, _keys: &[Self::Key]) -> anyhow::Result<()> {
        anyhow::bail!("cache invalidation failed")
    }

    fn name(&self) -> &'static str {
        "failing-delete-cache"
    }
}

#[derive(Clone)]
struct FailingReadCache;

impl Cache for FailingReadCache {
    type Key = String;
    type Value = Entry<String, String>;

    async fn mget(&self, _keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        anyhow::bail!("cache read failed")
    }

    async fn mset(&self, _kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        Ok(())
    }

    async fn mdel(&self, _keys: &[Self::Key]) -> anyhow::Result<()> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "failing-read-cache"
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TenantKey {
    tenant: &'static str,
    id: &'static str,
}

impl AsRef<str> for TenantKey {
    fn as_ref(&self) -> &str {
        self.id
    }
}

#[tokio::test]
async fn test_builder() {
    let _ = tracing_subscriber::fmt::try_init();

    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption {
            segments: 8,
            max_capacity: 64,
            // ttl: std::time::Duration::from_secs(5),
            ..Default::default()
        }))
        .expire_time(std::time::Duration::from_secs(10))
        .use_expired_data(true)
        .on_metrics(on_metrics)
        .single_loader(|key: String| {
            async move {
                dbg!(&key);
                if key == "test-key4" {
                    return Ok(None);
                }

                Ok(Some(Item {
                    count: 1,
                    message: key.clone(),
                }))
            }
            .boxed()
        })
        .build()
        .unwrap();

    let v1 = ac.mget(&["test-key1".to_string()]).await.unwrap();
    dbg!(&v1);
    assert_eq!(v1.len(), 1);
    assert_eq!(v1[0].1.count, 1);
    assert_eq!(v1[0].1.message, "test-key1");
    // assert_eq!(v1.len(), 1);
    // assert_eq!(
    //     v1.get(0),
    //     Some(&("test-key1".to_string(), "test-key1".to_string()))
    // );

    // let v2 = ac
    //     .mget(&["test-key2".to_string(), "test-key3".to_string()])
    //     .await
    //     .unwrap();
    // assert_eq!(v2.len(), 2);

    // assert_eq!(
    //     v2.get(0),
    //     Some(&("test-key2".to_string(), "test-key2".to_string()))
    // );
    // assert_eq!(
    //     v2.get(1),
    //     Some(&("test-key3".to_string(), "test-key3".to_string()))
    // );

    // let v4 = ac.mget(&["test-key4".to_string()]).await.unwrap();
    // assert!(v4.is_empty());

    // let v5 = ac
    //     .mget(&["test-key3".to_string(), "test-key5".to_string()])
    //     .await
    //     .unwrap();

    // assert_eq!(v5.len(), 2);
    // assert_eq!(
    //     v5.get(0),
    //     Some(&("test-key3".to_string(), "test-key3".to_string()))
    // );
    // assert_eq!(
    //     v5.get(1),
    //     Some(&("test-key5".to_string(), "test-key5".to_string()))
    // );

    // tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // let v6 = ac.mget(&["test-key3".to_string()]).await.unwrap();
    // dbg!(&v6);

    // assert_eq!(v6.len(), 1);
}

#[tokio::test]
async fn test_key_only_and_context_apis() {
    #[derive(Clone)]
    struct SensitiveContext {
        token: String,
    }

    let key_only = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String| async move { Ok(Some(format!("value:{key}"))) })
        .build()
        .unwrap();

    assert_eq!(
        key_only.get(&"key-only".to_string()).await.unwrap(),
        Some("value:key-only".to_string())
    );

    let observed_context = Arc::new(parking_lot::Mutex::new(None));
    let loader_context = observed_context.clone();
    let with_context = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader_with_context(move |key: String, context: SensitiveContext| {
            *loader_context.lock() = Some(context.token);
            async move { Ok(Some(format!("value:{key}"))) }
        })
        .build()
        .unwrap();

    assert_eq!(
        with_context
            .mget_with_context(&[(
                "context-key".to_string(),
                SensitiveContext {
                    token: "secret-token".to_string(),
                },
            )])
            .await
            .unwrap(),
        vec![("context-key".to_string(), "value:context-key".to_string())]
    );
    assert_eq!(observed_context.lock().as_deref(), Some("secret-token"));
}

#[tokio::test]
async fn test_cache_read_errors_are_typed() {
    let cache = AutoCache::builder()
        .cache(FailingReadCache)
        .single_loader(|key: String| async move { Ok(Some(key)) })
        .build()
        .unwrap();

    let error = cache.mget(&["test-key".to_string()]).await.unwrap_err();
    assert!(matches!(
        error,
        Error::Cache {
            operation: CacheOperation::Read,
            cache: "failing-read-cache",
            ..
        }
    ));
}

#[tokio::test]
async fn test_request_scoped_expired_data_falls_back_to_sync_source_without_worker() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();

    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::ZERO)
        .single_loader(move |key: String| {
            let loader_calls = loader_calls.clone();
            async move {
                let generation = loader_calls.fetch_add(1, Ordering::SeqCst) + 1;
                Ok(Some(format!("{key}-{generation}")))
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    let first = ac.mget(std::slice::from_ref(&key)).await.unwrap();
    assert_eq!(first, vec![(key.clone(), "test-key-1".to_string())]);

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let second = ac
        .mget_with_option(
            std::slice::from_ref(&key),
            Options {
                use_expired_data: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(second, vec![(key, "test-key-2".to_string())]);
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_source_first_none_does_not_fall_back_to_expired_cache_entry() {
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::ZERO)
        .source_first(true)
        .single_loader(|_key: String| async move { Ok(None) }.boxed())
        .build()
        .unwrap();

    let key = "test-key".to_string();
    ac.mset(&[(key.clone(), "stale-value".to_string())])
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let result = ac.mget(&[key]).await.unwrap();

    assert!(result.is_empty());
}

#[tokio::test]
async fn test_single_loader_not_found_invalidates_existing_positive_cache() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(move |_key: String| {
            let loader_calls = loader_calls.clone();
            async move {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                Ok(None::<String>)
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    ac.mset(&[(key.clone(), "stale-value".to_string())])
        .await
        .unwrap();

    let source_first = ac
        .mget_with_option(
            std::slice::from_ref(&key),
            Options {
                source_first: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let cache_first = ac.mget(&[key]).await.unwrap();

    assert!(source_first.is_empty());
    assert!(cache_first.is_empty());
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_multi_loader_not_found_invalidates_existing_positive_cache() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .multi_loader(move |_keys: Vec<String>| {
            let loader_calls = loader_calls.clone();
            async move {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::<(String, String)>::new())
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    ac.mset(&[(key.clone(), "stale-value".to_string())])
        .await
        .unwrap();

    let source_first = ac
        .mget_with_option(
            std::slice::from_ref(&key),
            Options {
                source_first: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let cache_first = ac.mget(&[key]).await.unwrap();

    assert!(source_first.is_empty());
    assert!(cache_first.is_empty());
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_source_first_honors_request_scoped_negative_caching() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(move |_key: String| {
            let loader_calls = loader_calls.clone();
            async move {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                Ok(None::<String>)
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    let first = ac
        .mget_with_option(
            std::slice::from_ref(&key),
            Options {
                source_first: Some(true),
                cache_none: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let second = ac.mget(&[key]).await.unwrap();

    assert!(first.is_empty());
    assert!(second.is_empty());
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_source_first_records_success_and_error_metrics() {
    SOURCE_FIRST_METRIC_CALLS.store(0, Ordering::SeqCst);
    SOURCE_FIRST_METRIC_ERRORS.store(0, Ordering::SeqCst);
    SOURCE_FIRST_METRIC_SOURCE.store(0, Ordering::SeqCst);

    let success = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .source_first(true)
        .on_metrics(source_first_metrics)
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .build()
        .unwrap();
    success.mget(&["success-key".to_string()]).await.unwrap();

    let failure = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .source_first(true)
        .on_metrics(source_first_metrics)
        .single_loader(|_key: String| {
            async move { Err::<Option<String>, _>(anyhow::anyhow!("loader failed")) }.boxed()
        })
        .build()
        .unwrap();
    let error = failure
        .mget(&["failure-key".to_string()])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Loader {
            kind: LoaderKind::Single,
            ..
        }
    ));

    assert_eq!(SOURCE_FIRST_METRIC_CALLS.load(Ordering::SeqCst), 2);
    assert_eq!(SOURCE_FIRST_METRIC_ERRORS.load(Ordering::SeqCst), 1);
    assert_eq!(SOURCE_FIRST_METRIC_SOURCE.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_cache_first_records_multi_loader_errors_and_not_found_source() {
    CACHE_FIRST_METRIC_ERRORS.store(0, Ordering::SeqCst);
    CACHE_FIRST_METRIC_SOURCE.store(0, Ordering::SeqCst);

    let not_found = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .on_metrics(cache_first_metrics)
        .multi_loader(|_keys: Vec<String>| {
            async move { Ok(Vec::<(String, String)>::new()) }.boxed()
        })
        .build()
        .unwrap();
    assert!(not_found
        .mget(&["not-found-key".to_string()])
        .await
        .unwrap()
        .is_empty());

    let failure = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .on_metrics(cache_first_metrics)
        .multi_loader(|_keys: Vec<String>| {
            async move { Err::<Vec<(String, String)>, _>(anyhow::anyhow!("multi loader failed")) }
                .boxed()
        })
        .build()
        .unwrap();
    let error = failure
        .mget(&["failure-key".to_string()])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Loader {
            kind: LoaderKind::Batch,
            ..
        }
    ));

    assert_eq!(CACHE_FIRST_METRIC_SOURCE.load(Ordering::SeqCst), 1);
    assert_eq!(CACHE_FIRST_METRIC_ERRORS.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_failed_automatic_invalidation_records_metric() {
    CACHE_INVALIDATION_METRIC_ERRORS.store(0, Ordering::SeqCst);
    let ac = AutoCache::builder()
        .cache(FailingDeleteCache)
        .source_first(true)
        .on_metrics(cache_invalidation_metrics)
        .single_loader(|_key: String| async move { Ok(None::<String>) }.boxed())
        .build()
        .unwrap();

    let result = ac.mget(&["test-key".to_string()]).await.unwrap();

    assert!(result.is_empty());
    assert_eq!(CACHE_INVALIDATION_METRIC_ERRORS.load(Ordering::SeqCst), 1);
}

#[test]
fn test_async_cache_fill_falls_back_without_tokio_runtime() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .async_set_cache(true)
        .single_loader(move |key: String| {
            let loader_calls = loader_calls.clone();
            async move {
                loader_calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(format!("source:{key}")))
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    let first = futures::executor::block_on(ac.mget(std::slice::from_ref(&key))).unwrap();
    let second = futures::executor::block_on(ac.mget(std::slice::from_ref(&key))).unwrap();

    assert_eq!(first, vec![(key.clone(), "source:test-key".to_string())]);
    assert_eq!(second, vec![(key, "source:test-key".to_string())]);
    assert_eq!(source_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_async_cache_fill_is_skipped_when_concurrency_limit_is_reached() {
    const WRITE_LIMIT: usize = 2;

    ASYNC_CACHE_WRITE_METRIC_ERRORS.store(0, Ordering::SeqCst);
    let cache = BlockingWriteCache::new();
    let observer = cache.clone();
    let ac = AutoCache::builder()
        .cache(cache)
        .source_first(true)
        .async_set_cache(true)
        .max_concurrent_async_cache_writes(WRITE_LIMIT)
        .on_metrics(async_cache_write_metrics)
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .build()
        .unwrap();

    for index in 0..=WRITE_LIMIT {
        let key = format!("test-key-{index}");
        assert_eq!(
            ac.mget(std::slice::from_ref(&key)).await.unwrap(),
            vec![(key.clone(), key)]
        );
    }

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while observer.started.load(Ordering::SeqCst) < WRITE_LIMIT {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.started.load(Ordering::SeqCst), WRITE_LIMIT);

    observer.release.add_permits(WRITE_LIMIT);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while observer.completed.load(Ordering::SeqCst) < WRITE_LIMIT {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.completed.load(Ordering::SeqCst), WRITE_LIMIT);
    assert_eq!(ASYNC_CACHE_WRITE_METRIC_ERRORS.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_local_cache_zero_segments_uses_one_segment() {
    let cache = LocalCache::new(LocalCacheOption {
        segments: 0,
        ..Default::default()
    });
    let key = "test-key".to_string();

    cache
        .mset(&[(key.clone(), "test-value".to_string())])
        .await
        .unwrap();

    assert_eq!(
        cache.mget(&[key]).await.unwrap(),
        vec!["test-value".to_string()]
    );
}

#[test]
fn test_entry_expiration_handles_extreme_timestamps() {
    let future = Entry::<String, String> {
        key: "future".to_string(),
        value: None,
        expire_at_ms: Some(i64::MAX),
    };
    let past = Entry::<String, String> {
        key: "past".to_string(),
        value: None,
        expire_at_ms: Some(i64::MIN),
    };

    assert!(!future.is_expired());
    assert!(past.is_expired());
}

#[tokio::test]
async fn test_out_of_range_expiration_duration_returns_error() {
    let positive = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::MAX)
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .build()
        .unwrap();
    assert!(matches!(
        positive.mget(&["test-key".to_string()]).await.unwrap_err(),
        Error::Configuration(ConfigurationError::InvalidExpirationDuration)
    ));
    assert!(matches!(
        positive
            .mset(&[("test-key".to_string(), "test-value".to_string())])
            .await
            .unwrap_err(),
        Error::Configuration(ConfigurationError::InvalidExpirationDuration)
    ));

    let negative = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .cache_none(true)
        .none_value_expire_time(std::time::Duration::MAX)
        .single_loader(|_key: String| async move { Ok(None::<String>) }.boxed())
        .build()
        .unwrap();
    assert!(matches!(
        negative.mget(&["test-key".to_string()]).await.unwrap_err(),
        Error::Configuration(ConfigurationError::InvalidExpirationDuration)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_stale_reads_enqueue_only_one_refresh_per_key() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let refresh_started = Arc::new(tokio::sync::Notify::new());
    let loader_refresh_started = refresh_started.clone();
    let release_refresh = Arc::new(tokio::sync::Notify::new());
    let loader_release_refresh = release_refresh.clone();

    let ac = Arc::new(
        AutoCache::builder()
            .cache(LocalCache::new(LocalCacheOption::default()))
            .expire_time(std::time::Duration::ZERO)
            .use_expired_data(true)
            .single_loader(move |key: String| {
                let loader_calls = loader_calls.clone();
                let loader_refresh_started = loader_refresh_started.clone();
                let loader_release_refresh = loader_release_refresh.clone();
                async move {
                    let generation = loader_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if generation == 2 {
                        loader_refresh_started.notify_one();
                        loader_release_refresh.notified().await;
                    }
                    Ok(Some(format!("{key}-{generation}")))
                }
                .boxed()
            })
            .build()
            .unwrap(),
    );

    let key = "test-key".to_string();
    let first = ac.mget(std::slice::from_ref(&key)).await.unwrap();
    assert_eq!(first, vec![(key.clone(), "test-key-1".to_string())]);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut reads = Vec::new();
    for _ in 0..32 {
        let ac = ac.clone();
        let key = key.clone();
        reads.push(tokio::spawn(async move { ac.mget(&[key]).await.unwrap() }));
    }

    refresh_started.notified().await;
    for read in reads {
        assert_eq!(
            read.await.unwrap(),
            vec![(key.clone(), "test-key-1".to_string())]
        );
    }
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);

    release_refresh.notify_one();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_stale_read_does_not_wait_for_refresh_queue_capacity() {
    const REFRESH_QUEUE_CAPACITY: usize = 2;

    ASYNC_REFRESH_QUEUE_METRIC_ERRORS.store(0, Ordering::SeqCst);
    let refresh_started = Arc::new(tokio::sync::Notify::new());
    let loader_refresh_started = refresh_started.clone();
    let release_refresh = Arc::new(tokio::sync::Notify::new());
    let loader_release_refresh = release_refresh.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::ZERO)
        .use_expired_data(true)
        .async_refresh_queue_capacity(REFRESH_QUEUE_CAPACITY)
        .on_metrics(async_refresh_queue_metrics)
        .single_loader(move |key: String| {
            let loader_refresh_started = loader_refresh_started.clone();
            let loader_release_refresh = loader_release_refresh.clone();
            async move {
                if key == "blocking-key" {
                    loader_refresh_started.notify_one();
                    loader_release_refresh.notified().await;
                }
                Ok(Some(format!("source:{key}")))
            }
            .boxed()
        })
        .build()
        .unwrap();

    let stale_key = "stale-key".to_string();
    ac.mset(&[(stale_key.clone(), "stale-value".to_string())])
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    ac.refresh(&["blocking-key".to_string()]).await.unwrap();
    refresh_started.notified().await;
    for index in 0..REFRESH_QUEUE_CAPACITY {
        ac.refresh(&[format!("queued-key-{index}")]).await.unwrap();
    }

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        ac.mget(std::slice::from_ref(&stale_key)),
    )
    .await
    .expect("stale read should not wait for refresh queue capacity")
    .unwrap();

    assert_eq!(result, vec![(stale_key, "stale-value".to_string())]);
    assert_eq!(ASYNC_REFRESH_QUEUE_METRIC_ERRORS.load(Ordering::SeqCst), 1);
    release_refresh.notify_one();
}

#[tokio::test]
async fn test_async_source_refresh_failure_records_metric() {
    ASYNC_SOURCE_REFRESH_METRIC_ERRORS.store(0, Ordering::SeqCst);
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::ZERO)
        .use_expired_data(true)
        .on_metrics(async_source_refresh_metrics)
        .single_loader(move |key: String| {
            let loader_calls = loader_calls.clone();
            async move {
                if loader_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(Some("stale-value".to_string()))
                } else {
                    anyhow::bail!("source refresh failed for {key}")
                }
            }
            .boxed()
        })
        .build()
        .unwrap();

    let key = "test-key".to_string();
    assert_eq!(
        ac.mget(std::slice::from_ref(&key)).await.unwrap(),
        vec![(key.clone(), "stale-value".to_string())]
    );
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    assert_eq!(
        ac.mget(std::slice::from_ref(&key)).await.unwrap(),
        vec![(key, "stale-value".to_string())]
    );

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while ASYNC_SOURCE_REFRESH_METRIC_ERRORS.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
    assert_eq!(ASYNC_SOURCE_REFRESH_METRIC_ERRORS.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_singleflight_uses_the_complete_typed_key_identity() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let loaders_started = Arc::new(tokio::sync::Semaphore::new(0));
    let loader_started = loaders_started.clone();
    let release_loaders = Arc::new(tokio::sync::Semaphore::new(0));
    let loader_release = release_loaders.clone();

    let ac = Arc::new(
        AutoCache::builder()
            .cache(LocalCache::new(LocalCacheOption::default()))
            .single_loader(move |key: TenantKey| {
                let loader_calls = loader_calls.clone();
                let loader_started = loader_started.clone();
                let loader_release = loader_release.clone();
                async move {
                    loader_calls.fetch_add(1, Ordering::SeqCst);
                    loader_started.add_permits(1);
                    loader_release.acquire_owned().await.unwrap().forget();
                    Ok(Some(format!("{}:{}", key.tenant, key.id)))
                }
                .boxed()
            })
            .build()
            .unwrap(),
    );

    let tenant_a_key = TenantKey {
        tenant: "tenant-a",
        id: "shared-id",
    };
    let tenant_b_key = TenantKey {
        tenant: "tenant-b",
        id: "shared-id",
    };

    let tenant_a_read = {
        let ac = ac.clone();
        let key = tenant_a_key.clone();
        tokio::spawn(async move { ac.mget(&[key]).await.unwrap() })
    };
    let tenant_b_read = {
        let ac = ac.clone();
        let key = tenant_b_key.clone();
        tokio::spawn(async move { ac.mget(&[key]).await.unwrap() })
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        loaders_started.acquire_many(2),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    release_loaders.add_permits(2);

    assert_eq!(
        tenant_a_read.await.unwrap(),
        vec![(tenant_a_key, "tenant-a:shared-id".to_string())]
    );
    assert_eq!(
        tenant_b_read.await.unwrap(),
        vec![(tenant_b_key, "tenant-b:shared-id".to_string())]
    );
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_multi_loader_batch_identity_does_not_use_delimited_strings() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();
    let loaders_started = Arc::new(tokio::sync::Semaphore::new(0));
    let loader_started = loaders_started.clone();
    let release_loaders = Arc::new(tokio::sync::Semaphore::new(0));
    let loader_release = release_loaders.clone();

    let ac = Arc::new(
        AutoCache::builder()
            .cache(LocalCache::new(LocalCacheOption::default()))
            .multi_loader(move |keys: Vec<String>| {
                let loader_calls = loader_calls.clone();
                let loader_started = loader_started.clone();
                let loader_release = loader_release.clone();
                async move {
                    loader_calls.fetch_add(1, Ordering::SeqCst);
                    loader_started.add_permits(1);
                    loader_release.acquire_owned().await.unwrap().forget();
                    Ok(keys
                        .into_iter()
                        .map(|key| {
                            let value = format!("value:{key}");
                            (key, value)
                        })
                        .collect())
                }
                .boxed()
            })
            .build()
            .unwrap(),
    );

    let first_keys = vec!["a,b".to_string(), "c".to_string()];
    let second_keys = vec!["a".to_string(), "b,c".to_string()];

    let first_read = {
        let ac = ac.clone();
        let keys = first_keys.clone();
        tokio::spawn(async move { ac.mget(&keys).await.unwrap() })
    };
    let second_read = {
        let ac = ac.clone();
        let keys = second_keys.clone();
        tokio::spawn(async move { ac.mget(&keys).await.unwrap() })
    };

    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        loaders_started.acquire_many(2),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
    release_loaders.add_permits(2);

    assert_eq!(
        first_read.await.unwrap(),
        vec![
            ("a,b".to_string(), "value:a,b".to_string()),
            ("c".to_string(), "value:c".to_string()),
        ]
    );
    assert_eq!(
        second_read.await.unwrap(),
        vec![
            ("a".to_string(), "value:a".to_string()),
            ("b,c".to_string(), "value:b,c".to_string()),
        ]
    );
    assert_eq!(source_calls.load(Ordering::SeqCst), 2);
}

#[test]
fn test_builder_returns_errors_instead_of_panicking_for_invalid_configuration() {
    type StringCache = LocalCache<String, Entry<String, String>>;
    type StringAutoCache = AutoCache<String, String, StringCache>;

    let missing_cache = StringAutoCache::builder()
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .build();
    assert!(matches!(
        missing_cache.err().unwrap(),
        Error::Configuration(ConfigurationError::MissingCache)
    ));

    let missing_loader = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .build();
    assert!(matches!(
        missing_loader.err().unwrap(),
        Error::Configuration(ConfigurationError::MissingLoader)
    ));

    let invalid_batch_size = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .max_batch_size(0)
        .build();
    assert!(matches!(
        invalid_batch_size.err().unwrap(),
        Error::Configuration(ConfigurationError::InvalidMaxBatchSize)
    ));

    let invalid_async_write_limit = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .max_concurrent_async_cache_writes(0)
        .build();
    assert!(matches!(
        invalid_async_write_limit.err().unwrap(),
        Error::Configuration(ConfigurationError::InvalidMaxConcurrentAsyncCacheWrites)
    ));

    let invalid_refresh_queue_capacity = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .async_refresh_queue_capacity(0)
        .build();
    assert!(matches!(
        invalid_refresh_queue_capacity.err().unwrap(),
        Error::Configuration(ConfigurationError::InvalidAsyncRefreshQueueCapacity)
    ));

    let missing_runtime = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String| async move { Ok(Some(key)) }.boxed())
        .use_expired_data(true)
        .build();
    assert!(matches!(
        missing_runtime.err().unwrap(),
        Error::RuntimeUnavailable
    ));
}
