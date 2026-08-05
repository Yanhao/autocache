use std::{collections::BTreeMap, sync::Arc};

use futures::FutureExt;
use parking_lot::Mutex;

use crate::{twolevel_cache::TwoLevelCache, AutoCache, Cache, Entry};

type TestEntry = Entry<String, String>;

#[derive(Clone, Default)]
struct TestCache {
    data: Arc<Mutex<BTreeMap<String, TestEntry>>>,
    get_calls: Arc<Mutex<Vec<Vec<String>>>>,
    get_error: Arc<Mutex<Option<&'static str>>>,
    set_error: Arc<Mutex<Option<&'static str>>>,
    del_error: Arc<Mutex<Option<&'static str>>>,
}

impl Cache for TestCache {
    type Key = String;
    type Value = TestEntry;

    async fn mget(&self, keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        self.get_calls.lock().push(keys.to_vec());
        if let Some(error) = *self.get_error.lock() {
            anyhow::bail!(error);
        }

        let data = self.data.lock();
        Ok(keys
            .iter()
            .filter_map(|key| data.get(key).cloned())
            .collect())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        if let Some(error) = *self.set_error.lock() {
            anyhow::bail!(error);
        }

        let mut data = self.data.lock();
        for (key, value) in kvs {
            data.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> anyhow::Result<()> {
        if let Some(error) = *self.del_error.lock() {
            anyhow::bail!(error);
        }

        let mut data = self.data.lock();
        for key in keys {
            data.remove(key);
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "testcache"
    }
}

fn entry(key: &str, value: &str) -> TestEntry {
    Entry {
        key: key.to_string(),
        value: Some(value.to_string()),
        expire_at_ms: None,
    }
}

fn expired_entry(key: &str, value: &str) -> TestEntry {
    Entry {
        key: key.to_string(),
        value: Some(value.to_string()),
        expire_at_ms: Some(0),
    }
}

fn unavailable_two_level_cache() -> TwoLevelCache<String, TestEntry, TestCache, TestCache> {
    let l2 = TestCache::default();
    *l2.get_error.lock() = Some("L2 read unavailable");
    *l2.set_error.lock() = Some("L2 write unavailable");

    TwoLevelCache::new(TestCache::default(), l2)
}

#[tokio::test]
async fn test_mget_loads_only_l1_misses_from_l2_and_warms_l1() {
    let l1 = TestCache::default();
    l1.mset(&[("l1-key".to_string(), entry("l1-key", "l1-value"))])
        .await
        .unwrap();

    let l2 = TestCache::default();
    l2.mset(&[("l2-key".to_string(), entry("l2-key", "l2-value"))])
        .await
        .unwrap();

    let l2_observer = l2.clone();
    let cache = TwoLevelCache::new(l1, l2);
    let keys = vec!["l1-key".to_string(), "l2-key".to_string()];

    let entries = cache.mget(&keys).await.unwrap();
    let values = entries
        .into_iter()
        .map(|entry| (entry.key, entry.value.unwrap()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(values.get("l1-key"), Some(&"l1-value".to_string()));
    assert_eq!(values.get("l2-key"), Some(&"l2-value".to_string()));
    assert_eq!(
        l2_observer.get_calls.lock().clone(),
        vec![vec!["l2-key".to_string()]]
    );

    let entries = cache.mget(&keys).await.unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(l2_observer.get_calls.lock().len(), 1);
}

#[tokio::test]
async fn test_mget_prefers_l2_when_l1_entry_is_expired() {
    let l1 = TestCache::default();
    l1.mset(&[(
        "test-key".to_string(),
        expired_entry("test-key", "stale-value"),
    )])
    .await
    .unwrap();

    let l2 = TestCache::default();
    l2.mset(&[("test-key".to_string(), entry("test-key", "fresh-value"))])
        .await
        .unwrap();

    let l2_observer = l2.clone();
    let cache = TwoLevelCache::new(l1, l2);
    let keys = vec!["test-key".to_string()];

    let entries = cache.mget(&keys).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value.as_deref(), Some("fresh-value"));
    assert_eq!(
        l2_observer.get_calls.lock().clone(),
        vec![vec!["test-key".to_string()]]
    );

    let entries = cache.mget(&keys).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value.as_deref(), Some("fresh-value"));
    assert_eq!(l2_observer.get_calls.lock().len(), 1);
}

#[tokio::test]
async fn test_mget_preserves_expired_l1_entry_when_l2_misses() {
    let l1 = TestCache::default();
    l1.mset(&[(
        "test-key".to_string(),
        expired_entry("test-key", "stale-value"),
    )])
    .await
    .unwrap();

    let l2 = TestCache::default();
    let cache = TwoLevelCache::new(l1, l2);

    let entries = cache.mget(&["test-key".to_string()]).await.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value.as_deref(), Some("stale-value"));
}

#[tokio::test]
async fn test_mget_preserves_expired_l1_entry_when_l2_fails() {
    let l1 = TestCache::default();
    l1.mset(&[(
        "test-key".to_string(),
        expired_entry("test-key", "stale-value"),
    )])
    .await
    .unwrap();

    let l2 = TestCache::default();
    *l2.get_error.lock() = Some("L2 unavailable");
    let cache = TwoLevelCache::new(l1, l2);

    let entries = cache.mget(&["test-key".to_string()]).await.unwrap();

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].value.as_deref(), Some("stale-value"));
}

#[tokio::test]
async fn test_mdel_invalidates_l1_when_l2_fails() {
    let key = "test-key".to_string();
    let l1 = TestCache::default();
    l1.mset(&[(key.clone(), entry(&key, "l1-value"))])
        .await
        .unwrap();
    let l1_observer = l1.clone();

    let l2 = TestCache::default();
    l2.mset(&[(key.clone(), entry(&key, "l2-value"))])
        .await
        .unwrap();
    *l2.del_error.lock() = Some("L2 unavailable");
    let l2_observer = l2.clone();
    let cache = TwoLevelCache::new(l1, l2);

    let error = cache.mdel(std::slice::from_ref(&key)).await.unwrap_err();

    assert_eq!(error.to_string(), "L2 unavailable");
    assert!(!l1_observer.data.lock().contains_key(&key));
    assert!(l2_observer.data.lock().contains_key(&key));
}

#[tokio::test]
async fn test_single_loader_returns_source_value_when_cache_fill_fails() {
    let ac = AutoCache::builder()
        .cache(unavailable_two_level_cache())
        .single_loader(|key: String, ()| async move { Ok(Some(format!("source:{key}"))) }.boxed())
        .build()
        .unwrap();

    let key = "test-key".to_string();
    let result = ac.mget(&[(key.clone(), ())]).await.unwrap();

    assert_eq!(result, vec![(key, "source:test-key".to_string())]);
}

#[tokio::test]
async fn test_multi_loader_returns_source_values_when_cache_fill_fails() {
    let ac = AutoCache::builder()
        .cache(unavailable_two_level_cache())
        .multi_loader(|keys: Vec<(String, ())>| {
            async move {
                Ok(keys
                    .into_iter()
                    .map(|(key, ())| {
                        let value = format!("source:{key}");
                        (key, value)
                    })
                    .collect())
            }
            .boxed()
        })
        .build()
        .unwrap();

    let keys = vec![("key-1".to_string(), ()), ("key-2".to_string(), ())];
    let result = ac.mget(&keys).await.unwrap();

    assert_eq!(
        result,
        vec![
            ("key-1".to_string(), "source:key-1".to_string()),
            ("key-2".to_string(), "source:key-2".to_string()),
        ]
    );
}

#[tokio::test]
async fn test_explicit_mset_still_propagates_cache_write_errors() {
    let ac = AutoCache::builder()
        .cache(unavailable_two_level_cache())
        .single_loader(|key: String, ()| async move { Ok(Some(key)) }.boxed())
        .build()
        .unwrap();

    let error = ac
        .mset(&[("test-key".to_string(), "test-value".to_string())])
        .await
        .unwrap_err();

    assert_eq!(error.to_string(), "L2 write unavailable");
}

#[cfg(all(feature = "localcache", feature = "rediscache"))]
mod redis_integration {
    use futures::FutureExt;
    use serde::{Deserialize, Serialize};

    use crate::{
        local_cache::{LocalCache, LocalCacheOption},
        redis_cache::RedisCache,
        twolevel_cache::TwoLevelCache,
        AutoCache, Codec,
    };

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct Item {
        count: u32,
        message: String,
    }

    impl Codec for Item {}

    #[tokio::test]
    async fn test_redis_cache() {
        let _ = tracing_subscriber::fmt::try_init();

        let redis_cli = redis::Client::open("redis://127.0.0.1/").unwrap();

        let ac = AutoCache::builder()
            .cache(TwoLevelCache::new(
                LocalCache::new(LocalCacheOption {
                    segments: 8,
                    max_capacity: 64,
                    ..Default::default()
                }),
                RedisCache::new(redis_cli),
            ))
            .expire_time(std::time::Duration::from_secs(10))
            .use_expired_data(true)
            .single_loader(|key: String, ()| {
                async move {
                    Ok(Some(Item {
                        count: 1,
                        message: key.clone(),
                    }))
                }
                .boxed()
            })
            .build()
            .unwrap();

        let v1 = ac.mget(&[("test-key1".to_string(), ())]).await.unwrap();
        dbg!(&v1);
    }
}
