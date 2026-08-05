use std::{collections::BTreeMap, sync::Arc};

use parking_lot::Mutex;

use crate::{twolevel_cache::TwoLevelCache, Cache, Entry};

type TestEntry = Entry<String, String>;

#[derive(Clone, Default)]
struct TestCache {
    data: Arc<Mutex<BTreeMap<String, TestEntry>>>,
    get_calls: Arc<Mutex<Vec<Vec<String>>>>,
}

impl Cache for TestCache {
    type Key = String;
    type Value = TestEntry;

    async fn mget(&self, keys: &[Self::Key]) -> anyhow::Result<Vec<Self::Value>> {
        self.get_calls.lock().push(keys.to_vec());

        let data = self.data.lock();
        Ok(keys
            .iter()
            .filter_map(|key| data.get(key).cloned())
            .collect())
    }

    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> anyhow::Result<()> {
        let mut data = self.data.lock();
        for (key, value) in kvs {
            data.insert(key.clone(), value.clone());
        }
        Ok(())
    }

    async fn mdel(&self, keys: &[Self::Key]) -> anyhow::Result<()> {
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
