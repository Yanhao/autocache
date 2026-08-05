use std::sync::Arc;

use arc_swap::ArcSwapOption;
use futures::FutureExt;
use once_cell::sync::Lazy;

use crate::{autocache::AutoCache, ttl_cache::TtlCache, Cache, Entry};

type TestAutoCache = AutoCache<String, String, TtlCache<String, Entry<String, String>>, ()>;

static AC: Lazy<ArcSwapOption<TestAutoCache>> = Lazy::new(|| None.into());

#[tokio::test]
async fn test_builder() {
    let ttl_cache: TtlCache<String, _> =
        TtlCache::new_with_expire_listener(None, |keys: Vec<(String, _)>| {
            Box::pin(async move {
                let _ = AC
                    .load()
                    .as_ref()
                    .unwrap()
                    .refresh(&keys.iter().map(|k| (k.0.clone(), ())).collect::<Vec<_>>())
                    .await;
            })
            .boxed()
        });
    let _ = ttl_cache.start();

    AC.store(Some(Arc::new(
        AutoCache::builder()
            .cache(ttl_cache)
            .expire_time(std::time::Duration::from_secs(60))
            .single_loader(|key: String, ()| async move { Ok(Some(key.clone())) }.boxed())
            .build()
            .unwrap(),
    )));

    let v1 = AC
        .load()
        .as_ref()
        .unwrap()
        .mget(&[(String::from("test-key1"), ())])
        .await
        .unwrap();

    dbg!(&v1);
    assert_eq!(v1.len(), 1);
    assert_eq!(v1.first().unwrap().0, String::from("test-key1"));
    assert_eq!(v1.first().unwrap().1, String::from("test-key1"));
}

#[tokio::test]
async fn test_physical_ttl_is_enforced_without_listener_or_worker() {
    let cache = TtlCache::new(Some(std::time::Duration::from_millis(1)));
    let key = "test-key".to_string();
    let entry = Entry {
        key: key.clone(),
        value: Some("test-value".to_string()),
        expire_at_ms: None,
    };

    cache.mset(&[(key.clone(), entry)]).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    assert!(cache.mget(&[key]).await.unwrap().is_empty());
}

#[tokio::test]
async fn test_cleanup_worker_starts_without_expire_listener() {
    let cache: TtlCache<String, Entry<String, String>> =
        TtlCache::new(Some(std::time::Duration::from_secs(1)));

    cache.start().unwrap();
    cache.stop().unwrap();
}

#[test]
fn test_cleanup_worker_requires_tokio_runtime() {
    let cache: TtlCache<String, Entry<String, String>> =
        TtlCache::new(Some(std::time::Duration::from_secs(1)));

    let error = cache.start().err().unwrap();
    assert_eq!(
        error.to_string(),
        "a Tokio runtime is required for background tasks"
    );
}

// impl Codec for String {}
