use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures::FutureExt;

use crate::{
    autocache::AutoCache,
    local_cache::{LocalCache, LocalCacheOption},
    Entry, Options,
};

fn on_metrics(_method: &str, _is_error: bool, _ns: &str, _from: &str, _cache_name: &str) {}

#[derive(Debug, Clone)]
struct Item {
    count: u32,
    message: String,
}

#[tokio::test]
async fn test_builder() {
    tracing_subscriber::fmt::init();

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
        .single_loader(|key: String, ()| {
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

    let v1 = ac.mget(&[("test-key1".to_string(), ())]).await.unwrap();
    dbg!(&v1);
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
async fn test_request_scoped_expired_data_falls_back_to_sync_source_without_worker() {
    let source_calls = Arc::new(AtomicUsize::new(0));
    let loader_calls = source_calls.clone();

    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .expire_time(std::time::Duration::ZERO)
        .single_loader(move |key: String, ()| {
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
    let first = ac.mget(&[(key.clone(), ())]).await.unwrap();
    assert_eq!(first, vec![(key.clone(), "test-key-1".to_string())]);

    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let second = ac
        .mget_with_option(
            &[(key.clone(), ())],
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
            .single_loader(move |key: String, ()| {
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
    let first = ac.mget(&[(key.clone(), ())]).await.unwrap();
    assert_eq!(first, vec![(key.clone(), "test-key-1".to_string())]);
    tokio::time::sleep(std::time::Duration::from_millis(2)).await;

    let mut reads = Vec::new();
    for _ in 0..32 {
        let ac = ac.clone();
        let key = key.clone();
        reads.push(tokio::spawn(
            async move { ac.mget(&[(key, ())]).await.unwrap() },
        ));
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

#[test]
fn test_builder_returns_errors_instead_of_panicking_for_invalid_configuration() {
    type StringCache = LocalCache<String, Entry<String, String>>;
    type StringAutoCache = AutoCache<String, String, StringCache, ()>;

    let missing_cache = StringAutoCache::builder()
        .single_loader(|key: String, ()| async move { Ok(Some(key)) }.boxed())
        .build();
    assert_eq!(
        missing_cache.err().unwrap().to_string(),
        "cache is required"
    );

    let missing_loader = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .build();
    assert_eq!(
        missing_loader.err().unwrap().to_string(),
        "loader is required"
    );

    let invalid_batch_size = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String, ()| async move { Ok(Some(key)) }.boxed())
        .max_batch_size(0)
        .build();
    assert_eq!(
        invalid_batch_size.err().unwrap().to_string(),
        "max_batch_size must be greater than zero"
    );

    let missing_runtime = StringAutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption::default()))
        .single_loader(|key: String, ()| async move { Ok(Some(key)) }.boxed())
        .use_expired_data(true)
        .build();
    assert_eq!(
        missing_runtime.err().unwrap().to_string(),
        "a Tokio runtime is required for background tasks"
    );
}
