use futures::FutureExt;

use crate::{
    autocache::AutoCache,
    local_cache::{LocalCache, LocalCacheOption},
};

#[tokio::test]
async fn test_builder() {
    let ac = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption {
            segments: 8,
            max_capacity: 64,
            // ttl: std::time::Duration::from_secs(5),
            ..Default::default()
        }))
        .expire_time(std::time::Duration::from_secs(10))
        .single_loader(|key: String| {
            async move {
                dbg!(&key);
                if key == "test-key4" {
                    return Ok(None);
                }

                Ok(Some(key.clone()))
            }
            .boxed()
        })
        .build();

    let v1 = ac.mget(&["test-key1".to_string()]).await.unwrap();
    assert_eq!(v1.len(), 1);
    assert_eq!(
        v1.get(0),
        Some(&("test-key1".to_string(), "test-key1".to_string()))
    );

    let v2 = ac
        .mget(&["test-key2".to_string(), "test-key3".to_string()])
        .await
        .unwrap();
    assert_eq!(v2.len(), 2);

    assert_eq!(
        v2.get(0),
        Some(&("test-key2".to_string(), "test-key2".to_string()))
    );
    assert_eq!(
        v2.get(1),
        Some(&("test-key3".to_string(), "test-key3".to_string()))
    );

    let v4 = ac.mget(&["test-key4".to_string()]).await.unwrap();
    assert!(v4.is_empty());

    let v5 = ac
        .mget(&["test-key3".to_string(), "test-key5".to_string()])
        .await
        .unwrap();

    assert_eq!(v5.len(), 2);
    assert_eq!(
        v5.get(0),
        Some(&("test-key3".to_string(), "test-key3".to_string()))
    );
    assert_eq!(
        v5.get(1),
        Some(&("test-key5".to_string(), "test-key5".to_string()))
    );

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let v6 = ac.mget(&["test-key3".to_string()]).await.unwrap();
    dbg!(&v6);

    assert_eq!(v6.len(), 1);
}
