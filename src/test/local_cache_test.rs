use futures::FutureExt;

use crate::{
    acache::Acache,
    local_cache::{LocalCache, LocalCacheOption},
};

#[tokio::test]
async fn test_builder() {
    let ac = Acache::builder()
        .cache(LocalCache::new(LocalCacheOption {
            segments: 8,
            max_capacity: 64,
            ..Default::default()
        }))
        .expire_time(std::time::Duration::from_secs(60))
        .single_loader(|key: String| async move { Ok(key.clone()) }.boxed())
        .build();

    let v1 = ac.mget(&["test-key1".to_string()]).await.unwrap();
    dbg!(&v1);
    assert_eq!(v1.len(), 1);
    assert_eq!(v1.get(0).unwrap().0, String::from("test-key1"));
    assert_eq!(v1.get(0).unwrap().1, String::from("test-key1"));
}
