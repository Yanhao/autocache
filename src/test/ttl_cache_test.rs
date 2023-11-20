use futures::FutureExt;

use crate::{autocache::AutoCache, codec::Codec, ttl_cache::TtlCache};

#[tokio::test]
async fn test_builder() {
    let mut ttl_cache: TtlCache<String, _> = TtlCache::new();
    let _ = ttl_cache.start();

    let a = |key: String| async move { Ok(Some(key.clone())) }.boxed();

    let ac = AutoCache::builder()
        .cache(ttl_cache)
        .expire_time(std::time::Duration::from_secs(60))
        .single_loader(a)
        .build();

    let v1 = ac.mget(&[String::from("test-key1")]).await.unwrap();

    dbg!(&v1);
    assert_eq!(v1.len(), 1);
    assert_eq!(v1.get(0).unwrap().0, String::from("test-key1"));
    assert_eq!(v1.get(0).unwrap().1, String::from("test-key1"));
}

impl Codec for String {}
