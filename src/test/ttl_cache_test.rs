use std::sync::Arc;

use arc_swap::ArcSwapOption;
use futures::FutureExt;
use once_cell::sync::Lazy;

use crate::{autocache::AutoCache, ttl_cache::TtlCache, Entry};

static AC: Lazy<
    ArcSwapOption<AutoCache<String, String, TtlCache<String, Entry<String, String>>, ()>>,
> = Lazy::new(|| None.into());

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
            .build(),
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
    assert_eq!(v1.get(0).unwrap().0, String::from("test-key1"));
    assert_eq!(v1.get(0).unwrap().1, String::from("test-key1"));
}

// impl Codec for String {}
