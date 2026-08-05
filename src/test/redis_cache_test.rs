use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{redis_cache::RedisCache, AutoCache, Cache, Codec, Entry};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Item {
    count: u32,
    message: String,
}

impl Codec for Item {}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FailingEncodeItem;

impl Codec for FailingEncodeItem {
    fn encode(&self) -> anyhow::Result<bytes::Bytes> {
        anyhow::bail!("encode failed")
    }
}

#[tokio::test]
async fn test_mset_propagates_encode_errors() {
    let redis_cli = redis::Client::open("redis://127.0.0.1:1/").unwrap();
    let cache: RedisCache<String, Entry<String, FailingEncodeItem>> = RedisCache::new(redis_cli);
    let entry = Entry {
        key: "test-key".to_string(),
        value: Some(FailingEncodeItem),
        expire_at_ms: None,
    };

    let error = cache
        .mset(&[("test-key".to_string(), entry)])
        .await
        .err()
        .unwrap();

    assert_eq!(error.to_string(), "encode failed");
}

#[tokio::test]
#[ignore = "requires a Redis server"]
async fn test_redis_cache() {
    let _ = tracing_subscriber::fmt::try_init();

    let redis_url =
        std::env::var("AUTOCACHE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());
    let redis_cli = redis::Client::open(redis_url).unwrap();

    let ac = AutoCache::builder()
        .cache(RedisCache::new(redis_cli))
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

    let key = "autocache:test:redis-cache".to_string();
    ac.mdel(std::slice::from_ref(&key)).await.unwrap();

    let values = ac.mget(&[(key.clone(), ())]).await.unwrap();

    assert_eq!(values.len(), 1);
    assert_eq!(values[0].0, key);
    assert_eq!(values[0].1.message, "autocache:test:redis-cache");

    ac.mdel(std::slice::from_ref(&key)).await.unwrap();
}
