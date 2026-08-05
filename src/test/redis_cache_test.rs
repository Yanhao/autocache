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
async fn test_redis_cache() {
    tracing_subscriber::fmt::init();

    let redis_cli = redis::Client::open("redis://127.0.0.1/").unwrap();

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

    let v1 = ac.mget(&[("test-key1".to_string(), ())]).await.unwrap();
    dbg!(&v1);
}
