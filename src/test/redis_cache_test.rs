use futures::FutureExt;
use serde::{Deserialize, Serialize};

use crate::{redis_cache::RedisCache, AutoCache, Codec};

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Item {
    count: u32,
    message: String,
}

impl Codec for Item {}

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
        .build();

    let v1 = ac.mget(&[("test-key1".to_string(), ())]).await.unwrap();
    dbg!(&v1);
}
