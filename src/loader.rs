use anyhow::Result;
use futures::future::BoxFuture;

pub enum Loader<K, V> {
    SingleLoader(Box<dyn Fn(K) -> BoxFuture<'static, Result<Option<V>>> + Send + Sync>),
    MultiLoader(Box<dyn Fn(&[K]) -> BoxFuture<'static, Result<Vec<(K, V)>>> + Send + Sync>),
}
