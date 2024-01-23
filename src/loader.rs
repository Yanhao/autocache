use anyhow::Result;
use futures::future::BoxFuture;

pub enum Loader<K, V, E> {
    SingleLoader(Box<dyn Fn(K, E) -> BoxFuture<'static, Result<Option<V>>> + Send + Sync>),
    MultiLoader(Box<dyn Fn(Vec<(K, E)>) -> BoxFuture<'static, Result<Vec<(K, V)>>> + Send + Sync>),
}
