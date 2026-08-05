use anyhow::Result;
use futures::future::BoxFuture;

type SingleLoadFn<K, V, E> = dyn Fn(K, E) -> BoxFuture<'static, Result<Option<V>>> + Send + Sync;
type MultiLoadFn<K, V, E> =
    dyn Fn(Vec<(K, E)>) -> BoxFuture<'static, Result<Vec<(K, V)>>> + Send + Sync;

pub enum Loader<K, V, E> {
    SingleLoader(Box<SingleLoadFn<K, V, E>>),
    MultiLoader(Box<MultiLoadFn<K, V, E>>),
}
