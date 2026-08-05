use anyhow::Result;

pub trait Cache {
    type Key;
    type Value;

    /// Returns partial results when some keys are not found.
    fn mget(
        &self,
        keys: &[Self::Key],
    ) -> impl std::future::Future<Output = Result<Vec<Self::Value>>> + Send;

    fn mset(
        &self,
        kvs: &[(Self::Key, Self::Value)],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn mdel(&self, keys: &[Self::Key]) -> impl std::future::Future<Output = Result<()>> + Send;

    fn name(&self) -> &'static str;
    fn set_ns(&self, _ns: String) {}
}
