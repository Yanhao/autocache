use anyhow::Result;

use crate::Entry;

pub trait Cache {
    /// The cache lookup key.
    type Key;

    /// The logical value managed by [`crate::AutoCache`].
    ///
    /// Cache metadata is carried by [`Entry`] and must not be included in this
    /// associated type.
    type Value;

    /// Returns the entries found for `keys`; missing keys are omitted.
    fn mget(
        &self,
        keys: &[Self::Key],
    ) -> impl std::future::Future<Output = Result<Vec<Entry<Self::Key, Self::Value>>>> + Send;

    /// Stores entries using each entry's embedded key.
    fn mset(
        &self,
        entries: &[Entry<Self::Key, Self::Value>],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
    fn mdel(&self, keys: &[Self::Key]) -> impl std::future::Future<Output = Result<()>> + Send;

    fn name(&self) -> &'static str;
    fn set_ns(&self, _ns: String) {}
}
