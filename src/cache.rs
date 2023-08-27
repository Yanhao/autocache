use anyhow::Result;

pub trait Cache {
    type Key;
    type Value;

    // return partial results when some keys are not found
    async fn mget(&self, keys: &[Self::Key]) -> Result<Vec<Self::Value>>;
    async fn mset(&self, kvs: &[(Self::Key, Self::Value)]) -> Result<()>;
    async fn mdel(&self, keys: &[Self::Key]) -> Result<()>;
}
