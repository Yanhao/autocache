use std::fmt::Debug;

#[cfg(feature = "serilize")]
use bytes::Buf;
use chrono::prelude::*;
#[cfg(feature = "serilize")]
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[cfg(feature = "serilize")]
use crate::codec::Codec;

#[derive(Debug, Clone)]
pub struct Entry<K, V> {
    pub(crate) key: K,
    pub(crate) value: Option<V>,
    pub(crate) expire_at_ms: i64,
}

pub trait EntryTrait<K> {
    fn is_outdated(&self) -> bool;
    fn get_key(&self) -> K;
}

impl<K, V> EntryTrait<K> for Entry<K, V>
where
    K: Clone,
{
    fn is_outdated(&self) -> bool {
        Utc.timestamp_millis_opt(self.expire_at_ms).unwrap() < Utc::now()
    }

    fn get_key(&self) -> K {
        self.key.clone()
    }
}

#[cfg(feature = "serilize")]
pub trait SerilizableEntryTrait {
    fn decode(data: bytes::Bytes) -> anyhow::Result<Self>
    where
        Self: Sized;
    fn encode(&self) -> anyhow::Result<bytes::Bytes>;
}

#[cfg(feature = "serilize")]
impl<K, V> SerilizableEntryTrait for Entry<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Codec,
{
    fn decode(data: bytes::Bytes) -> anyhow::Result<Self> {
        let eni: EntryInner<K> = serde_json::from_reader(data.reader())?;

        let value = if eni.value_data.is_empty() {
            None
        } else {
            Some(V::decode(eni.value_data.into())?)
        };

        Ok(Self {
            key: eni.key,
            value,
            expire_at_ms: eni.expire_at_ms,
        })
    }

    fn encode(&self) -> anyhow::Result<bytes::Bytes> {
        let value_data = match &self.value {
            Some(v) => v.encode()?.to_vec(),
            None => vec![],
        };

        let eni = EntryInner {
            key: self.key.clone(),
            value_data,
            expire_at_ms: self.expire_at_ms,
        };

        Ok(serde_json::to_vec(&eni)?.into())
    }
}

#[cfg(feature = "serilize")]
#[derive(Serialize, Deserialize)]
struct EntryInner<K> {
    key: K,
    value_data: Vec<u8>,
    expire_at_ms: i64,
}
