use std::fmt::Debug;

#[cfg(feature = "serialize")]
use bytes::Buf;
use chrono::Utc;
#[cfg(feature = "serialize")]
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[cfg(feature = "serialize")]
use crate::{codec::Codec, Error, Result, SerializationOperation};

#[derive(Debug, Clone)]
pub struct Entry<K, V> {
    pub key: K,
    pub value: Option<V>,
    pub expire_at_ms: Option<i64>,
}

pub trait EntryTrait<K> {
    fn get_key(&self) -> K;
    fn is_expired(&self) -> bool;
}

impl<K, V> EntryTrait<K> for Entry<K, V>
where
    K: Clone,
{
    fn is_expired(&self) -> bool {
        self.expire_at_ms
            .is_some_and(|expire_at_ms| expire_at_ms < Utc::now().timestamp_millis())
    }

    fn get_key(&self) -> K {
        self.key.clone()
    }
}

#[cfg(feature = "serialize")]
pub trait SerializableEntryTrait {
    fn decode(data: bytes::Bytes) -> Result<Self>
    where
        Self: Sized;
    fn encode(&self) -> Result<bytes::Bytes>;
}

#[cfg(feature = "serialize")]
impl<K, V> SerializableEntryTrait for Entry<K, V>
where
    K: Serialize + DeserializeOwned + Clone,
    V: Codec,
{
    fn decode(data: bytes::Bytes) -> Result<Self> {
        let eni: EntryInner<K> = serde_json::from_reader(data.reader()).map_err(|error| {
            Error::serialization(SerializationOperation::Decode, "entry-json", error)
        })?;

        let value = eni
            .value_data
            .map(|data| {
                V::decode(data.into()).map_err(|error| {
                    Error::serialization(SerializationOperation::Decode, V::name(), error)
                })
            })
            .transpose()?;

        Ok(Self {
            key: eni.key,
            value,
            expire_at_ms: eni.expire_at_ms,
        })
    }

    fn encode(&self) -> Result<bytes::Bytes> {
        let value_data = self
            .value
            .as_ref()
            .map(|value| {
                value.encode().map(|data| data.to_vec()).map_err(|error| {
                    Error::serialization(SerializationOperation::Encode, V::name(), error)
                })
            })
            .transpose()?;

        let eni = EntryInner {
            key: self.key.clone(),
            value_data,
            expire_at_ms: self.expire_at_ms,
        };

        Ok(serde_json::to_vec(&eni)
            .map_err(|error| {
                Error::serialization(SerializationOperation::Encode, "entry-json", error)
            })?
            .into())
    }
}

#[cfg(feature = "serialize")]
#[derive(Serialize, Deserialize)]
struct EntryInner<K> {
    key: K,
    value_data: Option<Vec<u8>>,
    expire_at_ms: Option<i64>,
}

#[cfg(all(test, feature = "serialize"))]
mod tests {
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::{Entry, SerializableEntryTrait};
    use crate::{Codec, Error, SerializationOperation};

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
    struct EmptyValue;

    impl Codec for EmptyValue {
        fn decode(data: Bytes) -> anyhow::Result<Self> {
            anyhow::ensure!(data.is_empty(), "expected an empty payload");
            Ok(Self)
        }

        fn encode(&self) -> anyhow::Result<Bytes> {
            Ok(Bytes::new())
        }
    }

    #[test]
    fn some_empty_payload_round_trips_as_some() {
        let entry = Entry {
            key: "test-key".to_string(),
            value: Some(EmptyValue),
            expire_at_ms: Some(42),
        };

        let decoded = Entry::<String, EmptyValue>::decode(entry.encode().unwrap()).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.value, entry.value);
        assert_eq!(decoded.expire_at_ms, entry.expire_at_ms);
    }

    #[test]
    fn none_round_trips_as_none() {
        let entry = Entry::<String, EmptyValue> {
            key: "test-key".to_string(),
            value: None,
            expire_at_ms: Some(42),
        };

        let decoded = Entry::<String, EmptyValue>::decode(entry.encode().unwrap()).unwrap();

        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.value, entry.value);
        assert_eq!(decoded.expire_at_ms, entry.expire_at_ms);
    }

    #[test]
    fn invalid_entry_payload_returns_typed_decode_error() {
        let error =
            Entry::<String, EmptyValue>::decode(Bytes::from_static(b"not-json")).unwrap_err();

        assert!(matches!(
            error,
            Error::Serialization {
                operation: SerializationOperation::Decode,
                codec: "entry-json",
                ..
            }
        ));
    }
}
