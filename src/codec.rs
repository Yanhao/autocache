use bytes::Buf;

pub trait Codec: serde::Serialize + serde::de::DeserializeOwned {
    fn decode(data: bytes::Bytes) -> anyhow::Result<Self> {
        Ok(serde_json::from_reader(data.reader())?)
    }

    fn encode(&self) -> anyhow::Result<bytes::Bytes> {
        Ok(serde_json::to_vec(&self)?.into())
    }

    fn name() -> &'static str {
        "json"
    }
}
