use thiserror::Error;

#[derive(Debug, Error)]
pub enum Aerror {
    #[error("no such key")]
    NoSuchKey,
    #[error("base acache error")]
    Unknown,
}
