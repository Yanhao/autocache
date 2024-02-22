#[derive(Default)]
pub struct Options {
    pub cache_none: Option<bool>,
    pub expire_time: Option<std::time::Duration>,
    pub none_value_expire_time: Option<std::time::Duration>,
    pub source_first: Option<bool>,
    pub async_set_cache: Option<bool>,
    pub use_expired_data: Option<bool>,
}
