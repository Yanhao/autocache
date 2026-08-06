# autocache

[![Crates.io](https://img.shields.io/crates/v/autocache)](https://crates.io/crates/autocache)
[![Documentation](https://docs.rs/autocache/badge.svg)](https://docs.rs/autocache)
[![License](https://img.shields.io/crates/l/autocache)](#license)
[![Build Status][actions-badge]][actions-url]

[actions-badge]: https://github.com/Yanhao/autocache/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/Yanhao/autocache/actions/workflows/rust.yml

`autocache` is an asynchronous Rust cache-aside library. It combines a cache
backend with a single-key or batch source loader and handles cache misses,
logical expiration, negative caching, request coalescing, background refresh,
and best-effort cache fills.

## Features

- Cache-first and source-first read paths.
- Single-key and batch loaders.
- Singleflight request coalescing for concurrent source reads.
- Logical TTL and optional stale-while-revalidate behavior.
- Explicit negative-cache semantics for authoritative not-found results.
- Bounded, best-effort asynchronous cache writes and refresh queues.
- Local, Redis, TTL, and two-level cache implementations.
- A generic `Cache` trait for custom backends.

## Installation

The default feature enables the local Moka-backed cache:

```toml
[dependencies]
autocache = "0.4"
anyhow = "1"
futures = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Available crate features:

| Feature | Description |
| --- | --- |
| `localcache` | Enables `LocalCache`; enabled by default. |
| `ttlcache` | Enables `TtlCache`. |
| `rediscache` | Enables `RedisCache` and serialization support. |
| `twolevelcache` | Enables `TwoLevelCache` and serialization support. |
| `serialize` | Enables `Codec` and serialized entries. |

For a local + Redis two-level cache:

```toml
autocache = { version = "0.4", features = ["localcache", "rediscache", "twolevelcache"] }
redis = { version = "0.26", features = ["tokio-comp"] }
serde = { version = "1", features = ["derive"] }
```

## Quick start

```rust,no_run
use std::time::Duration;

use autocache::{
    local_cache::{LocalCache, LocalCacheOption},
    AutoCache,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cache = AutoCache::builder()
        .cache(LocalCache::new(LocalCacheOption {
            max_capacity: 10_000,
            ..Default::default()
        }))
        .expire_time(Duration::from_secs(60))
        .single_loader(|key: String| async move {
            // Replace this with a database or service call.
            Ok(Some(format!("value-for-{key}")))
        })
        .build()?;

    let values = cache
        .mget(&["user:42".to_string()])
        .await?;

    assert_eq!(
        values,
        vec![("user:42".to_string(), "value-for-user:42".to_string())]
    );
    Ok(())
}
```

The common API accepts keys directly. `K` is the cache and singleflight
identity:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
# async fn example() -> anyhow::Result<()> {
# let cache = AutoCache::builder()
#     .cache(LocalCache::new(LocalCacheOption::default()))
#     .single_loader(|key: String| async move { Ok(Some(key)) })
#     .build()?;
let value = cache.get(&"user:42".to_string()).await?;
let values = cache
    .mget(&["user:42".to_string(), "user:43".to_string()])
    .await?;
# let _ = (value, values);
# Ok(())
# }
```

If a loader needs per-request context, use the explicit context API:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
#[derive(Clone)]
struct RequestContext {
    trace_id: String,
}

# async fn example() -> anyhow::Result<()> {
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .single_loader_with_context(|key: String, context: RequestContext| async move {
        let _trace_id = context.trace_id;
        Ok(Some(format!("value-for-{key}")))
    })
    .build()?;

let values = cache
    .mget_with_context(&[(
        "user:42".to_string(),
        RequestContext {
            trace_id: "request-123".to_string(),
        },
    )])
    .await?;
# let _ = values;
# Ok(())
# }
```

Here `E` is extra input passed to the loader but is not part of the identity.

For a given `K`, the loader result must not vary based on `E`. Put tenant IDs,
versions, locales, or any other value-affecting input in `K` itself.

## Read paths

### Cache-first

Cache-first is the default:

1. Read requested keys from the cache.
2. Keep fresh cache entries.
3. Load missing or expired keys from the source.
4. Attempt to write source results back to the cache.

Cache write failures are logged and reported through metrics, but do not replace
a successful authoritative source result. Cache read errors currently propagate
to the caller rather than being treated as misses.

### Source-first

Use source-first when freshness is more important than avoiding source calls:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .source_first(true)
    .single_loader(|key: String| {
        async move { Ok(Some(key)) }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

Source-first bypasses cache reads. A successful source result is authoritative,
including a not-found result; it never falls back to an older cached value.

## Not-found and negative caching

A single loader returning `Ok(None)` means the key authoritatively does not
exist. A batch loader expresses the same result by omitting a requested key.
Return `Err` when the source could not reliably determine whether a key exists.

By default, `cache_none` is disabled. An authoritative not-found result removes
any existing positive cache entry without writing a negative entry. Failure to
perform that automatic invalidation is logged and reported, but the source
result is still returned.

Enable negative caching when repeated misses are expensive:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
# use std::time::Duration;
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .cache_none(true)
    .none_value_expire_time(Duration::from_secs(15))
    .single_loader(|_key: String| {
        async move { Ok(None::<String>) }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

## Expiration and refresh

`expire_time` is the logical TTL stored in each `Entry`; its default is 60
seconds. `none_value_expire_time` controls negative entries and also defaults to
60 seconds. Backend-specific physical TTLs are independent.

Enable stale-while-revalidate behavior with `use_expired_data(true)`:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .use_expired_data(true)
    .async_refresh_queue_capacity(256)
    .single_loader(|key: String| {
        async move { Ok(Some(key)) }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

When a logical entry is expired, the stale value is returned immediately and an
automatic refresh is queued. Automatic refresh is best-effort: if the queue is
full, the refresh is skipped without blocking the read. The queue holds batches,
defaults to 512, and must have a capacity greater than zero.

Calls to `AutoCache::refresh` are explicit operations and wait for queue
capacity. A refresh worker exists only when `use_expired_data` or
`manually_refresh` is enabled, and therefore these modes require a Tokio runtime
when the cache is built.

With `manually_refresh(true)`, expired entries are not automatically loaded or
refreshed. Enable `use_expired_data(true)` as well if reads should continue to
return stale values while your application calls `refresh` explicitly.

## Cache writes

Automatic source fills are synchronous by default, although a failed automatic
fill never replaces the successful source result. To detach them from the read:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .async_set_cache(true)
    .max_concurrent_async_cache_writes(64)
    .single_loader(|key: String| {
        async move { Ok(Some(key)) }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

Asynchronous fills are unordered and best-effort. If the concurrency limit is
reached, the fill is skipped. If no Tokio runtime is available, the fill runs
synchronously. Explicit `AutoCache::mset` and `AutoCache::mdel` operations still
return backend errors to the caller.

## Batch loading

Use a multi-loader to fetch source values in batches:

```rust,no_run
# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .max_batch_size(100)
    .multi_loader(|keys: Vec<String>| {
        async move {
            Ok(keys
                .into_iter()
                .map(|key| {
                    let value = format!("value-for-{key}");
                    (key, value)
                })
                .collect())
        }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

`max_batch_size` defaults to 100 and must be greater than zero. Omitting a
requested key from a successful batch is an authoritative not-found result.

## Per-request options

Builder settings can be overridden for an individual read:

```rust,no_run
# use autocache::{AutoCache, Options, local_cache::{LocalCache, LocalCacheOption}};
# use std::time::Duration;
# async fn example() -> anyhow::Result<()> {
# let cache = AutoCache::builder()
#     .cache(LocalCache::new(LocalCacheOption::default()))
#     .single_loader(|key: String| async move { Ok(Some(key)) })
#     .build()?;
let values = cache
    .mget_with_option(
        &["user:42".to_string()],
        Options {
            source_first: Some(true),
            expire_time: Some(Duration::from_secs(30)),
            async_set_cache: Some(true),
            ..Default::default()
        },
    )
    .await?;
# let _ = values;
# Ok(())
# }
```

`Options` can override `cache_none`, positive and negative TTLs, source-first,
asynchronous cache writes, and stale-data usage.

## Cache backends

### LocalCache

`LocalCache` uses Moka. Its defaults are eight segments, a five-minute physical
TTL, and a maximum capacity of 1024. A zero segment count is normalized to one.

The backend physical TTL is separate from AutoCache's logical `expire_time`.
Keeping the physical TTL longer than the logical TTL allows stale entries to be
returned during background refresh.

### RedisCache

Values stored in Redis must implement `Codec`. The default `Codec`
implementation uses JSON:

```rust,no_run
# #[cfg(feature = "rediscache")]
# mod redis_example {
use std::time::Duration;

use autocache::{redis_cache::RedisCache, AutoCache, Codec};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
struct User {
    name: String,
}

impl Codec for User {}

# async fn example() -> anyhow::Result<()> {
let client = redis::Client::open("redis://127.0.0.1/")?;
let cache = AutoCache::builder()
    .cache(RedisCache::new(client))
    .namespace("my-service".to_string())
    .expire_time(Duration::from_secs(60))
    .single_loader(|key: String| {
        async move {
            Ok(Some(User {
                name: format!("user-{key}"),
            }))
        }
    })
    .build()?;
# let _ = cache;
# Ok(())
# }
# }
```

`RedisCache::new` does not set a physical Redis TTL. Use
`RedisCache::new_with_ttl` when physical expiration is required. `namespace`
prefixes Redis keys and should be changed or the old keys cleared when deploying
an incompatible codec or cache wire format.

### TwoLevelCache

`TwoLevelCache` composes an L1 and L2 cache:

```rust,no_run
# #[cfg(all(feature = "localcache", feature = "rediscache", feature = "twolevelcache"))]
# mod two_level_example {
use autocache::{
    local_cache::{LocalCache, LocalCacheOption},
    redis_cache::RedisCache,
    twolevel_cache::TwoLevelCache,
    Codec, Entry,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, Serialize)]
struct Value(String);

impl Codec for Value {}

fn example(client: redis::Client) {
let l1 = LocalCache::<String, Entry<String, Value>>::new(LocalCacheOption::default());
let l2 = RedisCache::<String, Entry<String, Value>>::new(client);
let backend = TwoLevelCache::<String, Entry<String, Value>, _, _>::new(l1, l2);
let _ = backend;
}
# }
```

Fresh L1 hits avoid L2. L1 misses are read from L2 and successful L2 values
warm L1. L1 warm failures are logged without discarding the L2 result. L2 read
failures fall back to available L1 entries. L1 read failures currently propagate.
Writes go to L2 before L1; deletes are attempted in both levels.

### TtlCache

`TtlCache` provides an in-memory physical TTL and an optional expiration
listener. `mget` enforces physical expiration even if its cleanup worker is not
running. `start` enables proactive cleanup, `stop` cancels the worker, and a
stopped cache can be started again. Dropping the cache cancels its worker.

## Metrics

Register a function pointer with `on_metrics`:

```rust,no_run
fn record_metric(method: &str, is_error: bool, ns: &str, from: &str, cache: &str) {
    println!("method={method} error={is_error} namespace={ns} from={from} cache={cache}");
}

# use autocache::{AutoCache, local_cache::{LocalCache, LocalCacheOption}};
let cache = AutoCache::builder()
    .cache(LocalCache::new(LocalCacheOption::default()))
    .on_metrics(record_metric)
    .single_loader(|key: String| {
        async move { Ok(Some(key)) }
    })
    .build()?;
# Ok::<(), anyhow::Error>(())
```

Metric methods are:

| Method | Meaning |
| --- | --- |
| `mget` | A cache/source read completed, or a synchronous source read failed. |
| `mset` | An automatic cache fill failed or was skipped. |
| `refresh` | A refresh was skipped or its background source load failed. |
| `mdel` | Automatic invalidation after an authoritative miss failed. |

For successful `mget` metrics, `from` is `cache`, `source`, `both`, or `-` when
no cache/source origin was selected. Automatic maintenance failures use
`from="source"`.

## Custom cache backends

Implement `Cache` to integrate another backend. `mget` may return partial
results; each returned `Entry` carries its own key, value, and logical expiration
timestamp. Backend operations must return `Send` futures.

The public `with_cache` method can be used for backend-specific operations
without exposing ownership of the configured cache.

## Error handling

`AutoCache` operations return `autocache::Result<T>`. Errors retain their
underlying source and can be matched by category and operation:

```rust
use autocache::{CacheOperation, ConfigurationError, Error, LoaderKind};

fn classify(error: Error) {
    match error {
        Error::Configuration(ConfigurationError::MissingLoader) => {
            eprintln!("configure a loader before building the cache");
        }
        Error::Cache {
            operation: CacheOperation::Read,
            cache,
            source,
        } => {
            eprintln!("failed to read {cache}: {source}");
        }
        Error::Loader {
            kind: LoaderKind::Single,
            source,
        } => {
            eprintln!("single loader failed: {source}");
        }
        Error::Serialization { source, .. } => {
            eprintln!("serialization failed: {source}");
        }
        _ => {}
    }
}
```

Cache reads, explicit writes, and explicit deletes are distinguished by
`CacheOperation`. Single-key and batch loader failures are distinguished by
`LoaderKind`; encode and decode failures use `SerializationOperation`.

## Operational notes

- `max_batch_size`, `max_concurrent_async_cache_writes`, and
  `async_refresh_queue_capacity` must all be greater than zero.
- Cache and source keys should be stable and implement the required `Eq`, `Hash`,
  and thread-safety traits.
- Source errors propagate for foreground loads. Background refresh errors are
  logged and reported through metrics.
- Automatic writes and invalidations never replace an authoritative source
  result with a cache maintenance error.
- Enable a `tracing` subscriber to consume diagnostic logs.

## License

Licensed under the [MIT License](https://github.com/Yanhao/autocache/blob/master/LICENSE).
