//! # hash_mapper
//!
//! A Redis Streams-backed task queue with automatic struct ↔ `redis::Value`
//! mapping via a derive macro.
//!
//! ## Features
//!
//! - **`HashMapper` derive macro** — generates bidirectional conversions between
//!   Rust structs and `Vec<(String, redis::Value)>` pairs for zero-copy Redis
//!   integration.
//! - **Consumer queue** — reads from a Redis Stream consumer group with
//!   semaphore-based concurrency control and graceful shutdown.
//! - **Claimer** — reclaims stuck messages via `XAUTOCLAIM`, tracks delivery
//!   counts, and routes exhausted messages to a dead-letter callback.
//! - **Runtime-agnostic** — works with either `tokio` or `smol`
//!   (mutually exclusive feature flags).
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use hash_mapper::prelude::*;
//! use hash_mapper::task::{Task, Queue, QueueBuilder};
//!
//! #[derive(Debug, HashMapper)]
//! struct Job {
//!     url: String,
//!     retries: u32,
//! }
//!
//! // Build and run a consumer queue
//! let queue = Queue::new(QueueBuilder {
//!     name: "jobs".into(),
//!     consumer_group: "workers".into(),
//!     consumer_id: "worker-1".into(),
//!     block_timeout: 5000,
//!     max_concurrent_tasks: 10,
//!     worker: Arc::new(|job: &Job| async move {
//!         println!("processing {}", job.url);
//!         Ok::<(), String>(())
//!     }),
//!     claimer: None,
//!     conn,
//!     _marker: Default::default(),
//! });
//!
//! queue.init(None).await?;
//! let handle = queue.run();
//! // ... later ...
//! handle.shutdown().await;
//! ```
//!
//! ## Runtime selection
//!
//! Enable exactly one of:
//! - `tokio` — uses `tokio::sync::Semaphore`, `tokio::task::JoinSet`
//! - `smol` — uses `mea::semaphore::Semaphore`, `FuturesUnordered`

#[cfg(all(feature = "tokio", feature = "smol"))]
compile_error!("features `tokio` and `smol` are mutually exclusive");

#[cfg(not(any(feature = "tokio", feature = "smol")))]
compile_error!("either feature `tokio` or `smol` must be enabled");

pub use crate::hash_mappable::HashMappable;
use anyhow::Error;
pub use hash_mapper_derive::HashMapper;
pub mod hash_mappable;
mod runtime;
pub mod task;

// Re-export for use by the derive macro's generated code
#[doc(hidden)]
pub use redis;

// ── Generic utilities ─────────────────────────────────────────────────────────

/// Pretty-print every field of a `HashMappable` value.
pub fn debug_map<T: HashMappable + std::fmt::Debug>(value: T) -> Result<(), Error> {
    // Serialize the struct into its Redis-ready pairs
    let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
    // Print each field name right-aligned with its debug representation
    for (k, v) in &pairs {
        println!("{:>20} : {:?}", k, v);
    }
    Ok(())
}

/// Round-trip a value through pairs — useful for testing your mapper.
pub fn round_trip<T: HashMappable>(value: T) -> Result<T, Error> {
    // Serialize to pairs, then immediately deserialize back
    let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
    T::try_from_pairs(&pairs).map_err(|e| anyhow::anyhow!(e))
}

// ── Prelude ───────────────────────────────────────────────────────────────────

/// Convenience re-exports for common usage.
///
/// ```rust,ignore
/// use hash_mapper::prelude::*;
/// ```
pub mod prelude {
    pub use super::{HashMappable, HashMapper, debug_map, round_trip};
}
