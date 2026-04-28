//! # flowd
//!
//! A Redis Streams-backed task queue with automatic struct ↔ `redis::Value`
//! mapping via a derive macro.
//!
//! ## Features
//!
//! - **`Job` derive macro** — generates bidirectional conversions between
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
//! use flowd::prelude::*;
//! use flowd::task::{Queue, QueueBuilder};
//!
//! #[derive(Debug, Job)]
//! struct Email {
//!     to: String,
//!     subject: String,
//! }
//!
//! # async fn run() -> anyhow::Result<()> {
//! let client    = redis::Client::open("redis://127.0.0.1:6379")?;
//! let conn      = client.get_multiplexed_async_connection().await?;
//! let read_conn = client.get_multiplexed_async_connection().await?;
//!
//! let queue = Queue::new(
//!     QueueBuilder::new(
//!         "emails", "senders", "sender-1",
//!         |email: Email| async move {
//!             println!("sending to {}", email.to);
//!             Ok::<(), String>(())
//!         },
//!         conn,
//!         read_conn,
//!     )
//!     .block_timeout(5000)
//!     .max_concurrent_tasks(10),
//! );
//!
//! queue.init().await?;
//! let handle = queue.run();
//! // ... later ...
//! handle.shutdown().await;
//! # Ok(()) }
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

pub use crate::job::Job;
use anyhow::Error;
pub mod job;
mod runtime;
pub mod task;

// Re-export for use by the derive macro's generated code
#[doc(hidden)]
pub use redis;

// ── Generic utilities ─────────────────────────────────────────────────────────

/// Pretty-print every field of a `Job` value.
pub fn debug_map<T: Job + std::fmt::Debug>(value: T) -> Result<(), Error> {
   // Serialize the struct into its Redis-ready pairs
   let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
   // Print each field name right-aligned with its debug representation
   for (k, v) in &pairs {
      println!("{:>20} : {:?}", k, v);
   }
   Ok(())
}

/// Round-trip a value through pairs — useful for testing your mapper.
pub fn round_trip<T: Job>(value: T) -> Result<T, Error> {
   // Serialize to pairs, then immediately deserialize back
   let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
   T::try_from_pairs(&pairs).map_err(|e| anyhow::anyhow!(e))
}

// ── Prelude ───────────────────────────────────────────────────────────────────

/// Convenience re-exports for common usage.
///
/// ```rust,ignore
/// use flowd::prelude::*;
/// ```
pub mod prelude {
   pub use super::{Job, debug_map, round_trip};
}
