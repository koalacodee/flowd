//! Redis Streams consumer queue with concurrency control, graceful shutdown,
//! and dead-letter handling.
//!
//! # Architecture
//!
//! ```text
//!  Redis Stream ──XREADGROUP──► main consumer loop ──► worker(task)
//!                                  │                       │
//!                                  │ semaphore             ▼
//!                                  │ backpressure     XACK on success
//!                                  │
//!  PEL (stuck) ──XAUTOCLAIM──► claimer loop ──► worker(task) or DLQ
//!                                  │
//!                             delivery count
//!                             via XPENDING
//! ```
//!
//! - **[`Queue`]** — the main consumer. Built via [`QueueBuilder`], started
//!   with [`Queue::run()`], which returns a [`QueueHandle`] for graceful
//!   shutdown.
//! - **[`Claimer`]** — optional reclaimer for stuck messages. Attached to a
//!   `Queue` via `QueueBuilder::claimer`. Uses `XAUTOCLAIM` + `XPENDING` to
//!   track delivery counts, and routes exhausted messages to an optional
//!   dead-letter callback.
//! - **[`Task`]** — a wrapper pairing a stream message ID with a deserialized
//!   payload.
//!
//! # Example
//!
//! ```rust,ignore
//! use flowd::prelude::*;
//! use flowd::task::{ClaimerBuilder, Queue, QueueBuilder};
//!
//! let client    = redis::Client::open("redis://127.0.0.1:6379")?;
//! let conn      = client.get_multiplexed_async_connection().await?;
//! let read_conn = client.get_multiplexed_async_connection().await?;
//!
//! let claimer = ClaimerBuilder::<Email, _, _, _>::new()
//!     .min_idle_time(30_000)
//!     .max_retries(3)
//!     .dlq_worker(|email: Email, attempts: usize| async move {
//!         log::error!("dead-lettered after {attempts} attempts: {:?}", email);
//!         Ok::<(), String>(())
//!     });
//!
//! let queue = Queue::new(
//!     QueueBuilder::new(
//!         "emails", "senders", "sender-1",
//!         |email: Email| async move { send_email(email).await },
//!         conn,
//!         read_conn,
//!     )
//!     .block_timeout(5000)
//!     .max_concurrent_tasks(20)
//!     .claimer(claimer),
//! );
//!
//! queue.init().await?;
//! let handle = queue.run();
//!
//! // On SIGTERM:
//! handle.shutdown().await;
//! ```

mod claimer;
mod group;
mod helpers;
mod queue;
mod types;

pub use claimer::{Claimer, ClaimerBuilder};
pub use group::{QueueGroup, QueueGroupHandle};
pub use queue::{NoDlqError, NoDlqFn, NoDlqFut, Queue, QueueBuilder, QueueHandle};
pub use types::Task;
