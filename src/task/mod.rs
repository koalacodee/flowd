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
//! use hash_mapper::task::*;
//! use std::sync::Arc;
//!
//! let queue = Queue::new(QueueBuilder {
//!     name: "emails".into(),
//!     consumer_group: "senders".into(),
//!     consumer_id: "sender-1".into(),
//!     block_timeout: 5000,
//!     max_concurrent_tasks: 20,
//!     worker: Arc::new(|email: &Email| async move {
//!         send_email(email).await
//!     }),
//!     claimer: Some(Claimer {
//!         min_idle_time: 30_000,
//!         block_timeout: 10_000,
//!         max_concurrent_tasks: 5,
//!         max_retries: 3,
//!         dlq_worker: Some(Arc::new(|email: &Email, attempts: usize| async move {
//!             log::error!("dead-lettered after {attempts} attempts: {:?}", email);
//!             Ok::<(), String>(())
//!         })),
//!         _marker: Default::default(),
//!     }),
//!     conn,
//!     _marker: Default::default(),
//! });
//!
//! queue.init(None).await?;
//! let handle = queue.run();
//!
//! // On SIGTERM:
//! handle.shutdown().await;
//! ```

mod helpers;
mod queue;
mod types;

pub use types::{Claimer, ClaimerBuilder, Queue, QueueBuilder, QueueHandle, Task};
