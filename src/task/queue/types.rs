use redis::aio::MultiplexedConnection;
use std::future::Ready;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::Job;
use crate::runtime::{Runtime, SelectedRuntime};
use crate::task::{Claimer, ClaimerBuilder};

/// A Redis Streams consumer queue.
///
/// Reads messages from a stream via `XREADGROUP`, dispatches them to a
/// worker closure with semaphore-based concurrency control, and acknowledges
/// successful processing with `XACK`.
///
/// Construct via [`QueueBuilder`] and [`Queue::new()`], then call
/// [`Queue::run()`] to start consuming.
///
/// # Type parameters
///
/// | Param | Role |
/// |-------|------|
/// | `I`   | Payload type (must implement [`Job`]) |
/// | `E`   | Error type returned by the worker |
/// | `F`   | Worker closure: `Fn(I) -> Fut` |
/// | `Fut` | Future returned by the worker |
/// | `DE`  | Error type returned by the DLQ worker |
/// | `DF`  | DLQ worker closure: `Fn(I, usize) -> DFut` |
/// | `DFut`| Future returned by the DLQ worker |
pub struct Queue<I: Job, E, F, Fut, DE, DF, DFut>
where
   F: Fn(I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
   DF: Fn(I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
   /// Name of the Redis Stream key (e.g. `"emails"`, `"jobs"`).
   pub name: String,
   pub(super) consumer_group: String,
   pub(super) consumer_id: String,
   pub(super) block_timeout: usize,
   pub(super) max_concurrent_tasks: usize,
   pub(super) worker: Arc<F>,
   pub(super) claimer: Option<Claimer<I, DE, DF, DFut>>,
   pub(super) _marker: PhantomData<(I, Fut, E)>,
   /// Shared multiplexed connection used for non-blocking ops (XACK, XADD,
   /// XAUTOCLAIM, XPENDING).
   pub(super) conn: MultiplexedConnection,
   /// Dedicated connection reserved for the blocking `XREADGROUP` call.
   /// A blocking command stalls the multiplexer for every caller sharing
   /// the underlying socket, so the reader must not share with `conn`.
   pub(super) read_conn: MultiplexedConnection,
}

/// Builder struct for constructing a [`Queue`].
///
/// Construct via [`QueueBuilder::new()`] and configure with chained setters:
///
/// ```rust,ignore
/// let builder = QueueBuilder::new("emails", "senders", "sender-1", worker, conn, read_conn)
///     .block_timeout(5000)
///     .max_concurrent_tasks(10)
///     .claimer(claimer_builder);
/// ```
pub struct QueueBuilder<I, E, F, Fut, DE, DF, DFut>
where
   I: Job,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub(super) name: String,
   pub(super) consumer_group: String,
   pub(super) consumer_id: String,
   pub(super) block_timeout: usize,
   pub(super) max_concurrent_tasks: usize,
   pub(super) worker: Arc<F>,
   pub(super) claimer: Option<ClaimerBuilder<I, DE, DF, DFut>>,
   /// Shared multiplexed connection used for non-blocking ops (XACK, XADD,
   /// XAUTOCLAIM, XPENDING).
   pub(super) conn: MultiplexedConnection,
   /// Dedicated connection reserved for the blocking `XREADGROUP` call.
   pub(super) read_conn: MultiplexedConnection,
   pub(super) _marker: PhantomData<(I, Fut, E)>,
}

/// Handle returned by [`Queue::run()`].
///
/// Holds the shutdown flag and join handles for the main consumer and
/// claimer loops. Call [`shutdown()`](Self::shutdown) to signal both loops
/// to stop accepting new messages, then await the draining of all
/// in-flight tasks.
pub struct QueueHandle {
   pub(super) shutdown: Arc<AtomicBool>,
   pub(super) main_join: <SelectedRuntime as Runtime>::JoinHandle,
   pub(super) claimer_join: Option<<SelectedRuntime as Runtime>::JoinHandle>,
}

// ── Default types for a QueueBuilder with no claimer ─────────────────────────
//
// When no claimer is configured, the `DE/DF/DFut` generics on QueueBuilder
// are unused but still need concrete types. These aliases fill them with
// no-op placeholders so `QueueBuilder::new()` has a single, inferrable
// return type.

/// Placeholder DLQ error type used when no claimer is configured.
pub type NoDlqError = String;
/// Placeholder DLQ future type used when no claimer is configured.
pub type NoDlqFut = Ready<Result<(), NoDlqError>>;
/// Placeholder DLQ function pointer used when no claimer is configured.
pub type NoDlqFn<I> = fn(I, usize) -> NoDlqFut;
