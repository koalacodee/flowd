use crate::Job;
use crate::runtime::{Runtime, SelectedRuntime};
use redis::aio::MultiplexedConnection;
use std::sync::atomic::AtomicBool;
use std::{marker::PhantomData, sync::Arc};

/// A single unit of work to be enqueued into a Redis Stream.
///
/// The `id` is passed directly to `XADD` as the message ID. Use `"*"` to
/// let Redis auto-generate a timestamp-based ID.
pub struct Task<T: Job> {
   /// Stream message ID (e.g. `"*"` for auto-generated, or a specific ID).
   pub id: String,
   /// The payload to serialize into stream fields via [`Job`].
   pub payload: T,
}

/// Configuration for the claimer worker that reclaims stuck messages from
/// the pending entry list (PEL) via `XAUTOCLAIM`.
///
/// Messages whose delivery count exceeds [`max_retries`](Self::max_retries)
/// are routed to the optional [`dlq_worker`](Self::dlq_worker) callback and
/// then acknowledged to remove them from the PEL.
pub struct Claimer<I: Job, DE, DF, DFut>
where
   DF: Fn(I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
   /// Minimum idle time (ms) a message must have been pending before it can
   /// be claimed. Maps to the `min-idle-time` argument of `XAUTOCLAIM`.
   pub(super) min_idle_time: usize,
   /// How long (ms) the claimer sleeps when there are no claimable messages.
   pub(super) block_timeout: usize,
   /// Maximum number of messages the claimer processes concurrently,
   /// controlled via a semaphore.
   pub(super) max_concurrent_tasks: usize,
   /// After this many deliveries a message is considered dead and routed to
   /// the DLQ callback instead of retried.
   pub(super) max_retries: usize,
   /// Optional dead-letter callback. Called with `(&payload, delivery_count)`
   /// for messages that exceed `max_retries`. If `None`, exhausted messages
   /// are silently acknowledged.
   pub(super) dlq_worker: Option<Arc<DF>>,
   pub(super) _marker: PhantomData<(I, DE, DFut)>,
}

/// Builder for constructing a [`Claimer`].
///
/// Construct via [`ClaimerBuilder::new()`] and configure with chained setters:
///
/// ```rust,ignore
/// let claimer = ClaimerBuilder::new()
///     .min_idle_time(30_000)
///     .max_retries(3)
///     .dlq_worker(|payload: &Email, attempts| async move {
///         Ok::<(), String>(())
///     });
/// ```
pub struct ClaimerBuilder<I: Job, DE, DF, DFut>
where
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub(super) min_idle_time: usize,
   pub(super) block_timeout: usize,
   pub(super) max_concurrent_tasks: usize,
   pub(super) max_retries: usize,
   pub(super) dlq_worker: Option<Arc<DF>>,
   pub(super) _marker: PhantomData<(I, DE, DFut)>,
}

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
/// | `F`   | Worker closure: `Fn(&I) -> Fut` |
/// | `Fut` | Future returned by the worker |
/// | `DE`  | Error type returned by the DLQ worker |
/// | `DF`  | DLQ worker closure: `Fn(&I, usize) -> DFut` |
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
   pub(super) conn: MultiplexedConnection,
}

/// Builder struct for constructing a [`Queue`].
///
/// Construct via [`QueueBuilder::new()`] and configure with chained setters:
///
/// ```rust,ignore
/// let builder = QueueBuilder::new("emails", "senders", "sender-1", worker, conn)
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
   pub(super) conn: MultiplexedConnection,
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
