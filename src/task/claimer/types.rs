use crate::Job;
use std::marker::PhantomData;
use std::sync::Arc;

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
   /// Optional dead-letter callback. Called with `(payload, delivery_count)`
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
///     .dlq_worker(|payload: Email, attempts| async move {
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
