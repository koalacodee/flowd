use crate::HashMappable;
use crate::runtime::{Runtime, SelectedRuntime};
use redis::aio::MultiplexedConnection;
use std::sync::atomic::AtomicBool;
use std::{marker::PhantomData, sync::Arc};

pub struct Task<T: HashMappable> {
   pub id: String,
   pub payload: T,
}

pub struct Claimer<I: HashMappable, DE, DF, DFut>
where
   DF: Fn(&I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
   pub min_idle_time: usize,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub max_retries: usize,
   pub dlq_worker: Option<Arc<DF>>,
   pub(super) _marker: PhantomData<(I, DE, DFut)>,
}

pub struct ClaimerBuilder<I: HashMappable, DE, DF, DFut>
where
   DF: Fn(&I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub min_idle_time: usize,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub max_retries: usize,
   pub dlq_worker: Option<Arc<DF>>,
   pub(super) _marker: PhantomData<(I, DE, DFut)>,
}

pub struct Queue<I: HashMappable, E, F, Fut, DE, DF, DFut>
where
   F: Fn(&I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
   DF: Fn(&I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
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

pub struct QueueBuilder<I, E, F, Fut, DE, DF, DFut>
where
   I: HashMappable,
   F: Fn(&I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(&I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub name: String,
   pub consumer_group: String,
   pub consumer_id: String,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub worker: Arc<F>,
   pub claimer: Option<Claimer<I, DE, DF, DFut>>,
   pub conn: MultiplexedConnection,
   pub(super) _marker: PhantomData<(I, Fut, E)>,
}

/// Handle returned by `Queue::run()`. Call `.shutdown()` to gracefully stop
/// the consumer and claimer loops, waiting for all in-flight tasks to finish.
pub struct QueueHandle {
   pub(super) shutdown: Arc<AtomicBool>,
   pub(super) main_join: <SelectedRuntime as Runtime>::JoinHandle,
   pub(super) claimer_join: Option<<SelectedRuntime as Runtime>::JoinHandle>,
}
