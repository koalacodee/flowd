mod types;

use std::{marker::PhantomData, sync::Arc};

pub use types::{Claimer, ClaimerBuilder};

use crate::{
   Job,
   task::{NoDlqError, NoDlqFn, NoDlqFut},
};

// ── ClaimerBuilder builder methods ───────────────────────────────────────────

impl<I> ClaimerBuilder<I, NoDlqError, NoDlqFn<I>, NoDlqFut>
where
   I: Job,
{
   /// Construct a new `ClaimerBuilder` with sensible defaults.
   ///
   /// Defaults: `min_idle_time = 30_000`, `block_timeout = 10_000`,
   /// `max_concurrent_tasks = 1`, `max_retries = 3`, no DLQ worker.
   pub fn new() -> Self {
      Self {
         min_idle_time: 30_000,
         block_timeout: 10_000,
         max_concurrent_tasks: 1,
         max_retries: 3,
         dlq_worker: None,
         _marker: PhantomData,
      }
   }
}

impl<I> Default for ClaimerBuilder<I, NoDlqError, NoDlqFn<I>, NoDlqFut>
where
   I: Job,
{
   fn default() -> Self {
      Self::new()
   }
}

impl<I, DE, DF, DFut> ClaimerBuilder<I, DE, DF, DFut>
where
   I: Job,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   /// Minimum idle time (ms) before a message is eligible for reclaiming.
   pub fn min_idle_time(mut self, ms: usize) -> Self {
      self.min_idle_time = ms;
      self
   }

   /// How long (ms) the claimer sleeps when there are no claimable messages.
   pub fn block_timeout(mut self, ms: usize) -> Self {
      self.block_timeout = ms;
      self
   }

   /// Maximum messages reclaimed concurrently.
   pub fn max_concurrent_tasks(mut self, n: usize) -> Self {
      self.max_concurrent_tasks = n;
      self
   }

   /// Delivery attempts before a message is sent to the DLQ.
   pub fn max_retries(mut self, n: usize) -> Self {
      self.max_retries = n;
      self
   }

   /// Attach a dead-letter callback.
   ///
   /// Changes the builder's generic parameters to match the worker's
   /// signature.
   pub fn dlq_worker<DE2, DF2, DFut2>(self, worker: DF2) -> ClaimerBuilder<I, DE2, DF2, DFut2>
   where
      DF2: Fn(I, usize) -> DFut2 + 'static + Send + Sync,
      DE2: std::fmt::Display + Send + 'static,
      DFut2: Future<Output = Result<(), DE2>> + Send,
   {
      ClaimerBuilder {
         min_idle_time: self.min_idle_time,
         block_timeout: self.block_timeout,
         max_concurrent_tasks: self.max_concurrent_tasks,
         max_retries: self.max_retries,
         dlq_worker: Some(Arc::new(worker)),
         _marker: PhantomData,
      }
   }
}

impl<I, DE, DF, DFut> From<ClaimerBuilder<I, DE, DF, DFut>> for Claimer<I, DE, DF, DFut>
where
   I: Job,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   fn from(builder: ClaimerBuilder<I, DE, DF, DFut>) -> Self {
      Self {
         min_idle_time: builder.min_idle_time,
         block_timeout: builder.block_timeout,
         max_concurrent_tasks: builder.max_concurrent_tasks,
         max_retries: builder.max_retries,
         dlq_worker: builder.dlq_worker,
         _marker: PhantomData,
      }
   }
}

impl<I, DE, DF, DFut> Claimer<I, DE, DF, DFut>
where
   I: Job,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub(crate) fn dlq_worker(&self) -> Option<Arc<DF>> {
      self.dlq_worker.clone()
   }

   pub(crate) fn max_retries(&self) -> usize {
      self.max_retries
   }

   pub(crate) fn block_timeout(&self) -> usize {
      self.block_timeout
   }

   pub(crate) fn min_idle_time(&self) -> usize {
      self.min_idle_time
   }

   pub(crate) fn max_concurrent_tasks(&self) -> usize {
      self.max_concurrent_tasks
   }
}
