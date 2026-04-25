mod run;
mod types;

use crate::Job;
use crate::runtime::{Runtime, SelectedRuntime};
use crate::task::ClaimerBuilder;
use redis::aio::MultiplexedConnection;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

pub use types::{NoDlqError, NoDlqFn, NoDlqFut, Queue, QueueBuilder, QueueHandle};

// ── QueueBuilder builder methods ─────────────────────────────────────────────

impl<I, E, F, Fut> QueueBuilder<I, E, F, Fut, NoDlqError, NoDlqFn<I>, NoDlqFut>
where
   I: Job + Send + Sync + 'static,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
{
   /// Construct a new `QueueBuilder` with sensible defaults.
   ///
   /// Defaults: `block_timeout = 5000`, `max_concurrent_tasks = 1`, no claimer.
   /// Override via chained setters: [`block_timeout()`](Self::block_timeout),
   /// [`max_concurrent_tasks()`](Self::max_concurrent_tasks),
   /// [`claimer()`](Self::claimer).
   ///
   /// # Connections
   ///
   /// - `conn` is used for all non-blocking operations (XACK, XADD,
   ///   XAUTOCLAIM, XPENDING) and may be shared with other callers.
   /// - `read_conn` is used exclusively for the blocking `XREADGROUP` call.
   ///
   /// **`conn` and `read_conn` MUST be independent `MultiplexedConnection`
   /// instances obtained from separate calls to
   /// [`redis::Client::get_multiplexed_async_connection`]. Do NOT pass a
   /// clone of the same handle.** A `MultiplexedConnection` pipelines all
   /// commands over a single TCP socket, so a blocking `XREADGROUP` on a
   /// shared handle stalls every other command queued behind it —
   /// including the XACKs this queue sends to confirm processed messages,
   /// which will then time out.
   ///
   /// ```rust,ignore
   /// let client = redis::Client::open("redis://127.0.0.1:6379")?;
   /// let conn      = client.get_multiplexed_async_connection().await?;
   /// let read_conn = client.get_multiplexed_async_connection().await?;
   /// let builder = QueueBuilder::new("q", "g", "c", worker, conn, read_conn);
   /// ```
   pub fn new(
      name: impl Into<String>,
      consumer_group: impl Into<String>,
      consumer_id: impl Into<String>,
      worker: F,
      conn: MultiplexedConnection,
      read_conn: MultiplexedConnection,
   ) -> Self {
      Self {
         name: name.into(),
         consumer_group: consumer_group.into(),
         consumer_id: consumer_id.into(),
         block_timeout: 5000,
         max_concurrent_tasks: 1,
         worker: Arc::new(worker),
         claimer: None,
         conn,
         read_conn,
         _marker: PhantomData,
      }
   }
}

impl<I, E, F, Fut, DE, DF, DFut> QueueBuilder<I, E, F, Fut, DE, DF, DFut>
where
   I: Job + Send + Sync + 'static,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   /// How long (ms) `XREADGROUP` blocks waiting for new messages.
   pub fn block_timeout(mut self, ms: usize) -> Self {
      self.block_timeout = ms;
      self
   }

   /// Maximum messages processed concurrently (semaphore-enforced).
   /// Also caps the `COUNT` argument of `XREADGROUP`.
   pub fn max_concurrent_tasks(mut self, n: usize) -> Self {
      self.max_concurrent_tasks = n;
      self
   }

   /// Attach a [`ClaimerBuilder`] for reclaiming stuck messages.
   ///
   /// Changes the builder's generic parameters to match the claimer's DLQ
   /// types, so this must be called before finalizing the builder.
   pub fn claimer<DE2, DF2, DFut2>(
      self,
      claimer: ClaimerBuilder<I, DE2, DF2, DFut2>,
   ) -> QueueBuilder<I, E, F, Fut, DE2, DF2, DFut2>
   where
      DF2: Fn(I, usize) -> DFut2 + 'static + Send + Sync,
      DE2: std::fmt::Display + Send + 'static,
      DFut2: Future<Output = Result<(), DE2>> + Send,
   {
      QueueBuilder {
         name: self.name,
         consumer_group: self.consumer_group,
         consumer_id: self.consumer_id,
         block_timeout: self.block_timeout,
         max_concurrent_tasks: self.max_concurrent_tasks,
         worker: self.worker,
         claimer: Some(claimer),
         conn: self.conn,
         read_conn: self.read_conn,
         _marker: PhantomData,
      }
   }
}

impl QueueHandle {
   /// Signal both loops to stop reading new messages, then wait
   /// for all in-flight tasks to complete before returning.
   pub async fn shutdown(self) {
      // Signal both loops to exit on their next iteration
      self.shutdown.store(true, Ordering::Relaxed);

      // Wait for the main consumer to drain in-flight tasks and return
      SelectedRuntime::join(self.main_join).await;

      // If a claimer was running, wait for it too
      if let Some(claimer_join) = self.claimer_join {
         SelectedRuntime::join(claimer_join).await;
      }
   }
}
