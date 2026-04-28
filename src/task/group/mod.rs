mod types;

use crate::{
   Job,
   task::{Queue, QueueHandle},
};
use anyhow::Error;
use redis::RedisResult;
use redis::aio::MultiplexedConnection;

pub use types::{QueueGroup, QueueGroupHandle};

impl QueueGroupHandle {
   /// Signal every queue in the group to stop accepting new messages and
   /// wait for all in-flight tasks across every queue to drain.
   ///
   /// Each underlying [`QueueHandle::shutdown`] is awaited concurrently via
   /// `join_all`, so the total wait is bounded by the slowest queue rather
   /// than the sum.
   pub async fn shutdown(self) {
      let handles = self.handles;
      futures_util::future::join_all(handles.into_iter().map(|h| h.shutdown())).await;
   }
}

// Sealed-trait pattern: external crates can name `RunnableQueue` (it has
// to be `pub` because it appears as a bound on `QueueGroup::push`) but
// they cannot implement it, since the supertrait `Sealed` lives in a
// private module.
mod sealed {
   pub trait Sealed {}
}

/// Type-erased view of a [`Queue`] that hides its generic parameters so
/// queues with different payload, worker, and DLQ types can be stored
/// together inside a [`QueueGroup`].
///
/// This trait is **sealed**: the blanket impl below covers every
/// well-formed `Queue<...>`, and external crates cannot add their own
/// impls. It exists only to make `Vec<Box<dyn RunnableQueue>>` legal as
/// the group's storage.
///
/// `self: Box<Self>` is required for object safety — methods that take
/// `self` by value cannot be called through a `dyn` pointer.
pub trait RunnableQueue: sealed::Sealed {
   /// Consume the boxed queue and start its consumer (and optional claimer)
   /// loops. Equivalent to calling [`Queue::run`] on the underlying queue.
   fn run(self: Box<Self>) -> QueueHandle;

   /// Stream key (the `name` passed to [`QueueBuilder::new`]).
   fn name(&self) -> &str;

   /// Consumer group name.
   fn consumer_group(&self) -> &str;

   /// Configured starting ID for the consumer group, or `None` if none was
   /// set (in which case [`QueueGroup::init_all`] defaults to `"0"`).
   fn starting_id(&self) -> Option<&str>;

   /// Clone of the queue's shared (non-blocking) connection. Used by
   /// [`QueueGroup::init_all`] to send a pipelined `XGROUP CREATE` for
   /// every queue in the group from a single round-trip.
   fn conn(&self) -> MultiplexedConnection;
}

impl<I, E, F, Fut, DE, DF, DFut> sealed::Sealed for Queue<I, E, F, Fut, DE, DF, DFut>
where
   I: Job + Send + Sync + 'static,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
}

impl<I, E, F, Fut, DE, DF, DFut> RunnableQueue for Queue<I, E, F, Fut, DE, DF, DFut>
where
   I: Job + Send + Sync + 'static,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   fn run(self: Box<Self>) -> QueueHandle {
      Queue::run(*self)
   }

   fn name(&self) -> &str {
      Queue::name(self)
   }

   fn consumer_group(&self) -> &str {
      Queue::consumer_group(self)
   }

   fn starting_id(&self) -> Option<&str> {
      Queue::starting_id(self)
   }

   fn conn(&self) -> MultiplexedConnection {
      Queue::conn(self)
   }
}

impl QueueGroup {
   /// Add a queue to the group.
   ///
   /// Accepts any concrete `Queue<...>` (via the blanket `RunnableQueue`
   /// impl) — the `Box::new` happens internally so callers don't have to
   /// type-erase manually.
   ///
   /// ```rust,ignore
   /// let mut group = QueueGroup::default();
   /// group.push(email_queue);     // Queue<Email, ...>
   /// group.push(payment_queue);   // Queue<Payment, ...> — different generics, fine
   /// let handle = group.run_all();
   /// ```
   pub fn push(&mut self, queue: impl RunnableQueue + 'static) {
      self.queues.push(Box::new(queue));
   }

   /// Consume the group, start every queue, and return a single handle
   /// that controls all of them.
   ///
   /// Each queue is started with its own [`Queue::run`]; the returned
   /// [`QueueGroupHandle`] owns every individual [`QueueHandle`] for
   /// fan-out shutdown.
   pub fn run_all(self) -> QueueGroupHandle {
      QueueGroupHandle {
         handles: self.queues.into_iter().map(RunnableQueue::run).collect(),
      }
   }

   /// Initialize the consumer group for every queue in the bundle.
   ///
   /// Sends `XGROUP CREATE MKSTREAM` for every queue in a single Redis
   /// pipeline (one round-trip), using a clone of the first queue's
   /// shared connection. Per-command results are inspected individually:
   /// `BUSYGROUP` (group already exists) is ignored; any other error is
   /// returned as the function's `Err`.
   ///
   /// Returns `Ok(())` on an empty group.
   pub async fn init_all(&self) -> Result<(), Error> {
      let Some(first) = self.queues.first() else {
         return Ok(());
      };
      let mut conn = first.conn();

      let mut pipe = redis::pipe();
      for queue in self.queues.iter() {
         pipe.xgroup_create_mkstream(
            queue.name(),
            queue.consumer_group(),
            queue.starting_id().unwrap_or("0"),
         );
      }

      let results: Vec<RedisResult<()>> = pipe.query_async(&mut conn).await?;
      for r in results {
         match r {
            Ok(_) => {}
            Err(e) if e.code() == Some("BUSYGROUP") => {}
            Err(e) => return Err(anyhow::anyhow!(e)),
         }
      }

      Ok(())
   }
}
