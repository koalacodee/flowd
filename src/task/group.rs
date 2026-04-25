use crate::{
   Job,
   task::{
      Queue, QueueHandle,
      types::{QueueGroup, QueueGroupHandle},
   },
};

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
}

impl QueueGroup {
   /// Add a queue to the group.
   ///
   /// Accepts any concrete `Queue<...>` (via the blanket `RunnableQueue`
   /// impl) — the `Box::new` happens internally so callers don't have to
   /// type-erase manually.
   ///
   /// ```rust,ignore
   /// let mut group = QueueGroup::from_queues(vec![]);
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
}
