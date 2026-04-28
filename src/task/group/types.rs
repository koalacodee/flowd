use super::RunnableQueue;
use crate::task::QueueHandle;

/// A bundle of heterogeneous [`Queue`](crate::task::Queue)s started and shut
/// down as a unit.
///
/// Unlike a single `Queue<...>`, the group can hold queues with different
/// payload, worker, and DLQ types. Type erasure is handled internally so
/// callers never have to write `Box<dyn ...>` themselves.
///
/// Add queues with [`QueueGroup::push`], then call
/// [`QueueGroup::run_all`] to start them all and obtain a
/// [`QueueGroupHandle`] for coordinated shutdown.
///
/// # Example
///
/// ```rust,ignore
/// let mut group = QueueGroup::default();
/// group.push(email_queue);
/// group.push(payment_queue);
/// let handle = group.run_all();
/// // ... later ...
/// handle.shutdown().await;
/// ```
#[derive(Default)]
pub struct QueueGroup {
   pub(super) queues: Vec<Box<dyn RunnableQueue>>,
}

/// Handle returned by [`QueueGroup::run_all`].
///
/// Owns a [`QueueHandle`] for every queue in the group. Calling
/// [`QueueGroupHandle::shutdown`] signals every queue to stop accepting
/// new messages and waits for all in-flight tasks across the group to
/// drain.
pub struct QueueGroupHandle {
   pub(super) handles: Vec<QueueHandle>,
}
