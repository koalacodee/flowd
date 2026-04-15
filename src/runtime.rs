//! Runtime abstraction layer.
//!
//! Wraps tokio primitives behind a [`Runtime`] trait so the consumer/claimer
//! code references [`SelectedRuntime`] uniformly.

use std::sync::Arc;
use std::time::Duration;

/// Trait abstracting async runtime primitives (spawning, sleeping, semaphores,
/// task sets). Each associated type maps to the runtime's concrete type.
pub(crate) trait Runtime: 'static {
   type JoinHandle: Send;
   type Permit: Send + 'static;
   type Semaphore: Send + Sync + 'static;
   type TaskSet: Send + 'static;

   fn spawn<F: Future<Output = ()> + Send + 'static>(fut: F) -> Self::JoinHandle;
   fn join(handle: Self::JoinHandle) -> impl Future<Output = ()> + Send;
   fn sleep(duration: Duration) -> impl Future<Output = ()> + Send;

   fn new_semaphore(permits: usize) -> Arc<Self::Semaphore>;
   fn available_permits(sem: &Arc<Self::Semaphore>) -> usize;
   fn wait_for_permit(sem: &Arc<Self::Semaphore>) -> impl Future<Output = ()> + Send + '_;
   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send;

   fn new_task_set() -> Self::TaskSet;
   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F);
   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_;
}

// ── Tokio ────────────────────────────────────────────────────────────────────

pub(crate) struct TokioRuntime;

impl Runtime for TokioRuntime {
   type JoinHandle = tokio::task::JoinHandle<()>;
   type Permit = tokio::sync::OwnedSemaphorePermit;
   type Semaphore = tokio::sync::Semaphore;
   type TaskSet = tokio::task::JoinSet<()>;

   fn spawn<F: Future<Output = ()> + Send + 'static>(fut: F) -> Self::JoinHandle {
      tokio::spawn(fut)
   }

   fn join(handle: Self::JoinHandle) -> impl Future<Output = ()> + Send {
      // Await the handle, discarding the JoinError (panic in task)
      async {
         let _ = handle.await;
      }
   }

   fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
      tokio::time::sleep(duration)
   }

   fn new_semaphore(permits: usize) -> Arc<Self::Semaphore> {
      Arc::new(tokio::sync::Semaphore::new(permits))
   }

   fn available_permits(sem: &Arc<Self::Semaphore>) -> usize {
      sem.available_permits()
   }

   fn wait_for_permit(sem: &Arc<Self::Semaphore>) -> impl Future<Output = ()> + Send + '_ {
      // Acquire and immediately drop — used only to block until a slot opens
      async {
         let _ = sem.acquire().await.unwrap();
      }
   }

   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send {
      // Owned permit — kept alive for the duration of a spawned task
      async { sem.acquire_owned().await.unwrap() }
   }

   fn new_task_set() -> Self::TaskSet {
      tokio::task::JoinSet::new()
   }

   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F) {
      // JoinSet tracks the handle automatically
      set.spawn(fut);
   }

   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_ {
      async {
         // Await every remaining task, logging panics
         while let Some(result) = set.join_next().await {
            if let Err(e) = result {
               eprintln!("task failed during shutdown: {e}");
            }
         }
      }
   }
}

// ── Selected runtime ─────────────────────────────────────────────────────────

/// Type alias pointing to the tokio runtime implementation.
/// All consumer/claimer code is written against this.
pub(crate) type SelectedRuntime = TokioRuntime;
