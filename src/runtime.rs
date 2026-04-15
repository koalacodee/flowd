//! Runtime abstraction layer.
//!
//! Wraps runtime-specific primitives behind a [`Runtime`] trait so the
//! consumer/claimer code references [`SelectedRuntime`] uniformly.
//!
//! Exactly one of the `tokio` or `smol` features must be enabled (enforced
//! by `compile_error!` in the crate root). The [`SelectedRuntime`] type alias
//! resolves to whichever implementation is active.

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

#[cfg(feature = "tokio")]
pub(crate) struct TokioRuntime;

#[cfg(feature = "tokio")]
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

// ── Smol ─────────────────────────────────────────────────────────────────────

#[cfg(feature = "smol")]
pub(crate) struct SmolRuntime;

#[cfg(feature = "smol")]
impl Runtime for SmolRuntime {
   type JoinHandle = smol::Task<()>;
   type Permit = mea::semaphore::OwnedSemaphorePermit;
   type Semaphore = mea::semaphore::Semaphore;
   type TaskSet = futures::stream::FuturesUnordered<smol::Task<()>>;

   fn spawn<F: Future<Output = ()> + Send + 'static>(fut: F) -> Self::JoinHandle {
      smol::spawn(fut)
   }

   fn join(handle: Self::JoinHandle) -> impl Future<Output = ()> + Send {
      // smol::Task implements Future directly
      async {
         handle.await;
      }
   }

   fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
      async move { let _ = smol::Timer::after(duration).await; }
   }

   fn new_semaphore(permits: usize) -> Arc<Self::Semaphore> {
      Arc::new(mea::semaphore::Semaphore::new(permits))
   }

   fn available_permits(sem: &Arc<Self::Semaphore>) -> usize {
      sem.available_permits()
   }

   fn wait_for_permit(sem: &Arc<Self::Semaphore>) -> impl Future<Output = ()> + Send + '_ {
      // Acquire and immediately drop — used only to block until a slot opens
      async {
         let _ = sem.acquire(1).await;
      }
   }

   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send {
      // Owned permit — kept alive for the duration of a spawned task
      async { sem.acquire_owned(1).await }
   }

   fn new_task_set() -> Self::TaskSet {
      futures::stream::FuturesUnordered::new()
   }

   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F) {
      use futures::stream::FuturesUnordered;
      // Spawn on the smol executor then push the handle into the set
      FuturesUnordered::push(set, smol::spawn(fut));
   }

   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_ {
      use futures::StreamExt;
      async {
         // Drive all remaining tasks to completion
         while set.next().await.is_some() {}
      }
   }
}

// ── Selected runtime ─────────────────────────────────────────────────────────

/// Type alias pointing to the active runtime implementation.
/// All consumer/claimer code is written against this.
#[cfg(feature = "tokio")]
pub(crate) type SelectedRuntime = TokioRuntime;

#[cfg(feature = "smol")]
pub(crate) type SelectedRuntime = SmolRuntime;
