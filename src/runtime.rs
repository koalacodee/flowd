//! Runtime abstraction layer.
//!
//! Provides a single [`Runtime`] trait that unifies the differences between
//! `tokio` and `async-std`, so the rest of the crate can be written once
//! against [`SelectedRuntime`] without `#[cfg]` duplication.

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
      async { let _ = handle.await; }
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
      async { let _ = sem.acquire().await.unwrap(); }
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

// ── async-std ────────────────────────────────────────────────────────────────

#[cfg(feature = "async-std")]
pub(crate) struct AsyncStdRuntime;

#[cfg(feature = "async-std")]
impl Runtime for AsyncStdRuntime {
   type JoinHandle = async_std::task::JoinHandle<()>;
   type Permit = mea::semaphore::OwnedSemaphorePermit;
   type Semaphore = mea::semaphore::Semaphore;
   type TaskSet = futures::stream::FuturesUnordered<async_std::task::JoinHandle<()>>;

   fn spawn<F: Future<Output = ()> + Send + 'static>(fut: F) -> Self::JoinHandle {
      async_std::task::spawn(fut)
   }

   fn join(handle: Self::JoinHandle) -> impl Future<Output = ()> + Send {
      handle
   }

   fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
      async_std::task::sleep(duration)
   }

   fn new_semaphore(permits: usize) -> Arc<Self::Semaphore> {
      Arc::new(mea::semaphore::Semaphore::new(permits))
   }

   fn available_permits(sem: &Arc<Self::Semaphore>) -> usize {
      sem.available_permits()
   }

   fn wait_for_permit(sem: &Arc<Self::Semaphore>) -> impl Future<Output = ()> + Send + '_ {
      // Acquire 1 permit and immediately drop — blocks until a slot opens
      async { let _ = sem.acquire(1).await; }
   }

   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send {
      // Owned permit (1 slot) — kept alive for the spawned task's lifetime
      async { sem.acquire_owned(1).await }
   }

   fn new_task_set() -> Self::TaskSet {
      futures::stream::FuturesUnordered::new()
   }

   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F) {
      use futures::stream::FuturesUnordered;
      // Spawn the task and push its handle into the unordered set
      FuturesUnordered::push(set, async_std::task::spawn(fut));
   }

   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_ {
      async {
         use futures::stream::StreamExt;
         // Poll all remaining handles to completion
         while set.next().await.is_some() {}
      }
   }
}

// ── Selected runtime ─────────────────────────────────────────────────────────

/// Type alias that resolves to the runtime implementation matching the
/// active feature flag. All consumer/claimer code is generic over this.
#[cfg(feature = "tokio")]
pub(crate) type SelectedRuntime = TokioRuntime;

/// Type alias that resolves to the runtime implementation matching the
/// active feature flag. All consumer/claimer code is generic over this.
#[cfg(feature = "async-std")]
pub(crate) type SelectedRuntime = AsyncStdRuntime;
