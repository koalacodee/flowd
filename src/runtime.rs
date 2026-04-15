use std::sync::Arc;
use std::time::Duration;

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
      async { let _ = sem.acquire().await.unwrap(); }
   }

   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send {
      async { sem.acquire_owned().await.unwrap() }
   }

   fn new_task_set() -> Self::TaskSet {
      tokio::task::JoinSet::new()
   }

   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F) {
      set.spawn(fut);
   }

   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_ {
      async {
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
      async { let _ = sem.acquire(1).await; }
   }

   fn acquire_permit(sem: Arc<Self::Semaphore>) -> impl Future<Output = Self::Permit> + Send {
      async { sem.acquire_owned(1).await }
   }

   fn new_task_set() -> Self::TaskSet {
      futures::stream::FuturesUnordered::new()
   }

   fn spawn_task<F: Future<Output = ()> + Send + 'static>(set: &mut Self::TaskSet, fut: F) {
      use futures::stream::FuturesUnordered;
      FuturesUnordered::push(set, async_std::task::spawn(fut));
   }

   fn drain_task_set(set: &mut Self::TaskSet) -> impl Future<Output = ()> + Send + '_ {
      async {
         use futures::stream::StreamExt;
         while set.next().await.is_some() {}
      }
   }
}

// ── Selected runtime ─────────────────────────────────────────────────────────

#[cfg(feature = "tokio")]
pub(crate) type SelectedRuntime = TokioRuntime;

#[cfg(feature = "async-std")]
pub(crate) type SelectedRuntime = AsyncStdRuntime;
