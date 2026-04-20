use super::helpers::{ack, parse_message, process_and_ack};
use super::types::{ClaimerBuilder, Queue, QueueBuilder, QueueHandle, Task};
use crate::Job;
use crate::runtime::{Runtime, SelectedRuntime};
use crate::task::Claimer;
use anyhow::Error;
use redis::AsyncTypedCommands;
use redis::aio::MultiplexedConnection;
use std::future::Ready;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

// ── Default types for a QueueBuilder with no claimer ─────────────────────────
//
// When no claimer is configured, the `DE/DF/DFut` generics on QueueBuilder
// are unused but still need concrete types. These aliases fill them with
// no-op placeholders so `QueueBuilder::new()` has a single, inferrable
// return type.

/// Placeholder DLQ error type used when no claimer is configured.
pub type NoDlqError = String;
/// Placeholder DLQ future type used when no claimer is configured.
pub type NoDlqFut = Ready<Result<(), NoDlqError>>;
/// Placeholder DLQ function pointer used when no claimer is configured.
pub type NoDlqFn<I> = fn(I, usize) -> NoDlqFut;

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

impl<I, E, F, Fut, DE, DF, DFut> Queue<I, E, F, Fut, DE, DF, DFut>
where
   I: Job + Send + Sync + 'static,
   F: Fn(I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   /// Construct a new `Queue` from a [`QueueBuilder`].
   pub fn new(builder: QueueBuilder<I, E, F, Fut, DE, DF, DFut>) -> Self {
      Queue {
         name: builder.name,
         consumer_group: builder.consumer_group,
         consumer_id: builder.consumer_id,
         block_timeout: builder.block_timeout,
         max_concurrent_tasks: builder.max_concurrent_tasks,
         worker: builder.worker,
         claimer: match builder.claimer {
            Some(c) => Some(Claimer {
               min_idle_time: c.min_idle_time,
               block_timeout: c.block_timeout,
               max_concurrent_tasks: c.max_concurrent_tasks,
               max_retries: c.max_retries,
               dlq_worker: c.dlq_worker,
               _marker: Default::default(),
            }),
            None => None,
         },
         conn: builder.conn,
         read_conn: builder.read_conn,
         _marker: builder._marker,
      }
   }

   /// Create the consumer group on the stream (via `XGROUP CREATE`).
   ///
   /// Safe to call multiple times — silently ignores the `BUSYGROUP` error
   /// if the group already exists.
   ///
   /// `starting_id` controls where the group begins reading. Pass `None`
   /// to start from the beginning (`"0"`), or `Some("$".into())` to only
   /// see new messages.
   pub async fn init(&self, starting_id: Option<String>) -> Result<(), Error> {
      let mut conn = self.conn.clone();

      // Create the consumer group (and the stream itself if it doesn't exist)
      let _: () = match conn
         .xgroup_create_mkstream(
            &self.name,
            &self.consumer_group,
            starting_id.unwrap_or("0".to_string()),
         )
         .await
      {
         Ok(_) => (),
         // BUSYGROUP means the group already exists — safe to ignore
         Err(e) => {
            if e.to_string().contains("BUSYGROUP") {
               ()
            } else {
               return Err(anyhow::anyhow!(e));
            }
         }
      };
      Ok(())
   }

   /// Publish a [`Task`] to the stream via `XADD`.
   ///
   /// The task's payload is serialized to pairs via [`Job::try_to_pairs()`],
   /// and each `BulkString` value is written as a raw field.
   pub async fn enqueue(&mut self, task: Task<I>) -> Result<(), Error> {
      // Serialize the payload struct into (field_name, redis::Value) pairs
      let pairs = task
         .payload
         .try_to_pairs()
         .map_err(|e| anyhow::anyhow!(e))?;

      // Extract raw bytes from each BulkString value for XADD
      let items: Vec<(&str, Vec<u8>)> = pairs
         .iter()
         .filter_map(|(k, v)| match v {
            redis::Value::BulkString(bytes) => Some((k.as_str(), bytes.clone())),
            _ => None,
         })
         .collect();

      // Append the message to the stream
      self.conn.xadd(&self.name, &task.id, &items).await?;
      Ok(())
   }

   /// Publish multiple [`Task`]s to the stream in a single Redis pipeline.
   ///
   /// Each task is serialized and added via `XADD`. The entire batch is sent
   /// as one round-trip, making this significantly more efficient than
   /// calling [`enqueue()`](Self::enqueue) in a loop.
   pub async fn enqueue_bulk(&mut self, tasks: Vec<Task<I>>) -> Result<(), Error> {
      // Pre-serialize all tasks so we can reference their data when building
      // the pipeline commands
      let serialized: Vec<(String, Vec<(String, Vec<u8>)>)> = tasks
         .into_iter()
         .map(|task| {
            let pairs = task
               .payload
               .try_to_pairs()
               .map_err(|e| anyhow::anyhow!(e))?;

            let items: Vec<(String, Vec<u8>)> = pairs
               .into_iter()
               .filter_map(|(k, v)| match v {
                  redis::Value::BulkString(bytes) => Some((k, bytes)),
                  _ => None,
               })
               .collect();

            Ok((task.id, items))
         })
         .collect::<Result<_, Error>>()?;

      let mut pipe = redis::pipe();
      for (id, items) in &serialized {
         let refs: Vec<(&str, &[u8])> = items
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
         pipe.xadd(&self.name, id.as_str(), &refs);
      }

      // Send all XADDs in a single round-trip
      pipe.query_async::<()>(&mut self.conn).await?;
      Ok(())
   }

   /// Consume the `Queue` and spawn the consumer (and optional claimer) loops.
   ///
   /// Returns a [`QueueHandle`] that can be used to trigger graceful
   /// shutdown. The consumer loop:
   ///
   /// 1. Checks the shutdown flag each iteration.
   /// 2. Queries available semaphore permits to determine `COUNT`.
   /// 3. Calls `XREADGROUP` with blocking.
   /// 4. For each received message, acquires a permit and spawns a task
   ///    that parses the payload, calls the worker, and `XACK`s on success.
   /// 5. On shutdown, drains all in-flight tasks before returning.
   ///
   /// If a [`Claimer`] is configured, a second loop is spawned that uses
   /// `XAUTOCLAIM` to reclaim idle messages and retries or dead-letters them.
   pub fn run(self) -> QueueHandle {
      let shutdown = Arc::new(AtomicBool::new(false));

      // Wrap shared state in Arcs so both loops and their spawned tasks
      // can reference them without lifetime issues
      let name = Arc::new(self.name);
      let consumer_group = Arc::new(self.consumer_group);
      let consumer_id = Arc::new(self.consumer_id);
      let worker = self.worker;
      let conn = self.conn;
      // Dedicated connection for the blocking XREADGROUP — kept separate
      // from `conn` so that blocking the reader does not stall XACKs and
      // other non-blocking ops queued on the multiplexer.
      let read_conn = self.read_conn;

      // ── Main consumer loop ────────────────────────────────────────────────
      let main_join = {
         // Clone Arcs for the spawned loop task to own
         let shutdown_flag = Arc::clone(&shutdown);
         let semaphore = SelectedRuntime::new_semaphore(self.max_concurrent_tasks);
         let name = Arc::clone(&name);
         let consumer_group = Arc::clone(&consumer_group);
         let consumer_id = Arc::clone(&consumer_id);
         let worker = Arc::clone(&worker);
         let conn = conn.clone();
         let mut read_conn = read_conn;
         let block_timeout = self.block_timeout;

         SelectedRuntime::spawn(async move {
            use redis::streams::{StreamReadOptions, StreamReadReply};

            let mut set = SelectedRuntime::new_task_set();

            loop {
               // Step 1: Check if shutdown was requested
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               // Step 2: Check how many tasks we can accept right now
               let available = SelectedRuntime::available_permits(&semaphore);
               if available == 0 {
                  // All permits in use — block until one frees up, then re-check
                  SelectedRuntime::wait_for_permit(&semaphore).await;
                  continue;
               }

               // Step 3: XREADGROUP — read up to `available` new messages,
               // blocking for `block_timeout` ms if none are ready. Uses the
               // dedicated `read_conn` so this block does not stall XACKs.
               let opts = StreamReadOptions::default()
                  .count(available)
                  .block(block_timeout)
                  .group(consumer_group.as_str(), consumer_id.as_str());

               let reply: Option<StreamReadReply> = match read_conn
                  .xread_options(&[name.as_str()], &[">"], &opts)
                  .await
               {
                  Ok(r) => r,
                  Err(e) => {
                     // An empty stream surfaces the BLOCK timeout as an Err;
                     // that's expected, not a real failure, so swallow it.
                     if !e.is_timeout() {
                        eprintln!("failed to read from stream: {e}");
                     }
                     continue;
                  }
               };

               // Step 4: Dispatch each message as a concurrent task
               if let Some(reply) = reply {
                  for stream_key in reply.keys {
                     for message in stream_key.ids {
                        // Acquire a semaphore permit before spawning — this
                        // reserves a concurrency slot for the task
                        let permit = SelectedRuntime::acquire_permit(Arc::clone(&semaphore)).await;
                        let mut conn = conn.clone();
                        let name = Arc::clone(&name);
                        let consumer_group = Arc::clone(&consumer_group);
                        let worker = Arc::clone(&worker);

                        SelectedRuntime::spawn_task(&mut set, async move {
                           // Hold the permit until the task finishes
                           let _permit = permit;

                           // Parse stream fields into the payload struct
                           let Some(input) = parse_message::<I>(message.map) else {
                              return;
                           };

                           // Run the worker and XACK on success
                           process_and_ack(
                              input,
                              worker.as_ref(),
                              &mut conn,
                              name.as_str(),
                              consumer_group.as_str(),
                              &message.id,
                           )
                           .await;
                        });
                     }
                  }
               }
            }

            // Step 5: Shutdown — wait for all in-flight tasks to finish
            SelectedRuntime::drain_task_set(&mut set).await;
         })
      };

      // ── Claimer loop ──────────────────────────────────────────────────────
      let claimer_join = if let Some(claimer) = self.claimer {
         let dlq_worker = claimer.dlq_worker;
         let max_retries = claimer.max_retries;
         let claimer_block_timeout = claimer.block_timeout;
         let min_idle_time = claimer.min_idle_time;

         // Clone Arcs for the claimer's own spawned loop
         let shutdown_flag = Arc::clone(&shutdown);
         let semaphore = SelectedRuntime::new_semaphore(claimer.max_concurrent_tasks);
         let name = Arc::clone(&name);
         let consumer_group = Arc::clone(&consumer_group);
         let consumer_id = Arc::clone(&consumer_id);
         let worker = Arc::clone(&worker);
         let dlq_worker = dlq_worker.as_ref().map(Arc::clone);
         let conn = conn.clone();

         Some(SelectedRuntime::spawn(async move {
            use redis::streams::{StreamAutoClaimOptions, StreamAutoClaimReply};

            let mut set = SelectedRuntime::new_task_set();

            loop {
               // Step 1: Check if shutdown was requested
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               // Step 2: Backpressure — wait if all concurrency slots are in use
               let available = SelectedRuntime::available_permits(&semaphore);
               if available == 0 {
                  SelectedRuntime::wait_for_permit(&semaphore).await;
                  continue;
               }

               // Step 3: XAUTOCLAIM — try to claim messages idle for
               // longer than `min_idle_time`, starting from ID "0-0"
               let mut claim_conn = conn.clone();
               let opts = StreamAutoClaimOptions::default().count(available);

               let reply: StreamAutoClaimReply = match claim_conn
                  .xautoclaim_options(
                     name.as_str(),
                     consumer_group.as_str(),
                     consumer_id.as_str(),
                     min_idle_time,
                     "0-0",
                     opts,
                  )
                  .await
               {
                  Ok(r) => r,
                  Err(e) => {
                     eprintln!("failed to autoclaim from stream: {e}");
                     // Back off before retrying
                     SelectedRuntime::sleep(std::time::Duration::from_millis(
                        claimer_block_timeout as u64,
                     ))
                     .await;
                     continue;
                  }
               };

               // Nothing to claim — sleep before polling again
               if reply.claimed.is_empty() {
                  SelectedRuntime::sleep(std::time::Duration::from_millis(
                     claimer_block_timeout as u64,
                  ))
                  .await;
                  continue;
               }

               // Step 4: XPENDING — fetch delivery counts for the claimed
               // range so we know how many times each message was attempted
               let claimed_ids: Vec<&str> = reply.claimed.iter().map(|m| m.id.as_str()).collect();
               let first_id = claimed_ids.first().unwrap().to_string();
               let last_id = claimed_ids.last().unwrap().to_string();

               let mut pending_conn = conn.clone();
               let pending: redis::streams::StreamPendingCountReply = match pending_conn
                  .xpending_count(
                     name.as_str(),
                     consumer_group.as_str(),
                     &first_id,
                     &last_id,
                     reply.claimed.len(),
                  )
                  .await
               {
                  Ok(p) => p,
                  Err(e) => {
                     eprintln!("failed to get pending info: {e}");
                     continue;
                  }
               };

               // Build id → delivery count lookup for O(1) access per message
               let delivery_counts: std::collections::HashMap<&str, usize> = pending
                  .ids
                  .iter()
                  .map(|p| (p.id.as_str(), p.times_delivered))
                  .collect();

               // Step 5: Dispatch each claimed message as a concurrent task
               for message in reply.claimed {
                  let times_delivered = delivery_counts
                     .get(message.id.as_str())
                     .copied()
                     .unwrap_or(1);

                  let permit = SelectedRuntime::acquire_permit(Arc::clone(&semaphore)).await;
                  let mut conn = conn.clone();
                  let name = Arc::clone(&name);
                  let consumer_group = Arc::clone(&consumer_group);
                  let worker = Arc::clone(&worker);
                  let dlq_worker = dlq_worker.clone();

                  SelectedRuntime::spawn_task(&mut set, async move {
                     let _permit = permit;

                     // Parse stream fields into the payload struct
                     let Some(input) = parse_message::<I>(message.map) else {
                        return;
                     };

                     // Dead-letter path: too many delivery attempts
                     if times_delivered > max_retries {
                        // Invoke the DLQ callback if configured
                        if let Some(dlq) = &dlq_worker {
                           if let Err(e) = (dlq)(input, times_delivered).await {
                              eprintln!("dlq worker failed: {e}");
                           }
                        }
                        // XACK to remove from PEL regardless of DLQ outcome
                        ack(
                           &mut conn,
                           name.as_str(),
                           consumer_group.as_str(),
                           &message.id,
                        )
                        .await;
                        return;
                     }

                     // Normal retry path: run the worker, XACK on success
                     process_and_ack(
                        input,
                        worker.as_ref(),
                        &mut conn,
                        name.as_str(),
                        consumer_group.as_str(),
                        &message.id,
                     )
                     .await;
                  });
               }
            }

            // Step 6: Shutdown — drain in-flight claimer tasks
            SelectedRuntime::drain_task_set(&mut set).await;
         }))
      } else {
         None
      };

      QueueHandle {
         shutdown,
         main_join,
         claimer_join,
      }
   }
}
