use crate::HashMappable;
use crate::runtime::{Runtime, SelectedRuntime};
use anyhow::Error;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{marker::PhantomData, sync::Arc};

pub struct Task<T: HashMappable> {
   pub id: String,
   pub payload: T,
}

pub struct Claimer<I: HashMappable, DE, DF, DFut>
where
   DF: Fn(&I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
   pub min_idle_time: usize,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub max_retries: usize,
   pub dlq_worker: Option<Arc<DF>>,
   _marker: PhantomData<(I, DE, DFut)>,
}

pub struct ClaimerBuilder<I: HashMappable, DE, DF, DFut>
where
   DF: Fn(&I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub min_idle_time: usize,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub max_retries: usize,
   pub dlq_worker: Option<Arc<DF>>,
   _marker: PhantomData<(I, DE, DFut)>,
}

pub struct Queue<I: HashMappable, E, F, Fut, DE, DF, DFut>
where
   F: Fn(&I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
   DF: Fn(&I, usize) -> DFut,
   DE: std::fmt::Display,
   DFut: Future<Output = Result<(), DE>>,
{
   pub name: String,
   consumer_group: String,
   consumer_id: String,
   block_timeout: usize,
   max_concurrent_tasks: usize,
   worker: Arc<F>,
   claimer: Option<Claimer<I, DE, DF, DFut>>,
   _marker: PhantomData<(I, Fut, E)>,
   conn: MultiplexedConnection,
}

pub struct QueueBuilder<I, E, F, Fut, DE, DF, DFut>
where
   I: HashMappable,
   F: Fn(&I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(&I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub name: String,
   pub consumer_group: String,
   pub consumer_id: String,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub worker: Arc<F>,
   pub claimer: Option<Claimer<I, DE, DF, DFut>>,
   pub conn: MultiplexedConnection,
   _marker: PhantomData<(I, Fut, E)>,
}

/// Handle returned by `Queue::run()`. Call `.shutdown()` to gracefully stop
/// the consumer and claimer loops, waiting for all in-flight tasks to finish.
pub struct QueueHandle {
   shutdown: Arc<AtomicBool>,
   main_join: <SelectedRuntime as Runtime>::JoinHandle,
   claimer_join: Option<<SelectedRuntime as Runtime>::JoinHandle>,
}

impl QueueHandle {
   /// Signal both loops to stop reading new messages, then wait
   /// for all in-flight tasks to complete before returning.
   pub async fn shutdown(self) {
      self.shutdown.store(true, Ordering::Relaxed);
      SelectedRuntime::join(self.main_join).await;
      if let Some(claimer_join) = self.claimer_join {
         SelectedRuntime::join(claimer_join).await;
      }
   }
}

impl<I, E, F, Fut, DE, DF, DFut> Queue<I, E, F, Fut, DE, DF, DFut>
where
   I: HashMappable + Send + 'static,
   F: Fn(&I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
   DF: Fn(&I, usize) -> DFut + 'static + Send + Sync,
   DE: std::fmt::Display + Send + 'static,
   DFut: Future<Output = Result<(), DE>> + Send,
{
   pub fn new(builder: QueueBuilder<I, E, F, Fut, DE, DF, DFut>) -> Self {
      Queue {
         name: builder.name,
         consumer_group: builder.consumer_group,
         consumer_id: builder.consumer_id,
         block_timeout: builder.block_timeout,
         max_concurrent_tasks: builder.max_concurrent_tasks,
         worker: builder.worker,
         claimer: builder.claimer,
         conn: builder.conn,
         _marker: builder._marker,
      }
   }

   pub async fn init(&self, starting_id: Option<String>) -> Result<(), Error> {
      let mut conn = self.conn.clone();
      let _: () = match conn
         .xgroup_create_mkstream(
            &self.name,
            &self.consumer_group,
            starting_id.unwrap_or("0".to_string()),
         )
         .await
      {
         Ok(_) => (),
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

   pub async fn enqueue(&mut self, task: Task<I>) -> Result<(), Error> {
      let pairs = task
         .payload
         .try_to_pairs()
         .map_err(|e| anyhow::anyhow!(e))?;
      let items: Vec<(&str, Vec<u8>)> = pairs
         .iter()
         .filter_map(|(k, v)| match v {
            redis::Value::BulkString(bytes) => Some((k.as_str(), bytes.clone())),
            _ => None,
         })
         .collect();

      self.conn.xadd(&self.name, &task.id, &items).await?;
      Ok(())
   }

   pub fn run(self) -> QueueHandle {
      let shutdown = Arc::new(AtomicBool::new(false));

      let name = Arc::new(self.name);
      let consumer_group = Arc::new(self.consumer_group);
      let consumer_id = Arc::new(self.consumer_id);
      let worker = self.worker;
      let conn = self.conn;

      // ── Main consumer loop ────────────────────────────────────────────────
      let main_join = {
         let shutdown_flag = Arc::clone(&shutdown);
         let semaphore = SelectedRuntime::new_semaphore(self.max_concurrent_tasks);
         let name = Arc::clone(&name);
         let consumer_group = Arc::clone(&consumer_group);
         let consumer_id = Arc::clone(&consumer_id);
         let worker = Arc::clone(&worker);
         let conn = conn.clone();
         let block_timeout = self.block_timeout;

         SelectedRuntime::spawn(async move {
            use redis::streams::{StreamReadOptions, StreamReadReply};

            let mut set = SelectedRuntime::new_task_set();

            loop {
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               let available = SelectedRuntime::available_permits(&semaphore);
               if available == 0 {
                  SelectedRuntime::wait_for_permit(&semaphore).await;
                  continue;
               }

               let mut read_conn = conn.clone();
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
                     eprintln!("failed to read from stream: {e}");
                     continue;
                  }
               };

               if let Some(reply) = reply {
                  for stream_key in reply.keys {
                     for message in stream_key.ids {
                        let permit =
                           SelectedRuntime::acquire_permit(Arc::clone(&semaphore)).await;
                        let mut conn = conn.clone();
                        let name = Arc::clone(&name);
                        let consumer_group = Arc::clone(&consumer_group);
                        let worker = Arc::clone(&worker);

                        SelectedRuntime::spawn_task(&mut set, async move {
                           let _permit = permit;

                           let pairs: Vec<(String, redis::Value)> =
                              message.map.into_iter().collect();

                           let input = match I::try_from_pairs(&pairs) {
                              Ok(v) => v,
                              Err(e) => {
                                 eprintln!("failed to parse task: {e}");
                                 return;
                              }
                           };

                           let handler_result = (worker)(&input).await;

                           match handler_result {
                              Ok(_) => {
                                 if let Err(e) = conn
                                    .xack(
                                       name.as_str(),
                                       consumer_group.as_str(),
                                       &[&message.id],
                                    )
                                    .await
                                 {
                                    eprintln!("failed to ack message: {e}");
                                 }
                              }
                              Err(e) => {
                                 eprintln!("worker failed: {e}");
                              }
                           }
                        });
                     }
                  }
               }
            }

            SelectedRuntime::drain_task_set(&mut set).await;
         })
      };

      // ── Claimer loop ──────────────────────────────────────────────────────
      let claimer_join = if let Some(claimer) = self.claimer {
         let dlq_worker = claimer.dlq_worker;
         let max_retries = claimer.max_retries;
         let claimer_block_timeout = claimer.block_timeout;
         let min_idle_time = claimer.min_idle_time;

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
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               let available = SelectedRuntime::available_permits(&semaphore);
               if available == 0 {
                  SelectedRuntime::wait_for_permit(&semaphore).await;
                  continue;
               }

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
                     SelectedRuntime::sleep(std::time::Duration::from_millis(
                        claimer_block_timeout as u64,
                     ))
                     .await;
                     continue;
                  }
               };

               if reply.claimed.is_empty() {
                  SelectedRuntime::sleep(std::time::Duration::from_millis(
                     claimer_block_timeout as u64,
                  ))
                  .await;
                  continue;
               }

               // Get delivery counts for claimed messages
               let claimed_ids: Vec<&str> =
                  reply.claimed.iter().map(|m| m.id.as_str()).collect();
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

               // Build a map of id → times_delivered
               let delivery_counts: std::collections::HashMap<&str, usize> = pending
                  .ids
                  .iter()
                  .map(|p| (p.id.as_str(), p.times_delivered))
                  .collect();

               for message in reply.claimed {
                  let times_delivered =
                     delivery_counts.get(message.id.as_str()).copied().unwrap_or(1);

                  let permit =
                     SelectedRuntime::acquire_permit(Arc::clone(&semaphore)).await;
                  let mut conn = conn.clone();
                  let name = Arc::clone(&name);
                  let consumer_group = Arc::clone(&consumer_group);
                  let worker = Arc::clone(&worker);
                  let dlq_worker = dlq_worker.clone();

                  SelectedRuntime::spawn_task(&mut set, async move {
                     let _permit = permit;

                     let pairs: Vec<(String, redis::Value)> =
                        message.map.into_iter().collect();

                     let input = match I::try_from_pairs(&pairs) {
                        Ok(v) => v,
                        Err(e) => {
                           eprintln!("failed to parse task: {e}");
                           return;
                        }
                     };

                     if times_delivered > max_retries {
                        // Max retries exceeded — send to DLQ if configured
                        if let Some(dlq) = &dlq_worker {
                           if let Err(e) = (dlq)(&input, times_delivered).await {
                              eprintln!("dlq worker failed: {e}");
                           }
                        }
                        // ACK to remove from PEL regardless
                        if let Err(e) = conn
                           .xack(
                              name.as_str(),
                              consumer_group.as_str(),
                              &[&message.id],
                           )
                           .await
                        {
                           eprintln!("failed to ack dead-lettered message: {e}");
                        }
                        return;
                     }

                     // Normal retry
                     let handler_result = (worker)(&input).await;

                     match handler_result {
                        Ok(_) => {
                           if let Err(e) = conn
                              .xack(
                                 name.as_str(),
                                 consumer_group.as_str(),
                                 &[&message.id],
                              )
                              .await
                           {
                              eprintln!("failed to ack message: {e}");
                           }
                        }
                        Err(e) => {
                           eprintln!("worker failed (retry {}): {e}", times_delivered);
                        }
                     }
                  });
               }
            }

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
