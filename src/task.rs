use crate::HashMappable;
use anyhow::Error;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{marker::PhantomData, sync::Arc};

pub struct Task<T: HashMappable> {
   pub id: String,
   pub payload: T,
}

pub struct Queue<I: HashMappable, E, F, Fut>
where
   F: Fn(&I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
{
   pub name: String,
   consumer_group: String,
   consumer_id: String,
   block_timeout: usize,
   max_concurrent_tasks: usize,
   worker: Arc<F>,
   _marker: PhantomData<(I, Fut, E)>,
   conn: MultiplexedConnection,
}

pub struct QueueBuilder<I, E, F, Fut>
where
   I: HashMappable,
   F: Fn(&I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
{
   pub name: String,
   pub consumer_group: String,
   pub consumer_id: String,
   pub block_timeout: usize,
   pub max_concurrent_tasks: usize,
   pub worker: Arc<F>,
   pub conn: MultiplexedConnection,
   _marker: PhantomData<(I, Fut, E)>,
}

/// Handle returned by `Queue::run()`. Call `.shutdown()` to gracefully stop
/// the consumer loop and wait for all in-flight tasks to finish.
pub struct QueueHandle {
   shutdown: Arc<AtomicBool>,
   #[cfg(feature = "tokio")]
   join: tokio::task::JoinHandle<()>,
   #[cfg(feature = "async-std")]
   join: async_std::task::JoinHandle<()>,
}

impl QueueHandle {
   /// Signal the consumer loop to stop reading new messages, then wait
   /// for all in-flight tasks to complete before returning.
   pub async fn shutdown(self) {
      self.shutdown.store(true, Ordering::Relaxed);
      #[cfg(feature = "tokio")]
      {
         let _ = self.join.await;
      }
      #[cfg(feature = "async-std")]
      {
         self.join.await;
      }
   }
}

impl<I, E, F, Fut> Queue<I, E, F, Fut>
where
   I: HashMappable + Send,
   F: Fn(&I) -> Fut + 'static + Send + Sync,
   E: std::fmt::Display + Send + 'static,
   Fut: Future<Output = Result<(), E>> + Send,
{
   pub fn new(builder: QueueBuilder<I, E, F, Fut>) -> Self {
      Queue {
         name: builder.name,
         consumer_group: builder.consumer_group,
         consumer_id: builder.consumer_id,
         block_timeout: builder.block_timeout,
         max_concurrent_tasks: builder.max_concurrent_tasks,
         worker: builder.worker,
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
      let shutdown_flag = Arc::clone(&shutdown);

      let name = Arc::new(self.name);
      let consumer_group = Arc::new(self.consumer_group);
      let consumer_id = Arc::new(self.consumer_id);
      let block_timeout = self.block_timeout;
      let max_concurrent_tasks = self.max_concurrent_tasks;
      let worker = self.worker;
      let conn = self.conn;

      #[cfg(feature = "tokio")]
      let join = {
         let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_tasks));

         tokio::spawn(async move {
            use redis::streams::{StreamReadOptions, StreamReadReply};
            use tokio::task::JoinSet;

            let mut set: JoinSet<Result<(), Error>> = JoinSet::new();

            loop {
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               let available = semaphore.available_permits();
               if available == 0 {
                  // All slots full — wait for at least one to free up
                  let _permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                  // just needed to wait, free the slot back
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
                        let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                        let mut conn = conn.clone();
                        let name = Arc::clone(&name);
                        let consumer_group = Arc::clone(&consumer_group);
                        let worker = Arc::clone(&worker);

                        set.spawn(async move {
                           let _permit = permit; // dropped when task finishes

                           let pairs: Vec<(String, redis::Value)> =
                              message.map.into_iter().collect();

                           let input = I::try_from_pairs(&pairs).map_err(|e| anyhow::anyhow!(e))?;

                           let handler_result = (worker)(&input).await;

                           match handler_result {
                              Ok(_) => {
                                 conn
                                    .xack(name.as_str(), consumer_group.as_str(), &[&message.id])
                                    .await?;
                              }
                              Err(e) => {
                                 eprintln!("worker failed: {e}");
                              }
                           }

                           Ok(())
                        });
                     }
                  }
               }
            }

            // Drain all in-flight tasks before exiting
            while let Some(result) = set.join_next().await {
               if let Err(e) = result {
                  eprintln!("task failed during shutdown: {e}");
               }
            }
         })
      };

      #[cfg(feature = "async-std")]
      let join = {
         let semaphore = Arc::new(mea::semaphore::Semaphore::new(max_concurrent_tasks));

         async_std::task::spawn(async move {
            use futures::stream::{FuturesUnordered, StreamExt};
            use redis::streams::{StreamReadOptions, StreamReadReply};

            let mut tasks: FuturesUnordered<async_std::task::JoinHandle<()>> =
               FuturesUnordered::new();

            loop {
               if shutdown_flag.load(Ordering::Relaxed) {
                  break;
               }

               let available = semaphore.available_permits();
               if available == 0 {
                  // All slots full — wait for at least one to free up
                  let _permit = semaphore.acquire(1).await;
                  // just needed to wait, free the slot back
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
                        let permit = Arc::clone(&semaphore).acquire_owned(1).await;
                        let mut conn = conn.clone();
                        let name = Arc::clone(&name);
                        let consumer_group = Arc::clone(&consumer_group);
                        let worker = Arc::clone(&worker);

                        tasks.push(async_std::task::spawn(async move {
                           let _permit = permit; // dropped when task finishes

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
                                    .xack(name.as_str(), consumer_group.as_str(), &[&message.id])
                                    .await
                                 {
                                    eprintln!("failed to ack message: {e}");
                                 }
                              }
                              Err(e) => {
                                 eprintln!("worker failed: {e}");
                              }
                           }
                        }));
                     }
                  }
               }
            }

            // Drain all in-flight tasks before exiting
            while tasks.next().await.is_some() {}
         })
      };

      QueueHandle { shutdown, join }
   }
}
