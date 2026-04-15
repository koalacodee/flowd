use crate::HashMappable;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};

pub(super) fn parse_message<I: HashMappable>(
   map: impl IntoIterator<Item = (String, redis::Value)>,
) -> Option<I> {
   let pairs: Vec<(String, redis::Value)> = map.into_iter().collect();
   match I::try_from_pairs(&pairs) {
      Ok(v) => Some(v),
      Err(e) => {
         eprintln!("failed to parse task: {e}");
         None
      }
   }
}

pub(super) async fn ack(
   conn: &mut MultiplexedConnection,
   stream_name: &str,
   consumer_group: &str,
   message_id: &str,
) {
   if let Err(e) = conn.xack(stream_name, consumer_group, &[message_id]).await {
      eprintln!("failed to ack message: {e}");
   }
}

pub(super) async fn process_and_ack<I, E, F, Fut>(
   input: &I,
   worker: &F,
   conn: &mut MultiplexedConnection,
   stream_name: &str,
   consumer_group: &str,
   message_id: &str,
) where
   F: Fn(&I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
{
   match (worker)(input).await {
      Ok(_) => ack(conn, stream_name, consumer_group, message_id).await,
      Err(e) => eprintln!("worker failed: {e}"),
   }
}
