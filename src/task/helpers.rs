use crate::Job;
use redis::{AsyncTypedCommands, aio::MultiplexedConnection};

// Collect stream message fields into pairs and deserialize into the
// payload struct. Returns None (with a log) if deserialization fails.
pub(super) fn parse_message<I: Job>(
   map: impl IntoIterator<Item = (String, redis::Value)>,
) -> Option<I> {
   // Collect the HashMap from the StreamId into a Vec for try_from_pairs
   let pairs: Vec<(String, redis::Value)> = map.into_iter().collect();
   match I::try_from_pairs(&pairs) {
      Ok(v) => Some(v),
      Err(e) => {
         eprintln!("failed to parse task: {e}");
         None
      }
   }
}

// Send XACK for a single message, logging on failure rather than
// propagating — the message stays in the PEL for the claimer to retry.
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

// Run the worker on a parsed payload, then XACK the message on success.
// On worker failure the message is left unacknowledged in the PEL so
// the claimer can retry it later.
pub(super) async fn process_and_ack<I, E, F, Fut>(
   input: I,
   worker: &F,
   conn: &mut MultiplexedConnection,
   stream_name: &str,
   consumer_group: &str,
   message_id: &str,
) where
   F: Fn(I) -> Fut,
   E: std::fmt::Display,
   Fut: Future<Output = Result<(), E>>,
{
   match (worker)(input).await {
      // Success — acknowledge so the message leaves the PEL
      Ok(_) => ack(conn, stream_name, consumer_group, message_id).await,
      // Failure — leave unacknowledged for the claimer to pick up
      Err(e) => eprintln!("worker failed: {e}"),
   }
}
