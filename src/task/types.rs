use crate::Job;

/// A single unit of work to be enqueued into a Redis Stream.
///
/// The `id` is passed directly to `XADD` as the message ID. Use `"*"` to
/// let Redis auto-generate a timestamp-based ID.
pub struct Task<T: Job> {
   /// Stream message ID (e.g. `"*"` for auto-generated, or a specific ID).
   pub id: String,
   /// The payload to serialize into stream fields via [`Job`].
   pub payload: T,
}
