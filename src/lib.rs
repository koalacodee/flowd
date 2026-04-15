#[cfg(all(feature = "tokio", feature = "async-std"))]
compile_error!("features `tokio` and `async-std` are mutually exclusive");

#[cfg(not(any(feature = "tokio", feature = "async-std")))]
compile_error!("either feature `tokio` or `async-std` must be enabled");

pub use crate::hash_mappable::HashMappable;
use anyhow::Error;
pub use hash_mapper_derive::HashMapper;
pub mod hash_mappable;
mod runtime;
pub mod task;

// Re-export for use by the derive macro's generated code
#[doc(hidden)]
pub use redis;

// ── Generic utilities ─────────────────────────────────────────────────────────

/// Pretty-print every field of a `HashMappable` value.
pub fn debug_map<T: HashMappable + std::fmt::Debug>(value: T) -> Result<(), Error> {
    let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
    for (k, v) in &pairs {
        println!("{:>20} : {:?}", k, v);
    }
    Ok(())
}

/// Round-trip a value through pairs — useful for testing your mapper.
pub fn round_trip<T: HashMappable>(value: T) -> Result<T, Error> {
    let pairs = value.try_to_pairs().map_err(|e| anyhow::anyhow!(e))?;
    T::try_from_pairs(&pairs).map_err(|e| anyhow::anyhow!(e))
}

// ── Prelude ───────────────────────────────────────────────────────────────────

pub mod prelude {
    pub use super::{HashMappable, HashMapper, debug_map, round_trip};
}
