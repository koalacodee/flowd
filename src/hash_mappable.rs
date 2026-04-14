pub use hash_mapper_derive::HashMapper;

pub trait HashMappable: Sized {
    /// Serialize self into a `Vec<(String, redis::Value)>`.
    /// Returns `Err` if any custom serializer fails.
    fn try_to_pairs(self) -> Result<Vec<(String, redis::Value)>, String>;

    /// Deserialize from a borrowed slice of `(String, redis::Value)` pairs.
    /// Returns `Err` if a required field is missing or fails to parse.
    fn try_from_pairs(pairs: &[(String, redis::Value)]) -> Result<Self, String>;

    /// Returns the field names baked in at compile time.
    fn fields() -> &'static [&'static str];
}
