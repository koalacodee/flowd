pub use flowd_derive::Job;

/// Bidirectional conversion between a Rust struct and Redis-friendly
/// `Vec<(String, redis::Value)>` pairs.
///
/// Typically derived via `#[derive(Job)]` rather than implemented
/// by hand. The derive macro generates all three methods plus `From`/`TryFrom`
/// impls for seamless integration with Redis Streams (`XADD` / `XREADGROUP`).
///
/// # Example
///
/// ```rust,ignore
/// #[derive(Debug, Job)]
/// struct Email {
///     url: String,
///     priority: u8,
///     #[mapper(serialize = "ser_tags", deserialize = "de_tags")]
///     tags: Vec<String>,
/// }
///
/// let email = Email { url: "https://example.com".into(), priority: 1, tags: vec![] };
/// let pairs = email.try_to_pairs().unwrap();
/// let restored = Email::try_from_pairs(&pairs).unwrap();
/// ```
pub trait Job: Sized {
    /// Serialize `self` into a list of `(field_name, redis::Value)` pairs.
    ///
    /// Each value is wrapped as `redis::Value::BulkString`. `Option<T>` fields
    /// that are `None` are omitted entirely.
    ///
    /// Returns `Err` if a custom `#[mapper(serialize = "...")]` function fails.
    fn try_to_pairs(self) -> Result<Vec<(String, redis::Value)>, String>;

    /// Reconstruct `Self` from a borrowed slice of pairs.
    ///
    /// Internally builds a `HashMap<&str, &redis::Value>` for O(1) field
    /// lookups. Missing keys cause an error for required fields and `None`
    /// for `Option<T>` fields.
    fn try_from_pairs(pairs: &[(String, redis::Value)]) -> Result<Self, String>;

    /// Returns the struct's field names, known at compile time.
    ///
    /// Useful for building Redis queries that only fetch specific fields.
    fn fields() -> &'static [&'static str];
}
