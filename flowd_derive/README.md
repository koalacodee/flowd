# flowd_derive

Derive macro for [`flowd`](https://crates.io/crates/flowd).

**You should not depend on this crate directly.** The `Job` derive is
re-exported from `flowd`:

```rust,ignore
use flowd::prelude::*;

#[derive(Debug, Job)]
struct Email {
    to: String,
    subject: String,
}
```

See the [`flowd` documentation](https://docs.rs/flowd) for full details on
the `Job` trait, the `#[mapper(...)]` attribute, and the rest of the task
queue API.

## License

Dual-licensed under either [MIT](./LICENSE-MIT) or
[Apache-2.0](./LICENSE-APACHE), at your option.
