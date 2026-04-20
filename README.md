# flowd

[![Crates.io](https://img.shields.io/crates/v/flowd.svg)](https://crates.io/crates/flowd)
[![Docs.rs](https://docs.rs/flowd/badge.svg)](https://docs.rs/flowd)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A Redis Streams-backed task queue for Rust, with a derive macro for automatic
struct ↔ `redis::Value` mapping. Runs on either **tokio** or **smol**.

- Consumer groups with `XREADGROUP`, semaphore-based concurrency control, and
  graceful shutdown.
- Optional claimer that reclaims stuck messages via `XAUTOCLAIM`, tracks
  delivery counts via `XPENDING`, and routes exhausted messages to a
  dead-letter callback.
- Batch enqueue via Redis pipelining (`enqueue_bulk`).
- `#[derive(Job)]` generates all the glue between your struct and Redis Stream
  fields — no manual serialization.

## Installation

```toml
[dependencies]
flowd = { version = "0.1", features = ["tokio"] }
redis = "1"
```

Exactly one runtime feature must be enabled: `tokio` or `smol`.

## Quick start

```rust,ignore
use flowd::prelude::*;
use flowd::task::{Queue, QueueBuilder, Task};

#[derive(Debug, Job)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client    = redis::Client::open("redis://127.0.0.1:6379")?;
    let conn      = client.get_multiplexed_async_connection().await?;
    let read_conn = client.get_multiplexed_async_connection().await?;

    let queue = Queue::new(
        QueueBuilder::new(
            "emails", "senders", "sender-1",
            |email: Email| async move {
                println!("sending to {}", email.to);
                Ok::<(), String>(())
            },
            conn,
            read_conn,
        )
        .block_timeout(5000)
        .max_concurrent_tasks(10),
    );

    queue.init(None).await?;
    let handle = queue.run();

    // On SIGTERM:
    handle.shutdown().await;
    Ok(())
}
```

## Architecture

```text
 Redis Stream ──XREADGROUP──► main consumer loop ──► worker(task)
                                 │                       │
                                 │ semaphore             ▼
                                 │ backpressure     XACK on success
                                 │
 PEL (stuck) ──XAUTOCLAIM──► claimer loop ──► worker(task) or DLQ
                                 │
                            delivery count
                            via XPENDING
```

- **`Queue`** — the main consumer. Built with `QueueBuilder`, started with
  `Queue::run()`, which returns a `QueueHandle` for graceful shutdown.
- **`Claimer`** — optional reclaimer. Attached via `QueueBuilder::claimer`.
  Messages whose delivery count exceeds `max_retries` are routed to the
  optional `dlq_worker` callback and acknowledged.
- **`Task`** — a stream message ID paired with a typed payload.

## Enqueueing jobs

```rust,ignore
// One-shot
producer.enqueue(Task {
    id: "*".into(),
    payload: Email { to: "a@b.c".into(), subject: "hi".into() },
}).await?;

// Batch — single round-trip via pipelining
producer.enqueue_bulk(
    (0..1000).map(|i| Task {
        id: "*".into(),
        payload: Email { to: format!("user{i}@b.c"), subject: "hi".into() },
    }).collect(),
).await?;
```

## Dead-letter handling

```rust,ignore
use flowd::task::{ClaimerBuilder, Queue, QueueBuilder};

let claimer = ClaimerBuilder::<Email, _, _, _>::new()
    .min_idle_time(30_000)
    .max_retries(3)
    .dlq_worker(|email: Email, attempts: usize| async move {
        eprintln!("dead-lettered after {attempts}: {:?}", email);
        Ok::<(), String>(())
    });

let queue = Queue::new(
    QueueBuilder::new("emails", "senders", "sender-1", worker, conn, read_conn)
        .claimer(claimer),
);
```

## The `Job` derive

```rust,ignore
use flowd::prelude::*;

fn ser_tags(tags: &Vec<String>) -> Result<String, String> {
    Ok(tags.join(","))
}
fn de_tags(s: &str) -> Result<Vec<String>, String> {
    Ok(s.split(',').map(String::from).collect())
}

#[derive(Debug, Job)]
struct Crawl {
    url: String,
    priority: u32,
    #[mapper(serialize = "ser_tags", deserialize = "de_tags")]
    tags: Vec<String>,
    /// Optional fields are skipped on serialize when `None`.
    assigned_to: Option<String>,
}
```

- Required fields need `Display` + `FromRedisValue` (or a custom `#[mapper]`).
- `Option<T>` fields are skipped on `None` and become `None` on missing keys.

## Connection handling

`QueueBuilder::new` takes **two** `MultiplexedConnection`s:

- `conn` — for all non-blocking ops (XACK, XADD, XAUTOCLAIM, XPENDING).
  Safe to clone and share across queues.
- `read_conn` — used exclusively for the blocking `XREADGROUP`.

> **Important.** `conn` and `read_conn` must be independent connections
> obtained from **separate** calls to
> `redis::Client::get_multiplexed_async_connection`. Do **not** pass a clone
> of the same handle. A `MultiplexedConnection` pipelines all commands over a
> single socket, so a blocking `XREADGROUP` on a shared handle stalls every
> other command queued behind it — including this queue's own XACKs, which
> will then time out.

## Runtime features

| Feature | Enables                                                      |
|---------|--------------------------------------------------------------|
| `tokio` | `tokio::sync::Semaphore`, `tokio::task::JoinSet`, `tokio-comp` |
| `smol`  | `mea::semaphore::Semaphore`, `FuturesUnordered`, `smol-comp`   |

The two are mutually exclusive; enabling neither or both is a compile error.

## MSRV

Rust `1.85` (2024 edition).

## License

Dual-licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](./LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual-licensed as above, without any additional terms or conditions.
