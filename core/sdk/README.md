<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-darkbg.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg">
    <img alt="Apache Iggy" src="https://raw.githubusercontent.com/apache/iggy/refs/heads/master/assets/logo/SVG/iggy-apache-color-lightbg.svg" width="360">
  </picture>
</div>

# Apache Iggy Rust SDK

<div align="center">

[Website](https://iggy.apache.org) | [Getting started](https://iggy.apache.org/docs/introduction/getting-started/) | [Documentation](https://iggy.apache.org/docs/) | [Examples](https://github.com/apache/iggy/tree/master/examples/rust) | [Discord](https://discord.gg/apache-iggy)

</div>

<p align="center">
  <a href="https://crates.io/crates/iggy"><img alt="Crate" src="https://img.shields.io/crates/v/iggy?logo=rust&style=flat-square"></a>
  <a href="https://iggy.apache.org/docs/sdk/rust/intro/"><img alt="Docs" src="https://img.shields.io/badge/docs-iggy.apache.org-blue?style=flat-square"></a>
  <a href="https://crates.io/crates/iggy"><img alt="Downloads" src="https://img.shields.io/crates/d/iggy?style=flat-square"></a>
  <a href="https://github.com/apache/iggy/blob/master/LICENSE"><img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square"></a>
</p>

Official Rust client SDK for [Apache Iggy](https://iggy.apache.org), the persistent message streaming platform written in Rust. The SDK ships a low-level transport client (QUIC, TCP, HTTP, WebSocket) for direct command access and a high-level producer/consumer API with batching, consumer groups, and auto-commit.

## Features

- **Transports**: TCP (custom binary), QUIC, HTTP, WebSocket. One unified `IggyClient` API across all four.
- **TLS**: TCP and WebSocket expose TLS connection-string options; QUIC always uses TLS; HTTP uses an HTTPS URL configured through the builder.
- **Connection strings**: `iggy://` (TCP default), `iggy+tcp://`, `iggy+quic://`, `iggy+http://`, `iggy+ws://`. Binary transports apply credentials on `connect()`; HTTP requires an explicit login. Option keys and reconnection support differ by transport.
- **Authentication**: username/password and Personal Access Tokens (PAT).
- **Async, non-blocking** client built on Tokio with custom zero-copy (de)serialization.
- **High-level builders** on `IggyClient`: `producer(stream, topic)`, `consumer(name, stream, topic, partition)`, and `consumer_group(name, stream, topic)`.
- **Producer modes**: `direct` (awaited send) and `background` (buffered with parallel shard workers using `OrderedSharding` or `BalancedSharding`). Configurable batch length / size and linger time.
- **Partitioning**: `balanced`, `messages_key`, or explicit `partition_id`. Custom `Partitioner` is pluggable.
- **Consumer**: standalone or consumer-group over binary transports; HTTP supports standalone consumers only. Consumed as an async `Stream`. Polling strategies: `next`, `offset`, `timestamp`, `first`, `last`.
- **Auto-commit** offset policies: `Interval`, `When`, `After`, `IntervalOrWhen`, `IntervalOrAfter`, or disabled.
- **Stream builder** (`IggyStream`, `IggyStreamProducer`, `IggyStreamConsumer`) for declarative producer + consumer setup on shared or separate stream/topic.
- **Reliability**: automatic reconnection with retries, heartbeat, send retries, and offset auto-commit handled by the high-level API.
- **Message features**: optional headers (`HeaderKey` / `HeaderValue`), client-side AES-256-GCM encryption (via `Aes256GcmEncryptor`), topic compression metadata (`None` and `Gzip`; no runtime compression yet), server-honored message expiry, and server-side deduplication.
- **Admin**: stream/topic/partition CRUD, consumer-group management, server-side consumer offsets, system stats.

## Installation

Run from your application crate. Use a release compatible with your server; for
unreleased changes, build the SDK and server from the same source checkout.

Cluster auto-commit polling requires servers that support consumer session
attachment and primary poll routing (binary commands 14, 103 and 104).
Rust manual and interval offset writes also require command 123. It discovers
the primary while allowing final commits for partitions awaiting handoff;
stores and deletes retain their existing wire formats and deduplication keys. Pause
binary auto-commit consumers during this upgrade, upgrade every server first,
then upgrade the SDKs and restart the consumers so they join their groups again.
Older SDKs can lose group membership when a backup refuses an offset commit;
the new SDK does not fall back to that path on an older server.
HTTP clients use server-side forwarding and need no new routing commands.

```bash
cargo add iggy
```

All four transports are included; this crate declares no optional Cargo features.

## Quick start

Start a source server from the repository root in a separate terminal:

```bash
cargo run --bin iggy-server -- --fresh --with-default-root-credentials
```

Use disposable replica data with `--fresh`. Environment credentials override the
flag, and recovered credentials are not replaced. The sample expects `iggy`/`iggy`
and requires `iggy`, Tokio and `futures-util` in the application.

```rust
use std::error::Error;
use std::str::FromStr;
use futures_util::StreamExt;
use iggy::prelude::*;

const STREAM: &str = "telemetry";
const TOPIC: &str = "device-events";
const CONSUMER_GROUP: &str = "telemetry-ingester";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = IggyClient::from_connection_string(
        "iggy://iggy:iggy@localhost:8090",
    )?;
    client.connect().await?;

    let producer = client
        .producer(STREAM, TOPIC)?
        .direct(
            DirectConfig::builder()
                .batch_length(1000)
                .linger_time(IggyDuration::from_str("1ms")?)
                .build(),
        )
        .partitioning(Partitioning::balanced())
        .build();
    producer.init().await?;
    producer
        .send(vec![IggyMessage::from_str("Hello Apache Iggy")?])
        .await?;

    let mut consumer = client
        .consumer_group(CONSUMER_GROUP, STREAM, TOPIC)?
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .batch_length(1000)
        .build();
    consumer.init().await?;

    while let Some(message) = consumer.next().await {
        match message {
            Ok(message) => {
                let payload = std::str::from_utf8(&message.message.payload)
                    .unwrap_or("<non-utf8>");
                println!(
                    "offset={} partition={} current_offset={} payload={payload}",
                    message.message.header.offset,
                    message.partition_id,
                    message.current_offset,
                );
                if let Some(headers) = message.message.user_headers_map()? {
                    for (key, value) in headers {
                        println!("  header {key} = {value:?}");
                    }
                }
                consumer
                    .store_offset(message.message.header.offset, Some(message.partition_id))
                    .await?;
            }
            Err(error) => eprintln!("poll error: {error}"),
        }
    }
    Ok(())
}
```

For lower-level control over individual commands (login, stream/topic management, raw send, polling by offset or timestamp), use the transport-specific clients directly. See the [examples](https://github.com/apache/iggy/tree/master/examples/rust) and the [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/).

For `IggyConsumerConfig`, `partitions_count` controls topic creation only. An ordinary consumer uses partition `0` unless the builder's `partition_id` or the config's `with_partition_id` selects another partition. Code that previously used `partitions_count` to select an existing partition must set `partition_id` explicitly. Consumer-group assignment ignores `partition_id`.

## Deferred polling

High-level consumers use long polling by default. `DeferredPollOptions` separates
readiness from batch limits: `min_count = 1`, `max_wait = 1s`, `max_bytes = 1 MiB`
and `request_timeout = 11s`. `batch_length` remains the maximum message count.
Use `.poll_options(options)` on either consumer builder, or pass options to the
low-level `poll_messages_deferred` method. A zero `max_wait` skips readiness
waiting while preserving the same byte limits and request budget.

The server responds when the minimum is readable, the byte cap is reached, or the
readiness wait expires. Expiry allows a final bounded read; the separate request
timeout bounds routing, waiting, I/O and response handling. It returns partial or
empty data when appropriate. Read failures and exhausted request budgets return
errors. `max_bytes` covers the binary body, including response metadata and batch
framing; HTTP applies the same selection limit before JSON encoding. A first
record that cannot fit returns `InvalidSizeBytes`, without advancing its offset.

Both builders default to `Next` and manual commits. Process messages in partition order,
then call `store_offset(offset, Some(partition_id))`. Fetch positions advance
independently of stored offsets; an initial `Next` is resolved once per partition
and assignment. Restarting before a commit can replay work. Explicit auto-commit
policies remain available, but `PollingMessages` can commit prefetched messages
before application delivery or processing.

Prefetch reserves both byte and message capacity before each request and holds
it through queued and partially consumed batches. Defaults are 16 MiB of encoded
response bytes and 16,000 messages; use `.prefetch_bytes(...)` and
`.prefetch_messages(...)` to change them. Each must fit at least one maximum
response. Decoded message metadata and bounded HTTP JSON overhead are additional;
these limits are not process RSS limits. Memory handed to application code is
outside the consumer budget. Assignment refresh, reconnection and cancellation
continue while the application stalls. Revoked generations are discarded before
delivery. The normal `poll_interval` setting has been removed; error backoff is
still configurable.

Binary data requests lease separate connections, preserving control traffic.
At most 16 single-partition polls run concurrently, rotating through larger
assignments. This does not watch every partition simultaneously when there are
more than 16. Cancellation discards the leased connection. An ambiguous reply
failure is not replayed inside the transport; the high-level worker can retry its
explicit position. With opt-in server auto-commit, cancellation may race an
accepted offset update.

HTTP uses `GET /streams/{stream}/topics/{topic}/messages/deferred` with the ordinary
poll fields plus `wait_us`, `min_count`, `max_bytes` and `request_timeout_us`.
Omitted options use the SDK defaults. Binary commands 105 and 106 carry the same
contract. Upgrade servers before SDKs; unsupported servers reject these requests.
Retries and forwarding deduct from the original budgets. Proxy timeouts should
exceed the configured request timeout. The low-level immediate poll API retains
its existing command and route.

The server defaults to a 30-second maximum readiness wait, 1024 pending polls per
shard, 64 per logical session (or HTTP user), 16 MiB per read reservation and
64 MiB of in-flight read reservations. Conservative snapshot and disk allocation
bounds can reject a small selection backed by a large allocation. Configure
`sharding.deferred_poll_*` accordingly. The response cap is applied to the selected
result; it does not cap all storage work. Capacity refusal returns
`TransientNotAccepted`. Metrics expose pending polls, read reservations and
completion outcomes.

Bench exposes `--max-wait`, `--min-count`, `--max-bytes` and `--request-timeout`
for both consumer APIs and records them in report names. Intentional batching
waits contribute to measured latency.

## Versioning

Stable releases follow semver (`x.y.z`). Edge releases (`x.y.z-edge.N`) are cut from `master` between stable versions and may include unreleased fixes; pin to a stable version for production.

## Resources

- [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/)
- [High-level SDK guide](https://iggy.apache.org/docs/sdk/rust/high-level-sdk/)
- [Stream builder guide](https://iggy.apache.org/docs/sdk/rust/stream-builder/)
- [Project documentation](https://iggy.apache.org/docs/)
- [Runnable examples](https://github.com/apache/iggy/tree/master/examples/rust): getting-started, basic, new-sdk, stream-builder, multi-tenant, message-envelope, message-headers, tcp-tls, sink-data-producer.
- [Benchmarking platform](https://benchmarks.iggy.apache.org)
- [GitHub repository](https://github.com/apache/iggy)
- [Discord community](https://discord.gg/apache-iggy)

## Related crates

- [`iggy_common`](https://crates.io/crates/iggy_common): shared types and traits.
- [`iggy_binary_protocol`](https://crates.io/crates/iggy_binary_protocol): wire protocol codec.
- [`iggy-cli`](https://crates.io/crates/iggy-cli): command-line tool, `cargo install iggy-cli`.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/apache/iggy/blob/master/LICENSE).
