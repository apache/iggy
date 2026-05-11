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

> Apache Iggy (Incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.
>
> Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects.
>
> While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

## Features

- **Transports**: TCP (custom binary), QUIC, HTTP, WebSocket. One unified `IggyClient` API across all four.
- **TLS** on every transport, configured via connection string (`?tls=true&tls_ca_file=/path/to/ca.crt`) or builder.
- **Connection strings** with auto-login on `connect()`: `iggy://` (TCP default), `iggy+tcp://`, `iggy+quic://`, `iggy+http://`, `iggy+ws://`. Reconnection retries and heartbeat interval are configurable as URL options.
- **Authentication**: username/password and Personal Access Tokens (PAT).
- **Async, non-blocking** client built on Tokio with custom zero-copy (de)serialization.
- **High-level builders** on `IggyClient`: `producer(stream, topic)`, `consumer(name, stream, topic, partition)`, and `consumer_group(name, stream, topic)`.
- **Producer modes**: `direct` (synchronous send) and `background` (buffered with parallel shard workers using `OrderedSharding` or `BalancedSharding`). Configurable batch length / size and linger time.
- **Partitioning**: `balanced`, `partition_key`, or explicit `partition_id`. Custom `Partitioner` is pluggable.
- **Consumer**: standalone or consumer-group; consumed as an async `Stream`. Polling strategies: `next`, `offset`, `timestamp`, `first`, `last`.
- **Auto-commit** offset policies: `Interval`, `When`, `After`, `IntervalOrWhen`, `IntervalOrAfter`, or disabled.
- **Stream builder** (`IggyStream`, `IggyStreamProducer`, `IggyStreamConsumer`) for declarative producer + consumer setup on shared or separate stream/topic.
- **Reliability**: automatic reconnection with retries, heartbeat, send retries, and offset auto-commit handled by the high-level API.
- **Message features**: optional headers (`HeaderKey` / `HeaderValue`), client-side AES-256-GCM encryption (via `Aes256GcmEncryptor`), per-topic compression (currently `None` and `Gzip`), server-honored message expiry, and server-side deduplication.
- **Admin**: stream/topic/partition CRUD, consumer-group management, server-side consumer offsets, system stats.

## Installation

```bash
cargo add iggy
```

Optional features map to common scenarios. See the [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/) for the full list.

## Quick start

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
        .auto_commit(AutoCommit::IntervalOrWhen(
            IggyDuration::from_str("1s")?,
            AutoCommitWhen::ConsumingAllMessages,
        ))
        .create_consumer_group_if_not_exists()
        .auto_join_consumer_group()
        .polling_strategy(PollingStrategy::next())
        .poll_interval(IggyDuration::from_str("1ms")?)
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
            }
            Err(error) => eprintln!("poll error: {error}"),
        }
    }
    Ok(())
}
```

For lower-level control over individual commands (login, stream/topic management, raw send, polling by offset or timestamp), use the transport-specific clients directly. See the [examples](https://github.com/apache/iggy/tree/master/examples/rust) and the [Rust SDK docs](https://iggy.apache.org/docs/sdk/rust/intro/).

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
