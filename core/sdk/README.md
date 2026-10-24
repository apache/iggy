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
- **Reliability**: managed replica connections, heartbeat, ordered mutation lanes, explicit recovery of uncertain mutations, and offset auto-commit.
- **Message features**: optional headers (`HeaderKey` / `HeaderValue`), client-side AES-256-GCM encryption (via `Aes256GcmEncryptor`), topic compression metadata (`None` and `Gzip`; no runtime compression yet), server-honored message expiry, and server-side deduplication.
- **Admin**: stream/topic/partition CRUD, consumer-group management, server-side consumer offsets, system stats.
- **Resource options**: typed create and update options for streams, topics and users (`StreamCreateOptions`, `TopicUpdateOptions`, `UserUpdateOptions` and friends). An update sends only the settings it names and resets others by key. A stream can carry its own at-rest key (`StreamCreateOptions::encryption_key`), reads report only its source and fingerprints.

## Installation

Run from your application crate. Use a release compatible with your server; for
unreleased changes, build the SDK and server from the same source checkout.

This checkout requires matching servers and SDKs with fresh storage. Older data
directories are refused without modification; `--fresh` deletes existing data
in the configured directory. See the [server recovery guide](../server/README.md#upgrade-recovery).
Mixed versions and rolling upgrades are unsupported. Binary clients share one
logical session across replica links using BindSession (15) and discover
partition primaries with GetPartitionRoute (105). Commands 14, 103, 104, 123 and 124
are retired. HTTP clients use server-side forwarding.

```bash
cargo add iggy
```

All four transports are included; this crate declares no optional Cargo features.

The durable session API adds required `session_identity` and `session_bind_secret`
methods to `VsrSessionControl`. External transport implementations must provide
them, retain their logical identity and bind proof across transport reconnects,
and clear them on explicit logout. `VsrSessionSealed` is an implementation marker,
not a restriction on external implementations. The bind proof is a bearer
credential for internal session control and must never be logged or exposed.
`refresh_session_password` and `refresh_session_username` are removed. A
transport that signs in again applies a committed credential change where its
calls complete.

`IggyError::ProducerSendFailed` changes its public `stream_name` and `topic_name`
fields from `String` to `Box<str>`. Existing constructors must convert their names
with `.into()`; the callback's `ErrorCtx` fields remain `String`. It also adds a
public `uncertain: usize` field, so struct literals and exhaustive patterns must
name it. `ConsumerGroupClientState` no longer has its public `partition_count` and
`set_partition_context` methods.

An established session has its own lifetime. Password or PAT changes and PAT
expiry do not end it. Logout, session lease expiry, or user deactivation end the
session and prevent its bind proof from restoring it.

## Mutation recovery

TCP, QUIC and WebSocket serialize mutations within each partition history and
within the metadata lane. Different lanes can progress concurrently. An
`IggyError::OperationUncertain(operation_id)` retains the exact request for
recovery. A later mutation on the same lane returns
`IggyError::LaneBlocked(operation_id)` until that operation is resolved or abandoned.

Call `client.resolve_operation(operation_id, deadline).await`, where `deadline`
is a `std::time::Instant`. A proved success returns `Ok(())`; a proved committed
business rejection returns its original error. Both release the lane. Another
`OperationUncertain` keeps the request available for a later recovery attempt.
The client keeps the proved outcomes of its last `max_pending_operations`
replays, also across a session reset. So a later resolve returns the same
outcome without a request, also after a producer or a consumer of the client
resolved the operation first.
`client.abandon_operation(operation_id).await` releases the lane while leaving
the outcome unknown. A session reset drops retained requests. An
`OperationNotRetained` error does not establish the outcome. A reset or an
abandon came before a proved outcome, or the proved outcome aged out.
Recovery is unavailable over HTTP.

Receipt access follows the server's [session and revocation policy](../server/README.md#sessions-and-retry-receipts).
An unresolved operation cannot recover its receipt after its original session
ends, even after a new login or account reactivation. With a live session,
partition permission revocation prevents resolution with 41 until a re-grant;
metadata can replay the user's own committed cached result without a new
permission check. A fresh application send can duplicate an uncertain operation.

Each node counts the messages in its stream, topic and system stats. After a
successful send, a node that is not the partition primary shows the new count
only after the next prepare or commit broadcast. When no other write follows,
this delay is at most `cluster.commit_broadcast_interval` (500 ms by default).
See [metadata freshness and counters](../server/README.md#metadata-freshness-and-counters).

An explicit sign-in or logout ends earlier calls: unsent calls fail with 40,
and possibly applied mutations remain uncertain. SDK-managed recovery can keep
an unsent call waiting for a session or a primary within its original deadline.
Expiry returns 58. A refused automatic sign-in ends each unsent call that waits
for it with 40. Until the caller signs in, later calls also fail with 40.
`IggyError` carries no cause, so the client logs the refusal as a warning. If
the sign-in of a `connect()` call is refused, that call returns the refusal.
Recovery uses the caller's last explicit sign-in, or configured automatic
credentials only if the caller never signed in. Logout disables automatic sign-in.

HTTP 503 `relay_refused` uses the configured HTTP retry policy, regardless of
its body code. Rust's HTTP retry schedule does not use the server's `Retry-After`
header. This retry support does not provide retained-operation recovery.
If the last 503 body carries code 58, the call returns `TransientNotAccepted`.
A write that possibly committed, for example after a 504 or a lost reply, is
not sent again and returns `TransientNotCommitted`.

On a multi-node cluster, an HTTP call often reaches a node that is not the
partition primary. For message sends, polls and consumer offset calls, that
node relays the call to the primary. The relay needs identical `http.jwt`
secrets or `cluster.auth` on every node. Without that key material, these calls
answer 503 with code 58. See
[HTTP routing and backpressure](../server/README.md#http-routing-and-backpressure).

A committed password change or rename of the signed-in user updates the
credentials that the client signs in with again. This holds for a typed call, a
raw request and a change that `resolve_operation` proves. A committed rejection,
such as a wrong current password, leaves them unchanged.

The client defaults to 256 active or retained mutations and 64 MiB of frozen
request frames. Configure positive limits through `PendingOperationsConfig`
on the binary transport builder, or through connection-string options:

```text
iggy://iggy:iggy@localhost:8090?max_pending_operations=256&max_pending_operation_bytes=67108864
```

Admission waits within the call deadline before allocating a request ID or
sending. A frame larger than the byte limit fails locally. Completion,
abandonment and session reset release operation capacity; bytes remain charged
while a writer still holds the frame. Replaying a retained request reuses its
reservation.

Auto-commit and `Next` polls share their partition's mutation barrier while the
call is active. They retain their weak acknowledgement: cancellation or failover
can leave progress unknown. Error 57 or a lost reply after send ends the call
immediately without replay; errors 58/89 still retry known non-admission.
These polls are not retained for recovery.

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
        .auto_commit(AutoCommit::IntervalOrWhen(
            NonZeroIggyDuration::from_str("1s")?,
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

For `IggyConsumerConfig`, `partitions_count` controls topic creation only. An ordinary consumer uses partition `0` unless the builder's `partition_id` or the config's `with_partition_id` selects another partition. Code that previously used `partitions_count` to select an existing partition must set `partition_id` explicitly. Consumer-group assignment ignores `partition_id`.

## Metadata changes

`get_metadata_changes` reads the node's change log: one record per public result of a committed metadata operation, such as a stream created, a topic renamed, a user's permissions changed or a session closed. A mirror keeps a cursor, asks for the records after it, and stores the returned `next` cursor. The call needs root or the global `manage_servers` permission.

```rust
use iggy::prelude::*;
use std::time::Duration;

async fn follow(client: &IggyClient, groups: Vec<MetadataChangeGroup>) -> Result<(), IggyError> {
    let mut query = MetadataChangesQuery {
        groups,
        ..MetadataChangesQuery::default()
    };
    loop {
        let page = client.get_metadata_changes(&query).await?;
        match page.status {
            MetadataChangeStatus::Ok => {
                for record in &page.records {
                    println!("op {} by {:?}: {:?}", record.op, record.actor_user_id, record.change);
                }
                if let Some(next) = page.next {
                    query.start = MetadataChangeStart::After(next);
                }
                if !page.has_more {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            MetadataChangeStatus::Behind => tokio::time::sleep(Duration::from_millis(100)).await,
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                query.start = rebuild(client, &query.groups).await?;
            }
        }
    }
}

/// Pages the current state, then returns the position to follow from.
async fn rebuild(
    client: &IggyClient,
    groups: &[MetadataChangeGroup],
) -> Result<MetadataChangeStart, IggyError> {
    let mut scan_groups = groups.to_vec();
    scan_groups.extend(groups.iter().flat_map(|group| group.parents()));
    let mut query = MetadataChangesQuery {
        start: MetadataChangeStart::Current(None),
        groups: scan_groups,
        ..MetadataChangesQuery::default()
    };
    loop {
        let page = client.get_metadata_changes(&query).await?;
        match page.status {
            MetadataChangeStatus::Ok => {}
            MetadataChangeStatus::Behind => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            MetadataChangeStatus::Expired | MetadataChangeStatus::Reset => {
                query.start = MetadataChangeStart::Current(None);
                continue;
            }
        }
        for item in &page.items {
            println!("{item:?}");
        }
        match (page.current_page_token, page.next) {
            (Some(token), _) => query.start = MetadataChangeStart::Current(Some(token)),
            (None, Some(anchor)) => return Ok(MetadataChangeStart::After(anchor)),
            (None, None) => query.start = MetadataChangeStart::Current(None),
        }
    }
}
```

`Expired` means the log no longer holds the history after the cursor, and `Reset` means the cursor belongs to another history, for example after the data directory was rebuilt. Either way the mirror rebuilds from current state pages and then follows the changes after the anchor they return.

Current pages need the parent groups of every selected group, for example `topics` with `streams`, so `rebuild` adds them through `MetadataChangeGroup::parents()`. Keep a rebuilt state private until a change page answers `Ok` with `has_more == false` and a `head_op` at or above the last current page's.

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
