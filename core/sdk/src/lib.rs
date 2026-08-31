// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Apache Iggy is a high-performance, persistent message streaming platform written in
//! Rust, capable of processing millions of messages per second with ultra-low latency.
//! It is part of the [`Apache Software Foundation`] (ASF).
//!
//! **This library is the Apache Iggy SDK.**
//! It exposes a low-level and a high-level API for the Apache Iggy message streaming
//! infrastructure for the Rust programming language.
//! SDKs for other programming languages can be found in [`foreign`] of the root
//! repository on GitHub.
//!
//! The core of Iggy is the message streaming server.
//! In essence it is a persisted append-only log data structure concerned with making
//! reads and writes highly efficient.
//! For that, the server exposes *commands* that can be triggered to change its state,
//! such as adding users, setting permissions, adding new streams and topics or reading
//! and writing messages to and from the log.
//! A comprehensive overview of commands can be found in the [`schema spec`] on the
//! website, or in the [`server command enum`] within the source code.
//!
//! The SDK provides tools to build production ready message-streaming applications.
//! It exposes its functionality at two levels. The [high-level API](#high-level-api)
//! is transport-agnostic and already ships with useful features that a production
//! application needs, such as message batching, retry policies and offset-tracking.
//! The [low-level API](#low-level-api) is the set of concrete transport clients that
//! speak the wire protocol directly and that the high-level API is built on top of.
//! It is recommended to start with the high-level API, and utilize the low-level API
//! in case the high-level API cannot satisfy your requirements.
//!
//! # High-level API
//!
//! The high-level API is most likely what you are looking for, especially if you are new
//! to building message-streaming applications with Iggy.
//! High-level API clients already provide common message-streaming features that
//! you would otherwise need to build yourself.
//!
//! There are three client types:
//! - [`IggyClient`] is the entry point and the full API surface. It owns the
//!   connection and implements every domain trait, including [`MessageClient`]
//!   with the raw [`send_messages`] and [`poll_messages`] primitives. For both, each call
//!   is a single, stateless request, i.e. there is no batching, retries, offset tracking,
//!   or polling loop.
//! - [`IggyProducer`] exposes all configuration and functionality to produce (send)
//!   messages to a specific topic in a stream. It shares the connection from the
//!   [`IggyClient`].
//! - [`IggyConsumer`] exposes all configuration and functionality to consume (read)
//!   messages from a specific topic and stream. It also shares the connection from the
//!   [`IggyClient`].
//!
//! You do not construct the producer and consumer independently. Spawn their builders
//! from an [`IggyClient`] with [`IggyClient::producer`] and
//! [`IggyClient::consumer`] so they share its connection.
//!
//! ## When to use each
//!
//! Reach for [`IggyClient`] directly for administrative tasks such as
//! creating streams, topics, users, and consumer groups, reading or storing
//! offsets, or sending and polling a handful of messages in a script.
//! Reach for [`IggyProducer`] and [`IggyConsumer`] when producing and consuming messages.
//!
//! The [`IggyProducer`] has two modes you can pick from when building it:
//! **direct** ([`DirectConfig`]), where a send goes out on the calling task, or
//! **background** ([`BackgroundConfig`]), where a send is buffered and sending
//! is offloaded to worker tasks.
//!
//! Additionally it provides the following features:
//!
//! - **Retries** with a configurable count and interval.
//! - A pluggable **partitioning strategy** ([`Partitioner`]).
//! - **Encryption** of message payloads before they leave the process.
//! - **Auto-creation** of the stream and topic if they do not exist yet.
//!
//! Only in **direct** mode:
//! - **Chunking** splits an input larger than `batch_length` into several
//!   requests, and `linger_time` sets a minimum delay between them.
//!
//! Only in **background** mode:
//! - **Batching** collects messages until the batch size in bytes, the message
//!   count, or the linger interval is reached, whichever comes first.
//! - **Shard workers** run several send loops in parallel (`num_shards`), which
//!   helps when one producer writes to several streams or topics.
//! - A **sharding strategy** ([`Sharding`]) decides which worker a batch goes to:
//!   [`OrderedSharding`] keeps messages for the same stream and topic in order,
//!   [`BalancedSharding`] spreads them round-robin for throughput.
//! - **Backpressure** bounds the bytes buffered across all workers and the number
//!   of in-flight batches. With a [`BackpressureMode`] you can decide whether a full buffer
//!   blocks, blocks with a timeout, or fails immediately.
//! - **Ordering control** through `max_in_flight`.
//! - An **error callback** ([`ErrorCallback`]) receives the messages a background
//!   send could not deliver, together with the confirmations that did commit.
//! - **Graceful shutdown** flushes what is still buffered, which dropping the
//!   producer does not.
//!
//! The [`IggyConsumer`] has:
//! - A [`futures::Stream`] implementation, so a `while let Some(message) =
//!   consumer.next().await` loop drives polling, paging, and the poll interval
//!   for you.
//! - A **polling strategy** ([`PollingStrategy`]: `next`, `offset`, or
//!   `timestamp`) that tracks position.
//! - **Auto-commit** ([`AutoCommit`]) stores the offset on an interval, or when
//!   messages are polled, consumed one by one, consumed in full, or every Nth
//!   message, so a restart resumes where it left off.
//! - **Manual offset control** to store, read, or delete the offset of any
//!   partition yourself when auto-commit is disabled or not enough.
//! - A **shared state handle** ([`IggyConsumerState`]) that another task can clone
//!   to read offsets or commit while the consuming loop holds the consumer.
//! - **Replay control** drops messages this consumer already consumed, unless you
//!   opt into replaying them with `allow_replay`.
//! - **Auto-join** of the consumer group, optionally creating it, plus a rejoin
//!   once the server revokes membership or the connection comes back.
//! - **Reconnection handling** pauses polling while the client is disconnected and
//!   resumes it after the reconnect and the rejoin have gone through.
//! - **Init retries** wait for the stream and topic to appear instead of failing
//!   right away when the consumer starts before they exist.
//! - Payload **decryption**.
//! - **Graceful shutdown** flushes pending offsets and leaves the consumer group.
//!
//! For details on the specific behavior of each feature, reach for the type-level
//! documentation.
//!
//! # Stream builder API
//!
//! The stream builder API is a convenient way to use the high-level API.
//! [`IggyStream`], [`IggyStreamProducer`], and [`IggyStreamConsumer`] construct
//! everything at once. You can pass an [`IggyClient`] (or just a connection string)
//! together with a config, and they hand back a ready, connected
//! [`IggyProducer`] / [`IggyConsumer`].
//! Compared to the **high-level API**, it changes how you construct
//! producers and consumers, not what they can do. Instead of chaining an
//! [`IggyProducerBuilder`] / [`IggyConsumerBuilder`] and setting each option
//! with a method call, you describe the whole setup once in an
//! [`IggyStreamConfig`] (or in a single [`IggyProducerConfig`] /
//! [`IggyConsumerConfig`] when you only need one side) and build from it. The result is
//! the same [`IggyProducer`] and [`IggyConsumer`] the builders produce, backed
//! by the same [`IggyClient`].
//!
//! # Low-level API
//!
//! The low-level API is the set of concrete transport clients: [`TcpClient`],
//! [`QuicClient`], [`WebSocketClient`], and [`HttpClient`]. Each one implements
//! [`Client`], the supertrait that pulls in every domain-specific trait, so a
//! transport client on its own can already drive the full server API. The
//! high-level [`IggyClient`] is one more layer over exactly these types.
//!
//! ## Differences to the high-level API
//!
//! - **Transport is fixed at compile time.** You name a concrete type
//!   ([`TcpClient`], [`QuicClient`], and so on) instead of configuring a
//!   transport-agnostic [`IggyClient`]. Swapping transports means swapping the
//!   type, not changing a connection-string scheme.
//! - **No managed connection.** [`IggyClient`] owns a shared connection and
//!   spawns a heartbeat task to keep it alive. A transport client does neither.
//!   You own the connection lifecycle and must ping the server
//!   yourself if you want that liveness signal.
//! - **No producer or consumer helpers.** [`IggyProducer`] and [`IggyConsumer`]
//!   are spawned from an [`IggyClient`], so a raw transport client gives you no
//!   background batching, retries, polling loop, auto-commit, consumer-group
//!   auto-join, or payload encryption. You get the request-response primitives
//!   ([`send_messages`], [`poll_messages`]) and nothing layered on top.
//! - **Raw wire access.** [`BinaryTransport::send_raw_with_response`] sends an
//!   arbitrary command code and payload and returns the raw response bytes.
//!   The high-level equivalents are [`IggyClient::send_binary_request`] and
//!   [`IggyClient::send_http_request`].
//!   Either way you need to know the server command codes and the wire format.
//!
//! ## When to use it
//!
//! Prefer the high-level API. Reach for the low-level API only when you need one
//! of the things it exposes that [`IggyClient`] deliberately hides:
//!
//! - You want to own the connection lifecycle yourself, with custom pooling,
//!   supervision, or a different heartbeat strategy, rather than let
//!   [`IggyClient`] manage it.
//! - You are building your own abstraction on top of the SDK, for example a
//!   different producer or consumer, and want the primitives.
//! - You forked the server and need to issue a command the typed API does not recognize
//!   and want the raw [`send_raw_with_response`][`BinaryTransport::send_raw_with_response`]
//!   instruction.
//!
//! If none of these apply, the high-level API gives you the same reach with far
//! less to get wrong.
//!
//! # Async runtime
//!
//! The SDK is async and runs on the [Tokio] runtime. Note that this is a hard
//! requirement and not optional. The SDK uses [quinn] (for QUIC), [reqwest] (for HTTP),
//! [tokio-tungstenite] (for WebSocket) and [tokio-rustls] (for TLS) which all build on
//! Tokio.
//! The SDK also spawns its own background work with [`tokio::spawn`] (the
//! [`IggyClient::connect`] heartbeat, and the [`IggyProducer`] and
//! [`IggyConsumer`] tasks) and drives timeouts, retries, and poll intervals with
//! [`tokio::time`].
//! Note that dropping down to the low-level transport clients does not change this.
//! **Thus, everything you do with the Rust SDK must happen inside a Tokio runtime.**
//!
//! ```no_run
//! use iggy::prelude::*;
//! use futures_util::StreamExt;
//! use std::error::Error;
//! use std::str::FromStr;
//!
//! // `#[tokio::main]` starts the runtime the SDK requires.
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let client = IggyClient::from_connection_string(
//!         "iggy://iggy:iggy@localhost:8090",
//!     )?;
//!     client.connect().await?;
//!
//!     let producer = client.producer("stream_name", "topic_name")?.build();
//!     producer.init().await?;
//!     producer
//!         .send(vec![IggyMessage::from_str("some_message_payload")?])
//!         .await?;
//!
//!     let mut consumer = client
//!         .consumer("consumer_name", "stream_name", "topic_name", 1)?
//!         .build();
//!     consumer.init().await?;
//!     while let Some(message) = consumer.next().await {
//!         let _message = message?;
//!         break;
//!     }
//!
//!     client.shutdown().await?;
//!     Ok(())
//! }
//! ```
//!
//! [`IggyClient`]: crate::prelude::IggyClient
//! [`IggyClient::producer`]: crate::prelude::IggyClient::producer
//! [`IggyClient::consumer`]: crate::prelude::IggyClient::consumer
//! [`IggyProducer`]: crate::prelude::IggyProducer
//! [`IggyConsumer`]: crate::prelude::IggyConsumer
//! [`MessageClient`]: crate::prelude::MessageClient
//! [`send_messages`]: crate::prelude::MessageClient::send_messages
//! [`poll_messages`]: crate::prelude::MessageClient::poll_messages
//! [`DirectConfig`]: crate::prelude::DirectConfig
//! [`BackgroundConfig`]: crate::prelude::BackgroundConfig
//! [`BackpressureMode`]: crate::clients::producer_config::BackpressureMode
//! [`Sharding`]: crate::prelude::Sharding
//! [`OrderedSharding`]: crate::prelude::OrderedSharding
//! [`BalancedSharding`]: crate::prelude::BalancedSharding
//! [`ErrorCallback`]: crate::clients::producer_error_callback::ErrorCallback
//! [`Partitioner`]: crate::prelude::Partitioner
//! [`PollingStrategy`]: crate::prelude::PollingStrategy
//! [`AutoCommit`]: crate::prelude::AutoCommit
//! [`IggyConsumerState`]: crate::prelude::IggyConsumerState
//! [`futures::Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
//! [`TcpClient`]: crate::prelude::TcpClient
//! [`QuicClient`]: crate::quic::quic_client::QuicClient
//! [`WebSocketClient`]: crate::prelude::WebSocketClient
//! [`HttpClient`]: crate::http::http_client::HttpClient
//! [`Client`]: crate::prelude::Client
//! [`BinaryTransport::send_raw_with_response`]: crate::binary::BinaryTransport::send_raw_with_response
//! [`IggyClient::send_binary_request`]: crate::prelude::IggyClient::send_binary_request
//! [`IggyClient::send_http_request`]: crate::prelude::IggyClient::send_http_request
//! [`IggyStream`]: crate::prelude::IggyStream
//! [`IggyStreamProducer`]: crate::prelude::IggyStreamProducer
//! [`IggyStreamConsumer`]: crate::prelude::IggyStreamConsumer
//! [`IggyStreamConfig`]: crate::prelude::IggyStreamConfig
//! [`IggyProducerConfig`]: crate::prelude::IggyProducerConfig
//! [`IggyConsumerConfig`]: crate::prelude::IggyConsumerConfig
//! [`IggyProducerBuilder`]: crate::prelude::IggyProducerBuilder
//! [`IggyConsumerBuilder`]: crate::prelude::IggyConsumerBuilder
//! [`IggyClient::connect`]: crate::prelude::Client::connect
//!
//! [Tokio]: https://tokio.rs
//! [`tokio::spawn`]: https://docs.rs/tokio/latest/tokio/task/fn.spawn.html
//! [`tokio::time`]: https://docs.rs/tokio/latest/tokio/time/index.html
//! [quinn]: https://docs.rs/quinn
//! [reqwest]: https://docs.rs/reqwest
//! [tokio-tungstenite]: https://docs.rs/tokio-tungstenite
//! [tokio-rustls]: https://docs.rs/tokio-rustls
//!
//! [`Apache Software Foundation`]: https://www.apache.org/
//! [`foreign`]: https://github.com/apache/iggy/tree/master/foreign
//! [`schema spec`]: https://iggy.apache.org/docs/server/schema/
//! [`server command enum`]: https://github.com/apache/iggy/blob/3e27ebc8dd5dbf257b816993908dc0747c4f8849/core/server/src/binary/command.rs#L74
pub mod binary;
pub mod client_provider;
pub mod client_wrappers;
pub mod clients;
pub mod consumer_ext;
pub mod http;
mod leader_aware;
pub mod prelude;
pub mod quic;
pub mod session;
pub mod stream_builder;
pub mod tcp;
mod vsr;
pub mod websocket;

/// Rust SDK version sent in the login-register version prefix; must be this
/// crate's version, see `VsrSessionControl::sdk_version`.
pub(crate) const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");
