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

//! Apache Iggy is a high-performance, persistent message streaming platform written in Rust,
//! capable of processing millions of messages per second with ultra-low latency.
//! It is part of the [`Apache Incubating Program`] of the [`Apache Software Foundation`] (ASF).
//!
//! **This library is the Apache Iggy SDK.**
//! It exposes a low-level and a high-level API for the Apache Iggy message streaming infrastructure for the Rust programming language.
//! SDKs for other programming languages can be found in [`core/foreign`] of the root repository on GitHub.
//!
//! The core of the Iggy server is a persisted append-only log data structure.
//! It is concerned with allowing read and writes in the most efficient way.
//! Reading and writing to the server is the domain of this SDK.
//! The server exposes *commands* that can be triggered to change its state.
//! These commands allow administrative tasks, such as handling users, permissions and setting up streams and topics
//! or writing and reading messages from the log.
//! A comprehensive overview of commands can be found in the [`schema spec`] on the website or checking the [`server command enum`] within the source code.
//!
//! The SDK provides tools to build production ready message-streaming applications.
//! It exposes its functionality at two levels. The [high-level API](#high-level-api)
//! is transport-agnostic and ships with the batching, retry, offset-tracking, and
//! connection-management machinery a production application needs. The
//! [low-level API](#low-level-api) is the set of concrete transport clients that
//! speak the wire protocol directly and that the high-level API is built on top of.
//! It is recommended to start with the high-level API, and utilize the low level API
//! in case the high-level API cannot satisfy your requirements.
//!
//! # High-level API
//!
//! The high-level API is most likely what you are looking for, especially if you are new to building
//! message-streaming applications with Iggy.
//! Clients provided by the high-level API already provide common message-streaming features that
//! you would otherwise need to build yourself.
//!
//! There are three client types:
//! - [`IggyClient`] is the entry point and the full API surface. It owns the
//!   connection and implements every domain trait, including [`MessageClient`]
//!   with the raw [`send_messages`] and [`poll_messages`] primitives. Each call
//!   is a single, stateless request: no batching, retries, offset tracking, or
//!   polling loop.
//! - [`IggyProducer`] is a stateful helper for high-throughput sending, built on
//!   [`send_messages`].
//! - [`IggyConsumer`] is a stateful helper for continuous consumption, built on
//!   [`poll_messages`].
//!
//! You do not construct the producer and consumer independently. Spawn them
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
//! The [`IggyProducer`] adds, on top of [`send_messages`]:
//! - **Background batching** that flushes by size, message count, or a linger
//!   interval, instead of one network round-trip per send.
//! - **Retries** with a configurable count and interval (three attempts one
//!   second apart by default).
//! - A pluggable **partitioning strategy**, so the target partition is not
//!   passed on every call.
//! - **In-flight and ordering control**, optional payload **encryption**, and
//!   `create_stream_if_not_exists` / `create_topic_if_not_exists` convenience.
//!
//! The [`IggyConsumer`] adds, on top of [`poll_messages`]:
//! - A [`futures::Stream`] implementation, so a `while let Some(message) =
//!   consumer.next().await` loop drives polling, paging, and the poll interval
//!   for you.
//! - A **polling strategy** (`next`, `offset`, or `timestamp`) that tracks
//!   position instead of taking an offset on every call.
//! - **Auto-commit** and offset storage on an interval or after a number of
//!   messages, so a restart resumes where it left off.
//! - **Auto-join** of consumer groupsa and assignment refresh should the server have
//!   assigned the consumer another partition
//! - Reconnection handling should the client disconnect
//! - Payload **decryption**
//!
//! # Stream builder API
//!
//! The stream builder API is a convenient way to use the high-level API.
//! [`IggyStream`], [`IggyStreamProducer`], and
//! [`IggyStreamConsumer`] construct everything at once.
//! You can pass an [`IggyClient`] (or just a connection string) together with a config,
//! and they hand back a ready, connected [`IggyProducer`] / [`IggyConsumer`].
//! Compared to the **high-level API**, it changes how you construct
//! producers and consumers, not what they can do. Instead of chaining an
//! [`IggyProducerBuilder`] / [`IggyConsumerBuilder`] and setting each option
//! with a method call, you describe the whole setup once in an
//! [`IggyStreamConfig`] and build from it. The result is
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
//! - You forked the server and need to issue a command the typed API does not recognize and want the
//!   raw [`send_raw_with_response`](BinaryTransport::send_raw_with_response) instruction.
//!
//! If none of these apply, the high-level API gives you the same reach with far
//! less to get wrong.
//!
//! # Async runtime
//!
//! The SDK is async and runs on the [Tokio] runtime. Note, this is a hard
//! requirement rather not optional. The SDK uses [quinn] (for QUIC), [reqwest] (for HTTP),
//! [tokio-tungstenite] (for WebSocket) and [tokio-rustls] (for TLS) which all build on Tokio.
//! The SDK also spawns its own background work with [`tokio::spawn`] (the
//! [`IggyClient::connect`] heartbeat, and the [`IggyProducer`] and
//! [`IggyConsumer`] tasks) and drives timeouts, retries, and poll intervals with
//! [`tokio::time`]. Note that dropping to the low-level transport clients does
//! not change this. They spawn and time out on Tokio internally too.
//! Thus, everything you do with the Rust SDK must happen inside a Tokio runtime.
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
//!     let mut consumer = client.consumer("consumer_name", "stream_name", "topic_name", 1)?.build();
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
//! [`futures::Stream`]: https://docs.rs/futures/latest/futures/stream/trait.Stream.html
//! [`TcpClient`]: crate::prelude::TcpClient
//! [`QuicClient`]: crate::quic::quic_client::QuicClient
//! [`WebSocketClient`]: crate::prelude::WebSocketClient
//! [`HttpClient`]: crate::http::http_client::HttpClient
//! [`Client`]: crate::prelude::Client
//! [`StreamClient`]: crate::prelude::StreamClient
//! [`TopicClient`]: crate::prelude::TopicClient
//! [`ClientWrapper`]: crate::prelude::ClientWrapper
//! [`IggyRwLock`]: crate::prelude::locking::IggyRwLock
//! [`BinaryTransport`]: crate::binary::BinaryTransport
//! [`BinaryTransport::send_raw_with_response`]: crate::binary::BinaryTransport::send_raw_with_response
//! [`IggyClient::send_binary_request`]: crate::prelude::IggyClient::send_binary_request
//! [`IggyClient::send_http_request`]: crate::prelude::IggyClient::send_http_request
//! [`IggyStream`]: crate::prelude::IggyStream
//! [`IggyStream::build`]: crate::prelude::IggyStream::build
//! [`IggyStream::with_client_from_connection_string`]: crate::prelude::IggyStream::with_client_from_connection_string
//! [`IggyStreamProducer`]: crate::prelude::IggyStreamProducer
//! [`IggyStreamConsumer`]: crate::prelude::IggyStreamConsumer
//! [`IggyStreamConfig`]: crate::prelude::IggyStreamConfig
//! [`IggyProducerConfig`]: crate::prelude::IggyProducerConfig
//! [`IggyConsumerConfig`]: crate::prelude::IggyConsumerConfig
//! [`IggyProducerBuilder`]: crate::prelude::IggyProducerBuilder
//! [`IggyConsumerBuilder`]: crate::prelude::IggyConsumerBuilder
//! [`IggyClient::connect`]: crate::prelude::Client::connect
//! [`IggyError`]: crate::prelude::IggyError
//!
//! [Tokio]: https://tokio.rs
//! [`tokio::spawn`]: https://docs.rs/tokio/latest/tokio/task/fn.spawn.html
//! [`tokio::time`]: https://docs.rs/tokio/latest/tokio/time/index.html
//! [quinn]: https://docs.rs/quinn
//! [reqwest]: https://docs.rs/reqwest
//! [tokio-tungstenite]: https://docs.rs/tokio-tungstenite
//! [tokio-rustls]: https://docs.rs/tokio-rustls
//! [`StreamExt`]: https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html
//!
//! [`Apache Incubating Program`]: https://incubator.apache.org/
//! [`Apache Software Foundation`]: https://www.apache.org/
//! [`core/foreign`]: https://github.com/apache/iggy/tree/master/foreign
//! [`schema spec`]: https://iggy.apache.org/docs/server/schema/
//! [`server command enum`]: https://github.com/apache/iggy/blob/3e27ebc8dd5dbf257b816993908dc0747c4f8849/core/server/src/binary/command.rs#L74
//! [`website`]: https://iggy.apache.org/docs/introduction/architecture/
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
