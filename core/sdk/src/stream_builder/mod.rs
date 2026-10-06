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

//! Builds a connected [`IggyProducer`] or [`IggyConsumer`] from one configuration value.
//!
//! The [`clients`](crate::clients) module builds a producer or a consumer through a builder chain
//! and an `init()` call. This module wraps that chain. You describe the stream, the topic and the
//! options once, and a single call returns a client that is ready to send or to read messages.
//!
//! | Entry point | Returns | Configuration |
//! | --- | --- | --- |
//! | [`IggyStream`] | an [`IggyProducer`] and an [`IggyConsumer`] | [`IggyStreamConfig`] |
//! | [`IggyStreamProducer`] | an [`IggyProducer`] | [`IggyProducerConfig`] |
//! | [`IggyStreamConsumer`] | an [`IggyConsumer`] | [`IggyConsumerConfig`] |
//!
//! Each entry point is a namespace of associated functions. It holds no state, and you never
//! create a value of it. Each one offers the same three functions:
//!
//! - `build()` uses an [`IggyClient`] that is connected already.
//! - `with_client_from_url()`, called `with_client_from_connection_string()` on [`IggyStream`],
//!   creates and connects the client as well, and returns it next to the clients it built.
//! - `build_iggy_client()` only creates and connects an [`IggyClient`].
//!
//! # What you give up
//!
//! A configuration value covers the options that most applications set. The builders behind it
//! reach further. Build through [`IggyClient::producer()`], [`IggyClient::consumer()`] or
//! [`IggyClient::consumer_group()`] when you need one of these:
//!
//! - A background producer. This module always builds a direct producer, see [`IggyProducer`].
//! - A custom [`Partitioner`], or a producer that does not create its stream and topic.
//! - A consumer that does not join or create its group, or that replays messages.
//! - Any option this module does not carry, such as the expiry and the size limit of a topic.
//!
//! # Reading the messages
//!
//! An [`IggyConsumer`] built here is a [`Stream`](futures::Stream), so
//! [`StreamExt::next`](futures_util::StreamExt::next) reads from it. To run that loop without
//! writing it, use [`IggyConsumerMessageExt::consume_messages`] from the
//! [`consumer_ext`](crate::consumer_ext) module.
//!
//! # Examples
//!
//! Build a producer and a consumer for one topic, send a message, and read it back:
//!
//! ```rust,no_run
//! use futures_util::StreamExt;
//! use iggy::prelude::*;
//! use std::str::FromStr;
//!
//! # async fn example() -> Result<(), IggyError> {
//! let config = IggyStreamConfig::from_stream_topic(
//!     "my-stream",
//!     "my-topic",
//!     100,
//!     IggyDuration::new_from_secs(1),
//!     IggyDuration::new_from_secs(1),
//! )?;
//!
//! let (client, producer, mut consumer) = IggyStream::with_client_from_connection_string(
//!     "iggy://iggy:iggy@localhost:8090",
//!     &config,
//! )
//! .await?;
//!
//! // Both clients are initialized already, so no init() call is needed.
//! producer.send_one(IggyMessage::from_str("hello")?).await?;
//!
//! if let Some(Ok(received)) = consumer.next().await {
//!     println!("Offset: {}", received.message.header.offset);
//! }
//!
//! consumer.shutdown().await?;
//! producer.shutdown().await;
//! client.shutdown().await?;
//! # Ok(())
//! # }
//! ```
//!
//! [`IggyClient`]: crate::prelude::IggyClient
//! [`IggyClient::consumer()`]: crate::prelude::IggyClient::consumer
//! [`IggyClient::consumer_group()`]: crate::prelude::IggyClient::consumer_group
//! [`IggyClient::producer()`]: crate::prelude::IggyClient::producer
//! [`IggyConsumer`]: crate::prelude::IggyConsumer
//! [`IggyConsumerMessageExt::consume_messages`]: crate::prelude::IggyConsumerMessageExt::consume_messages
//! [`IggyProducer`]: crate::prelude::IggyProducer
//! [`Partitioner`]: crate::prelude::Partitioner

mod build;
mod config;
mod iggy_stream;
mod iggy_stream_consumer;
mod iggy_stream_producer;

pub use config::{IggyConsumerConfig, IggyConsumerConfigBuilder};
pub use config::{IggyProducerConfig, IggyProducerConfigBuilder};
pub use config::{IggyStreamConfig, IggyStreamConfigBuilder};
pub use iggy_stream::IggyStream;
pub use iggy_stream_consumer::IggyStreamConsumer;
pub use iggy_stream_producer::IggyStreamProducer;
