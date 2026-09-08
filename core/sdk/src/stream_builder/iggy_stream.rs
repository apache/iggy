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

use crate::clients::client::IggyClient;
use crate::clients::consumer::IggyConsumer;
use crate::clients::producer::IggyProducer;
use crate::prelude::{IggyError, SystemClient};
use crate::stream_builder::{IggyStreamConfig, build};
use tracing::trace;

/// Builds a connected [`IggyProducer`] and [`IggyConsumer`] pair from one [`IggyStreamConfig`].
///
/// `IggyStream` is a namespace of associated functions. It holds no state, and you never create a
/// value of it. Use it when one process both writes to and reads from a topic. For a process that only
/// produces or consumes, use [`IggyStreamProducer`] or [`IggyStreamConsumer`].
///
/// # Building the pair
///
/// [`build()`](Self::build) takes an [`IggyClient`] that is connected already.
/// [`with_client_from_connection_string()`](Self::with_client_from_connection_string) creates and
/// connects the client as well, and returns it next to the pair. Both functions do the same three
/// steps:
///
/// 1. Send a ping. A failed ping fails the call with [`IggyError::NotConnected`].
/// 2. Build the producer from [`IggyStreamConfig::producer_config()`] and await
///    [`IggyProducer::init()`]. This step creates the stream and the topic when they are missing.
/// 3. Build the consumer from [`IggyStreamConfig::consumer_config()`] and await
///    [`IggyConsumer::init()`]. This step creates the consumer group and joins it.
///
/// Both clients come back initialized, so you can send and read messages right away.
///
/// Because the producer creates the stream and the topic on every call, the
/// [`create_stream_if_not_exists()`] and [`create_topic_if_not_exists()`] switches of the consumer
/// configuration have no effect here. They apply to [`IggyStreamConsumer`] only. The topic gets
/// [`topic_partitions_count()`] partitions, and the server defaults for message expiry and maximum
/// size.
///
/// The two halves of an [`IggyStreamConfig`] carry their own stream and topic. A configuration
/// built with [`IggyStreamConfig::new()`] can therefore point the producer and the consumer at
/// different topics. [`IggyStreamConfig::from_stream_topic()`] gives both the same pair.
///
/// # Examples
///
/// Build a pair for one topic, send a message, and read it back:
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let config = IggyStreamConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
///     IggyDuration::new_from_secs(1),
/// )?;
/// let (producer, mut consumer) = IggyStream::build(&client, &config).await?;
///
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
///
/// if let Some(Ok(received)) = consumer.next().await {
///     println!("Offset: {}", received.message.header.offset);
/// }
///
/// consumer.shutdown().await?;
/// producer.shutdown().await;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// Hand the consumer to a task that runs the read loop for you, and stop it with a signal:
///
/// ```rust,no_run
/// use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
/// use iggy::prelude::*;
/// use std::str::FromStr;
/// use tokio::sync::oneshot;
///
/// struct PrintMessage;
///
/// impl MessageConsumer for PrintMessage {
///     async fn consume(&self, message: ReceivedMessage) -> Result<(), IggyError> {
///         println!("Offset: {}", message.message.header.offset);
///         Ok(())
///     }
/// }
///
/// # async fn example() -> Result<(), IggyError> {
/// let config = IggyStreamConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
///     IggyDuration::new_from_secs(1),
/// )?;
/// let (client, producer, mut consumer) = IggyStream::with_client_from_connection_string(
///     "iggy://iggy:iggy@localhost:8090",
///     &config,
/// )
/// .await?;
///
/// let (sender, receiver) = oneshot::channel();
/// let reader = tokio::spawn(async move {
///     consumer.consume_messages(&PrintMessage, receiver).await?;
///     // The loop leaves the consumer usable, so commit and leave the group here.
///     consumer.shutdown().await
/// });
///
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
///
/// sender.send(()).expect("the reader task is gone");
/// reader.await.expect("the reader task panicked")?;
///
/// producer.shutdown().await;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # What you give up
///
/// The producer is always a direct producer with [`batch_length()`] messages per request and
/// [`linger_time()`] between calls. The consumer always creates its group when it is missing, and
/// always joins it. Build through [`IggyClient::producer()`], [`IggyClient::consumer()`] or
/// [`IggyClient::consumer_group()`] when you need another mode, a custom [`Partitioner`], or an
/// option that [`IggyStreamConfig`] does not carry.
///
/// # Shutting down
///
/// `IggyStream` builds the clients and then steps out of the way, so stopping them is your job.
/// Call [`IggyConsumer::shutdown()`], then [`IggyProducer::shutdown()`], then
/// [`IggyClient::shutdown()`].
///
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyStreamConsumer`]: crate::prelude::IggyStreamConsumer
/// [`IggyStreamProducer`]: crate::prelude::IggyStreamProducer
/// [`IggyClient::consumer()`]: crate::prelude::IggyClient::consumer
/// [`IggyClient::consumer_group()`]: crate::prelude::IggyClient::consumer_group
/// [`IggyClient::producer()`]: crate::prelude::IggyClient::producer
/// [`IggyClient::shutdown()`]: crate::prelude::Client::shutdown
/// [`IggyConsumer`]: crate::prelude::IggyConsumer
/// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
/// [`IggyConsumer::shutdown()`]: crate::prelude::IggyConsumer::shutdown
/// [`IggyProducer`]: crate::prelude::IggyProducer
/// [`IggyProducer::init()`]: crate::prelude::IggyProducer::init
/// [`IggyProducer::shutdown()`]: crate::prelude::IggyProducer::shutdown
/// [`Partitioner`]: crate::prelude::Partitioner
/// [`batch_length()`]: crate::prelude::IggyProducerConfig::batch_length
/// [`create_stream_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_stream_if_not_exists
/// [`create_topic_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_topic_if_not_exists
/// [`linger_time()`]: crate::prelude::IggyProducerConfig::linger_time
/// [`topic_partitions_count()`]: crate::prelude::IggyProducerConfig::topic_partitions_count
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStream;

impl IggyStream {
    /// Builds an initialized [`IggyProducer`] and [`IggyConsumer`] pair on an existing client.
    ///
    /// The producer is built first and creates the stream and the topic when they are missing.
    ///
    /// # Examples
    ///
    /// Build a pair on a client that is connected already:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
    /// client.connect().await?;
    ///
    /// let config = IggyStreamConfig::default();
    /// let (producer, consumer) = IggyStream::build(&client, &config).await?;
    /// # let _ = (producer, consumer);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::NotConnected`] when the ping that opens the call fails.
    /// - Any error returned by [`IggyProducer::init()`] or [`IggyConsumer::init()`].
    ///
    /// [`IggyConsumer`]: crate::prelude::IggyConsumer
    /// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
    /// [`IggyProducer`]: crate::prelude::IggyProducer
    /// [`IggyProducer::init()`]: crate::prelude::IggyProducer::init
    pub async fn build(
        client: &IggyClient,
        config: &IggyStreamConfig,
    ) -> Result<(IggyProducer, IggyConsumer), IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(client, config.producer_config()).await?;

        trace!("Build iggy consumer");
        let iggy_consumer = build::build_iggy_consumer(client, config.consumer_config()).await?;

        Ok((iggy_producer, iggy_consumer))
    }

    /// Creates the client as well, and returns it with the initialized producer and consumer.
    ///
    /// The client is created from `connection_string` and connected. The rest matches
    /// [`build()`](Self::build). Keep the returned [`IggyClient`], because you need it to shut the
    /// connection down.
    ///
    /// # Examples
    ///
    /// Connect and build the pair in one call:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let config = IggyStreamConfig::default();
    /// let (client, producer, consumer) = IggyStream::with_client_from_connection_string(
    ///     "iggy://iggy:iggy@localhost:8090",
    ///     &config,
    /// )
    /// .await?;
    /// # let _ = (client, producer, consumer);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConnectionString`] when `connection_string` cannot be parsed.
    /// - Any error raised while connecting to the server.
    /// - Any error returned by [`build()`](Self::build).
    ///
    /// [`IggyClient`]: crate::prelude::IggyClient
    pub async fn with_client_from_connection_string(
        connection_string: &str,
        config: &IggyStreamConfig,
    ) -> Result<(IggyClient, IggyProducer, IggyConsumer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        trace!("Build iggy producer and consumer");
        let (iggy_producer, iggy_consumer) = Self::build(&client, config).await?;
        Ok((client, iggy_producer, iggy_consumer))
    }

    /// Creates an [`IggyClient`] from `connection_string` and connects it.
    ///
    /// This builds no producer and no consumer. It is a shorthand for
    /// [`IggyClient::from_connection_string()`] followed by [`IggyClient::connect()`], and it is
    /// useful when one client serves several calls to [`build()`](Self::build).
    ///
    /// # Examples
    ///
    /// Connect one client and build two pairs on it:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyStream::build_iggy_client("iggy://iggy:iggy@localhost:8090").await?;
    ///
    /// let orders = IggyStreamConfig::from_stream_topic(
    ///     "my-stream",
    ///     "orders",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    /// let shipments = IggyStreamConfig::from_stream_topic(
    ///     "my-stream",
    ///     "shipments",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    ///
    /// let (order_producer, order_consumer) = IggyStream::build(&client, &orders).await?;
    /// let (shipment_producer, shipment_consumer) = IggyStream::build(&client, &shipments).await?;
    /// # let _ = (order_producer, order_consumer, shipment_producer, shipment_consumer);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConnectionString`] when `connection_string` cannot be parsed.
    /// - Any error raised while connecting to the server.
    ///
    /// [`IggyClient`]: crate::prelude::IggyClient
    /// [`IggyClient::connect()`]: crate::prelude::Client::connect
    /// [`IggyClient::from_connection_string()`]: crate::prelude::IggyClient::from_connection_string
    pub async fn build_iggy_client(connection_string: &str) -> Result<IggyClient, IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        Ok(client)
    }
}
