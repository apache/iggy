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
use crate::clients::producer::IggyProducer;
use crate::prelude::{IggyError, SystemClient};
use crate::stream_builder::{IggyProducerConfig, build};
use tracing::trace;

/// Builds a connected [`IggyProducer`] from one [`IggyProducerConfig`].
///
/// `IggyStreamProducer` is a namespace of associated functions. It holds no state, and you never
/// create a value of it. Use it for a process that only writes to a topic. To build a producer and
/// a consumer together, use [`IggyStream`].
///
/// # Building the producer
///
/// [`build()`](Self::build) takes an [`IggyClient`] that is connected already. It sends a ping
/// first and fails with [`IggyError::NotConnected`] when that ping fails.
/// [`with_client_from_url()`](Self::with_client_from_url) creates and connects the client itself,
/// so it skips the ping and returns the client next to the producer.
///
/// Both functions then build the producer from the fields of the configuration and await
/// [`IggyProducer::init()`], which creates the stream and the topic when they are missing. The
/// producer comes back initialized, so you can send messages right away.
///
/// # What the configuration sets
///
/// | Field of [`IggyProducerConfig`] | Effect |
/// | --- | --- |
/// | [`stream_name()`], [`topic_name()`] | where messages are appended |
/// | [`topic_partitions_count()`] | partitions of a topic this call creates |
/// | [`batch_length()`], [`linger_time()`] | the [`DirectConfig`] of the producer |
/// | [`partitioning()`] | which partition a batch lands in |
/// | [`send_retries_count()`], [`send_retries_interval()`] | retrying a failed request |
/// | [`encryptor()`] | encrypting payloads and user headers, and it replaces the client's own |
///
/// The stream and the topic are always created when they are missing. A topic created here gets
/// the server defaults for message expiry and maximum size. The producer is always a direct
/// producer, so every [`send()`] awaits the server. Build through [`IggyClient::producer()`] when
/// you need a background producer, a custom [`Partitioner`], or a producer that leaves the stream
/// and the topic alone.
///
/// # Examples
///
/// Connect and send one message:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let config = IggyProducerConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
/// )?;
/// let (client, producer) =
///     IggyStreamProducer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
///         .await?;
///
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
///
/// producer.shutdown().await;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// Create a topic with ten partitions and key every message to one of them:
///
/// ```rust,no_run
/// use iggy::prelude::*;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), IggyError> {
/// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
/// client.connect().await?;
///
/// let config = IggyProducerConfig::builder()
///     .stream_id(Identifier::from_str_value("my-stream")?)
///     .stream_name("my-stream")
///     .topic_id(Identifier::from_str_value("my-topic")?)
///     .topic_name("my-topic")
///     .topic_partitions_count(10)
///     .batch_length(100)
///     .linger_time(IggyDuration::new_from_secs(1))
///     .partitioning(Partitioning::messages_key_str("my-key")?)
///     .send_retries_count(3)
///     .send_retries_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// let producer = IggyStreamProducer::build(&client, &config).await?;
/// producer.send_one(IggyMessage::from_str("hello")?).await?;
/// # Ok(())
/// # }
/// ```
///
/// # Shutting down
///
/// `IggyStreamProducer` builds the producer and then steps out of the way, so stopping it is your
/// job. A direct producer buffers nothing, so [`IggyProducer::shutdown()`] does nothing here.
/// [`IggyClient::shutdown()`] closes the connection.
///
/// [`DirectConfig`]: crate::prelude::DirectConfig
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyClient::producer()`]: crate::prelude::IggyClient::producer
/// [`IggyClient::shutdown()`]: crate::prelude::Client::shutdown
/// [`IggyProducer`]: crate::prelude::IggyProducer
/// [`IggyProducer::init()`]: crate::prelude::IggyProducer::init
/// [`IggyProducer::shutdown()`]: crate::prelude::IggyProducer::shutdown
/// [`Partitioner`]: crate::prelude::Partitioner
/// [`batch_length()`]: crate::prelude::IggyProducerConfig::batch_length
/// [`encryptor()`]: crate::prelude::IggyProducerConfig::encryptor
/// [`linger_time()`]: crate::prelude::IggyProducerConfig::linger_time
/// [`partitioning()`]: crate::prelude::IggyProducerConfig::partitioning
/// [`send()`]: crate::prelude::IggyProducer::send
/// [`send_retries_count()`]: crate::prelude::IggyProducerConfig::send_retries_count
/// [`send_retries_interval()`]: crate::prelude::IggyProducerConfig::send_retries_interval
/// [`stream_name()`]: crate::prelude::IggyProducerConfig::stream_name
/// [`topic_name()`]: crate::prelude::IggyProducerConfig::topic_name
/// [`topic_partitions_count()`]: crate::prelude::IggyProducerConfig::topic_partitions_count
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStreamProducer;

impl IggyStreamProducer {
    /// Builds an initialized [`IggyProducer`] on an existing client.
    ///
    /// The call creates the stream and the topic when they are missing.
    ///
    /// # Examples
    ///
    /// Build a producer on a client that is connected already:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
    /// client.connect().await?;
    ///
    /// let config = IggyProducerConfig::default();
    /// let producer = IggyStreamProducer::build(&client, &config).await?;
    /// # let _ = producer;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::NotConnected`] when the ping that opens the call fails.
    /// - Any error returned by [`IggyProducer::init()`].
    ///
    /// [`IggyProducer`]: crate::prelude::IggyProducer
    /// [`IggyProducer::init()`]: crate::prelude::IggyProducer::init
    pub async fn build(
        client: &IggyClient,
        config: &IggyProducerConfig,
    ) -> Result<IggyProducer, IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(client, config).await?;

        Ok(iggy_producer)
    }

    /// Creates the client as well, and returns it with the initialized producer.
    ///
    /// The client is created from `connection_string` and connected, so this function sends no
    /// ping. The rest matches [`build()`](Self::build). Keep the returned [`IggyClient`], because
    /// you need it to shut the connection down.
    ///
    /// # Examples
    ///
    /// Connect and build the producer in one call:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let config = IggyProducerConfig::default();
    /// let (client, producer) =
    ///     IggyStreamProducer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
    ///         .await?;
    /// # let _ = (client, producer);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConnectionString`] when `connection_string` cannot be parsed.
    /// - Any error raised while connecting to the server.
    /// - Any error returned by [`IggyProducer::init()`].
    ///
    /// [`IggyClient`]: crate::prelude::IggyClient
    /// [`IggyProducer::init()`]: crate::prelude::IggyProducer::init
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &IggyProducerConfig,
    ) -> Result<(IggyClient, IggyProducer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client::build_iggy_client(connection_string).await?;

        trace!("Build iggy producer");
        // The producer creates stream and topic if it doesn't exist
        let iggy_producer = build::build_iggy_producer(&client, config).await?;

        Ok((client, iggy_producer))
    }

    /// Creates an [`IggyClient`] from `connection_string` and connects it.
    ///
    /// This builds no producer. It is a shorthand for [`IggyClient::from_connection_string()`]
    /// followed by [`IggyClient::connect()`], and it is useful when one client serves several
    /// calls to [`build()`](Self::build).
    ///
    /// # Examples
    ///
    /// Connect one client and build a producer for two topics:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyStreamProducer::build_iggy_client("iggy://iggy:iggy@localhost:8090").await?;
    ///
    /// let orders = IggyProducerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "orders",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    /// let shipments = IggyProducerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "shipments",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    ///
    /// let order_producer = IggyStreamProducer::build(&client, &orders).await?;
    /// let shipment_producer = IggyStreamProducer::build(&client, &shipments).await?;
    /// # let _ = (order_producer, shipment_producer);
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
