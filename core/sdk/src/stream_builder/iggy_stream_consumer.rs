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
use crate::prelude::{IggyError, SystemClient};
use crate::stream_builder::{IggyConsumerConfig, build};
use tracing::trace;

/// Builds a connected [`IggyConsumer`] from one [`IggyConsumerConfig`].
///
/// `IggyStreamConsumer` is a namespace of associated functions. It holds no state, and you never
/// create a value of it. Use it for a process that only reads from a topic. To build a producer
/// and a consumer together, use [`IggyStream`].
///
/// # Building the consumer
///
/// [`build()`](Self::build) takes an [`IggyClient`] that is connected already. It sends a ping
/// first and fails with [`IggyError::NotConnected`] when that ping fails.
/// [`with_client_from_url()`](Self::with_client_from_url) creates and connects the client itself,
/// so it skips the ping and returns the client next to the consumer.
///
/// Both functions then create the stream and the topic when [`create_stream_if_not_exists()`] and
/// [`create_topic_if_not_exists()`] allow it, build the consumer, and await
/// [`IggyConsumer::init()`]. The consumer comes back initialized and, for a group, joined. You can
/// read messages right away.
///
/// # Reading from a stream that does not exist yet
///
/// Both switches are off by default. When the stream or the topic is missing and its switch is
/// off, the call logs a warning and carries on to [`IggyConsumer::init()`]. That call then retries
/// [`init_retries()`] times, [`init_interval()`] apart, and fails with
/// [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`] once the retries are
/// spent. The default allows five retries three seconds apart, which covers a producer that
/// creates the topic at the same time.
///
/// # What the configuration sets
///
/// | Field of [`IggyConsumerConfig`] | Effect |
/// | --- | --- |
/// | [`stream_name()`], [`topic_name()`] | what is read |
/// | [`consumer_name()`] | the name of the consumer, or of the group for [`ConsumerKind::ConsumerGroup`] |
/// | [`consumer_kind()`] | whether the consumer joins a group or reads one partition alone |
/// | [`partitions_count()`] | two things, see below |
/// | [`batch_length()`] | messages fetched per request |
/// | [`polling_interval()`], [`polling_strategy()`] | how often reading happens, and where it starts |
/// | [`auto_commit()`] | when offsets are committed |
/// | [`polling_retry_interval()`] | the wait between attempts while polling is blocked |
/// | [`init_retries()`], [`init_interval()`] | retries for a missing stream or topic |
/// | [`encryptor()`] | decrypting payloads and user headers, and it replaces the client's own |
///
/// [`partitions_count()`] carries two meanings. It is the partition count of a topic that this
/// call creates. For [`ConsumerKind::Consumer`] it is also the ID of the single partition the
/// consumer reads, so a standalone consumer of a topic with three partitions reads partition 3 and
/// nothing else. Run one consumer per partition to cover such a topic, or use
/// [`ConsumerKind::ConsumerGroup`] and let the server divide the partitions.
///
/// A consumer built here always creates its group when the group is missing, and always joins it.
/// Build through [`IggyClient::consumer()`] or [`IggyClient::consumer_group()`] when you need to
/// pick the partition and the group name apart, to replay messages, or to join the group yourself.
///
/// # Defaults worth knowing
///
/// [`IggyConsumerConfig`] does not repeat the defaults of [`IggyConsumerBuilder`]:
///
/// | Option | [`IggyConsumerConfig`] | [`IggyConsumerBuilder`] |
/// | --- | --- | --- |
/// | [`polling_strategy()`] | [`PollingStrategy::last()`], so a run starts at the end of the partition and reads no history | [`PollingStrategy::next()`] |
/// | [`auto_commit()`] | [`AutoCommitWhen::PollingMessages`], with no interval | the same trigger, plus a one-second interval |
/// | [`batch_length()`] | 100 | 1000 |
/// | [`init_retries()`] | five retries, three seconds apart | none |
///
/// With [`PollingStrategy::last()`] the stored offset is never consulted, so a restarted consumer
/// skips whatever arrived while it was down. Set [`PollingStrategy::next()`] to resume where the
/// previous run stopped.
///
/// An encryptor rules out the default [`auto_commit()`]. That setting commits a batch before it is
/// decrypted, so [`IggyConsumer::init()`] rejects the pair with
/// [`IggyError::InvalidConfiguration`]. Pick another [`AutoCommit`] variant next to an encryptor.
///
/// # Examples
///
/// Connect and read messages as a group member:
///
/// ```rust,no_run
/// use futures_util::StreamExt;
/// use iggy::prelude::*;
///
/// # async fn example() -> Result<(), IggyError> {
/// let config = IggyConsumerConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
/// )?;
/// let (client, mut consumer) =
///     IggyStreamConsumer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
///         .await?;
///
/// while let Some(received) = consumer.next().await {
///     match received {
///         Ok(received) => println!("Offset: {}", received.message.header.offset),
///         Err(error) => eprintln!("Failed to read a message: {error}"),
///     }
/// }
///
/// consumer.shutdown().await?;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// A group member that creates the topic it reads, resumes from the stored offset, and commits
/// once a message has been handled:
///
/// ```rust,no_run
/// use iggy::consumer_ext::{IggyConsumerMessageExt, MessageConsumer};
/// use iggy::prelude::*;
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
/// let config = IggyConsumerConfig::builder()
///     .stream_id(Identifier::from_str_value("my-stream")?)
///     .stream_name("my-stream")
///     .topic_id(Identifier::from_str_value("my-topic")?)
///     .topic_name("my-topic")
///     .consumer_name("order-workers")
///     .consumer_kind(ConsumerKind::ConsumerGroup)
///     .create_stream_if_not_exists(true)
///     .create_topic_if_not_exists(true)
///     .partitions_count(3)
///     .batch_length(100)
///     .polling_interval(IggyDuration::new_from_secs(1))
///     .polling_strategy(PollingStrategy::next())
///     .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
///     .auto_commit(AutoCommit::After(AutoCommitAfter::ConsumingEachMessage))
///     .init_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// let (client, mut consumer) =
///     IggyStreamConsumer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
///         .await?;
///
/// let (sender, receiver) = oneshot::channel();
/// tokio::spawn(async move {
///     tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
///     let _ = sender.send(());
/// });
///
/// // The AutoCommitAfter variants only commit under consume_messages().
/// consumer.consume_messages(&PrintMessage, receiver).await?;
///
/// consumer.shutdown().await?;
/// client.shutdown().await?;
/// # Ok(())
/// # }
/// ```
///
/// # Shutting down
///
/// `IggyStreamConsumer` builds the consumer and then steps out of the way, so stopping it is your
/// job. [`IggyConsumer::shutdown()`] commits the reading position and leaves the group, and
/// [`IggyClient::shutdown()`] closes the connection.
///
/// [`AutoCommit`]: crate::prelude::AutoCommit
/// [`AutoCommitWhen::PollingMessages`]: crate::prelude::AutoCommitWhen::PollingMessages
/// [`ConsumerKind::Consumer`]: crate::prelude::ConsumerKind::Consumer
/// [`ConsumerKind::ConsumerGroup`]: crate::prelude::ConsumerKind::ConsumerGroup
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyClient::consumer()`]: crate::prelude::IggyClient::consumer
/// [`IggyClient::consumer_group()`]: crate::prelude::IggyClient::consumer_group
/// [`IggyClient::shutdown()`]: crate::prelude::Client::shutdown
/// [`IggyConsumer`]: crate::prelude::IggyConsumer
/// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
/// [`IggyConsumer::shutdown()`]: crate::prelude::IggyConsumer::shutdown
/// [`IggyConsumerBuilder`]: crate::prelude::IggyConsumerBuilder
/// [`PollingStrategy::last()`]: crate::prelude::PollingStrategy::last
/// [`PollingStrategy::next()`]: crate::prelude::PollingStrategy::next
/// [`auto_commit()`]: crate::prelude::IggyConsumerConfig::auto_commit
/// [`batch_length()`]: crate::prelude::IggyConsumerConfig::batch_length
/// [`consumer_kind()`]: crate::prelude::IggyConsumerConfig::consumer_kind
/// [`consumer_name()`]: crate::prelude::IggyConsumerConfig::consumer_name
/// [`create_stream_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_stream_if_not_exists
/// [`create_topic_if_not_exists()`]: crate::prelude::IggyConsumerConfig::create_topic_if_not_exists
/// [`encryptor()`]: crate::prelude::IggyConsumerConfig::encryptor
/// [`init_interval()`]: crate::prelude::IggyConsumerConfig::init_interval
/// [`init_retries()`]: crate::prelude::IggyConsumerConfig::init_retries
/// [`partitions_count()`]: crate::prelude::IggyConsumerConfig::partitions_count
/// [`polling_interval()`]: crate::prelude::IggyConsumerConfig::polling_interval
/// [`polling_retry_interval()`]: crate::prelude::IggyConsumerConfig::polling_retry_interval
/// [`polling_strategy()`]: crate::prelude::IggyConsumerConfig::polling_strategy
/// [`stream_name()`]: crate::prelude::IggyConsumerConfig::stream_name
/// [`topic_name()`]: crate::prelude::IggyConsumerConfig::topic_name
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct IggyStreamConsumer;

impl IggyStreamConsumer {
    /// Builds an initialized [`IggyConsumer`] on an existing client.
    ///
    /// The call creates the stream and the topic when they are missing and the configuration
    /// allows it.
    ///
    /// # Examples
    ///
    /// Build a consumer on a client that is connected already:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;
    /// client.connect().await?;
    ///
    /// let config = IggyConsumerConfig::default();
    /// let consumer = IggyStreamConsumer::build(&client, &config).await?;
    /// # let _ = consumer;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::NotConnected`] when the ping that opens the call fails.
    /// - Any error raised while creating the stream or the topic.
    /// - Any error returned by [`IggyConsumer::init()`], such as
    ///   [`IggyError::StreamNameNotFound`] when the stream is still missing once the retries are
    ///   spent.
    ///
    /// [`IggyConsumer`]: crate::prelude::IggyConsumer
    /// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
    pub async fn build(
        client: &IggyClient,
        config: &IggyConsumerConfig,
    ) -> Result<IggyConsumer, IggyError> {
        trace!("Check if client is connected");
        if client.ping().await.is_err() {
            return Err(IggyError::NotConnected);
        }

        trace!("Check if stream and topic exist");
        build::build_iggy_stream_topic_if_not_exists(client, config).await?;

        trace!("Build iggy consumer");
        let iggy_consumer = build::build_iggy_consumer(client, config).await?;

        Ok(iggy_consumer)
    }

    /// Creates the client as well, and returns it with the initialized consumer.
    ///
    /// The client is created from `connection_string` and connected, so this function sends no
    /// ping. The rest matches [`build()`](Self::build). Keep the returned [`IggyClient`], because
    /// you need it to shut the connection down.
    ///
    /// # Examples
    ///
    /// Connect and build the consumer in one call:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let config = IggyConsumerConfig::default();
    /// let (client, consumer) =
    ///     IggyStreamConsumer::with_client_from_url("iggy://iggy:iggy@localhost:8090", &config)
    ///         .await?;
    /// # let _ = (client, consumer);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// - [`IggyError::InvalidConnectionString`] when `connection_string` cannot be parsed.
    /// - Any error raised while connecting to the server, or while creating the stream or the
    ///   topic.
    /// - Any error returned by [`IggyConsumer::init()`].
    ///
    /// [`IggyClient`]: crate::prelude::IggyClient
    /// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
    pub async fn with_client_from_url(
        connection_string: &str,
        config: &IggyConsumerConfig,
    ) -> Result<(IggyClient, IggyConsumer), IggyError> {
        trace!("Build and connect iggy client");
        let client = build::build_iggy_client(connection_string).await?;

        trace!("Check if stream and topic exist");
        build::build_iggy_stream_topic_if_not_exists(&client, config).await?;

        trace!("Build iggy consumer");
        let iggy_consumer = build::build_iggy_consumer(&client, config).await?;

        Ok((client, iggy_consumer))
    }

    /// Creates an [`IggyClient`] from `connection_string` and connects it.
    ///
    /// This builds no consumer. It is a shorthand for [`IggyClient::from_connection_string()`]
    /// followed by [`IggyClient::connect()`], and it is useful when one client serves several
    /// calls to [`build()`](Self::build).
    ///
    /// # Examples
    ///
    /// Connect one client and build a consumer for two topics:
    ///
    /// ```rust,no_run
    /// use iggy::prelude::*;
    ///
    /// # async fn example() -> Result<(), IggyError> {
    /// let client = IggyStreamConsumer::build_iggy_client("iggy://iggy:iggy@localhost:8090").await?;
    ///
    /// let orders = IggyConsumerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "orders",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    /// let shipments = IggyConsumerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "shipments",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    ///
    /// let order_consumer = IggyStreamConsumer::build(&client, &orders).await?;
    /// let shipment_consumer = IggyStreamConsumer::build(&client, &shipments).await?;
    /// # let _ = (order_consumer, shipment_consumer);
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
