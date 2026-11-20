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

use crate::clients::reader_builder::{DEFAULT_MAX_WAIT, DEFAULT_PREFETCH_BYTES};
use crate::prelude::{
    ConsumerKind, EncryptorKind, Identifier, IggyError, NonZeroIggyDuration, PollOptions,
    PollingStrategy, Validatable,
};
use bon::Builder;
use std::str::FromStr;
use std::sync::Arc;
use tracing::error;

const DEFAULT_PARTITION_ID: u32 = 0;

/// Describes the consumer that [`IggyStreamConsumer`] and [`IggyStream`] build.
///
/// The value is a plain description. It never talks to the server, and changing it later has no
/// effect on a consumer that is built already.
///
/// # Creating a configuration
///
/// | Constructor | Use it for |
/// | --- | --- |
/// | [`builder()`](Self::builder) | naming every field you set |
/// | [`from_stream_topic()`](Self::from_stream_topic) | `ConsumerGroup` for one stream and one topic, with `poll_options`, defaults elsewhere |
/// | [`new()`](Self::new) | most fields, positional |
/// | [`default()`](Self::default) | the stream `test_stream` and the topic `test_topic`, for examples and tests |
///
/// The builder requires every field that is not an [`Option`] and carries no
/// `#[builder(default)]`. It lets you leave out [`prefetch_messages()`](Self::prefetch_messages),
/// [`init_retries()`](Self::init_retries) and [`encryptor()`](Self::encryptor), which stay
/// unset, and [`prefetch_bytes()`](Self::prefetch_bytes), [`partitions()`](Self::partitions),
/// [`poll_options()`](Self::poll_options) and [`polling_strategy()`](Self::polling_strategy),
/// which take the defaults below.
///
/// # Fields and defaults
///
/// The defaults below are what [`default()`](Self::default) sets.
/// [`from_stream_topic()`](Self::from_stream_topic) sets the same, except for the consumer name
/// and the `poll_options` you pass.
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`stream_name()`](Self::stream_name), [`topic_name()`](Self::topic_name) | `test_stream`, `test_topic` | name of the stream and topic that is either looked up or automatically created if enabled |
/// | [`consumer_name()`](Self::consumer_name) | `test_consumer`, or `consumer-{stream}-{topic}` | the name of the consumer, or of its group |
/// | [`consumer_kind()`](Self::consumer_kind) | [`ConsumerKind::ConsumerGroup`] | whether the consumer joins a group or reads chosen partitions alone |
/// | [`partitions()`](Self::partitions) | partition 0 | partitions for ordinary consumers, ignored by groups |
/// | [`partitions_count()`](Self::partitions_count) | 1 | see below |
/// | [`create_stream_if_not_exists()`](Self::create_stream_if_not_exists) | off | creating a missing stream |
/// | [`create_topic_if_not_exists()`](Self::create_topic_if_not_exists) | off | creating a missing topic |
/// | [`poll_options()`](Self::poll_options) | [`PollOptions::wait`] of five seconds | the limits of each request and the [`CommitPolicy`], [`CommitPolicy::Manual`] by default |
/// | [`polling_strategy()`](Self::polling_strategy) | [`PollingStrategy::next()`] | where reading a partition without a position starts |
/// | [`polling_retry_interval()`](Self::polling_retry_interval) | one second | how long a refused partition waits before the next read |
/// | [`prefetch_bytes()`](Self::prefetch_bytes), [`prefetch_messages()`](Self::prefetch_messages) | 16 MiB, unset | the requested budget for buffered and in-flight batches |
/// | [`init_retries()`](Self::init_retries), [`init_interval()`](Self::init_interval) | five retries, three seconds apart | retries for a missing stream or topic |
/// | [`encryptor()`](Self::encryptor) | none | decrypting payloads and user headers |
///
/// ## Some callouts on defaults
///
/// - [`partitions_count()`](Self::partitions_count) is the number of partitions that the build
///   creates per topic. The partitions a consumer reads are
///   [`partitions()`](Self::partitions), which a [`ConsumerKind::ConsumerGroup`] ignores
///   because the server assigns its partitions instead.
/// - [`encryptor()`](Self::encryptor) if you build from an [`IggyClient`] that already has an `encryptor` the one you define here
///   replaces the encryptor of the [`IggyClient`] for consumer.
///   An encryptor rules out [`CommitPolicy::OnRead`], because the server would store the offset
///   of a batch before it is decrypted. If decryption fails the offset would be stored even though
///   the message could not be consumed. Hence, [`IggyConsumer::init()`] refuses the pair with
///   [`IggyError::InvalidConfiguration`].
/// - [`CommitPolicy::Manual`] stores no offset by itself. With [`PollingStrategy::next()`] a
///   restarted consumer reads the same messages again, unless the application commits. Set
///   [`CommitPolicy::Auto`] or [`CommitPolicy::OnRead`] in the poll options to store them.
/// - [`create_stream_if_not_exists()`](Self::create_stream_if_not_exists) and
///   [`create_topic_if_not_exists()`](Self::create_topic_if_not_exists) are off, so a consumer alone
///   creates nothing. It waits out [`init_retries()`](Self::init_retries) instead, which covers a
///   producer that creates the topic at the same time.
///
/// [`IggyStreamConsumer`] creates the stream and the topic under
/// [`stream_name()`](Self::stream_name) and [`topic_name()`](Self::topic_name) if the create flags
/// above ask for it, and [`IggyConsumer::init()`] then looks the stream and the topic up under
/// those same two names. If neither exists and creation is off, [`IggyConsumer::init()`] fails
/// with [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`].
///
/// # Examples
///
/// One topic, with the defaults for everything else:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn main() -> Result<(), IggyError> {
/// let config = IggyConsumerConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     PollOptions::wait(IggyDuration::ONE_SECOND),
/// )?;
///
/// assert_eq!(config.consumer_name(), "consumer-my-stream-my-topic");
/// assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
/// assert_eq!(config.polling_strategy(), PollingStrategy::next());
/// # Ok(())
/// # }
/// ```
///
/// A group that creates the topic it reads, resumes from the stored offset, and stores an offset
/// once a message has been handled:
///
/// ```rust
/// use iggy::prelude::*;
///
/// let config = IggyConsumerConfig::builder()
///     .stream_name("my-stream")
///     .topic_name("my-topic")
///     .consumer_name("order-workers")
///     .consumer_kind(ConsumerKind::ConsumerGroup)
///     .create_stream_if_not_exists(true)
///     .create_topic_if_not_exists(true)
///     .partitions_count(3)
///     .poll_options(PollOptions {
///         count: 100,
///         commit: CommitPolicy::Auto(AutoCommit::default()),
///         ..PollOptions::wait(IggyDuration::ONE_SECOND)
///     })
///     .polling_strategy(PollingStrategy::next())
///     .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
///     .init_retries(5)
///     .init_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// assert_eq!(config.partitions_count(), 3);
/// ```
///
/// [`AutoCommit::default()`](crate::prelude::AutoCommit) counts a message under
/// [`CommitAfter::Processing`](crate::prelude::CommitAfter::Processing). On the stream that is
/// when the next message is asked for. Under
/// [`IggyConsumerMessageExt::consume_messages`] it is when the handler returned `Ok`.
///
/// [`CommitPolicy`]: crate::prelude::CommitPolicy
/// [`CommitPolicy::Auto`]: crate::prelude::CommitPolicy::Auto
/// [`CommitPolicy::Manual`]: crate::prelude::CommitPolicy::Manual
/// [`CommitPolicy::OnRead`]: crate::prelude::CommitPolicy::OnRead
/// [`ConsumerKind::ConsumerGroup`]: crate::prelude::ConsumerKind::ConsumerGroup
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
/// [`IggyConsumerMessageExt::consume_messages`]: crate::prelude::IggyConsumerMessageExt::consume_messages
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyStreamConsumer`]: crate::prelude::IggyStreamConsumer
/// [`PollOptions::wait`]: crate::prelude::PollOptions::wait
/// [`PollingStrategy::next()`]: crate::prelude::PollingStrategy::next
#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyConsumerConfig {
    /// Name of the stream. Must be unique.
    stream_name: String,
    /// Name of the topic. Must be unique.
    topic_name: String,
    /// Requested encoded bytes for buffered and in-flight batches, raised to at least 32 batches.
    #[builder(default = DEFAULT_PREFETCH_BYTES)]
    prefetch_bytes: u32,
    /// Requested messages for buffered and in-flight batches. Defaults to 16,000, raised to at
    /// least 32 batches.
    prefetch_messages: Option<u32>,
    /// Create the stream if it doesn't exist.
    create_stream_if_not_exists: bool,
    /// Create the topic if it doesn't exist.
    create_topic_if_not_exists: bool,
    /// Members of the same consumer group use the same name.
    consumer_name: String,
    /// The type of consumer: `Consumer` or `ConsumerGroup`, the default. `ExternalGroup` only holds
    /// offsets, so a consumer built with it fails with `FeatureUnavailable`, as a poll with it does.
    consumer_kind: ConsumerKind,
    /// Partition count when creating a topic.
    partitions_count: u32,
    /// Partitions of an ordinary consumer. Defaults to partition 0 and is ignored by consumer
    /// groups.
    #[builder(default = vec![DEFAULT_PARTITION_ID])]
    partitions: Vec<u32>,
    /// The limits of each request and the `CommitPolicy` of the consumer, the defaults of
    /// `IggyConsumerBuilder`. The greater the `count`, the higher the throughput for bulk data.
    /// Note, there is a tradeoff between batch size and latency, so you want to benchmark your
    /// setup.
    #[builder(default = default_poll_options())]
    poll_options: PollOptions,
    /// `PollingStrategy` specifies from where to start polling messages. See `PollingStrategy` for details.
    #[builder(default = PollingStrategy::next())]
    polling_strategy: PollingStrategy,
    /// Sets how long a refused partition waits before the next read.
    polling_retry_interval: NonZeroIggyDuration,
    /// Sets the number of retries when initializing the consumer if the stream or topic is not found.
    init_retries: Option<u32>,
    /// Sets the interval between retries when initializing the consumer if the stream or topic is not found.
    init_interval: NonZeroIggyDuration,
    /// Sets client-side payload and user-header decryption. Currently only Aes256Gcm is supported.
    /// Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
    encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for IggyConsumerConfig {
    fn default() -> Self {
        Self {
            stream_name: "test_stream".to_string(),
            topic_name: "test_topic".to_string(),
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            prefetch_messages: None,
            create_stream_if_not_exists: false,
            create_topic_if_not_exists: false,
            consumer_name: "test_consumer".to_string(),
            consumer_kind: ConsumerKind::ConsumerGroup,
            poll_options: default_poll_options(),
            polling_strategy: PollingStrategy::next(),
            partitions_count: 1,
            partitions: vec![DEFAULT_PARTITION_ID],
            encryptor: None,
            polling_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            init_retries: Some(5),
            init_interval: NonZeroIggyDuration::from_str("3s").unwrap(),
        }
    }
}

impl IggyConsumerConfig {
    /// Sets most fields at once, positionally.
    ///
    /// The prefetch limits keep their defaults, and an ordinary consumer reads partition 0. Use
    /// [`with_partitions()`](Self::with_partitions) to select others. [`builder()`](Self::builder)
    /// sets the same fields by name and is easier to read.
    ///
    /// # Examples
    ///
    /// Describe a group that reads from the stored offset:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// let config = IggyConsumerConfig::new(
    ///     "my-stream".to_string(),
    ///     "my-topic".to_string(),
    ///     true,
    ///     true,
    ///     "order-workers".to_string(),
    ///     ConsumerKind::ConsumerGroup,
    ///     PollOptions::wait(IggyDuration::ONE_SECOND),
    ///     PollingStrategy::next(),
    ///     3,
    ///     None,
    ///     NonZeroIggyDuration::ONE_SECOND,
    ///     Some(5),
    ///     NonZeroIggyDuration::ONE_SECOND,
    /// );
    ///
    /// assert_eq!(config.partitions_count(), 3);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_name: String,
        topic_name: String,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        consumer_name: String,
        consumer_kind: ConsumerKind,
        poll_options: PollOptions,
        polling_strategy: PollingStrategy,
        partitions_count: u32,
        encryptor: Option<Arc<EncryptorKind>>,
        polling_retry_interval: NonZeroIggyDuration,
        init_retries: Option<u32>,
        init_interval: NonZeroIggyDuration,
    ) -> Self {
        Self {
            stream_name,
            topic_name,
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            prefetch_messages: None,
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            consumer_name,
            consumer_kind,
            poll_options,
            polling_strategy,
            partitions_count,
            partitions: vec![DEFAULT_PARTITION_ID],
            encryptor,
            polling_retry_interval,
            init_retries,
            init_interval,
        }
    }

    /// Get a config for a `ConsumerGroup` that names one stream and one topic, and sets the
    /// `poll_options`. The rest take their defaults.
    ///
    /// `poll_options` sets the limits of each request and the commit policy. The consumer is
    /// called `consumer-{stream}-{topic}` and joins a group under that name. It creates neither
    /// the stream nor the topic, and it starts with [`PollingStrategy::next()`].
    ///
    /// # Examples
    ///
    /// Describe one topic with 100 messages per request:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn main() -> Result<(), IggyError> {
    /// let config = IggyConsumerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "my-topic",
    ///     PollOptions {
    ///         count: 100,
    ///         ..PollOptions::wait(IggyDuration::ONE_SECOND)
    ///     },
    /// )?;
    ///
    /// assert_eq!(config.consumer_name(), "consumer-my-stream-my-topic");
    /// assert!(!config.create_topic_if_not_exists());
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`IggyError::InvalidIdentifier`] when `stream` or `topic` is not a valid
    /// identifier.
    ///
    /// [`PollingStrategy::next()`]: crate::prelude::PollingStrategy::next
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        poll_options: PollOptions,
    ) -> Result<Self, IggyError> {
        // Validate stream and topic names. Returns IggyError if names are not allowed.
        Identifier::named(stream)?;
        Identifier::named(topic)?;

        Ok(Self {
            stream_name: stream.to_string(),
            topic_name: topic.to_string(),
            prefetch_bytes: DEFAULT_PREFETCH_BYTES,
            prefetch_messages: None,
            create_stream_if_not_exists: false,
            create_topic_if_not_exists: false,
            consumer_name: format!("consumer-{stream}-{topic}"),
            consumer_kind: ConsumerKind::ConsumerGroup,
            poll_options,
            polling_strategy: PollingStrategy::next(),
            partitions_count: 1,
            partitions: vec![DEFAULT_PARTITION_ID],
            encryptor: None,
            polling_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            init_retries: Some(5),
            init_interval: NonZeroIggyDuration::from_str("3s").unwrap(),
        })
    }
}

impl IggyConsumerConfig {
    /// Selects the partitions of an ordinary consumer. Consumer groups ignore this setting.
    pub fn with_partitions(mut self, partitions: impl IntoIterator<Item = u32>) -> Self {
        self.partitions = partitions.into_iter().collect();
        self
    }

    /// Sets the limits of each request and whether the server stores the offset of a read.
    pub fn with_poll_options(mut self, poll_options: PollOptions) -> Self {
        self.poll_options = poll_options;
        self
    }

    /// Returns the name of the stream that is read.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Returns the name of the topic that is read.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns the requested encoded bytes for buffered and in-flight batches.
    pub fn prefetch_bytes(&self) -> u32 {
        self.prefetch_bytes
    }

    /// Returns the requested messages for buffered and in-flight batches, if one was set.
    pub fn prefetch_messages(&self) -> Option<u32> {
        self.prefetch_messages
    }

    /// Returns whether the build creates the stream when it is missing.
    pub fn create_stream_if_not_exists(&self) -> bool {
        self.create_stream_if_not_exists
    }

    /// Returns whether the build creates the topic when it is missing.
    pub fn create_topic_if_not_exists(&self) -> bool {
        self.create_topic_if_not_exists
    }

    /// Returns the name of the consumer, or of its group.
    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    /// Returns whether the consumer joins a group or reads one partition alone.
    pub fn consumer_kind(&self) -> ConsumerKind {
        self.consumer_kind
    }

    /// Returns the limits of each request and the commit policy of the consumer.
    pub fn poll_options(&self) -> PollOptions {
        self.poll_options
    }

    /// Returns where reading each partition starts.
    pub fn polling_strategy(&self) -> PollingStrategy {
        self.polling_strategy
    }

    /// Returns how many partitions a topic created by the build gets.
    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    /// Returns the partitions that an ordinary consumer reads. A consumer group ignores them.
    pub fn partitions(&self) -> &[u32] {
        &self.partitions
    }

    /// Returns the encryptor for payloads and user headers, if there is one.
    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }

    /// Returns how long a refused partition waits before the next read.
    pub fn polling_retry_interval(&self) -> NonZeroIggyDuration {
        self.polling_retry_interval
    }

    /// Returns how often the build retries a missing stream or topic, if at all.
    pub fn init_retries(&self) -> Option<u32> {
        self.init_retries
    }

    /// Returns the wait between the `init_retries`.
    pub fn init_interval(&self) -> NonZeroIggyDuration {
        self.init_interval
    }
}

impl Validatable<IggyError> for IggyConsumerConfig {
    /// Refuses a configuration that no consumer can poll with, before any call reaches the server.
    fn validate(&self) -> Result<(), IggyError> {
        if self.consumer_kind == ConsumerKind::ExternalGroup {
            error!(
                consumer_kind = %self.consumer_kind,
                "consumer_kind must be consumer or consumer_group: an external group only holds \
                 offsets and cannot poll"
            );
            return Err(IggyError::FeatureUnavailable);
        }
        Ok(())
    }
}

/// The default of `IggyConsumerBuilder`: at most 1000 messages, a wait of up to five seconds, and
/// no commit.
fn default_poll_options() -> PollOptions {
    PollOptions::wait(DEFAULT_MAX_WAIT.into())
}

#[cfg(test)]
impl IggyConsumerConfig {
    /// The default configuration with the one kind that no consumer can poll with.
    pub(crate) fn with_external_group_kind() -> Self {
        Self {
            consumer_kind: ConsumerKind::ExternalGroup,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{CommitPolicy, IggyDuration};
    use std::time::Duration;

    const POLL_COUNT: u32 = 100;
    const PARTITIONS: [u32; 2] = [1, 3];

    fn poll_options() -> PollOptions {
        PollOptions {
            count: POLL_COUNT,
            ..PollOptions::wait(IggyDuration::ONE_SECOND)
        }
    }

    #[test]
    fn given_external_group_kind_when_validated_should_refuse_it() {
        assert!(matches!(
            IggyConsumerConfig::with_external_group_kind().validate(),
            Err(IggyError::FeatureUnavailable)
        ));
        assert!(IggyConsumerConfig::default().validate().is_ok());
    }

    #[test]
    fn should_be_equal() {
        // Builder is generated by the bon macro
        let config = IggyConsumerConfig::builder()
            .stream_name("test_stream".to_string())
            .topic_name("test_topic".to_string())
            .create_stream_if_not_exists(true)
            .create_topic_if_not_exists(true)
            .consumer_name("test_consumer".to_string())
            .consumer_kind(ConsumerKind::ConsumerGroup)
            .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
            .partitions_count(1)
            .init_retries(3)
            .init_interval(NonZeroIggyDuration::from_str("3s").unwrap())
            .build();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert!(config.create_stream_if_not_exists());
        assert!(config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.poll_options(),
            PollOptions::wait(IggyDuration::from(Duration::from_secs(5))),
            "the defaults of IggyConsumerBuilder"
        );
        assert_eq!(config.polling_strategy(), PollingStrategy::next());
        assert_eq!(config.partitions_count(), 1);
        assert_eq!(config.partitions(), [DEFAULT_PARTITION_ID]);

        assert_eq!(
            config.polling_retry_interval(),
            NonZeroIggyDuration::ONE_SECOND
        );
        assert_eq!(config.init_retries(), Some(3));

        assert_eq!(
            config.init_interval(),
            NonZeroIggyDuration::from_str("3s").unwrap()
        );
    }

    #[test]
    fn should_be_default() {
        let config = IggyConsumerConfig::default();
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.poll_options(),
            PollOptions::wait(IggyDuration::from(Duration::from_secs(5))),
            "the defaults of IggyConsumerBuilder"
        );
        assert_eq!(config.polling_strategy(), PollingStrategy::next());
        assert_eq!(config.partitions_count(), 1);
        assert_eq!(config.partitions(), [DEFAULT_PARTITION_ID]);

        assert_eq!(
            config.polling_retry_interval(),
            NonZeroIggyDuration::ONE_SECOND
        );
        assert_eq!(config.init_retries(), Some(5));
        assert_eq!(
            config.init_interval(),
            NonZeroIggyDuration::from_str("3s").unwrap()
        );
    }

    #[test]
    fn should_be_new() {
        let config = IggyConsumerConfig::new(
            "test_stream".to_string(),
            "test_topic".to_string(),
            false,
            false,
            "test_consumer".to_string(),
            ConsumerKind::ConsumerGroup,
            poll_options(),
            PollingStrategy::last(),
            1,
            None,
            NonZeroIggyDuration::ONE_SECOND,
            Some(3),
            NonZeroIggyDuration::from_str("3s").unwrap(),
        );
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(config.poll_options(), poll_options());
        assert_eq!(config.polling_strategy(), PollingStrategy::last());
        assert_eq!(config.partitions_count(), 1);

        assert_eq!(
            config.polling_retry_interval(),
            NonZeroIggyDuration::ONE_SECOND
        );
        assert_eq!(config.init_retries(), Some(3));
        assert_eq!(
            config.init_interval(),
            NonZeroIggyDuration::from_str("3s").unwrap()
        );
    }

    #[test]
    fn should_be_from_stream_topic() {
        let options = PollOptions {
            commit: CommitPolicy::OnRead,
            ..poll_options()
        };
        let res = IggyConsumerConfig::from_stream_topic("test_stream", "test_topic", options);
        assert!(res.is_ok());
        let config = res.unwrap();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "consumer-test_stream-test_topic");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(config.poll_options(), options);
        assert_eq!(config.polling_strategy(), PollingStrategy::next());
        assert_eq!(config.partitions_count(), 1);
    }

    #[test]
    fn given_partitions_when_set_should_replace_the_default() {
        let config = IggyConsumerConfig::default().with_partitions(PARTITIONS);

        assert_eq!(config.partitions(), PARTITIONS);
    }

    #[test]
    fn given_poll_options_when_set_should_replace_the_defaults() {
        let options = PollOptions {
            commit: CommitPolicy::OnRead,
            ..poll_options()
        };
        let config = IggyConsumerConfig::default().with_poll_options(options);

        assert_eq!(config.poll_options(), options);
    }
}
