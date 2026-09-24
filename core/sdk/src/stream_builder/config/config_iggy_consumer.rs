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

use crate::clients::consumer::{AutoCommit, AutoCommitWhen};
use crate::prelude::{
    ConsumerKind, EncryptorKind, Identifier, IggyDuration, IggyError, NonZeroIggyDuration,
    PollingStrategy,
};
use bon::Builder;
use std::str::FromStr;
use std::sync::Arc;

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
/// | [`from_stream_topic()`](Self::from_stream_topic) | `ConsumerGroup` for one stream and one topic, with `batch_length` and `polling_interval`, defaults elsewhere |
/// | [`new()`](Self::new) | every field, positional |
/// | [`default()`](Self::default) | the stream `test_stream` and the topic `test_topic`, for examples and tests |
///
/// The builder requires every field that is not an [`Option`], so
/// [`init_retries()`](Self::init_retries) and [`encryptor()`](Self::encryptor) are the only two it
/// lets you leave out.
///
/// # Fields and defaults
///
/// The defaults below are what [`default()`](Self::default) and
/// [`from_stream_topic()`](Self::from_stream_topic) set. The builder has no defaults.
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`stream_name()`](Self::stream_name), [`topic_name()`](Self::topic_name) | `test_stream`, `test_topic` | what is read |
/// | [`stream_id()`](Self::stream_id), [`topic_id()`](Self::topic_id) | the same two names | the lookup that [`IggyStreamConsumer`] runs to decide whether the stream and the topic exist, and the name it creates them under |
/// | [`consumer_name()`](Self::consumer_name) | `test_consumer`, or `consumer-{stream}-{topic}` | the name of the consumer, or of its group |
/// | [`consumer_kind()`](Self::consumer_kind) | [`ConsumerKind::ConsumerGroup`] | whether the consumer joins a group or reads one partition alone |
/// | [`partitions_count()`](Self::partitions_count) | 1 | two things, see below |
/// | [`create_stream_if_not_exists()`](Self::create_stream_if_not_exists) | off | creating a missing stream |
/// | [`create_topic_if_not_exists()`](Self::create_topic_if_not_exists) | off | creating a missing topic |
/// | [`batch_length()`](Self::batch_length) | 100 | messages fetched per request |
/// | [`polling_interval()`](Self::polling_interval) | 5 milliseconds | smallest gap between two requests |
/// | [`polling_strategy()`](Self::polling_strategy) | [`PollingStrategy::last()`] | where reading each partition starts |
/// | [`auto_commit()`](Self::auto_commit) | [`AutoCommitWhen::PollingMessages`] | when offsets are committed |
/// | [`polling_retry_interval()`](Self::polling_retry_interval) | one second | the wait while polling is blocked |
/// | [`init_retries()`](Self::init_retries), [`init_interval()`](Self::init_interval) | five retries, three seconds apart | retries for a missing stream or topic |
/// | [`encryptor()`](Self::encryptor) | none | decrypting payloads and user headers |
///
/// ## Some callouts on defaults
///
/// - [`partitions_count()`](Self::partitions_count) carries two meanings. It is the number of partitions
///   that the build creates per topic. For [`ConsumerKind::Consumer`] it is also the ID of the
///   single partition the consumer reads. A [`ConsumerKind::ConsumerGroup`] ignores that second
///   meaning, because the server assigns its partitions.
/// - [`encryptor()`](Self::encryptor) if you build from an [`IggyClient`] that already has an `encryptor` the one you define here
///   replaces the encryptor of the [`IggyClient`] for consumer.
///   An encryptor also rules out the default [`auto_commit()`](Self::auto_commit), because that setting commits a batch before it is
///   decrypted. If decryption fails the offset would commit even though the message could not be consumed.
///   Hence, the pair with [`IggyError::InvalidConfiguration`]. Pick any other [`auto_commit()`](Self::auto_commit) with an `encryptor`.
/// - With [`PollingStrategy::last()`] the first and the first poll after a crash poll the latest `batch_length` messages.
///   After that, only the newest messages are polled. This can be fewer than `batch_length`, but never more.
/// - [`create_stream_if_not_exists()`](Self::create_stream_if_not_exists) and
///   [`create_topic_if_not_exists()`](Self::create_topic_if_not_exists) are off, so a consumer alone
///   creates nothing. It waits out [`init_retries()`](Self::init_retries) instead, which covers a
///   producer that creates the topic at the same time.
///
/// Keep [`stream_id()`](Self::stream_id) and [`stream_name()`](Self::stream_name) in agreement, and
/// keep [`topic_id()`](Self::topic_id) and [`topic_name()`](Self::topic_name) in agreement.
/// [`IggyStreamConsumer`] creates the stream and the topic under the name it reads from the two
/// identifiers, and [`IggyConsumer::init()`] then looks the two names up. If a pair disagrees, the
/// build creates one topic and reads another, and [`IggyConsumer::init()`] fails with
/// [`IggyError::StreamNameNotFound`] or [`IggyError::TopicNameNotFound`].
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
///     100,
///     IggyDuration::new_from_secs(1),
/// )?;
///
/// assert_eq!(config.consumer_name(), "consumer-my-stream-my-topic");
/// assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
/// assert_eq!(config.polling_strategy(), PollingStrategy::last());
/// # Ok(())
/// # }
/// ```
///
/// A group that creates the topic it reads, resumes from the stored offset, and commits once a
/// message has been handled:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn main() -> Result<(), IggyError> {
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
///     .init_retries(5)
///     .init_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// assert_eq!(config.partitions_count(), 3);
/// # Ok(())
/// # }
/// ```
///
/// The [`AutoCommitAfter`] variants above only commit under
/// [`IggyConsumerMessageExt::consume_messages`].
///
/// [`AutoCommitAfter`]: crate::prelude::AutoCommitAfter
/// [`AutoCommitWhen::PollingMessages`]: crate::prelude::AutoCommitWhen::PollingMessages
/// [`ConsumerKind::Consumer`]: crate::prelude::ConsumerKind::Consumer
/// [`ConsumerKind::ConsumerGroup`]: crate::prelude::ConsumerKind::ConsumerGroup
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyConsumer::init()`]: crate::prelude::IggyConsumer::init
/// [`IggyConsumerMessageExt::consume_messages`]: crate::prelude::IggyConsumerMessageExt::consume_messages
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyStreamConsumer`]: crate::prelude::IggyStreamConsumer
/// [`PollingStrategy::last()`]: crate::prelude::PollingStrategy::last
/// [`PollingStrategy::next()`]: crate::prelude::PollingStrategy::next
#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyConsumerConfig {
    /// Identifier of the stream. Must be unique.
    stream_id: Identifier,
    /// Name of the stream. Must be unique.
    stream_name: String,
    /// Identifier of the topic. Must be unique.
    topic_id: Identifier,
    /// Name of the topic. Must be unique.
    topic_name: String,
    /// The auto-commit configuration for storing the message offset on the server. See  `AutoCommit` for details.
    auto_commit: AutoCommit,
    /// The max number of messages to send in a batch. The greater the batch length, the higher the throughput for bulk data.
    /// Note, there is a tradeoff between batch size and latency.
    batch_length: u32,
    /// Create the stream if it doesn't exist.
    create_stream_if_not_exists: bool,
    /// Create the topic if it doesn't exist.
    create_topic_if_not_exists: bool,
    /// Members of the same consumer group use the same name.
    consumer_name: String,
    /// The type of consumer. It can be either `Consumer` or `ConsumerGroup`. ConsumerGroup is default.
    consumer_kind: ConsumerKind,
    /// Partition count when creating a topic.
    partitions_count: u32,
    /// Partition ID for an ordinary consumer. Defaults to 0 and is ignored by consumer groups.
    #[builder(default = DEFAULT_PARTITION_ID)]
    partition_id: u32,
    /// The polling interval for messages.
    polling_interval: IggyDuration,
    /// `PollingStrategy` specifies from where to start polling messages. See `PollingStrategy` for details.
    polling_strategy: PollingStrategy,
    /// Sets the polling retry interval in case of server disconnection.
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
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        Self {
            stream_id,
            stream_name: "test_stream".to_string(),
            topic_id,
            topic_name: "test_topic".to_string(),
            auto_commit: AutoCommit::When(AutoCommitWhen::PollingMessages),
            batch_length: 100,
            create_stream_if_not_exists: false,
            create_topic_if_not_exists: false,
            consumer_name: "test_consumer".to_string(),
            consumer_kind: ConsumerKind::ConsumerGroup,
            polling_interval: IggyDuration::from_str("5ms").unwrap(),
            polling_strategy: PollingStrategy::last(),
            partitions_count: 1,
            partition_id: DEFAULT_PARTITION_ID,
            encryptor: None,
            polling_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            init_retries: Some(5),
            init_interval: NonZeroIggyDuration::from_str("3s").unwrap(),
        }
    }
}

impl IggyConsumerConfig {
    /// Sets every field at once, positionally.
    ///
    /// This applies no defaults. [`builder()`](Self::builder) sets the same fields by name and is
    /// easier to read. Ordinary consumers use partition 0. Use [`Self::with_partition_id`] to select another partition.
    ///
    /// Return a new instance of a `IggyConsumerConfig`.
    ///
    /// # Examples
    ///
    /// Describe a group that reads from the stored offset:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn example() -> Result<(), IggyError> {
    /// # fn main() -> Result<(), IggyError> {
    /// let config = IggyConsumerConfig::new(
    ///     Identifier::from_str_value("my-stream")?,
    ///     "my-stream".to_string(),
    ///     Identifier::from_str_value("my-topic")?,
    ///     "my-topic".to_string(),
    ///     AutoCommit::When(AutoCommitWhen::ConsumingEachMessage),
    ///     100,
    ///     true,
    ///     true,
    ///     "order-workers".to_string(),
    ///     ConsumerKind::ConsumerGroup,
    ///     IggyDuration::new_from_secs(1),
    ///     PollingStrategy::next(),
    ///     3,
    ///     None,
    ///     NonZeroIggyDuration::ONE_SECOND,
    ///     Some(5),
    ///     NonZeroIggyDuration::ONE_SECOND,
    /// );
    ///
    /// assert_eq!(config.partitions_count(), 3);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        stream_name: String,
        topic_id: Identifier,
        topic_name: String,
        auto_commit: AutoCommit,
        batch_length: u32,
        create_stream_if_not_exists: bool,
        create_topic_if_not_exists: bool,
        consumer_name: String,
        consumer_kind: ConsumerKind,
        polling_interval: IggyDuration,
        polling_strategy: PollingStrategy,
        partitions_count: u32,
        encryptor: Option<Arc<EncryptorKind>>,
        polling_retry_interval: NonZeroIggyDuration,
        init_retries: Option<u32>,
        init_interval: NonZeroIggyDuration,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            auto_commit,
            batch_length,
            create_stream_if_not_exists,
            create_topic_if_not_exists,
            consumer_name,
            consumer_kind,
            polling_interval,
            polling_strategy,
            partitions_count,
            partition_id: DEFAULT_PARTITION_ID,
            encryptor,
            polling_retry_interval,
            init_retries,
            init_interval,
        }
    }

    /// Get a config for a `ConsumerGroup` that names one stream, one topic, sets a `batch_length` and
    /// `polling_interval`. The rest take their defaults.
    ///
    /// The consumer is called `consumer-{stream}-{topic}` and joins a group under that name.
    /// It creates neither the stream nor the topic. It reads with [`PollingStrategy::last()`],
    /// so every poll starts `batch_length` messages back from the end of the partition.
    ///
    /// [`PollingStrategy::last()`]: crate::prelude::PollingStrategy::last
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
    ///     100,
    ///     IggyDuration::new_from_secs(1),
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
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        batch_length: u32,
        polling_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        let stream_id = Identifier::from_str_value(stream)?;
        let topic_id = Identifier::from_str_value(topic)?;

        Ok(Self {
            stream_id,
            stream_name: stream.to_string(),
            topic_id,
            topic_name: topic.to_string(),
            auto_commit: AutoCommit::When(AutoCommitWhen::PollingMessages),
            batch_length,
            create_stream_if_not_exists: false,
            create_topic_if_not_exists: false,
            consumer_name: format!("consumer-{stream}-{topic}"),
            consumer_kind: ConsumerKind::ConsumerGroup,
            polling_interval,
            polling_strategy: PollingStrategy::last(),
            partitions_count: 1,
            partition_id: DEFAULT_PARTITION_ID,
            encryptor: None,
            polling_retry_interval: NonZeroIggyDuration::ONE_SECOND,
            init_retries: Some(5),
            init_interval: NonZeroIggyDuration::from_str("3s").unwrap(),
        })
    }
}

impl IggyConsumerConfig {
    /// Selects the partition for an ordinary consumer. Consumer groups ignore this setting.
    pub fn with_partition_id(mut self, partition_id: u32) -> Self {
        self.partition_id = partition_id;
        self
    }

    /// Returns the stream identifier that the build looks the stream up by.
    pub fn stream_id(&self) -> &Identifier {
        &self.stream_id
    }

    /// Returns the name of the stream that is read.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Returns the topic identifier that the build looks the topic up by.
    pub fn topic_id(&self) -> &Identifier {
        &self.topic_id
    }

    /// Returns the name of the topic that is read.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns when the consumer commits an offset by itself.
    pub fn auto_commit(&self) -> AutoCommit {
        self.auto_commit
    }

    /// Returns how many messages one request fetches at most.
    pub fn batch_length(&self) -> u32 {
        self.batch_length
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

    /// Returns the smallest gap the consumer keeps between two requests.
    pub fn polling_interval(&self) -> IggyDuration {
        self.polling_interval
    }

    /// Returns where reading each partition starts.
    pub fn polling_strategy(&self) -> PollingStrategy {
        self.polling_strategy
    }

    /// Returns how many partitions a topic created by the build gets.
    ///
    /// For [`ConsumerKind::Consumer`] this is also the ID of the single partition that the
    /// consumer reads.
    ///
    /// [`ConsumerKind::Consumer`]: crate::prelude::ConsumerKind::Consumer
    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    /// Returns the partition id the consumer binds to.
    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    /// Returns the encryptor for payloads and user headers, if there is one.
    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }

    /// Returns the wait between attempts while polling is blocked.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_equal() {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        // Builder is generated by the bon macro
        let config = IggyConsumerConfig::builder()
            .stream_id(stream_id)
            .stream_name("test_stream".to_string())
            .topic_id(topic_id)
            .topic_name("test_topic".to_string())
            .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
            .batch_length(100)
            .create_stream_if_not_exists(true)
            .create_topic_if_not_exists(true)
            .consumer_name("test_consumer".to_string())
            .consumer_kind(ConsumerKind::ConsumerGroup)
            .polling_interval(IggyDuration::from_str("5ms").unwrap())
            .polling_strategy(PollingStrategy::last())
            .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
            .partitions_count(1)
            .init_retries(3)
            .init_interval(NonZeroIggyDuration::from_str("3s").unwrap())
            .build();

        assert_eq!(
            config.stream_id(),
            &Identifier::from_str_value("test_stream").unwrap()
        );
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(
            config.topic_id(),
            &Identifier::from_str_value("test_topic").unwrap()
        );
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(
            config.auto_commit(),
            AutoCommit::When(AutoCommitWhen::PollingMessages)
        );
        assert_eq!(config.batch_length(), 100);
        assert!(config.create_stream_if_not_exists());
        assert!(config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
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
    fn should_be_default() {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        let config = IggyConsumerConfig::default();
        assert_eq!(config.stream_id(), &stream_id);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_id(), &topic_id);
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(
            config.auto_commit(),
            AutoCommit::When(AutoCommitWhen::PollingMessages)
        );
        assert_eq!(config.batch_length(), 100);
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(config.polling_strategy(), PollingStrategy::last());
        assert_eq!(config.partitions_count(), 1);

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
            Identifier::from_str_value("test_stream").unwrap(),
            "test_stream".to_string(),
            Identifier::from_str_value("test_topic").unwrap(),
            "test_topic".to_string(),
            AutoCommit::When(AutoCommitWhen::PollingMessages),
            100,
            false,
            false,
            "test_consumer".to_string(),
            ConsumerKind::ConsumerGroup,
            IggyDuration::from_str("5ms").unwrap(),
            PollingStrategy::last(),
            1,
            None,
            NonZeroIggyDuration::ONE_SECOND,
            Some(3),
            NonZeroIggyDuration::from_str("3s").unwrap(),
        );
        assert_eq!(
            config.stream_id(),
            &Identifier::from_str_value("test_stream").unwrap(),
        );
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(
            config.topic_id(),
            &Identifier::from_str_value("test_topic").unwrap()
        );
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(
            config.auto_commit(),
            AutoCommit::When(AutoCommitWhen::PollingMessages)
        );
        assert_eq!(config.batch_length(), 100);
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "test_consumer");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
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
        let res = IggyConsumerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            IggyDuration::from_str("5ms").unwrap(),
        );
        assert!(res.is_ok());
        let config = res.unwrap();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert!(!config.create_stream_if_not_exists());
        assert!(!config.create_topic_if_not_exists());
        assert_eq!(config.consumer_name(), "consumer-test_stream-test_topic");
        assert_eq!(config.consumer_kind(), ConsumerKind::ConsumerGroup);
        assert_eq!(
            config.polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(config.polling_strategy(), PollingStrategy::last());
        assert_eq!(config.partitions_count(), 1);
    }
}
