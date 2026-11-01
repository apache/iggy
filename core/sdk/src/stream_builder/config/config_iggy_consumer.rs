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

#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyConsumerConfig {
    /// Name of the stream. Must be unique.
    stream_name: String,
    /// Name of the topic. Must be unique.
    topic_name: String,
    /// Maximum encoded bytes reserved for buffered and in-flight batches.
    #[builder(default = DEFAULT_PREFETCH_BYTES)]
    prefetch_bytes: u32,
    /// Maximum messages reserved for buffered and in-flight batches. Defaults to 16,000, or
    /// the `count` of the poll options when that is larger.
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
    /// Sets the number of retries and the interval when initializing the consumer if the stream or topic is not found.
    /// Might be useful when the stream or topic is created dynamically by the producer.
    init_retries: Option<u32>,
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
    /// Creates a new `IggyConsumerConfig` from the given arguments.
    ///
    /// # Args
    ///
    /// * `stream_name` - The stream name.
    /// * `topic_name` - The topic name.
    /// * `create_stream_if_not_exists` - Whether to create the stream if it does not exists.
    /// * `create_topic_if_not_exists` - Whether to create the topic if it does not exists.
    /// * `consumer_name` - The consumer name.
    /// * `consumer_kind` - The consumer kind.
    /// * `poll_options` - The limits of each request and the commit policy.
    /// * `polling_strategy` - The polling strategy.
    /// * `partitions_count` - Topic creation count.
    /// * `encryptor` - The encryptor.
    /// * `polling_retry_interval` - The polling retry interval.
    /// * `init_retries` - The number of init retries.
    /// * `init_interval` - The init interval.
    ///
    ///
    /// Returns:
    /// A new `IggyConsumerConfig`.
    ///
    /// Ordinary consumers read partition 0. Use [`Self::with_partitions`] to select others.
    ///
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

    /// Creates a new `IggyConsumerConfig` from the given arguments.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `poll_options` - The limits of each request and the commit policy.
    ///
    /// Returns:
    /// A new `IggyConsumerConfig`.
    ///
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

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub fn prefetch_bytes(&self) -> u32 {
        self.prefetch_bytes
    }

    pub fn prefetch_messages(&self) -> Option<u32> {
        self.prefetch_messages
    }

    pub fn create_stream_if_not_exists(&self) -> bool {
        self.create_stream_if_not_exists
    }

    pub fn create_topic_if_not_exists(&self) -> bool {
        self.create_topic_if_not_exists
    }

    pub fn consumer_name(&self) -> &str {
        &self.consumer_name
    }

    pub fn consumer_kind(&self) -> ConsumerKind {
        self.consumer_kind
    }

    pub fn poll_options(&self) -> PollOptions {
        self.poll_options
    }

    pub fn polling_strategy(&self) -> PollingStrategy {
        self.polling_strategy
    }

    pub fn partitions_count(&self) -> u32 {
        self.partitions_count
    }

    pub fn partitions(&self) -> &[u32] {
        &self.partitions
    }

    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }

    pub fn polling_retry_interval(&self) -> NonZeroIggyDuration {
        self.polling_retry_interval
    }

    pub fn init_retries(&self) -> Option<u32> {
        self.init_retries
    }

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
