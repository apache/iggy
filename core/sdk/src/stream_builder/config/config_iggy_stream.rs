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

use crate::prelude::{Identifier, IggyDuration, IggyError};
use crate::stream_builder::{IggyConsumerConfig, IggyProducerConfig};
use bon::Builder;

/// Describes the producer and the consumer that [`IggyStream`] builds.
///
/// The value is a pair: one [`IggyProducerConfig`] and one [`IggyConsumerConfig`]. It is a plain
/// description that never talks to the server, and changing it later has no effect on a producer
/// or a consumer that is built already.
///
/// # Creating a configuration
///
/// | Constructor | Use it for |
/// | --- | --- |
/// | [`from_stream_topic()`](Self::from_stream_topic) | one stream and one topic, defaults elsewhere |
/// | [`new()`](Self::new) | two halves you built yourself |
/// | [`builder()`](Self::builder) | the same two halves, named |
/// | [`default()`](Self::default) | the stream `test_stream` and the topic `test_topic`, for examples and tests |
///
/// The two halves carry their own stream and topic. [`from_stream_topic()`](Self::from_stream_topic)
/// gives both the same pair. [`new()`](Self::new) and [`builder()`](Self::builder) do not, so they
/// can point the producer and the consumer at different topics. [`stream_id()`](Self::stream_id),
/// [`stream_name()`](Self::stream_name), [`topic_id()`](Self::topic_id) and
/// [`topic_name()`](Self::topic_name) report what the producer half says. Read
/// [`consumer_config()`](Self::consumer_config) for the other half.
///
/// # Examples
///
/// One topic, a batch of 100 messages, and one second between requests in both directions:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn example() -> Result<(), IggyError> {
/// let config = IggyStreamConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
///     IggyDuration::new_from_secs(1),
/// )?;
///
/// assert_eq!(config.stream_name(), "my-stream");
/// assert_eq!(config.producer_config().batch_length(), 100);
/// assert_eq!(config.consumer_config().consumer_name(), "consumer-my-stream-my-topic");
/// # Ok(())
/// # }
/// ```
///
/// A producer that writes to ten partitions, and a group that reads them:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn example() -> Result<(), IggyError> {
/// let producer_config = IggyProducerConfig::builder()
///     .stream_id(Identifier::from_str_value("my-stream")?)
///     .stream_name("my-stream")
///     .topic_id(Identifier::from_str_value("my-topic")?)
///     .topic_name("my-topic")
///     .topic_partitions_count(10)
///     .batch_length(100)
///     .linger_time(IggyDuration::new_from_secs(1))
///     .partitioning(Partitioning::balanced())
///     .build();
///
/// let consumer_config = IggyConsumerConfig::builder()
///     .stream_id(Identifier::from_str_value("my-stream")?)
///     .stream_name("my-stream")
///     .topic_id(Identifier::from_str_value("my-topic")?)
///     .topic_name("my-topic")
///     .consumer_name("order-workers")
///     .consumer_kind(ConsumerKind::ConsumerGroup)
///     .create_stream_if_not_exists(false)
///     .create_topic_if_not_exists(false)
///     .partitions_count(10)
///     .batch_length(100)
///     .polling_interval(IggyDuration::new_from_secs(1))
///     .polling_strategy(PollingStrategy::next())
///     .polling_retry_interval(NonZeroIggyDuration::ONE_SECOND)
///     .auto_commit(AutoCommit::When(AutoCommitWhen::ConsumingEachMessage))
///     .init_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// let config = IggyStreamConfig::new(consumer_config, producer_config);
/// # let _ = config;
/// # Ok(())
/// # }
/// ```
///
/// [`IggyStream`]: crate::prelude::IggyStream
#[derive(Builder, Default, Debug, Clone)]
pub struct IggyStreamConfig {
    consumer_config: IggyConsumerConfig,
    producer_config: IggyProducerConfig,
}

impl IggyStreamConfig {
    /// Pairs a consumer configuration with a producer configuration.
    ///
    /// The two halves are kept as given, so they can name different streams and topics.
    ///
    /// # Examples
    ///
    /// Pair the two defaults:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// let config = IggyStreamConfig::new(
    ///     IggyConsumerConfig::default(),
    ///     IggyProducerConfig::default(),
    /// );
    /// assert_eq!(config.stream_name(), "test_stream");
    /// ```
    pub fn new(consumer_config: IggyConsumerConfig, producer_config: IggyProducerConfig) -> Self {
        Self {
            consumer_config,
            producer_config,
        }
    }

    /// Points both halves at one stream and one topic, and takes the defaults for the rest.
    ///
    /// `batch_length` becomes the batch length of both halves. `linger_time` paces the producer
    /// and `polling_interval` paces the consumer. The consumer is named
    /// `consumer-{stream}-{topic}` and joins a group under that name.
    ///
    /// # Examples
    ///
    /// Describe one topic for both directions:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn example() -> Result<(), IggyError> {
    /// let config = IggyStreamConfig::from_stream_topic(
    ///     "my-stream",
    ///     "my-topic",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    /// assert_eq!(config.topic_name(), "my-topic");
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
        linger_time: IggyDuration,
        polling_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        let consumer_config =
            IggyConsumerConfig::from_stream_topic(stream, topic, batch_length, polling_interval)?;

        let producer_config =
            IggyProducerConfig::from_stream_topic(stream, topic, batch_length, linger_time)?;

        Ok(Self {
            consumer_config,
            producer_config,
        })
    }
}

impl IggyStreamConfig {
    /// Returns the half that describes the consumer.
    pub fn consumer_config(&self) -> &IggyConsumerConfig {
        &self.consumer_config
    }

    /// Returns the half that describes the producer.
    pub fn producer_config(&self) -> &IggyProducerConfig {
        &self.producer_config
    }

    /// Returns the stream identifier of the producer half.
    ///
    /// The consumer half carries its own, which can differ. Read
    /// [`consumer_config()`](Self::consumer_config) for it.
    pub fn stream_id(&self) -> &Identifier {
        self.producer_config.stream_id()
    }

    /// Returns the stream name of the producer half.
    ///
    /// The consumer half carries its own, which can differ. Read
    /// [`consumer_config()`](Self::consumer_config) for it.
    pub fn stream_name(&self) -> &str {
        self.producer_config.stream_name()
    }

    /// Returns the topic identifier of the producer half.
    ///
    /// The consumer half carries its own, which can differ. Read
    /// [`consumer_config()`](Self::consumer_config) for it.
    pub fn topic_id(&self) -> &Identifier {
        self.producer_config.topic_id()
    }

    /// Returns the topic name of the producer half.
    ///
    /// The consumer half carries its own, which can differ. Read
    /// [`consumer_config()`](Self::consumer_config) for it.
    pub fn topic_name(&self) -> &str {
        self.producer_config.topic_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn should_be_equal() {
        let consumer_config = IggyConsumerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            IggyDuration::from_str("5ms").unwrap(),
        )
        .unwrap();

        let producer_config = IggyProducerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            IggyDuration::from_str("5ms").unwrap(),
        )
        .unwrap();
        let config = IggyStreamConfig::new(consumer_config, producer_config);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            IggyDuration::from_str("5ms").unwrap()
        );
    }

    #[test]
    fn should_be_default() {
        let config = IggyStreamConfig::default();
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            IggyDuration::from_str("5ms").unwrap()
        );
    }

    #[test]
    fn should_be_from_stream_topic() {
        let res = IggyStreamConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            IggyDuration::from_str("5ms").unwrap(),
            IggyDuration::from_str("5ms").unwrap(),
        );

        assert!(res.is_ok());
        let config = res.unwrap();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_length(), 100);
        assert_eq!(config.producer_config().batch_length(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().linger_time(),
            IggyDuration::from_str("5ms").unwrap()
        );
    }
}
