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

use crate::prelude::{
    Durability, EncryptorKind, Identifier, IggyDuration, IggyError, Partitioning,
};
use bon::Builder;
use std::str::FromStr;
use std::sync::Arc;

/// Describes the producer that [`IggyStreamProducer`] and [`IggyStream`] build.
///
/// The value is a plain description. It never talks to the server, and changing it later has no
/// effect on a producer that is built already.
///
/// # Creating a configuration
///
/// | Constructor | Use it for |
/// | --- | --- |
/// | [`builder()`](Self::builder) | naming every field you set |
/// | [`from_stream_topic()`](Self::from_stream_topic) | one stream and one topic, defaults elsewhere |
/// | [`new()`](Self::new) | every field, positional |
/// | [`default()`](Self::default) | the stream `test_stream` and the topic `test_topic`, for examples and tests |
///
/// The builder requires every field that is not an [`Option`] and carries no
/// `#[builder(default)]`, so it requires [`stream_name()`](Self::stream_name),
/// [`topic_name()`](Self::topic_name), [`topic_partitions_count()`](Self::topic_partitions_count),
/// [`batch_length()`](Self::batch_length), [`linger_time()`](Self::linger_time) and
/// [`partitioning()`](Self::partitioning). [`topic_durability()`](Self::topic_durability) is the
/// one field the builder itself defaults.
///
/// # Fields and defaults
///
/// The defaults below are what [`default()`](Self::default) and
/// [`from_stream_topic()`](Self::from_stream_topic) set. The builder leaves every field but
/// [`topic_durability()`](Self::topic_durability) unset, so it takes the same default there too.
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`stream_name()`](Self::stream_name), [`topic_name()`](Self::topic_name) | `test_stream`, `test_topic` | where messages are appended |
/// | [`topic_partitions_count()`](Self::topic_partitions_count) | 1 | partitions of a topic the build creates |
/// | [`topic_durability()`](Self::topic_durability) | [`Durability::Replicated`] | durability of a topic the build creates |
/// | [`batch_length()`](Self::batch_length) | 100 | messages per request |
/// | [`linger_time()`](Self::linger_time) | 5 milliseconds | smallest gap between two send calls |
/// | [`partitioning()`](Self::partitioning) | [`Partitioning::balanced()`] | which partition a batch lands in |
/// | [`encryptor()`](Self::encryptor) | none | encrypting payloads and user headers |
///
/// [`topic_partitions_count()`](Self::topic_partitions_count) applies to a topic that the build
/// creates. An existing topic keeps the partitions it has.
///
/// [`encryptor()`](Self::encryptor) replaces the encryptor of the [`IggyClient`] for this
/// producer. A consumer needs the same key to read those messages back.
///
/// # Examples
///
/// One topic, with the defaults for everything else:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn main() -> Result<(), IggyError> {
/// let config = IggyProducerConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
/// )?;
///
/// assert_eq!(config.topic_partitions_count(), 1);
/// # Ok(())
/// # }
/// ```
///
/// Ten partitions, every message keyed to one of them, and five retries:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn main() -> Result<(), IggyError> {
/// let config = IggyProducerConfig::builder()
///     .stream_name("my-stream")
///     .topic_name("my-topic")
///     .topic_partitions_count(10)
///     .batch_length(100)
///     .linger_time(IggyDuration::new_from_secs(1))
///     .partitioning(Partitioning::messages_key_str("my-key")?)
///     .build();
///
/// assert_eq!(config.topic_partitions_count(), 10);
/// # Ok(())
/// # }
/// ```
///
/// [`Durability::Replicated`]: crate::prelude::Durability::Replicated
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyStreamProducer`]: crate::prelude::IggyStreamProducer
/// [`Partitioning::balanced()`]: crate::prelude::Partitioning::balanced
#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyProducerConfig {
    /// Name of the stream. Must be unique.
    stream_name: String,
    /// Name of the topic. Must be unique.
    topic_name: String,
    /// Sets the number of partitions to create for the topic
    topic_partitions_count: u32,
    /// Message durability for topic creation. Persisted provides crash-safe retry receipts;
    /// Replicated accepts sends without that crash-recovery guarantee.
    #[builder(default)]
    topic_durability: Durability,
    /// Maximum messages per direct-send request. Zero uses the SDK's maximum batch length.
    batch_length: u32,
    /// Minimum gap between sequential direct sends, measured from the previous successful send.
    linger_time: IggyDuration,
    /// Strategy that defines to which partition the messages should be sent.
    partitioning: Partitioning,
    /// Sets a optional client side encryptor for encrypting the messages' payloads. Currently only Aes256Gcm is supported.
    /// Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
    encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for IggyProducerConfig {
    fn default() -> Self {
        Self {
            stream_name: "test_stream".to_string(),
            topic_name: "test_topic".to_string(),
            batch_length: 100,
            linger_time: IggyDuration::from_str("5ms").unwrap(),
            partitioning: Partitioning::balanced(),
            topic_partitions_count: 1,
            topic_durability: Durability::default(),
            encryptor: None,
        }
    }
}

impl IggyProducerConfig {
    /// Return a new instance of a `IggyProducerConfig`.
    ///
    /// # Examples
    ///
    /// Describe a topic with three partitions and no retries:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn main() -> Result<(), IggyError> {
    /// let config = IggyProducerConfig::new(
    ///     "my-stream".to_string(),
    ///     "my-topic".to_string(),
    ///     3,
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    ///     Partitioning::balanced(),
    ///     None,
    /// );
    ///
    /// assert_eq!(config.batch_length(), 100);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_name: String,
        topic_name: String,
        topic_partitions_count: u32,
        batch_length: u32,
        linger_time: IggyDuration,
        partitioning: Partitioning,
        encryptor: Option<Arc<EncryptorKind>>,
    ) -> Self {
        Self {
            stream_name,
            topic_name,
            topic_partitions_count,
            topic_durability: Durability::default(),
            batch_length,
            linger_time,
            partitioning,
            encryptor,
        }
    }

    /// Names one stream, one topic, `batch_length` and `linger_time`. The rest defaults.
    ///
    /// The topic gets one partition, balanced partitioning, three send retries one second apart, and no encryptor.
    ///
    /// # Examples
    ///
    /// Describe one topic with 100 messages per request:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn main() -> Result<(), IggyError> {
    /// let config = IggyProducerConfig::from_stream_topic(
    ///     "my-stream",
    ///     "my-topic",
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    /// )?;
    ///
    /// assert_eq!(config.stream_name(), "my-stream");
    /// assert_eq!(config.partitioning(), &Partitioning::balanced());
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
    ) -> Result<Self, IggyError> {
        // Validate stream and topic names. Returns IggyError if names are not allowed.
        Identifier::named(stream)?;
        Identifier::named(topic)?;

        Ok(Self {
            stream_name: stream.to_string(),
            topic_name: topic.to_string(),
            batch_length,
            linger_time,
            partitioning: Partitioning::balanced(),
            topic_partitions_count: 1,
            topic_durability: Durability::default(),
            encryptor: None,
        })
    }
}

impl IggyProducerConfig {
    /// Returns the name of the stream that messages are appended to.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Returns the name of the topic that messages are appended to.
    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    /// Returns how many messages one request carries at most.
    pub fn batch_length(&self) -> u32 {
        self.batch_length
    }

    /// Returns the smallest gap the producer keeps between two send calls.
    pub fn linger_time(&self) -> IggyDuration {
        self.linger_time
    }

    /// Returns the strategy that decides which partition a batch of messages lands in.
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    /// Sets message durability when this configuration creates a topic.
    pub fn with_topic_durability(mut self, durability: Durability) -> Self {
        self.topic_durability = durability;
        self
    }

    pub fn topic_durability(&self) -> Durability {
        self.topic_durability
    }

    /// Returns how many partitions a topic created by the build gets.
    pub fn topic_partitions_count(&self) -> u32 {
        self.topic_partitions_count
    }

    /// Returns the encryptor for payloads and user headers, if there is one.
    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_equal() {
        let stream = "test_stream";
        let topic = "test_topic";

        // Builder is generated by the bon macro
        let config = IggyProducerConfig::builder()
            .stream_name(stream)
            .topic_name(topic)
            .topic_partitions_count(3)
            .batch_length(100)
            .linger_time(IggyDuration::from_str("5ms").unwrap())
            .partitioning(Partitioning::balanced())
            .build();

        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 3);
    }

    #[test]
    fn should_be_default() {
        let config = IggyProducerConfig::default();
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 1);
    }

    #[test]
    fn should_be_new() {
        let config = IggyProducerConfig::new(
            String::from("test_stream"),
            String::from("test_topic"),
            3,
            100,
            IggyDuration::from_str("5ms").unwrap(),
            Partitioning::balanced(),
            None,
        );
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 3);
    }

    #[test]
    fn should_be_from_stream_topic() {
        let res = IggyProducerConfig::from_stream_topic(
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
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 1);
    }
}
