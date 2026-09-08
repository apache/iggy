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
    EncryptorKind, Identifier, IggyDuration, IggyError, NonZeroIggyDuration, Partitioning,
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
/// The builder requires every field that is not an [`Option`], so it requires
/// [`stream_id()`](Self::stream_id), [`stream_name()`](Self::stream_name),
/// [`topic_id()`](Self::topic_id), [`topic_name()`](Self::topic_name),
/// [`topic_partitions_count()`](Self::topic_partitions_count),
/// [`batch_length()`](Self::batch_length), [`linger_time()`](Self::linger_time) and
/// [`partitioning()`](Self::partitioning).
///
/// # Fields and defaults
///
/// The defaults below are what [`default()`](Self::default) and
/// [`from_stream_topic()`](Self::from_stream_topic) set. The builder has no defaults.
///
/// | Field | Default | Controls |
/// | --- | --- | --- |
/// | [`stream_name()`](Self::stream_name), [`topic_name()`](Self::topic_name) | `test_stream`, `test_topic` | where messages are appended |
/// | [`stream_id()`](Self::stream_id), [`topic_id()`](Self::topic_id) | the same two names | nothing in the producer, see below |
/// | [`topic_partitions_count()`](Self::topic_partitions_count) | 1 | partitions of a topic the build creates |
/// | [`batch_length()`](Self::batch_length) | 100 | messages per request |
/// | [`linger_time()`](Self::linger_time) | 5 milliseconds | smallest gap between two send calls |
/// | [`partitioning()`](Self::partitioning) | [`Partitioning::balanced()`] | which partition a batch lands in |
/// | [`send_retries_count()`](Self::send_retries_count) | three retries | retrying a failed request |
/// | [`send_retries_interval()`](Self::send_retries_interval) | one second | pacing those retries |
/// | [`encryptor()`](Self::encryptor) | none | encrypting payloads and user headers |
///
/// The build reads the two names, not the two identifiers, so [`stream_id()`](Self::stream_id) and
/// [`topic_id()`](Self::topic_id) reach neither the producer nor the server. They are there for
/// your own calls, such as passing [`stream_id()`](Self::stream_id) to
/// [`StreamClient::delete_stream()`]. Keep each identifier and its name in step, because nothing
/// checks that they match.
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
/// # fn example() -> Result<(), IggyError> {
/// let config = IggyProducerConfig::from_stream_topic(
///     "my-stream",
///     "my-topic",
///     100,
///     IggyDuration::new_from_secs(1),
/// )?;
///
/// assert_eq!(config.topic_partitions_count(), 1);
/// assert_eq!(config.send_retries_count(), Some(3));
/// # Ok(())
/// # }
/// ```
///
/// Ten partitions, every message keyed to one of them, and five retries:
///
/// ```rust
/// use iggy::prelude::*;
///
/// # fn example() -> Result<(), IggyError> {
/// let config = IggyProducerConfig::builder()
///     .stream_id(Identifier::from_str_value("my-stream")?)
///     .stream_name("my-stream")
///     .topic_id(Identifier::from_str_value("my-topic")?)
///     .topic_name("my-topic")
///     .topic_partitions_count(10)
///     .batch_length(100)
///     .linger_time(IggyDuration::new_from_secs(1))
///     .partitioning(Partitioning::messages_key_str("my-key")?)
///     .send_retries_count(5)
///     .send_retries_interval(NonZeroIggyDuration::ONE_SECOND)
///     .build();
///
/// assert_eq!(config.topic_partitions_count(), 10);
/// # Ok(())
/// # }
/// ```
///
/// [`IggyClient`]: crate::prelude::IggyClient
/// [`IggyStream`]: crate::prelude::IggyStream
/// [`IggyStreamProducer`]: crate::prelude::IggyStreamProducer
/// [`Partitioning::balanced()`]: crate::prelude::Partitioning::balanced
/// [`StreamClient::delete_stream()`]: crate::prelude::StreamClient::delete_stream
#[derive(Builder, Debug, Clone)]
#[builder(on(String, into))]
pub struct IggyProducerConfig {
    /// Identifier of the stream. Must be unique.
    stream_id: Identifier,
    /// Name of the stream. Must be unique.
    stream_name: String,
    /// Identifier of the topic. Must be unique.
    topic_id: Identifier,
    /// Name of the topic. Must be unique.
    topic_name: String,
    /// Sets the number of partitions to create for the topic
    topic_partitions_count: u32,
    /// Maximum messages per direct-send request. Zero uses the SDK's maximum batch length.
    batch_length: u32,
    /// Minimum gap between sequential direct sends, measured from the previous successful send.
    linger_time: IggyDuration,
    /// Specifies to which partition the messages should be sent.
    partitioning: Partitioning,
    /// Sets the maximum number of send retries in case of a message sending failure.
    send_retries_count: Option<u32>,
    /// Sets the interval between send retries in case of a message sending failure.
    send_retries_interval: Option<NonZeroIggyDuration>,
    /// Sets a optional client side encryptor for encrypting the messages' payloads. Currently only Aes256Gcm is supported.
    /// Note, this is independent of server side encryption meaning you can add client encryption, server encryption, or both.
    encryptor: Option<Arc<EncryptorKind>>,
}

impl Default for IggyProducerConfig {
    fn default() -> Self {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        Self {
            stream_id,
            stream_name: "test_stream".to_string(),
            topic_id,
            topic_name: "test_topic".to_string(),
            batch_length: 100,
            linger_time: IggyDuration::from_str("5ms").unwrap(),
            partitioning: Partitioning::balanced(),
            topic_partitions_count: 1,
            encryptor: None,
            send_retries_count: Some(3),
            send_retries_interval: Some(NonZeroIggyDuration::ONE_SECOND),
        }
    }
}

impl IggyProducerConfig {
    /// Sets every field at once, positionally.
    ///
    /// This applies no defaults. [`builder()`](Self::builder) sets the same fields by name and is
    /// easier to read.
    ///
    /// # Examples
    ///
    /// Describe a topic with three partitions and no retries:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn example() -> Result<(), IggyError> {
    /// let config = IggyProducerConfig::new(
    ///     Identifier::from_str_value("my-stream")?,
    ///     "my-stream".to_string(),
    ///     Identifier::from_str_value("my-topic")?,
    ///     "my-topic".to_string(),
    ///     3,
    ///     100,
    ///     IggyDuration::new_from_secs(1),
    ///     Partitioning::balanced(),
    ///     None,
    ///     None,
    ///     None,
    /// );
    ///
    /// assert_eq!(config.send_retries_count(), None);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        stream_name: String,
        topic_id: Identifier,
        topic_name: String,
        topic_partitions_count: u32,
        batch_length: u32,
        linger_time: IggyDuration,
        partitioning: Partitioning,
        encryptor: Option<Arc<EncryptorKind>>,
        send_retries_count: Option<u32>,
        send_retries_interval: Option<NonZeroIggyDuration>,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            topic_partitions_count,
            batch_length,
            linger_time,
            partitioning,
            encryptor,
            send_retries_count,
            send_retries_interval,
        }
    }

    /// Names one stream and one topic, and takes the defaults for the rest.
    ///
    /// Each identifier is derived from the matching name. The topic gets one partition, balanced
    /// partitioning, three send retries one second apart, and no encryptor.
    ///
    /// # Examples
    ///
    /// Describe one topic with 100 messages per request:
    ///
    /// ```rust
    /// use iggy::prelude::*;
    ///
    /// # fn example() -> Result<(), IggyError> {
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
        let stream_id = Identifier::from_str_value(stream)?;
        let topic_id = Identifier::from_str_value(topic)?;

        Ok(Self {
            stream_id,
            stream_name: stream.to_string(),
            topic_id,
            topic_name: topic.to_string(),
            batch_length,
            linger_time,
            partitioning: Partitioning::balanced(),
            topic_partitions_count: 1,
            encryptor: None,
            send_retries_count: Some(3),
            send_retries_interval: Some(NonZeroIggyDuration::ONE_SECOND),
        })
    }
}

impl IggyProducerConfig {
    /// Returns the stream identifier, which the build itself does not read.
    pub fn stream_id(&self) -> &Identifier {
        &self.stream_id
    }

    /// Returns the name of the stream that messages are appended to.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    /// Returns the topic identifier, which the build itself does not read.
    pub fn topic_id(&self) -> &Identifier {
        &self.topic_id
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

    /// Returns the strategy that decides which partition a batch lands in.
    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    /// Returns how many partitions a topic created by the build gets.
    ///
    /// An existing topic keeps the partitions it has.
    pub fn topic_partitions_count(&self) -> u32 {
        self.topic_partitions_count
    }

    /// Returns the encryptor for payloads and user headers, if there is one.
    ///
    /// It replaces the encryptor of the client that builds the producer.
    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }

    /// Returns how often a failed request is sent again, if at all.
    pub fn send_retries_count(&self) -> Option<u32> {
        self.send_retries_count
    }

    /// Returns the interval that paces those retries, if there is one.
    ///
    /// [`None`] retries back to back without any wait.
    pub fn send_retries_interval(&self) -> Option<NonZeroIggyDuration> {
        self.send_retries_interval
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
            .stream_id(Identifier::from_str_value(stream).unwrap())
            .stream_name(stream)
            .topic_id(Identifier::from_str_value(topic).unwrap())
            .topic_name(topic)
            .topic_partitions_count(3)
            .batch_length(100)
            .linger_time(IggyDuration::from_str("5ms").unwrap())
            .partitioning(Partitioning::balanced())
            .send_retries_count(3)
            .send_retries_interval(NonZeroIggyDuration::ONE_SECOND)
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
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 3);
        assert_eq!(config.send_retries_count(), Some(3));
        assert_eq!(
            config.send_retries_interval(),
            Some(NonZeroIggyDuration::ONE_SECOND)
        );
    }

    #[test]
    fn should_be_default() {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        let config = IggyProducerConfig::default();
        assert_eq!(config.stream_id(), &stream_id);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_id(), &topic_id);
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 1);
        assert_eq!(config.send_retries_count(), Some(3));
        assert_eq!(
            config.send_retries_interval(),
            Some(NonZeroIggyDuration::ONE_SECOND)
        );
    }

    #[test]
    fn should_be_new() {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        let config = IggyProducerConfig::new(
            stream_id.clone(),
            String::from("test_stream"),
            topic_id.clone(),
            String::from("test_topic"),
            3,
            100,
            IggyDuration::from_str("5ms").unwrap(),
            Partitioning::balanced(),
            None,
            None,
            None,
        );
        assert_eq!(config.stream_id(), &stream_id);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_id(), &topic_id);
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 3);
        assert_eq!(config.send_retries_count(), None);
        assert_eq!(config.send_retries_interval(), None);
    }

    #[test]
    fn should_be_from_stream_topic() {
        let stream_id = Identifier::from_str_value("test_stream").unwrap();
        let topic_id = Identifier::from_str_value("test_topic").unwrap();

        let res = IggyProducerConfig::from_stream_topic(
            "test_stream",
            "test_topic",
            100,
            IggyDuration::from_str("5ms").unwrap(),
        );

        assert!(res.is_ok());
        let config = res.unwrap();

        assert_eq!(config.stream_id(), &stream_id);
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_id(), &topic_id);
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.batch_length(), 100);
        assert_eq!(config.linger_time(), IggyDuration::from_str("5ms").unwrap());
        assert_eq!(config.partitioning(), &Partitioning::balanced());
        assert_eq!(config.topic_partitions_count(), 1);
    }
}
