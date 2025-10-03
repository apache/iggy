/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::prelude::{EncryptorKind, Identifier, IggyDuration, IggyError, Partitioning};
use bon::Builder;
use std::str::FromStr;
use std::sync::Arc;

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
    topic_partitions_count: usize,
    /// Set the topic replication factor
    topic_replication_factor: Option<u8>,
    /// The max number of messages to send in a batch. Must be greater than 0.
    batch_length: u32,
    /// Sets the interval between sending the messages, can be combined with `batch_length`.
    linger_time: IggyDuration,
    /// Specifies to which partition the messages should be sent.
    partitioning: Partitioning,
    /// Sets the maximum number of send retries in case of a message sending failure.
    send_retries_count: Option<u32>,
    /// Sets the interval between send retries in case of a message sending failure.
    send_retries_interval: Option<IggyDuration>,
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
            topic_replication_factor: None,
            encryptor: None,
            send_retries_count: Some(3),
            send_retries_interval: Some(IggyDuration::new_from_secs(1)),
        }
    }
}

impl IggyProducerConfig {
    /// Creates a new `IggyProducerConfig` with all fields defined.
    ///
    /// # Args
    ///
    /// * `stream_id` - The stream identifier.
    /// * `stream_name` - The stream name.
    /// * `topic_id` - The topic identifier.
    /// * `topic_name` - The topic name.
    /// * `topic_partitions_count` - The number of partitions to create.
    /// * `topic_replication_factor` - The replication factor to use.
    /// * `batch_length` - The max number of messages to send in a batch.
    /// * `linger_time` - The interval between messages sent.
    /// * `partitioning` - The partitioning strategy to use.
    /// * `encryptor` - The encryptor to use.
    /// * `send_retries_count` - The number of retries to send messages.
    /// * `send_retries_interval` - The interval between retries.
    ///
    /// Returns:
    /// A new `IggyProducerConfig`.
    ///
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream_id: Identifier,
        stream_name: String,
        topic_id: Identifier,
        topic_name: String,
        topic_partitions_count: usize,
        topic_replication_factor: Option<u8>,
        batch_length: u32,
        linger_time: IggyDuration,
        partitioning: Partitioning,
        encryptor: Option<Arc<EncryptorKind>>,
        send_retries_count: Option<u32>,
        send_retries_interval: Option<IggyDuration>,
    ) -> Self {
        Self {
            stream_id,
            stream_name,
            topic_id,
            topic_name,
            topic_partitions_count,
            topic_replication_factor,
            batch_length,
            linger_time,
            partitioning,
            encryptor,
            send_retries_count,
            send_retries_interval,
        }
    }

    /// Creates a new `IggyProducerConfig` from the given stream and topic names, along with the
    /// max batch size and the send interval.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `batch_length` - The max number of messages to send in a batch.
    /// * `linger_time` - The interval between messages sent.
    ///
    /// Returns:
    /// A new `IggyProducerConfig`.
    ///
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
            topic_replication_factor: None,
            encryptor: None,
            send_retries_count: Some(3),
            send_retries_interval: Some(IggyDuration::new_from_secs(1)),
        })
    }
}

impl IggyProducerConfig {
    pub fn stream_id(&self) -> &Identifier {
        &self.stream_id
    }

    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    pub fn topic_id(&self) -> &Identifier {
        &self.topic_id
    }

    pub fn topic_name(&self) -> &str {
        &self.topic_name
    }

    pub fn batch_length(&self) -> u32 {
        self.batch_length
    }

    pub fn linger_time(&self) -> IggyDuration {
        self.linger_time
    }

    pub fn partitioning(&self) -> &Partitioning {
        &self.partitioning
    }

    pub fn topic_partitions_count(&self) -> usize {
        self.topic_partitions_count
    }

    pub fn topic_replication_factor(&self) -> Option<u8> {
        self.topic_replication_factor
    }

    pub fn encryptor(&self) -> Option<Arc<EncryptorKind>> {
        self.encryptor.clone()
    }

    pub fn send_retries_count(&self) -> Option<u32> {
        self.send_retries_count
    }

    pub fn send_retries_interval(&self) -> Option<IggyDuration> {
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
            .send_retries_interval(IggyDuration::new_from_secs(1))
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
        assert_eq!(config.topic_replication_factor(), None);
        assert_eq!(config.send_retries_count(), Some(3));
        assert_eq!(
            config.send_retries_interval(),
            Some(IggyDuration::new_from_secs(1))
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
        assert_eq!(config.topic_replication_factor(), None);
        assert_eq!(config.send_retries_count(), Some(3));
        assert_eq!(
            config.send_retries_interval(),
            Some(IggyDuration::new_from_secs(1))
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
            None,
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
        assert_eq!(config.topic_replication_factor(), None);
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
        assert_eq!(config.topic_replication_factor(), None);
    }
}
