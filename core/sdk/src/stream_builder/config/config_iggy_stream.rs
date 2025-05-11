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

use crate::prelude::{Identifier, IggyDuration, IggyError};
use crate::stream_builder::{IggyConsumerConfig, IggyProducerConfig};
use bon::Builder;

#[derive(Builder, Default, Debug, Clone)]
pub struct IggyStreamConfig {
    consumer_config: IggyConsumerConfig,
    producer_config: IggyProducerConfig,
}

impl IggyStreamConfig {
    /// Creates a new `IggyStreamConfig` with the given consumer and producer configurations.
    ///
    /// # Args
    ///
    /// * `consumer_config` - The consumer configuration.
    /// * `producer_config` - The producer configuration.
    ///
    /// Returns:
    /// A new `IggyStreamConfig`.
    ///
    pub fn new(consumer_config: IggyConsumerConfig, producer_config: IggyProducerConfig) -> Self {
        Self {
            consumer_config,
            producer_config,
        }
    }

    /// Creates a new `IggyStreamConfig` from the given stream and topic names, along with the max
    /// batch size, the send interval and the polling interval.
    ///
    /// # Args
    ///
    /// * `stream` - The stream name.
    /// * `topic` - The topic name.
    /// * `batch_size` - The max number of messages to send in a batch.
    /// * `send_interval` - The interval between messages sent.
    /// * `polling_interval` - The interval between polling for new messages.
    ///
    /// Returns:
    /// A new `IggyStreamConfig`.
    ///
    pub fn from_stream_topic(
        stream: &str,
        topic: &str,
        batch_size: u32,
        send_interval: IggyDuration,
        polling_interval: IggyDuration,
    ) -> Result<Self, IggyError> {
        let consumer_config =
            IggyConsumerConfig::from_stream_topic(stream, topic, batch_size, polling_interval)?;

        let producer_config =
            IggyProducerConfig::from_stream_topic(stream, topic, batch_size, send_interval)?;

        Ok(Self {
            consumer_config,
            producer_config,
        })
    }
}

impl IggyStreamConfig {
    pub fn consumer_config(&self) -> &IggyConsumerConfig {
        &self.consumer_config
    }

    pub fn producer_config(&self) -> &IggyProducerConfig {
        &self.producer_config
    }

    pub fn stream_id(&self) -> &Identifier {
        self.producer_config.stream_id()
    }

    pub fn stream_name(&self) -> &str {
        self.producer_config.stream_name()
    }

    pub fn topic_id(&self) -> &Identifier {
        self.producer_config.topic_id()
    }

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
        assert_eq!(config.consumer_config().batch_size(), 100);
        assert_eq!(config.producer_config().batch_size(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().send_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
    }

    #[test]
    fn should_be_default() {
        let config = IggyStreamConfig::default();
        assert_eq!(config.stream_name(), "test_stream");
        assert_eq!(config.topic_name(), "test_topic");
        assert_eq!(config.consumer_config().batch_size(), 100);
        assert_eq!(config.producer_config().batch_size(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().send_interval(),
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
        assert_eq!(config.consumer_config().batch_size(), 100);
        assert_eq!(config.producer_config().batch_size(), 100);
        assert_eq!(
            config.consumer_config().polling_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
        assert_eq!(
            config.producer_config().send_interval(),
            IggyDuration::from_str("5ms").unwrap()
        );
    }
}
