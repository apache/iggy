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

//! Produce-side bridge calls.
//!
//! Separate from `fetch.rs` so the two never edit one file.

use iggy::prelude::{Identifier, IggyMessage, MessageClient, Partitioning};

use super::{IggyBridge, with_request_timeout};
use crate::bridge::error::BridgeError;
use crate::bridge::topic_map::validate_kafka_topic_name;

impl IggyBridge {
    /// Appends `messages` to one partition of the Iggy topic `kafka_topic` maps to, and reports
    /// where the batch landed.
    ///
    /// Takes the Kafka-side name so a topic with a mapping override reaches the same Iggy
    /// resource the other bridge calls do. `partition` passes through unconverted, since both
    /// systems number partitions from 0 and the client already chose one. An index the topic
    /// does not have is rejected by the server, so this spends no round trip checking first.
    ///
    /// Creates nothing. A producer asks `Metadata` to create a missing topic before it produces
    /// to it, so auto-creation belongs to that handler.
    ///
    /// `Ok(None)` is a committed send the server named no offset for, which happens for a
    /// request it classified as a duplicate and for a batch whose journal entry is gone. The
    /// caller answers `-1`. Retrying would duplicate a write that already committed.
    ///
    /// The first confirmation naming `partition` wins: the server reports one per send, and a
    /// reply carrying several would carry them in send order.
    ///
    /// # Errors
    ///
    /// [`BridgeError::InvalidKafkaTopicName`] if the name fails Kafka's own rules.
    /// [`BridgeError::Timeout`] if the send outruns `REQUEST_TIMEOUT`, which leaves the outcome
    /// unknown rather than known-failed. [`BridgeError::Iggy`] if the stream, topic or partition
    /// is missing, the message is too large, or the connection failed.
    pub async fn send_records(
        &self,
        kafka_topic: &str,
        partition: u32,
        messages: &mut [IggyMessage],
    ) -> Result<Option<u64>, BridgeError> {
        validate_kafka_topic_name("kafka_topic", kafka_topic)?;
        let (stream_name, topic_name) = self.config.topic_mapping.resolve(kafka_topic);
        let stream_id = Identifier::named(stream_name).map_err(BridgeError::Iggy)?;
        let topic_id = Identifier::named(topic_name).map_err(BridgeError::Iggy)?;
        let partitioning = Partitioning::partition_id(partition);

        let response = with_request_timeout(self.client.send_messages(
            &stream_id,
            &topic_id,
            &partitioning,
            messages,
        ))
        .await?;

        Ok(response
            .confirmations
            .iter()
            .find(|confirmation| confirmation.partition_id == partition)
            .map(|confirmation| confirmation.base_offset))
    }
}
