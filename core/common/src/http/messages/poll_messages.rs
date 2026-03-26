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

use crate::Consumer;
use crate::error::IggyError;
use crate::{BytesSerializable, Identifier, PollingStrategy, Validatable};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

pub const DEFAULT_PARTITION_ID: u32 = 0;
pub const DEFAULT_NUMBER_OF_MESSAGES_TO_POLL: u32 = 10;

/// `PollMessages` command is used to poll messages from a topic in a stream.
/// It has additional payload:
/// - `consumer` - consumer which will poll messages. Either regular consumer or consumer group.
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partition_id` - partition ID from which messages will be polled. Has to be specified for the regular consumer. For consumer group it is ignored (use `None`).
/// - `strategy` - polling strategy which specifies from where to start polling messages.
/// - `count` - number of messages to poll.
/// - `auto_commit` - whether to commit offset on the server automatically after polling the messages.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct PollMessages {
    /// Consumer which will poll messages. Either regular consumer or consumer group.
    #[serde(flatten)]
    pub consumer: Consumer,
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Partition ID from which messages will be polled. Has to be specified for the regular consumer. For consumer group it is ignored (use `None`).
    #[serde(default = "PollMessages::default_partition_id")]
    pub partition_id: Option<u32>,
    /// Polling strategy which specifies from where to start polling messages.
    #[serde(default = "PollingStrategy::default", flatten)]
    pub strategy: PollingStrategy,
    /// Number of messages to poll.
    #[serde(default = "PollMessages::default_number_of_messages_to_poll")]
    pub count: u32,
    /// Whether to commit offset on the server automatically after polling the messages.
    #[serde(default)]
    pub auto_commit: bool,
}

impl PollMessages {
    pub fn bytes(
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        count: u32,
        auto_commit: bool,
    ) -> Bytes {
        let consumer_bytes = consumer.to_bytes();
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let strategy_bytes = strategy.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            10 + consumer_bytes.len()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + strategy_bytes.len(),
        );
        bytes.put_slice(&consumer_bytes);
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        // Encode partition_id with a flag byte: 1 = Some, 0 = None
        if let Some(partition_id) = partition_id {
            bytes.put_u8(1);
            bytes.put_u32_le(partition_id);
        } else {
            bytes.put_u8(0);
            bytes.put_u32_le(0); // Padding to keep structure consistent
        }
        bytes.put_slice(&strategy_bytes);
        bytes.put_u32_le(count);
        if auto_commit {
            bytes.put_u8(1);
        } else {
            bytes.put_u8(0);
        }

        bytes.freeze()
    }

    pub fn default_number_of_messages_to_poll() -> u32 {
        DEFAULT_NUMBER_OF_MESSAGES_TO_POLL
    }

    pub fn default_partition_id() -> Option<u32> {
        Some(DEFAULT_PARTITION_ID)
    }
}

impl Default for PollMessages {
    fn default() -> Self {
        Self {
            consumer: Consumer::default(),
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(1).unwrap(),
            partition_id: PollMessages::default_partition_id(),
            strategy: PollingStrategy::default(),
            count: PollMessages::default_number_of_messages_to_poll(),
            auto_commit: false,
        }
    }
}

impl Validatable<IggyError> for PollMessages {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}
