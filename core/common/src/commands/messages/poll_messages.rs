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

use crate::error::IggyError;
use crate::{BytesSerializable, Identifier, PollingKind, PollingStrategy, Sizeable, Validatable};
use crate::{Command, POLL_MESSAGES_CODE};
use crate::{Consumer, ConsumerKind};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

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

impl Command for PollMessages {
    fn code(&self) -> u32 {
        POLL_MESSAGES_CODE
    }
}

impl Validatable<IggyError> for PollMessages {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl BytesSerializable for PollMessages {
    fn to_bytes(&self) -> Bytes {
        PollMessages::bytes(
            &self.stream_id,
            &self.topic_id,
            self.partition_id,
            &self.consumer,
            &self.strategy,
            self.count,
            self.auto_commit,
        )
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() < 30 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0])?;
        let consumer_id = Identifier::from_bytes(bytes.slice(1..))?;
        position += 1 + consumer_id.get_size_bytes().as_bytes_usize();
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        // Decode partition_id with flag byte: 1 = Some, 0 = None
        let has_partition_id = bytes[position];
        let partition_id_value = u32::from_le_bytes(
            bytes[position + 1..position + 5]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let partition_id = if has_partition_id == 1 {
            Some(partition_id_value)
        } else {
            None
        };
        let polling_kind = PollingKind::from_code(bytes[position + 5])?;
        position += 6;
        let value = u64::from_le_bytes(
            bytes[position..position + 8]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let strategy = PollingStrategy {
            kind: polling_kind,
            value,
        };
        let count = u32::from_le_bytes(
            bytes[position + 8..position + 12]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let auto_commit = bytes[position + 12];
        let auto_commit = matches!(auto_commit, 1);
        let command = PollMessages {
            consumer,
            stream_id,
            topic_id,
            partition_id,
            strategy,
            count,
            auto_commit,
        };
        Ok(command)
    }
}

impl Display for PollMessages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}|{}|{}|{}",
            self.consumer,
            self.stream_id,
            self.topic_id,
            self.partition_id.unwrap_or(0),
            self.strategy,
            self.count,
            auto_commit_to_string(self.auto_commit)
        )
    }
}

fn auto_commit_to_string(auto_commit: bool) -> &'static str {
    if auto_commit { "a" } else { "n" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = PollMessages {
            consumer: Consumer::new(Identifier::numeric(1).unwrap()),
            stream_id: Identifier::numeric(2).unwrap(),
            topic_id: Identifier::numeric(3).unwrap(),
            partition_id: Some(4),
            strategy: PollingStrategy::offset(2),
            count: 3,
            auto_commit: true,
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let consumer_kind = ConsumerKind::from_code(bytes[0]).unwrap();
        let consumer_id = Identifier::from_bytes(bytes.slice(1..)).unwrap();
        position += 1 + consumer_id.get_size_bytes().as_bytes_usize();
        let consumer = Consumer {
            kind: consumer_kind,
            id: consumer_id,
        };
        let stream_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let has_partition_id = bytes[position];
        let partition_id =
            u32::from_le_bytes(bytes[position + 1..position + 5].try_into().unwrap());
        let partition_id = if has_partition_id == 1 {
            Some(partition_id)
        } else {
            None
        };
        let polling_kind = PollingKind::from_code(bytes[position + 5]).unwrap();
        position += 6;
        let value = u64::from_le_bytes(bytes[position..position + 8].try_into().unwrap());
        let strategy = PollingStrategy {
            kind: polling_kind,
            value,
        };
        let count = u32::from_le_bytes(bytes[position + 8..position + 12].try_into().unwrap());
        let auto_commit = bytes[position + 12];
        let auto_commit = matches!(auto_commit, 1);

        assert!(!bytes.is_empty());
        assert_eq!(consumer, command.consumer);
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(strategy, command.strategy);
        assert_eq!(count, command.count);
        assert_eq!(auto_commit, command.auto_commit);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let consumer = Consumer::new(Identifier::numeric(1).unwrap());
        let stream_id = Identifier::numeric(2).unwrap();
        let topic_id = Identifier::numeric(3).unwrap();
        let partition_id = 4u32;
        let strategy = PollingStrategy::offset(2);
        let count = 3u32;
        let auto_commit = 1u8;

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
        bytes.put_u8(1); // Flag: partition_id is Some
        bytes.put_u32_le(partition_id);
        bytes.put_slice(&strategy_bytes);
        bytes.put_u32_le(count);
        bytes.put_u8(auto_commit);

        let command = PollMessages::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let auto_commit = matches!(auto_commit, 1);

        let command = command.unwrap();
        assert_eq!(command.consumer, consumer);
        assert_eq!(command.stream_id, stream_id);
        assert_eq!(command.topic_id, topic_id);
        assert_eq!(command.partition_id, Some(partition_id));
        assert_eq!(command.strategy, strategy);
        assert_eq!(command.count, count);
        assert_eq!(command.auto_commit, auto_commit);
    }
}
