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

use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, POLL_MESSAGES_CODE};
use crate::consumer::{Consumer, ConsumerKind};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::utils::sizeable::Sizeable;
use crate::utils::timestamp::IggyTimestamp;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::fmt::Display;
use std::str::FromStr;

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
    #[serde(default = "default_partition_id")]
    pub partition_id: Option<u32>,
    /// Polling strategy which specifies from where to start polling messages.
    #[serde(default = "default_strategy", flatten)]
    pub strategy: PollingStrategy,
    #[serde(default = "default_count")]
    /// Number of messages to poll.
    pub count: u32,
    #[serde(default)]
    /// Whether to commit offset on the server automatically after polling the messages.
    pub auto_commit: bool,
}

/// `PollingStrategy` specifies from where to start polling messages.
/// It has the following kinds:
/// - `Offset` - start polling from the specified offset.
/// - `Timestamp` - start polling from the specified timestamp.
/// - `First` - start polling from the first message in the partition.
/// - `Last` - start polling from the last message in the partition.
/// - `Next` - start polling from the next message after the last polled message based on the stored consumer offset.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Copy, Clone)]
pub struct PollingStrategy {
    /// Kind of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_kind")]
    pub kind: PollingKind,
    /// Value of the polling strategy.
    #[serde_as(as = "DisplayFromStr")]
    #[serde(default = "default_value")]
    pub value: u64,
}

/// `PollingKind` is an enum which specifies from where to start polling messages and is used by `PollingStrategy`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PollingKind {
    #[default]
    /// Start polling from the specified offset.
    Offset,
    /// Start polling from the specified timestamp.
    Timestamp,
    /// Start polling from the first message in the partition.
    First,
    /// Start polling from the last message in the partition.
    Last,
    /// Start polling from the next message after the last polled message based on the stored consumer offset. Should be used with `auto_commit` set to `true`.
    Next,
}

impl Default for PollMessages {
    fn default() -> Self {
        Self {
            consumer: Consumer::default(),
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(1).unwrap(),
            partition_id: default_partition_id(),
            strategy: default_strategy(),
            count: default_count(),
            auto_commit: false,
        }
    }
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self {
            kind: PollingKind::Offset,
            value: 0,
        }
    }
}

impl Command for PollMessages {
    fn code(&self) -> u32 {
        POLL_MESSAGES_CODE
    }
}

fn default_partition_id() -> Option<u32> {
    Some(1)
}

fn default_kind() -> PollingKind {
    PollingKind::Offset
}

fn default_value() -> u64 {
    0
}

fn default_strategy() -> PollingStrategy {
    PollingStrategy::default()
}

fn default_count() -> u32 {
    10
}

impl Validatable<IggyError> for PollMessages {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl PollingStrategy {
    /// Poll messages from the specified offset.
    pub fn offset(value: u64) -> Self {
        Self {
            kind: PollingKind::Offset,
            value,
        }
    }

    /// Poll messages from the specified timestamp.
    pub fn timestamp(value: IggyTimestamp) -> Self {
        Self {
            kind: PollingKind::Timestamp,
            value: value.into(),
        }
    }

    /// Poll messages from the first message in the partition.
    pub fn first() -> Self {
        Self {
            kind: PollingKind::First,
            value: 0,
        }
    }

    /// Poll messages from the last message in the partition.
    pub fn last() -> Self {
        Self {
            kind: PollingKind::Last,
            value: 0,
        }
    }

    /// Poll messages from the next message after the last polled message based on the stored consumer offset. Should be used with `auto_commit` set to `true`.
    pub fn next() -> Self {
        Self {
            kind: PollingKind::Next,
            value: 0,
        }
    }

    /// Change the value of the polling strategy, affects only `Offset` and `Timestamp` kinds.
    pub fn set_value(&mut self, value: u64) {
        if self.kind == PollingKind::Offset || self.kind == PollingKind::Timestamp {
            self.value = value;
        }
    }
}

impl PollingKind {
    /// Returns code of the polling kind.
    pub fn as_code(&self) -> u8 {
        match self {
            PollingKind::Offset => 1,
            PollingKind::Timestamp => 2,
            PollingKind::First => 3,
            PollingKind::Last => 4,
            PollingKind::Next => 5,
        }
    }

    /// Returns polling kind from the specified code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(PollingKind::Offset),
            2 => Ok(PollingKind::Timestamp),
            3 => Ok(PollingKind::First),
            4 => Ok(PollingKind::Last),
            5 => Ok(PollingKind::Next),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl FromStr for PollingKind {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "o" | "offset" => Ok(PollingKind::Offset),
            "t" | "timestamp" => Ok(PollingKind::Timestamp),
            "f" | "first" => Ok(PollingKind::First),
            "l" | "last" => Ok(PollingKind::Last),
            "n" | "next" => Ok(PollingKind::Next),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for PollingKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PollingKind::Offset => write!(f, "offset"),
            PollingKind::Timestamp => write!(f, "timestamp"),
            PollingKind::First => write!(f, "first"),
            PollingKind::Last => write!(f, "last"),
            PollingKind::Next => write!(f, "next"),
        }
    }
}

impl BytesSerializable for PollMessages {
    fn to_bytes(&self) -> Bytes {
        as_bytes(
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
        if bytes.len() < 29 {
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
        let partition_id = u32::from_le_bytes(
            bytes[position..position + 4]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let partition_id = match partition_id {
            0 => None,
            partition_id => Some(partition_id),
        };
        let polling_kind = PollingKind::from_code(bytes[position + 4])?;
        position += 5;
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

// This method is used by the new version of `IggyClient` to serialize `PollMessages` without cloning the args.
pub(crate) fn as_bytes(
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
        9 + consumer_bytes.len()
            + stream_id_bytes.len()
            + topic_id_bytes.len()
            + strategy_bytes.len(),
    );
    bytes.put_slice(&consumer_bytes);
    bytes.put_slice(&stream_id_bytes);
    bytes.put_slice(&topic_id_bytes);
    if let Some(partition_id) = partition_id {
        bytes.put_u32_le(partition_id);
    } else {
        bytes.put_u32_le(0);
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

impl Display for PollingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.value)
    }
}

fn auto_commit_to_string(auto_commit: bool) -> &'static str {
    if auto_commit {
        "a"
    } else {
        "n"
    }
}

impl BytesSerializable for PollingStrategy {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(9);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u64_le(self.value);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.len() != 9 {
            return Err(IggyError::InvalidCommand);
        }

        let kind = PollingKind::from_code(bytes[0])?;
        let value = u64::from_le_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let strategy = PollingStrategy { kind, value };
        Ok(strategy)
    }
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
        let partition_id = u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap());
        let polling_kind = PollingKind::from_code(bytes[position + 4]).unwrap();
        position += 5;
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
        assert_eq!(Some(partition_id), command.partition_id);
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
            9 + consumer_bytes.len()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + strategy_bytes.len(),
        );
        bytes.put_slice(&consumer_bytes);
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
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
