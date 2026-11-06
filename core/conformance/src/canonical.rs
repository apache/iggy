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

//! The canonical JSON forms of call results.
//!
//! Every agent returns these forms, and the orchestrator decodes each result
//! into its type before it compares values. The types reject unknown fields,
//! so a misnamed field fails a test even when the test does not mention it.
//! Field names follow the Rust SDK types. Sizes are plain byte counts, and an
//! option map uses the HTTP JSON form of the shared codec fixtures.

use bytes::Bytes;
use iggy_common::{
    ConsumerGroupDetails, HeaderKey, HeaderValue, IdentityInfo, IggyError, IggyMessage, Partition,
    PolledMessages, ResourceOptions, Stream, StreamDetails, Topic, TopicDetails,
    resource_options_json,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// An option of a stream or a topic: the value as reversible text, its kind
/// and whether a client set it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionEntry {
    pub value: String,
    pub kind: String,
    pub explicit: bool,
}

pub type Options = BTreeMap<String, OptionEntry>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityResult {
    pub user_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamResult {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub topics_count: u32,
    pub options: Options,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDetailsResult {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub topics_count: u32,
    pub topics: Vec<TopicResult>,
    pub options: Options,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicResult {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub partitions_count: u32,
    pub options: Options,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicDetailsResult {
    pub id: u32,
    pub name: String,
    pub created_at: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub partitions_count: u32,
    pub partitions: Vec<PartitionResult>,
    pub options: Options,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionResult {
    pub id: u32,
    pub created_at: u64,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolledMessagesResult {
    pub partition_id: u32,
    pub current_offset: u64,
    pub count: u32,
    pub messages: Vec<MessageResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageResult {
    pub offset: u64,
    /// The u128 id as decimal text, which no JSON number holds exactly.
    pub id: String,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub checksum: u64,
    /// The payload as UTF-8 text, absent when it is not valid UTF-8.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<String>,
    pub payload_hex: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, HeaderEntry>,
}

/// A user header: its kind and its value as reversible text, as in an option.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderEntry {
    pub kind: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumerGroupResult {
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
    pub members_count: u32,
    pub members: Vec<MemberResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberResult {
    pub id: u32,
    pub partitions_count: u32,
    pub partitions: Vec<u32>,
}

impl From<&IdentityInfo> for IdentityResult {
    fn from(identity: &IdentityInfo) -> Self {
        Self {
            user_id: identity.user_id,
        }
    }
}

impl TryFrom<&Stream> for StreamResult {
    type Error = IggyError;

    fn try_from(stream: &Stream) -> Result<Self, IggyError> {
        Ok(Self {
            id: stream.id,
            name: stream.name.clone(),
            created_at: stream.created_at.as_micros(),
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            options: options(&stream.options)?,
        })
    }
}

impl TryFrom<&StreamDetails> for StreamDetailsResult {
    type Error = IggyError;

    fn try_from(stream: &StreamDetails) -> Result<Self, IggyError> {
        Ok(Self {
            id: stream.id,
            name: stream.name.clone(),
            created_at: stream.created_at.as_micros(),
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            topics: stream
                .topics
                .iter()
                .map(TopicResult::try_from)
                .collect::<Result<_, _>>()?,
            options: options(&stream.options)?,
        })
    }
}

impl TryFrom<&Topic> for TopicResult {
    type Error = IggyError;

    fn try_from(topic: &Topic) -> Result<Self, IggyError> {
        Ok(Self {
            id: topic.id,
            name: topic.name.clone(),
            created_at: topic.created_at.as_micros(),
            size_bytes: topic.size.as_bytes_u64(),
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            options: options(&topic.options)?,
        })
    }
}

impl TryFrom<&TopicDetails> for TopicDetailsResult {
    type Error = IggyError;

    fn try_from(topic: &TopicDetails) -> Result<Self, IggyError> {
        Ok(Self {
            id: topic.id,
            name: topic.name.clone(),
            created_at: topic.created_at.as_micros(),
            size_bytes: topic.size.as_bytes_u64(),
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            partitions: topic.partitions.iter().map(PartitionResult::from).collect(),
            options: options(&topic.options)?,
        })
    }
}

impl From<&Partition> for PartitionResult {
    fn from(partition: &Partition) -> Self {
        Self {
            id: partition.id,
            created_at: partition.created_at.as_micros(),
            segments_count: partition.segments_count,
            current_offset: partition.current_offset,
            size_bytes: partition.size.as_bytes_u64(),
            messages_count: partition.messages_count,
        }
    }
}

impl TryFrom<&PolledMessages> for PolledMessagesResult {
    type Error = IggyError;

    fn try_from(polled: &PolledMessages) -> Result<Self, IggyError> {
        Ok(Self {
            partition_id: polled.partition_id,
            current_offset: polled.current_offset,
            count: polled.count,
            messages: polled
                .messages
                .iter()
                .map(MessageResult::try_from)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<&IggyMessage> for MessageResult {
    type Error = IggyError;

    fn try_from(message: &IggyMessage) -> Result<Self, IggyError> {
        let headers = message
            .user_headers_map()?
            .unwrap_or_default()
            .iter()
            .map(|(key, value)| Ok((header_key(key), header_entry(value)?)))
            .collect::<Result<_, IggyError>>()?;
        Ok(Self {
            offset: message.header.offset,
            id: message.header.id.to_string(),
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            checksum: message.header.checksum,
            payload: utf8(&message.payload),
            payload_hex: hex::encode(&message.payload),
            headers,
        })
    }
}

impl From<&ConsumerGroupDetails> for ConsumerGroupResult {
    fn from(group: &ConsumerGroupDetails) -> Self {
        Self {
            id: group.id,
            name: group.name.clone(),
            partitions_count: group.partitions_count,
            members_count: group.members_count,
            members: group
                .members
                .iter()
                .map(|member| MemberResult {
                    id: member.id,
                    partitions_count: member.partitions_count,
                    partitions: member.partitions.clone(),
                })
                .collect(),
        }
    }
}

/// The HTTP JSON form of an option map, which the codec fixtures share.
fn options(options: &ResourceOptions) -> Result<Options, IggyError> {
    options
        .iter()
        .map(|(key, option)| {
            Ok((
                header_key(key),
                OptionEntry {
                    value: resource_options_json::encode_value(&option.value)?,
                    kind: kind_name(&option.value)?,
                    explicit: option.explicit,
                },
            ))
        })
        .collect()
}

fn header_entry(value: &HeaderValue) -> Result<HeaderEntry, IggyError> {
    Ok(HeaderEntry {
        kind: kind_name(value)?,
        value: resource_options_json::encode_value(value)?,
    })
}

fn kind_name(value: &HeaderValue) -> Result<String, IggyError> {
    match serde_json::to_value(value.kind()) {
        Ok(serde_json::Value::String(name)) => Ok(name),
        _ => Err(IggyError::InvalidFormat),
    }
}

fn header_key(key: &HeaderKey) -> String {
    String::from_utf8_lossy(key.as_bytes()).into_owned()
}

fn utf8(payload: &Bytes) -> Option<String> {
    std::str::from_utf8(payload).ok().map(str::to_owned)
}
