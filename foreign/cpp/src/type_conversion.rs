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

use crate::ffi;
use bytes::Bytes;
use iggy::prelude::{
    ConsumerGroupDetails as RustConsumerGroupDetails, IdKind, Identifier as RustIdentifier,
    IggyMessage as RustIggyMessage, Partition as RustPartition,
    PolledMessages as RustPolledMessages, Stream as RustStream, StreamDetails as RustStreamDetails,
    Topic as RustTopic, TopicDetails as RustTopicDetails, Validatable,
};
use iggy_binary_protocol::WireUserHeaders;
use iggy_common::{
    CacheMetrics as RustCacheMetrics, CacheMetricsKey as RustCacheMetricsKey,
    ClientInfo as RustClientInfo, ClientInfoDetails as RustClientInfoDetails,
    ClusterMetadata as RustClusterMetadata, ClusterNode as RustClusterNode,
    ConsumerGroup as RustConsumerGroup, ConsumerGroupInfo as RustConsumerGroupInfo,
    ConsumerGroupMember as RustConsumerGroupMember, ConsumerOffsetInfo as RustConsumerOffsetInfo,
    GlobalPermissions as RustGlobalPermissions, HeaderEntry as RustHeaderEntry,
    HeaderField as RustHeaderField, HeaderKind as RustHeaderKind, Permissions as RustPermissions,
    Stats as RustStats, StreamPermissions as RustStreamPermissions,
    TopicPermissions as RustTopicPermissions, TransportEndpoints as RustTransportEndpoints,
};
use std::collections::BTreeMap;

impl From<RustIdentifier> for ffi::Identifier {
    fn from(identifier: RustIdentifier) -> Self {
        let kind = match identifier.kind {
            IdKind::Numeric => "numeric".to_string(),
            IdKind::String => "string".to_string(),
        };

        ffi::Identifier {
            kind,
            length: identifier.length,
            value: identifier.value,
        }
    }
}

impl TryFrom<ffi::Identifier> for RustIdentifier {
    type Error = String;

    fn try_from(identifier: ffi::Identifier) -> Result<Self, Self::Error> {
        let kind = match identifier.kind.as_str() {
            "numeric" => IdKind::Numeric,
            "string" => IdKind::String,
            _ => {
                return Err(format!(
                    "unsupported identifier kind '{}'. Expected 'numeric' or 'string'.",
                    identifier.kind
                ));
            }
        };

        let rust_identifier = RustIdentifier {
            kind,
            length: identifier.length,
            value: identifier.value,
        };

        rust_identifier
            .validate()
            .map_err(|error| format!("invalid identifier: {error}"))?;

        Ok(rust_identifier)
    }
}

impl From<RustClientInfo> for ffi::ClientInfo {
    fn from(client: RustClientInfo) -> Self {
        let has_user_id = client.user_id.is_some();
        ffi::ClientInfo {
            client_id: client.client_id,
            has_user_id,
            user_id: client.user_id.unwrap_or(u32::MAX),
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
        }
    }
}

impl From<RustClientInfoDetails> for ffi::ClientInfoDetails {
    fn from(client: RustClientInfoDetails) -> Self {
        let has_user_id = client.user_id.is_some();
        ffi::ClientInfoDetails {
            client_id: client.client_id,
            has_user_id,
            user_id: client.user_id.unwrap_or(u32::MAX),
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups: client
                .consumer_groups
                .into_iter()
                .map(ffi::ConsumerGroupInfo::from)
                .collect(),
        }
    }
}

impl TryFrom<Option<RustClientInfoDetails>> for ffi::ClientInfoDetails {
    type Error = String;

    fn try_from(client: Option<RustClientInfoDetails>) -> Result<Self, Self::Error> {
        match client {
            Some(client) => Ok(ffi::ClientInfoDetails::from(client)),
            None => Err("client not found".to_string()),
        }
    }
}

impl TryFrom<Option<RustConsumerOffsetInfo>> for ffi::ConsumerOffsetInfo {
    type Error = String;

    fn try_from(offset: Option<RustConsumerOffsetInfo>) -> Result<Self, Self::Error> {
        match offset {
            Some(offset) => Ok(ffi::ConsumerOffsetInfo {
                partition_id: offset.partition_id,
                current_offset: offset.current_offset,
                stored_offset: offset.stored_offset,
            }),
            None => Err("consumer offset not found".to_string()),
        }
    }
}

impl From<(RustCacheMetricsKey, RustCacheMetrics)> for ffi::CacheMetricEntry {
    fn from((key, metrics): (RustCacheMetricsKey, RustCacheMetrics)) -> Self {
        ffi::CacheMetricEntry {
            stream_id: key.stream_id,
            topic_id: key.topic_id,
            partition_id: key.partition_id,
            hits: metrics.hits,
            misses: metrics.misses,
            hit_ratio: metrics.hit_ratio,
        }
    }
}

impl From<RustStats> for ffi::Stats {
    fn from(stats: RustStats) -> Self {
        let has_server_semver = stats.iggy_server_semver.is_some();
        ffi::Stats {
            process_id: stats.process_id,
            cpu_usage: stats.cpu_usage,
            total_cpu_usage: stats.total_cpu_usage,
            memory_usage: stats.memory_usage.as_bytes_u64(),
            total_memory: stats.total_memory.as_bytes_u64(),
            available_memory: stats.available_memory.as_bytes_u64(),
            run_time_micros: stats.run_time.as_micros(),
            start_time_epoch_micros: stats.start_time.as_micros(),
            read_bytes: stats.read_bytes.as_bytes_u64(),
            written_bytes: stats.written_bytes.as_bytes_u64(),
            messages_size_bytes: stats.messages_size_bytes.as_bytes_u64(),
            streams_count: stats.streams_count,
            topics_count: stats.topics_count,
            partitions_count: stats.partitions_count,
            segments_count: stats.segments_count,
            messages_count: stats.messages_count,
            clients_count: stats.clients_count,
            consumer_groups_count: stats.consumer_groups_count,
            hostname: stats.hostname,
            os_name: stats.os_name,
            os_version: stats.os_version,
            kernel_version: stats.kernel_version,
            iggy_server_version: stats.iggy_server_version,
            has_server_semver,
            iggy_server_semver: stats.iggy_server_semver.unwrap_or(0),
            cache_metrics: stats
                .cache_metrics
                .into_iter()
                .map(ffi::CacheMetricEntry::from)
                .collect(),
            threads_count: stats.threads_count,
            free_disk_space: stats.free_disk_space.as_bytes_u64(),
            total_disk_space: stats.total_disk_space.as_bytes_u64(),
        }
    }
}

impl From<RustTransportEndpoints> for ffi::TransportEndpoints {
    fn from(endpoints: RustTransportEndpoints) -> Self {
        ffi::TransportEndpoints {
            tcp: endpoints.tcp,
            quic: endpoints.quic,
            http: endpoints.http,
            websocket: endpoints.websocket,
        }
    }
}

impl From<RustClusterNode> for ffi::ClusterNode {
    fn from(node: RustClusterNode) -> Self {
        ffi::ClusterNode {
            name: node.name,
            ip: node.ip,
            endpoints: ffi::TransportEndpoints::from(node.endpoints),
            role: node.role.to_string(),
            status: node.status.to_string(),
        }
    }
}

impl From<RustClusterMetadata> for ffi::ClusterMetadata {
    fn from(metadata: RustClusterMetadata) -> Self {
        ffi::ClusterMetadata {
            name: metadata.name,
            nodes: metadata
                .nodes
                .into_iter()
                .map(ffi::ClusterNode::from)
                .collect(),
        }
    }
}

impl From<ffi::GlobalPermissions> for RustGlobalPermissions {
    fn from(permissions: ffi::GlobalPermissions) -> Self {
        RustGlobalPermissions {
            manage_servers: permissions.manage_servers,
            read_servers: permissions.read_servers,
            manage_users: permissions.manage_users,
            read_users: permissions.read_users,
            manage_streams: permissions.manage_streams,
            read_streams: permissions.read_streams,
            manage_topics: permissions.manage_topics,
            read_topics: permissions.read_topics,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
        }
    }
}

impl From<ffi::TopicPermissions> for RustTopicPermissions {
    fn from(permissions: ffi::TopicPermissions) -> Self {
        RustTopicPermissions {
            manage_topic: permissions.manage_topic,
            read_topic: permissions.read_topic,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
        }
    }
}

impl TryFrom<ffi::StreamPermissions> for RustStreamPermissions {
    type Error = String;

    fn try_from(permissions: ffi::StreamPermissions) -> Result<Self, Self::Error> {
        let mut topics = BTreeMap::new();
        for entry in permissions.topics {
            let topic_id = entry.topic_id as usize;
            if topics
                .insert(topic_id, RustTopicPermissions::from(entry.permissions))
                .is_some()
            {
                return Err(format!("duplicate topic permission ID: {topic_id}"));
            }
        }
        let topics = (!topics.is_empty()).then_some(topics);

        Ok(RustStreamPermissions {
            manage_stream: permissions.manage_stream,
            read_stream: permissions.read_stream,
            manage_topics: permissions.manage_topics,
            read_topics: permissions.read_topics,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
            topics,
        })
    }
}

impl TryFrom<ffi::Permissions> for RustPermissions {
    type Error = String;

    fn try_from(permissions: ffi::Permissions) -> Result<Self, Self::Error> {
        let mut streams = BTreeMap::new();
        for entry in permissions.streams {
            let stream_id = entry.stream_id as usize;
            let stream_permissions = RustStreamPermissions::try_from(entry.permissions)?;
            if streams.insert(stream_id, stream_permissions).is_some() {
                return Err(format!("duplicate stream permission ID: {stream_id}"));
            }
        }
        let streams = (!streams.is_empty()).then_some(streams);

        Ok(RustPermissions {
            global: RustGlobalPermissions::from(permissions.global),
            streams,
        })
    }
}

impl From<RustPartition> for ffi::Partition {
    fn from(partition: RustPartition) -> Self {
        ffi::Partition {
            id: partition.id,
            created_at: partition.created_at.as_micros(),
            segments_count: partition.segments_count,
            current_offset: partition.current_offset,
            size_bytes: partition.size.as_bytes_u64(),
            messages_count: partition.messages_count,
        }
    }
}

impl From<RustTopic> for ffi::Topic {
    fn from(topic: RustTopic) -> Self {
        ffi::Topic {
            id: topic.id,
            created_at: topic.created_at.as_micros(),
            name: topic.name,
            size_bytes: topic.size.as_bytes_u64(),
            message_expiry: u64::from(topic.message_expiry),
            compression_algorithm: topic.compression_algorithm.to_string(),
            max_topic_size: u64::from(topic.max_topic_size),
            replication_factor: topic.replication_factor,
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
        }
    }
}

impl From<RustTopicDetails> for ffi::TopicDetails {
    fn from(topic: RustTopicDetails) -> Self {
        ffi::TopicDetails {
            id: topic.id,
            created_at: topic.created_at.as_micros(),
            name: topic.name,
            size_bytes: topic.size.as_bytes_u64(),
            message_expiry: u64::from(topic.message_expiry),
            compression_algorithm: topic.compression_algorithm.to_string(),
            max_topic_size: u64::from(topic.max_topic_size),
            replication_factor: topic.replication_factor,
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            partitions: topic
                .partitions
                .into_iter()
                .map(ffi::Partition::from)
                .collect(),
        }
    }
}

impl From<RustStream> for ffi::Stream {
    fn from(stream: RustStream) -> Self {
        ffi::Stream {
            id: stream.id,
            created_at: stream.created_at.as_micros(),
            name: stream.name,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
        }
    }
}

impl From<RustStreamDetails> for ffi::StreamDetails {
    fn from(stream: RustStreamDetails) -> Self {
        ffi::StreamDetails {
            id: stream.id,
            created_at: stream.created_at.as_micros(),
            name: stream.name,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            topics: stream.topics.into_iter().map(ffi::Topic::from).collect(),
        }
    }
}

impl From<RustConsumerGroupInfo> for ffi::ConsumerGroupInfo {
    fn from(group: RustConsumerGroupInfo) -> Self {
        ffi::ConsumerGroupInfo {
            stream_id: group.stream_id,
            topic_id: group.topic_id,
            group_id: group.group_id,
        }
    }
}

impl From<RustConsumerGroupMember> for ffi::ConsumerGroupMember {
    fn from(member: RustConsumerGroupMember) -> Self {
        ffi::ConsumerGroupMember {
            id: member.id,
            partitions_count: member.partitions_count,
            partitions: member.partitions,
        }
    }
}

impl From<RustConsumerGroup> for ffi::ConsumerGroup {
    fn from(group: RustConsumerGroup) -> Self {
        ffi::ConsumerGroup {
            id: group.id,
            name: group.name,
            partitions_count: group.partitions_count,
            members_count: group.members_count,
        }
    }
}

impl From<RustConsumerGroupDetails> for ffi::ConsumerGroupDetails {
    fn from(group: RustConsumerGroupDetails) -> Self {
        ffi::ConsumerGroupDetails {
            id: group.id,
            name: group.name,
            partitions_count: group.partitions_count,
            members_count: group.members_count,
            members: group
                .members
                .into_iter()
                .map(ffi::ConsumerGroupMember::from)
                .collect(),
        }
    }
}

impl From<RustIggyMessage> for ffi::IggyMessagePolled {
    fn from(message: RustIggyMessage) -> Self {
        let id_bytes = message.header.id.to_le_bytes();
        let id_lo = u64::from_le_bytes(id_bytes[0..8].try_into().unwrap());
        let id_hi = u64::from_le_bytes(id_bytes[8..16].try_into().unwrap());
        let user_headers = match message.user_headers {
            Some(raw_headers) => {
                // Keep polling forward-compatible with future header kinds. Unlike the Rust SDK's
                // typed decoder, this structural decoder preserves unknown kinds and values whose
                // lengths do not match their fixed-width kind.
                match WireUserHeaders::from_bytes(raw_headers) {
                    Ok(wire_headers) => wire_headers
                        .iter()
                        .map(|entry| ffi::HeaderEntry {
                            key: ffi::HeaderField {
                                kind: entry.key_kind.0,
                                value: entry.key.to_vec(),
                            },
                            value: ffi::HeaderField {
                                kind: entry.value_kind.0,
                                value: entry.value.to_vec(),
                            },
                        })
                        .collect(),
                    // A malformed header must not make its message or poll batch unreadable.
                    Err(_) => Vec::new(),
                }
            }
            None => Vec::new(),
        };

        ffi::IggyMessagePolled {
            checksum: message.header.checksum,
            id_lo,
            id_hi,
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            user_headers_length: message.header.user_headers_length,
            payload_length: message.header.payload_length,
            reserved: message.header.reserved,
            payload: message.payload.to_vec(),
            user_headers,
        }
    }
}

impl TryFrom<ffi::HeaderEntry> for RustHeaderEntry {
    type Error = String;

    fn try_from(entry: ffi::HeaderEntry) -> Result<Self, Self::Error> {
        Ok(RustHeaderEntry {
            key: decode_field(entry.key.kind, entry.key.value)?,
            value: decode_field(entry.value.kind, entry.value.value)?,
        })
    }
}

fn decode_field<T>(kind: u8, value: Vec<u8>) -> Result<RustHeaderField<T>, String> {
    let kind = RustHeaderKind::from_code(kind)
        .map_err(|error| format!("Could not convert header field: {error}"))?;

    match kind {
        RustHeaderKind::Raw => RustHeaderField::try_from(value)
            .map_err(|error| format!("Could not convert header field: {error}")),
        RustHeaderKind::String => {
            let value = String::from_utf8(value)
                .map_err(|_| "Could not convert header field: invalid UTF-8 string".to_string())?;
            RustHeaderField::try_from(value)
                .map_err(|error| format!("Could not convert header field: {error}"))
        }
        RustHeaderKind::Bool => match value.as_slice() {
            [0] => Ok(RustHeaderField::from(false)),
            [1] => Ok(RustHeaderField::from(true)),
            _ => {
                Err("Could not convert header field: bool values must encode as 0 or 1".to_string())
            }
        },
        RustHeaderKind::Int8 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(i8::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: int8 values require exactly 1 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Int16 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(i16::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: int16 values require exactly 2 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Int32 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(i32::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: int32 values require exactly 4 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Int64 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(i64::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: int64 values require exactly 8 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Int128 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(i128::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: int128 values require exactly 16 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Uint8 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(u8::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: uint8 values require exactly 1 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Uint16 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(u16::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: uint16 values require exactly 2 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Uint32 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(u32::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: uint32 values require exactly 4 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Uint64 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(u64::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: uint64 values require exactly 8 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Uint128 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(u128::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: uint128 values require exactly 16 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Float32 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(f32::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: float32 values require exactly 4 bytes, got {}",
                value.len()
            )),
        },
        RustHeaderKind::Float64 => match value.try_into() {
            Ok(bytes) => Ok(RustHeaderField::from(f64::from_le_bytes(bytes))),
            Err(value) => Err(format!(
                "Could not convert header field: float64 values require exactly 8 bytes, got {}",
                value.len()
            )),
        },
    }
}

impl TryFrom<ffi::IggyMessageToSend> for RustIggyMessage {
    type Error = String;

    fn try_from(message: ffi::IggyMessageToSend) -> Result<Self, Self::Error> {
        let mut user_headers = BTreeMap::new();
        for entry in message.user_headers {
            let header_entry = RustHeaderEntry::try_from(entry)
                .map_err(|error| format!("Could not convert message user headers: {error}"))?;
            if user_headers
                .insert(header_entry.key, header_entry.value)
                .is_some()
            {
                return Err(
                    "Could not convert message user headers: duplicate header key".to_string(),
                );
            }
        }
        let id = ((message.id_hi as u128) << 64) | (message.id_lo as u128);
        let payload = Bytes::from(message.payload);
        if user_headers.is_empty() {
            RustIggyMessage::builder()
                .id(id)
                .payload(payload)
                .build()
                .map_err(|error| format!("Could not convert message: {error}"))
        } else {
            RustIggyMessage::builder()
                .id(id)
                .payload(payload)
                .user_headers(user_headers)
                .build()
                .map_err(|error| format!("Could not convert message: {error}"))
        }
    }
}

impl From<RustPolledMessages> for ffi::PolledMessages {
    fn from(messages: RustPolledMessages) -> Self {
        ffi::PolledMessages {
            partition_id: messages.partition_id,
            current_offset: messages.current_offset,
            count: messages.count,
            messages: messages
                .messages
                .into_iter()
                .map(ffi::IggyMessagePolled::from)
                .collect(),
        }
    }
}
