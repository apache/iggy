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

use crate::metadata::{ConsumerGroupMeta, StreamMeta, TopicMeta, UserMeta};
use crate::shard::transmission::frame::{
    ConsumerGroupResponseData, StreamResponseData, TopicResponseData,
};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::users::user::User;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_common::{
    BytesSerializable, ConsumerOffsetInfo, PersonalAccessToken, Stats, TransportProtocol, UserId,
};

pub fn map_stats(stats: &Stats) -> Bytes {
    let mut bytes = BytesMut::with_capacity(104);
    bytes.put_u32_le(stats.process_id);
    bytes.put_f32_le(stats.cpu_usage);
    bytes.put_f32_le(stats.total_cpu_usage);
    bytes.put_u64_le(stats.memory_usage.as_bytes_u64());
    bytes.put_u64_le(stats.total_memory.as_bytes_u64());
    bytes.put_u64_le(stats.available_memory.as_bytes_u64());
    bytes.put_u64_le(stats.run_time.into());
    bytes.put_u64_le(stats.start_time.into());
    bytes.put_u64_le(stats.read_bytes.as_bytes_u64());
    bytes.put_u64_le(stats.written_bytes.as_bytes_u64());
    bytes.put_u64_le(stats.messages_size_bytes.as_bytes_u64());
    bytes.put_u32_le(stats.streams_count);
    bytes.put_u32_le(stats.topics_count);
    bytes.put_u32_le(stats.partitions_count);
    bytes.put_u32_le(stats.segments_count);
    bytes.put_u64_le(stats.messages_count);
    bytes.put_u32_le(stats.clients_count);
    bytes.put_u32_le(stats.consumer_groups_count);
    bytes.put_u32_le(stats.hostname.len() as u32);
    bytes.put_slice(stats.hostname.as_bytes());
    bytes.put_u32_le(stats.os_name.len() as u32);
    bytes.put_slice(stats.os_name.as_bytes());
    bytes.put_u32_le(stats.os_version.len() as u32);
    bytes.put_slice(stats.os_version.as_bytes());
    bytes.put_u32_le(stats.kernel_version.len() as u32);
    bytes.put_slice(stats.kernel_version.as_bytes());
    bytes.put_u32_le(stats.iggy_server_version.len() as u32);
    bytes.put_slice(stats.iggy_server_version.as_bytes());
    if let Some(semver) = stats.iggy_server_semver {
        bytes.put_u32_le(semver);
    }

    bytes.put_u32_le(stats.cache_metrics.len() as u32);
    for (key, metrics) in &stats.cache_metrics {
        bytes.put_u32_le(key.stream_id);
        bytes.put_u32_le(key.topic_id);
        bytes.put_u32_le(key.partition_id);

        bytes.put_u64_le(metrics.hits);
        bytes.put_u64_le(metrics.misses);
        bytes.put_f32_le(metrics.hit_ratio);
    }

    bytes.freeze()
}

pub fn map_consumer_offset(offset: &ConsumerOffsetInfo) -> Bytes {
    let mut bytes = BytesMut::with_capacity(20);
    bytes.put_u32_le(offset.partition_id);
    bytes.put_u64_le(offset.current_offset);
    bytes.put_u64_le(offset.stored_offset);
    bytes.freeze()
}

pub fn map_client(client: &Client) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_client(client, &mut bytes);
    for consumer_group in &client.consumer_groups {
        bytes.put_u32_le(consumer_group.stream_id);
        bytes.put_u32_le(consumer_group.topic_id);
        bytes.put_u32_le(consumer_group.group_id);
    }
    bytes.freeze()
}

pub async fn map_clients(clients: Vec<Client>) -> Bytes {
    let mut bytes = BytesMut::new();
    for client in clients.iter() {
        extend_client(client, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_user(user: &User) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_user(user, &mut bytes);
    if let Some(permissions) = &user.permissions {
        bytes.put_u8(1);
        let permissions = permissions.to_bytes();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u32_le(permissions.len() as u32);
        bytes.put_slice(&permissions);
    } else {
        bytes.put_u32_le(0);
    }
    bytes.freeze()
}

pub fn map_users(users: Vec<User>) -> Bytes {
    let mut bytes = BytesMut::new();
    for user in users.iter() {
        extend_user(user, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_identity_info(user_id: UserId) -> Bytes {
    let mut bytes = BytesMut::with_capacity(4);
    bytes.put_u32_le(user_id);
    bytes.freeze()
}

pub fn map_raw_pat(token: &str) -> Bytes {
    let mut bytes = BytesMut::with_capacity(1 + token.len());
    bytes.put_u8(token.len() as u8);
    bytes.put_slice(token.as_bytes());
    bytes.freeze()
}

pub fn map_personal_access_tokens(personal_access_tokens: Vec<PersonalAccessToken>) -> Bytes {
    let mut bytes = BytesMut::new();
    for personal_access_token in personal_access_tokens.iter() {
        extend_pat(personal_access_token, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_stream_from_response(data: &StreamResponseData) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u32_le(data.id);
    bytes.put_u64_le(data.created_at.into());
    bytes.put_u32_le(0); // topics_count = 0 for new stream
    bytes.put_u64_le(0); // total_size = 0
    bytes.put_u64_le(0); // total_messages = 0
    bytes.put_u8(data.name.len() as u8);
    bytes.put_slice(data.name.as_bytes());
    bytes.freeze()
}

pub fn map_topic_from_response(data: &TopicResponseData) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u32_le(data.id);
    bytes.put_u64_le(data.created_at.into());
    bytes.put_u32_le(data.partitions.len() as u32);
    bytes.put_u64_le(data.message_expiry.into());
    bytes.put_u8(data.compression_algorithm.as_code());
    bytes.put_u64_le(data.max_topic_size.into());
    bytes.put_u8(data.replication_factor);
    bytes.put_u64_le(0); // topic_size = 0
    bytes.put_u64_le(0); // topic_messages = 0
    bytes.put_u8(data.name.len() as u8);
    bytes.put_slice(data.name.as_bytes());

    for partition in &data.partitions {
        bytes.put_u32_le(partition.id as u32);
        bytes.put_u64_le(partition.created_at.into());
        bytes.put_u32_le(0); // segments_count = 0 for new partition
        bytes.put_u64_le(0); // current_offset = 0
        bytes.put_u64_le(0); // size_bytes = 0
        bytes.put_u64_le(0); // messages_count = 0
    }

    bytes.freeze()
}

pub fn map_consumer_group_from_response(data: &ConsumerGroupResponseData) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u32_le(data.id);
    bytes.put_u32_le(data.partitions_count);
    bytes.put_u32_le(0); // members_count = 0 for new group
    bytes.put_u8(data.name.len() as u8);
    bytes.put_slice(data.name.as_bytes());
    bytes.freeze()
}

/// Map consumer group from SharedMetadata format.
pub fn map_consumer_group_from_meta(meta: &ConsumerGroupMeta) -> Bytes {
    let mut bytes = BytesMut::new();

    // Header: id, partitions_count, members_count, name_len, name
    bytes.put_u32_le(meta.id as u32);
    bytes.put_u32_le(meta.partitions.len() as u32);
    bytes.put_u32_le(meta.members.len() as u32);
    bytes.put_u8(meta.name.len() as u8);
    bytes.put_slice(meta.name.as_bytes());

    // Members
    for (_, member) in meta.members.iter() {
        bytes.put_u32_le(member.id as u32);
        bytes.put_u32_le(member.partitions.len() as u32);
        for &partition_id in &member.partitions {
            bytes.put_u32_le(partition_id as u32);
        }
    }

    bytes.freeze()
}

fn extend_client(client: &Client, bytes: &mut BytesMut) {
    bytes.put_u32_le(client.session.client_id);
    bytes.put_u32_le(client.user_id.unwrap_or(u32::MAX));
    let transport: u8 = match client.transport {
        TransportProtocol::Tcp => 1,
        TransportProtocol::Quic => 2,
        TransportProtocol::Http => 3,
        TransportProtocol::WebSocket => 4,
    };
    bytes.put_u8(transport);
    let address = client.session.ip_address.to_string();
    bytes.put_u32_le(address.len() as u32);
    bytes.put_slice(address.as_bytes());
    bytes.put_u32_le(client.consumer_groups.len() as u32);
}

fn extend_user(user: &User, bytes: &mut BytesMut) {
    bytes.put_u32_le(user.id);
    bytes.put_u64_le(user.created_at.into());
    bytes.put_u8(user.status.as_code());
    bytes.put_u8(user.username.len() as u8);
    bytes.put_slice(user.username.as_bytes());
}

fn extend_pat(personal_access_token: &PersonalAccessToken, bytes: &mut BytesMut) {
    bytes.put_u8(personal_access_token.name.len() as u8);
    bytes.put_slice(personal_access_token.name.as_bytes());
    match &personal_access_token.expiry_at {
        Some(expiry_at) => {
            bytes.put_u64_le(expiry_at.as_micros());
        }
        None => {
            bytes.put_u64_le(0);
        }
    }
}

fn compute_stream_stats(stream: &StreamMeta) -> (u64, u64) {
    let mut size = 0u64;
    let mut messages = 0u64;
    for (_, topic) in stream.topics.iter() {
        for partition in topic.partitions.iter() {
            size += partition.stats.size_bytes_inconsistent();
            messages += partition.stats.messages_count_inconsistent();
        }
    }
    (size, messages)
}

fn compute_topic_stats(topic: &TopicMeta) -> (u64, u64) {
    let mut size = 0u64;
    let mut messages = 0u64;
    for partition in topic.partitions.iter() {
        size += partition.stats.size_bytes_inconsistent();
        messages += partition.stats.messages_count_inconsistent();
    }
    (size, messages)
}

fn extend_topic_header(topic: &TopicMeta, bytes: &mut BytesMut) {
    let (size, messages) = compute_topic_stats(topic);
    bytes.put_u32_le(topic.id as u32);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.partitions.len() as u32);
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    bytes.put_u64_le(size);
    bytes.put_u64_le(messages);
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());
}

fn extend_topic_with_partitions(topic: &TopicMeta, bytes: &mut BytesMut) {
    extend_topic_header(topic, bytes);

    for (partition_id, partition) in topic.partitions.iter().enumerate() {
        let created_at = partition.created_at;
        let segments_count = partition.stats.segments_count_inconsistent();
        let offset = partition.stats.current_offset();
        let size_bytes = partition.stats.size_bytes_inconsistent();
        let messages_count = partition.stats.messages_count_inconsistent();

        bytes.put_u32_le(partition_id as u32);
        bytes.put_u64_le(created_at.into());
        bytes.put_u32_le(segments_count);
        bytes.put_u64_le(offset);
        bytes.put_u64_le(size_bytes);
        bytes.put_u64_le(messages_count);
    }
}

/// Map a single stream metadata to wire format (includes nested topics).
pub fn map_stream(stream: &StreamMeta) -> Bytes {
    let mut bytes = BytesMut::new();

    let mut topic_ids: Vec<_> = stream.topics.iter().map(|(k, _)| k).collect();
    topic_ids.sort_unstable();

    let (total_size, total_messages) = compute_stream_stats(stream);

    bytes.put_u32_le(stream.id as u32);
    bytes.put_u64_le(stream.created_at.into());
    bytes.put_u32_le(topic_ids.len() as u32);
    bytes.put_u64_le(total_size);
    bytes.put_u64_le(total_messages);
    bytes.put_u8(stream.name.len() as u8);
    bytes.put_slice(stream.name.as_bytes());

    for &topic_id in &topic_ids {
        if let Some(topic) = stream.topics.get(topic_id) {
            extend_topic_header(topic, &mut bytes);
        }
    }

    bytes.freeze()
}

/// Map multiple streams to wire format (header only, no nested topics).
pub fn map_streams(streams: &[StreamMeta]) -> Bytes {
    let mut bytes = BytesMut::new();

    let mut sorted: Vec<_> = streams.iter().collect();
    sorted.sort_by_key(|s| s.id);

    for stream in sorted {
        let (total_size, total_messages) = compute_stream_stats(stream);
        let topics_count = stream.topics.len();

        bytes.put_u32_le(stream.id as u32);
        bytes.put_u64_le(stream.created_at.into());
        bytes.put_u32_le(topics_count as u32);
        bytes.put_u64_le(total_size);
        bytes.put_u64_le(total_messages);
        bytes.put_u8(stream.name.len() as u8);
        bytes.put_slice(stream.name.as_bytes());
    }

    bytes.freeze()
}

/// Map a single topic metadata to wire format (includes partitions).
pub fn map_topic(topic: &TopicMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_topic_with_partitions(topic, &mut bytes);
    bytes.freeze()
}

/// Map multiple topics to wire format (header only, no partitions).
pub fn map_topics(topics: &[TopicMeta]) -> Bytes {
    let mut bytes = BytesMut::new();

    let mut sorted: Vec<_> = topics.iter().collect();
    sorted.sort_by_key(|t| t.id);

    for topic in sorted {
        extend_topic_header(topic, &mut bytes);
    }

    bytes.freeze()
}

/// Map multiple consumer groups to wire format.
pub fn map_consumer_groups(groups: &[ConsumerGroupMeta]) -> Bytes {
    let mut bytes = BytesMut::new();
    for cg in groups {
        bytes.put_u32_le(cg.id as u32);
        bytes.put_u32_le(cg.partitions.len() as u32);
        bytes.put_u32_le(cg.members.len() as u32);
        bytes.put_u8(cg.name.len() as u8);
        bytes.put_slice(cg.name.as_bytes());
    }
    bytes.freeze()
}

fn extend_user_meta(user: &UserMeta, bytes: &mut BytesMut) {
    bytes.put_u32_le(user.id);
    bytes.put_u64_le(user.created_at.into());
    bytes.put_u8(user.status.as_code());
    bytes.put_u8(user.username.len() as u8);
    bytes.put_slice(user.username.as_bytes());
}

/// Map a single user metadata to wire format.
pub fn map_user_meta(user: &UserMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_user_meta(user, &mut bytes);
    if let Some(permissions) = &user.permissions {
        bytes.put_u8(1);
        let permissions = permissions.to_bytes();
        #[allow(clippy::cast_possible_truncation)]
        bytes.put_u32_le(permissions.len() as u32);
        bytes.put_slice(&permissions);
    } else {
        bytes.put_u32_le(0);
    }
    bytes.freeze()
}

/// Map multiple user metadata to wire format.
pub fn map_users_meta(users: &[UserMeta]) -> Bytes {
    let mut bytes = BytesMut::new();
    for user in users {
        extend_user_meta(user, &mut bytes);
    }
    bytes.freeze()
}
