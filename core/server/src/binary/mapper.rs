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

use std::sync::Arc;

use crate::metadata::{ConsumerGroupMeta, PartitionMeta, StreamMeta, TopicMeta, UserMeta};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use crate::streaming::users::user::User;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_common::{BytesSerializable, ConsumerOffsetInfo, Stats, TransportProtocol, UserId};

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

/// Maps a user from SharedMetadata.
pub fn map_user_from_metadata(user: &UserMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_user_from_metadata(user, &mut bytes);
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

fn extend_user_from_metadata(user: &UserMeta, bytes: &mut BytesMut) {
    bytes.put_u32_le(user.id);
    bytes.put_u64_le(user.created_at.into());
    bytes.put_u8(user.status.as_code());
    bytes.put_u8(user.username.len() as u8);
    bytes.put_slice(user.username.as_bytes());
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

/// Maps streams from SharedMetadata. Stats are zeros since they're not stored in metadata.
pub fn map_streams_from_metadata(streams: &[StreamMeta]) -> Bytes {
    let mut bytes = BytesMut::new();
    for stream in streams {
        extend_stream_from_metadata(stream, &mut bytes);
    }
    bytes.freeze()
}

fn extend_stream_from_metadata(stream: &StreamMeta, bytes: &mut BytesMut) {
    bytes.put_u32_le(stream.id as u32);
    bytes.put_u64_le(stream.created_at.into());
    bytes.put_u32_le(stream.topics_count() as u32);
    // Stats are zero since we don't store them in SharedMetadata
    bytes.put_u64_le(0); // size_bytes
    bytes.put_u64_le(0); // messages_count
    bytes.put_u8(stream.name.len() as u8);
    bytes.put_slice(stream.name.as_bytes());
}

/// Maps a single stream with topics from SharedMetadata. Stats are zeros.
pub fn map_stream_from_metadata(stream: &StreamMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_stream_from_metadata(stream, &mut bytes);
    // Include topics
    for topic in stream.topics.values() {
        extend_topic_from_metadata(topic, &mut bytes);
    }
    bytes.freeze()
}

/// Maps a single stream with topics from SharedMetadata with stats from SharedStatsStore.
pub fn map_stream_from_metadata_with_stats<F>(
    stream: &StreamMeta,
    stream_stats: Option<&StreamStats>,
    mut get_topic_stats: F,
) -> Bytes
where
    F: FnMut(usize) -> Option<Arc<TopicStats>>,
{
    let mut bytes = BytesMut::new();

    // Extend stream header with stats
    bytes.put_u32_le(stream.id as u32);
    bytes.put_u64_le(stream.created_at.into());
    bytes.put_u32_le(stream.topics_count() as u32);
    if let Some(stats) = stream_stats {
        bytes.put_u64_le(stats.size_bytes_inconsistent());
        bytes.put_u64_le(stats.messages_count_inconsistent());
    } else {
        bytes.put_u64_le(0);
        bytes.put_u64_le(0);
    }
    bytes.put_u8(stream.name.len() as u8);
    bytes.put_slice(stream.name.as_bytes());

    // Include topics with stats
    for topic in stream.topics.values() {
        let topic_stats = get_topic_stats(topic.id);
        extend_topic_from_metadata_with_stats(topic, topic_stats.as_deref(), &mut bytes);
    }
    bytes.freeze()
}

fn extend_topic_from_metadata_with_stats(
    topic: &TopicMeta,
    stats: Option<&TopicStats>,
    bytes: &mut BytesMut,
) {
    bytes.put_u32_le(topic.id as u32);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.partitions_count());
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    if let Some(s) = stats {
        bytes.put_u64_le(s.size_bytes_inconsistent());
        bytes.put_u64_le(s.messages_count_inconsistent());
    } else {
        bytes.put_u64_le(0);
        bytes.put_u64_le(0);
    }
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());
}

fn extend_topic_from_metadata(topic: &TopicMeta, bytes: &mut BytesMut) {
    bytes.put_u32_le(topic.id as u32);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.partitions_count());
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    // Stats are zero since we don't store them in SharedMetadata
    bytes.put_u64_le(0); // size_bytes
    bytes.put_u64_le(0); // messages_count
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());
}

pub fn map_topic_from_metadata(topic: &TopicMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_topic_from_metadata(topic, &mut bytes);
    // Include partitions (all with zero stats)
    for partition in topic.partitions.values() {
        extend_partition_from_metadata(partition, &mut bytes);
    }
    bytes.freeze()
}

/// Maps a topic from metadata with stats from SharedStatsStore.
pub fn map_topic_from_metadata_with_stats<F>(
    topic: &TopicMeta,
    topic_stats: Option<&TopicStats>,
    mut get_partition_stats: F,
) -> Bytes
where
    F: FnMut(usize) -> Option<Arc<PartitionStats>>,
{
    let mut bytes = BytesMut::new();

    // Extend topic header with stats
    bytes.put_u32_le(topic.id as u32);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.partitions_count());
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    if let Some(stats) = topic_stats {
        bytes.put_u64_le(stats.size_bytes_inconsistent());
        bytes.put_u64_le(stats.messages_count_inconsistent());
    } else {
        bytes.put_u64_le(0);
        bytes.put_u64_le(0);
    }
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());

    // Include partitions with stats
    for partition in topic.partitions.values() {
        let partition_stats = get_partition_stats(partition.id);
        extend_partition_from_metadata_with_stats(
            partition,
            partition_stats.as_deref(),
            &mut bytes,
        );
    }
    bytes.freeze()
}

fn extend_partition_from_metadata(partition: &PartitionMeta, bytes: &mut BytesMut) {
    bytes.put_u32_le(partition.id as u32);
    bytes.put_u64_le(partition.created_at.into());
    bytes.put_u32_le(0); // segments_count (not stored in metadata)
    bytes.put_u64_le(partition.offset.load(std::sync::atomic::Ordering::Relaxed)); // current_offset
    // Stats are zero since we don't store them in SharedMetadata
    bytes.put_u64_le(0); // size_bytes
    bytes.put_u64_le(0); // messages_count
}

fn extend_partition_from_metadata_with_stats(
    partition: &PartitionMeta,
    stats: Option<&PartitionStats>,
    bytes: &mut BytesMut,
) {
    bytes.put_u32_le(partition.id as u32);
    bytes.put_u64_le(partition.created_at.into());
    if let Some(s) = stats {
        bytes.put_u32_le(s.segments_count_inconsistent());
        bytes.put_u64_le(partition.offset.load(std::sync::atomic::Ordering::Relaxed));
        bytes.put_u64_le(s.size_bytes_inconsistent());
        bytes.put_u64_le(s.messages_count_inconsistent());
    } else {
        bytes.put_u32_le(0);
        bytes.put_u64_le(partition.offset.load(std::sync::atomic::Ordering::Relaxed));
        bytes.put_u64_le(0);
        bytes.put_u64_le(0);
    }
}

/// Map a single consumer group from SharedMetadata to binary format.
pub fn map_consumer_group_meta(cg: &ConsumerGroupMeta) -> Bytes {
    let mut bytes = BytesMut::new();
    bytes.put_u32_le(cg.id as u32);
    bytes.put_u32_le(cg.partitions.len() as u32);
    bytes.put_u32_le(cg.members.len() as u32);
    bytes.put_u8(cg.name.len() as u8);
    bytes.put_slice(cg.name.as_bytes());

    for (_, member) in cg.members.iter() {
        bytes.put_u32_le(member.client_id);
        bytes.put_u32_le(member.partitions.len() as u32);
        for partition in &member.partitions {
            bytes.put_u32_le(*partition as u32);
        }
    }
    bytes.freeze()
}

/// Map multiple consumer groups from SharedMetadata to binary format.
pub fn map_consumer_groups_meta(groups: &[ConsumerGroupMeta]) -> Bytes {
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

/// Map topics from SharedMetadata to binary format.
pub fn map_topics_meta(topics: &[(&TopicMeta, &TopicStats)]) -> Bytes {
    let mut bytes = BytesMut::new();
    for (topic, stats) in topics {
        extend_topic_meta(topic, stats, &mut bytes);
    }
    bytes.freeze()
}

/// Map a single topic from SharedMetadata to binary format (with partitions).
pub fn map_topic_meta(
    topic: &TopicMeta,
    stats: &TopicStats,
    partition_stats: &[(&PartitionMeta, &PartitionStats)],
) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_topic_meta(topic, stats, &mut bytes);

    for (partition, partition_stat) in partition_stats {
        extend_partition_meta(partition, partition_stat, &mut bytes);
    }
    bytes.freeze()
}

fn extend_topic_meta(topic: &TopicMeta, stats: &TopicStats, bytes: &mut BytesMut) {
    bytes.put_u32_le(topic.id as u32);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.partitions.len() as u32);
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    bytes.put_u64_le(stats.size_bytes_inconsistent());
    bytes.put_u64_le(stats.messages_count_inconsistent());
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());
}

fn extend_partition_meta(partition: &PartitionMeta, stats: &PartitionStats, bytes: &mut BytesMut) {
    bytes.put_u32_le(partition.id as u32);
    bytes.put_u64_le(partition.created_at.into());
    bytes.put_u32_le(stats.segments_count_inconsistent());
    bytes.put_u64_le(stats.current_offset());
    bytes.put_u64_le(stats.size_bytes_inconsistent());
    bytes.put_u64_le(stats.messages_count_inconsistent());
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
