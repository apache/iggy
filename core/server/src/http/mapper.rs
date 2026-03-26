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

use crate::http::jwt::json_web_token::GeneratedToken;
use crate::metadata::{ConsumerGroupMeta, InnerMetadata, PartitionMeta, StreamMeta, TopicMeta};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::users::user::User;
use iggy_common::PersonalAccessToken;
use iggy_common::{ConsumerGroupDetails, ConsumerGroupInfo, ConsumerGroupMember, IggyByteSize};
use iggy_common::{IdentityInfo, PersonalAccessTokenInfo, TokenInfo, TopicDetails};
use iggy_common::{UserInfo, UserInfoDetails};

pub fn map_user(user: &User) -> UserInfoDetails {
    UserInfoDetails {
        id: user.id,
        username: user.username.clone(),
        created_at: user.created_at,
        status: user.status,
        permissions: user.permissions.clone(),
    }
}

pub fn map_users(users: &[&User]) -> Vec<UserInfo> {
    let mut users_data = Vec::with_capacity(users.len());
    for user in users {
        let user = UserInfo {
            id: user.id,
            username: user.username.clone(),
            created_at: user.created_at,
            status: user.status,
        };
        users_data.push(user);
    }
    users_data.sort_by(|a, b| a.id.cmp(&b.id));
    users_data
}

pub fn map_personal_access_tokens(
    personal_access_tokens: &[PersonalAccessToken],
) -> Vec<PersonalAccessTokenInfo> {
    let mut personal_access_tokens_data = Vec::with_capacity(personal_access_tokens.len());
    for personal_access_token in personal_access_tokens {
        let personal_access_token = PersonalAccessTokenInfo {
            name: (*personal_access_token.name).to_owned(),
            expiry_at: personal_access_token.expiry_at,
        };
        personal_access_tokens_data.push(personal_access_token);
    }
    personal_access_tokens_data.sort_by(|a, b| a.name.cmp(&b.name));
    personal_access_tokens_data
}

pub fn map_client(client: &Client) -> iggy_common::ClientInfoDetails {
    iggy_common::ClientInfoDetails {
        client_id: client.session.client_id,
        user_id: client.user_id,
        transport: client.transport.to_string(),
        address: client.session.ip_address.to_string(),
        consumer_groups_count: client.consumer_groups.len() as u32,
        consumer_groups: client
            .consumer_groups
            .iter()
            .map(|consumer_group| ConsumerGroupInfo {
                stream_id: consumer_group.stream_id,
                topic_id: consumer_group.topic_id,
                group_id: consumer_group.group_id,
            })
            .collect(),
    }
}

pub fn map_clients(clients: &[Client]) -> Vec<iggy_common::ClientInfo> {
    let mut all_clients = Vec::new();
    for client in clients {
        let client = iggy_common::ClientInfo {
            client_id: client.session.client_id,
            user_id: client.user_id,
            transport: client.transport.to_string(),
            address: client.session.ip_address.to_string(),
            consumer_groups_count: client.consumer_groups.len() as u32,
        };
        all_clients.push(client);
    }

    all_clients.sort_by(|a, b| a.client_id.cmp(&b.client_id));
    all_clients
}

pub fn map_generated_access_token_to_identity_info(token: GeneratedToken) -> IdentityInfo {
    IdentityInfo {
        user_id: token.user_id,
        access_token: Some(TokenInfo {
            token: token.access_token,
            expiry: token.access_token_expiry,
        }),
    }
}

/// Map a stream from SharedMetadata to StreamDetails (with topics)
pub fn map_stream_details_from_metadata(stream_meta: &StreamMeta) -> iggy_common::StreamDetails {
    // Get topic IDs sorted
    let mut topic_ids: Vec<_> = stream_meta.topics.iter().map(|(k, _)| k).collect();
    topic_ids.sort_unstable();

    // Map topics
    let mut topics = Vec::with_capacity(topic_ids.len());
    for topic_id in topic_ids {
        if let Some(topic_meta) = stream_meta.topics.get(topic_id) {
            topics.push(map_topic_from_metadata(topic_meta));
        }
    }

    // Aggregate stats
    let (total_size, total_messages) = aggregate_stream_stats(stream_meta);

    iggy_common::StreamDetails {
        id: stream_meta.id as u32,
        created_at: stream_meta.created_at,
        name: stream_meta.name.to_string(),
        topics_count: topics.len() as u32,
        size: IggyByteSize::from(total_size),
        messages_count: total_messages,
        topics,
    }
}

/// Map a stream from SharedMetadata to Stream (without topics)
pub fn map_stream_from_metadata(stream_meta: &StreamMeta) -> iggy_common::Stream {
    let (total_size, total_messages) = aggregate_stream_stats(stream_meta);

    iggy_common::Stream {
        id: stream_meta.id as u32,
        created_at: stream_meta.created_at,
        name: stream_meta.name.to_string(),
        topics_count: stream_meta.topics.len() as u32,
        size: IggyByteSize::from(total_size),
        messages_count: total_messages,
    }
}

/// Map all streams from SharedMetadata
pub fn map_streams_from_metadata(metadata: &InnerMetadata) -> Vec<iggy_common::Stream> {
    let mut stream_ids: Vec<_> = metadata.streams.iter().map(|(k, _)| k).collect();
    stream_ids.sort_unstable();

    let mut streams = Vec::with_capacity(stream_ids.len());
    for stream_id in stream_ids {
        if let Some(stream_meta) = metadata.streams.get(stream_id) {
            streams.push(map_stream_from_metadata(stream_meta));
        }
    }
    streams
}

/// Map a topic from SharedMetadata to Topic (without partitions)
pub fn map_topic_from_metadata(topic_meta: &TopicMeta) -> iggy_common::Topic {
    let (total_size, total_messages) = aggregate_topic_stats(topic_meta);

    iggy_common::Topic {
        id: topic_meta.id as u32,
        created_at: topic_meta.created_at,
        name: topic_meta.name.to_string(),
        size: IggyByteSize::from(total_size),
        partitions_count: topic_meta.partitions.len() as u32,
        messages_count: total_messages,
        message_expiry: topic_meta.message_expiry,
        compression_algorithm: topic_meta.compression_algorithm,
        max_topic_size: topic_meta.max_topic_size,
        replication_factor: topic_meta.replication_factor,
    }
}

/// Map all topics for a stream from SharedMetadata
pub fn map_topics_from_metadata(stream_meta: &StreamMeta) -> Vec<iggy_common::Topic> {
    let mut topic_ids: Vec<_> = stream_meta.topics.iter().map(|(k, _)| k).collect();
    topic_ids.sort_unstable();

    let mut topics = Vec::with_capacity(topic_ids.len());
    for topic_id in topic_ids {
        if let Some(topic_meta) = stream_meta.topics.get(topic_id) {
            topics.push(map_topic_from_metadata(topic_meta));
        }
    }
    topics
}

/// Map a topic from SharedMetadata to TopicDetails (with partitions)
pub fn map_topic_details_from_metadata(topic_meta: &TopicMeta) -> TopicDetails {
    // Get partition IDs sorted
    let mut partition_ids: Vec<_> = topic_meta
        .partitions
        .iter()
        .enumerate()
        .map(|(k, _)| k)
        .collect();
    partition_ids.sort_unstable();

    // Map partitions
    let mut partitions = Vec::with_capacity(partition_ids.len());
    for partition_id in partition_ids {
        if let Some(partition_meta) = topic_meta.partitions.get(partition_id) {
            partitions.push(map_partition_from_metadata(partition_meta));
        }
    }

    // Aggregate stats
    let (total_size, total_messages) = aggregate_topic_stats(topic_meta);

    TopicDetails {
        id: topic_meta.id as u32,
        created_at: topic_meta.created_at,
        name: topic_meta.name.to_string(),
        size: IggyByteSize::from(total_size),
        messages_count: total_messages,
        partitions_count: partitions.len() as u32,
        partitions,
        message_expiry: topic_meta.message_expiry,
        compression_algorithm: topic_meta.compression_algorithm,
        max_topic_size: topic_meta.max_topic_size,
        replication_factor: topic_meta.replication_factor,
    }
}

/// Map a partition from SharedMetadata
pub fn map_partition_from_metadata(partition_meta: &PartitionMeta) -> iggy_common::Partition {
    let stats = &partition_meta.stats;
    let segments_count = stats.segments_count_inconsistent();
    let size_bytes = stats.size_bytes_inconsistent();
    let messages_count = stats.messages_count_inconsistent();
    let current_offset = stats.current_offset();

    iggy_common::Partition {
        id: partition_meta.id as u32,
        created_at: partition_meta.created_at,
        segments_count,
        current_offset,
        size: IggyByteSize::from(size_bytes),
        messages_count,
    }
}

/// Map a consumer group from SharedMetadata
pub fn map_consumer_group_from_metadata(cg_meta: &ConsumerGroupMeta) -> iggy_common::ConsumerGroup {
    iggy_common::ConsumerGroup {
        id: cg_meta.id as u32,
        name: cg_meta.name.to_string(),
        partitions_count: cg_meta.partitions.len() as u32,
        members_count: cg_meta.members.len() as u32,
    }
}

/// Map a consumer group to ConsumerGroupDetails from SharedMetadata
pub fn map_consumer_group_details_from_metadata(
    cg_meta: &ConsumerGroupMeta,
) -> ConsumerGroupDetails {
    let members: Vec<ConsumerGroupMember> = cg_meta
        .members
        .iter()
        .map(|(_, member)| ConsumerGroupMember {
            id: member.id as u32,
            partitions_count: member.partitions.len() as u32,
            partitions: member.partitions.iter().map(|&p| p as u32).collect(),
        })
        .collect();

    ConsumerGroupDetails {
        id: cg_meta.id as u32,
        name: cg_meta.name.to_string(),
        partitions_count: cg_meta.partitions.len() as u32,
        members_count: members.len() as u32,
        members,
    }
}

/// Map all consumer groups for a topic from SharedMetadata
pub fn map_consumer_groups_from_metadata(
    topic_meta: &TopicMeta,
) -> Vec<iggy_common::ConsumerGroup> {
    let mut group_ids: Vec<_> = topic_meta.consumer_groups.iter().map(|(k, _)| k).collect();
    group_ids.sort_unstable();

    let mut groups = Vec::with_capacity(group_ids.len());
    for group_id in group_ids {
        if let Some(cg_meta) = topic_meta.consumer_groups.get(group_id) {
            groups.push(map_consumer_group_from_metadata(cg_meta));
        }
    }
    groups
}

fn aggregate_stream_stats(stream_meta: &StreamMeta) -> (u64, u64) {
    let mut total_size = 0u64;
    let mut total_messages = 0u64;

    for (_, topic_meta) in stream_meta.topics.iter() {
        for partition_meta in topic_meta.partitions.iter() {
            total_size += partition_meta.stats.size_bytes_inconsistent();
            total_messages += partition_meta.stats.messages_count_inconsistent();
        }
    }

    (total_size, total_messages)
}

fn aggregate_topic_stats(topic_meta: &TopicMeta) -> (u64, u64) {
    let mut total_size = 0u64;
    let mut total_messages = 0u64;

    for partition_meta in topic_meta.partitions.iter() {
        total_size += partition_meta.stats.size_bytes_inconsistent();
        total_messages += partition_meta.stats.messages_count_inconsistent();
    }

    (total_size, total_messages)
}
