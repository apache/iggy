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
use crate::metadata::{ConsumerGroupMeta, PartitionMeta, StreamMeta, TopicMeta, UserMeta};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use crate::streaming::users::user::User;
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

pub fn map_user_from_metadata(user: &UserMeta) -> UserInfoDetails {
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
            name: personal_access_token.name.as_str().to_owned(),
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

/// Map StreamMeta to Stream for HTTP responses
pub fn map_stream_from_metadata(
    stream: &StreamMeta,
    stats: Option<&StreamStats>,
) -> iggy_common::Stream {
    let (size, messages_count) = stats
        .map(|s| {
            (
                s.size_bytes_inconsistent().into(),
                s.messages_count_inconsistent(),
            )
        })
        .unwrap_or((0u64.into(), 0));

    iggy_common::Stream {
        id: stream.id as u32,
        created_at: stream.created_at,
        name: stream.name.clone(),
        topics_count: stream.topics_count() as u32,
        size,
        messages_count,
    }
}

/// Map StreamMeta to StreamDetails for HTTP responses
pub fn map_stream_details_from_metadata(
    stream: &StreamMeta,
    stream_stats: Option<&StreamStats>,
    topics_with_stats: &[(&TopicMeta, Option<&TopicStats>)],
) -> iggy_common::StreamDetails {
    let (size, messages_count) = stream_stats
        .map(|s| {
            (
                s.size_bytes_inconsistent().into(),
                s.messages_count_inconsistent(),
            )
        })
        .unwrap_or((0u64.into(), 0));

    let mut topics: Vec<iggy_common::Topic> = topics_with_stats
        .iter()
        .map(|(t, s)| map_topic_from_metadata(t, *s))
        .collect();
    topics.sort_by(|a, b| a.id.cmp(&b.id));

    iggy_common::StreamDetails {
        id: stream.id as u32,
        created_at: stream.created_at,
        name: stream.name.clone(),
        topics_count: stream.topics_count() as u32,
        size,
        messages_count,
        topics,
    }
}

/// Map TopicMeta to Topic for HTTP responses
pub fn map_topic_from_metadata(
    topic: &TopicMeta,
    stats: Option<&TopicStats>,
) -> iggy_common::Topic {
    let (size, messages_count) = stats
        .map(|s| {
            (
                s.size_bytes_inconsistent().into(),
                s.messages_count_inconsistent(),
            )
        })
        .unwrap_or((0u64.into(), 0));

    iggy_common::Topic {
        id: topic.id as u32,
        created_at: topic.created_at,
        name: topic.name.clone(),
        partitions_count: topic.partitions_count(),
        size,
        messages_count,
        message_expiry: topic.message_expiry,
        compression_algorithm: topic.compression_algorithm,
        max_topic_size: topic.max_topic_size,
        replication_factor: topic.replication_factor,
    }
}

/// Map TopicMeta to TopicDetails for HTTP responses
pub fn map_topic_details_from_metadata(
    topic: &TopicMeta,
    topic_stats: Option<&TopicStats>,
    partition_stats: &[(&PartitionMeta, Option<&PartitionStats>)],
) -> TopicDetails {
    let (size, messages_count) = topic_stats
        .map(|s| {
            (
                s.size_bytes_inconsistent().into(),
                s.messages_count_inconsistent(),
            )
        })
        .unwrap_or((0u64.into(), 0));

    let mut partitions: Vec<iggy_common::Partition> = partition_stats
        .iter()
        .map(|(p, stats)| {
            let (p_size, p_messages, p_offset, p_segments) = stats
                .map(|s| {
                    (
                        IggyByteSize::from(s.size_bytes_inconsistent()),
                        s.messages_count_inconsistent(),
                        s.current_offset(),
                        s.segments_count_inconsistent(),
                    )
                })
                .unwrap_or((0u64.into(), 0, 0, 0));

            iggy_common::Partition {
                id: p.id as u32,
                created_at: p.created_at,
                segments_count: p_segments,
                current_offset: p_offset,
                size: p_size,
                messages_count: p_messages,
            }
        })
        .collect();

    partitions.sort_by(|a, b| a.id.cmp(&b.id));

    TopicDetails {
        id: topic.id as u32,
        created_at: topic.created_at,
        name: topic.name.clone(),
        size,
        messages_count,
        partitions_count: partitions.len() as u32,
        partitions,
        message_expiry: topic.message_expiry,
        compression_algorithm: topic.compression_algorithm,
        max_topic_size: topic.max_topic_size,
        replication_factor: topic.replication_factor,
    }
}

/// Map multiple StreamMeta to Vec<Stream>
pub fn map_streams_from_metadata(streams: &[StreamMeta]) -> Vec<iggy_common::Stream> {
    let mut result: Vec<iggy_common::Stream> = streams
        .iter()
        .map(|s| map_stream_from_metadata(s, None))
        .collect();
    result.sort_by(|a, b| a.id.cmp(&b.id));
    result
}

/// Map ConsumerGroupMeta to ConsumerGroup for HTTP responses
pub fn map_consumer_group_from_metadata(cg: &ConsumerGroupMeta) -> iggy_common::ConsumerGroup {
    iggy_common::ConsumerGroup {
        id: cg.id as u32,
        name: cg.name.clone(),
        partitions_count: cg.partitions.len() as u32,
        members_count: cg.members.len() as u32,
    }
}

/// Map ConsumerGroupMeta to ConsumerGroupDetails for HTTP responses
pub fn map_consumer_group_details_from_metadata(cg: &ConsumerGroupMeta) -> ConsumerGroupDetails {
    let members: Vec<ConsumerGroupMember> = cg
        .members
        .values()
        .map(|m| ConsumerGroupMember {
            id: m.client_id,
            partitions_count: m.partitions.len() as u32,
            partitions: m.partitions.iter().map(|p| *p as u32).collect(),
        })
        .collect();

    ConsumerGroupDetails {
        id: cg.id as u32,
        name: cg.name.clone(),
        partitions_count: cg.partitions.len() as u32,
        members_count: cg.members.len() as u32,
        members,
    }
}

/// Map multiple ConsumerGroupMeta to Vec<ConsumerGroup>
pub fn map_consumer_groups_from_metadata(
    cgs: &[ConsumerGroupMeta],
) -> Vec<iggy_common::ConsumerGroup> {
    let mut groups: Vec<iggy_common::ConsumerGroup> =
        cgs.iter().map(map_consumer_group_from_metadata).collect();
    groups.sort_by(|a, b| a.id.cmp(&b.id));
    groups
}

/// Map multiple topics from metadata with stats
pub fn map_topics_from_metadata(
    topics: &[(&TopicMeta, Option<&TopicStats>)],
) -> Vec<iggy_common::Topic> {
    let mut result: Vec<iggy_common::Topic> = topics
        .iter()
        .map(|(t, s)| map_topic_from_metadata(t, *s))
        .collect();
    result.sort_by(|a, b| a.id.cmp(&b.id));
    result
}
