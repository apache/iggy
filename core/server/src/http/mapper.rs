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
use crate::slab::Keyed;
use crate::slab::traits_ext::{EntityComponentSystem, IntoComponents};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::stats::TopicStats;
use crate::streaming::topics::consumer_group::{ConsumerGroupMembers, ConsumerGroupRoot};
use crate::streaming::topics::topic::TopicRoot;
use crate::streaming::users::user::User;
use iggy_common::{ConsumerGroupDetails, ConsumerGroupInfo, ConsumerGroupMember, IggyByteSize};
use iggy_common::{IdentityInfo, PersonalAccessTokenInfo, TokenInfo, TopicDetails};
use iggy_common::{UserInfo, UserInfoDetails};
use slab::Slab;
use std::sync::Arc;

/// Map TopicRoot with partitions to TopicDetails for HTTP responses
pub fn map_topic_details(root: &TopicRoot, stats: &TopicStats) -> TopicDetails {
    let mut partitions = Vec::new();

    // Get partition details similar to binary mapper
    root.partitions().with_components(|partition_components| {
        let (partition_roots, partition_stats, _, offsets, _, _, _) =
            partition_components.into_components();
        for (partition_root, partition_stat, offset) in partition_roots
            .iter()
            .map(|(_, val)| val)
            .zip(partition_stats.iter().map(|(_, val)| val))
            .zip(offsets.iter().map(|(_, val)| val))
            .map(|((root, stat), offset)| (root, stat, offset))
        {
            partitions.push(iggy_common::Partition {
                id: partition_root.id() as u32,
                created_at: partition_root.created_at(),
                segments_count: partition_stat.segments_count_inconsistent(),
                current_offset: offset.load(std::sync::atomic::Ordering::Relaxed),
                size: IggyByteSize::from(partition_stat.size_bytes_inconsistent()),
                messages_count: partition_stat.messages_count_inconsistent(),
            });
        }
    });

    // Sort partitions by ID
    partitions.sort_by(|a, b| a.id.cmp(&b.id));

    TopicDetails {
        id: root.id() as u32,
        created_at: root.created_at(),
        name: root.name().clone(),
        size: stats.size_bytes_inconsistent().into(),
        messages_count: stats.messages_count_inconsistent(),
        partitions_count: partitions.len() as u32,
        partitions,
        message_expiry: root.message_expiry(),
        compression_algorithm: root.compression_algorithm(),
        max_topic_size: root.max_topic_size(),
        replication_factor: root.replication_factor(),
    }
}

/// Map TopicRoot and TopicStats to Topic for HTTP responses
pub fn map_topic(root: &TopicRoot, stats: &TopicStats) -> iggy_common::Topic {
    iggy_common::Topic {
        id: root.id() as u32,
        created_at: root.created_at(),
        name: root.name().clone(),
        size: stats.size_bytes_inconsistent().into(),
        partitions_count: root.partitions().len() as u32,
        messages_count: stats.messages_count_inconsistent(),
        message_expiry: root.message_expiry(),
        compression_algorithm: root.compression_algorithm(),
        max_topic_size: root.max_topic_size(),
        replication_factor: root.replication_factor(),
    }
}

/// Map multiple topics from slab components to Vec<Topic> for HTTP responses
pub fn map_topics_from_components(
    roots: &Slab<TopicRoot>,
    stats: &Slab<Arc<TopicStats>>,
) -> Vec<iggy_common::Topic> {
    let mut topics = roots
        .iter()
        .map(|(_, root)| root)
        .zip(stats.iter().map(|(_, stat)| stat))
        .map(|(root, stat)| map_topic(root, stat))
        .collect::<Vec<_>>();

    topics.sort_by(|a, b| a.id.cmp(&b.id));
    topics
}

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

pub fn map_consumer_groups(
    roots: &slab::Slab<ConsumerGroupRoot>,
    members: &slab::Slab<ConsumerGroupMembers>,
) -> Vec<iggy_common::ConsumerGroup> {
    let mut groups = Vec::new();
    for (root, member) in roots
        .iter()
        .map(|(_, val)| val)
        .zip(members.iter().map(|(_, val)| val))
    {
        let members_guard = member.inner().shared_get();
        let consumer_group = iggy_common::ConsumerGroup {
            id: root.id() as u32,
            name: root.key().clone(),
            partitions_count: root.partitions().len() as u32,
            members_count: members_guard.len() as u32,
        };
        groups.push(consumer_group);
    }
    groups.sort_by(|a, b| a.id.cmp(&b.id));
    groups
}

pub fn map_consumer_group(
    root: &ConsumerGroupRoot,
    members: &ConsumerGroupMembers,
) -> ConsumerGroupDetails {
    let members_guard = members.inner().shared_get();
    let mut consumer_group_details = ConsumerGroupDetails {
        id: root.id() as u32,
        name: root.key().clone(),
        partitions_count: root.partitions().len() as u32,
        members_count: members_guard.len() as u32,
        members: Vec::new(),
    };

    for (_, member) in members_guard.iter() {
        consumer_group_details.members.push(ConsumerGroupMember {
            id: member.id as u32,
            partitions_count: member.partitions.len() as u32,
            partitions: member.partitions.iter().map(|p| *p as u32).collect(),
        });
    }
    consumer_group_details
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

/// Map StreamRoot and StreamStats to StreamDetails for HTTP responses
pub fn map_stream_details(
    root: &crate::streaming::streams::stream::StreamRoot,
    stats: &crate::streaming::stats::StreamStats,
) -> iggy_common::StreamDetails {
    // Get topics using the new slab-based API
    let topics = root.topics().with_components(|topic_ref| {
        let (topic_roots, _topic_auxiliaries, topic_stats) = topic_ref.into_components();
        let mut topics_vec = Vec::new();

        // Iterate over topics in the stream
        for (topic_root, topic_stat) in topic_roots
            .iter()
            .map(|(_, root)| root)
            .zip(topic_stats.iter().map(|(_, stat)| stat))
        {
            topics_vec.push(map_topic(topic_root, topic_stat));
        }

        // Sort topics by ID for consistent ordering
        topics_vec.sort_by(|a, b| a.id.cmp(&b.id));
        topics_vec
    });

    iggy_common::StreamDetails {
        id: root.id() as u32,
        created_at: root.created_at(),
        name: root.name().clone(),
        topics_count: root.topics_count() as u32,
        size: stats.size_bytes_inconsistent().into(),
        messages_count: stats.messages_count_inconsistent(),
        topics,
    }
}

/// Map StreamRoot and StreamStats to Stream for HTTP responses
pub fn map_stream(
    root: &crate::streaming::streams::stream::StreamRoot,
    stats: &crate::streaming::stats::StreamStats,
) -> iggy_common::Stream {
    iggy_common::Stream {
        id: root.id() as u32,
        created_at: root.created_at(),
        name: root.name().clone(),
        topics_count: root.topics_count() as u32,
        size: stats.size_bytes_inconsistent().into(),
        messages_count: stats.messages_count_inconsistent(),
    }
}

/// Map multiple streams from slabs
pub fn map_streams_from_slabs(
    roots: &slab::Slab<crate::streaming::streams::stream::StreamRoot>,
    stats: &slab::Slab<Arc<crate::streaming::stats::StreamStats>>,
) -> Vec<iggy_common::Stream> {
    let mut streams = Vec::new();
    for (root, stat) in roots
        .iter()
        .map(|(_, val)| val)
        .zip(stats.iter().map(|(_, val)| val))
    {
        streams.push(map_stream(root, stat));
    }
    streams.sort_by(|a, b| a.id.cmp(&b.id));
    streams
}
