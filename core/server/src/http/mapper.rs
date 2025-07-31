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
use crate::streaming::clients::client_manager::Client;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::streams::stream::Stream;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::user::User;
use iggy_common::ConsumerGroupInfo;
use iggy_common::PersonalAccessTokenInfo;
use iggy_common::Sizeable;
use iggy_common::StreamDetails;
use iggy_common::TopicDetails;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{ConsumerGroupDetails, ConsumerGroupMember};
use iggy_common::{IdentityInfo, TokenInfo};
use iggy_common::{UserInfo, UserInfoDetails};

pub fn map_stream(stream: &Stream) -> StreamDetails {
    let topics = map_topics(&stream.get_topics());
    let mut stream_details = StreamDetails {
        id: stream.stream_id,
        created_at: stream.created_at,
        name: stream.name.clone(),
        topics_count: topics.len() as u32,
        size: stream.get_size(),
        messages_count: stream.get_messages_count(),
        topics,
    };
    stream_details.topics.sort_by(|a, b| a.id.cmp(&b.id));
    stream_details
}

pub fn map_streams(streams: &[&Stream]) -> Vec<iggy_common::Stream> {
    let mut streams_data = Vec::with_capacity(streams.len());
    for stream in streams {
        let stream = iggy_common::Stream {
            id: stream.stream_id,
            created_at: stream.created_at,
            name: stream.name.clone(),
            size: stream.get_size(),
            topics_count: stream.get_topics().len() as u32,
            messages_count: stream.get_messages_count(),
        };
        streams_data.push(stream);
    }

    streams_data.sort_by(|a, b| a.id.cmp(&b.id));
    streams_data
}

pub fn map_topics(topics: &[&Topic]) -> Vec<iggy_common::Topic> {
    let mut topics_data = Vec::with_capacity(topics.len());
    for topic in topics {
        let topic = iggy_common::Topic {
            id: topic.topic_id,
            created_at: topic.created_at,
            name: topic.name.clone(),
            size: topic.get_size_bytes(),
            partitions_count: topic.get_partitions().len() as u32,
            messages_count: topic.get_messages_count(),
            message_expiry: topic.message_expiry,
            compression_algorithm: topic.compression_algorithm,
            max_topic_size: topic.max_topic_size,
            replication_factor: topic.replication_factor,
        };
        topics_data.push(topic);
    }
    topics_data.sort_by(|a, b| a.id.cmp(&b.id));
    topics_data
}

pub async fn map_topic(topic: &Topic) -> TopicDetails {
    let mut topic_details = TopicDetails {
        id: topic.topic_id,
        created_at: topic.created_at,
        name: topic.name.clone(),
        size: topic.get_size_bytes(),
        messages_count: topic.get_messages_count(),
        partitions_count: topic.get_partitions().len() as u32,
        partitions: Vec::new(),
        message_expiry: topic.message_expiry,
        compression_algorithm: topic.compression_algorithm,
        max_topic_size: topic.max_topic_size,
        replication_factor: topic.replication_factor,
    };
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        topic_details.partitions.push(iggy_common::Partition {
            id: partition.partition_id,
            created_at: partition.created_at,
            segments_count: partition.get_segments().len() as u32,
            current_offset: partition.current_offset,
            size: partition.get_size_bytes(),
            messages_count: partition.get_messages_count(),
        });
    }
    topic_details.partitions.sort_by(|a, b| a.id.cmp(&b.id));
    topic_details
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

pub fn map_consumer_groups(consumer_groups: &[ConsumerGroup]) -> Vec<iggy_common::ConsumerGroup> {
    let mut groups = Vec::new();
    for consumer_group in consumer_groups {
        let consumer_group = iggy_common::ConsumerGroup {
            id: consumer_group.group_id,
            name: consumer_group.name.clone(),
            partitions_count: consumer_group.partitions_count,
            members_count: consumer_group.get_members().len() as u32,
        };
        groups.push(consumer_group);
    }
    groups.sort_by(|a, b| a.id.cmp(&b.id));
    groups
}

pub fn map_consumer_group(consumer_group: &ConsumerGroup) -> ConsumerGroupDetails {
    let mut consumer_group_details = ConsumerGroupDetails {
        id: consumer_group.group_id,
        name: consumer_group.name.clone(),
        partitions_count: consumer_group.partitions_count,
        members_count: consumer_group.get_members().len() as u32,
        members: Vec::new(),
    };
    let members = consumer_group.get_members();
    for member in members {
        let partitions = member.get_partitions();
        consumer_group_details.members.push(ConsumerGroupMember {
            id: member.id,
            partitions_count: partitions.len() as u32,
            partitions,
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
