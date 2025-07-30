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

use std::cell::Ref;

use crate::streaming::clients::client_manager::{Client, Transport};
use crate::streaming::partitions::partition::Partition;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::streams::stream::Stream;
use crate::streaming::streams::stream2;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use crate::streaming::topics::topic::Topic;
use crate::streaming::users::user::User;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{BytesSerializable, ConsumerOffsetInfo, Sizeable, Stats, UserId};

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

pub fn map_stream2(stream: &stream2::Stream) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_stream2(stream, &mut bytes);
    /*
    for topic in stream.get_topics() {
        extend_topic(topic, &mut bytes);
    }
    */
    bytes.freeze()
}
pub fn map_stream(stream: &Stream) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_stream(stream, &mut bytes);
    for topic in stream.get_topics() {
        extend_topic(topic, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_streams(streams: Vec<Ref<'_, Stream>>) -> Bytes {
    let mut bytes = BytesMut::new();
    for stream in streams.iter() {
        extend_stream(stream, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_topics(topics: Vec<&Topic>) -> Bytes {
    let mut bytes = BytesMut::new();
    for topic in topics {
        extend_topic(topic, &mut bytes);
    }
    bytes.freeze()
}

pub async fn map_topic(topic: &Topic) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_topic(topic, &mut bytes);
    for partition in topic.get_partitions() {
        let partition = partition.read().await;
        extend_partition(&partition, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_consumer_group(consumer_group: &ConsumerGroup) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_consumer_group(consumer_group, &mut bytes);
    let members = consumer_group.get_members();
    for member in members {
        bytes.put_u32_le(member.id);
        let partitions = member.get_partitions();
        bytes.put_u32_le(partitions.len() as u32);
        for partition in partitions {
            bytes.put_u32_le(partition);
        }
    }
    bytes.freeze()
}

pub fn map_consumer_groups(consumer_groups: Vec<ConsumerGroup>) -> Bytes {
    let mut bytes = BytesMut::new();
    for consumer_group in consumer_groups {
        extend_consumer_group(&consumer_group, &mut bytes);
    }
    bytes.freeze()
}

fn extend_stream2(stream: &stream2::Stream, bytes: &mut BytesMut) {
    //stream.serialize_into(bytes);
    // TODO:
}

fn extend_stream(stream: &Stream, bytes: &mut BytesMut) {
    bytes.put_u32_le(stream.stream_id);
    bytes.put_u64_le(stream.created_at.into());
    bytes.put_u32_le(stream.get_topics().len() as u32);
    bytes.put_u64_le(stream.get_size().as_bytes_u64());
    bytes.put_u64_le(stream.get_messages_count());
    bytes.put_u8(stream.name.len() as u8);
    bytes.put_slice(stream.name.as_bytes());
}

fn extend_topic(topic: &Topic, bytes: &mut BytesMut) {
    bytes.put_u32_le(topic.topic_id);
    bytes.put_u64_le(topic.created_at.into());
    bytes.put_u32_le(topic.get_partitions().len() as u32);
    bytes.put_u64_le(topic.message_expiry.into());
    bytes.put_u8(topic.compression_algorithm.as_code());
    bytes.put_u64_le(topic.max_topic_size.into());
    bytes.put_u8(topic.replication_factor);
    bytes.put_u64_le(topic.get_size_bytes().as_bytes_u64());
    bytes.put_u64_le(topic.get_messages_count());
    bytes.put_u8(topic.name.len() as u8);
    bytes.put_slice(topic.name.as_bytes());
}

fn extend_partition(partition: &Partition, bytes: &mut BytesMut) {
    bytes.put_u32_le(partition.partition_id);
    bytes.put_u64_le(partition.created_at.into());
    bytes.put_u32_le(partition.get_segments().len() as u32);
    bytes.put_u64_le(partition.current_offset);
    bytes.put_u64_le(partition.get_size_bytes().as_bytes_u64());
    bytes.put_u64_le(partition.get_messages_count());
}

fn extend_consumer_group(consumer_group: &ConsumerGroup, bytes: &mut BytesMut) {
    bytes.put_u32_le(consumer_group.group_id);
    bytes.put_u32_le(consumer_group.partitions_count);
    bytes.put_u32_le(consumer_group.get_members().len() as u32);
    bytes.put_u8(consumer_group.name.len() as u8);
    bytes.put_slice(consumer_group.name.as_bytes());
}

fn extend_client(client: &Client, bytes: &mut BytesMut) {
    bytes.put_u32_le(client.session.client_id);
    bytes.put_u32_le(client.user_id.unwrap_or(0));
    let transport: u8 = match client.transport {
        Transport::Tcp => 1,
        Transport::Quic => 2,
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
