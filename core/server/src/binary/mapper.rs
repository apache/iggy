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

use std::sync::{Arc, atomic::AtomicU64};

use crate::slab::Keyed;
use crate::slab::traits_ext::{EntityComponentSystem, IntoComponents};
use crate::streaming::clients::client_manager::Client;
use crate::streaming::partitions::partition::PartitionRoot;
use crate::streaming::personal_access_tokens::personal_access_token::PersonalAccessToken;
use crate::streaming::stats::{PartitionStats, StreamStats, TopicStats};
use crate::streaming::streams::stream;
use crate::streaming::topics::consumer_group::{ConsumerGroupMembers, ConsumerGroupRoot, Member};
use crate::streaming::topics::topic::{self, TopicRoot};
use crate::streaming::users::user::User;
use arcshift::SharedGetGuard;
use bytes::{BufMut, Bytes, BytesMut};
use iggy_common::{BytesSerializable, ConsumerOffsetInfo, Stats, TransportProtocol, UserId};
use slab::Slab;

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

pub fn map_streams(roots: &Slab<stream::StreamRoot>, stats: &Slab<Arc<StreamStats>>) -> Bytes {
    let mut bytes = BytesMut::new();
    for (root, stat) in roots
        .iter()
        .map(|(_, val)| val)
        .zip(stats.iter().map(|(_, val)| val))
    {
        extend_stream(root, stat, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_stream(root: &stream::StreamRoot, stats: &StreamStats) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_stream(root, stats, &mut bytes);
    root.topics().with_components(|topics| {
        let (roots, _, stats, ..) = topics.into_components();
        for (root, stat) in roots
            .iter()
            .map(|(_, val)| val)
            .zip(stats.iter().map(|(_, val)| val))
        {
            extend_topic(root, stat, &mut bytes);
        }
    });
    bytes.freeze()
}

pub fn map_topics(roots: &Slab<TopicRoot>, stats: &Slab<Arc<TopicStats>>) -> Bytes {
    let mut bytes = BytesMut::new();
    for (root, stat) in roots
        .iter()
        .map(|(_, val)| val)
        .zip(stats.iter().map(|(_, val)| val))
    {
        extend_topic(root, stat, &mut bytes);
    }
    bytes.freeze()
}

pub fn map_topic(root: &topic::TopicRoot, stats: &TopicStats) -> Bytes {
    let mut bytes = BytesMut::new();
    extend_topic(root, stats, &mut bytes);
    root.partitions().with_components(|partitions| {
        let (roots, stats, _, offsets, _, _, _) = partitions.into_components();
        for (root, stat, offset) in roots
            .iter()
            .map(|(_, val)| val)
            .zip(stats.iter().map(|(_, val)| val))
            .zip(offsets.iter().map(|(_, val)| val))
            .map(|((root, stat), offset)| (root, stat, offset))
        {
            extend_partition(root, stat, offset, &mut bytes);
        }
    });

    bytes.freeze()
}

pub fn map_consumer_group(root: &ConsumerGroupRoot, members: &ConsumerGroupMembers) -> Bytes {
    let mut bytes = BytesMut::new();
    let members = members.inner().shared_get();
    extend_consumer_group(root, &members, &mut bytes);

    for (_, member) in members.iter() {
        bytes.put_u32_le(member.id as u32);
        bytes.put_u32_le(member.partitions.len() as u32);
        for partition in &member.partitions {
            bytes.put_u32_le(*partition as u32);
        }
    }
    bytes.freeze()
}

pub fn map_consumer_groups(
    roots: &Slab<ConsumerGroupRoot>,
    members: &Slab<ConsumerGroupMembers>,
) -> Bytes {
    let mut bytes = BytesMut::new();
    for (root, member) in roots
        .iter()
        .map(|(_, val)| val)
        .zip(members.iter().map(|(_, val)| val.inner().shared_get()))
    {
        extend_consumer_group(root, &member, &mut bytes);
    }
    bytes.freeze()
}

fn extend_stream(root: &stream::StreamRoot, stats: &StreamStats, bytes: &mut BytesMut) {
    bytes.put_u32_le(root.id() as u32);
    bytes.put_u64_le(root.created_at().into());
    bytes.put_u32_le(root.topics_count() as u32);
    bytes.put_u64_le(stats.size_bytes_inconsistent());
    bytes.put_u64_le(stats.messages_count_inconsistent());
    bytes.put_u8(root.name().len() as u8);
    bytes.put_slice(root.name().as_bytes());
}

fn extend_topic(root: &TopicRoot, stats: &TopicStats, bytes: &mut BytesMut) {
    bytes.put_u32_le(root.id() as u32);
    bytes.put_u64_le(root.created_at().into());
    bytes.put_u32_le(root.partitions().len() as u32);
    bytes.put_u64_le(root.message_expiry().into());
    bytes.put_u8(root.compression_algorithm().as_code());
    bytes.put_u64_le(root.max_topic_size().into());
    bytes.put_u8(root.replication_factor());
    bytes.put_u64_le(stats.size_bytes_inconsistent());
    bytes.put_u64_le(stats.messages_count_inconsistent());
    bytes.put_u8(root.name().len() as u8);
    bytes.put_slice(root.name().as_bytes());
}

fn extend_partition(
    root: &PartitionRoot,
    stats: &PartitionStats,
    offset: &Arc<AtomicU64>,
    bytes: &mut BytesMut,
) {
    bytes.put_u32_le(root.id() as u32);
    bytes.put_u64_le(root.created_at().into());
    bytes.put_u32_le(stats.segments_count_inconsistent());
    bytes.put_u64_le(offset.load(std::sync::atomic::Ordering::Relaxed));
    bytes.put_u64_le(stats.size_bytes_inconsistent());
    bytes.put_u64_le(stats.messages_count_inconsistent());
}

fn extend_consumer_group(
    root: &ConsumerGroupRoot,
    members: &SharedGetGuard<'_, Slab<Member>>,
    bytes: &mut BytesMut,
) {
    bytes.put_u32_le(root.id() as u32);
    bytes.put_u32_le(root.partitions().len() as u32);
    bytes.put_u32_le(members.len() as u32);
    bytes.put_u8(root.key().len() as u8);
    bytes.put_slice(root.key().as_bytes());
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
