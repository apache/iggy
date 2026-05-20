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

//! Conversions between `iggy_binary_protocol` wire types and `iggy_common` domain types.
//!
//! Where the orphan rule allows it, conversions are `From`/`TryFrom` impls on domain types.
//! Collection conversions (`Vec<T>`) and tuple conversions use free functions instead,
//! since neither the container nor the wire type is local.

use crate::{
    CacheMetrics, CacheMetricsKey, ClientInfo, ClientInfoDetails, ClusterMetadata, ClusterNode,
    ClusterNodeRole, ClusterNodeStatus, CompressionAlgorithm, Consumer, ConsumerGroup,
    ConsumerGroupDetails, ConsumerGroupInfo, ConsumerGroupMember, ConsumerOffsetInfo,
    GlobalPermissions, HeaderKey, HeaderKind, HeaderValue, IdKind, IdentityInfo, IggyByteSize,
    IggyError, IggyExpiry, MaxTopicSize, Partition, Permissions, PersonalAccessTokenInfo,
    RawPersonalAccessToken, Stats, Stream, StreamDetails, StreamPermissions, Topic, TopicDetails,
    TopicPermissions, TransportEndpoints, UserInfo, UserInfoDetails, UserStatus,
};
use iggy_binary_protocol::WireConsumer;
use iggy_binary_protocol::primitives::permissions::{
    WireGlobalPermissions, WirePermissions, WireStreamPermissions, WireTopicPermissions,
};
use iggy_binary_protocol::responses::clients::client_response::{
    ClientResponse, ConsumerGroupInfoResponse,
};
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::consumer_groups::consumer_group_response::ConsumerGroupResponse;
use iggy_binary_protocol::responses::consumer_groups::get_consumer_group::{
    ConsumerGroupDetailsResponse, ConsumerGroupMemberResponse,
};
use iggy_binary_protocol::responses::consumer_groups::get_consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::consumer_offsets::get_consumer_offset::ConsumerOffsetResponse;
use iggy_binary_protocol::responses::personal_access_tokens::create_personal_access_token::RawPersonalAccessTokenResponse;
use iggy_binary_protocol::responses::personal_access_tokens::get_personal_access_tokens::{
    GetPersonalAccessTokensResponse, PersonalAccessTokenResponse,
};
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{GetStreamResponse, TopicHeader};
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::get_cluster_metadata::{
    ClusterMetadataResponse, ClusterNodeResponse,
};
use iggy_binary_protocol::responses::system::get_stats::{CacheMetricEntry, StatsResponse};
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::login_user::IdentityResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::responses::users::{GetUsersResponse, UserDetailsResponse};
use std::collections::{BTreeMap, HashMap};

/// Sentinel value in the wire protocol indicating no authenticated user.
const WIRE_NO_USER_ID: u32 = u32::MAX;

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

impl From<StreamResponse> for Stream {
    fn from(w: StreamResponse) -> Self {
        Self {
            id: w.id,
            created_at: w.created_at.into(),
            name: w.name.to_string(),
            size: IggyByteSize::from(w.size_bytes),
            messages_count: w.messages_count,
            topics_count: w.topics_count,
        }
    }
}

impl TryFrom<GetStreamResponse> for StreamDetails {
    type Error = IggyError;

    fn try_from(w: GetStreamResponse) -> Result<Self, Self::Error> {
        let mut topics: Vec<Topic> = w
            .topics
            .into_iter()
            .map(Topic::try_from)
            .collect::<Result<_, _>>()?;
        topics.sort_by_key(|t| t.id);
        Ok(Self {
            id: w.stream.id,
            created_at: w.stream.created_at.into(),
            name: w.stream.name.to_string(),
            size: IggyByteSize::from(w.stream.size_bytes),
            messages_count: w.stream.messages_count,
            topics_count: w.stream.topics_count,
            topics,
        })
    }
}

pub fn streams_from_wire(w: GetStreamsResponse) -> Vec<Stream> {
    let mut streams: Vec<Stream> = w.streams.into_iter().map(Stream::from).collect();
    streams.sort_by_key(|s| s.id);
    streams
}

// ---------------------------------------------------------------------------
// Topics
// ---------------------------------------------------------------------------

impl TryFrom<TopicHeader> for Topic {
    type Error = IggyError;

    fn try_from(w: TopicHeader) -> Result<Self, Self::Error> {
        let message_expiry = match w.message_expiry {
            0 => IggyExpiry::NeverExpire,
            v => v.into(),
        };
        let max_topic_size: MaxTopicSize = w.max_topic_size.into();
        Ok(Self {
            id: w.id,
            created_at: w.created_at.into(),
            name: w.name.to_string(),
            partitions_count: w.partitions_count,
            size: IggyByteSize::from(w.size_bytes),
            messages_count: w.messages_count,
            message_expiry,
            compression_algorithm: CompressionAlgorithm::from_code(w.compression_algorithm)?,
            max_topic_size,
            replication_factor: w.replication_factor,
        })
    }
}

impl From<PartitionResponse> for Partition {
    fn from(w: PartitionResponse) -> Self {
        Self {
            id: w.id,
            created_at: w.created_at.into(),
            segments_count: w.segments_count,
            current_offset: w.current_offset,
            size: IggyByteSize::from(w.size_bytes),
            messages_count: w.messages_count,
        }
    }
}

impl TryFrom<GetTopicResponse> for TopicDetails {
    type Error = IggyError;

    fn try_from(w: GetTopicResponse) -> Result<Self, Self::Error> {
        let topic = Topic::try_from(w.topic)?;
        let mut partitions: Vec<Partition> =
            w.partitions.into_iter().map(Partition::from).collect();
        partitions.sort_by_key(|p| p.id);
        Ok(Self {
            id: topic.id,
            created_at: topic.created_at,
            name: topic.name,
            size: topic.size,
            messages_count: topic.messages_count,
            message_expiry: topic.message_expiry,
            compression_algorithm: topic.compression_algorithm,
            max_topic_size: topic.max_topic_size,
            replication_factor: topic.replication_factor,
            partitions_count: topic.partitions_count,
            partitions,
        })
    }
}

pub fn topics_from_wire(w: GetTopicsResponse) -> Result<Vec<Topic>, IggyError> {
    let mut topics: Vec<Topic> = w
        .topics
        .into_iter()
        .map(Topic::try_from)
        .collect::<Result<_, _>>()?;
    topics.sort_by_key(|t| t.id);
    Ok(topics)
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

impl TryFrom<UserResponse> for UserInfo {
    type Error = IggyError;

    fn try_from(w: UserResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            id: w.id,
            created_at: w.created_at.into(),
            status: UserStatus::from_code(w.status)?,
            username: w.username.to_string(),
        })
    }
}

impl TryFrom<UserDetailsResponse> for UserInfoDetails {
    type Error = IggyError;

    fn try_from(w: UserDetailsResponse) -> Result<Self, Self::Error> {
        let user = UserInfo::try_from(w.user)?;
        let permissions = w.permissions.map(Permissions::from);
        Ok(Self {
            id: user.id,
            created_at: user.created_at,
            status: user.status,
            username: user.username,
            permissions,
        })
    }
}

pub fn users_from_wire(w: GetUsersResponse) -> Result<Vec<UserInfo>, IggyError> {
    let mut users: Vec<UserInfo> = w
        .users
        .into_iter()
        .map(UserInfo::try_from)
        .collect::<Result<_, _>>()?;
    users.sort_by_key(|u| u.id);
    Ok(users)
}

impl From<IdentityResponse> for IdentityInfo {
    fn from(w: IdentityResponse) -> Self {
        Self {
            user_id: w.user_id,
            access_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

impl From<ConsumerGroupInfoResponse> for ConsumerGroupInfo {
    fn from(w: ConsumerGroupInfoResponse) -> Self {
        Self {
            stream_id: w.stream_id,
            topic_id: w.topic_id,
            group_id: w.group_id,
        }
    }
}

impl From<ClientResponse> for ClientInfo {
    fn from(w: ClientResponse) -> Self {
        let user_id = match w.user_id {
            WIRE_NO_USER_ID => None,
            id => Some(id),
        };
        let transport = match w.transport {
            1 => "TCP",
            2 => "QUIC",
            3 => "HTTP",
            4 => "WebSocket",
            _ => "Unknown",
        }
        .to_string();
        Self {
            client_id: w.client_id,
            user_id,
            address: w.address,
            transport,
            consumer_groups_count: w.consumer_groups_count,
        }
    }
}

impl From<ClientDetailsResponse> for ClientInfoDetails {
    fn from(w: ClientDetailsResponse) -> Self {
        let client = ClientInfo::from(w.client);
        let mut consumer_groups: Vec<ConsumerGroupInfo> = w
            .consumer_groups
            .into_iter()
            .map(ConsumerGroupInfo::from)
            .collect();
        consumer_groups.sort_by_key(|cg| cg.group_id);
        Self {
            client_id: client.client_id,
            user_id: client.user_id,
            address: client.address,
            transport: client.transport,
            consumer_groups_count: client.consumer_groups_count,
            consumer_groups,
        }
    }
}

pub fn clients_from_wire(w: GetClientsResponse) -> Vec<ClientInfo> {
    let mut clients: Vec<ClientInfo> = w.clients.into_iter().map(ClientInfo::from).collect();
    clients.sort_by_key(|c| c.client_id);
    clients
}

// ---------------------------------------------------------------------------
// Consumer Groups
// ---------------------------------------------------------------------------

impl From<ConsumerGroupResponse> for ConsumerGroup {
    fn from(w: ConsumerGroupResponse) -> Self {
        Self {
            id: w.id,
            partitions_count: w.partitions_count,
            members_count: w.members_count,
            name: w.name.to_string(),
        }
    }
}

impl From<ConsumerGroupMemberResponse> for ConsumerGroupMember {
    fn from(w: ConsumerGroupMemberResponse) -> Self {
        Self {
            id: w.id,
            partitions_count: w.partitions_count,
            partitions: w.partitions,
        }
    }
}

impl From<ConsumerGroupDetailsResponse> for ConsumerGroupDetails {
    fn from(w: ConsumerGroupDetailsResponse) -> Self {
        let group = ConsumerGroup::from(w.group);
        let mut members: Vec<ConsumerGroupMember> = w
            .members
            .into_iter()
            .map(ConsumerGroupMember::from)
            .collect();
        members.sort_by_key(|m| m.id);
        Self {
            id: group.id,
            name: group.name,
            partitions_count: group.partitions_count,
            members_count: group.members_count,
            members,
        }
    }
}

pub fn consumer_groups_from_wire(w: GetConsumerGroupsResponse) -> Vec<ConsumerGroup> {
    let mut groups: Vec<ConsumerGroup> = w.groups.into_iter().map(ConsumerGroup::from).collect();
    groups.sort_by_key(|g| g.id);
    groups
}

// ---------------------------------------------------------------------------
// Consumer Offsets
// ---------------------------------------------------------------------------

impl From<ConsumerOffsetResponse> for ConsumerOffsetInfo {
    fn from(w: ConsumerOffsetResponse) -> Self {
        Self {
            partition_id: w.partition_id,
            current_offset: w.current_offset,
            stored_offset: w.stored_offset,
        }
    }
}

// ---------------------------------------------------------------------------
// Personal Access Tokens
// ---------------------------------------------------------------------------

impl From<PersonalAccessTokenResponse> for PersonalAccessTokenInfo {
    fn from(w: PersonalAccessTokenResponse) -> Self {
        let expiry_at = match w.expiry_at {
            0 => None,
            v => Some(v.into()),
        };
        Self {
            name: w.name.to_string(),
            expiry_at,
        }
    }
}

pub fn personal_access_tokens_from_wire(
    w: GetPersonalAccessTokensResponse,
) -> Vec<PersonalAccessTokenInfo> {
    let mut tokens: Vec<PersonalAccessTokenInfo> = w
        .tokens
        .into_iter()
        .map(PersonalAccessTokenInfo::from)
        .collect();
    tokens.sort_by(|a, b| a.name.cmp(&b.name));
    tokens
}

impl From<RawPersonalAccessTokenResponse> for RawPersonalAccessToken {
    fn from(w: RawPersonalAccessTokenResponse) -> Self {
        Self {
            token: w.token.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// System - Stats
// ---------------------------------------------------------------------------

fn cache_metric_from_wire(w: CacheMetricEntry) -> (CacheMetricsKey, CacheMetrics) {
    (
        CacheMetricsKey {
            stream_id: w.stream_id,
            topic_id: w.topic_id,
            partition_id: w.partition_id,
        },
        CacheMetrics {
            hits: w.hits,
            misses: w.misses,
            hit_ratio: w.hit_ratio,
        },
    )
}

impl From<StatsResponse> for Stats {
    fn from(w: StatsResponse) -> Self {
        let cache_metrics: HashMap<CacheMetricsKey, CacheMetrics> = w
            .cache_metrics
            .into_iter()
            .map(cache_metric_from_wire)
            .collect();
        Self {
            process_id: w.process_id,
            cpu_usage: w.cpu_usage,
            total_cpu_usage: w.total_cpu_usage,
            memory_usage: IggyByteSize::from(w.memory_usage),
            total_memory: IggyByteSize::from(w.total_memory),
            available_memory: IggyByteSize::from(w.available_memory),
            run_time: w.run_time.into(),
            start_time: w.start_time.into(),
            read_bytes: IggyByteSize::from(w.read_bytes),
            written_bytes: IggyByteSize::from(w.written_bytes),
            messages_size_bytes: IggyByteSize::from(w.messages_size_bytes),
            streams_count: w.streams_count,
            topics_count: w.topics_count,
            partitions_count: w.partitions_count,
            segments_count: w.segments_count,
            messages_count: w.messages_count,
            clients_count: w.clients_count,
            consumer_groups_count: w.consumer_groups_count,
            hostname: w.hostname,
            os_name: w.os_name,
            os_version: w.os_version,
            kernel_version: w.kernel_version,
            iggy_server_version: w.iggy_server_version,
            iggy_server_semver: w.iggy_server_semver,
            cache_metrics,
            threads_count: w.threads_count,
            free_disk_space: IggyByteSize::from(w.free_disk_space),
            total_disk_space: IggyByteSize::from(w.total_disk_space),
        }
    }
}

// ---------------------------------------------------------------------------
// System - Cluster Metadata
// ---------------------------------------------------------------------------

impl TryFrom<ClusterNodeResponse> for ClusterNode {
    type Error = IggyError;

    fn try_from(w: ClusterNodeResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            name: w.name,
            ip: w.ip,
            endpoints: TransportEndpoints::new(
                w.tcp_port,
                w.quic_port,
                w.http_port,
                w.websocket_port,
            ),
            role: ClusterNodeRole::try_from(w.role)?,
            status: ClusterNodeStatus::try_from(w.status)?,
        })
    }
}

impl TryFrom<ClusterMetadataResponse> for ClusterMetadata {
    type Error = IggyError;

    fn try_from(w: ClusterMetadataResponse) -> Result<Self, Self::Error> {
        let nodes: Vec<ClusterNode> = w
            .nodes
            .into_iter()
            .map(ClusterNode::try_from)
            .collect::<Result<_, _>>()?;
        Ok(Self {
            name: w.name,
            nodes,
        })
    }
}

/// Helper to convert a `ClusterMetadataResponse` into domain `ClusterMetadata`.
/// Errors if node role or status codes are unrecognized.
pub fn cluster_metadata_from_wire(
    w: ClusterMetadataResponse,
) -> Result<ClusterMetadata, IggyError> {
    ClusterMetadata::try_from(w)
}

// ---------------------------------------------------------------------------
// Identifier (domain -> wire)
// ---------------------------------------------------------------------------

/// Convert a domain `Identifier` to `WireIdentifier`.
pub fn identifier_to_wire(
    id: &crate::Identifier,
) -> Result<iggy_binary_protocol::WireIdentifier, IggyError> {
    match id.kind {
        IdKind::Numeric => {
            let value = id.get_u32_value()?;
            Ok(iggy_binary_protocol::WireIdentifier::numeric(value))
        }
        IdKind::String => {
            let name = id.get_string_value()?;
            iggy_binary_protocol::WireIdentifier::named(name)
                .map_err(|_| IggyError::InvalidIdentifier)
        }
    }
}

/// Convert a domain `Consumer` to `WireConsumer`.
pub fn consumer_to_wire(consumer: &Consumer) -> Result<WireConsumer, IggyError> {
    let wire_id = identifier_to_wire(&consumer.id)?;
    Ok(WireConsumer {
        kind: consumer.kind.as_code(),
        id: wire_id,
    })
}

/// Convert a domain `PollingStrategy` to `WirePollingStrategy`.
pub fn polling_strategy_to_wire(
    strategy: &crate::PollingStrategy,
) -> iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy {
    iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy {
        kind: strategy.kind.as_code(),
        value: strategy.value,
    }
}

/// Convert a domain `Partitioning` to `WirePartitioning`.
pub fn partitioning_to_wire(
    partitioning: &crate::Partitioning,
) -> Result<iggy_binary_protocol::primitives::partitioning::WirePartitioning, IggyError> {
    use iggy_binary_protocol::primitives::partitioning::WirePartitioning;
    match partitioning.kind {
        crate::PartitioningKind::Balanced => Ok(WirePartitioning::Balanced),
        crate::PartitioningKind::PartitionId => {
            let bytes: [u8; 4] = partitioning
                .value
                .get(..4)
                .and_then(|s| s.try_into().ok())
                .ok_or(IggyError::InvalidCommand)?;
            Ok(WirePartitioning::PartitionId(u32::from_le_bytes(bytes)))
        }
        crate::PartitioningKind::MessagesKey => {
            Ok(WirePartitioning::MessagesKey(partitioning.value.clone()))
        }
    }
}

// ---------------------------------------------------------------------------
// Permissions (wire -> domain)
// ---------------------------------------------------------------------------

impl From<WireGlobalPermissions> for GlobalPermissions {
    fn from(w: WireGlobalPermissions) -> Self {
        Self {
            manage_servers: w.manage_servers,
            read_servers: w.read_servers,
            manage_users: w.manage_users,
            read_users: w.read_users,
            manage_streams: w.manage_streams,
            read_streams: w.read_streams,
            manage_topics: w.manage_topics,
            read_topics: w.read_topics,
            poll_messages: w.poll_messages,
            send_messages: w.send_messages,
        }
    }
}

fn wire_topic_permissions_to_domain(w: WireTopicPermissions) -> (usize, TopicPermissions) {
    (
        w.topic_id as usize,
        TopicPermissions {
            manage_topic: w.manage_topic,
            read_topic: w.read_topic,
            poll_messages: w.poll_messages,
            send_messages: w.send_messages,
        },
    )
}

fn wire_stream_permissions_to_domain(w: WireStreamPermissions) -> (usize, StreamPermissions) {
    let topics: Option<BTreeMap<usize, TopicPermissions>> = if w.topics.is_empty() {
        None
    } else {
        Some(
            w.topics
                .into_iter()
                .map(wire_topic_permissions_to_domain)
                .collect(),
        )
    };
    (
        w.stream_id as usize,
        StreamPermissions {
            manage_stream: w.manage_stream,
            read_stream: w.read_stream,
            manage_topics: w.manage_topics,
            read_topics: w.read_topics,
            poll_messages: w.poll_messages,
            send_messages: w.send_messages,
            topics,
        },
    )
}

impl From<WirePermissions> for Permissions {
    fn from(w: WirePermissions) -> Self {
        let streams: Option<BTreeMap<usize, StreamPermissions>> = if w.streams.is_empty() {
            None
        } else {
            Some(
                w.streams
                    .into_iter()
                    .map(wire_stream_permissions_to_domain)
                    .collect(),
            )
        };
        Self {
            global: GlobalPermissions::from(w.global),
            streams,
        }
    }
}

/// Convert `&WirePermissions` to domain `Permissions` without consuming the input.
pub fn wire_permissions_to_permissions(wp: &WirePermissions) -> Permissions {
    Permissions::from(wp.clone())
}

// ---------------------------------------------------------------------------
// Permissions (domain -> wire)
// ---------------------------------------------------------------------------

/// Convert domain `Permissions` to `WirePermissions`.
pub fn permissions_to_wire(perms: &Permissions) -> WirePermissions {
    let streams: Vec<WireStreamPermissions> = perms
        .streams
        .as_ref()
        .map(|map| {
            map.iter()
                .map(|(&sid, sp)| stream_permissions_to_wire(sid, sp))
                .collect()
        })
        .unwrap_or_default();
    WirePermissions {
        global: WireGlobalPermissions {
            manage_servers: perms.global.manage_servers,
            read_servers: perms.global.read_servers,
            manage_users: perms.global.manage_users,
            read_users: perms.global.read_users,
            manage_streams: perms.global.manage_streams,
            read_streams: perms.global.read_streams,
            manage_topics: perms.global.manage_topics,
            read_topics: perms.global.read_topics,
            poll_messages: perms.global.poll_messages,
            send_messages: perms.global.send_messages,
        },
        streams,
    }
}

fn stream_permissions_to_wire(stream_id: usize, sp: &StreamPermissions) -> WireStreamPermissions {
    let topics: Vec<WireTopicPermissions> = sp
        .topics
        .as_ref()
        .map(|map| {
            map.iter()
                .map(|(&tid, tp)| topic_permissions_to_wire(tid, tp))
                .collect()
        })
        .unwrap_or_default();
    WireStreamPermissions {
        stream_id: stream_id as u32,
        manage_stream: sp.manage_stream,
        read_stream: sp.read_stream,
        manage_topics: sp.manage_topics,
        read_topics: sp.read_topics,
        poll_messages: sp.poll_messages,
        send_messages: sp.send_messages,
        topics,
    }
}

fn topic_permissions_to_wire(topic_id: usize, tp: &TopicPermissions) -> WireTopicPermissions {
    WireTopicPermissions {
        topic_id: topic_id as u32,
        manage_topic: tp.manage_topic,
        read_topic: tp.read_topic,
        poll_messages: tp.poll_messages,
        send_messages: tp.send_messages,
    }
}

// -- User Headers conversions --

/// Encode domain user headers into a [`WireUserHeaders`] wrapper.
pub fn user_headers_to_wire(
    headers: &BTreeMap<HeaderKey, HeaderValue>,
) -> iggy_binary_protocol::WireUserHeaders {
    use bytes::{BufMut, BytesMut};
    use iggy_binary_protocol::WireUserHeaders;

    if headers.is_empty() {
        return WireUserHeaders::empty();
    }
    let size: usize = headers
        .iter()
        .map(|(k, v)| 1 + 4 + k.as_bytes().len() + 1 + 4 + v.as_bytes().len())
        .sum();
    let mut buf = BytesMut::with_capacity(size);
    for (key, value) in headers {
        buf.put_u8(key.kind().as_code());
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(key.as_bytes().len() as u32);
        buf.put_slice(key.as_bytes());
        buf.put_u8(value.kind().as_code());
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(value.as_bytes().len() as u32);
        buf.put_slice(value.as_bytes());
    }
    // Buffer was just encoded from valid HeaderKey/HeaderValue entries,
    // so structural TLV validity is guaranteed by construction.
    WireUserHeaders::from_validated(buf.freeze())
}

/// Decode a [`WireUserHeaders`] wrapper into domain user headers.
///
/// Wire-level validation accepts unknown kind codes for forward compatibility
/// (VSR rolling upgrades). Domain-level `from_code()` rejects them - the wire
/// layer stores structurally valid data, the domain layer requires known semantics.
pub fn user_headers_from_wire(
    wire: &iggy_binary_protocol::WireUserHeaders,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, IggyError> {
    if wire.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut headers = BTreeMap::new();
    for entry in wire.iter() {
        let key_kind = HeaderKind::from_code(entry.key_kind.0)?;
        if let Some(expected) = key_kind.expected_size()
            && entry.key.len() != expected
        {
            return Err(IggyError::InvalidHeaderKey);
        }

        let value_kind = HeaderKind::from_code(entry.value_kind.0)?;
        if let Some(expected) = value_kind.expected_size()
            && entry.value.len() != expected
        {
            return Err(IggyError::InvalidHeaderValue);
        }

        headers.insert(
            HeaderKey::new_unchecked(key_kind, entry.key),
            HeaderValue::new_unchecked(value_kind, entry.value),
        );
    }
    Ok(headers)
}
