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

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
};

use bytes::Bytes;
use iggy::prelude::{
    CompressionAlgorithm as RustCompressionAlgorithm,
    ConsumerGroupDetails as RustConsumerGroupDetails, Durability as RustDurability, IdKind,
    Identifier as RustIdentifier, IggyByteSize as RustIggyByteSize, IggyExpiry as RustIggyExpiry,
    IggyMessage as RustIggyMessage, MaxTopicSize as RustMaxTopicSize, OptionKey,
    OptionSpec as RustOptionSpec, Partition as RustPartition,
    PartitionContext as RustPartitionContext, PartitionResizePolicy as RustPartitionResizePolicy,
    PolledMessages as RustPolledMessages,
    SendMessagesConfirmationResponse as RustSendMessagesConfirmationResponse,
    SendMessagesResponse as RustSendMessagesResponse, Stream as RustStream,
    StreamDetails as RustStreamDetails, StreamEncryptionKey,
    StreamEncryptionSource as RustStreamEncryptionSource,
    StreamUpdateOptions as RustStreamUpdateOptions, Topic as RustTopic,
    TopicCreateOptions as RustTopicCreateOptions, TopicDetails as RustTopicDetails,
    TopicUpdateOptions as RustTopicUpdateOptions, UserCreateOptions as RustUserCreateOptions,
    UserUpdateOptions as RustUserUpdateOptions, Validatable,
};
use iggy_binary_protocol::WireUserHeaders;
use iggy_binary_protocol::primitives::metadata_changes::{
    WireMemberLeftReason, WireMetadataChangeGroup, WireMetadataChangeKind, WireMetadataChangePhase,
    WireMetadataChangeStatus, WirePermissionsUpdatedReason, WireSessionClosedReason,
    WireTokenDeletedReason,
};
use iggy_common::{
    CacheMetrics as RustCacheMetrics, CacheMetricsKey as RustCacheMetricsKey,
    ClientInfo as RustClientInfo, ClientInfoDetails as RustClientInfoDetails,
    ClusterMetadata as RustClusterMetadata, ClusterNode as RustClusterNode,
    ConsumerGroup as RustConsumerGroup, ConsumerGroupInfo as RustConsumerGroupInfo,
    ConsumerGroupMember as RustConsumerGroupMember, ConsumerOffsetInfo as RustConsumerOffsetInfo,
    GlobalPermissions as RustGlobalPermissions, HeaderEntry as RustHeaderEntry,
    HeaderField as RustHeaderField, HeaderKind as RustHeaderKind, IdentityInfo as RustIdentityInfo,
    IggyError, IggyTimestamp, MetadataChange, MetadataChangeCursor, MetadataChangeGroup,
    MetadataChangeRecord, MetadataChangeStart, MetadataChanges, MetadataChangesQuery,
    MetadataConsumerGroupCreated, MetadataConsumerGroupDeleted, MetadataConsumerGroupItem,
    MetadataCurrentItem, MetadataCurrentPageToken, MetadataMemberItem, MetadataMemberJoined,
    MetadataMemberLeft, MetadataOptions, MetadataPartitionHandedOver, MetadataPartitionsChanged,
    MetadataPartitionsItem, MetadataSession, MetadataSessionClosed, MetadataStreamChange,
    MetadataStreamDeleted, MetadataTokenChange, MetadataTokenDeleted, MetadataTopicChange,
    MetadataTopicDeleted, MetadataTopicItem, MetadataUserChange, MetadataUserIdentity,
    MetadataUserPermissionsUpdated, Permissions as RustPermissions, Stats as RustStats,
    StreamPermissions as RustStreamPermissions, TopicPermissions as RustTopicPermissions,
    TransportEndpoints as RustTransportEndpoints, UserInfo as RustUserInfo,
    UserInfoDetails as RustUserInfoDetails, UserStatus as RustUserStatus,
};

use crate::ffi;

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

impl From<RustIdentityInfo> for ffi::LoginInfo {
    fn from(identity: RustIdentityInfo) -> Self {
        let has_access_token = identity.access_token.is_some();
        let (access_token, access_token_expiry) = identity
            .access_token
            .map(|token| (token.token, token.expiry))
            .unwrap_or_default();

        ffi::LoginInfo {
            user_id: identity.user_id,
            has_access_token,
            access_token,
            access_token_expiry,
        }
    }
}

impl From<RustUserInfo> for ffi::UserInfo {
    fn from(user: RustUserInfo) -> Self {
        ffi::UserInfo {
            id: user.id,
            created_at: user.created_at.as_micros(),
            status: ffi::UserStatus::from(user.status),
            username: user.username,
            options: resource_options_to_ffi(&user.options, true),
            derived_options: resource_options_to_ffi(&user.options, false),
        }
    }
}

impl From<RustUserInfoDetails> for ffi::UserInfoDetails {
    fn from(user: RustUserInfoDetails) -> Self {
        let has_permissions = user.permissions.is_some();
        ffi::UserInfoDetails {
            id: user.id,
            created_at: user.created_at.as_micros(),
            status: ffi::UserStatus::from(user.status),
            username: user.username,
            has_permissions,
            permissions: ffi::Permissions::from(user.permissions.unwrap_or_default()),
            options: resource_options_to_ffi(&user.options, true),
            derived_options: resource_options_to_ffi(&user.options, false),
        }
    }
}

impl From<RustUserStatus> for ffi::UserStatus {
    fn from(status: RustUserStatus) -> Self {
        match status {
            RustUserStatus::Active => ffi::UserStatus::Active,
            RustUserStatus::Inactive => ffi::UserStatus::Inactive,
        }
    }
}

impl TryFrom<ffi::UserStatus> for RustUserStatus {
    type Error = String;

    fn try_from(status: ffi::UserStatus) -> Result<Self, Self::Error> {
        match status {
            ffi::UserStatus::Active => Ok(RustUserStatus::Active),
            ffi::UserStatus::Inactive => Ok(RustUserStatus::Inactive),
            _ => Err("invalid user status".to_owned()),
        }
    }
}

impl TryFrom<Option<RustUserInfoDetails>> for ffi::UserInfoDetails {
    type Error = String;

    fn try_from(user: Option<RustUserInfoDetails>) -> Result<Self, Self::Error> {
        match user {
            Some(user) => Ok(ffi::UserInfoDetails::from(user)),
            None => Err("user not found".to_string()),
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
            open_files_count: stats.open_files_count,
            open_files_limit: stats.open_files_limit,
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
            replica_id: node.replica_id,
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

impl From<RustGlobalPermissions> for ffi::GlobalPermissions {
    fn from(permissions: RustGlobalPermissions) -> Self {
        ffi::GlobalPermissions {
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

impl From<RustTopicPermissions> for ffi::TopicPermissions {
    fn from(permissions: RustTopicPermissions) -> Self {
        ffi::TopicPermissions {
            manage_topic: permissions.manage_topic,
            read_topic: permissions.read_topic,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
        }
    }
}

impl From<RustStreamPermissions> for ffi::StreamPermissions {
    fn from(permissions: RustStreamPermissions) -> Self {
        ffi::StreamPermissions {
            manage_stream: permissions.manage_stream,
            read_stream: permissions.read_stream,
            manage_topics: permissions.manage_topics,
            read_topics: permissions.read_topics,
            poll_messages: permissions.poll_messages,
            send_messages: permissions.send_messages,
            topics: permissions
                .topics
                .unwrap_or_default()
                .into_iter()
                .map(|(topic_id, permissions)| ffi::TopicPermissionEntry {
                    topic_id: topic_id as u32,
                    permissions: ffi::TopicPermissions::from(permissions),
                })
                .collect(),
        }
    }
}

impl From<RustPermissions> for ffi::Permissions {
    fn from(permissions: RustPermissions) -> Self {
        ffi::Permissions {
            global: ffi::GlobalPermissions::from(permissions.global),
            streams: permissions
                .streams
                .unwrap_or_default()
                .into_iter()
                .map(|(stream_id, permissions)| ffi::StreamPermissionEntry {
                    stream_id: stream_id as u32,
                    permissions: ffi::StreamPermissions::from(permissions),
                })
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

impl From<RustPartitionContext> for ffi::PartitionContext {
    fn from(context: RustPartitionContext) -> Self {
        ffi::PartitionContext {
            incarnation: context.incarnation,
            owner_generation: context.owner_generation,
            metadata_op: context.metadata_op,
        }
    }
}

impl From<ffi::PartitionContext> for RustPartitionContext {
    fn from(context: ffi::PartitionContext) -> Self {
        RustPartitionContext {
            incarnation: context.incarnation,
            owner_generation: context.owner_generation,
            metadata_op: context.metadata_op,
        }
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
            context: partition.context.into(),
        }
    }
}

/// The entries of one provenance, as the `HeaderEntry` the message path uses.
///
/// Options ride the user-headers codec, so they cross the bridge as the type
/// already there for `user_headers` rather than a second one meaning the same
/// thing. Values keep the kind the server sent, so a `Uint64` stays a `Uint64`.
fn resource_options_to_ffi(
    options: &iggy::prelude::ResourceOptions,
    explicit: bool,
) -> Vec<ffi::HeaderEntry> {
    options
        .iter()
        .filter(|(_, option)| option.explicit == explicit)
        .map(|(key, option)| ffi::HeaderEntry {
            key: ffi::HeaderField {
                kind: key.kind().as_code(),
                value: key.as_bytes().to_vec(),
            },
            value: ffi::HeaderField {
                kind: option.value.kind().as_code(),
                value: option.value.as_bytes().to_vec(),
            },
        })
        .collect()
}

/// Render option entries into the string map `TopicCreateOptions::raw` takes.
///
/// The Rust SDK expresses arbitrary option keys as strings that admission
/// parses by the same rules a config file value goes through, so a typed value
/// handed in here is rendered rather than passed through: sending `Uint64`
/// 134217728 for `segment_size` and sending `"134217728"` land the same stored
/// value. A key this build cannot read at all is dropped rather than guessed.
/// String entries skip the header codec so the Rust SDK reports length bounds
/// with the same error categories it uses for its own raw entries.
pub(crate) fn ffi_options_to_raw(
    options: Vec<ffi::HeaderEntry>,
) -> Result<BTreeMap<String, String>, String> {
    let string_kind = RustHeaderKind::String.as_code();
    let mut raw = BTreeMap::new();
    for entry in options {
        let (key, value) = if entry.key.kind == string_kind && entry.value.kind == string_kind {
            let key = String::from_utf8(entry.key.value)
                .map_err(|error| format!("Option key is not a string: {error}"))?;
            let value = String::from_utf8(entry.value.value)
                .map_err(|error| format!("Option value of '{key}' is not a string: {error}"))?;
            (key, value)
        } else {
            let RustHeaderEntry { key, value } = RustHeaderEntry::try_from(entry)?;
            let key = key
                .as_str()
                .map_err(|error| format!("Option key is not a string: {error}"))?
                .to_owned();
            (key, value.to_string_value())
        };
        if raw.insert(key.clone(), value).is_some() {
            return Err(format!("Duplicate option key: {key}"));
        }
    }
    Ok(raw)
}

/// Resolve reset key names into the typed set the Rust SDK validates against
/// the scope's catalog. The C++ wrapper sends only catalog names.
pub(crate) fn ffi_reset_keys(names: Vec<String>) -> Result<BTreeSet<OptionKey>, String> {
    names
        .into_iter()
        .map(|name| {
            OptionKey::from_str(&name).map_err(|_| format!("Unknown option key to reset: {name}"))
        })
        .collect()
}

impl ffi::StreamCreateOptions {
    /// The typed key, refused with `InvalidEncryptionKey` before anything is
    /// sent. The error never carries key material.
    pub(crate) fn encryption_key(&self) -> Result<Option<StreamEncryptionKey>, IggyError> {
        if self.has_encryption_key {
            return StreamEncryptionKey::try_from(self.encryption_key.as_slice()).map(Some);
        }
        if self.has_encryption_key_base64 {
            return StreamEncryptionKey::from_str(&self.encryption_key_base64).map(Some);
        }
        Ok(None)
    }
}

fn parse_message_expiry(kind: &str, value: u64) -> Result<RustIggyExpiry, String> {
    match kind {
        "server_default" | "default" => Ok(RustIggyExpiry::ServerDefault),
        "never_expire" => Ok(RustIggyExpiry::NeverExpire),
        "duration" if value == 0 => {
            Err("message expiry duration must be greater than zero".to_string())
        }
        "duration" => Ok(RustIggyExpiry::ExpireDuration(
            iggy::prelude::IggyDuration::from(value),
        )),
        _ => Err(format!("invalid message expiry kind '{kind}'")),
    }
}

fn parse_compression_algorithm(value: &str) -> Result<RustCompressionAlgorithm, String> {
    RustCompressionAlgorithm::from_str(value)
        .map_err(|error| format!("invalid compression algorithm '{value}': {error}"))
}

fn parse_user_status(
    has_status: bool,
    status: ffi::UserStatus,
) -> Result<Option<RustUserStatus>, String> {
    has_status
        .then(|| RustUserStatus::try_from(status))
        .transpose()
}

fn parse_max_topic_size(value: &str) -> Result<RustMaxTopicSize, String> {
    RustMaxTopicSize::from_str(value)
        .map_err(|error| format!("invalid max topic size '{value}': {error}"))
}

impl From<RustOptionSpec> for ffi::OptionSpec {
    fn from(spec: RustOptionSpec) -> Self {
        ffi::OptionSpec {
            key: spec.key,
            kind: spec.kind.as_code(),
            default_value: spec.default_value,
            description: spec.description,
            updatable: spec.updatable,
            resettable: spec.resettable,
            secret: spec.secret,
            read_only: spec.read_only,
            critical: spec.critical,
        }
    }
}

impl TryFrom<ffi::TopicCreateOptions> for RustTopicCreateOptions {
    type Error = String;

    fn try_from(options: ffi::TopicCreateOptions) -> Result<Self, Self::Error> {
        let compression_algorithm = if options.has_compression_algorithm {
            Some(parse_compression_algorithm(&options.compression_algorithm)?)
        } else {
            None
        };
        let message_expiry = if options.has_message_expiry {
            Some(parse_message_expiry(
                &options.message_expiry_kind,
                options.message_expiry_value,
            )?)
        } else {
            None
        };
        let max_topic_size = if options.has_max_topic_size {
            Some(parse_max_topic_size(&options.max_topic_size)?)
        } else {
            None
        };
        let durability = if options.has_durability {
            RustDurability::from_str(&options.durability)
                .map_err(|error| format!("invalid durability '{}': {error}", options.durability))?
        } else {
            RustDurability::default()
        };
        let consumer_offset_durability = if options.has_consumer_offset_durability {
            RustDurability::from_str(&options.consumer_offset_durability).map_err(|error| {
                format!(
                    "invalid consumer offset durability '{}': {error}",
                    options.consumer_offset_durability
                )
            })?
        } else {
            RustDurability::default()
        };
        let partition_resize_policy = if options.has_partition_resize_policy {
            Some(
                RustPartitionResizePolicy::from_str(&options.partition_resize_policy).map_err(
                    |error| {
                        format!(
                            "invalid partition resize policy '{}': {error}",
                            options.partition_resize_policy
                        )
                    },
                )?,
            )
        } else {
            None
        };
        let mut raw = ffi_options_to_raw(options.raw_options)?;
        if options.has_durability {
            raw.remove("durability");
        }
        if options.has_consumer_offset_durability {
            raw.remove("consumer_offset_durability");
        }
        Ok(RustTopicCreateOptions {
            partitions_count: if options.has_partitions_count {
                Some(options.partitions_count)
            } else {
                None
            },
            compression_algorithm,
            message_expiry,
            max_topic_size,
            segment_size: if options.has_segment_size {
                Some(RustIggyByteSize::from(options.segment_size))
            } else {
                None
            },
            durability,
            consumer_offset_durability,
            partition_resize_policy,
            messages_required_to_save: if options.has_messages_required_to_save {
                Some(options.messages_required_to_save)
            } else {
                None
            },
            size_of_messages_required_to_save: if options.has_size_of_messages_required_to_save {
                Some(RustIggyByteSize::from(
                    options.size_of_messages_required_to_save,
                ))
            } else {
                None
            },
            preallocate_segments: if options.has_preallocate_segments {
                Some(options.preallocate_segments)
            } else {
                None
            },
            raw,
        })
    }
}

impl TryFrom<ffi::TopicUpdateOptions> for RustTopicUpdateOptions {
    type Error = String;

    fn try_from(options: ffi::TopicUpdateOptions) -> Result<Self, Self::Error> {
        let message_expiry = if options.has_message_expiry {
            Some(parse_message_expiry(
                &options.message_expiry_kind,
                options.message_expiry_value,
            )?)
        } else {
            None
        };
        let max_topic_size = if options.has_max_topic_size {
            Some(parse_max_topic_size(&options.max_topic_size)?)
        } else {
            None
        };
        Ok(RustTopicUpdateOptions {
            name: options.has_name.then_some(options.name),
            message_expiry,
            max_topic_size,
            reset: ffi_reset_keys(options.reset)?,
            raw_reset: options.raw_reset.into_iter().collect(),
            raw: ffi_options_to_raw(options.raw_options)?,
        })
    }
}

impl TryFrom<ffi::StreamUpdateOptions> for RustStreamUpdateOptions {
    type Error = String;

    fn try_from(options: ffi::StreamUpdateOptions) -> Result<Self, Self::Error> {
        Ok(RustStreamUpdateOptions {
            name: options.has_name.then_some(options.name),
            reset: ffi_reset_keys(options.reset)?,
            raw_reset: options.raw_reset.into_iter().collect(),
            raw: ffi_options_to_raw(options.raw_options)?,
        })
    }
}

impl TryFrom<ffi::UserCreateOptions> for RustUserCreateOptions {
    type Error = String;

    fn try_from(options: ffi::UserCreateOptions) -> Result<Self, Self::Error> {
        Ok(RustUserCreateOptions {
            raw: ffi_options_to_raw(options.raw_options)?,
        })
    }
}

impl TryFrom<ffi::UserUpdateOptions> for RustUserUpdateOptions {
    type Error = String;

    fn try_from(options: ffi::UserUpdateOptions) -> Result<Self, Self::Error> {
        Ok(RustUserUpdateOptions {
            username: options.has_username.then_some(options.username),
            status: parse_user_status(options.has_status, options.status)?,
            reset: ffi_reset_keys(options.reset)?,
            raw_reset: options.raw_reset.into_iter().collect(),
            raw: ffi_options_to_raw(options.raw_options)?,
        })
    }
}

impl From<RustStreamEncryptionSource> for ffi::StreamEncryptionSource {
    fn from(source: RustStreamEncryptionSource) -> Self {
        match source {
            RustStreamEncryptionSource::None => ffi::StreamEncryptionSource::None,
            RustStreamEncryptionSource::Stream => ffi::StreamEncryptionSource::Stream,
            RustStreamEncryptionSource::Server => ffi::StreamEncryptionSource::Server,
        }
    }
}

impl From<RustTopic> for ffi::Topic {
    fn from(topic: RustTopic) -> Self {
        let resolved = topic.resolved_options();
        ffi::Topic {
            id: topic.id,
            created_at: topic.created_at.as_micros(),
            name: topic.name,
            size_bytes: topic.size.as_bytes_u64(),
            message_expiry: u64::from(resolved.message_expiry),
            compression_algorithm: resolved.compression_algorithm.to_string(),
            max_topic_size: u64::from(resolved.max_topic_size),
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            options: resource_options_to_ffi(&topic.options, true),
            derived_options: resource_options_to_ffi(&topic.options, false),
        }
    }
}

impl From<RustTopicDetails> for ffi::TopicDetails {
    fn from(topic: RustTopicDetails) -> Self {
        let resolved = topic.resolved_options();
        ffi::TopicDetails {
            id: topic.id,
            created_at: topic.created_at.as_micros(),
            name: topic.name,
            size_bytes: topic.size.as_bytes_u64(),
            message_expiry: u64::from(resolved.message_expiry),
            compression_algorithm: resolved.compression_algorithm.to_string(),
            max_topic_size: u64::from(resolved.max_topic_size),
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            partitions: topic
                .partitions
                .into_iter()
                .map(ffi::Partition::from)
                .collect(),
            options: resource_options_to_ffi(&topic.options, true),
            derived_options: resource_options_to_ffi(&topic.options, false),
        }
    }
}

impl From<RustStream> for ffi::Stream {
    fn from(stream: RustStream) -> Self {
        let source = stream.encryption_key_source();
        let fingerprint = stream.encryption_key_fingerprint();
        let kek_fingerprints = stream.encryption_kek_fingerprints();
        ffi::Stream {
            id: stream.id,
            created_at: stream.created_at.as_micros(),
            name: stream.name,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            options: resource_options_to_ffi(&stream.options, true),
            derived_options: resource_options_to_ffi(&stream.options, false),
            has_encryption_key_source: source.is_some(),
            encryption_key_source: source.map_or(ffi::StreamEncryptionSource::None, Into::into),
            has_encryption_key_fingerprint: fingerprint.is_some(),
            encryption_key_fingerprint: fingerprint.unwrap_or_default(),
            encryption_kek_fingerprints: kek_fingerprints,
        }
    }
}

impl From<RustStreamDetails> for ffi::StreamDetails {
    fn from(stream: RustStreamDetails) -> Self {
        let source = stream.encryption_key_source();
        let fingerprint = stream.encryption_key_fingerprint();
        let kek_fingerprints = stream.encryption_kek_fingerprints();
        ffi::StreamDetails {
            id: stream.id,
            created_at: stream.created_at.as_micros(),
            name: stream.name,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            topics_count: stream.topics_count,
            topics: stream.topics.into_iter().map(ffi::Topic::from).collect(),
            options: resource_options_to_ffi(&stream.options, true),
            derived_options: resource_options_to_ffi(&stream.options, false),
            has_encryption_key_source: source.is_some(),
            encryption_key_source: source.map_or(ffi::StreamEncryptionSource::None, Into::into),
            has_encryption_key_fingerprint: fingerprint.is_some(),
            encryption_key_fingerprint: fingerprint.unwrap_or_default(),
            encryption_kek_fingerprints: kek_fingerprints,
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
        // TODO: Document in the C++ SDK that user headers are unordered and keys must be unique.
        // The BTreeMap sorts entries by kind and value, discards Vec insertion order on send and
        // poll, and rejects duplicate keys.
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
        RustIggyMessage::builder()
            .id(id)
            .payload(payload)
            .maybe_user_headers((!user_headers.is_empty()).then_some(user_headers))
            .build()
            .map_err(|error| format!("Could not convert message: {error}"))
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
            context: messages.context.into(),
        }
    }
}

impl From<RustSendMessagesConfirmationResponse> for ffi::SendMessagesConfirmation {
    fn from(confirmation: RustSendMessagesConfirmationResponse) -> Self {
        ffi::SendMessagesConfirmation {
            stream_id: confirmation.stream_id,
            topic_id: confirmation.topic_id,
            partition_id: confirmation.partition_id,
            base_offset: confirmation.base_offset,
        }
    }
}

impl From<RustSendMessagesResponse> for ffi::SendMessagesResponse {
    fn from(response: RustSendMessagesResponse) -> Self {
        ffi::SendMessagesResponse {
            confirmations: response
                .confirmations
                .into_iter()
                .map(ffi::SendMessagesConfirmation::from)
                .collect(),
        }
    }
}

impl TryFrom<ffi::MetadataChangesQuery> for MetadataChangesQuery {
    type Error = IggyError;

    fn try_from(query: ffi::MetadataChangesQuery) -> Result<Self, Self::Error> {
        let timestamp =
            |micros| IggyTimestamp::checked_from_micros(micros).ok_or(IggyError::InvalidCommand);
        let start = match query.start {
            ffi::MetadataChangeStart::Earliest => MetadataChangeStart::Earliest,
            ffi::MetadataChangeStart::Latest => MetadataChangeStart::Latest,
            ffi::MetadataChangeStart::After => {
                MetadataChangeStart::After(MetadataChangeCursor::from_str(&query.cursor)?)
            }
            ffi::MetadataChangeStart::Current if query.page_token.is_empty() => {
                MetadataChangeStart::Current(None)
            }
            ffi::MetadataChangeStart::Current => MetadataChangeStart::Current(Some(
                MetadataCurrentPageToken::from_str(&query.page_token)?,
            )),
            _ => return Err(IggyError::InvalidCommand),
        };
        let groups = query
            .groups
            .iter()
            .map(|&code| {
                WireMetadataChangeGroup::from_code(code)
                    .map(MetadataChangeGroup::from)
                    .map_err(|_| IggyError::InvalidCommand)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            start,
            groups,
            since: query
                .has_since
                .then(|| timestamp(query.since))
                .transpose()?,
            until: query
                .has_until
                .then(|| timestamp(query.until))
                .transpose()?,
            limit: query.limit,
        })
    }
}

impl From<MetadataChangeCursor> for ffi::MetadataChangeCursor {
    fn from(cursor: MetadataChangeCursor) -> Self {
        ffi::MetadataChangeCursor {
            log_id_lo: cursor.log_id as u64,
            log_id_hi: (cursor.log_id >> 64) as u64,
            op: cursor.op,
            checksum_lo: cursor.checksum as u64,
            checksum_hi: (cursor.checksum >> 64) as u64,
            ordinal: cursor.ordinal,
            text: cursor.to_string(),
        }
    }
}

impl From<MetadataCurrentPageToken> for ffi::MetadataCurrentPageToken {
    fn from(token: MetadataCurrentPageToken) -> Self {
        ffi::MetadataCurrentPageToken {
            text: token.to_string(),
            anchor: token.anchor().into(),
            required_head: token.required_head(),
            groups_mask: token.groups().mask(),
            last_group: WireMetadataChangeGroup::from(token.last_group()).as_u8(),
            last_key: token.last_key().to_vec(),
        }
    }
}

impl From<MetadataChanges> for ffi::MetadataChanges {
    fn from(changes: MetadataChanges) -> Self {
        let log_id = changes.log_id.unwrap_or_default();
        ffi::MetadataChanges {
            status: WireMetadataChangeStatus::from(changes.status).as_u8(),
            phase: WireMetadataChangePhase::from(changes.phase).as_u8(),
            has_log_id: changes.log_id.is_some(),
            log_id_lo: log_id as u64,
            log_id_hi: (log_id >> 64) as u64,
            oldest_op: changes.oldest_op,
            retained_since: changes.retained_since,
            head_op: changes.head_op,
            has_next: changes.next.is_some(),
            next: changes.next.map(Into::into).unwrap_or_default(),
            has_more: changes.has_more,
            records: changes
                .records
                .into_iter()
                .map(ffi::MetadataChangeRecord::from)
                .collect(),
            items: changes
                .items
                .into_iter()
                .map(ffi::MetadataCurrentItem::from)
                .collect(),
            has_current_page_token: changes.current_page_token.is_some(),
            current_page_token: changes
                .current_page_token
                .map(Into::into)
                .unwrap_or_default(),
        }
    }
}

impl From<MetadataChangeRecord> for ffi::MetadataChangeRecord {
    fn from(record: MetadataChangeRecord) -> Self {
        let mut converted = ffi::MetadataChangeRecord {
            op: record.op,
            ordinal: record.ordinal,
            checksum_lo: record.checksum as u64,
            checksum_hi: (record.checksum >> 64) as u64,
            timestamp: record.timestamp,
            has_actor_user_id: record.actor_user_id.is_some(),
            actor_user_id: record.actor_user_id.unwrap_or_default(),
            form: ffi::MetadataChangeForm::Known,
            group: 0,
            kind: 0,
            group_name: String::new(),
            kind_name: String::new(),
            body: ffi::MetadataChangeBody::default(),
        };
        if let Some(kind) = record.change.kind() {
            converted.kind = WireMetadataChangeKind::from(kind).as_u8();
            converted.group = WireMetadataChangeGroup::from(kind.group()).as_u8();
        }
        converted.body = match record.change {
            MetadataChange::StreamCreated(body) | MetadataChange::StreamUpdated(body) => {
                body.into()
            }
            MetadataChange::StreamDeleted(body) => body.into(),
            MetadataChange::TopicCreated(body) | MetadataChange::TopicUpdated(body) => body.into(),
            MetadataChange::TopicDeleted(body) => body.into(),
            MetadataChange::PartitionsChanged(body) => body.into(),
            MetadataChange::ConsumerGroupCreated(body) => body.into(),
            MetadataChange::ConsumerGroupDeleted(body) => body.into(),
            MetadataChange::ConsumerGroupMemberJoined(body) => body.into(),
            MetadataChange::ConsumerGroupMemberLeft(body) => body.into(),
            MetadataChange::ConsumerGroupPartitionHandedOver(body) => body.into(),
            MetadataChange::UserCreated(body) | MetadataChange::UserUpdated(body) => body.into(),
            MetadataChange::UserDeleted(body) | MetadataChange::UserPasswordChanged(body) => {
                body.into()
            }
            MetadataChange::UserPermissionsUpdated(body) => body.into(),
            MetadataChange::PersonalAccessTokenCreated(body) => body.into(),
            MetadataChange::PersonalAccessTokenDeleted(body) => body.into(),
            MetadataChange::SessionRegistered(body) => body.into(),
            MetadataChange::SessionClosed(body) => body.into(),
            MetadataChange::Unknown { group, kind, body } => {
                converted.form = ffi::MetadataChangeForm::Unknown;
                converted.group = group;
                converted.kind = kind;
                ffi::MetadataChangeBody {
                    unknown_body: body.to_vec(),
                    ..ffi::MetadataChangeBody::default()
                }
            }
            MetadataChange::UnknownJson { group, kind, body } => {
                converted.form = ffi::MetadataChangeForm::UnknownJson;
                converted.group_name = group;
                converted.kind_name = kind;
                ffi::MetadataChangeBody {
                    unknown_body: body.get().as_bytes().to_vec(),
                    ..ffi::MetadataChangeBody::default()
                }
            }
        };
        converted
    }
}

impl From<MetadataCurrentItem> for ffi::MetadataCurrentItem {
    fn from(item: MetadataCurrentItem) -> Self {
        let group = item
            .group()
            .map_or(0, |group| WireMetadataChangeGroup::from(group).as_u8());
        let known = |body: ffi::MetadataChangeBody| ffi::MetadataCurrentItem {
            form: ffi::MetadataChangeForm::Known,
            group,
            group_name: String::new(),
            body,
        };
        match item {
            MetadataCurrentItem::Stream(body) => known(body.into()),
            MetadataCurrentItem::Topic(body) => known(body.into()),
            MetadataCurrentItem::Partitions(body) => known(body.into()),
            MetadataCurrentItem::ConsumerGroup(body) => known(body.into()),
            MetadataCurrentItem::ConsumerGroupMember(body) => known(body.into()),
            MetadataCurrentItem::User(body) => known(body.into()),
            MetadataCurrentItem::PersonalAccessToken(body) => known(body.into()),
            MetadataCurrentItem::Session(body) => known(body.into()),
            MetadataCurrentItem::Unknown { group, body } => ffi::MetadataCurrentItem {
                form: ffi::MetadataChangeForm::Unknown,
                group,
                group_name: String::new(),
                body: ffi::MetadataChangeBody {
                    unknown_body: body.to_vec(),
                    ..ffi::MetadataChangeBody::default()
                },
            },
            MetadataCurrentItem::UnknownJson { group, body } => ffi::MetadataCurrentItem {
                form: ffi::MetadataChangeForm::UnknownJson,
                group: 0,
                group_name: group,
                body: ffi::MetadataChangeBody {
                    unknown_body: body.get().as_bytes().to_vec(),
                    ..ffi::MetadataChangeBody::default()
                },
            },
        }
    }
}

impl Default for ffi::MetadataChangeBody {
    fn default() -> Self {
        ffi::MetadataChangeBody {
            stream_id: 0,
            stream_created_op: 0,
            topic_id: 0,
            topic_created_op: 0,
            group_id: 0,
            has_member_id: false,
            member_id: 0,
            partition_id: 0,
            created_at: 0,
            name: String::new(),
            partitions_count: 0,
            consumer_groups_count: 0,
            members_count: 0,
            client_id_lo: 0,
            client_id_hi: 0,
            partition_ids: Vec::new(),
            has_requested_op: false,
            requested_op: 0,
            reason: 0,
            user_id: 0,
            user_created_at: 0,
            status: ffi::UserStatus::Active,
            has_permissions: false,
            permissions: ffi::Permissions::from(RustPermissions::default()),
            options_readable: false,
            options: Vec::new(),
            derived_options: Vec::new(),
            has_expiry: false,
            expiry: 0,
            session: 0,
            unknown_body: Vec::new(),
        }
    }
}

impl From<MetadataOptions> for ffi::MetadataChangeBody {
    fn from(options: MetadataOptions) -> Self {
        match options {
            MetadataOptions::Readable(options) => ffi::MetadataChangeBody {
                options_readable: true,
                options: resource_options_to_ffi(&options, true),
                derived_options: resource_options_to_ffi(&options, false),
                ..ffi::MetadataChangeBody::default()
            },
            MetadataOptions::Unreadable => ffi::MetadataChangeBody::default(),
        }
    }
}

impl From<MetadataStreamChange> for ffi::MetadataChangeBody {
    fn from(body: MetadataStreamChange) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            created_at: body.created_at,
            name: body.name,
            ..body.options.into()
        }
    }
}

impl From<MetadataStreamDeleted> for ffi::MetadataChangeBody {
    fn from(body: MetadataStreamDeleted) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            created_at: body.created_at,
            name: body.name,
            has_requested_op: body.requested_op.is_some(),
            requested_op: body.requested_op.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataTopicChange> for ffi::MetadataChangeBody {
    fn from(body: MetadataTopicChange) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            partitions_count: body.partitions_count,
            ..body.options.into()
        }
    }
}

impl From<MetadataTopicItem> for ffi::MetadataChangeBody {
    fn from(body: MetadataTopicItem) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            ..body.options.into()
        }
    }
}

impl From<MetadataTopicDeleted> for ffi::MetadataChangeBody {
    fn from(body: MetadataTopicDeleted) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            created_at: body.created_at,
            name: body.name,
            has_requested_op: body.requested_op.is_some(),
            requested_op: body.requested_op.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataPartitionsChanged> for ffi::MetadataChangeBody {
    fn from(body: MetadataPartitionsChanged) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            partitions_count: body.partitions_count,
            partition_ids: body.removed_partition_ids,
            has_requested_op: body.requested_op.is_some(),
            requested_op: body.requested_op.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataPartitionsItem> for ffi::MetadataChangeBody {
    fn from(body: MetadataPartitionsItem) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            partitions_count: body.partitions_count,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataConsumerGroupCreated> for ffi::MetadataChangeBody {
    fn from(body: MetadataConsumerGroupCreated) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
            consumer_groups_count: body.consumer_groups_count,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataConsumerGroupDeleted> for ffi::MetadataChangeBody {
    fn from(body: MetadataConsumerGroupDeleted) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
            consumer_groups_count: body.consumer_groups_count,
            has_requested_op: body.requested_op.is_some(),
            requested_op: body.requested_op.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataConsumerGroupItem> for ffi::MetadataChangeBody {
    fn from(body: MetadataConsumerGroupItem) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            name: body.name,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataMemberJoined> for ffi::MetadataChangeBody {
    fn from(body: MetadataMemberJoined) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            has_member_id: true,
            member_id: body.member_id,
            client_id_lo: body.client_id as u64,
            client_id_hi: (body.client_id >> 64) as u64,
            members_count: body.members_count,
            partition_ids: body.partition_ids,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataMemberLeft> for ffi::MetadataChangeBody {
    fn from(body: MetadataMemberLeft) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            has_member_id: true,
            member_id: body.member_id,
            client_id_lo: body.client_id as u64,
            client_id_hi: (body.client_id >> 64) as u64,
            members_count: body.members_count,
            reason: WireMemberLeftReason::from(body.reason).as_u8(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataPartitionHandedOver> for ffi::MetadataChangeBody {
    fn from(body: MetadataPartitionHandedOver) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            partition_id: body.partition_id,
            has_member_id: body.member_id.is_some(),
            member_id: body.member_id.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataMemberItem> for ffi::MetadataChangeBody {
    fn from(body: MetadataMemberItem) -> Self {
        ffi::MetadataChangeBody {
            stream_id: body.stream_id,
            stream_created_op: body.stream_created_op,
            topic_id: body.topic_id,
            topic_created_op: body.topic_created_op,
            group_id: body.group_id,
            has_member_id: true,
            member_id: body.member_id,
            client_id_lo: body.client_id as u64,
            client_id_hi: (body.client_id >> 64) as u64,
            partition_ids: body.partition_ids,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataUserChange> for ffi::MetadataChangeBody {
    fn from(body: MetadataUserChange) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.username,
            status: body.status.into(),
            has_permissions: body.permissions.is_some(),
            permissions: ffi::Permissions::from(body.permissions.unwrap_or_default()),
            ..body.options.into()
        }
    }
}

impl From<MetadataUserIdentity> for ffi::MetadataChangeBody {
    fn from(body: MetadataUserIdentity) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.username,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataUserPermissionsUpdated> for ffi::MetadataChangeBody {
    fn from(body: MetadataUserPermissionsUpdated) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.username,
            has_permissions: body.permissions.is_some(),
            permissions: ffi::Permissions::from(body.permissions.unwrap_or_default()),
            reason: WirePermissionsUpdatedReason::from(body.reason).as_u8(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataTokenChange> for ffi::MetadataChangeBody {
    fn from(body: MetadataTokenChange) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.name,
            has_expiry: body.expiry.is_some(),
            expiry: body.expiry.unwrap_or_default(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataTokenDeleted> for ffi::MetadataChangeBody {
    fn from(body: MetadataTokenDeleted) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            name: body.name,
            reason: WireTokenDeletedReason::from(body.reason).as_u8(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataSession> for ffi::MetadataChangeBody {
    fn from(body: MetadataSession) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            client_id_lo: body.client_id as u64,
            client_id_hi: (body.client_id >> 64) as u64,
            session: body.session,
            ..ffi::MetadataChangeBody::default()
        }
    }
}

impl From<MetadataSessionClosed> for ffi::MetadataChangeBody {
    fn from(body: MetadataSessionClosed) -> Self {
        ffi::MetadataChangeBody {
            user_id: body.user_id,
            user_created_at: body.user_created_at,
            client_id_lo: body.client_id as u64,
            client_id_hi: (body.client_id >> 64) as u64,
            session: body.session,
            reason: WireSessionClosedReason::from(body.reason).as_u8(),
            ..ffi::MetadataChangeBody::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{HeaderKey, HeaderValue, IggyTimestamp, OptionValue, ResourceOptions};

    // Distinct values so a swapped field fails the comparison.
    const CONTEXT: RustPartitionContext = RustPartitionContext {
        incarnation: 3,
        owner_generation: 5,
        metadata_op: 7,
    };

    fn fields(context: &ffi::PartitionContext) -> (u64, u64, u64) {
        (
            context.incarnation,
            context.owner_generation,
            context.metadata_op,
        )
    }

    #[test]
    fn given_optional_bridge_fields_when_converting_should_preserve_presence() {
        let unnamed = RustStreamUpdateOptions::try_from(ffi::StreamUpdateOptions {
            has_name: false,
            name: "ignored".to_owned(),
            reset: vec!["name".to_owned()],
            raw_reset: Vec::new(),
            raw_options: Vec::new(),
        })
        .expect("stream update");
        assert_eq!(
            unnamed,
            RustStreamUpdateOptions {
                reset: BTreeSet::from([OptionKey::Name]),
                ..RustStreamUpdateOptions::default()
            }
        );
        let empty_name = RustStreamUpdateOptions::try_from(ffi::StreamUpdateOptions {
            has_name: true,
            name: String::new(),
            reset: Vec::new(),
            raw_reset: Vec::new(),
            raw_options: Vec::new(),
        })
        .expect("stream update");
        assert_eq!(empty_name.name, Some(String::new()));

        let topic = RustTopicUpdateOptions::try_from(ffi::TopicUpdateOptions {
            has_name: false,
            name: "ignored".to_owned(),
            has_message_expiry: false,
            message_expiry_kind: "duration".to_owned(),
            message_expiry_value: 5,
            has_max_topic_size: false,
            max_topic_size: "unlimited".to_owned(),
            reset: vec!["max_topic_size".to_owned()],
            raw_reset: Vec::new(),
            raw_options: Vec::new(),
        })
        .expect("topic update");
        assert_eq!(
            topic,
            RustTopicUpdateOptions {
                reset: BTreeSet::from([OptionKey::MaxTopicSize]),
                ..RustTopicUpdateOptions::default()
            }
        );

        let user = RustUserUpdateOptions::try_from(ffi::UserUpdateOptions {
            has_username: false,
            username: "ignored".to_owned(),
            has_status: true,
            status: ffi::UserStatus::Inactive,
            reset: Vec::new(),
            raw_reset: Vec::new(),
            raw_options: Vec::new(),
        })
        .expect("user update");
        assert_eq!(
            user,
            RustUserUpdateOptions {
                status: Some(RustUserStatus::Inactive),
                ..RustUserUpdateOptions::default()
            }
        );
        assert!(
            RustUserUpdateOptions::try_from(ffi::UserUpdateOptions {
                has_username: false,
                username: String::new(),
                has_status: false,
                status: ffi::UserStatus { repr: 0 },
                reset: vec!["not_a_catalog_key".to_owned()],
                raw_reset: Vec::new(),
                raw_options: Vec::new(),
            })
            .is_err()
        );

        let key: Vec<u8> = (0..32).collect();
        let keyless = ffi::StreamCreateOptions {
            has_encryption_key: false,
            encryption_key: key.clone(),
            has_encryption_key_base64: false,
            encryption_key_base64: String::new(),
            raw_options: Vec::new(),
        };
        assert_eq!(keyless.encryption_key(), Ok(None));
        let keyed = ffi::StreamCreateOptions {
            has_encryption_key: true,
            ..keyless
        };
        assert_eq!(
            keyed
                .encryption_key()
                .map(|key| key.map(|key| key.fingerprint())),
            Ok(Some("8441e38538cbfa57".to_owned()))
        );
        let short = ffi::StreamCreateOptions {
            encryption_key: key[..31].to_vec(),
            ..keyed
        };
        assert_eq!(short.encryption_key(), Err(IggyError::InvalidEncryptionKey));
    }

    #[test]
    fn given_raw_reset_names_when_converting_should_leave_them_to_the_server() {
        let topic = RustTopicUpdateOptions::try_from(ffi::TopicUpdateOptions {
            has_name: false,
            name: String::new(),
            has_message_expiry: false,
            message_expiry_kind: String::new(),
            message_expiry_value: 0,
            has_max_topic_size: false,
            max_topic_size: String::new(),
            reset: vec!["max_topic_size".to_owned()],
            raw_reset: vec!["future_key".to_owned()],
            raw_options: Vec::new(),
        })
        .expect("topic update");
        assert_eq!(
            topic,
            RustTopicUpdateOptions {
                reset: BTreeSet::from([OptionKey::MaxTopicSize]),
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                ..RustTopicUpdateOptions::default()
            }
        );
    }

    #[test]
    fn given_raw_entry_outside_codec_bounds_when_converting_should_keep_the_sdk_error_category() {
        let entry = |key: &str, value: &str| ffi::HeaderEntry {
            key: ffi::HeaderField {
                kind: RustHeaderKind::String.as_code(),
                value: key.as_bytes().to_vec(),
            },
            value: ffi::HeaderField {
                kind: RustHeaderKind::String.as_code(),
                value: value.as_bytes().to_vec(),
            },
        };
        let patch = |raw_options: Vec<ffi::HeaderEntry>| {
            RustStreamUpdateOptions::try_from(ffi::StreamUpdateOptions {
                has_name: false,
                name: String::new(),
                reset: Vec::new(),
                raw_reset: Vec::new(),
                raw_options,
            })
            .map(|options| options.to_patch().map(|_| ()))
        };
        let long_key = "k".repeat(256);

        assert!(matches!(
            patch(vec![entry("future_key", "")]),
            Ok(Err(IggyError::InvalidOptionValue(_)))
        ));
        assert!(matches!(
            patch(vec![entry(&long_key, "value")]),
            Ok(Err(IggyError::UnsupportedOptionKey(_)))
        ));
    }

    #[test]
    fn given_stream_projection_when_converting_should_split_provenance() {
        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::EncryptionKeySource),
            OptionValue::derived(HeaderValue::from_str("none").expect("source")),
        );
        let stream = ffi::StreamDetails::from(RustStreamDetails {
            id: 1,
            created_at: IggyTimestamp::from(2),
            name: "plain".to_owned(),
            size: RustIggyByteSize::from(0),
            messages_count: 0,
            topics_count: 0,
            topics: Vec::new(),
            options,
        });

        assert!(stream.options.is_empty());
        assert_eq!(stream.derived_options.len(), 1);
        assert!(stream.has_encryption_key_source);
        assert!(stream.encryption_key_source == ffi::StreamEncryptionSource::None);
        assert!(!stream.has_encryption_key_fingerprint);
        assert!(stream.encryption_kek_fingerprints.is_empty());
    }

    #[test]
    fn should_reject_zero_message_expiry_duration() {
        assert_eq!(
            parse_message_expiry("duration", 0).err().as_deref(),
            Some("message expiry duration must be greater than zero")
        );
    }

    #[test]
    fn should_keep_every_partition_context_field_across_the_bridge() {
        let bridged = ffi::PartitionContext::from(CONTEXT);

        assert_eq!(fields(&bridged), (3, 5, 7));
        assert_eq!(RustPartitionContext::from(bridged), CONTEXT);
    }

    #[test]
    fn should_keep_the_partition_context_of_a_topic_partition() {
        let partition = RustPartition {
            id: 1,
            created_at: IggyTimestamp::from(11),
            segments_count: 1,
            current_offset: 2,
            size: RustIggyByteSize::from(13),
            messages_count: 3,
            context: CONTEXT,
        };

        assert_eq!(fields(&ffi::Partition::from(partition).context), (3, 5, 7));
    }

    #[test]
    fn should_keep_the_partition_context_of_polled_messages() {
        let polled = RustPolledMessages {
            context: CONTEXT,
            partition_id: 1,
            current_offset: 2,
            count: 0,
            messages: Vec::new(),
        };

        assert_eq!(
            fields(&ffi::PolledMessages::from(polled).context),
            (3, 5, 7)
        );
    }

    #[test]
    fn given_json_record_of_unknown_kind_when_converting_should_keep_names_and_the_whole_object() {
        let json = r#"{"op":5,"ordinal":1,"checksum":"0000000000000000000000000000002a","timestamp":7,"group":"future_group","kind":"future_kind","extra":true}"#;
        let record: MetadataChangeRecord = serde_json::from_str(json).expect("record parses");

        let converted = ffi::MetadataChangeRecord::from(record);

        assert!(converted.form == ffi::MetadataChangeForm::UnknownJson);
        assert_eq!(
            (converted.group_name.as_str(), converted.kind_name.as_str()),
            ("future_group", "future_kind")
        );
        assert_eq!((converted.op, converted.ordinal), (5, 1));
        assert_eq!((converted.checksum_lo, converted.checksum_hi), (42, 0));
        assert!(!converted.has_actor_user_id);
        assert_eq!(converted.body.unknown_body, json.as_bytes());
    }

    #[test]
    fn given_json_item_of_unknown_group_when_converting_should_keep_the_whole_object() {
        let json = r#"{"group":"future_group","count":3}"#;
        let item: MetadataCurrentItem = serde_json::from_str(json).expect("item parses");

        let converted = ffi::MetadataCurrentItem::from(item);

        assert!(converted.form == ffi::MetadataChangeForm::UnknownJson);
        assert_eq!(converted.group_name, "future_group");
        assert_eq!(converted.body.unknown_body, json.as_bytes());
    }

    #[test]
    fn given_bridge_query_when_converting_should_map_every_start_mode_and_refuse_unknown_codes() {
        let query = |start: ffi::MetadataChangeStart, groups: Vec<u8>| ffi::MetadataChangesQuery {
            start,
            cursor: String::new(),
            page_token: String::new(),
            groups,
            has_since: false,
            since: 9,
            has_until: true,
            until: 11,
            limit: 7,
        };

        let current =
            MetadataChangesQuery::try_from(query(ffi::MetadataChangeStart::Current, vec![1]))
                .expect("current query");
        assert_eq!(
            current,
            MetadataChangesQuery {
                start: MetadataChangeStart::Current(None),
                groups: vec![MetadataChangeGroup::Streams],
                since: None,
                until: Some(IggyTimestamp::from(11)),
                limit: 7,
            }
        );
        assert_eq!(
            MetadataChangesQuery::try_from(query(ffi::MetadataChangeStart { repr: 4 }, Vec::new())),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            MetadataChangesQuery::try_from(query(ffi::MetadataChangeStart::Earliest, vec![9])),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            MetadataChangesQuery::try_from(query(ffi::MetadataChangeStart::After, Vec::new())),
            Err(IggyError::InvalidCommand)
        );
    }
}
