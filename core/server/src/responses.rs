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

//! Wire-response builders for the non-replicated read path.
//!
//! Assemble `get_me` / `get_clients` / `get_stream(s)` / `get_topic(s)` /
//! `get_user(s)` / `get_personal_access_tokens` / stats / cluster-metadata
//! responses from per-shard session state and the metadata state machine, plus the
//! `NonReplicatedResponse` dispatch shim.

use crate::cluster_meta::ClusterRoster;
use crate::namespace::ensure_topic_exists;
use crate::reply_frame::{build_empty_reply, build_reply_from_bytes};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::sysinfo_probe::{probe_system_stats, stats_disk_space};
use crate::wire::{transport_kind_to_wire, usize_to_u32};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, FLUSH_UNSAVED_BUFFER_CODE, GET_CLUSTER_METADATA_CODE,
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
    GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::personal_access_tokens::GetPersonalAccessTokensRequest;
use iggy_binary_protocol::requests::streams::{GetStreamRequest, GetStreamsRequest};
use iggy_binary_protocol::requests::system::{
    DescribeOptionsRequest, OPTIONS_SCOPE_STREAM, OPTIONS_SCOPE_TOPIC, OPTIONS_SCOPE_USER,
};
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::GetUserRequest;
use iggy_binary_protocol::responses::clients::client_response::ClientResponse;
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::get_personal_access_tokens::{
    GetPersonalAccessTokensResponse, PersonalAccessTokenResponse,
};
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{
    GetStreamResponse, TopicHeader as StreamTopicHeader,
};
use iggy_binary_protocol::responses::streams::get_streams::GetStreamsResponse;
use iggy_binary_protocol::responses::system::get_cluster_metadata::{
    ClusterMetadataResponse, ClusterNodeResponse,
};
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use iggy_binary_protocol::responses::system::{DescribeOptionsResponse, OptionDescriptor};
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{
    ReplyHeader, RoutedRequestHeader, WireDecode, WireEncode, WireIdentifier, WireName,
};
use iggy_common::wire_conversions::{resource_options_to_wire, resource_options_to_wire_split};
use iggy_common::{HeaderKind, IggyError, IggyTimestamp, OptionsProvenance, topic_option_keys};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use server_common::Message;
use shard::ConnectedClientInfo;
use std::cell::RefCell;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;

/// Build the `get_me` reply for the requesting connection. Identity
/// (`user_id`, transport kind, peer address) comes from the per-shard
/// [`SessionManager`]; the `consumer_groups` list is read from the
/// (replicated) consumer-group STM by the connection's bound VSR client id.
pub fn build_get_personal_access_tokens_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) -> GetPersonalAccessTokensResponse
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // PATs are per-user; list the requesting connection's own tokens, resolved
    // from this shard's `SessionManager` (like `get_me`) then read out of the
    // replicated Users STM.
    let Some(user_id) = sessions.borrow().get_user_id(transport_client_id) else {
        return GetPersonalAccessTokensResponse { tokens: Vec::new() };
    };
    shard.plane.metadata().mux_stm.users().read(|users| {
        let tokens = users
            .personal_access_tokens
            .get(&user_id)
            .map(|pats| {
                pats.values()
                    .filter_map(|pat| {
                        Some(PersonalAccessTokenResponse {
                            name: WireName::new(pat.name.as_ref()).ok()?,
                            expiry_at: pat.expiry_at.map_or(0, |expiry| expiry.as_micros()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        GetPersonalAccessTokensResponse { tokens }
    })
}

pub fn build_get_me_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) -> ClientDetailsResponse
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let mut client = sessions
        .borrow()
        .client_record(transport_client_id)
        .map_or_else(
            || {
                // No session record (shouldn't happen on an auth-gated
                // read). Report the connection id with the "no user"
                // sentinel + TCP default rather than impersonating root
                // (user id 0 is a real user; server is 0-based).
                #[allow(clippy::cast_possible_truncation)]
                ClientResponse {
                    client_id: transport_client_id as u32,
                    user_id: u32::MAX,
                    transport: 1,
                    address: String::new(),
                    consumer_groups_count: 0,
                }
            },
            |record| connected_client_to_response(shard, &record),
        );

    // The wire `consumer_groups` list keys off the connection's bound VSR
    // client id (the same id recorded as a group member by the replicated
    // Join op), not the transport id.
    let consumer_groups = sessions
        .borrow()
        .get_session(transport_client_id)
        .map(|(vsr_client_id, _)| {
            shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .consumer_group_memberships(vsr_client_id)
        })
        .unwrap_or_default()
        .into_iter()
        .map(
            |(stream_id, topic_id, group_id)| ConsumerGroupInfoResponse {
                stream_id,
                topic_id,
                group_id,
            },
        )
        .collect::<Vec<_>>();

    #[allow(clippy::cast_possible_truncation)]
    {
        client.consumer_groups_count = consumer_groups.len() as u32;
    }
    ClientDetailsResponse {
        client,
        consumer_groups,
    }
}

/// Convert a [`ConnectedClientInfo`] (one connected client, from the local
/// `SessionManager` or a `get_clients` gather) into the wire
/// [`ClientResponse`]. Shared by `get_me`, `get_clients`, and `get_client`.
///
/// `consumer_groups_count` is resolved from the connection's bound VSR client
/// id against the replicated `Streams` STM (memberships are keyed by VSR id, not
/// transport id). Connections that never bound (pre-register) count 0.
pub fn connected_client_to_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    info: &ConnectedClientInfo,
) -> ClientResponse
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let consumer_groups_count = info.vsr_client_id.map_or(0, |vsr_client_id| {
        #[allow(clippy::cast_possible_truncation)]
        let count = shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_memberships(vsr_client_id)
            .len() as u32;
        count
    });
    // The transport client id is a u128 `(shard << 112) | seq`; the wire
    // `client_id` is the u32 seq tail.
    #[allow(clippy::cast_possible_truncation)]
    ClientResponse {
        client_id: info.client_id as u32,
        user_id: info.user_id.unwrap_or(u32::MAX),
        transport: transport_kind_to_wire(info.transport),
        address: info.address.to_string(),
        consumer_groups_count,
    }
}

/// `user_id` is the authenticated caller, used only by the identity-scoped
/// reads (currently the PAT list); every other arm ignores it. Authorization
/// stays with the per-transport gates that run before this builder. `client_ip`
/// is the caller's transport-level peer address, used only by the
/// cluster-metadata read to pick each node's advertised address; `None`
/// degrades to the catch-all address. `clients_count` is the cross-shard
/// connected-client total, used only by the stats read: it comes from the async
/// `ListClients` scatter-gather, which this sync builder cannot run, so both
/// transport callers gather it up front (0 for every other opcode).
pub fn build_non_replicated_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    code: u32,
    body: &[u8],
    user_id: Option<u32>,
    roster: &ClusterRoster,
    client_ip: Option<IpAddr>,
    clients_count: u32,
) -> Result<NonReplicatedResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    match code {
        DESCRIBE_OPTIONS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_describe_options_response(body)?.to_bytes(),
        )),
        GET_CLUSTER_METADATA_CODE => Ok(NonReplicatedResponse::Bytes(
            build_cluster_metadata_response(roster, shard, client_ip).to_bytes(),
        )),
        GET_STATS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_stats_response(shard, clients_count)?.to_bytes(),
        )),
        GET_STREAM_CODE => {
            let request =
                GetStreamRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_stream_response(shard, &request.stream_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_STREAMS_CODE => {
            let _ = GetStreamsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            Ok(NonReplicatedResponse::Bytes(
                build_get_streams_response(shard)?.to_bytes(),
            ))
        }
        GET_TOPIC_CODE => {
            let request =
                GetTopicRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_topic_response(shard, &request.stream_id, &request.topic_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_TOPICS_CODE => {
            let request =
                GetTopicsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            Ok(NonReplicatedResponse::Bytes(
                build_get_topics_response(shard, &request.stream_id)?.to_bytes(),
            ))
        }
        GET_USERS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_users_response(shard)?.to_bytes(),
        )),
        GET_USER_CODE => {
            let request =
                GetUserRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_user_response(shard, &request.user_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_PERSONAL_ACCESS_TOKENS_CODE => {
            let _ = GetPersonalAccessTokensRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            // Caller-scoped: both transport gates reject unauthenticated
            // callers before this read runs, so a missing id is a gate
            // bug; fail closed rather than serve another scope.
            let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
            let tokens = shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.personal_access_tokens_of(user_id));
            Ok(NonReplicatedResponse::Bytes(
                personal_access_tokens_response(tokens)?.to_bytes(),
            ))
        }
        GET_CONSUMER_GROUP_CODE => build_consumer_group_response(shard, body),
        GET_CONSUMER_GROUPS_CODE => build_consumer_groups_response(shard, body),
        // The server has no on-demand flush primitive, so it denies honestly.
        // The non-replicated catch-all's empty-ok would otherwise attest a
        // durability guarantee the server never gave.
        FLUSH_UNSAVED_BUFFER_CODE => Err(IggyError::FeatureUnavailable),
        // Snapshot collection blocks on shell-outs, so the dedicated dispatch
        // and HTTP handlers await it off-thread; this synchronous builder
        // cannot, and reaching it here is a routing bug. Fail closed rather
        // than let the catch-all's empty-ok attest an artifact that was never
        // produced.
        GET_SNAPSHOT_FILE_CODE => Err(IggyError::InvalidCommand),
        // Sequenced AFTER the named arms above, so flush keeps answering
        // `FeatureUnavailable`. A table-listed non-replicated code with no arm
        // is a routing bug and an unknown code is a client bug; the empty-ok
        // that used to cover both attested a read that never ran. Only the
        // named arms return `Empty`, and there it means "resolved to nothing"
        // (the 404 the HTTP path maps).
        _ => match iggy_binary_protocol::dispatch::lookup_command(code) {
            Some(meta) if meta.is_replicated() => Err(IggyError::FeatureUnavailable),
            _ => Err(IggyError::InvalidCommand),
        },
    }
}

fn build_consumer_group_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
) -> Result<NonReplicatedResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request =
        GetConsumerGroupRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    ensure_topic_exists(shard, &request.stream_id, &request.topic_id)?;
    let response = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_group_details(&request.stream_id, &request.topic_id, &request.group_id);
    Ok(response.map_or(NonReplicatedResponse::Empty, |response| {
        NonReplicatedResponse::Bytes(response.to_bytes())
    }))
}

fn build_consumer_groups_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
) -> Result<NonReplicatedResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request =
        GetConsumerGroupsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    ensure_topic_exists(shard, &request.stream_id, &request.topic_id)?;
    let groups = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_group_list(&request.stream_id, &request.topic_id);
    Ok(groups.map_or(NonReplicatedResponse::Empty, |groups| {
        NonReplicatedResponse::Bytes(GetConsumerGroupsResponse { groups }.to_bytes())
    }))
}

/// Build the binary `GetClusterMetadata` reply from the shared roster assembly.
/// The leader marking comes from this shard's consensus view; a shard without
/// consensus (any shard but 0) still serves the full roster, only with no node
/// marked leader.
fn build_cluster_metadata_response<B, MJ, S, SB>(
    roster: &ClusterRoster,
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    client_ip: Option<IpAddr>,
) -> ClusterMetadataResponse
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    // Shard 0 reads its live consensus; delegated shards use the view shard 0
    // publishes into the roster, so leader marking works on every shard.
    let primary_index = shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .and_then(|consensus| {
            let primary_index = consensus.primary_index(consensus.view());
            // A restarted replica that ceded the primaryship its stale view
            // assigns it must not advertise itself as leader: clients would
            // pin to a node that never heartbeats. Report "no leader" until
            // the election resolves the role.
            (!(consensus.has_ceded_primaryship() && primary_index == consensus.replica()))
                .then_some(primary_index)
        })
        .or_else(|| roster.current_primary_replica_id());
    let metadata = roster.cluster_metadata(primary_index, client_ip);
    ClusterMetadataResponse {
        name: metadata.name,
        nodes: metadata
            .nodes
            .into_iter()
            .map(|node| ClusterNodeResponse {
                name: node.name,
                ip: node.ip,
                tcp_port: node.endpoints.tcp,
                quic_port: node.endpoints.quic,
                http_port: node.endpoints.http,
                websocket_port: node.endpoints.websocket,
                role: node.role as u8,
                status: node.status as u8,
            })
            .collect(),
    }
}

/// `(streams, topics, partitions, segments, message bytes, messages)` for the
/// whole node, from committed metadata plus the shared stats registry.
///
/// Segments are summed PER PARTITION through the same floor the detail
/// responses apply (see [`partition_response`]), not from the stream's rolled-up
/// counter: that counter only advances once a partition materialises, which
/// trails its commit by a reconciler pass. Summing it made `[stats]` report
/// fewer segments than `get_topic` did for the same partitions, and let the
/// total climb between two reads with no write in between.
fn aggregate_stats_totals(
    streams: &metadata::stm::stream::StreamsInner,
) -> Result<(u32, u32, u32, u32, u64, u64), IggyError> {
    let mut topics_count = 0u32;
    let mut partitions_count = 0u32;
    let mut segments_count = 0u32;
    let mut messages_size_bytes = 0u64;
    let mut messages_count = 0u64;
    for (_, stream) in &streams.items {
        topics_count = topics_count.saturating_add(usize_to_u32(stream.topics.len())?);
        messages_size_bytes =
            messages_size_bytes.saturating_add(stream.stats.size_bytes_inconsistent());
        messages_count = messages_count.saturating_add(stream.stats.messages_count_inconsistent());
        for (_, topic) in &stream.topics {
            partitions_count =
                partitions_count.saturating_add(usize_to_u32(topic.partitions.len())?);
            for partition in &topic.partitions {
                segments_count = segments_count.saturating_add(partition_segments_count(
                    streams,
                    stream.id,
                    topic.id,
                    partition.id,
                ));
            }
        }
    }
    Ok((
        usize_to_u32(streams.items.len())?,
        topics_count,
        partitions_count,
        segments_count,
        messages_size_bytes,
        messages_count,
    ))
}

fn build_stats_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    clients_count: u32,
) -> Result<StatsResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let (
        streams_count,
        topics_count,
        partitions_count,
        segments_count,
        messages_size_bytes,
        messages_count,
    ) = shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(aggregate_stats_totals)?;
    let consumer_groups_count = usize_to_u32(
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_count(),
    )?;

    let system = probe_system_stats();
    let (free_disk_space, total_disk_space) = stats_disk_space();
    Ok(StatsResponse {
        process_id: system.process_id,
        cpu_usage: system.cpu_usage,
        total_cpu_usage: system.total_cpu_usage,
        memory_usage: system.memory_usage,
        total_memory: system.total_memory,
        available_memory: system.available_memory,
        run_time: system.run_time,
        start_time: system.start_time,
        read_bytes: system.read_bytes,
        written_bytes: system.written_bytes,
        messages_size_bytes,
        streams_count,
        topics_count,
        partitions_count,
        segments_count,
        messages_count,
        clients_count,
        consumer_groups_count,
        hostname: system.hostname,
        os_name: system.os_name,
        os_version: system.os_version,
        kernel_version: system.kernel_version,
        iggy_server_version: crate::VERSION.to_owned(),
        iggy_server_semver: crate::SEMANTIC_VERSION.get_numeric_version().ok(),
        cache_metrics: Vec::new(),
        threads_count: system.threads_count,
        free_disk_space,
        total_disk_space,
    })
}

fn build_get_stream_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
) -> Result<Option<GetStreamResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = streams.resolve_stream_id(stream_id) else {
            return Ok(None);
        };
        let stream = streams
            .items
            .get(stream_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(GetStreamResponse {
            stream: stream_response(stream)?,
            topics: stream
                .topics
                .iter()
                .map(|(_, topic)| topic_header(topic))
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_streams_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
) -> Result<GetStreamsResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        streams
            .items
            .iter()
            .map(|(_, stream)| stream_response(stream))
            .collect::<Result<Vec<_>, _>>()
            .map(|streams| GetStreamsResponse { streams })
    })
}

/// Every key `CreateTopic` accepts, with the kind, default and bounds of each.
///
/// Split out of [`build_describe_options_response`] so the descriptions have room
/// to state the bounds each value is checked against: this catalog is the only
/// place an operator learns them.
///
/// Every default is a build constant: these knobs stopped being config-derived
/// when the `[system.*]` keys became topic options, so the catalog reads them
/// straight from `iggy_common`.
fn topic_option_descriptors() -> Result<Vec<OptionDescriptor>, IggyError> {
    Ok(vec![
        OptionDescriptor {
            key: WireName::new(topic_option_keys::COMPRESSION_ALGORITHM)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::String.as_code(),
            default_value: Bytes::from_static(b"none"),
            description: "Compression algorithm (none, gzip)".to_string(),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::MESSAGE_EXPIRY)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Uint64.as_code(),
            default_value: Bytes::copy_from_slice(
                &iggy_common::DEFAULT_MESSAGE_EXPIRY.to_le_bytes(),
            ),
            description: "Message expiry in microseconds, or a humantime string \
                              (e.g. 7 days)"
                .to_string(),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::MAX_TOPIC_SIZE)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Uint64.as_code(),
            default_value: Bytes::copy_from_slice(
                &iggy_common::DEFAULT_MAX_TOPIC_SIZE.to_le_bytes(),
            ),
            description: "Topic size cap in bytes, or a byte-size string (e.g. 1 GiB); \
                              must be at least the segment size"
                .to_string(),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::SEGMENT_SIZE)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Uint64.as_code(),
            default_value: Bytes::copy_from_slice(&iggy_common::DEFAULT_SEGMENT_SIZE.to_le_bytes()),
            description: format!(
                "Segment size in bytes, or a byte-size string (e.g. 128 MiB); a 512-byte \
                     multiple within {}..={}",
                iggy_common::MIN_TOPIC_SEGMENT_SIZE,
                iggy_common::MAX_TOPIC_SEGMENT_SIZE
            ),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::ENFORCE_FSYNC)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Bool.as_code(),
            default_value: Bytes::copy_from_slice(&[u8::from(iggy_common::DEFAULT_ENFORCE_FSYNC)]),
            description: "Whether writes to this topic's partitions fsync".to_string(),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::MESSAGES_REQUIRED_TO_SAVE)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Uint32.as_code(),
            default_value: Bytes::copy_from_slice(
                &iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE.to_le_bytes(),
            ),
            description: format!(
                "Flush the journal once it holds this many messages; \
                     1..={}. A threshold no segment can reach leaves committed \
                     messages in the journal, which a crash does not preserve",
                iggy_common::MAX_MESSAGES_REQUIRED_TO_SAVE
            ),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::SIZE_OF_MESSAGES_REQUIRED_TO_SAVE)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Uint64.as_code(),
            default_value: Bytes::copy_from_slice(
                &iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE.to_le_bytes(),
            ),
            description: format!(
                "Flush the journal once it holds this many bytes, or a byte-size \
                     string; whichever threshold trips first flushes. At most {}",
                iggy_common::MAX_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE
            ),
        },
        OptionDescriptor {
            key: WireName::new(topic_option_keys::PREALLOCATE_SEGMENTS)
                .map_err(|_| IggyError::InvalidFormat)?,
            kind: HeaderKind::Bool.as_code(),
            default_value: Bytes::copy_from_slice(&[u8::from(
                iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
            )]),
            description: format!(
                "Reserve each segment's bytes up front where the filesystem supports \
                     it; pairs with segment_size. The reservation is real disk and runs \
                     inline on the owning shard, at every rotation and once per owned \
                     partition at boot, so segment_size * partitions_count is capped at \
                     {} bytes",
                iggy_common::MAX_PREALLOCATED_TOPIC_BYTES
            ),
        },
    ])
}

/// Serve the option catalog for one resource scope.
///
/// Streams and users have no catalog keys yet, so their scopes return empty
/// (every key is rejected at create until one lands).
fn build_describe_options_response(body: &[u8]) -> Result<DescribeOptionsResponse, IggyError> {
    let request =
        DescribeOptionsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let entries = match request.scope {
        OPTIONS_SCOPE_TOPIC => topic_option_descriptors()?,
        OPTIONS_SCOPE_STREAM | OPTIONS_SCOPE_USER => Vec::new(),
        _ => return Err(IggyError::InvalidCommand),
    };
    Ok(DescribeOptionsResponse { entries })
}

#[allow(clippy::cast_possible_truncation)]
fn user_response(user: &metadata::stm::user::User) -> Result<UserResponse, IggyError> {
    Ok(UserResponse {
        id: user.id,
        created_at: user.created_at.as_micros(),
        status: user.status.as_code(),
        username: WireName::new(user.username.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
        options: resource_options_to_wire(&user.options, OptionsProvenance::Explicit)?,
    })
}

fn build_get_users_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
) -> Result<GetUsersResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.users().read(|users| {
        users
            .items
            .iter()
            .map(|(_, user)| user_response(user))
            .collect::<Result<Vec<_>, _>>()
            .map(|users| GetUsersResponse { users })
    })
}

fn build_get_user_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    user_id: &WireIdentifier,
) -> Result<Option<UserDetailsResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.users().read(|users| {
        let Some(id) = users.resolve_user_id(user_id) else {
            return Ok(None);
        };
        let user = users.items.get(id).ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(UserDetailsResponse {
            user: user_response(user)?,
            permissions: user
                .permissions
                .as_ref()
                .map(|p| iggy_common::wire_conversions::permissions_to_wire(p)),
        }))
    })
}

fn personal_access_tokens_response(
    tokens: Vec<(Arc<str>, Option<IggyTimestamp>)>,
) -> Result<GetPersonalAccessTokensResponse, IggyError> {
    let tokens = tokens
        .into_iter()
        .map(|(name, expiry_at)| {
            Ok(PersonalAccessTokenResponse {
                name: WireName::new(name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
                // 0 is the wire encoding for a never-expiring token, matching
                // the legacy handler and the SDK-side decode.
                expiry_at: expiry_at.map_or(0, |expiry_at| expiry_at.as_micros()),
            })
        })
        .collect::<Result<Vec<_>, IggyError>>()?;
    Ok(GetPersonalAccessTokensResponse { tokens })
}

fn build_get_topic_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<Option<GetTopicResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = streams.resolve_stream_id(stream_id) else {
            return Ok(None);
        };
        let Some(topic_id) = streams.resolve_topic_id(stream_id, topic_id) else {
            return Ok(None);
        };
        let stream = streams
            .items
            .get(stream_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        let topic = stream
            .topics
            .get(topic_id)
            .ok_or(IggyError::InvalidIdentifier)?;
        Ok(Some(GetTopicResponse {
            topic: topic_header(topic)?,
            partitions: topic
                .partitions
                .iter()
                .map(|partition| partition_response(streams, stream_id, topic_id, partition))
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_topics_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
) -> Result<GetTopicsResponse, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        // Legacy parity: a missing stream lists as empty, not StreamNotFound.
        let Some(resolved_stream) = streams.resolve_stream_id(stream_id) else {
            return Ok(GetTopicsResponse { topics: Vec::new() });
        };
        let stream = streams
            .items
            .get(resolved_stream)
            .ok_or(IggyError::InvalidIdentifier)?;
        stream
            .topics
            .iter()
            .map(|(_, topic)| topic_header(topic))
            .collect::<Result<Vec<_>, _>>()
            .map(|topics| GetTopicsResponse { topics })
    })
}

fn stream_response(stream: &metadata::stm::stream::Stream) -> Result<StreamResponse, IggyError> {
    Ok(StreamResponse {
        id: usize_to_u32(stream.id)?,
        created_at: stream.created_at.as_micros(),
        topics_count: usize_to_u32(stream.topics.len())?,
        size_bytes: stream.stats.size_bytes_inconsistent(),
        messages_count: stream.stats.messages_count_inconsistent(),
        name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
        options: resource_options_to_wire(&stream.options, OptionsProvenance::Explicit)?,
    })
}

/// Stored `message_expiry` and `max_topic_size` echo verbatim, `ServerDefault`
/// as the wire sentinel (0), matching legacy: create admission resolves the
/// sentinels against server config before replication, so a stored sentinel
/// came from an update and must read back as `ServerDefault`, not as the node
/// default frozen at read time.
fn topic_header(topic: &metadata::stm::stream::Topic) -> Result<StreamTopicHeader, IggyError> {
    let (options, derived_options) = resource_options_to_wire_split(&topic.options)?;
    Ok(StreamTopicHeader {
        id: usize_to_u32(topic.id)?,
        created_at: topic.created_at.as_micros(),
        partitions_count: usize_to_u32(topic.partitions.len())?,
        message_expiry: u64::from(topic.message_expiry),
        compression_algorithm: topic.compression_algorithm.as_code(),
        max_topic_size: topic.max_topic_size.as_bytes_u64(),
        size_bytes: topic.stats.size_bytes_inconsistent(),
        messages_count: topic.stats.messages_count_inconsistent(),
        name: WireName::new(topic.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
        options,
        derived_options,
    })
}

/// Segments a committed partition reports before its storage exists.
/// [`partition_response`] carries the reasoning.
const MATERIALIZED_SEGMENTS_FLOOR: u32 = 1;

/// Segments one committed partition reports. The single source for every
/// client-facing segment count, so the `[stats]` total and the per-partition
/// detail cannot disagree about the same partition.
fn partition_segments_count(
    streams: &metadata::stm::stream::StreamsInner,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
) -> u32 {
    streams
        .stats_registry
        .partition_get(stream_id, topic_id, partition_id)
        .map_or(MATERIALIZED_SEGMENTS_FLOOR, |stats| {
            stats
                .segments_count_inconsistent()
                .max(MATERIALIZED_SEGMENTS_FLOOR)
        })
}

fn partition_response(
    streams: &metadata::stm::stream::StreamsInner,
    stream_id: usize,
    topic_id: usize,
    partition: &metadata::stm::stream::Partition,
) -> Result<PartitionResponse, IggyError> {
    // Per-partition counters live in the shared stats registry (one `Arc`
    // across all shards and both left-right buffers).
    //
    // Registration is NOT materialization: the owning shard's reconciler mints
    // the entry (get-or-create in `fetch_partition_stats`) before it builds the
    // partition, and `ensure_initial_segment` only bumps `segments_count` once
    // the segment file is open. So a registry MISS and a registered entry still
    // reading zero segments are the same thing to a caller -- committed, not yet
    // holding storage -- and both report the deterministic shape every
    // materialization lands on: one empty segment at offset 0. A bare zero
    // would read as "no storage" to a client polling right after `create_topic`.
    //
    // Cost of the clamp: a partition fenced for rebuild (tombstoned after a
    // refused chain) also reads as one empty segment rather than zero. Telling
    // the two apart needs a materialization signal the registry does not carry
    // today; the counters are still the honest source for size and messages.
    // TODO(hubcio): carry that materialization signal in the stats registry
    // (segment planted vs fenced-for-rebuild) so monitoring can see a real
    // zero-segment partition instead of this clamp.
    let stats = streams
        .stats_registry
        .partition_get(stream_id, topic_id, partition.id);
    let (current_offset, size_bytes, messages_count) = stats.map_or((0, 0, 0), |stats| {
        (
            stats.current_offset(),
            stats.size_bytes_inconsistent(),
            stats.messages_count_inconsistent(),
        )
    });
    Ok(PartitionResponse {
        id: usize_to_u32(partition.id)?,
        created_at: partition.created_at.as_micros(),
        segments_count: partition_segments_count(streams, stream_id, topic_id, partition.id),
        current_offset,
        size_bytes,
        messages_count,
    })
}

pub enum NonReplicatedResponse {
    Empty,
    Bytes(Bytes),
}

impl NonReplicatedResponse {
    pub(crate) fn into_reply(
        self,
        request_header: &RoutedRequestHeader,
        client_id: u128,
        session: u64,
        commit: u64,
    ) -> Message<ReplyHeader> {
        match self {
            Self::Empty => build_empty_reply(request_header, client_id, session, commit),
            Self::Bytes(body) => {
                build_reply_from_bytes(request_header, client_id, session, commit, &body)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn personal_access_tokens_response_preserves_order_and_encodes_never_as_zero() {
        let expiry = IggyTimestamp::from(123_456u64);
        let tokens: Vec<(Arc<str>, Option<IggyTimestamp>)> = vec![
            (Arc::from("alpha"), Some(expiry)),
            (Arc::from("zeta"), None),
        ];

        let response = personal_access_tokens_response(tokens).expect("mapping succeeds");

        assert_eq!(response.tokens.len(), 2);
        assert_eq!(response.tokens[0].name.as_str(), "alpha");
        assert_eq!(response.tokens[0].expiry_at, expiry.as_micros());
        assert_eq!(response.tokens[1].name.as_str(), "zeta");
        assert_eq!(response.tokens[1].expiry_at, 0);
    }

    #[test]
    fn personal_access_tokens_response_encodes_empty_list_as_empty_body() {
        let response = personal_access_tokens_response(Vec::new()).expect("mapping succeeds");
        // An empty body is the wire shape the SDK decodes as "no tokens"; it
        // must stay `Bytes` (not the not-found `Empty` variant) end to end.
        assert!(response.to_bytes().is_empty());
    }

    #[test]
    fn partition_response_reports_the_initial_shape_until_a_segment_exists() {
        use iggy_common::{StreamStats, TopicStats};
        use metadata::stm::stream::{Partition, StreamsInner};

        let streams = StreamsInner::new();
        let partition = Partition::new(0, 1, IggyTimestamp::from(1u64), 0, 0);

        // Registry miss: the owning shard has not started building.
        let predicted = partition_response(&streams, 0, 0, &partition).expect("response builds");
        assert_eq!(predicted.segments_count, 1);
        assert_eq!(predicted.messages_count, 0);

        // Registered but not yet segmented: the reconciler mints the entry
        // before `ensure_initial_segment` runs, so this is the SAME state to a
        // caller and must not read as "no storage".
        let topic_stats = Arc::new(TopicStats::new(Arc::new(StreamStats::default())));
        let stats = streams.stats_registry.partition(0, 0, 0, topic_stats);
        let mid_build = partition_response(&streams, 0, 0, &partition).expect("response builds");
        assert_eq!(mid_build.segments_count, 1);

        // Materialized: the real counters answer from here on.
        stats.increment_segments_count(1);
        stats.increment_messages_count(7);
        stats.increment_size_bytes(64);
        let live = partition_response(&streams, 0, 0, &partition).expect("response builds");
        assert_eq!(live.segments_count, 1);
        assert_eq!(live.messages_count, 7);
        assert_eq!(live.size_bytes, 64);
    }

    #[test]
    fn stats_totals_count_every_committed_partition_before_it_materialises() {
        use iggy_common::{StreamStats, TopicStats};
        use metadata::stm::stream::{Partition, Stream, StreamsInner, Topic};
        use std::sync::atomic::AtomicUsize;

        let created_at = IggyTimestamp::from(1u64);
        let mut streams = StreamsInner::new();
        let mut stream = Stream::new(Arc::from("stream"), created_at);
        let topic_stats = Arc::new(TopicStats::new(stream.stats.clone()));
        stream.topics.insert(Topic {
            id: 0,
            name: Arc::from("topic"),
            created_at,
            message_expiry: iggy_common::IggyExpiry::NeverExpire,
            compression_algorithm: iggy_common::CompressionAlgorithm::None,
            max_topic_size: iggy_common::MaxTopicSize::Unlimited,
            options: iggy_common::ResourceOptions::default(),
            stats: topic_stats.clone(),
            partitions: vec![
                Partition::new(0, 1, created_at, 0, 0),
                Partition::new(1, 1, created_at, 0, 0),
            ],
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: ahash::AHashMap::default(),
            consumer_group_index: ahash::AHashMap::default(),
            next_consumer_group_id: 0,
        });
        streams.items.insert(stream);

        // Only partition 0 has materialised. Counting the stream's rolled-up
        // counter reported 1 here, so a caller polling `[stats]` twice saw the
        // total climb to 2 with no write in between (and `get_topic` already
        // reported 2 for the same partitions).
        let stats = streams.stats_registry.partition(0, 0, 0, topic_stats);
        stats.increment_segments_count(1);

        let (_, _, partitions, segments, _, _) =
            aggregate_stats_totals(&streams).expect("totals aggregate");
        assert_eq!(partitions, 2);
        assert_eq!(
            segments, 2,
            "an unmaterialised partition must contribute the same floor the detail response reports"
        );

        // Materialising the second partition changes nothing: the total was
        // already the steady-state answer.
        let late = streams.stats_registry.partition(
            0,
            0,
            1,
            Arc::new(TopicStats::new(Arc::new(StreamStats::default()))),
        );
        late.increment_segments_count(1);
        let (_, _, _, segments_after, _, _) =
            aggregate_stats_totals(&streams).expect("totals aggregate");
        assert_eq!(segments_after, 2);
    }

    #[test]
    fn topic_header_echoes_stored_size_and_expiry_verbatim() {
        use iggy_common::{
            CompressionAlgorithm, IggyDuration, IggyExpiry, MaxTopicSize, ResourceOptions,
            StreamStats, TopicStats,
        };
        use std::sync::atomic::AtomicUsize;

        let parent = Arc::new(StreamStats::default());
        let topic_with = |max_topic_size, message_expiry| metadata::stm::stream::Topic {
            id: 0,
            name: Arc::from("topic"),
            created_at: IggyTimestamp::from(1u64),
            message_expiry,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size,
            options: ResourceOptions::default(),
            stats: Arc::new(TopicStats::new(parent.clone())),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: ahash::AHashMap::default(),
            consumer_group_index: ahash::AHashMap::default(),
            next_consumer_group_id: 0,
        };

        // Stored `ServerDefault` sentinels echo the wire sentinel (0)
        // verbatim, so an update to `ServerDefault` reads back as
        // `ServerDefault` instead of the node default frozen at read time.
        let sentinel = topic_header(&topic_with(
            MaxTopicSize::ServerDefault,
            IggyExpiry::ServerDefault,
        ))
        .expect("topic header builds");
        assert_eq!(sentinel.max_topic_size, 0);
        assert_eq!(sentinel.message_expiry, 0);

        // Explicit values round-trip unchanged.
        let custom = topic_header(&topic_with(
            MaxTopicSize::from(1024u64),
            IggyExpiry::ExpireDuration(IggyDuration::from(5_000_000u64)),
        ))
        .expect("topic header builds");
        assert_eq!(custom.max_topic_size, 1024);
        assert_eq!(custom.message_expiry, 5_000_000);
        let unlimited = topic_header(&topic_with(
            MaxTopicSize::Unlimited,
            IggyExpiry::NeverExpire,
        ))
        .expect("topic header builds");
        assert_eq!(unlimited.max_topic_size, u64::MAX);
        assert_eq!(unlimited.message_expiry, u64::MAX);
    }
}
