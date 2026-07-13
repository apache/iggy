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
//! [`NonReplicatedResponse`] dispatch shim and the partition-namespace
//! resolvers.

use crate::bootstrap::ServerNgShard;
use crate::cluster_meta::ClusterRoster;
use crate::session_manager::SessionManager;
use crate::wire::{transport_kind_to_wire, usize_to_u32};
use bytes::{BufMut, Bytes, BytesMut};
use consensus::{MetadataHandle, VsrConsensus};
use iggy_binary_protocol::codes::{
    FLUSH_UNSAVED_BUFFER_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_GROUP_CODE,
    GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_SNAPSHOT_FILE_CODE,
    GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
    GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::consensus::{RESULT_COUNT_LEN, result_code};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, DeleteConsumerOffsetRequest, StoreConsumerOffset2Request,
    StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::SendMessagesHeader;
use iggy_binary_protocol::requests::personal_access_tokens::GetPersonalAccessTokensRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
use iggy_binary_protocol::requests::streams::{GetStreamRequest, GetStreamsRequest};
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::GetUserRequest;
use iggy_binary_protocol::responses::clients::client_response::ClientResponse;
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::GetConsumerGroupsResponse;
use iggy_binary_protocol::responses::personal_access_tokens::RawPersonalAccessTokenResponse;
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
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{
    Command2, GenericHeader, IGGY_PROTOCOL_VERSION, KIND_CONSUMER_GROUP, Operation, ReplyHeader,
    RequestHeader, WireDecode, WireEncode, WireIdentifier, WireName, WirePartitioning,
};
use iggy_common::{EncryptorKind, Identifier, IggyError, IggyTimestamp, MaxTopicSize};
use metadata::impls::metadata::StreamsFrontend;
use partitions::PollFragments;
use server_common::Message;
use server_common::send_messages2::{COMMAND_HEADER_SIZE, SendMessages2Header};
use server_common::sharding::IggyNamespace;
use shard::ConnectedClientInfo;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use sysinfo::System as SysinfoSystem;
use system_stats::SystemProbe;

/// Build the `get_me` reply for the requesting connection. Identity
/// (`user_id`, transport kind, peer address) comes from the per-shard
/// [`SessionManager`]; the `consumer_groups` list is read from the
/// (replicated) consumer-group STM by the connection's bound VSR client id.
pub(crate) fn build_get_personal_access_tokens_response(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) -> GetPersonalAccessTokensResponse {
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

pub(crate) fn build_get_me_response(
    shard: &Rc<ServerNgShard>,
    sessions: &Rc<RefCell<SessionManager>>,
    transport_client_id: u128,
) -> ClientDetailsResponse {
    let mut client = sessions
        .borrow()
        .client_record(transport_client_id)
        .map_or_else(
            || {
                // No session record (shouldn't happen on an auth-gated
                // read). Report the connection id with the "no user"
                // sentinel + TCP default rather than impersonating root
                // (user id 0 is a real user; server-ng is 0-based).
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
pub(crate) fn connected_client_to_response(
    shard: &Rc<ServerNgShard>,
    info: &ConnectedClientInfo,
) -> ClientResponse {
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

/// Fence a consumer-group offset commit/delete: a group consumer may only
/// touch the offset of a partition it currently owns. `Ok` for individual
/// consumers (no fence) and for owned group partitions; `Err` otherwise so a
/// stale client re-syncs instead of corrupting the shared group offset.
fn fence_group_offset(
    shard: &Rc<ServerNgShard>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<(), IggyError> {
    if consumer.kind != KIND_CONSUMER_GROUP {
        return Ok(());
    }
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    #[allow(clippy::cast_possible_truncation)]
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        // Commit fence: allow a pending-revoked partition (the source commits it
        // to drain the cooperative handoff), so `require_pollable = false`.
        .consumer_group_fence(
            stream_id,
            topic_id,
            &consumer.id,
            client_id,
            partition_id,
            false,
        )
        .map(|_| ())
        .ok_or(IggyError::ConsumerGroupPartitionNotOwned(
            client_id as u32,
            partition_id,
        ))
}

/// Fence a consumer-group offset op then resolve its target partition
/// namespace. Shared by the four `Store`/`Delete` consumer-offset arms.
fn fence_and_resolve_offset_namespace(
    shard: &Rc<ServerNgShard>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<IggyNamespace, IggyError> {
    fence_group_offset(
        shard,
        consumer,
        stream_id,
        topic_id,
        partition_id,
        client_id,
    )?;
    resolve_partition_namespace(shard, stream_id, topic_id, partition_id)
}

pub(crate) fn resolve_partition_request_namespace(
    shard: &Rc<ServerNgShard>,
    operation: Operation,
    body: &[u8],
    client_id: u128,
) -> Result<u64, IggyError> {
    let namespace = match operation {
        Operation::SendMessages => {
            if body.len() < 4 {
                return Err(IggyError::InvalidCommand);
            }
            let metadata_length = u32::from_le_bytes(
                body[..4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if body.len() < 4 + metadata_length {
                return Err(IggyError::InvalidCommand);
            }
            let header = SendMessagesHeader::decode_from(&body[4..4 + metadata_length])
                .map_err(|_| IggyError::InvalidCommand)?;
            resolve_send_messages_namespace(shard, &header)?
        }
        Operation::StoreConsumerOffset => {
            let request = StoreConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::DeleteConsumerOffset => {
            let request = DeleteConsumerOffsetRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::StoreConsumerOffset2 => {
            let request = StoreConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::DeleteConsumerOffset2 => {
            let request = DeleteConsumerOffset2Request::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            fence_and_resolve_offset_namespace(
                shard,
                &request.consumer,
                &request.stream_id,
                &request.topic_id,
                request.partition_id,
                client_id,
            )?
        }
        Operation::DeleteSegments => {
            let request =
                DeleteSegmentsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            resolve_partition_namespace(
                shard,
                &request.stream_id,
                &request.topic_id,
                Some(request.partition_id),
            )?
        }
        _ => return Err(IggyError::FeatureUnavailable),
    };
    Ok(namespace.inner())
}

fn resolve_send_messages_namespace(
    shard: &Rc<ServerNgShard>,
    header: &SendMessagesHeader,
) -> Result<IggyNamespace, IggyError> {
    let partition_id = match &header.partitioning {
        WirePartitioning::PartitionId(partition_id) => *partition_id,
        WirePartitioning::Balanced => shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .next_balanced_partition(&header.stream_id, &header.topic_id)
            .ok_or(IggyError::InvalidIdentifier)?,
        WirePartitioning::MessagesKey(key) => shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .partition_by_messages_key(&header.stream_id, &header.topic_id, key)
            .ok_or(IggyError::InvalidIdentifier)?,
    };
    resolve_partition_namespace(
        shard,
        &header.stream_id,
        &header.topic_id,
        Some(partition_id),
    )
}

pub(crate) fn resolve_partition_namespace(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
) -> Result<IggyNamespace, IggyError> {
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .namespace_from_partition(stream_id, topic_id, partition_id)
        .ok_or(IggyError::InvalidIdentifier)
}

/// `user_id` is the authenticated caller, used only by the identity-scoped
/// reads (currently the PAT list); every other arm ignores it. Authorization
/// stays with the per-transport gates that run before this builder.
pub(crate) fn build_non_replicated_response(
    shard: &Rc<ServerNgShard>,
    code: u32,
    body: &[u8],
    user_id: Option<u32>,
    roster: &ClusterRoster,
) -> Result<NonReplicatedResponse, IggyError> {
    match code {
        GET_CLUSTER_METADATA_CODE => Ok(NonReplicatedResponse::Bytes(
            build_cluster_metadata_response(roster, shard).to_bytes(),
        )),
        GET_STATS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_stats_response(shard)?.to_bytes(),
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
        GET_CONSUMER_GROUP_CODE => {
            let request = GetConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
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
        GET_CONSUMER_GROUPS_CODE => {
            let request = GetConsumerGroupsRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
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
        // server-ng has no on-demand flush primitive, so it denies honestly.
        // The non-replicated catch-all's empty-ok would otherwise attest a
        // durability guarantee the server never gave.
        FLUSH_UNSAVED_BUFFER_CODE => Err(IggyError::FeatureUnavailable),
        // Snapshot collection blocks on shell-outs, so the dedicated dispatch
        // and HTTP handlers await it off-thread; this synchronous builder
        // cannot, and reaching it here is a routing bug. Fail closed rather
        // than let the catch-all's empty-ok attest an artifact that was never
        // produced.
        GET_SNAPSHOT_FILE_CODE => Err(IggyError::InvalidCommand),
        _ => match iggy_binary_protocol::dispatch::lookup_command(code) {
            Some(meta) if !meta.is_replicated() => Ok(NonReplicatedResponse::Empty),
            Some(_) => Err(IggyError::FeatureUnavailable),
            None => Err(IggyError::InvalidCommand),
        },
    }
}

/// Build the binary `GetClusterMetadata` reply from the shared roster assembly.
/// The leader marking comes from this shard's consensus view; a shard without
/// consensus (any shard but 0) still serves the full roster, only with no node
/// marked leader.
fn build_cluster_metadata_response(
    roster: &ClusterRoster,
    shard: &Rc<ServerNgShard>,
) -> ClusterMetadataResponse {
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
        .or_else(|| roster.current_primary_index());
    let metadata = roster.cluster_metadata(primary_index);
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

fn build_stats_response(shard: &Rc<ServerNgShard>) -> Result<StatsResponse, IggyError> {
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
        .read(|streams| -> Result<_, IggyError> {
            let mut topics_count = 0u32;
            let mut partitions_count = 0u32;
            let mut segments_count = 0u32;
            let mut messages_size_bytes = 0u64;
            let mut messages_count = 0u64;
            for (_, stream) in &streams.items {
                topics_count = topics_count.saturating_add(usize_to_u32(stream.topics.len())?);
                messages_size_bytes =
                    messages_size_bytes.saturating_add(stream.stats.size_bytes_inconsistent());
                messages_count =
                    messages_count.saturating_add(stream.stats.messages_count_inconsistent());
                // Segment counts roll up from the partition plane through
                // the shared stats registry (partition -> topic -> stream).
                segments_count =
                    segments_count.saturating_add(stream.stats.segments_count_inconsistent());
                for (_, topic) in &stream.topics {
                    partitions_count =
                        partitions_count.saturating_add(usize_to_u32(topic.partitions.len())?);
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
        })?;
    let consumer_groups_count = usize_to_u32(
        shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .consumer_group_count(),
    )?;

    let system = probe_system_stats();
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
        // Connected clients are per-shard `SessionManager` state, aggregated
        // across shards only by the async `ListClients` broadcast (see
        // `get_clients`). This sync single-shard read can't gather it, and one
        // shard's local count is a fraction of the total, so report 0 rather
        // than a misleading partial.
        clients_count: 0,
        consumer_groups_count,
        hostname: system.hostname,
        os_name: system.os_name,
        os_version: system.os_version,
        kernel_version: system.kernel_version,
        iggy_server_version: crate::VERSION.to_owned(),
        iggy_server_semver: crate::SEMANTIC_VERSION.get_numeric_version().ok(),
        cache_metrics: Vec::new(),
        threads_count: system.threads_count,
        // Disk space needs the configured data directory (legacy reads
        // `config.system.path`); the shard doesn't expose server config on the
        // read path, so report 0 rather than probe an unrelated mount.
        free_disk_space: 0,
        total_disk_space: 0,
    })
}

/// Process- and host-level portion of the stats reply, probed via `sysinfo`.
/// These describe the whole process, not shard or metadata state, so any one
/// shard can serve them without aggregation. The CPU fields are deltas over the
/// serving thread's own [`SYSINFO`] refresh history, so they vary by serving
/// shard (a shard's first probe reports zero CPU).
struct SystemStats {
    process_id: u32,
    cpu_usage: f32,
    total_cpu_usage: f32,
    memory_usage: u64,
    total_memory: u64,
    available_memory: u64,
    run_time: u64,
    start_time: u64,
    read_bytes: u64,
    written_bytes: u64,
    threads_count: u32,
    hostname: String,
    os_name: String,
    os_version: String,
    kernel_version: String,
}

thread_local! {
    // `cpu_usage` is a delta since the previous refresh, so the sampled
    // `System` is kept alive across `GetStats` calls (a freshly created one
    // reports zero CPU). Mirrors the legacy shard-0 stats path.
    static SYSINFO: RefCell<Option<SysinfoSystem>> = const { RefCell::new(None) };
}

/// Host / OS identity is process-static (unlike the per-call CPU and memory
/// samples), so probe it once and clone from the cache on each `GetStats`
/// rather than re-querying sysinfo every call. Process-global, so a `OnceLock`
/// fits better than the per-thread [`SYSINFO`] cell.
struct HostIdentity {
    hostname: String,
    os_name: String,
    os_version: String,
    kernel_version: String,
}

impl HostIdentity {
    fn probe() -> Self {
        Self {
            hostname: SysinfoSystem::host_name().unwrap_or_else(|| "unknown_hostname".to_owned()),
            os_name: SysinfoSystem::name().unwrap_or_else(|| "unknown_os_name".to_owned()),
            os_version: SysinfoSystem::long_os_version()
                .unwrap_or_else(|| "unknown_os_version".to_owned()),
            kernel_version: SysinfoSystem::kernel_version()
                .unwrap_or_else(|| "unknown_kernel_version".to_owned()),
        }
    }
}

static HOST_IDENTITY: OnceLock<HostIdentity> = OnceLock::new();

fn probe_system_stats() -> SystemStats {
    let host = HOST_IDENTITY.get_or_init(HostIdentity::probe);
    let probe = SYSINFO.with_borrow_mut(|slot| {
        let sys = slot.get_or_insert_with(SysinfoSystem::new);
        SystemProbe::capture(sys)
    });

    SystemStats {
        process_id: probe.process_id,
        cpu_usage: probe.cpu_usage,
        total_cpu_usage: probe.total_cpu_usage,
        memory_usage: probe.memory_usage,
        total_memory: probe.total_memory,
        available_memory: probe.available_memory,
        // sysinfo reports whole seconds; the wire fields are micros (the
        // SDK decodes them via `IggyDuration` / `IggyTimestamp::from`, both
        // micro-based).
        run_time: probe.run_time_secs.saturating_mul(1_000_000),
        start_time: probe.start_time_secs.saturating_mul(1_000_000),
        read_bytes: probe.read_bytes,
        written_bytes: probe.written_bytes,
        threads_count: probe.threads_count,
        hostname: host.hostname.clone(),
        os_name: host.os_name.clone(),
        os_version: host.os_version.clone(),
        kernel_version: host.kernel_version.clone(),
    }
}

fn build_get_stream_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
) -> Result<Option<GetStreamResponse>, IggyError> {
    let default_max_topic_size = shard.plane.metadata().default_max_topic_size();
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = resolve_stream_id(streams, stream_id) else {
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
                .map(|(_, topic)| topic_header(topic, default_max_topic_size))
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_streams_response(shard: &Rc<ServerNgShard>) -> Result<GetStreamsResponse, IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        streams
            .items
            .iter()
            .map(|(_, stream)| stream_response(stream))
            .collect::<Result<Vec<_>, _>>()
            .map(|streams| GetStreamsResponse { streams })
    })
}

#[allow(clippy::cast_possible_truncation)]
fn user_response(user: &metadata::stm::user::User) -> Result<UserResponse, IggyError> {
    Ok(UserResponse {
        id: user.id,
        created_at: user.created_at.as_micros(),
        status: user.status.as_code(),
        username: WireName::new(user.username.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

fn build_get_users_response(shard: &Rc<ServerNgShard>) -> Result<GetUsersResponse, IggyError> {
    shard.plane.metadata().mux_stm.users().read(|users| {
        users
            .items
            .iter()
            .map(|(_, user)| user_response(user))
            .collect::<Result<Vec<_>, _>>()
            .map(|users| GetUsersResponse { users })
    })
}

fn build_get_user_response(
    shard: &Rc<ServerNgShard>,
    user_id: &WireIdentifier,
) -> Result<Option<UserDetailsResponse>, IggyError> {
    shard.plane.metadata().mux_stm.users().read(|users| {
        let resolved = match user_id {
            WireIdentifier::Numeric(id) => {
                let id = *id as usize;
                users.items.contains(id).then_some(id)
            }
            WireIdentifier::String(name) => users.index.get(name.as_str()).map(|&id| id as usize),
        };
        let Some(id) = resolved else {
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

fn build_get_topic_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<Option<GetTopicResponse>, IggyError> {
    let default_max_topic_size = shard.plane.metadata().default_max_topic_size();
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let Some(stream_id) = resolve_stream_id(streams, stream_id) else {
            return Ok(None);
        };
        let Some(topic_id) = resolve_topic_id(streams, stream_id, topic_id) else {
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
            topic: topic_header(topic, default_max_topic_size)?,
            partitions: topic
                .partitions
                .iter()
                .map(|partition| partition_response(streams, stream_id, topic_id, partition))
                .collect::<Result<Vec<_>, _>>()?,
        }))
    })
}

fn build_get_topics_response(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
) -> Result<GetTopicsResponse, IggyError> {
    let default_max_topic_size = shard.plane.metadata().default_max_topic_size();
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let resolved_stream =
            resolve_stream_id(streams, stream_id).ok_or_else(|| stream_not_found(stream_id))?;
        let stream = streams
            .items
            .get(resolved_stream)
            .ok_or(IggyError::InvalidIdentifier)?;
        stream
            .topics
            .iter()
            .map(|(_, topic)| topic_header(topic, default_max_topic_size))
            .collect::<Result<Vec<_>, _>>()
            .map(|topics| GetTopicsResponse { topics })
    })
}

/// Reject a consumer-group read whose parent stream/topic is absent with the
/// legacy typed error naming the level that missed; the group itself missing
/// stays the shared not-found reply (empty over TCP, 404 over HTTP).
fn ensure_topic_exists(
    shard: &Rc<ServerNgShard>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<(), IggyError> {
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let resolved_stream =
            resolve_stream_id(streams, stream_id).ok_or_else(|| stream_not_found(stream_id))?;
        resolve_topic_id(streams, resolved_stream, topic_id)
            .ok_or_else(|| topic_not_found(stream_id, topic_id))?;
        Ok(())
    })
}

/// Convert a `WireIdentifier` to the domain `Identifier`.
fn wire_id_to_identifier(wire: &WireIdentifier) -> Result<Identifier, IggyError> {
    match wire {
        WireIdentifier::Numeric(id) => Identifier::numeric(*id),
        WireIdentifier::String(name) => Identifier::named(name.as_str()),
    }
}

/// Typed miss for a read's parent stream, matching the legacy servers' error
/// shape. The identifier only feeds the error message; a wire form with no
/// domain equivalent (numeric 0 is a live slab id here but not a legacy id)
/// falls back to the default identifier.
fn stream_not_found(stream_id: &WireIdentifier) -> IggyError {
    IggyError::StreamIdNotFound(wire_id_to_identifier(stream_id).unwrap_or_default())
}

/// Typed miss for a read's parent topic; see [`stream_not_found`]. The variant's
/// display order is (topic, stream).
fn topic_not_found(stream_id: &WireIdentifier, topic_id: &WireIdentifier) -> IggyError {
    IggyError::TopicIdNotFound(
        wire_id_to_identifier(topic_id).unwrap_or_default(),
        wire_id_to_identifier(stream_id).unwrap_or_default(),
    )
}

pub(crate) fn resolve_stream_id(
    streams: &metadata::stm::stream::StreamsInner,
    identifier: &WireIdentifier,
) -> Option<usize> {
    match identifier {
        WireIdentifier::Numeric(id) => {
            let id = *id as usize;
            streams.items.contains(id).then_some(id)
        }
        WireIdentifier::String(name) => streams.index.get(name.as_str()).copied(),
    }
}

pub(crate) fn resolve_topic_id(
    streams: &metadata::stm::stream::StreamsInner,
    stream_id: usize,
    identifier: &WireIdentifier,
) -> Option<usize> {
    let stream = streams.items.get(stream_id)?;
    match identifier {
        WireIdentifier::Numeric(id) => {
            let id = *id as usize;
            stream.topics.contains(id).then_some(id)
        }
        WireIdentifier::String(name) => stream.topic_index.get(name.as_str()).copied(),
    }
}

fn stream_response(stream: &metadata::stm::stream::Stream) -> Result<StreamResponse, IggyError> {
    Ok(StreamResponse {
        id: usize_to_u32(stream.id)?,
        created_at: stream.created_at.as_micros(),
        topics_count: usize_to_u32(stream.topics.len())?,
        size_bytes: stream.stats.size_bytes_inconsistent(),
        messages_count: stream.stats.messages_count_inconsistent(),
        name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

/// Resolve a topic's stored [`MaxTopicSize`] to the wire byte value, mapping
/// the `ServerDefault` sentinel to this node's configured default. Replicated
/// apply stores the sentinel verbatim so commit stays config-independent across
/// a cluster; the per-node default is applied here on read, matching the legacy
/// "what this server treats the limit as" semantics. Primary admission stamps
/// the same default before replication, so this only bites topics whose stored
/// size predates that stamping (older snapshot / WAL data). Explicit `Custom`
/// and `Unlimited` values pass through.
fn resolve_max_topic_size(max_topic_size: MaxTopicSize, default_bytes: u64) -> u64 {
    match max_topic_size {
        MaxTopicSize::ServerDefault => default_bytes,
        resolved => resolved.as_bytes_u64(),
    }
}

fn topic_header(
    topic: &metadata::stm::stream::Topic,
    default_max_topic_size: u64,
) -> Result<StreamTopicHeader, IggyError> {
    Ok(StreamTopicHeader {
        id: usize_to_u32(topic.id)?,
        created_at: topic.created_at.as_micros(),
        partitions_count: usize_to_u32(topic.partitions.len())?,
        message_expiry: u64::from(topic.message_expiry),
        compression_algorithm: topic.compression_algorithm.as_code(),
        max_topic_size: resolve_max_topic_size(topic.max_topic_size, default_max_topic_size),
        replication_factor: topic.replication_factor,
        size_bytes: topic.stats.size_bytes_inconsistent(),
        messages_count: topic.stats.messages_count_inconsistent(),
        name: WireName::new(topic.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
    })
}

fn partition_response(
    streams: &metadata::stm::stream::StreamsInner,
    stream_id: usize,
    topic_id: usize,
    partition: &metadata::stm::stream::Partition,
) -> Result<PartitionResponse, IggyError> {
    // Per-partition counters live in the shared stats registry (one `Arc`
    // across all shards and both left-right buffers), populated when the
    // owning shard materializes the partition; `None` only in the window
    // before that first materialization.
    let stats = streams
        .stats_registry
        .partition_get(stream_id, topic_id, partition.id);
    let (segments_count, current_offset, size_bytes, messages_count) =
        stats.map_or((0, 0, 0, 0), |stats| {
            (
                stats.segments_count_inconsistent(),
                stats.current_offset(),
                stats.size_bytes_inconsistent(),
                stats.messages_count_inconsistent(),
            )
        });
    Ok(PartitionResponse {
        id: usize_to_u32(partition.id)?,
        created_at: partition.created_at.as_micros(),
        segments_count,
        current_offset,
        size_bytes,
        messages_count,
    })
}

pub(crate) enum NonReplicatedResponse {
    Empty,
    Bytes(Bytes),
}

impl NonReplicatedResponse {
    pub(crate) fn into_reply(
        self,
        request_header: &RequestHeader,
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

pub(crate) fn build_empty_reply(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
) -> Message<ReplyHeader> {
    build_reply_with_body(request_header, client_id, session, commit, 0, |_| {})
}

/// Build an empty reply that denies a dispatch-time authorization check: the
/// same request echo as [`build_empty_reply`] but with `ReplyHeader.status`
/// set to the rule's error code -- the request-level failure channel the SDK
/// peeks before any body decode. Every deny frame shares this shape (empty
/// body, nonzero status); op carries the builder's session argument like every
/// reply, and only the partition primary's pre-pipeline deny pins it to 0,
/// stamped through `consensus::build_deny_reply_from_request`.
pub(crate) fn build_deny_reply(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    status: u32,
) -> Message<ReplyHeader> {
    let mut reply = build_empty_reply(request_header, client_id, session, commit);
    let header_len = std::mem::size_of::<ReplyHeader>();
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
        &mut reply.as_mut_slice()[..header_len],
    )
    .expect("empty reply header is a valid ReplyHeader");
    header.status = status;
    reply
}

/// Server build version advertised in the login-register response.
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub(crate) fn build_login_register_reply(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    user_id: u32,
) -> Message<ReplyHeader> {
    // Result-framed like every metadata reply: a zero result-count (success)
    // followed by the `LoginRegisterResponse` payload. A transient Register
    // instead ships a `[count=1][index=0][TransientNotCommitted]` frame
    // (`build_transient_reply`), which the SDK decodes and replays. The matching
    // strip is in the SDK `split_metadata_result` (Register is result-framed).
    let payload = LoginRegisterResponse {
        user_id,
        session,
        server_protocol_version: IGGY_PROTOCOL_VERSION,
        server_version: WireName::new(SERVER_VERSION).expect("SERVER_VERSION is 1-255 bytes"),
    }
    .to_bytes();
    let mut body = Vec::with_capacity(RESULT_COUNT_LEN + payload.len());
    body.extend_from_slice(&[0u8; RESULT_COUNT_LEN]);
    body.extend_from_slice(&payload);
    build_reply_from_bytes(
        request_header,
        client_id,
        session,
        commit,
        &Bytes::from(body),
    )
}

pub(crate) fn build_reply_from_bytes(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body: &Bytes,
) -> Message<ReplyHeader> {
    build_reply_with_body(
        request_header,
        client_id,
        session,
        commit,
        body.len(),
        |out| out.copy_from_slice(body),
    )
}

/// If a raw PAT token was minted (`CreatePersonalAccessToken`) and the commit
/// succeeded, replace the committed reply -- whose body is empty because the
/// raw token never entered consensus -- with a `RawPersonalAccessTokenResponse`,
/// reusing the confirmed commit position from the committed reply. Otherwise
/// (no token, a committed business rejection, or an eviction frame) the
/// committed reply passes through unchanged.
pub(crate) fn build_raw_pat_reply(
    request_header: &RequestHeader,
    committed: Message<GenericHeader>,
    raw_token: Option<String>,
) -> Result<Message<GenericHeader>, IggyError> {
    let Some(raw) = raw_token else {
        return Ok(committed);
    };
    // `submit_request_in_process` hands back an `EvictionHeader`-backed message
    // on the evict outcome (e.g. a `CreatePersonalAccessToken` whose session
    // was evicted between bind and request). Its byte pattern is a valid
    // `ReplyHeader`, so the checked cast below would silently pass and we would
    // both swallow the eviction and ship a raw token whose hash never
    // committed. Only rewrite a genuine committed `Reply`; pass anything else
    // (the eviction) through untouched so the client learns its session died.
    if committed.header().command != Command2::Reply {
        return Ok(committed);
    }
    let header_len = std::mem::size_of::<ReplyHeader>();
    let committed_header =
        bytemuck::checked::try_from_bytes::<ReplyHeader>(&committed.as_slice()[..header_len])
            .map_err(|_| IggyError::InvalidFormat)?;
    let commit = committed_header.commit;
    let size = committed_header.size as usize;
    // A committed create can still be a business rejection (duplicate name,
    // invalid expiry) whose reply body carries a nonzero result code. Splice
    // the secret only into a genuine success; pass everything else through
    // untouched so the client decodes the committed error instead of a
    // success-shaped token reply (the minted raw secret is simply dropped).
    // Mirrors the HTTP handler's `committed_payload` gate.
    let reply_body = committed
        .as_slice()
        .get(header_len..size)
        .unwrap_or_default();
    if result_code(reply_body) != Some(0) {
        return Ok(committed);
    }
    let token = WireName::new(raw.as_str()).map_err(|_| IggyError::InvalidFormat)?;
    let response = RawPersonalAccessTokenResponse { token };
    // The SDK strips a leading result section from every metadata reply
    // (`split_metadata_result`), so the spliced body must carry the success
    // section like any committed metadata reply; a bare token body would be
    // read as a garbage result code.
    let mut body = BytesMut::with_capacity(RESULT_COUNT_LEN + response.encoded_size());
    body.put_u32_le(0);
    response.encode(&mut body);
    let reply = build_reply_from_bytes(
        request_header,
        request_header.client,
        request_header.session,
        commit,
        &body.freeze(),
    );
    Ok(reply.into_generic())
}

pub(crate) fn build_reply_with_body(
    request_header: &RequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body_len: usize,
    write_body: impl FnOnce(&mut [u8]),
) -> Message<ReplyHeader> {
    let header_len = std::mem::size_of::<ReplyHeader>();
    let total_size = header_len + body_len;
    let mut reply = Message::<ReplyHeader>::new(total_size);
    let header_size = u32::try_from(total_size).expect("reply size must fit into u32");
    let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
        &mut reply.as_mut_slice()[..header_len],
    )
    .expect("zeroed bytes are valid");
    *header = ReplyHeader {
        cluster: request_header.cluster,
        size: header_size,
        view: request_header.view,
        release: request_header.release,
        command: Command2::Reply,
        replica: request_header.replica,
        request_checksum: request_header.request_checksum,
        client: client_id,
        op: session,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        namespace: request_header.namespace,
        ..Default::default()
    };
    write_body(&mut reply.as_mut_slice()[header_len..total_size]);
    reply
}

pub(crate) fn current_metadata_commit(shard: &Rc<ServerNgShard>) -> u64 {
    shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map_or(0, VsrConsensus::commit_max)
}

/// Size of the in-storage (`IggyMessage2`) per-message header inside a
/// `SendMessages2` batch blob: `checksum`(8) + `id`(16) + `offset_delta`(4)
/// + `timestamp_delta`(4) + `user_headers_length`(4) + `payload_length`(4)
/// + reserved(8). See `server_common::send_messages2::from_legacy_request`.
const STORED_MESSAGE_HEADER_SIZE: usize = 48;

/// Build the `PolledMessages` reply body from the owning shard's poll
/// fragments.
///
/// Fragments carry the stored `SendMessages2` batches: a 256-byte command
/// header followed by `IggyMessage2`-format messages
/// (`[48B header][payload][user_headers]`, offsets/timestamps delta-encoded
/// against the batch). The SDK decodes the legacy wire format
/// (`[64B header][payload][user_headers]`, absolute offsets); the message
/// sections share the legacy order, so only the header is re-encoded here
/// and the section bytes copy through contiguously.
///
/// Body layout: `[partition_id:4][current_offset:8][count:4][messages...]`.
pub(crate) fn build_polled_messages_body(
    partition_id: u32,
    current_offset: u64,
    fragments: PollFragments,
    encryptor: Option<&EncryptorKind>,
) -> Result<Bytes, IggyError> {
    // Body head: [partition_id:4][current_offset:8][count:4]. `count` sits at
    // COUNT_OFFSET and is backpatched once the walk below knows it.
    const HEAD_LEN: usize = 16;
    const COUNT_OFFSET: usize = 12;
    // Batches may arrive split across fragments (rewritten command header +
    // sliced blob); concatenate into one stream before walking batches.
    let mut stream: Vec<u8> = Vec::new();
    for fragment in fragments {
        let frozen = fragment.into_frozen();
        stream.extend_from_slice(frozen.as_slice());
    }

    // Reserve the head and encode the messages straight into `body`; encoding
    // directly avoids a separate messages buffer and its copy-through into the
    // head.
    let mut body: Vec<u8> = Vec::with_capacity(HEAD_LEN + stream.len());
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&current_offset.to_le_bytes());
    body.extend_from_slice(&[0u8; 4]); // count placeholder, backpatched below
    let mut count: u32 = 0;
    let mut position = 0usize;
    while position < stream.len() {
        let batch = SendMessages2Header::decode(&stream[position..])?;
        let batch_end = position
            .checked_add(
                usize::try_from(batch.batch_length).map_err(|_| IggyError::InvalidCommand)?,
            )
            .ok_or(IggyError::InvalidCommand)?;
        if batch_end > stream.len() {
            return Err(IggyError::InvalidCommand);
        }
        let mut cursor = position + COMMAND_HEADER_SIZE;
        while cursor < batch_end {
            if cursor + STORED_MESSAGE_HEADER_SIZE > batch_end {
                return Err(IggyError::InvalidCommand);
            }
            let header = &stream[cursor..cursor + STORED_MESSAGE_HEADER_SIZE];
            let checksum = &header[0..8];
            let id = &header[8..24];
            let offset_delta = u32::from_le_bytes(header[24..28].try_into().expect("4-byte slice"));
            let timestamp_delta =
                u32::from_le_bytes(header[28..32].try_into().expect("4-byte slice"));
            let user_headers_length =
                u32::from_le_bytes(header[32..36].try_into().expect("4-byte slice")) as usize;
            let payload_length =
                u32::from_le_bytes(header[36..40].try_into().expect("4-byte slice")) as usize;

            let sections_start = cursor + STORED_MESSAGE_HEADER_SIZE;
            let sections_end = sections_start + payload_length + user_headers_length;
            if sections_end > batch_end {
                return Err(IggyError::InvalidCommand);
            }

            let offset = batch.base_offset + u64::from(offset_delta);
            // `base_timestamp` is the flat broker append time stamped once per
            // batch; `timestamp_delta` is a per-message delta against the
            // producer origin, so it only applies to `origin_timestamp`. Adding
            // it to the broker base would mix two clocks.
            let timestamp = batch.base_timestamp;
            let origin_timestamp = batch.origin_timestamp + u64::from(timestamp_delta);

            body.extend_from_slice(checksum);
            body.extend_from_slice(id);
            body.extend_from_slice(&offset.to_le_bytes());
            body.extend_from_slice(&timestamp.to_le_bytes());
            body.extend_from_slice(&origin_timestamp.to_le_bytes());
            if let Some(encryptor) = encryptor {
                // At-rest encryption: stored sections are ciphertext (encrypted
                // once at ingestion, replicated verbatim); this reply is the
                // single decrypt point, so lengths are rewritten to the
                // plaintext sizes. The stored per-message checksum still covers
                // the ciphertext and is passed through untouched (the SDK does
                // not re-validate it against the reply layout).
                let payload_end = sections_start + payload_length;
                let payload = encryptor
                    .decrypt(&stream[sections_start..payload_end])
                    .map_err(|_| IggyError::CannotDecryptData)?;
                let user_headers = if user_headers_length > 0 {
                    Some(
                        encryptor
                            .decrypt(&stream[payload_end..sections_end])
                            .map_err(|_| IggyError::CannotDecryptData)?,
                    )
                } else {
                    None
                };
                let user_headers_bytes: &[u8] = user_headers.as_deref().unwrap_or_default();
                body.extend_from_slice(
                    &u32::try_from(user_headers_bytes.len())
                        .map_err(|_| IggyError::InvalidCommand)?
                        .to_le_bytes(),
                );
                body.extend_from_slice(
                    &u32::try_from(payload.len())
                        .map_err(|_| IggyError::InvalidCommand)?
                        .to_le_bytes(),
                );
                body.extend_from_slice(&0u64.to_le_bytes()); // reserved
                body.extend_from_slice(&payload);
                body.extend_from_slice(user_headers_bytes);
            } else {
                body.extend_from_slice(
                    &u32::try_from(user_headers_length)
                        .expect("length came from u32")
                        .to_le_bytes(),
                );
                body.extend_from_slice(
                    &u32::try_from(payload_length)
                        .expect("length came from u32")
                        .to_le_bytes(),
                );
                body.extend_from_slice(&0u64.to_le_bytes()); // reserved
                // Stored sections are already in legacy order
                // (`[payload][user_headers]`): copy through contiguously.
                body.extend_from_slice(&stream[sections_start..sections_end]);
            }

            count += 1;
            cursor = sections_end;
        }
        position = batch_end;
    }

    body[COUNT_OFFSET..HEAD_LEN].copy_from_slice(&count.to_le_bytes());
    Ok(Bytes::from(body))
}

/// Build the `ConsumerOffsetResponse` reply body:
/// `[partition_id:4][current_offset:8][stored_offset:8]`.
pub(crate) fn build_consumer_offset_body(
    partition_id: u32,
    current_offset: u64,
    stored_offset: u64,
) -> Bytes {
    let mut body = Vec::with_capacity(20);
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&current_offset.to_le_bytes());
    body.extend_from_slice(&stored_offset.to_le_bytes());
    Bytes::from(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pat_request_header() -> RequestHeader {
        let zeroed = [0u8; std::mem::size_of::<RequestHeader>()];
        let mut header = *bytemuck::checked::try_from_bytes::<RequestHeader>(&zeroed)
            .expect("zeroed bytes form a valid RequestHeader");
        header.command = Command2::Request;
        header.operation = Operation::CreatePersonalAccessToken;
        header.client = 42;
        header.session = 7;
        header.request = 3;
        header
    }

    /// A committed metadata reply whose body is the given result section
    /// (`[count][{index, result}]*`), as the commit path emits it.
    fn committed_reply(result_body: &[u8]) -> Message<GenericHeader> {
        let request_header = pat_request_header();
        build_reply_from_bytes(
            &request_header,
            42,
            7,
            9,
            &Bytes::copy_from_slice(result_body),
        )
        .into_generic()
    }

    #[test]
    fn raw_pat_reply_splices_token_into_a_committed_success() {
        let success = committed_reply(&0u32.to_le_bytes());
        let reply =
            build_raw_pat_reply(&pat_request_header(), success, Some("raw-token".to_owned()))
                .expect("splice succeeds");
        let header_len = std::mem::size_of::<ReplyHeader>();
        let body = &reply.as_slice()[header_len..reply.header().size as usize];
        // Framed like every committed metadata reply: the SDK reads the result
        // section first, then decodes the token payload past it.
        assert_eq!(result_code(body), Some(0));
        let response = RawPersonalAccessTokenResponse::decode_from(&body[RESULT_COUNT_LEN..])
            .expect("token body decodes");
        assert_eq!(response.token.as_str(), "raw-token");
    }

    #[test]
    fn raw_pat_reply_passes_a_committed_rejection_through_untouched() {
        let rejection_code =
            IggyError::PersonalAccessTokenAlreadyExists(String::new(), 0).as_code();
        let mut result_body = Vec::new();
        result_body.extend_from_slice(&1u32.to_le_bytes());
        result_body.extend_from_slice(&0u32.to_le_bytes());
        result_body.extend_from_slice(&rejection_code.to_le_bytes());
        let rejection = committed_reply(&result_body);
        let original = rejection.as_slice().to_vec();

        let reply = build_raw_pat_reply(
            &pat_request_header(),
            rejection,
            Some("raw-token".to_owned()),
        )
        .expect("pass-through succeeds");
        assert_eq!(
            reply.as_slice(),
            original.as_slice(),
            "a committed rejection must not be rewritten into a token reply"
        );
    }

    #[test]
    fn raw_pat_reply_without_a_token_passes_through() {
        let success = committed_reply(&0u32.to_le_bytes());
        let original = success.as_slice().to_vec();
        let reply =
            build_raw_pat_reply(&pat_request_header(), success, None).expect("pass-through");
        assert_eq!(reply.as_slice(), original.as_slice());
    }

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
    fn probe_system_stats_reports_this_process_and_host_memory() {
        let stats = probe_system_stats();
        // Straight from `sysinfo`, independent of shard state: the pid is our
        // own and any host the test runs on has nonzero total memory. A zero
        // here means the probe wired nothing (the pre-fix stubbed literal).
        assert_eq!(stats.process_id, std::process::id());
        assert!(stats.total_memory > 0);
        assert!(!stats.hostname.is_empty());
    }

    #[test]
    fn topic_header_resolves_server_default_and_passes_explicit_sizes_through() {
        use iggy_common::{CompressionAlgorithm, IggyExpiry, StreamStats, TopicStats};
        use std::sync::atomic::AtomicUsize;

        const NODE_DEFAULT: u64 = 4 * 1024 * 1024 * 1024;

        let parent = Arc::new(StreamStats::default());
        let topic_with = |max_topic_size| metadata::stm::stream::Topic {
            id: 0,
            name: Arc::from("topic"),
            created_at: IggyTimestamp::from(1u64),
            replication_factor: 1,
            message_expiry: IggyExpiry::NeverExpire,
            compression_algorithm: CompressionAlgorithm::None,
            max_topic_size,
            stats: Arc::new(TopicStats::new(parent.clone())),
            partitions: Vec::new(),
            round_robin_counter: Arc::new(AtomicUsize::new(0)),
            consumer_groups: ahash::AHashMap::default(),
            consumer_group_index: ahash::AHashMap::default(),
            next_consumer_group_id: 0,
        };

        // ServerDefault (0 on the wire) resolves to this node's configured
        // default, not the raw 0 the pre-fix read path echoed.
        let resolved = topic_header(&topic_with(MaxTopicSize::ServerDefault), NODE_DEFAULT)
            .expect("topic header builds");
        assert_eq!(resolved.max_topic_size, NODE_DEFAULT);

        // Explicit sizes round-trip unchanged, independent of the node default.
        let custom = topic_header(&topic_with(MaxTopicSize::from(1024u64)), NODE_DEFAULT)
            .expect("topic header builds");
        assert_eq!(custom.max_topic_size, 1024);
        let unlimited = topic_header(&topic_with(MaxTopicSize::Unlimited), NODE_DEFAULT)
            .expect("topic header builds");
        assert_eq!(unlimited.max_topic_size, u64::MAX);
    }
}
