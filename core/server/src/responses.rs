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
//! `NonReplicatedResponse` dispatch shim and the partition-namespace
//! resolvers.

use crate::cluster_meta::ClusterRoster;
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::wire::{transport_kind_to_wire, usize_to_u32};
use bytes::{Bytes, BytesMut};
use consensus::{MetadataHandle, VsrConsensus};
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, FLUSH_UNSAVED_BUFFER_CODE, GET_CLUSTER_METADATA_CODE,
    GET_CONSUMER_GROUP_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
    GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE,
    GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
};
use iggy_binary_protocol::consensus::{RESULT_COUNT_LEN, result_code};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::messages::SendMessagesHeader;
use iggy_binary_protocol::requests::personal_access_tokens::GetPersonalAccessTokensRequest;
use iggy_binary_protocol::requests::segments::DeleteSegmentsRequest;
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
use iggy_binary_protocol::responses::system::{DescribeOptionsResponse, OptionDescriptor};
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::topics::get_topics::GetTopicsResponse;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::get_users::GetUsersResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{
    Command, GenericHeader, IGGY_PROTOCOL_VERSION, KIND_CONSUMER_GROUP, Operation, ReplyHeader,
    RoutedRequestHeader, WireDecode, WireEncode, WireIdentifier, WireName, WirePartitioning,
};
use iggy_common::wire_conversions::{resource_options_to_wire, resource_options_to_wire_split};
use iggy_common::{
    EncryptorKind, HeaderKind, Identifier, IggyError, IggyTimestamp, OptionsProvenance,
    topic_option_keys,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::BusMessage;
use metadata::impls::metadata::StreamsFrontend;
use partitions::{Fragment, PollFragments};
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages;
use server_common::sharding::IggyNamespace;
use server_common::{MESSAGE_ALIGN, Message, ResponseBacking, ResponseFragments};
use shard::ConnectedClientInfo;
use std::cell::RefCell;
use std::net::IpAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{Arc, OnceLock};
use sysinfo::System as SysinfoSystem;
use system_stats::SystemProbe;
use tracing::warn;

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

/// Fence a consumer-group offset commit/delete: a group consumer may only
/// touch the offset of a partition it currently owns. `Ok` for individual
/// consumers (no fence) and for owned group partitions; `Err` otherwise so a
/// stale client re-syncs instead of corrupting the shared group offset.
fn fence_group_offset<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    if consumer.kind != KIND_CONSUMER_GROUP {
        return Ok(());
    }
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    let streams = shard.plane.metadata().mux_stm.streams();
    if streams
        .resolve_consumer_group_id(stream_id, topic_id, &consumer.id)
        .is_none()
    {
        return Err(IggyError::InvalidIdentifier);
    }
    #[allow(clippy::cast_possible_truncation)]
    streams
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
fn fence_and_resolve_offset_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    consumer: &WireConsumer,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
    client_id: u128,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
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

pub fn resolve_partition_request_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    operation: Operation,
    body: &[u8],
    client_id: u128,
) -> Result<u64, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
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

fn resolve_send_messages_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    header: &SendMessagesHeader,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
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

pub fn resolve_partition_namespace<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    partition_id: Option<u32>,
) -> Result<IggyNamespace, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let partition_id = partition_id.ok_or(IggyError::InvalidIdentifier)?;
    let streams = shard.plane.metadata().mux_stm.streams();
    if let Some(namespace) = streams.namespace_from_partition(stream_id, topic_id, partition_id) {
        return Ok(namespace);
    }
    // Name the level that missed - partition, topic, or stream - with the
    // legacy typed not-found, so a client can tell an addressing typo from an
    // empty partition. Callers that shape their own reply (empty poll, group
    // gather) treat every variant the same, so the split is reply-visible only
    // where a caller denies typed.
    if streams.topic_partition_ids(stream_id, topic_id).is_some() {
        return Err(IggyError::PartitionNotFound(
            partition_id as usize,
            wire_identifier_for_display(topic_id),
            wire_identifier_for_display(stream_id),
        ));
    }
    Err(streams.read(|inner| {
        let Some(resolved_stream) = resolve_stream_id(inner, stream_id) else {
            return stream_not_found(stream_id);
        };
        if resolve_topic_id(inner, resolved_stream, topic_id).is_none() {
            return topic_not_found(stream_id, topic_id);
        }
        // Unreachable while `topic_partition_ids` misses only on stream/topic;
        // kept as the safe generic rejection should that invariant drift.
        IggyError::InvalidIdentifier
    }))
}

/// Best-effort conversion for error payloads only: the wire reply carries just
/// the error code, so a failed conversion may fall back to a default without
/// changing what the client sees.
fn wire_identifier_for_display(id: &WireIdentifier) -> Identifier {
    match id {
        WireIdentifier::Numeric(numeric_id) => Identifier::numeric(*numeric_id),
        WireIdentifier::String(name) => Identifier::named(name.as_str()),
    }
    .unwrap_or_default()
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
    // Disk usage of the volume holding iggy data. The data directory is
    // captured process-globally at bootstrap (the shard doesn't carry server
    // config on the read path); absent that, or on a probe error, report 0.
    let (free_disk_space, total_disk_space) = STATS_DATA_PATH.get().map_or((0, 0), |path| {
        (
            fs2::available_space(path).unwrap_or(0),
            fs2::total_space(path).unwrap_or(0),
        )
    });
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

/// Configured data directory, captured once at bootstrap so the sync stats
/// read path can report disk usage of the volume that holds iggy data rather
/// than an unrelated mount. Unset (disk stats fall back to 0) until bootstrap.
static STATS_DATA_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Capture the configured data directory for `GetStats` disk reporting.
/// Idempotent: only the first call (process bootstrap) takes effect.
pub fn init_stats_data_path(path: PathBuf) {
    let _ = STATS_DATA_PATH.set(path);
}

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
        let Some(resolved_stream) = resolve_stream_id(streams, stream_id) else {
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

/// Reject a consumer-group read whose parent stream/topic is absent with the
/// legacy typed error naming the level that missed; the group itself missing
/// stays the shared not-found reply (empty over TCP, 404 over HTTP).
fn ensure_topic_exists<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Result<(), IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
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

pub fn resolve_stream_id(
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

pub fn resolve_topic_id(
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

pub fn build_empty_reply(
    request_header: &RoutedRequestHeader,
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
pub fn build_deny_reply(
    request_header: &RoutedRequestHeader,
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

/// Build a metadata reply carrying `payload` behind a success result section.
///
/// The SDK strips a result section off exactly the replies whose operation is
/// [`iggy_binary_protocol::Operation::is_result_framed`] (every metadata op plus the
/// four consumer-offset ops), and a non-empty `Register`, which it handles on its
/// own. For those, a payload missing the leading zero count has its first four bytes
/// eaten as a result count, and the decode fails or, worse, succeeds on the shifted
/// remainder: the raw-PAT reply shipped once without the prefix and broke the SDK.
///
/// The only way to BUILD a result-framed success body, though not the only path to a
/// success reply with one: [`build_reply_from_bytes`] passes a committed body
/// through, framed or not according to the operation. Framing a reply whose operation
/// is not result-framed breaks decoding just as badly, so the choice belongs with the
/// operation rather than here.
fn build_result_framed_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    payload: &impl WireEncode,
) -> Message<ReplyHeader> {
    let mut encoded = BytesMut::with_capacity(payload.encoded_size());
    payload.encode(&mut encoded);
    build_reply_with_body(
        request_header,
        client_id,
        session,
        commit,
        RESULT_COUNT_LEN + encoded.len(),
        |out| {
            let (count, body) = out.split_at_mut(RESULT_COUNT_LEN);
            count.copy_from_slice(&0u32.to_le_bytes());
            body.copy_from_slice(&encoded);
        },
    )
}

pub fn build_login_register_reply(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    user_id: u32,
) -> Message<ReplyHeader> {
    // A transient Register instead ships a `[count=1][index=0]
    // [TransientNotCommitted]` frame (`build_transient_reply`), which the SDK
    // decodes and replays.
    let payload = LoginRegisterResponse {
        user_id,
        session,
        server_protocol_version: IGGY_PROTOCOL_VERSION,
        server_version: WireName::new(SERVER_VERSION).expect("SERVER_VERSION is 1-255 bytes"),
    };
    build_result_framed_reply(request_header, client_id, session, commit, &payload)
}

pub fn build_reply_from_bytes(
    request_header: &RoutedRequestHeader,
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

/// The reply body past the generic header, bounded by the header's `size`
/// rather than by the buffer length: `size` is the frame's authoritative
/// extent, so a short frame reads as "no result section" instead of into
/// allocation padding.
#[must_use]
pub fn reply_body(reply: &Message<GenericHeader>) -> &[u8] {
    let size = reply.header().size as usize;
    reply
        .as_slice()
        .get(std::mem::size_of::<ReplyHeader>()..size)
        .unwrap_or_default()
}

/// The header of a SUCCESSFULLY COMMITTED metadata reply, or `None` when the
/// frame promises the caller nothing.
///
/// Three checks, in this order, and both callers need all three:
///
/// - an eviction is an `EvictionHeader` whose bytes would cast cleanly as a
///   `ReplyHeader`, so the command is checked FIRST: casting it would both
///   swallow the eviction and grade it as a commit;
/// - a request-level denial names itself in `ReplyHeader.status`, the channel
///   the SDK peeks before body decode (see [`build_deny_reply`]);
/// - a nonzero result section is a rejection, transient or committed. Every
///   reply here is result-framed (`Operation::is_result_framed` covers the
///   metadata ops; the partition plane grades through
///   `classify_partition_reply` instead), so a missing section is a malformed
///   frame, not a bare payload.
///
/// The read-your-writes floor and the raw-PAT splice both hang off exactly
/// this predicate - the floor must not advance on a frame that committed
/// nothing, and the token must not be grafted onto a rejection body - so they
/// share one implementation rather than two that have to stay in step.
///
/// A frame too short to hold a header, or one whose header will not cast, is
/// `None` with a warning: it is malformed, and the alternative is a panic on
/// the reply path.
#[must_use]
pub fn committed_reply_header(reply: &Message<GenericHeader>) -> Option<&ReplyHeader> {
    if reply.header().command != Command::Reply {
        return None;
    }
    let Some(bytes) = reply.as_slice().get(..std::mem::size_of::<ReplyHeader>()) else {
        warn!(
            size = reply.header().size,
            "metadata reply shorter than its own header"
        );
        return None;
    };
    let header = match bytemuck::checked::try_from_bytes::<ReplyHeader>(bytes) {
        Ok(header) => header,
        Err(error) => {
            warn!(?error, "metadata reply header failed to cast");
            return None;
        }
    };
    if header.status != 0 || result_code(reply_body(reply)) != Some(0) {
        return None;
    }
    Some(header)
}

/// The transient variant of a reply-shaped pre-consensus rejection frame
/// (`[count=1][index=0][code]`, see `build_result_rejection_reply`), or `None`
/// for a committed outcome. Either transient means the op did not commit, so
/// the write path must replay the same request id rather than grade it as a
/// committed result or advance the session gate. The two codes are kept
/// distinct because they exhaust differently: `TransientNotAccepted` never
/// entered the pipeline and is safe to re-issue anywhere, while
/// `TransientNotCommitted` may still commit and only a same-session same-id
/// replay is safe.
///
/// Lives here rather than in the HTTP reply module both planes' write paths
/// grade through: the dispatch spine needs it too, and importing it from
/// `http` would close a module cycle.
#[must_use]
pub fn transient_code(reply: &Message<GenericHeader>) -> Option<IggyError> {
    match result_code(reply_body(reply)) {
        Some(code) if code == IggyError::TransientNotCommitted.as_code() => {
            Some(IggyError::TransientNotCommitted)
        }
        Some(code) if code == IggyError::TransientNotAccepted.as_code() => {
            Some(IggyError::TransientNotAccepted)
        }
        _ => None,
    }
}

/// If a raw PAT token was minted (`CreatePersonalAccessToken`) and the commit
/// succeeded, replace the committed reply -- whose body is empty because the
/// raw token never entered consensus -- with a `RawPersonalAccessTokenResponse`,
/// reusing the confirmed commit position from the committed reply. Otherwise
/// (no token, a committed business rejection, or an eviction frame) the
/// committed reply passes through unchanged.
pub fn build_raw_pat_reply(
    request_header: &RoutedRequestHeader,
    committed: Message<GenericHeader>,
    raw_token: Option<String>,
) -> Result<Message<GenericHeader>, IggyError> {
    let Some(raw) = raw_token else {
        return Ok(committed);
    };
    // Only a genuine committed success gets the secret spliced in. An eviction
    // frame (a `CreatePersonalAccessToken` whose session died between bind and
    // request), a request-level denial, and a rejection result section all pass
    // through untouched, so the client decodes the typed outcome - or, for a
    // transient, replays - instead of having a raw token grafted onto a
    // rejection body whose hash never committed.
    let Some(commit) = committed_reply_header(&committed).map(|header| header.commit) else {
        return Ok(committed);
    };
    let token = WireName::new(raw.as_str()).map_err(|_| IggyError::InvalidFormat)?;
    let response = RawPersonalAccessTokenResponse { token };
    let reply = build_result_framed_reply(
        request_header,
        request_header.client,
        request_header.session,
        commit,
        &response,
    );
    Ok(reply.into_generic())
}

pub fn build_reply_with_body(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    body_len: usize,
    write_body: impl FnOnce(&mut [u8]),
) -> Message<ReplyHeader> {
    let header_len = std::mem::size_of::<ReplyHeader>();
    let total_size = header_len + body_len;
    let size = u32::try_from(total_size).expect("reply size must fit into u32");
    let mut reply = Message::<ReplyHeader>::new(total_size);
    let header = reply_header(request_header, client_id, session, commit, size);
    reply.as_mut_slice()[..header_len].copy_from_slice(bytemuck::bytes_of(&header));
    write_body(&mut reply.as_mut_slice()[header_len..total_size]);
    reply
}

/// The header of a `size`-byte reply frame answering `request_header`.
fn reply_header(
    request_header: &RoutedRequestHeader,
    client_id: u128,
    session: u64,
    commit: u64,
    size: u32,
) -> ReplyHeader {
    ReplyHeader {
        cluster: request_header.cluster,
        size,
        view: request_header.view,
        release: request_header.release,
        command: Command::Reply,
        replica: request_header.replica,
        request_checksum: request_header.request_checksum,
        client: client_id,
        op: session,
        commit,
        timestamp: request_header.timestamp,
        request: request_header.request,
        operation: request_header.operation,
        ..Default::default()
    }
}

pub fn current_metadata_commit<B, MJ, S, SB>(shard: &Rc<ShellShard<B, MJ, S, SB>>) -> u64
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    shard
        .plane
        .metadata()
        .consensus
        .as_ref()
        .map_or(0, VsrConsensus::commit_max)
}

/// Body head of a `PolledMessages` reply:
/// `[partition_id:4][current_offset:8][count:4]`, before the batch records.
const POLLED_HEAD_LEN: usize = 16;

/// Build the `PolledMessages` reply for the wire as a vectored frame: one
/// buffer holding the reply header and the body head, then the poll
/// fragments as they are. The record bytes are never copied or gathered;
/// their reply encoding IS the storage encoding (see
/// [`build_polled_messages_body`]), so `count` comes from walking the batch
/// headers in place. At-rest decryption is the one case that must rewrite
/// records, and it takes the flattening builder instead.
pub fn build_polled_messages_reply(
    request_header: &RoutedRequestHeader,
    commit: u64,
    partition_id: u32,
    current_offset: u64,
    fragments: PollFragments,
    encryptor: Option<&EncryptorKind>,
) -> Result<BusMessage, IggyError> {
    let client_id = request_header.client;
    let session = request_header.session;
    if encryptor.is_some() {
        let body = build_polled_messages_body(partition_id, current_offset, fragments, encryptor)?;
        let reply = build_reply_from_bytes(request_header, client_id, session, commit, &body);
        return Ok(reply.into_generic().into_frozen().into());
    }

    let mut frames = ResponseFragments::with_capacity(fragments.len() + 1);
    frames.extend(fragments.into_iter().map(Fragment::into_frozen));
    let count = polled_message_count(&frames)?;
    let records_len: usize = frames.iter().map(Frozen::len).sum();

    let header_len = std::mem::size_of::<ReplyHeader>();
    let size = u32::try_from(header_len + POLLED_HEAD_LEN + records_len)
        .map_err(|_| IggyError::InvalidCommand)?;
    let header = reply_header(request_header, client_id, session, commit, size);
    let mut head = Owned::<MESSAGE_ALIGN>::zeroed(header_len + POLLED_HEAD_LEN);
    let (header_bytes, body_head) = head.as_mut_slice().split_at_mut(header_len);
    header_bytes.copy_from_slice(bytemuck::bytes_of(&header));
    body_head[..4].copy_from_slice(&partition_id.to_le_bytes());
    body_head[4..12].copy_from_slice(&current_offset.to_le_bytes());
    body_head[12..].copy_from_slice(&count.to_le_bytes());
    frames.insert(0, head.into());

    // Re-checks the header and that the fragments cover `size`.
    Message::<ReplyHeader, ResponseBacking>::try_from(frames)
        .map(Message::into_inner)
        .map_err(|_| IggyError::InvalidCommand)
}

/// Sum of `message_count` over the batch records spanning `fragments`, read
/// from each batch header in place. Rejects a stream that is not a whole
/// number of batches, as [`build_polled_messages_body`] does.
fn polled_message_count(fragments: &[Frozen<MESSAGE_ALIGN>]) -> Result<u32, IggyError> {
    let mut cursor = FragmentCursor::new(fragments);
    let mut count = 0u32;
    let mut header = [0u8; send_messages::COMMAND_HEADER_SIZE];
    while !cursor.is_exhausted() {
        cursor.read_exact(&mut header)?;
        let batch =
            send_messages::BatchHeader::decode(&header).map_err(|_| IggyError::InvalidCommand)?;
        cursor.skip(batch.blob_len().map_err(|_| IggyError::InvalidCommand)?)?;
        count = count
            .checked_add(batch.message_count)
            .ok_or(IggyError::InvalidCommand)?;
    }
    Ok(count)
}

/// Byte cursor over the virtual concatenation of `fragments`. Rests on an
/// unread byte or at the end of the stream, never inside an exhausted
/// fragment, so a batch header split across fragments reads the same as one
/// stored whole.
struct FragmentCursor<'a> {
    fragments: &'a [Frozen<MESSAGE_ALIGN>],
    index: usize,
    offset: usize,
}

impl<'a> FragmentCursor<'a> {
    fn new(fragments: &'a [Frozen<MESSAGE_ALIGN>]) -> Self {
        let mut cursor = Self {
            fragments,
            index: 0,
            offset: 0,
        };
        cursor.settle();
        cursor
    }

    const fn is_exhausted(&self) -> bool {
        self.index == self.fragments.len()
    }

    fn read_exact(&mut self, out: &mut [u8]) -> Result<(), IggyError> {
        let mut filled = 0;
        while filled < out.len() {
            let available = self.available()?;
            let take = available.len().min(out.len() - filled);
            out[filled..filled + take].copy_from_slice(&available[..take]);
            filled += take;
            self.advance(take);
        }
        Ok(())
    }

    fn skip(&mut self, mut len: usize) -> Result<(), IggyError> {
        while len > 0 {
            let take = self.available()?.len().min(len);
            len -= take;
            self.advance(take);
        }
        Ok(())
    }

    /// Unread bytes of the current fragment; `Err` past the end of the stream.
    fn available(&self) -> Result<&'a [u8], IggyError> {
        self.fragments
            .get(self.index)
            .map(|fragment| &fragment.as_slice()[self.offset..])
            .ok_or(IggyError::InvalidCommand)
    }

    fn advance(&mut self, len: usize) {
        self.offset += len;
        self.settle();
    }

    /// Step past the current fragment once it is used up, and past empty ones.
    fn settle(&mut self) {
        while let Some(fragment) = self.fragments.get(self.index) {
            if self.offset < fragment.len() {
                break;
            }
            self.offset = 0;
            self.index += 1;
        }
    }
}

/// Build the `PolledMessages` reply body from the owning shard's poll
/// fragments, gathered into one buffer.
///
/// Fragments carry the stored batch records (a 256-byte batch header plus
/// `[48B header][payload][user_headers]` frames, deltas resolved against the
/// stamped bases) and are served to the client as they are - the reply's
/// message encoding IS the storage encoding. The one rewrite left is at-rest
/// decryption: stored sections are ciphertext, and this reply is the single
/// decrypt point, so encrypted records are rebuilt over the plaintext.
///
/// The binary transports reply through [`build_polled_messages_reply`], which
/// ships the fragments without gathering them; this builder serves the
/// decrypt path and the HTTP handler, which decodes the body into JSON.
///
/// Body layout: `[partition_id:4][current_offset:8][count:4][batch records...]`.
pub fn build_polled_messages_body(
    partition_id: u32,
    current_offset: u64,
    fragments: PollFragments,
    encryptor: Option<&EncryptorKind>,
) -> Result<Bytes, IggyError> {
    // Body head: [partition_id:4][current_offset:8][count:4]. `count` sits at
    // COUNT_OFFSET and is backpatched once the walk below knows it.
    const HEAD_LEN: usize = 16;
    const COUNT_OFFSET: usize = 12;
    // Batches may arrive split across fragments (rewritten batch header +
    // sliced blob); concatenate into one stream before walking records.
    let mut stream: Vec<u8> = Vec::new();
    for fragment in fragments {
        let frozen = fragment.into_frozen();
        stream.extend_from_slice(frozen.as_slice());
    }

    let mut body: Vec<u8> = Vec::with_capacity(HEAD_LEN + stream.len());
    body.extend_from_slice(&partition_id.to_le_bytes());
    body.extend_from_slice(&current_offset.to_le_bytes());
    body.extend_from_slice(&[0u8; 4]); // count placeholder, backpatched below
    let mut count: u32 = 0;
    let mut position = 0usize;
    while position < stream.len() {
        let batch = send_messages::BatchHeader::decode(&stream[position..])
            .map_err(|_| IggyError::InvalidCommand)?;
        let batch_end = position
            .checked_add(batch.total_size())
            .ok_or(IggyError::InvalidCommand)?;
        if batch_end > stream.len() {
            return Err(IggyError::InvalidCommand);
        }
        let record = &stream[position..batch_end];
        if let Some(encryptor) = encryptor {
            let decrypted = send_messages::decrypt_batch_record(record, encryptor)?;
            body.extend_from_slice(&decrypted);
        } else {
            body.extend_from_slice(record);
        }
        count = count
            .checked_add(batch.message_count)
            .ok_or(IggyError::InvalidCommand)?;
        position = batch_end;
    }

    body[COUNT_OFFSET..HEAD_LEN].copy_from_slice(&count.to_le_bytes());
    Ok(Bytes::from(body))
}

/// Build the `ConsumerOffsetResponse` reply body:
/// `[partition_id:4][current_offset:8][stored_offset:8]`.
pub fn build_consumer_offset_body(
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

    fn pat_request_header() -> RoutedRequestHeader {
        let zeroed = [0u8; std::mem::size_of::<RoutedRequestHeader>()];
        let mut header = *bytemuck::checked::try_from_bytes::<RoutedRequestHeader>(&zeroed)
            .expect("zeroed bytes form a valid RoutedRequestHeader");
        header.command = Command::Request;
        header.operation = Operation::CreatePersonalAccessToken;
        header.client = 42;
        header.session = 7;
        header.request = 3;
        header
    }

    #[test]
    fn login_register_reply_carries_the_success_result_prefix() {
        // The other `build_result_framed_reply` caller. The SDK strips the result
        // section off every metadata reply, so a payload emitted without the prefix
        // loses its first four bytes to a phantom result count -- the decode break
        // the raw-PAT reply shipped once. Pin it on both callers, not just the one
        // that regressed.
        let mut header = pat_request_header();
        header.operation = Operation::Register;
        let reply = build_login_register_reply(&header, 42, 7, 9, 5);

        let header_len = std::mem::size_of::<ReplyHeader>();
        let body = &reply.as_slice()[header_len..reply.header().size as usize];
        assert_eq!(result_code(body), Some(0));

        let payload = LoginRegisterResponse::decode_from(&body[RESULT_COUNT_LEN..])
            .expect("login-register payload decodes past the result section");
        assert_eq!(payload.user_id, 5);
        assert_eq!(payload.session, 7);
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

    // Vectored `PolledMessages` replies against the flattening builder as the
    // byte-for-byte oracle.

    use iggy_common::Aes256GcmEncryptor;
    use server_common::send_messages::{
        BatchHeader, COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages,
        PREPARE_SPLIT_POINT, SendMessagesOwned, encrypt_batch_request, frozen_batch_header,
    };
    use server_common::sharding::IggyNamespace;

    const POLL_PARTITION_ID: u32 = 9;
    const POLL_CURRENT_OFFSET: u64 = 1_234;
    const POLL_COMMIT: u64 = 17;

    fn poll_request_header() -> RoutedRequestHeader {
        pat_request_header()
    }

    /// A stored batch record over an opaque blob. Both builders decode only
    /// the 256-byte batch header, so the blob needs no message framing.
    fn batch_record(base_offset: u64, message_count: u32, blob: &[u8]) -> Frozen<MESSAGE_ALIGN> {
        let batch_length = u64::try_from(COMMAND_HEADER_SIZE + blob.len()).expect("fits u64");
        let mut header =
            BatchHeader::new(u64::from(POLL_PARTITION_ID), 5, batch_length, message_count);
        header.base_offset = base_offset;
        let mut bytes = vec![0u8; COMMAND_HEADER_SIZE + blob.len()];
        header.encode_into(&mut bytes[..COMMAND_HEADER_SIZE]);
        bytes[COMMAND_HEADER_SIZE..].copy_from_slice(blob);
        Owned::<MESSAGE_ALIGN>::copy_from_slice(&bytes).into()
    }

    /// The wire bytes the flattening builder ships for `fragments`.
    fn flattened_reply(fragments: PollFragments, encryptor: Option<&EncryptorKind>) -> Vec<u8> {
        let header = poll_request_header();
        let body = build_polled_messages_body(
            POLL_PARTITION_ID,
            POLL_CURRENT_OFFSET,
            fragments,
            encryptor,
        )
        .expect("flattening builder accepts the fragments");
        build_reply_from_bytes(&header, header.client, header.session, POLL_COMMIT, &body)
            .into_generic()
            .into_frozen()
            .as_slice()
            .to_vec()
    }

    fn vectored_reply(
        fragments: PollFragments,
        encryptor: Option<&EncryptorKind>,
    ) -> Result<BusMessage, IggyError> {
        build_polled_messages_reply(
            &poll_request_header(),
            POLL_COMMIT,
            POLL_PARTITION_ID,
            POLL_CURRENT_OFFSET,
            fragments,
            encryptor,
        )
    }

    /// The vectored reply must be byte-identical to the flattened one and
    /// ship exactly `fragment_count` buffers.
    fn assert_vectored_matches_flattened(fragments: PollFragments, fragment_count: usize) {
        let expected = flattened_reply(fragments.clone(), None);
        let reply =
            vectored_reply(fragments, None).expect("vectored builder accepts the fragments");
        assert_eq!(reply.fragments().len(), fragment_count);
        assert_eq!(reply.total_len(), expected.len());
        assert_eq!(reply.into_contiguous().as_slice(), expected.as_slice());
    }

    fn polled_count(reply: &[u8]) -> u32 {
        let count_at = std::mem::size_of::<ReplyHeader>() + 12;
        u32::from_le_bytes(reply[count_at..count_at + 4].try_into().expect("4 bytes"))
    }

    #[test]
    fn polled_reply_single_fragment_matches_flattened_builder() {
        let record = batch_record(0, 3, &[0xAB; 100]);
        let fragments = PollFragments::from_iter([Fragment::whole(record)]);
        assert_vectored_matches_flattened(fragments.clone(), 2);

        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &reply.as_slice()[..std::mem::size_of::<ReplyHeader>()],
        )
        .expect("reply header decodes");
        assert_eq!(header.size as usize, reply.len());
        assert_eq!(header.client, 42);
        assert_eq!(header.op, 7);
        assert_eq!(header.commit, POLL_COMMIT);
        assert_eq!(polled_count(reply.as_slice()), 3);
    }

    #[test]
    fn polled_reply_split_batch_matches_flattened_builder() {
        // The journal slices a partially selected batch into a rewritten header
        // plus a blob slice, exactly how `push_selected_batch_fragments` does.
        let source = batch_record(10, 4, &[0x11; 400]);
        let (start, end) = (100, 300);
        let batch_length = u64::try_from(COMMAND_HEADER_SIZE + (end - start)).expect("fits u64");
        let mut rewritten = BatchHeader::new(u64::from(POLL_PARTITION_ID), 5, batch_length, 2);
        rewritten.base_offset = 10;
        let fragments = PollFragments::from_iter([
            Fragment::whole(frozen_batch_header(&rewritten)),
            Fragment::slice(
                source,
                COMMAND_HEADER_SIZE + start,
                COMMAND_HEADER_SIZE + end,
            ),
        ]);
        assert_vectored_matches_flattened(fragments.clone(), 3);
        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(polled_count(reply.as_slice()), 2);
    }

    #[test]
    fn polled_reply_multiple_batches_counts_every_header() {
        let first = batch_record(0, 1, &[0x01; 50]);
        let second = batch_record(1, 4, &[0x02; 700]);
        let third = batch_record(5, 7, &[0x03; 20]);
        // `second` arrives cut mid-header so the count walk has to read a batch
        // header spanning two fragments.
        let fragments = PollFragments::from_iter([
            Fragment::whole(first),
            Fragment::slice(second.clone(), 0, 100),
            Fragment::slice(second.clone(), 100, second.len()),
            Fragment::whole(third),
        ]);
        assert_vectored_matches_flattened(fragments.clone(), 5);
        let reply = vectored_reply(fragments, None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(polled_count(reply.as_slice()), 12);
    }

    #[test]
    fn polled_reply_empty_poll_is_the_head_alone() {
        assert_vectored_matches_flattened(PollFragments::new(), 1);
        let reply = vectored_reply(PollFragments::new(), None)
            .expect("reply")
            .into_contiguous();
        assert_eq!(
            reply.len(),
            std::mem::size_of::<ReplyHeader>() + POLLED_HEAD_LEN
        );
        assert_eq!(polled_count(reply.as_slice()), 0);
    }

    #[test]
    fn polled_reply_rejects_a_truncated_record() {
        let record = batch_record(0, 3, &[0xAB; 100]);
        let truncated =
            PollFragments::from_iter([Fragment::slice(record, 0, COMMAND_HEADER_SIZE + 99)]);
        assert!(matches!(
            build_polled_messages_body(
                POLL_PARTITION_ID,
                POLL_CURRENT_OFFSET,
                truncated.clone(),
                None
            ),
            Err(IggyError::InvalidCommand)
        ));
        assert!(matches!(
            vectored_reply(truncated, None),
            Err(IggyError::InvalidCommand)
        ));
    }

    /// A stored record encrypted the way the primary encrypts at ingestion.
    fn encrypted_record(encryptor: &EncryptorKind) -> Frozen<MESSAGE_ALIGN> {
        let namespace = IggyNamespace::new(1, 1, 3);
        let mut messages = IggyMessages::with_capacity(2);
        for (id, payload) in [(7u128, &b"first-payload"[..]), (8, &b"second-payload"[..])] {
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    id,
                    origin_timestamp: 1_000,
                    ..Default::default()
                },
                payload: Bytes::copy_from_slice(payload),
                user_headers: None,
            });
        }
        let owned = SendMessagesOwned::from_messages(namespace, &messages).expect("build batch");
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let total = header_size + owned.header.total_size();
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total);
        {
            let header: &mut RoutedRequestHeader =
                bytemuck::checked::try_from_bytes_mut(&mut buffer.as_mut_slice()[..header_size])
                    .expect("zeroed bytes form a valid RoutedRequestHeader");
            header.command = Command::Request;
            header.operation = Operation::SendMessages;
            header.client = 1;
            header.session = 1;
            header.request = 1;
            header.size = u32::try_from(total).expect("size fits u32");
        }
        let bytes = buffer.as_mut_slice();
        owned
            .header
            .encode_into(&mut bytes[header_size..header_size + COMMAND_HEADER_SIZE]);
        bytes[PREPARE_SPLIT_POINT..].copy_from_slice(&owned.blob);
        let canonical = Message::try_from(buffer).expect("request message is valid");
        let encrypted = encrypt_batch_request(canonical, encryptor).expect("encrypt batch");
        let record = &encrypted.as_slice()[header_size..encrypted.header().size as usize];
        Owned::<MESSAGE_ALIGN>::copy_from_slice(record).into()
    }

    #[test]
    fn polled_reply_encrypted_records_take_the_flattening_path() {
        let encryptor =
            EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&[7u8; 32]).expect("valid 32B key"));
        let fragments = PollFragments::from_iter([Fragment::whole(encrypted_record(&encryptor))]);
        let expected = flattened_reply(fragments.clone(), Some(&encryptor));
        let reply = vectored_reply(fragments, Some(&encryptor)).expect("decrypting reply");
        assert_eq!(reply.fragments().len(), 1);
        assert_eq!(reply.into_contiguous().as_slice(), expected.as_slice());
        assert_eq!(polled_count(&expected), 2);
    }
}
