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
use crate::namespace::{ensure_topic_exists, resolve_topic, stream_not_found};
use crate::reply_frame::{build_empty_reply, build_reply_from_bytes};
use crate::session_manager::SessionManager;
use crate::shell::{ShellBus, ShellShard};
use crate::sysinfo_probe::probe_system_stats;
use crate::wire::{transport_kind_to_wire, usize_to_u32};
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_GROUP_CODE,
    GET_CONSUMER_GROUPS_CODE, GET_PARTITIONS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE, GET_STATS_CODE,
    GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE,
    GET_USERS_CODE,
};
use iggy_binary_protocol::primitives::page::{WirePageCursor, WirePageQuery};
use iggy_binary_protocol::requests::consumer_groups::{
    GetConsumerGroupRequest, GetConsumerGroupsRequest,
};
use iggy_binary_protocol::requests::partitions::GetPartitionsRequest;
use iggy_binary_protocol::requests::personal_access_tokens::GetPersonalAccessTokensRequest;
use iggy_binary_protocol::requests::streams::{GetStreamRequest, GetStreamsRequest};
use iggy_binary_protocol::requests::system::{
    DescribeOptionsRequest, OPTIONS_SCOPE_STREAM, OPTIONS_SCOPE_TOPIC, OPTIONS_SCOPE_USER,
};
use iggy_binary_protocol::requests::topics::{GetTopicRequest, GetTopicsRequest};
use iggy_binary_protocol::requests::users::{GetUserRequest, GetUsersRequest};
use iggy_binary_protocol::responses::PageResponse;
use iggy_binary_protocol::responses::clients::client_response::ClientResponse;
use iggy_binary_protocol::responses::clients::client_response::ConsumerGroupInfoResponse;
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::consumer_groups::ConsumerGroupResponse;
use iggy_binary_protocol::responses::personal_access_tokens::get_personal_access_tokens::{
    GetPersonalAccessTokensResponse, PersonalAccessTokenResponse,
};
use iggy_binary_protocol::responses::streams::StreamResponse;
use iggy_binary_protocol::responses::streams::get_stream::{GetStreamResponse, TopicResponse};
use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
use iggy_binary_protocol::responses::system::get_cluster_metadata::{
    ClusterMetadataResponse, ClusterNodeResponse,
};
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;
use iggy_binary_protocol::responses::topics::get_topic::{GetTopicResponse, PartitionResponse};
use iggy_binary_protocol::responses::users::get_user::UserDetailsResponse;
use iggy_binary_protocol::responses::users::user_response::UserResponse;
use iggy_binary_protocol::{
    ReplyHeader, RoutedRequestHeader, WireDecode, WireEncode, WireIdentifier, WireName, WireOptions,
};
use iggy_common::wire_conversions::{describe_options_response, resource_options_to_wire_split};
use iggy_common::{
    GeoMode, HeaderValue, IggyError, IggyTimestamp, NamedPageQuery, OptionKey, OptionsScope,
    PageCursor, PageOrder, PageQuery, Permissions, ResourceOptions, UserStatus,
    project_shadow_origin, project_shadow_topic, public_options,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::change_log::{MAX_PAGE_BYTES, MAX_SCANNED_ENTRIES};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::{GrantsByCreation, ReadGrants};
use metadata::stm::stream::{ResourceCounts, Stream, StreamsInner, Topic};
use metadata::stm::user::{User, UsersInner};
use metadata::stm::walk::slice_after;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shard::ConnectedClientInfo;
use std::borrow::Cow;
use std::cell::RefCell;
use std::net::IpAddr;
use std::rc::Rc;
use std::sync::Arc;
use tracing::warn;

/// The stored fields a stream or topic read returns. A page projects them
/// under the metadata read guard with the options borrowed, because it holds
/// at most `MAX_PAGE_LIMIT` items. A detail read copies them with
/// [`Self::into_owned`] under the guard and projects the options after it,
/// because the writer publishing the next apply waits for every reader to
/// leave.
struct StoredEntry<'a> {
    id: usize,
    created_at: IggyTimestamp,
    /// Topics of a stream, partitions of a topic.
    children_count: usize,
    size_bytes: u64,
    messages_count: u64,
    name: Arc<str>,
    options: Cow<'a, ResourceOptions>,
    /// The origin of a shadow stream. `None` for any other stream and for a
    /// topic.
    shadow_origin: Option<String>,
    /// The peer's geo mode of a topic in a shadow stream. `None` for any
    /// other topic and for a stream.
    shadow_geo_mode: Option<GeoMode>,
}

/// The stored fields a user read returns, read like [`StoredEntry`].
struct StoredUser<'a> {
    id: u32,
    created_at: IggyTimestamp,
    status: UserStatus,
    username: Arc<str>,
    options: Cow<'a, ResourceOptions>,
}

impl StoredEntry<'_> {
    fn into_owned(self) -> StoredEntry<'static> {
        StoredEntry {
            id: self.id,
            created_at: self.created_at,
            children_count: self.children_count,
            size_bytes: self.size_bytes,
            messages_count: self.messages_count,
            name: self.name,
            options: Cow::Owned(self.options.into_owned()),
            shadow_origin: self.shadow_origin,
            shadow_geo_mode: self.shadow_geo_mode,
        }
    }
}

impl StoredUser<'_> {
    fn into_owned(self) -> StoredUser<'static> {
        StoredUser {
            id: self.id,
            created_at: self.created_at,
            status: self.status,
            username: self.username,
            options: Cow::Owned(self.options.into_owned()),
        }
    }
}

impl<'a> From<&'a Stream> for StoredEntry<'a> {
    fn from(stream: &'a Stream) -> Self {
        Self {
            id: stream.id,
            created_at: stream.created_at,
            children_count: stream.topics.len(),
            size_bytes: stream.stats.size_bytes_inconsistent(),
            messages_count: stream.stats.messages_count_inconsistent(),
            name: Arc::clone(&stream.name),
            options: Cow::Borrowed(&stream.options),
            shadow_origin: stream.shadow.as_ref().map(|source| source.origin.clone()),
            shadow_geo_mode: None,
        }
    }
}

impl<'a> From<&'a Topic> for StoredEntry<'a> {
    fn from(topic: &'a Topic) -> Self {
        Self {
            id: topic.id,
            created_at: topic.created_at,
            children_count: topic.partitions.len(),
            size_bytes: topic.stats.size_bytes_inconsistent(),
            messages_count: topic.stats.messages_count_inconsistent(),
            name: Arc::clone(&topic.name),
            options: Cow::Borrowed(&topic.options),
            shadow_origin: None,
            shadow_geo_mode: topic.shadow.map(|shadow| shadow.geo_mode),
        }
    }
}

impl<'a> From<&'a User> for StoredUser<'a> {
    fn from(user: &'a User) -> Self {
        Self {
            id: user.id,
            created_at: user.created_at,
            status: user.status,
            username: Arc::clone(&user.username),
            options: Cow::Borrowed(&user.options),
        }
    }
}

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
            .map_or_default(|pats| {
                pats.values()
                    .filter_map(|pat| {
                        Some(PersonalAccessTokenResponse {
                            name: WireName::new(pat.name.as_ref()).ok()?,
                            expiry_at: pat.expiry_at.map_or(0, |expiry| expiry.as_micros()),
                        })
                    })
                    .collect()
            });
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
        .map_or_default(|(vsr_client_id, _)| {
            shard
                .plane
                .metadata()
                .mux_stm
                .streams()
                .consumer_group_memberships(vsr_client_id)
        })
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
    // The transport client id is a u128 `shard | boot nonce | seq`; the wire
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

/// `user_id` is the authenticated caller, used by the identity-scoped reads
/// (the PAT list) and by the stream and topic lists, which return only the
/// entries the caller may read. Every other arm ignores it. Every other
/// authorization stays with the per-transport gates that run before this
/// builder. `client_ip`
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
            build_describe_options_response(
                body,
                shard
                    .plane
                    .metadata()
                    .default_replication_factor(shard.replica_topology().replica_count),
            )?
            .to_bytes(),
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
            build_get_stream_response(shard, &request.stream_id, user_id).map(|response| {
                response.map_or(NonReplicatedResponse::Empty, |response| {
                    NonReplicatedResponse::Bytes(response.to_bytes())
                })
            })
        }
        GET_STREAMS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_streams_response(shard, body, user_id)?.to_bytes(),
        )),
        GET_TOPIC_CODE => {
            let request =
                GetTopicRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            build_get_topic_response(shard, &request.stream_id, &request.topic_id, user_id).map(
                |response| {
                    response.map_or(NonReplicatedResponse::Empty, |response| {
                        NonReplicatedResponse::Bytes(response.to_bytes())
                    })
                },
            )
        }
        GET_TOPICS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_topics_response(shard, body, user_id)?.to_bytes(),
        )),
        GET_PARTITIONS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_partitions_response(shard, body)?.to_bytes(),
        )),
        GET_USERS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_get_users_response(shard, body)?.to_bytes(),
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
        GET_CONSUMER_GROUPS_CODE => Ok(NonReplicatedResponse::Bytes(
            build_consumer_groups_response(shard, body)?.to_bytes(),
        )),
        // A table-listed non-replicated code with no arm is a routing bug and
        // an unknown code is a client bug; the empty-ok that used to cover both
        // attested a read that never ran. Only the named arms return `Empty`,
        // and there it means "resolved to nothing" (the 404 the HTTP path maps).
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
) -> Result<PageResponse<ConsumerGroupResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request =
        GetConsumerGroupsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|streams| consumer_groups_page(streams, request, PAGE_BUDGET))
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
            .map(ClusterNodeResponse::from)
            .collect(),
    }
}

/// The node totals a stats read reports, taken from the committed counts and
/// the node stats, so the read walks no stream.
struct StatsTotals {
    counts: ResourceCounts,
    segments_count: u64,
    messages_size_bytes: u64,
    messages_count: u64,
}

/// Segments count every committed partition through the floor the detail
/// responses apply (see [`partition_response`]), not only the partitions that
/// materialised: a partition materialises a reconciler pass after its commit,
/// so the total would otherwise report fewer segments than `get_topic` does
/// and climb between two reads with no write in between. With a floor of one,
/// the sum of `max(segments, floor)` is the node sum plus the floor for each
/// partition without a segment of its own.
fn stats_totals(streams: &StreamsInner) -> StatsTotals {
    const _: () = assert!(MATERIALIZED_SEGMENTS_FLOOR == 1);
    let counts = streams.resource_counts();
    let node = streams.stats_registry.node();
    let without_segments = u64::try_from(counts.partitions)
        .unwrap_or(u64::MAX)
        .saturating_sub(node.partitions_with_segments_inconsistent());
    StatsTotals {
        counts,
        segments_count: node
            .segments_count_inconsistent()
            .saturating_add(without_segments),
        messages_size_bytes: node.size_bytes_inconsistent(),
        messages_count: node.messages_count_inconsistent(),
    }
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
    let totals = shard.plane.metadata().mux_stm.streams().read(stats_totals);

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
        messages_size_bytes: totals.messages_size_bytes,
        streams_count: usize_to_u32(totals.counts.streams)?,
        topics_count: usize_to_u32(totals.counts.topics)?,
        partitions_count: usize_to_u32(totals.counts.partitions)?,
        segments_count: u32::try_from(totals.segments_count).unwrap_or(u32::MAX),
        messages_count: totals.messages_count,
        clients_count,
        consumer_groups_count: usize_to_u32(totals.counts.consumer_groups)?,
        hostname: system.hostname,
        os_name: system.os_name,
        os_version: system.os_version,
        kernel_version: system.kernel_version,
        iggy_server_version: crate::VERSION.to_owned(),
        iggy_server_semver: crate::SEMANTIC_VERSION.get_numeric_version().ok(),
        cache_metrics: Vec::new(),
        threads_count: system.threads_count,
        free_disk_space: system.free_disk_space,
        total_disk_space: system.total_disk_space,
        open_files_count: system.open_files_count,
        open_files_limit: system.open_files_limit,
    })
}

/// The stream `user_id` may read, checked against grants of the same
/// checkpoint so a reused id never serves the grant of its predecessor.
fn build_get_stream_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    user_id: Option<u32>,
) -> Result<Option<GetStreamResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    let stored = shard.plane.metadata().mux_stm.read_scoped_page(
        user_id,
        |streams, permissions, _| -> Result<_, IggyError> {
            let Some(stream_id) = streams.resolve_stream_id(stream_id) else {
                return Ok(None);
            };
            if !ReadGrants::for_stream(permissions, Some(stream_id)).reads_stream() {
                return Err(IggyError::Unauthorized);
            }
            let stream = streams
                .items
                .get(stream_id)
                .ok_or(IggyError::InvalidIdentifier)?;
            Ok(Some((
                StoredEntry::from(stream.as_ref()).into_owned(),
                stored_topics(stream),
            )))
        },
    )?;
    let Some((stream, topics)) = stored else {
        return Ok(None);
    };
    Ok(Some(GetStreamResponse {
        stream: stream_response(&stream)?,
        topics: listed_topics(stream.id, &topics)?,
    }))
}

/// One page of the streams `user_id` may read, the ones `GetStream` serves it.
fn build_get_streams_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
    user_id: Option<u32>,
) -> Result<PageResponse<StreamResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    let request = GetStreamsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let metadata = shard.plane.metadata();
    let (page, skipped) =
        metadata
            .mux_stm
            .read_scoped_page(user_id, |streams, permissions, ordered| {
                streams_page(streams, request.page, PAGE_BUDGET, permissions, ordered)
            })?;
    log_skipped(&skipped);
    Ok(page)
}

fn stored_topics(stream: &Stream) -> Vec<StoredEntry<'static>> {
    stream
        .topics
        .iter()
        .map(|(_, topic)| StoredEntry::from(topic).into_owned())
        .collect()
}

/// Every topic of stream `stream_id` a list read serves, see [`servable`].
fn listed_topics(
    stream_id: usize,
    topics: &[StoredEntry<'_>],
) -> Result<Vec<TopicResponse>, IggyError> {
    let mut skipped = Vec::new();
    let listed = topics
        .iter()
        .filter_map(|topic| {
            servable(
                topic_response(topic),
                &mut skipped,
                OptionsScope::Topic,
                topic.id,
                Some(stream_id),
            )
            .transpose()
        })
        .collect();
    log_skipped(&skipped);
    listed
}

/// A list entry that this build cannot read, see [`servable`].
struct SkippedEntry {
    scope: OptionsScope,
    resource_id: usize,
    stream_id: Option<usize>,
    key: String,
}

/// A list entry, or `None` for a resource whose stored critical state this
/// build cannot read. That resource still fails its own read, but it must not
/// fail the list every other resource is served in. It lands in `skipped`, and
/// the caller logs it with [`log_skipped`] after the metadata read guard.
fn servable<T>(
    entry: Result<T, IggyError>,
    skipped: &mut Vec<SkippedEntry>,
    scope: OptionsScope,
    resource_id: usize,
    stream_id: Option<usize>,
) -> Result<Option<T>, IggyError> {
    match entry {
        Ok(entry) => Ok(Some(entry)),
        Err(IggyError::UnsupportedCriticalOption(key)) => {
            skipped.push(SkippedEntry {
                scope,
                resource_id,
                stream_id,
                key,
            });
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn log_skipped(skipped: &[SkippedEntry]) {
    for entry in skipped {
        let (scope, resource_id, key) = (entry.scope, entry.resource_id, &entry.key);
        warn!(
            scope = %scope,
            resource_id,
            stream_id = entry.stream_id,
            "Leaving {scope} {resource_id} out of the list, its critical option {key} is not readable by this build."
        );
    }
}

/// Serve the option catalog for one resource scope, with the replication
/// factor this node gives a topic created without one.
fn build_describe_options_response(
    body: &[u8],
    default_replication_factor: u8,
) -> Result<DescribeOptionsResponse, IggyError> {
    let request =
        DescribeOptionsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let scope = match request.scope {
        OPTIONS_SCOPE_TOPIC | OPTIONS_SCOPE_STREAM | OPTIONS_SCOPE_USER => {
            OptionsScope::from_code(request.scope)?
        }
        _ => return Err(IggyError::InvalidCommand),
    };
    describe_options_response(
        scope,
        &[(
            OptionKey::ReplicationFactor,
            HeaderValue::from(u32::from(default_replication_factor)),
        )],
    )
}

/// The explicit and derived blocks a read returns for `stored`.
fn public_blocks(
    scope: OptionsScope,
    stored: &ResourceOptions,
) -> Result<(WireOptions, WireOptions), IggyError> {
    resource_options_to_wire_split(&public_options(scope, stored)?)
}

fn user_response(user: &StoredUser<'_>) -> Result<UserResponse, IggyError> {
    let (options, derived_options) = public_blocks(OptionsScope::User, &user.options)?;
    Ok(UserResponse {
        id: user.id,
        created_at: user.created_at.as_micros(),
        status: user.status.as_code(),
        username: WireName::new(user.username.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
        options,
        derived_options,
    })
}

fn build_get_users_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
) -> Result<PageResponse<UserResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request = GetUsersRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let (page, skipped) = shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| users_page(users, request.page, PAGE_BUDGET))?;
    log_skipped(&skipped);
    Ok(page)
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
    let stored = shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| -> Result<_, IggyError> {
            let Some(id) = users.resolve_user_id(user_id) else {
                return Ok(None);
            };
            let user = users.items.get(id).ok_or(IggyError::InvalidIdentifier)?;
            Ok(Some((
                StoredUser::from(user).into_owned(),
                user.permissions.clone(),
            )))
        })?;
    let Some((user, permissions)) = stored else {
        return Ok(None);
    };
    Ok(Some(UserDetailsResponse {
        user: user_response(&user)?,
        permissions: permissions
            .as_deref()
            .map(iggy_common::wire_conversions::permissions_to_wire),
    }))
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

/// The topic `user_id` may read, checked like [`build_get_stream_response`].
fn build_get_topic_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    user_id: Option<u32>,
) -> Result<Option<GetTopicResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    let stored = shard.plane.metadata().mux_stm.read_scoped_page(
        user_id,
        |streams, permissions, _| -> Result<_, IggyError> {
            let Some(stream_id) = streams.resolve_stream_id(stream_id) else {
                return Ok(None);
            };
            let Some(topic_id) = streams.resolve_topic_id(stream_id, topic_id) else {
                return Ok(None);
            };
            if !ReadGrants::for_stream(permissions, Some(stream_id)).reads_topic(topic_id) {
                return Err(IggyError::Unauthorized);
            }
            let stream = streams
                .items
                .get(stream_id)
                .ok_or(IggyError::InvalidIdentifier)?;
            let topic = stream
                .topics
                .get(topic_id)
                .ok_or(IggyError::InvalidIdentifier)?;
            let nodes = shard.replica_topology().replica_count;
            let partitions = topic
                .partitions
                .iter()
                .map(|partition| {
                    partition_response(
                        streams,
                        stream_id,
                        topic_id,
                        partition,
                        shard.plane.metadata().applied_frontier().get(),
                        nodes,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Some((StoredEntry::from(topic).into_owned(), partitions)))
        },
    )?;
    let Some((topic, partitions)) = stored else {
        return Ok(None);
    };
    Ok(Some(GetTopicResponse {
        topic: topic_response(&topic)?,
        partitions,
    }))
}

/// One page of the topics of a stream `user_id` may read, the ones `GetTopic`
/// serves it.
fn build_get_topics_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
    user_id: Option<u32>,
) -> Result<PageResponse<TopicResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let user_id = user_id.ok_or(IggyError::Unauthenticated)?;
    let request = GetTopicsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let metadata = shard.plane.metadata();
    let (page, skipped) =
        metadata
            .mux_stm
            .read_scoped_page(user_id, |streams, permissions, ordered| {
                topics_page(streams, request, PAGE_BUDGET, permissions, ordered)
            })?;
    log_skipped(&skipped);
    Ok(page)
}

fn stream_response(stream: &StoredEntry<'_>) -> Result<StreamResponse, IggyError> {
    let mut public = public_options(OptionsScope::Stream, &stream.options)?;
    if let Some(origin) = &stream.shadow_origin {
        project_shadow_origin(&mut public, origin)?;
    }
    let (options, derived_options) = resource_options_to_wire_split(&public)?;
    Ok(StreamResponse {
        id: usize_to_u32(stream.id)?,
        created_at: stream.created_at.as_micros(),
        topics_count: usize_to_u32(stream.children_count)?,
        size_bytes: stream.size_bytes,
        messages_count: stream.messages_count,
        name: WireName::new(stream.name.as_ref()).map_err(|_| IggyError::InvalidFormat)?,
        options,
        derived_options,
    })
}

fn topic_response(topic: &StoredEntry<'_>) -> Result<TopicResponse, IggyError> {
    let mut public = public_options(OptionsScope::Topic, &topic.options)?;
    if let Some(geo_mode) = topic.shadow_geo_mode {
        project_shadow_topic(&mut public, geo_mode)?;
    }
    let (options, derived_options) = resource_options_to_wire_split(&public)?;
    Ok(TopicResponse {
        id: usize_to_u32(topic.id)?,
        created_at: topic.created_at.as_micros(),
        partitions_count: usize_to_u32(topic.children_count)?,
        size_bytes: topic.size_bytes,
        messages_count: topic.messages_count,
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
    metadata_op: u64,
    nodes: u8,
) -> Result<PartitionResponse, IggyError> {
    // Per-partition counters live in the shared stats registry (one `Arc`
    // across all shards and both left-right buffers).
    //
    // Registration is NOT materialization: the owning shard's reconciler mints
    // the entry (get-or-create in `fetch_partition_build_inputs`) before it
    // builds the partition, and `ensure_initial_segment` only bumps
    // `segments_count` once the segment file is open. So a registry MISS and a
    // registered entry still reading zero segments are the same thing to a
    // caller -- committed, not yet holding storage -- and both report the
    // deterministic shape every materialization lands on: one empty segment at
    // offset 0. A bare zero would read as "no storage" to a client polling
    // right after `create_topic`.
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
    let members = partition.replica_set(nodes).nodes().to_vec();
    // The primary's last report, narrowed on the metadata primary to the nodes
    // it hears from. No report yet leaves nobody known in sync.
    let in_sync_nodes = streams
        .stats_registry
        .directory()
        .get(
            IggyNamespace::new(stream_id, topic_id, partition.id),
            partition.created_revision,
        )
        .map_or(0, |status| status.in_sync_nodes);
    let in_sync_members = members
        .iter()
        .copied()
        .filter(|node| {
            1_u32
                .checked_shl(u32::from(*node))
                .is_some_and(|bit| in_sync_nodes & bit != 0)
        })
        .collect();
    Ok(PartitionResponse {
        id: usize_to_u32(partition.id)?,
        created_at: partition.created_at.as_micros(),
        segments_count: partition_segments_count(streams, stream_id, topic_id, partition.id),
        current_offset,
        size_bytes,
        messages_count,
        context: iggy_binary_protocol::primitives::partition_history::PartitionContext {
            incarnation: partition.created_revision,
            owner_generation: 0,
            metadata_op,
        },
        members,
        in_sync_members,
    })
}

/// The budgets of one page read, see [`collect_page`].
#[derive(Debug, Clone, Copy)]
struct PageBudget {
    scanned_entries: usize,
    page_bytes: usize,
}

const PAGE_BUDGET: PageBudget = PageBudget {
    scanned_entries: MAX_SCANNED_ENTRIES,
    page_bytes: MAX_PAGE_BYTES,
};

/// One page of an ordered walk, built under the metadata read guard.
///
/// `walk` yields each entry after the cursor with its position, and `project`
/// builds the wire item of an entry, or `None` for an entry the page skips.
/// The page stops after `limit` items, after `scanned_entries` walked entries,
/// or before the item that would take it past `page_bytes`. The first item
/// always fits, so every page read makes progress. `next` is the position of
/// the last walked entry, absent once the walk reached the end of the order.
///
/// An id walk over an `IdSlab` also passes the free slots of deleted entries,
/// and they do not count. One page can therefore walk every slot up to the
/// highest id ever used: at most `MAX_STREAMS` for streams and `MAX_TOPICS` for
/// topics, and the peak number of users for users.
fn collect_page<E, T: WireEncode>(
    mut walk: impl Iterator<Item = (WirePageCursor, E)>,
    limit: usize,
    budget: PageBudget,
    mut project: impl FnMut(E) -> Result<Option<T>, IggyError>,
) -> Result<PageResponse<T>, IggyError> {
    let mut items = Vec::new();
    let mut bytes = 0;
    let mut scanned = 0;
    let mut walked = None;
    while let Some((position, entry)) = walk.next() {
        if let Some(item) = project(entry)? {
            let size = item.encoded_size();
            if !items.is_empty() && bytes + size > budget.page_bytes {
                return Ok(PageResponse {
                    items,
                    next: walked,
                });
            }
            bytes += size;
            items.push(item);
        }
        walked = Some(position);
        scanned += 1;
        if items.len() == limit || scanned == budget.scanned_entries {
            // The look-ahead only tells whether the order goes on.
            let next = walk.next().and(walked);
            return Ok(PageResponse { items, next });
        }
    }
    Ok(PageResponse { items, next: None })
}

/// The start of a walk in id order. A creation cursor names no position in it.
fn id_after(cursor: Option<PageCursor>) -> Result<Option<usize>, IggyError> {
    match cursor.map(WirePageCursor::from) {
        None => Ok(None),
        // An id beyond `usize` lies past every entry, as `usize::MAX` does.
        Some(WirePageCursor::Id(id)) => Ok(Some(usize::try_from(id).unwrap_or(usize::MAX))),
        Some(WirePageCursor::Created { .. }) => Err(IggyError::InvalidCommand),
    }
}

fn build_get_partitions_response<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    body: &[u8],
) -> Result<PageResponse<PartitionResponse>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let request = GetPartitionsRequest::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
    let nodes = shard.replica_topology().replica_count;
    shard.plane.metadata().mux_stm.streams().read(|streams| {
        let metadata_op = shard.plane.metadata().applied_frontier().get();
        partitions_page(streams, request, metadata_op, nodes)
    })
}

/// One page of the partitions of a topic, in id order. Partition ids follow
/// creation, so the creation order walks the same way.
fn partitions_page(
    streams: &StreamsInner,
    request: GetPartitionsRequest,
    metadata_op: u64,
    nodes: u8,
) -> Result<PageResponse<PartitionResponse>, IggyError> {
    let query = PageQuery::try_from(request.page)?;
    let after = id_after(query.cursor)?;
    let (stream_id, topic_id) = resolve_topic(streams, &request.stream_id, &request.topic_id)?;
    let topic = streams
        .items
        .get(stream_id)
        .and_then(|stream| stream.topics.get(topic_id))
        .ok_or(IggyError::InvalidIdentifier)?;
    let walk = topic
        .partitions_after(after, query.descending)
        .map(|partition| (WirePageCursor::Id(partition.id as u64), partition));
    collect_page(walk, query.limit as usize, PAGE_BUDGET, |partition| {
        partition_response(streams, stream_id, topic_id, partition, metadata_op, nodes).map(Some)
    })
}

/// A scoped walk seeks into the caller's read grants in the requested order.
/// Only readable streams count toward the scan budget, including those that miss the name filter.
fn streams_page(
    streams: &StreamsInner,
    page: WirePageQuery,
    budget: PageBudget,
    permissions: Option<&Permissions>,
    ordered: Option<&GrantsByCreation>,
) -> Result<(PageResponse<StreamResponse>, Vec<SkippedEntry>), IggyError> {
    let NamedPageQuery { page: query, name } = NamedPageQuery::try_from(page)?;
    let start = WalkStart::try_from(&query)?;
    // A caller that reads no stream gets the last page without a walk it would only skip.
    let Some(permissions) = permissions else {
        return Ok((
            PageResponse {
                items: Vec::new(),
                next: None,
            },
            Vec::new(),
        ));
    };
    let limit = query.limit as usize;
    let mut skipped = Vec::new();
    let project = |stream: &Stream| {
        if !matches_name(&stream.name, name.as_deref()) {
            return Ok(None);
        }
        servable(
            stream_response(&StoredEntry::from(stream)),
            &mut skipped,
            OptionsScope::Stream,
            stream.id,
            None,
        )
    };
    let page = match start {
        WalkStart::Id(after)
            if let Some(ids) =
                ReadGrants::granted_streams(permissions, after, query.descending) =>
        {
            collect_page(
                ids.filter_map(|id| {
                    Some((
                        WirePageCursor::Id(id as u64),
                        streams.items.get(id)?.as_ref(),
                    ))
                }),
                limit,
                budget,
                project,
            )
        }
        WalkStart::Created(after)
            if !ReadGrants::for_stream(Some(permissions), None).reads_stream() =>
        {
            let keys = ordered.map_or(&[][..], |ordered| ordered.streams.as_slice());
            collect_page(
                created_grant_walk(keys, after, query.descending, |id| {
                    streams.items.get(id).map(Arc::as_ref)
                }),
                limit,
                budget,
                project,
            )
        }
        WalkStart::Id(after) => collect_page(
            streams
                .items
                .walk(after, query.descending)
                .map(|(id, stream)| (WirePageCursor::Id(id as u64), stream.as_ref())),
            limit,
            budget,
            project,
        ),
        WalkStart::Created(after) => collect_page(
            streams
                .streams_by_created(after, query.descending)
                .map(|((created_at, id), stream)| (created_cursor(created_at, id), stream)),
            limit,
            budget,
            project,
        ),
    }?;
    Ok((page, skipped))
}

/// One page of the topics of a stream the caller may read, and the topics it
/// leaves out because this build cannot read them. A missing stream lists as an
/// empty last page, as the full list did, and so does a stream none of the
/// caller's grants reach, before its topics are walked. After the first page a
/// missing stream is `StreamIdNotFound` for a caller who reads every stream:
/// the stream was deleted during the walk, so the pages read are not the list.
/// A cursor cannot prove what an earlier page showed, so a scoped caller gets
/// the empty last page for a missing stream too, the answer it gets for one it
/// cannot see. A caller whose grants read only some topics walks just those.
fn topics_page(
    streams: &StreamsInner,
    request: GetTopicsRequest,
    budget: PageBudget,
    permissions: Option<&Permissions>,
    ordered: Option<&GrantsByCreation>,
) -> Result<(PageResponse<TopicResponse>, Vec<SkippedEntry>), IggyError> {
    let NamedPageQuery { page: query, name } = NamedPageQuery::try_from(request.page)?;
    let start = WalkStart::try_from(&query)?;
    let last_empty_page = || {
        Ok((
            PageResponse {
                items: Vec::new(),
                next: None,
            },
            Vec::new(),
        ))
    };
    let stream_id = streams.resolve_stream_id(&request.stream_id);
    let grants = ReadGrants::for_stream(permissions, stream_id);
    let Some(stream_id) = stream_id else {
        if query.cursor.is_some() && grants.reads_topics_globally() {
            return Err(stream_not_found(&request.stream_id));
        }
        return last_empty_page();
    };
    let stream = streams
        .items
        .get(stream_id)
        .ok_or(IggyError::InvalidIdentifier)?;
    let limit = query.limit as usize;
    let mut skipped = Vec::new();
    let project = |topic: &Topic| {
        if !matches_name(&topic.name, name.as_deref()) {
            return Ok(None);
        }
        servable(
            topic_response(&StoredEntry::from(topic)),
            &mut skipped,
            OptionsScope::Topic,
            topic.id,
            Some(stream_id),
        )
    };
    let page = match start {
        WalkStart::Id(after) if let Some(ids) = grants.granted_topics(after, query.descending) => {
            collect_page(
                ids.filter_map(|id| Some((WirePageCursor::Id(id as u64), stream.topics.get(id)?))),
                limit,
                budget,
                project,
            )
        }
        WalkStart::Created(after) if !grants.reads_every_topic() => {
            let keys = ordered
                .and_then(|ordered| ordered.topics.get(&stream_id))
                .map_or(&[][..], Vec::as_slice);
            collect_page(
                created_grant_walk(keys, after, query.descending, |id| stream.topics.get(id)),
                limit,
                budget,
                project,
            )
        }
        WalkStart::Id(after) => collect_page(
            stream
                .topics
                .walk(after, query.descending)
                .map(|(id, topic)| (WirePageCursor::Id(id as u64), topic)),
            limit,
            budget,
            project,
        ),
        WalkStart::Created(after) => collect_page(
            stream
                .topics_by_created_after(after, query.descending)
                .map(|((created_at, id), topic)| (created_cursor(created_at, id), topic)),
            limit,
            budget,
            project,
        ),
    }?;
    Ok((page, skipped))
}

fn created_grant_walk<E>(
    keys: &[(u64, usize)],
    after: Option<(u64, usize)>,
    descending: bool,
    mut entry: impl FnMut(usize) -> Option<E>,
) -> impl Iterator<Item = (WirePageCursor, E)> {
    slice_after(keys, |&key| key, after, descending)
        .filter_map(move |&(time, id)| Some((created_cursor(time, id), entry(id)?)))
}

/// One page of the consumer groups of a topic, in id order. Group ids follow
/// creation, so the creation order walks the same way.
fn consumer_groups_page(
    streams: &StreamsInner,
    request: GetConsumerGroupsRequest,
    budget: PageBudget,
) -> Result<PageResponse<ConsumerGroupResponse>, IggyError> {
    let NamedPageQuery { page: query, name } = NamedPageQuery::try_from(request.page)?;
    let after = id_after(query.cursor)?;
    let (stream_id, topic_id) = resolve_topic(streams, &request.stream_id, &request.topic_id)?;
    let topic = streams
        .items
        .get(stream_id)
        .and_then(|stream| stream.topics.get(topic_id))
        .ok_or(IggyError::InvalidIdentifier)?;
    let walk = topic
        .consumer_groups_after(after.map(|id| id as u64), query.descending)
        .map(|group| (WirePageCursor::Id(group.id), group));
    collect_page(walk, query.limit as usize, budget, |group| {
        Ok(
            matches_name(&group.name, name.as_deref())
                .then(|| topic.consumer_group_response(group)),
        )
    })
}

/// One page of the users, and the users it leaves out because this build
/// cannot read them.
fn users_page(
    users: &UsersInner,
    page: WirePageQuery,
    budget: PageBudget,
) -> Result<(PageResponse<UserResponse>, Vec<SkippedEntry>), IggyError> {
    let NamedPageQuery { page: query, name } = NamedPageQuery::try_from(page)?;
    let start = WalkStart::try_from(&query)?;
    let limit = query.limit as usize;
    let mut skipped = Vec::new();
    let project = |user: &User| {
        if !matches_name(&user.username, name.as_deref()) {
            return Ok(None);
        }
        servable(
            user_response(&StoredUser::from(user)),
            &mut skipped,
            OptionsScope::User,
            user.id as usize,
            None,
        )
    };
    let page = match start {
        WalkStart::Id(after) => collect_page(
            users
                .items
                .walk(after, query.descending)
                .map(|(id, user)| (WirePageCursor::Id(id as u64), user)),
            limit,
            budget,
            project,
        ),
        WalkStart::Created(after) => collect_page(
            users
                .users_by_created(after, query.descending)
                .map(|((created_at, id), user)| (created_cursor(created_at, id), user)),
            limit,
            budget,
            project,
        ),
    }?;
    Ok((page, skipped))
}

/// Where a walk in the order of a query starts.
#[derive(Clone, Copy)]
enum WalkStart {
    Id(Option<usize>),
    Created(Option<(u64, usize)>),
}

impl TryFrom<&PageQuery> for WalkStart {
    type Error = IggyError;

    fn try_from(query: &PageQuery) -> Result<Self, Self::Error> {
        match query.order {
            PageOrder::Id => id_after(query.cursor).map(Self::Id),
            PageOrder::Created => created_after(query.cursor).map(Self::Created),
        }
    }
}

/// The start of a walk in creation order. An id cursor names no position in
/// it.
fn created_after(cursor: Option<PageCursor>) -> Result<Option<(u64, usize)>, IggyError> {
    match cursor.map(WirePageCursor::from) {
        None => Ok(None),
        // An id beyond `usize` lies past every entry of its time, as `usize::MAX` does.
        Some(WirePageCursor::Created { created_at, id }) => Ok(Some((
            created_at,
            usize::try_from(id).unwrap_or(usize::MAX),
        ))),
        Some(WirePageCursor::Id(_)) => Err(IggyError::InvalidCommand),
    }
}

const fn created_cursor(created_at: u64, id: usize) -> WirePageCursor {
    WirePageCursor::Created {
        created_at,
        id: id as u64,
    }
}

/// Whether `name` holds the name filter of a page, as a case-sensitive
/// substring.
fn matches_name(name: &str, filter: Option<&str>) -> bool {
    filter.is_none_or(|filter| name.contains(filter))
}

pub enum NonReplicatedResponse {
    Empty,
    Bytes(Bytes),
}

impl NonReplicatedResponse {
    pub(crate) const fn body_len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Bytes(body) => body.len(),
        }
    }

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
    use crate::dispatch::test_support::{
        FIRST_BOOT, SpyBus, create_one_partition, create_reader_request, prepare_message,
        test_shard,
    };
    use iggy_binary_protocol::Operation;
    use iggy_binary_protocol::primitives::page::WirePageOrder;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::streams::{CreateStreamRequest, DeleteStreamRequest};
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, DeleteTopicRequest,
    };
    use iggy_binary_protocol::requests::users::{CreateUserRequest, DeleteUserRequest};
    use iggy_common::{
        GlobalPermissions, HeaderKey, Identifier, OptionValue, PartitionStats, StreamPermissions,
        TopicPermissions,
    };
    use metadata::permissioner::Permissioner;
    use metadata::stm::snapshot::{FillSnapshot, MetadataSnapshot, RestoreSnapshotInPlace};
    use metadata::stm::stream::Partition;
    use metadata::stm::{StateHandler, StateMachine as _};
    use std::str::FromStr;

    #[compio::test]
    async fn given_partially_installed_checkpoint_when_listing_should_refuse_both_pages_as_retryable()
     {
        let shard = Rc::new(test_shard(&SpyBus::default(), 0, 1, FIRST_BOOT));
        let metadata = shard.plane.metadata();
        metadata.mux_stm.users().ensure_root_user("root", "hash");
        let mut snapshot = MetadataSnapshot::new(10);
        metadata.mux_stm.fill_snapshot(&mut snapshot).unwrap();
        let streams = GetStreamsRequest {
            page: WirePageQuery::default(),
        }
        .to_bytes();
        let topics = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(0),
            page: WirePageQuery::default(),
        }
        .to_bytes();

        metadata
            .mux_stm
            .users()
            .restore_snapshot_in_place(&snapshot)
            .unwrap();
        assert!(matches!(
            build_get_streams_response(&shard, &streams, Some(0)),
            Err(IggyError::TransientNotAccepted)
        ));
        assert!(matches!(
            build_get_topics_response(&shard, &topics, Some(0)),
            Err(IggyError::TransientNotAccepted)
        ));

        metadata
            .mux_stm
            .streams()
            .restore_snapshot_in_place(&snapshot)
            .unwrap();
        assert!(build_get_streams_response(&shard, &streams, Some(0)).is_ok());
        assert!(build_get_topics_response(&shard, &topics, Some(0)).is_ok());
    }

    #[test]
    fn given_a_reader_without_a_grant_when_getting_a_stream_or_topic_should_refuse_in_the_read() {
        const READER: u32 = 1;
        let shard = Rc::new(test_shard(&SpyBus::default(), 0, 1, FIRST_BOOT));
        let mux_stm = &shard.plane.metadata().mux_stm;
        create_one_partition(&shard);
        mux_stm
            .update(prepare_message(
                Operation::CreateUser,
                1,
                3,
                &create_reader_request().to_bytes(),
            ))
            .unwrap();
        let stream = WireIdentifier::numeric(0);
        let topic = WireIdentifier::numeric(0);

        for user_id in [Some(READER), None] {
            assert!(build_get_stream_response(&shard, &stream, user_id).is_err());
            assert!(build_get_topic_response(&shard, &stream, &topic, user_id).is_err());
        }
        assert!(matches!(
            build_get_stream_response(&shard, &stream, Some(READER)),
            Err(IggyError::Unauthorized)
        ));
        assert!(matches!(
            build_get_topic_response(&shard, &stream, &topic, Some(READER)),
            Err(IggyError::Unauthorized)
        ));
        assert!(matches!(
            build_get_stream_response(&shard, &stream, Some(0)),
            Ok(Some(_))
        ));
        assert!(matches!(
            build_get_topic_response(&shard, &stream, &topic, Some(0)),
            Ok(Some(_))
        ));
        assert!(matches!(
            build_get_stream_response(&shard, &WireIdentifier::numeric(9), Some(READER)),
            Ok(None)
        ));
    }

    #[test]
    fn given_one_unreadable_stream_and_topic_when_listing_should_serve_the_rest() {
        let unreadable = || {
            ResourceOptions::from([
                (
                    HeaderKey::from(OptionKey::CriticalKeys),
                    OptionValue::explicit(HeaderValue::try_from("future_key").unwrap()),
                ),
                (
                    HeaderKey::from_str("future_key").unwrap(),
                    OptionValue::explicit(HeaderValue::from(true)),
                ),
            ])
        };
        let created_at = IggyTimestamp::from(1u64);
        let mut streams = StreamsInner::new();
        let mut readable = Stream::new(Arc::from("readable"), created_at);
        for (name, options) in [("kept", ResourceOptions::new()), ("hidden", unreadable())] {
            let mut topic = Topic::new(Arc::from(name), created_at, readable.stats.clone());
            topic.set_options(options);
            readable.topics.insert(topic);
        }
        let mut hidden = Stream::new(Arc::from("hidden"), created_at);
        hidden.options = unreadable();
        let readable_slot = streams.items.insert(Arc::new(readable));
        let hidden_slot = streams.items.insert(Arc::new(hidden));

        let names = |names: Vec<&WireName>| -> Vec<String> {
            names.iter().map(|name| name.as_str().to_owned()).collect()
        };
        let skipped_keys = |skipped: Vec<SkippedEntry>| -> Vec<(OptionsScope, String)> {
            skipped
                .into_iter()
                .map(|entry| (entry.scope, entry.key))
                .collect()
        };
        let (page, skipped) = streams_page(
            &streams,
            WirePageQuery::default(),
            PAGE_BUDGET,
            Some(&Permissions::root()),
        )
        .expect("a stream page");
        assert_eq!(
            names(page.items.iter().map(|stream| &stream.name).collect()),
            ["readable"]
        );
        assert_eq!(page.next, None);
        assert_eq!(
            skipped_keys(skipped),
            [(OptionsScope::Stream, "future_key".to_owned())]
        );
        let topics_request = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(u32::try_from(readable_slot).unwrap()),
            page: WirePageQuery::default(),
        };
        let (page, skipped) = topics_page(
            &streams,
            topics_request,
            PAGE_BUDGET,
            Some(&Permissions::root()),
        )
        .expect("a topic page");
        assert_eq!(
            names(page.items.iter().map(|topic| &topic.name).collect()),
            ["kept"]
        );
        assert_eq!(
            skipped_keys(skipped),
            [(OptionsScope::Topic, "future_key".to_owned())]
        );
        let readable = streams.items.get(readable_slot).expect("stream stored");
        let topics =
            listed_topics(readable.id, &stored_topics(readable)).expect("the topic list builds");
        assert_eq!(
            names(topics.iter().map(|topic| &topic.name).collect()),
            ["kept"]
        );
        let hidden = streams.items.get(hidden_slot).expect("stream stored");
        assert!(matches!(
            stream_response(&StoredEntry::from(hidden.as_ref())),
            Err(IggyError::UnsupportedCriticalOption(_))
        ));
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
    fn partition_response_reports_the_initial_shape_until_a_segment_exists() {
        use iggy_common::{StreamStats, TopicStats};
        use metadata::stm::stream::{Partition, StreamsInner};

        let streams = StreamsInner::new();
        let partition = Partition::new(0, 1, IggyTimestamp::from(1u64), 0, 0);

        // Registry miss: the owning shard has not started building.
        let predicted =
            partition_response(&streams, 0, 0, &partition, 0, 1).expect("response builds");
        assert_eq!(predicted.segments_count, 1);
        assert_eq!(predicted.messages_count, 0);

        // Registered but not yet segmented: the reconciler mints the entry
        // before `ensure_initial_segment` runs, so this is the SAME state to a
        // caller and must not read as "no storage".
        let topic_stats = Arc::new(TopicStats::new(Arc::new(StreamStats::default())));
        let stats = streams
            .stats_registry
            .partition(0, 0, &partition, topic_stats);
        let mid_build =
            partition_response(&streams, 0, 0, &partition, 0, 1).expect("response builds");
        assert_eq!(mid_build.segments_count, 1);

        // Materialized: the real counters answer from here on.
        stats.increment_segments_count(1);
        stats.increment_messages_count(7);
        stats.increment_size_bytes(64);
        let live = partition_response(&streams, 0, 0, &partition, 0, 1).expect("response builds");
        assert_eq!(live.segments_count, 1);
        assert_eq!(live.messages_count, 7);
        assert_eq!(live.size_bytes, 64);
    }

    #[test]
    fn partition_response_lists_members_and_the_reported_ones_in_sync() {
        use iggy_binary_protocol::primitives::partition_status::PartitionStatusEntry;
        use metadata::stm::stream::{Partition, StreamsInner};

        let streams = StreamsInner::new();
        let legacy = Partition::new(0, 1, IggyTimestamp::from(1u64), 0, 0);
        let unreported = partition_response(&streams, 0, 0, &legacy, 0, 3).unwrap();
        assert_eq!(
            unreported.members,
            [0, 1, 2],
            "no member list is every node"
        );
        assert_eq!(unreported.in_sync_members, Vec::<u8>::new());

        let report = |in_sync_nodes| PartitionStatusEntry {
            namespace: IggyNamespace::new(0, 0, 0).inner(),
            created_revision: 0,
            view: 0,
            commit: 0,
            current_offset: 0,
            messages_count: 0,
            size_bytes: 0,
            segments_count: 1,
            in_sync_nodes,
        };
        streams
            .stats_registry
            .directory()
            .record([report(0b101)], std::time::Instant::now());
        let reported = partition_response(&streams, 0, 0, &legacy, 0, 3).unwrap();
        assert_eq!(reported.in_sync_members, [0, 2]);

        streams.stats_registry.directory().set_ready_nodes(0b011);
        let narrowed = partition_response(&streams, 0, 0, &legacy, 0, 3).unwrap();
        assert_eq!(
            narrowed.in_sync_members,
            [0],
            "a member the metadata primary cannot hear is not in sync"
        );
    }

    #[test]
    fn describe_options_reports_this_nodes_default_replication_factor() {
        let body = DescribeOptionsRequest {
            scope: OPTIONS_SCOPE_TOPIC,
        }
        .to_bytes();
        let response = build_describe_options_response(&body, 2).unwrap();
        let entry = response
            .entries
            .iter()
            .find(|entry| entry.key.as_str() == "replication_factor")
            .unwrap();
        assert_eq!(entry.default_value.as_ref(), 2_u32.to_le_bytes());
    }

    /// Applies `request` as a committed prepare would.
    fn commit(
        streams: &mut StreamsInner,
        request: &(impl StateHandler<State = StreamsInner> + std::fmt::Debug),
    ) {
        let reply = request.apply(streams, IggyTimestamp::default());
        assert_eq!(reply.code, 0, "{request:?} must commit");
    }

    /// One stream, one topic of `partitions` partitions and one consumer group.
    fn committed_tree(partitions: u32) -> StreamsInner {
        let mut streams = StreamsInner::new();
        commit(
            &mut streams,
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            },
        );
        commit(
            &mut streams,
            &CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    partitions_count: partitions,
                    name: WireName::new("topic").unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: (0..partitions)
                    .map(|partition_id| CreatedPartitionAssignment {
                        initial_view: 0,
                        partition_id,
                        consensus_group_id: u64::from(partition_id) + 1,
                        members: Vec::new(),
                    })
                    .collect(),
            },
        );
        commit(
            &mut streams,
            &CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(0),
                name: WireName::new("group").unwrap(),
            },
        );
        streams
    }

    /// The registry entry a shard takes when it builds the partition.
    fn materialise(streams: &StreamsInner, partition_id: usize) -> Arc<PartitionStats> {
        let topic = &streams.items[0].topics[0];
        let partition = topic
            .partitions
            .iter()
            .find(|partition| partition.id == partition_id)
            .unwrap();
        streams
            .stats_registry
            .partition(0, 0, partition, topic.stats.clone())
    }

    #[test]
    fn stats_totals_count_every_committed_partition_before_it_materialises() {
        let streams = committed_tree(2);

        // Only partition 0 has materialised. Counting the stream's rolled-up
        // counter reported 1 here, so a caller polling `[stats]` twice saw the
        // total climb to 2 with no write in between (and `get_topic` already
        // reported 2 for the same partitions).
        let first = materialise(&streams, 0);
        first.increment_segments_count(1);
        first.increment_size_bytes(100);
        first.increment_messages_count(10);
        let totals = stats_totals(&streams);
        assert_eq!(
            totals.counts,
            ResourceCounts {
                streams: 1,
                topics: 1,
                partitions: 2,
                consumer_groups: 1,
            }
        );
        assert_eq!(
            totals.segments_count, 2,
            "an unmaterialised partition must contribute the same floor the detail response reports"
        );
        assert_eq!(totals.messages_size_bytes, 100);
        assert_eq!(totals.messages_count, 10);

        // Materialising the second partition changes nothing: the total was
        // already the steady-state answer.
        materialise(&streams, 1).increment_segments_count(1);
        assert_eq!(stats_totals(&streams).segments_count, 2);

        first.increment_segments_count(2);
        assert_eq!(stats_totals(&streams).segments_count, 4);

        // An emptied partition falls back to the floor.
        first.zero_out_all();
        let totals = stats_totals(&streams);
        assert_eq!(totals.segments_count, 2);
        assert_eq!(totals.messages_size_bytes, 0);
        assert_eq!(totals.messages_count, 0);
    }

    #[test]
    fn given_topic_options_when_projecting_header_should_split_blocks_by_provenance() {
        use iggy_common::wire_conversions::resource_options_from_wire;
        use iggy_common::{HeaderKey, HeaderValue, OptionKey, OptionValue, StreamStats};

        let mut options = ResourceOptions::new();
        options.insert(
            HeaderKey::from(OptionKey::MessageExpiry),
            OptionValue::explicit(HeaderValue::from(5_000_000u64)),
        );
        options.insert(
            HeaderKey::from(OptionKey::MaxTopicSize),
            OptionValue::derived(HeaderValue::from(u64::MAX)),
        );
        let mut topic = metadata::stm::stream::Topic::new(
            Arc::from("topic"),
            IggyTimestamp::from(1u64),
            Arc::new(StreamStats::default()),
        );
        topic.set_options(options.clone());

        let header = topic_response(&StoredEntry::from(&topic)).expect("topic header builds");

        let mut explicit = resource_options_from_wire(&header.options, true).unwrap();
        explicit.extend(resource_options_from_wire(&header.derived_options, false).unwrap());
        assert_eq!(explicit, options);
        assert_eq!((&header.options).into_iter().count(), 1);
        assert_eq!((&header.derived_options).into_iter().count(), 1);
    }

    const ENTRIES: u64 = 10;
    /// The encoded size of each test item, `e0` to `e9` behind a length byte.
    const ITEM_BYTES: usize = 3;

    /// One page of the entries 0 to 9 after `after`, keeping the entries that
    /// `keep` accepts.
    fn entries_page(
        after: Option<u64>,
        limit: usize,
        budget: PageBudget,
        keep: impl Fn(u64) -> bool,
    ) -> PageResponse<WireName> {
        let walk = (0..ENTRIES)
            .filter(|id| after.is_none_or(|after| *id > after))
            .map(|id| (WirePageCursor::Id(id), id));
        collect_page(walk, limit, budget, |id| {
            Ok(keep(id).then(|| WireName::new(format!("e{id}")).unwrap()))
        })
        .unwrap()
    }

    /// Every page from the start, as (items, pages read). `read_page` reads the
    /// page after a cursor, as a client does with the `next` of each page.
    fn walk_pages<T>(
        mut read_page: impl FnMut(Option<WirePageCursor>) -> PageResponse<T>,
    ) -> (Vec<T>, usize) {
        let mut items = Vec::new();
        let mut cursor = None;
        let mut pages = 0;
        loop {
            pages += 1;
            let page = read_page(cursor);
            items.extend(page.items);
            let Some(next) = page.next else {
                return (items, pages);
            };
            cursor = Some(next);
        }
    }

    /// Every page of the entries from the start, as (item names, pages read).
    fn walk_entries(
        limit: usize,
        budget: PageBudget,
        keep: impl Fn(u64) -> bool + Copy,
    ) -> (Vec<String>, usize) {
        let (items, pages) = walk_pages(|cursor| {
            let after = cursor.map(|cursor| match cursor {
                WirePageCursor::Id(id) => id,
                cursor @ WirePageCursor::Created { .. } => {
                    panic!("an id walk returned {cursor:?}")
                }
            });
            entries_page(after, limit, budget, keep)
        });
        let names = items.iter().map(|name| name.as_str().to_owned()).collect();
        (names, pages)
    }

    fn names(ids: impl IntoIterator<Item = u64>) -> Vec<String> {
        ids.into_iter().map(|id| format!("e{id}")).collect()
    }

    #[test]
    fn given_any_limit_when_walking_every_page_should_return_each_entry_once() {
        for limit in [1, 2, 3, 5, 9, 10, 11, 1000] {
            let (walked, pages) = walk_entries(limit, PAGE_BUDGET, |_| true);
            assert_eq!(walked, names(0..ENTRIES), "limit {limit}");
            assert_eq!(
                pages,
                10_usize.div_ceil(limit),
                "limit {limit}: only the page that reaches the end has no next"
            );
            let (even, _) = walk_entries(limit, PAGE_BUDGET, |id| id % 2 == 0);
            assert_eq!(even, names((0..ENTRIES).step_by(2)), "limit {limit}");
        }
    }

    #[test]
    fn given_a_byte_budget_when_paging_should_stop_before_the_item_that_passes_it() {
        let budget = |items| PageBudget {
            page_bytes: items * ITEM_BYTES,
            ..PAGE_BUDGET
        };
        let page = entries_page(None, 1000, budget(3), |_| true);
        assert_eq!(page.items.len(), 3);
        assert_eq!(page.next, Some(WirePageCursor::Id(2)));

        let tight = PageBudget {
            page_bytes: 1,
            ..PAGE_BUDGET
        };
        let page = entries_page(None, 1000, tight, |_| true);
        assert_eq!(page.items.len(), 1, "the first item fits any budget");
        assert_eq!(page.next, Some(WirePageCursor::Id(0)));
        assert_eq!(walk_entries(1000, tight, |_| true).0, names(0..ENTRIES));
    }

    #[test]
    fn given_skipped_entries_before_the_byte_stop_when_paging_should_continue_after_them() {
        let keep = |id| id != 1 && id != 2;
        let one_item = PageBudget {
            page_bytes: ITEM_BYTES,
            ..PAGE_BUDGET
        };
        let page = entries_page(None, 1000, one_item, keep);
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.next, Some(WirePageCursor::Id(2)));
        let page = entries_page(Some(2), 1000, one_item, keep);
        assert_eq!(page.items[0].as_str(), "e3");
    }

    #[test]
    fn given_a_filter_without_a_match_when_paging_should_end_after_the_scan_budget_pages() {
        let scan = |entries| PageBudget {
            scanned_entries: entries,
            ..PAGE_BUDGET
        };
        let page = entries_page(None, 1000, scan(4), |_| false);
        assert_eq!(page.items, Vec::<WireName>::new());
        assert_eq!(page.next, Some(WirePageCursor::Id(3)));
        assert_eq!(walk_entries(1000, scan(4), |_| false), (Vec::new(), 3));
        assert_eq!(walk_entries(1000, scan(5), |_| false), (Vec::new(), 2));
    }

    fn topic_with_partitions(count: usize) -> StreamsInner {
        let created_at = IggyTimestamp::from(1u64);
        let mut streams = StreamsInner::new();
        let mut stream = Stream::new(Arc::from("orders"), created_at);
        let mut topic = Topic::new(Arc::from("events"), created_at, stream.stats.clone());
        topic.partitions = (0..count)
            .map(|id| Partition::new(id, id as u64, created_at, 0, 0))
            .collect();
        stream.topics.insert(topic);
        streams.items.insert(Arc::new(stream));
        streams
    }

    fn partition_ids(
        streams: &StreamsInner,
        stream_id: u32,
        page: WirePageQuery,
    ) -> Result<(Vec<u32>, Option<WirePageCursor>), IggyError> {
        let request = GetPartitionsRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            topic_id: WireIdentifier::numeric(0),
            page,
        };
        partitions_page(streams, request, 0, 1).map(|page| {
            let ids = page.items.iter().map(|partition| partition.id).collect();
            (ids, page.next)
        })
    }

    #[test]
    fn given_a_topic_when_paging_its_partitions_should_walk_ids_in_both_directions() {
        let streams = topic_with_partitions(5);
        let page = |descending, limit, cursor: Option<u64>| {
            let query = WirePageQuery {
                descending,
                limit,
                cursor: cursor.map(WirePageCursor::Id),
                ..WirePageQuery::default()
            };
            partition_ids(&streams, 0, query).unwrap()
        };
        let id = |id| Some(WirePageCursor::Id(id));
        assert_eq!(page(false, 2, None), (vec![0, 1], id(1)));
        assert_eq!(page(false, 2, Some(1)), (vec![2, 3], id(3)));
        assert_eq!(page(false, 2, Some(3)), (vec![4], None));
        assert_eq!(page(true, 2, None), (vec![4, 3], id(3)));
        assert_eq!(page(true, 2, Some(3)), (vec![2, 1], id(1)));
        assert_eq!(page(true, 2, Some(1)), (vec![0], None));
        assert_eq!(page(false, 5, None), (vec![0, 1, 2, 3, 4], None));
        assert_eq!(page(false, 10, Some(u64::MAX)), (Vec::new(), None));
        assert_eq!(page(true, 10, Some(u64::MAX)), (vec![4, 3, 2, 1, 0], None));

        let created = WirePageQuery {
            order: WirePageOrder::Created,
            limit: 2,
            ..WirePageQuery::default()
        };
        assert_eq!(
            partition_ids(&streams, 0, created).unwrap(),
            (vec![0, 1], id(1)),
            "partition ids follow creation"
        );
    }

    #[test]
    fn given_a_page_block_partitions_cannot_serve_when_paging_should_refuse_it() {
        let streams = topic_with_partitions(3);
        let refused = [
            WirePageQuery {
                cursor: Some(WirePageCursor::Created {
                    created_at: 1,
                    id: 0,
                }),
                ..WirePageQuery::default()
            },
            WirePageQuery {
                name: Some(WireName::new("p").unwrap()),
                ..WirePageQuery::default()
            },
            WirePageQuery {
                limit: 0,
                ..WirePageQuery::default()
            },
            WirePageQuery {
                limit: 1001,
                ..WirePageQuery::default()
            },
        ];
        for query in refused {
            assert_eq!(
                partition_ids(&streams, 0, query.clone()),
                Err(IggyError::InvalidCommand),
                "{query:?}"
            );
        }
        assert_eq!(
            partition_ids(&streams, 7, WirePageQuery::default()),
            Err(IggyError::StreamIdNotFound(Identifier::default()))
        );
        let missing_topic = GetPartitionsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(7),
            page: WirePageQuery::default(),
        };
        assert_eq!(
            partitions_page(&streams, missing_topic, 0, 1).map(|page| page.items.len()),
            Err(IggyError::TopicIdNotFound(
                Identifier::default(),
                Identifier::default()
            ))
        );
    }

    /// Applies one fixture request at `micros`, as a commit does.
    fn apply_at<R: StateHandler>(request: &R, state: &mut R::State, micros: u64) {
        let reply = request.apply(state, IggyTimestamp::from(micros));
        assert_eq!(reply.code, 0, "the fixture request applies");
    }

    fn create_stream(name: &str) -> CreateStreamRequest {
        CreateStreamRequest {
            name: WireName::new(name).unwrap(),
            options: WireOptions::empty(),
        }
    }

    /// A topic of stream 0 without partitions, so that a delete of it ends in
    /// its own apply.
    fn create_topic(name: &str) -> CreateTopicWithAssignmentsRequest {
        CreateTopicWithAssignmentsRequest {
            request: CreateTopicRequest {
                stream_id: WireIdentifier::numeric(0),
                partitions_count: 0,
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
            derived_options: WireOptions::empty(),
            partitions: Vec::new(),
        }
    }

    fn create_user(username: &str) -> CreateUserRequest {
        CreateUserRequest {
            username: WireName::new(username).unwrap(),
            password: "hash".to_owned(),
            status: 1,
            permissions: None,
            options: WireOptions::empty(),
        }
    }

    /// Streams `a` to `e`, then `b` deleted and `f` created in its id: the id
    /// order is `a f c d e`, the creation order `a c d e f`. Stream `a` holds
    /// the topics `ta` to `tc`, then `ta` deleted and `td` created in its id:
    /// the id order is `td tb tc`, the creation order `tb tc td`. Topic `tb`
    /// holds the groups `g0` to `g4`.
    const GRANTED_STREAM: u32 = 0;
    const GRANTED_TOPIC: usize = 1;

    fn streams_page(
        streams: &StreamsInner,
        page: WirePageQuery,
        budget: PageBudget,
        permissions: Option<&Permissions>,
    ) -> Result<(PageResponse<StreamResponse>, Vec<SkippedEntry>), IggyError> {
        let ordered =
            permissions.map(|permissions| GrantsByCreation::from_catalog(permissions, streams));
        super::streams_page(streams, page, budget, permissions, ordered.as_ref())
    }

    fn topics_page(
        streams: &StreamsInner,
        request: GetTopicsRequest,
        budget: PageBudget,
        permissions: Option<&Permissions>,
    ) -> Result<(PageResponse<TopicResponse>, Vec<SkippedEntry>), IggyError> {
        let ordered =
            permissions.map(|permissions| GrantsByCreation::from_catalog(permissions, streams));
        super::topics_page(streams, request, budget, permissions, ordered.as_ref())
    }

    /// No grant, then each global, stream and topic grant alone, on [`GRANTED_STREAM`] and [`GRANTED_TOPIC`].
    fn single_grants() -> Vec<Permissions> {
        let global = |grant: fn(&mut GlobalPermissions)| {
            let mut permissions = Permissions::default();
            grant(&mut permissions.global);
            permissions
        };
        let on_stream = |stream: StreamPermissions| Permissions {
            streams: Some([(GRANTED_STREAM as usize, stream)].into()),
            ..Default::default()
        };
        let stream = |grant: fn(&mut StreamPermissions)| {
            let mut stream = StreamPermissions::default();
            grant(&mut stream);
            on_stream(stream)
        };
        let topic = |grant: fn(&mut TopicPermissions)| {
            let mut topic = TopicPermissions::default();
            grant(&mut topic);
            on_stream(StreamPermissions {
                topics: Some([(GRANTED_TOPIC, topic)].into()),
                ..Default::default()
            })
        };
        vec![
            Permissions::default(),
            global(|global| global.read_streams = true),
            global(|global| global.manage_streams = true),
            global(|global| global.read_topics = true),
            global(|global| global.manage_topics = true),
            global(|global| global.poll_messages = true),
            global(|global| global.send_messages = true),
            stream(|stream| stream.read_stream = true),
            stream(|stream| stream.manage_stream = true),
            stream(|stream| stream.read_topics = true),
            stream(|stream| stream.manage_topics = true),
            stream(|stream| stream.poll_messages = true),
            stream(|stream| stream.send_messages = true),
            topic(|topic| topic.read_topic = true),
            topic(|topic| topic.manage_topic = true),
            topic(|topic| topic.poll_messages = true),
            topic(|topic| topic.send_messages = true),
        ]
    }

    fn paged_streams() -> StreamsInner {
        let mut streams = StreamsInner::new();
        for (micros, name) in [(10, "a"), (20, "b"), (30, "c"), (40, "d"), (50, "e")] {
            apply_at(&create_stream(name), &mut streams, micros);
        }
        let delete_stream = DeleteStreamRequest {
            stream_id: WireIdentifier::numeric(1),
        };
        apply_at(&delete_stream, &mut streams, 60);
        apply_at(&create_stream("f"), &mut streams, 70);
        for (micros, name) in [(80, "ta"), (90, "tb"), (100, "tc")] {
            apply_at(&create_topic(name), &mut streams, micros);
        }
        let delete_topic = DeleteTopicRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
        };
        apply_at(&delete_topic, &mut streams, 110);
        apply_at(&create_topic("td"), &mut streams, 120);
        for (micros, name) in [
            (130, "g0"),
            (140, "g1"),
            (150, "g2"),
            (160, "g3"),
            (170, "g4"),
        ] {
            let create_group = CreateConsumerGroupRequest {
                stream_id: WireIdentifier::numeric(0),
                topic_id: WireIdentifier::numeric(1),
                name: WireName::new(name).unwrap(),
            };
            apply_at(&create_group, &mut streams, micros);
        }
        streams
    }

    /// Walks every page of `order` in both directions at each limit up to one
    /// past the entry count, and checks the names against `ascending` and the
    /// number of pages read.
    fn assert_walks<T>(
        order: WirePageOrder,
        ascending: &[&str],
        read_page: impl Fn(WirePageQuery) -> PageResponse<T>,
        name: impl Fn(&T) -> &str,
    ) {
        for descending in [false, true] {
            let mut expected = ascending.to_vec();
            if descending {
                expected.reverse();
            }
            for limit in 1..=expected.len() + 1 {
                let (items, pages) = walk_pages(|cursor| {
                    read_page(WirePageQuery {
                        order,
                        descending,
                        limit: u32::try_from(limit).unwrap(),
                        cursor,
                        ..WirePageQuery::default()
                    })
                });
                let names: Vec<&str> = items.iter().map(&name).collect();
                let case = format!("{order:?} descending={descending} limit={limit}");
                assert_eq!(names, expected, "{case}");
                assert_eq!(pages, expected.len().div_ceil(limit), "{case}");
            }
        }
    }

    #[test]
    fn given_a_reused_stream_id_when_paging_streams_should_walk_each_order_both_ways() {
        let streams = paged_streams();
        let read_page = |page| {
            streams_page(&streams, page, PAGE_BUDGET, Some(&Permissions::root()))
                .unwrap()
                .0
        };
        assert_walks(
            WirePageOrder::Id,
            &["a", "f", "c", "d", "e"],
            read_page,
            |stream| stream.name.as_str(),
        );
        assert_walks(
            WirePageOrder::Created,
            &["a", "c", "d", "e", "f"],
            read_page,
            |stream| stream.name.as_str(),
        );
    }

    #[test]
    fn given_creates_and_deletes_between_pages_when_walking_should_list_each_lasting_stream_once() {
        let cases = [
            // `g` takes id 0 behind the cursor, `h` takes id 3 ahead of it.
            (WirePageOrder::Id, ["a", "f", "c", "h", "e"].as_slice()),
            (
                WirePageOrder::Created,
                ["a", "c", "e", "f", "g", "h"].as_slice(),
            ),
        ];
        for (order, expected) in cases {
            let mut streams = paged_streams();
            let page = |streams: &StreamsInner, cursor| {
                let query = WirePageQuery {
                    order,
                    limit: 2,
                    cursor,
                    ..WirePageQuery::default()
                };
                streams_page(streams, query, PAGE_BUDGET, Some(&Permissions::root()))
                    .unwrap()
                    .0
            };
            let first = page(&streams, None);
            for (stream_id, micros) in [(0, 180), (3, 190)] {
                let delete = DeleteStreamRequest {
                    stream_id: WireIdentifier::numeric(stream_id),
                };
                apply_at(&delete, &mut streams, micros);
            }
            apply_at(&create_stream("g"), &mut streams, 200);
            apply_at(&create_stream("h"), &mut streams, 210);
            let resume = first.next.expect("a second page");
            let (rest, _) = walk_pages(|cursor| page(&streams, Some(cursor.unwrap_or(resume))));
            let names: Vec<&str> = first
                .items
                .iter()
                .chain(&rest)
                .map(|stream| stream.name.as_str())
                .collect();
            assert_eq!(names, expected, "{order:?} order");
        }
    }

    #[test]
    fn given_a_reused_topic_id_when_paging_topics_should_walk_each_order_both_ways() {
        let streams = paged_streams();
        let read_page = |page| {
            let request = GetTopicsRequest {
                stream_id: WireIdentifier::numeric(0),
                page,
            };
            topics_page(&streams, request, PAGE_BUDGET, Some(&Permissions::root()))
                .unwrap()
                .0
        };
        assert_walks(WirePageOrder::Id, &["td", "tb", "tc"], read_page, |topic| {
            topic.name.as_str()
        });
        assert_walks(
            WirePageOrder::Created,
            &["tb", "tc", "td"],
            read_page,
            |topic| topic.name.as_str(),
        );

        let missing_stream = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(9),
            page: WirePageQuery::default(),
        };
        let (page, skipped) = topics_page(
            &streams,
            missing_stream,
            PAGE_BUDGET,
            Some(&Permissions::root()),
        )
        .unwrap();
        assert_eq!(
            (page.items.len(), page.next, skipped.len()),
            (0, None, 0),
            "a missing stream lists as an empty last page"
        );
        let stream_deleted_during_the_walk = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(9),
            page: WirePageQuery {
                cursor: Some(WirePageCursor::Id(0)),
                ..WirePageQuery::default()
            },
        };
        assert!(
            matches!(
                topics_page(
                    &streams,
                    stream_deleted_during_the_walk,
                    PAGE_BUDGET,
                    Some(&Permissions::root())
                ),
                Err(IggyError::StreamIdNotFound(_))
            ),
            "a page after the first one needs its stream"
        );
    }

    #[test]
    fn given_a_reused_user_id_when_paging_users_should_start_the_creation_order_at_root() {
        let mut users = UsersInner::new();
        for (micros, username) in [(10, "root"), (20, "bravo"), (30, "charlie"), (40, "delta")] {
            apply_at(&create_user(username), &mut users, micros);
        }
        let delete_user = DeleteUserRequest {
            user_id: WireIdentifier::numeric(1),
        };
        apply_at(&delete_user, &mut users, 50);
        apply_at(&create_user("echo"), &mut users, 60);
        let read_page = |page| users_page(&users, page, PAGE_BUDGET).unwrap().0;
        assert_walks(
            WirePageOrder::Id,
            &["root", "echo", "charlie", "delta"],
            read_page,
            |user| user.username.as_str(),
        );
        assert_walks(
            WirePageOrder::Created,
            &["root", "charlie", "delta", "echo"],
            read_page,
            |user| user.username.as_str(),
        );
    }

    #[test]
    fn given_consumer_groups_when_paging_should_walk_ids_for_each_order() {
        let streams = paged_streams();
        let request = |topic_id, page| GetConsumerGroupsRequest {
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(topic_id),
            page,
        };
        let read_page =
            |page| consumer_groups_page(&streams, request(1, page), PAGE_BUDGET).unwrap();
        let groups = ["g0", "g1", "g2", "g3", "g4"];
        for order in [WirePageOrder::Id, WirePageOrder::Created] {
            assert_walks(order, &groups, read_page, |group| group.name.as_str());
        }
        let created = WirePageQuery {
            order: WirePageOrder::Created,
            limit: 2,
            ..WirePageQuery::default()
        };
        assert!(
            matches!(read_page(created).next, Some(WirePageCursor::Id(_))),
            "a creation walk of groups goes on with an id cursor"
        );

        let created_cursor = WirePageQuery {
            order: WirePageOrder::Created,
            cursor: Some(WirePageCursor::Created {
                created_at: 130,
                id: 0,
            }),
            ..WirePageQuery::default()
        };
        assert_eq!(
            consumer_groups_page(&streams, request(1, created_cursor), PAGE_BUDGET)
                .map(|page| page.items.len()),
            Err(IggyError::InvalidCommand)
        );
        assert_eq!(
            consumer_groups_page(&streams, request(9, WirePageQuery::default()), PAGE_BUDGET)
                .map(|page| page.items.len()),
            Err(IggyError::TopicIdNotFound(
                Identifier::default(),
                Identifier::default()
            ))
        );
        let missing_stream = GetConsumerGroupsRequest {
            stream_id: WireIdentifier::numeric(9),
            ..request(1, WirePageQuery::default())
        };
        assert_eq!(
            consumer_groups_page(&streams, missing_stream, PAGE_BUDGET)
                .map(|page| page.items.len()),
            Err(IggyError::StreamIdNotFound(Identifier::default()))
        );
    }

    #[test]
    fn given_a_name_filter_when_paging_streams_should_bound_the_scan_of_each_page() {
        let streams = paged_streams();
        let two_scanned = PageBudget {
            scanned_entries: 2,
            ..PAGE_BUDGET
        };
        let filtered = |name: &str, cursor| WirePageQuery {
            name: Some(WireName::new(name).unwrap()),
            cursor,
            ..WirePageQuery::default()
        };
        let read_page = |name, cursor| {
            streams_page(
                &streams,
                filtered(name, cursor),
                two_scanned,
                Some(&Permissions::root()),
            )
            .unwrap()
            .0
        };

        let first = read_page("z", None);
        assert_eq!(
            (first.items.len(), first.next),
            (0, Some(WirePageCursor::Id(1))),
            "streams a and f scanned"
        );
        let (found, pages) = walk_pages(|cursor| read_page("z", cursor));
        assert_eq!(
            (found.len(), pages),
            (0, 3),
            "five streams, two scanned per page"
        );
        let (found, _) = walk_pages(|cursor| read_page("d", cursor));
        let names: Vec<&str> = found.iter().map(|stream| stream.name.as_str()).collect();
        assert_eq!(names, ["d"]);
    }

    #[test]
    fn given_scoped_reader_when_paging_streams_should_list_only_readable_streams() {
        let streams = paged_streams();
        // Ids 0 to 4 hold streams a, f, c, d and e.
        let readable = Permissions {
            streams: Some(
                [1, 4]
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            StreamPermissions {
                                read_stream: true,
                                ..Default::default()
                            },
                        )
                    })
                    .collect(),
            ),
            ..Default::default()
        };
        let two_scanned = PageBudget {
            scanned_entries: 2,
            ..PAGE_BUDGET
        };
        for budget in [PAGE_BUDGET, two_scanned] {
            let (found, _) = walk_pages(|cursor| {
                let query = WirePageQuery {
                    limit: 1,
                    cursor,
                    ..WirePageQuery::default()
                };
                streams_page(&streams, query, budget, Some(&readable))
                    .unwrap()
                    .0
            });
            let names: Vec<&str> = found.iter().map(|stream| stream.name.as_str()).collect();
            assert_eq!(names, ["f", "e"], "{budget:?}");
        }

        let filtered = |name: &str| WirePageQuery {
            name: Some(WireName::new(name).unwrap()),
            ..WirePageQuery::default()
        };
        let (hidden, _) =
            streams_page(&streams, filtered("d"), PAGE_BUDGET, Some(&readable)).unwrap();
        let (shown, _) =
            streams_page(&streams, filtered("e"), PAGE_BUDGET, Some(&readable)).unwrap();
        assert_eq!((hidden.items.len(), hidden.next), (0, None));
        assert_eq!(shown.items.len(), 1);
    }

    #[test]
    fn given_each_single_grant_when_listing_should_serve_what_the_direct_reads_serve() {
        const USER: u32 = 7;
        let streams = paged_streams();
        for permissions in single_grants() {
            let mut direct = Permissioner::new();
            direct.init_permissions_for_user(USER, Some(permissions.clone()));
            let (listed_streams, _) = walk_pages(|cursor| {
                let query = WirePageQuery {
                    cursor,
                    ..WirePageQuery::default()
                };
                streams_page(&streams, query, PAGE_BUDGET, Some(&permissions))
                    .expect("a stream page")
                    .0
            });
            let (listed_topics, _) = walk_pages(|cursor| {
                let request = GetTopicsRequest {
                    stream_id: WireIdentifier::numeric(GRANTED_STREAM),
                    page: WirePageQuery {
                        cursor,
                        ..WirePageQuery::default()
                    },
                };
                topics_page(&streams, request, PAGE_BUDGET, Some(&permissions))
                    .expect("a topic page")
                    .0
            });

            let readable_streams: Vec<usize> = streams
                .items
                .iter()
                .map(|(stream_id, _)| stream_id)
                .filter(|stream_id| direct.get_stream(USER, *stream_id).is_ok())
                .collect();
            let granted_stream = GRANTED_STREAM as usize;
            let readable_topics: Vec<usize> = streams
                .items
                .get(granted_stream)
                .expect("the stream of the topics")
                .topics
                .iter()
                .map(|(topic_id, _)| topic_id)
                .filter(|topic_id| direct.get_topic(USER, granted_stream, *topic_id).is_ok())
                .collect();
            assert_eq!(
                listed_streams
                    .iter()
                    .map(|stream| stream.id as usize)
                    .collect::<Vec<_>>(),
                readable_streams,
                "streams with {permissions:?}"
            );
            assert_eq!(
                listed_topics
                    .iter()
                    .map(|topic| topic.id as usize)
                    .collect::<Vec<_>>(),
                readable_topics,
                "topics with {permissions:?}"
            );
        }
    }

    #[test]
    fn given_a_caller_reading_no_stream_or_topic_when_the_scan_budget_is_small_should_end_without_a_walk()
     {
        let streams = paged_streams();
        let one_scanned = PageBudget {
            scanned_entries: 1,
            ..PAGE_BUDGET
        };
        // A grant that reads one topic of stream 0 reads no stream.
        let topic_only = Permissions {
            streams: Some(
                [(
                    0,
                    StreamPermissions {
                        topics: Some(
                            [(
                                1,
                                TopicPermissions {
                                    read_topic: true,
                                    ..Default::default()
                                },
                            )]
                            .into(),
                        ),
                        ..Default::default()
                    },
                )]
                .into(),
            ),
            ..Default::default()
        };

        let (listed, _) = streams_page(
            &streams,
            WirePageQuery::default(),
            one_scanned,
            Some(&topic_only),
        )
        .expect("a stream page");
        assert_eq!((listed.items.len(), listed.next), (0, None));

        // A grant on stream 2 reads no topic of stream 0, which holds three, nor of a missing stream.
        let other_stream = Permissions {
            streams: Some(
                [(
                    2,
                    StreamPermissions {
                        read_stream: true,
                        ..Default::default()
                    },
                )]
                .into(),
            ),
            ..Default::default()
        };
        for (stream_id, cursor) in [
            (0, None),
            (0, Some(WirePageCursor::Id(0))),
            (9, Some(WirePageCursor::Id(0))),
        ] {
            let request = GetTopicsRequest {
                stream_id: WireIdentifier::numeric(stream_id),
                page: WirePageQuery {
                    cursor,
                    ..WirePageQuery::default()
                },
            };
            let (listed, _) = topics_page(&streams, request, one_scanned, Some(&other_stream))
                .expect("a topic page");
            assert_eq!(
                (listed.items.len(), listed.next),
                (0, None),
                "stream {stream_id} with cursor {cursor:?}"
            );
        }
    }

    #[test]
    fn given_a_global_topic_reader_when_its_stream_goes_missing_mid_walk_should_report_it() {
        let streams = paged_streams();
        let topic_reader = Permissions {
            global: GlobalPermissions {
                read_topics: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let request = GetTopicsRequest {
            stream_id: WireIdentifier::numeric(9),
            page: WirePageQuery {
                cursor: Some(WirePageCursor::Id(0)),
                ..WirePageQuery::default()
            },
        };

        assert!(matches!(
            topics_page(&streams, request, PAGE_BUDGET, Some(&topic_reader)),
            Err(IggyError::StreamIdNotFound(_))
        ));
    }

    #[test]
    fn given_reader_without_grants_when_paging_streams_should_end_empty() {
        let streams = paged_streams();
        let (first, _) =
            streams_page(&streams, WirePageQuery::default(), PAGE_BUDGET, None).unwrap();
        assert_eq!((first.items.len(), first.next), (0, None));

        let two_scanned = PageBudget {
            scanned_entries: 2,
            ..PAGE_BUDGET
        };
        let (page, _) =
            streams_page(&streams, WirePageQuery::default(), two_scanned, None).unwrap();
        assert_eq!((page.items.len(), page.next), (0, None));
    }

    #[test]
    fn given_topic_grant_when_paging_topics_should_list_only_that_topic() {
        let streams = paged_streams();
        // Topic ids 0 to 2 of stream 0 hold td, tb and tc.
        let only_tb = Permissions {
            streams: Some(
                [(
                    0,
                    StreamPermissions {
                        topics: Some(
                            [(
                                1,
                                TopicPermissions {
                                    read_topic: true,
                                    ..Default::default()
                                },
                            )]
                            .into(),
                        ),
                        ..Default::default()
                    },
                )]
                .into(),
            ),
            ..Default::default()
        };
        let request = |page| GetTopicsRequest {
            stream_id: WireIdentifier::numeric(0),
            page,
        };
        let (found, _) = walk_pages(|cursor| {
            let page = WirePageQuery {
                limit: 1,
                cursor,
                ..WirePageQuery::default()
            };
            topics_page(&streams, request(page), PAGE_BUDGET, Some(&only_tb))
                .unwrap()
                .0
        });
        let names: Vec<&str> = found.iter().map(|topic| topic.name.as_str()).collect();
        assert_eq!(names, ["tb"]);
    }

    #[test]
    fn given_sparse_creation_grants_when_a_page_misses_its_filter_should_bound_catalog_lookups() {
        let keys = [(10, 17), (20, 500_000), (30, 900_000)];
        let budget = PageBudget {
            scanned_entries: 1,
            ..PAGE_BUDGET
        };
        for descending in [false, true] {
            let mut looked_up = Vec::new();
            let walk = created_grant_walk(&keys, None, descending, |id| {
                looked_up.push(id);
                Some(id)
            });
            let page: PageResponse<WireName> =
                collect_page(walk, 10, budget, |_| Ok(None)).unwrap();
            let first = if descending { keys[2] } else { keys[0] };
            assert_eq!(looked_up, [first.1, keys[1].1]);
            assert_eq!(page.next, Some(created_cursor(first.0, first.1)));
            assert_eq!(page.items, Vec::<WireName>::new());
        }
    }

    #[test]
    fn given_scoped_reader_when_paging_should_walk_only_what_its_grants_read() {
        let streams = paged_streams();
        // Stream ids 1 and 4 hold f and e. Topic ids 1 and 2 of stream 0 hold tb and tc.
        let read_stream = StreamPermissions {
            read_stream: true,
            ..Default::default()
        };
        let read_topic = TopicPermissions {
            read_topic: true,
            ..Default::default()
        };
        let scoped = Permissions {
            streams: Some(
                [
                    (
                        0,
                        StreamPermissions {
                            topics: Some([(1, read_topic.clone()), (2, read_topic)].into()),
                            ..Default::default()
                        },
                    ),
                    (1, read_stream.clone()),
                    (4, read_stream),
                ]
                .into(),
            ),
            ..Default::default()
        };
        let read_streams = |budget, page| {
            streams_page(&streams, page, budget, Some(&scoped))
                .unwrap()
                .0
        };
        let read_topics = |budget, page| {
            let request = GetTopicsRequest {
                stream_id: WireIdentifier::numeric(0),
                page,
            };
            topics_page(&streams, request, budget, Some(&scoped))
                .unwrap()
                .0
        };
        let stream_name = |stream: &StreamResponse| stream.name.as_str().to_owned();
        let topic_name = |topic: &TopicResponse| topic.name.as_str().to_owned();
        for (order, streams_ascending, topics_ascending) in [
            (WirePageOrder::Id, ["f", "e"], ["tb", "tc"]),
            (WirePageOrder::Created, ["e", "f"], ["tb", "tc"]),
        ] {
            assert_walks(
                order,
                &streams_ascending,
                |page| read_streams(PAGE_BUDGET, page),
                |stream| stream.name.as_str(),
            );
            assert_walks(
                order,
                &topics_ascending,
                |page| read_topics(PAGE_BUDGET, page),
                |topic| topic.name.as_str(),
            );
        }

        let one_scanned = PageBudget {
            scanned_entries: 1,
            ..PAGE_BUDGET
        };
        let (streams_found, stream_pages) = walk_pages(|cursor| {
            read_streams(
                one_scanned,
                WirePageQuery {
                    cursor,
                    ..WirePageQuery::default()
                },
            )
        });
        let (topics_found, topic_pages) = walk_pages(|cursor| {
            read_topics(
                one_scanned,
                WirePageQuery {
                    cursor,
                    ..WirePageQuery::default()
                },
            )
        });
        assert_eq!(
            streams_found.iter().map(stream_name).collect::<Vec<_>>(),
            ["f", "e"]
        );
        assert_eq!(
            topics_found.iter().map(topic_name).collect::<Vec<_>>(),
            ["tb", "tc"]
        );
        assert_eq!(
            (stream_pages, topic_pages),
            (2, 2),
            "every scanned entry is one the caller reads"
        );
    }

    #[test]
    fn given_hidden_or_missing_stream_when_scoped_reader_pages_topics_should_answer_alike() {
        let streams = paged_streams();
        let page = |stream_id, cursor| GetTopicsRequest {
            stream_id: WireIdentifier::numeric(stream_id),
            page: WirePageQuery {
                cursor,
                ..WirePageQuery::default()
            },
        };
        for cursor in [None, Some(WirePageCursor::Id(0))] {
            for stream_id in [0, 9] {
                let (listed, skipped) =
                    topics_page(&streams, page(stream_id, cursor), PAGE_BUDGET, None).unwrap();
                assert_eq!(
                    (listed.items.len(), listed.next, skipped.len()),
                    (0, None, 0),
                    "stream {stream_id} with cursor {cursor:?}"
                );
            }
        }
        assert!(matches!(
            topics_page(
                &streams,
                page(9, Some(WirePageCursor::Id(0))),
                PAGE_BUDGET,
                Some(&Permissions::root())
            ),
            Err(IggyError::StreamIdNotFound(_))
        ));
    }

    #[test]
    fn given_a_cursor_of_the_other_order_when_paging_should_refuse_it() {
        let streams = paged_streams();
        let users = UsersInner::new();
        let created_cursor = WirePageCursor::Created {
            created_at: 10,
            id: 0,
        };
        for (order, cursor) in [
            (WirePageOrder::Id, created_cursor),
            (WirePageOrder::Created, WirePageCursor::Id(0)),
        ] {
            let page = WirePageQuery {
                order,
                cursor: Some(cursor),
                ..WirePageQuery::default()
            };
            assert_eq!(
                streams_page(
                    &streams,
                    page.clone(),
                    PAGE_BUDGET,
                    Some(&Permissions::root())
                )
                .map(|(page, _)| page.items.len()),
                Err(IggyError::InvalidCommand),
                "streams in {order:?} order"
            );
            let missing_stream = GetTopicsRequest {
                stream_id: WireIdentifier::numeric(9),
                page: page.clone(),
            };
            assert_eq!(
                topics_page(
                    &streams,
                    missing_stream,
                    PAGE_BUDGET,
                    Some(&Permissions::root())
                )
                .map(|(page, _)| page.items.len()),
                Err(IggyError::InvalidCommand),
                "the query is checked before the stream, in {order:?} order"
            );
            assert_eq!(
                users_page(&users, page, PAGE_BUDGET).map(|(page, _)| page.items.len()),
                Err(IggyError::InvalidCommand),
                "users in {order:?} order"
            );
        }
    }
}
