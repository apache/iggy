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

//! Read-path gates: the shared per-op RBAC + consistency check, the two waits
//! a local read serves behind (the post-restart recovery barrier and the
//! per-user read-your-writes frontier), the local metadata-STM read
//! entry, and the wire/domain identifier resolvers the read and data-plane
//! routes ground their scopes through.

use crate::dispatch::reads::{
    FrontierUnreached, FrontierWait, hold_for_frontier, read_needs_metadata_frontier,
};
use crate::shell::ServerShard;
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::WireIdentifier;
use iggy_binary_protocol::codes::GET_STATS_CODE;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{Identifier, IggyError};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
use send_wrapper::SendWrapper;
use std::rc::Rc;

use crate::http::error::{Consistency, ReadError};
use crate::http::extractor::Identity;
use crate::http::state::HttpInner;
use crate::responses::{NonReplicatedResponse, build_non_replicated_response};

/// The per-op RBAC + consistency check itself, without the waits: run the
/// route's `rule` against the caller's committed permissions via the live
/// permissioner. A denial (always `Unauthorized`) renders 403 through the
/// legacy `IggyError -> status` map; root holds every grant, so its reads pass
/// without a user-id short-circuit. A linearizable read must come from the
/// primary; on a follower it redirects (307) to the primary's HTTP address when
/// resolvable, else fails closed to a 503 (see
/// [`HttpInner::not_primary_read_error`]).
///
/// Every read route reaches this through [`gate_local_read`], which is what
/// pairs it with the two waits a local read must serve behind. A route that
/// awaits again after that gate (a cross-shard gather, the snapshot
/// collection, or a local partition read) calls it once more before building
/// its reply: the account behind the token can be replaced, and its grants can
/// change, during any await. Relayed partition reads are authorized by the
/// serving node, which verifies the forwarded bearer itself.
pub(in crate::http) fn authorize_read(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
    rule: impl Fn(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<(), ReadError> {
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| {
            if !users.is_active_account(identity.user_id, identity.user_created_at.as_micros()) {
                return Err(IggyError::Unauthenticated);
            }
            users.authorize(|permissioner| rule(permissioner, identity.user_id))
        })
        .map_err(ReadError::Rejected)?;
    if consistency == Consistency::Linearizable && !state.is_metadata_primary() {
        return Err(state.not_primary_read_error(&identity.path_and_query, identity.client_ip));
    }
    Ok(())
}

/// Serve one authenticated read from the local metadata STM and hand back the
/// wire response body. Shared chokepoint for every read route whose data lives
/// in the metadata STM: it runs the shared [`authorize_read`] gate, then
/// delegates to [`build_non_replicated_response`], the SAME local-read entry the
/// TCP dispatch spine uses (`handle_default_non_replicated`), so an HTTP read and
/// a TCP read of the same entity return byte-identical bodies.
///
/// Reads never touch consensus or a VSR session: `build_non_replicated_response`
/// is a pure STM read, with one exception - the stats read's cross-shard
/// connected-client gather, an async broadcast run here (under `SendWrapper`,
/// same as `/metrics`) and gated again before the sync builder. An absent
/// entity surfaces as [`NonReplicatedResponse::Empty`], mapped to 404 here
/// because every REST read whose entity can be missing shares that not-found
/// shape.
pub(in crate::http) async fn read_local(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
    code: u32,
    body: &[u8],
    rule: impl Fn(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<Bytes, ReadError> {
    gate_local_read(state, identity, consistency, code, &rule).await?;
    let clients_count = if code == GET_STATS_CODE {
        let clients_count = SendWrapper::new(state.shard.count_all_clients()).await;
        authorize_read(state, identity, consistency, &rule)?;
        u32::try_from(clients_count).unwrap_or(u32::MAX)
    } else {
        0
    };
    match build_non_replicated_response(
        &state.shard,
        code,
        body,
        Some(identity.user_id),
        &state.roster,
        identity.client_ip,
        clients_count,
    )
    .map_err(ReadError::Rejected)?
    {
        NonReplicatedResponse::Empty => Err(ReadError::NotFound),
        NonReplicatedResponse::Bytes(bytes) => Ok(bytes),
    }
}

/// Every gate a read served from THIS node's state has to pass, in the one
/// order that is safe.
///
/// The chokepoint for the whole REST read surface: [`read_local`] runs it for
/// the metadata-STM entity reads, and the routes that cannot use `read_local`
/// call it directly - the cross-shard client reads, which serve from each
/// shard's session manager; the snapshot route, which shells out; and the
/// partition reads, which answer from a partition group's log. Skipping it is
/// how a route silently loses authorization, the post-restart barrier, or the
/// read-your-writes hold; which of the two waits actually applies is
/// [`read_needs_metadata_frontier`]'s decision, not the caller's.
///
/// Order:
/// 1. the recovery barrier, so nothing is served off a WAL suffix that is
///    about to re-commit;
/// 2. authorization, because it is terminal: the linearizable follower
///    redirect must answer 307 immediately rather than after a park, and
///    holding a connection to then answer 403 buys nothing;
/// 3. the read-your-writes hold;
/// 4. authorization AGAIN if that hold actually parked. Every scoped route's
///    rule resolves its entity when the rule RUNS, and a park is precisely
///    the case where the state machine moved under it: an entity that did not
///    exist on the first pass resolved to nothing, where a scope miss is a
///    pass-through, and would be served with no permissioner call at all. Only
///    the parked outcome pays for the second pass.
pub(in crate::http) async fn gate_local_read(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
    code: u32,
    rule: impl Fn(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<(), ReadError> {
    await_recovery_barrier(&state.shard).await?;
    authorize_read(state, identity, consistency, &rule)?;
    if read_needs_metadata_frontier(code)
        && await_metadata_read_frontier(state, identity).await? == FrontierWait::CaughtUp
    {
        authorize_read(state, identity, consistency, &rule)?;
    }
    Ok(())
}

/// Hold a local metadata read until this node has applied everything the
/// calling user was told committed.
///
/// A committed control-plane reply hands the caller an op number; answering its
/// next read from a state machine below that op contradicts the response it is
/// holding. The lag is real on a node that is not the metadata primary: a
/// healthy backup FORWARDS a `Register` to the primary
/// (`dispatch::submit_register_local_or_forward`) and binds the committed epoch
/// while its own commit walk is still behind it, so the caller holds an op that
/// node has not applied before it has issued a single write. A control-plane
/// write posted to a backup never commits there either: it is relayed to the
/// primary (forwarding on) or refused transient (forwarding off), so every op a
/// backup promises is one it learned from the primary rather than applied
/// itself.
///
/// Adjacent to `?consistency=linearizable`, not in competition with it. That
/// asks for the freshest CLUSTER state and is answered by leaving this node
/// (307 to the primary), which [`authorize_read`] decides before this wait and
/// which this wait never sees. This gate makes an UNQUALIFIED read
/// read-your-writes for its own user, at no redirect and no consensus round
/// trip. Per user rather than per credential because a bearer is not stable:
/// a refreshed access token is a new credential for the same writer (see
/// [`crate::http::state::MetadataWatermarks`]).
///
/// Scope is this node's own view, which a RELAYED write is part of: the relay
/// records the serving primary's applied op off the response header
/// (`http::forward::record_relayed_floor`), so a caller that posted through
/// this follower is held here too. A user who wrote through a different node
/// entirely still leaves no floor here, and neither does a relayed write
/// answered with the `TransientNotCommitted` 503, whose op may have committed
/// anyway (see `http::submit::submit_committed`).
///
/// The wait itself is the binary plane's [`hold_for_frontier`]: woken by the
/// commit that advances the frontier, bounded by a `compio::time` timer like
/// the recovery barrier below, since this listener is pinned to shard 0's
/// compio thread and has no blocking pool to hand a wait to. Only this request
/// parks, and it parks on one wake rather than a timer per tick.
async fn await_metadata_read_frontier(
    state: &HttpInner,
    identity: &Identity,
) -> Result<FrontierWait, ReadError> {
    let frontier = state.shard.plane.metadata().applied_frontier();
    let budget = frontier.read_budget();
    hold_for_frontier(
        frontier,
        state.metadata_watermark(identity.user_id),
        compio::time::sleep(budget),
    )
    .await
    .map_err(|FrontierUnreached| {
        state
            .shard
            .metrics()
            .record_metadata_read_frontier_refusal();
        tracing::debug!(
            frontier = frontier.get(),
            watermark = state.metadata_watermark(identity.user_id),
            ?budget,
            "metadata read frontier unreached inside the budget; failing read with retryable 503"
        );
        ReadError::MetadataFrontierUnreached
    })
}

/// One recovery-barrier check's outcome, factored out of [`await_recovery_barrier`]
/// so the expiry decision is unit-testable without a runtime: the loop reads the
/// clock and injects whether the deadline has passed.
#[derive(Debug, PartialEq, Eq)]
enum BarrierWait {
    /// Barrier met, or none armed: serve the read.
    Ready,
    /// Barrier unmet and the deadline has passed: fail loud.
    Expired,
    /// Barrier unmet, deadline still ahead: keep polling.
    Pending,
}

/// Decide the barrier outcome from the armed barrier, the locally applied commit
/// point, and whether the deadline has passed. A met barrier wins over an
/// expired deadline, so recovery that completes as the deadline lands still
/// serves rather than 503-ing.
const fn barrier_state(barrier: u64, commit_min: u64, expired: bool) -> BarrierWait {
    if barrier == 0 || commit_min >= barrier {
        BarrierWait::Ready
    } else if expired {
        BarrierWait::Expired
    } else {
        BarrierWait::Pending
    }
}

/// Hold a local read while the recovered WAL suffix re-commits.
///
/// Recovery re-pipelines prepared-but-uncommitted ops that clients saw
/// committed before the restart; JWT-authenticated HTTP reads skip consensus
/// entirely, so without this wait they can observe state that rolls back
/// committed history in the first few hundred milliseconds after a restart.
/// `Ok(())` immediately when no suffix is pending (`recovery_barrier() == 0`).
///
/// Bounded by the barrier's paired deadline (scaled from the configured
/// cluster timeouts; see `recovery_barrier_deadline`). If the suffix has not
/// re-committed by then the read fails loud with a retryable 503
/// ([`ReadError::RecoveryIncomplete`]) instead of silently serving pre-restart
/// state a client already saw acked; the caller retries against a converged
/// cluster.
pub(in crate::http) async fn await_recovery_barrier(
    shard: &Rc<ServerShard>,
) -> Result<(), ReadError> {
    // The consensus tick, like the read gate's cadence: what lifts this
    // barrier is a commit walk, which advances on that clock.
    const POLL: std::time::Duration = consensus::TICK_INTERVAL;

    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return Ok(());
    };
    // Gate on commit_MIN (locally applied), not commit_max (known committed):
    // a StartView adoption advances commit_max first and only then walks the
    // journal applying ops, and this task interleaves with that walk at its
    // await points -- a commit_max gate would serve state from before the
    // suffix applied (e.g. a pre-restart password change not yet visible).
    if barrier_state(consensus.recovery_barrier(), consensus.commit_min(), false)
        == BarrierWait::Ready
    {
        return Ok(());
    }
    let deadline = std::time::Instant::now() + consensus.recovery_deadline();
    loop {
        // Re-read per poll, like `commit_min`. `redecide_recovery_barrier` lowers
        // the barrier when a view change settles the recovered suffix, so a reader
        // holding the value it captured on entry waits out a barrier that no longer
        // exists and then 503s on a replica that is serving.
        let barrier = consensus.recovery_barrier();
        let expired = std::time::Instant::now() >= deadline;
        match barrier_state(barrier, consensus.commit_min(), expired) {
            BarrierWait::Ready => return Ok(()),
            BarrierWait::Expired => {
                tracing::warn!(
                    barrier,
                    commit_min = consensus.commit_min(),
                    "recovered suffix still unapplied past deadline; failing read with retryable 503"
                );
                return Err(ReadError::RecoveryIncomplete);
            }
            BarrierWait::Pending => compio::time::sleep(POLL).await,
        }
    }
}

/// Resolve a wire stream identifier to its committed slab id for a read/write
/// gate, or `None` on a miss (the gate is then a pass-through, so the existing
/// not-found path renders the 404).
pub(in crate::http) fn resolve_gate_stream(
    state: &HttpInner,
    stream_id: &WireIdentifier,
) -> Option<usize> {
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .resolve_stream_id(stream_id)
}

/// Resolve a wire user identifier to its committed slab id, or `None` on a
/// miss. Mirrors the TCP dispatch gate's self-read resolution.
pub(in crate::http) fn resolve_gate_user(
    state: &HttpInner,
    user_id: &WireIdentifier,
) -> Option<usize> {
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|inner| inner.resolve_user_id(user_id))
}

/// Resolve a wire (stream, topic) pair to committed slab ids, or `None` if
/// either misses.
pub(in crate::http) fn resolve_gate_topic(
    state: &HttpInner,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
) -> Option<(usize, usize)> {
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .resolve_topic_ids(stream_id, topic_id)
}

/// Resolve an (`Identifier`, `Identifier`) pair to committed (stream, topic)
/// slab ids for a gate, or `None` on any conversion or resolution miss. For the
/// poll / consumer-offset read routes, which carry domain identifiers rather
/// than the pre-converted wire form the entity reads hold.
pub(in crate::http) fn resolve_gate_topic_ids(
    state: &HttpInner,
    stream_id: &Identifier,
    topic_id: &Identifier,
) -> Option<(usize, usize)> {
    let wire_stream = identifier_to_wire(stream_id).ok()?;
    let wire_topic = identifier_to_wire(topic_id).ok()?;
    resolve_gate_topic(state, &wire_stream, &wire_topic)
}

/// Authorize an HTTP data-plane write (produce / consumer-offset) on (stream,
/// topic). A resolution miss returns `Ok(())` so the write proceeds to the
/// dispatch gates, which render the existing 404; a resolved entity whose rule
/// rejects returns the `Unauthorized` error for a 403. Kept handler-side so an
/// HTTP denial never enters the partition plane (the plane's empty replies
/// carry no status a slot-waiting handler could read as a 403).
pub(in crate::http) fn authorize_data_plane(
    state: &HttpInner,
    user_id: u32,
    stream_id: &Identifier,
    topic_id: &Identifier,
    rule: impl FnOnce(&Permissioner, u32, usize, usize) -> Result<(), IggyError>,
) -> Result<(), IggyError> {
    let (Ok(wire_stream), Ok(wire_topic)) =
        (identifier_to_wire(stream_id), identifier_to_wire(topic_id))
    else {
        return Ok(());
    };
    let Some((stream_id, topic_id)) = resolve_gate_topic(state, &wire_stream, &wire_topic) else {
        return Ok(());
    };
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .authorize(|permissioner| rule(permissioner, user_id, stream_id, topic_id))
}

static DURABILITY_KEY: std::sync::LazyLock<iggy_common::HeaderKey> =
    std::sync::LazyLock::new(|| "durability".parse().expect("catalog key is valid"));

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::http) struct TopicDurability {
    stream_id: usize,
    topic_id: usize,
    created_revision: u64,
    pub durability: iggy_common::Durability,
}

impl TopicDurability {
    pub fn identifiers(self) -> Result<(Identifier, Identifier), IggyError> {
        let stream_id = u32::try_from(self.stream_id).map_err(|_| IggyError::InvalidIdentifier)?;
        let topic_id = u32::try_from(self.topic_id).map_err(|_| IggyError::InvalidIdentifier)?;
        Ok((
            Identifier::numeric(stream_id)?,
            Identifier::numeric(topic_id)?,
        ))
    }

    pub fn confirmed_policy(self, state: &HttpInner) -> iggy_common::Durability {
        state
            .shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .read(|inner| self.confirmed_policy_in(inner))
    }

    fn confirmed_policy_in(
        self,
        inner: &metadata::stm::stream::StreamsInner,
    ) -> iggy_common::Durability {
        let unchanged = inner
            .items
            .get(self.stream_id)
            .and_then(|stream| stream.topics.get(self.topic_id))
            .and_then(|topic| topic.partitions.first())
            .is_some_and(|partition| partition.created_revision == self.created_revision);
        if unchanged {
            self.durability
        } else {
            iggy_common::Durability::Replicated
        }
    }
}

pub(in crate::http) fn topic_durability(
    state: &HttpInner,
    stream: &Identifier,
    topic: &Identifier,
) -> Option<TopicDurability> {
    let stream = identifier_to_wire(stream).ok()?;
    let topic = identifier_to_wire(topic).ok()?;
    state
        .shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| {
            let stream_id = inner.resolve_stream_id(&stream)?;
            let topic_id = inner.resolve_topic_id(stream_id, &topic)?;
            let topic = inner.items.get(stream_id)?.topics.get(topic_id)?;
            let created_revision = topic.partitions.first()?.created_revision;
            Some(TopicDurability {
                stream_id,
                topic_id,
                created_revision,
                durability: topic
                    .options
                    .get(&DURABILITY_KEY)
                    .and_then(|option| std::str::from_utf8(option.value.as_bytes()).ok())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_default(),
            })
        })
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::pending;
    use std::net::Ipv4Addr;
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::Arc;

    use axum::Json;
    use axum::body::Body;
    use axum::extract::{FromRequestParts, Path, Query, State};
    use axum::http::{Request, StatusCode, header::AUTHORIZATION};
    use axum::response::IntoResponse;
    use configs::server::ServerConfig;
    use consensus::{LocalPipeline, MetadataHandle, VsrConsensus};
    use futures::poll;
    use hyper::service::Service as _;
    use hyper_util::service::TowerToHyperService;
    use iggy_binary_protocol::codes::{
        DESCRIBE_OPTIONS_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
        GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
        GET_USER_CODE, GET_USERS_CODE,
    };
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_binary_protocol::requests::streams::{
        CreateStreamRequest, DeleteStreamRequest, UpdateStreamRequest,
    };
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest, UpdateTopicRequest,
    };
    use iggy_binary_protocol::requests::users::{
        CreateUserRequest, DeleteUserRequest, UpdatePermissionsRequest,
    };
    use iggy_binary_protocol::responses::streams::get_stream::GetStreamResponse;
    use iggy_binary_protocol::responses::topics::get_topic::GetTopicResponse;
    use iggy_binary_protocol::{
        ConsumerSession, Operation, PrepareHeader, WireDecode, WireEncode, WireIdentifier,
        WireName, WireOptions,
    };
    use iggy_common::create_personal_access_token::CreatePersonalAccessToken;
    use iggy_common::{IggyByteSize, IggyError, IggyTimestamp, UserStatus};
    use message_bus::IggyMessageBus;
    use message_bus::installer::conn_info::ClientTransportKind;
    use message_bus::transports::tls::install_default_crypto_provider;
    use metadata::AppliedFrontier;
    use metadata::impls::metadata::StreamsFrontend;
    use metadata::stm::StateHandler;
    use metadata::stm::authz::GatedApply;
    use metadata::stm::stream::StreamsInner;
    use metadata::stm::user::{CreatePersonalAccessTokenRequest, PAT_TOKEN_HASH_BYTES};
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use send_wrapper::SendWrapper;
    use server_common::sharding::{METADATA_GROUP, PartitionLocation, ShardId};
    use shard::shards_table::{PapayaShardsTable, ShardsTable};
    use shard::{
        ConnectedClientInfo, LifecycleFrame, ListClientsReply, PartitionConsensusConfig,
        PartitionRead, PartitionReadReply, ReplicaTopology, ShardFrame, ShardIdentity,
        TaggedSender, shard_channel,
    };
    use tokio::sync::Mutex;

    use super::{
        BarrierWait, FrontierWait, ReadError, barrier_state, hold_for_frontier,
        read_needs_metadata_frontier,
    };
    use crate::cluster_meta::ClusterRoster;
    use crate::consumer_group::lease::SessionActivity;
    use crate::dispatch::test_support::{
        create_one_partition_in, create_reader_request, prepare_message, register_reply,
    };
    use crate::http::error::{ConsistencyQuery, WriteError};
    use crate::http::extractor::{Authenticated, Identity};
    use crate::http::handlers::{
        create_pat, get_client, get_clients, get_metrics, get_stats, get_user,
    };
    use crate::http::metrics::HttpMetrics;
    use crate::http::router;
    use crate::http::session::{FIRST_REQUEST_ID, HttpSession, RegistrationBarrier};
    use crate::http::state::{HttpInner, HttpState, MetadataWatermarks};
    use crate::http::wire::encode_send_messages;
    use crate::shell::{ServerMetadata, ServerMuxStateMachine, ServerShard};

    #[compio::test]
    async fn given_authenticated_read_when_account_replaced_while_held_should_reject() {
        const ACCOUNT_ID: u32 = 1;
        const CREATED_AT: u64 = 10;
        const DELETED_AT: u64 = 20;
        const REPLACED_AT: u64 = 30;
        const READ_FLOOR: u64 = 1;

        // The metrics scrape waits on the recovery barrier only.
        for (recovery_barrier, scrape_metrics) in [(true, false), (false, false), (true, true)] {
            let state = read_state();
            let mut account = CreateUserRequest {
                username: WireName::new("original").unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: None,
                options: WireOptions::empty(),
            };
            commit_user_request(
                &state,
                CREATED_AT,
                Operation::CreateUser,
                &account.to_bytes(),
            );
            let identity = authenticated_identity(&state, ACCOUNT_ID, CREATED_AT).await;
            let metadata = state.shard.plane.metadata();
            let consensus = metadata.consensus.as_ref().unwrap();
            if recovery_barrier {
                consensus.set_recovery_deadline(metadata.applied_frontier().read_budget());
                consensus.set_recovery_barrier(READ_FLOOR);
            } else {
                state.metadata_watermarks.record(ACCOUNT_ID, READ_FLOOR);
            }
            let handler_state = state.clone();
            let mut read = pin!(async move {
                if scrape_metrics {
                    get_metrics(State(handler_state), identity)
                        .await
                        .into_response()
                } else {
                    get_user(
                        State(handler_state),
                        identity,
                        Path("me".to_owned()),
                        Query(ConsistencyQuery::default()),
                    )
                    .await
                    .into_response()
                }
            });
            assert!(
                poll!(read.as_mut()).is_pending(),
                "the authenticated read must wait before account replacement"
            );

            commit_user_request(
                &state,
                DELETED_AT,
                Operation::DeleteUser,
                &DeleteUserRequest {
                    user_id: WireIdentifier::numeric(ACCOUNT_ID),
                }
                .to_bytes(),
            );
            account.username = WireName::new("replacement").unwrap();
            commit_user_request(
                &state,
                REPLACED_AT,
                Operation::CreateUser,
                &account.to_bytes(),
            );
            assert_eq!(
                metadata
                    .mux_stm
                    .users()
                    .read(|users| { users.user_by_name("replacement").unwrap().id }),
                ACCOUNT_ID,
                "the replacement must reuse the authenticated account's ID"
            );
            consensus.set_recovery_barrier(0);
            metadata.applied_frontier().advance(READ_FLOOR);

            assert_eq!(
                read.await.status(),
                StatusCode::UNAUTHORIZED,
                "a read held at recovery={recovery_barrier}, metrics={scrape_metrics} must reject a replacement account"
            );
            let identity = authenticated_identity(&state, ACCOUNT_ID, REPLACED_AT).await;
            assert_eq!(
                get_user(
                    State(state.clone()),
                    identity,
                    Path("me".to_owned()),
                    Query(ConsistencyQuery::default()),
                )
                .await
                .into_response()
                .status(),
                StatusCode::OK,
                "the replacement account must retain self access with its own token"
            );
        }
    }

    /// The client reads gather from every shard after their gate, and the
    /// account behind the token can change during the gather.
    #[compio::test]
    async fn given_a_client_read_in_its_gather_when_the_account_changes_should_reject() {
        const CLIENT_ID: u32 = 7;
        for route in ["/stats", "/clients", "/clients/{client_id}"] {
            for replace in [true, false] {
                let (sender, shard_inbox, _shard_replies) = shard_channel(0, 1, 1);
                let state = read_state_on(Some(sender));
                commit_user_request(
                    &state,
                    READER_CREATED_AT,
                    Operation::CreateUser,
                    &create_reader_request().to_bytes(),
                );
                let identity = authenticated_identity(&state, READER_ID, READER_CREATED_AT).await;
                let handler_state = state.clone();
                let mut read = pin!(async move {
                    let consistency = Query(ConsistencyQuery::default());
                    match route {
                        "/stats" => get_stats(State(handler_state), identity, consistency)
                            .await
                            .into_response(),
                        "/clients" => get_clients(State(handler_state), identity, consistency)
                            .await
                            .into_response(),
                        _ => {
                            get_client(State(handler_state), identity, Path(CLIENT_ID), consistency)
                                .await
                                .into_response()
                        }
                    }
                });
                assert!(poll!(read.as_mut()).is_pending());
                let Ok(ShardFrame::Lifecycle(LifecycleFrame::ListClients { reply })) =
                    shard_inbox.try_recv()
                else {
                    panic!("{route} must gather from every shard");
                };

                let rejection = change_reader(&state, replace);
                match reply {
                    ListClientsReply::Clients(reply) => reply
                        .try_send(vec![ConnectedClientInfo {
                            client_id: u128::from(CLIENT_ID),
                            vsr_client_id: None,
                            user_id: Some(READER_ID),
                            transport: ClientTransportKind::Tcp,
                            address: (Ipv4Addr::LOCALHOST, 0).into(),
                            sdk_name: None,
                            sdk_version: None,
                            protocol_version: None,
                        }])
                        .unwrap(),
                    ListClientsReply::Count(reply) => reply.try_send(1).unwrap(),
                    ListClientsReply::Sessions(_) => panic!("{route} must not gather sessions"),
                }

                assert_eq!(
                    read.await.status(),
                    rejection,
                    "{route} with the account replaced: {replace}"
                );
            }
        }
    }

    /// Account and path changes must not bypass either partition wait's gate.
    #[compio::test]
    #[allow(clippy::too_many_lines)]
    async fn given_a_partition_read_waiting_on_its_owner_when_the_account_changes_should_reject() {
        for uri in [
            "/streams/stream/topics/topic/messages?consumer_id=1&partition_id=0&kind=offset&value=0&count=1",
            "/streams/stream/topics/topic/consumer-offsets?consumer_id=1&partition_id=0",
        ] {
            for (replace, final_wait, change_path) in [
                (true, false, None),
                (false, false, None),
                (true, true, None),
                (false, true, None),
                (false, true, Some(Operation::DeleteStream)),
                (false, true, Some(Operation::UpdateStream)),
                (false, true, Some(Operation::UpdateTopic)),
            ] {
                let (sender, shard_inbox, _shard_replies) = shard_channel(0, 1, 1);
                let state = read_state_on(Some(sender));
                let namespace = create_one_partition_in(&state.shard.plane.metadata().mux_stm);
                state
                    .shard
                    .shards_table()
                    .insert(namespace, PartitionLocation::new(ShardId::new(0), 0));
                commit_user_request(
                    &state,
                    READER_CREATED_AT,
                    Operation::CreateUser,
                    &create_reader_request().to_bytes(),
                );
                let token = state
                    .jwt
                    .generate(READER_ID, IggyTimestamp::from(READER_CREATED_AT))
                    .unwrap();
                let service = TowerToHyperService::new(router(
                    state.clone(),
                    state.forward.body_limit,
                    None,
                    None,
                    false,
                ));
                let request = Request::builder()
                    .uri(uri)
                    .header(AUTHORIZATION, format!("Bearer {}", token.access_token))
                    .body(Body::empty())
                    .unwrap();
                let mut response = pin!(service.call(request));
                assert!(poll!(response.as_mut()).is_pending());
                let Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                    read: PartitionRead::Primary,
                    reply,
                    ..
                })) = shard_inbox.try_recv()
                else {
                    panic!("{uri} must ask its partition for the primary");
                };

                let primary = PartitionReadReply::Primary {
                    view: 0,
                    primary: 0,
                };
                let (reply, body) = if final_wait {
                    reply.try_send(primary).unwrap();
                    assert!(poll!(response.as_mut()).is_pending());
                    let Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                        read,
                        reply,
                        ..
                    })) = shard_inbox.try_recv()
                    else {
                        panic!("{uri} must wait for its partition data");
                    };
                    let body = match read {
                        PartitionRead::UnboundPollOnPrimary { metadata, .. } => {
                            PartitionReadReply::Poll {
                                context: metadata.context(0),
                                fragments: Vec::new().into(),
                                current_offset: 0,
                            }
                        }
                        PartitionRead::ConsumerOffsetOnPrimary { .. } => {
                            PartitionReadReply::ConsumerOffset {
                                stored: Some(0),
                                current_offset: 0,
                            }
                        }
                        _ => panic!("{uri} sent an unexpected partition read"),
                    };
                    (reply, body)
                } else {
                    (reply, primary)
                };
                let rejection = change_reader(&state, replace);
                if let Some(operation) = change_path {
                    let body = match operation {
                        Operation::DeleteStream => DeleteStreamRequest {
                            stream_id: WireIdentifier::numeric(0),
                        }
                        .to_bytes(),
                        Operation::UpdateStream => UpdateStreamRequest {
                            stream_id: WireIdentifier::numeric(0),
                            name: WireName::new("renamed").unwrap(),
                            options: WireOptions::empty(),
                        }
                        .to_bytes(),
                        Operation::UpdateTopic => UpdateTopicRequest {
                            stream_id: WireIdentifier::numeric(0),
                            topic_id: WireIdentifier::numeric(0),
                            name: WireName::new("renamed").unwrap(),
                            options: WireOptions::empty(),
                        }
                        .to_bytes(),
                        _ => unreachable!(),
                    };
                    commit_user_request(&state, READER_CREATED_AT + 30, operation, &body);
                }
                reply.try_send(body).unwrap();
                // A read that went on would send to the partition again and
                // find it gone, instead of waiting for an answer.
                drop(shard_inbox);

                assert_eq!(
                    response.await.unwrap().status(),
                    rejection,
                    "{uri} with the account replaced: {replace}, final wait: {final_wait}, path change: {change_path:?}"
                );
            }
        }
    }

    /// The account of [`create_reader_request`], the first one after root.
    const READER_ID: u32 = 1;
    const READER_CREATED_AT: u64 = 10;

    /// Replace the reader account under its ID, or revoke its grants, and
    /// return the status that a read the reader started before must answer.
    fn change_reader(state: &HttpState, replace: bool) -> StatusCode {
        const CHANGED_AT: u64 = 20;
        const REPLACED_AT: u64 = 30;
        if !replace {
            commit_user_request(
                state,
                CHANGED_AT,
                Operation::UpdatePermissions,
                &UpdatePermissionsRequest {
                    user_id: WireIdentifier::numeric(READER_ID),
                    permissions: None,
                }
                .to_bytes(),
            );
            return StatusCode::FORBIDDEN;
        }
        commit_user_request(
            state,
            CHANGED_AT,
            Operation::DeleteUser,
            &DeleteUserRequest {
                user_id: WireIdentifier::numeric(READER_ID),
            }
            .to_bytes(),
        );
        commit_user_request(
            state,
            REPLACED_AT,
            Operation::CreateUser,
            &CreateUserRequest {
                username: WireName::new("replacement").unwrap(),
                ..create_reader_request()
            }
            .to_bytes(),
        );
        assert_eq!(
            state
                .shard
                .plane
                .metadata()
                .mux_stm
                .users()
                .read(|users| users.user_by_name("replacement").unwrap().id),
            READER_ID,
            "the replacement must reuse the reader's ID"
        );
        StatusCode::UNAUTHORIZED
    }

    #[test]
    fn produce_routing_keeps_the_attested_topic_when_names_are_reused() {
        for rename_stream in [false, true] {
            let mut inner = StreamsInner::default();
            let stream_id = create_stream(&mut inner, "events");
            let topic_id = create_topic(&mut inner, "events", "orders", 1);
            let original = super::TopicDurability {
                stream_id,
                topic_id,
                created_revision: inner.items[stream_id].topics[topic_id].partitions[0]
                    .created_revision,
                durability: iggy_common::Durability::Persisted,
            };
            let (stream, topic) = original.identifiers().unwrap();
            let message = iggy_common::IggyMessage::builder()
                .payload(bytes::Bytes::from_static(b"payload"))
                .build()
                .unwrap();
            let command = iggy_common::SendMessages {
                batch: iggy_common::IggyMessagesBatch::from(&vec![message]),
                ..Default::default()
            };
            let body = encode_send_messages(&stream, &topic, &command).unwrap();
            let metadata_length = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
            let (request, _) = SendMessagesHeader::decode(&body[4..4 + metadata_length]).unwrap();

            commit(
                &mut inner,
                &UpdateTopicRequest {
                    stream_id: WireIdentifier::named("events").unwrap(),
                    topic_id: WireIdentifier::named("orders").unwrap(),
                    name: WireName::new("renamed").unwrap(),
                    options: WireOptions::empty(),
                },
            );
            let replacement_topic = create_topic(&mut inner, "events", "orders", 2);
            assert_eq!(
                inner.resolve_topic_id(stream_id, &WireIdentifier::named("orders").unwrap()),
                Some(replacement_topic)
            );
            if rename_stream {
                commit(
                    &mut inner,
                    &UpdateStreamRequest {
                        stream_id: WireIdentifier::named("events").unwrap(),
                        name: WireName::new("renamed-stream").unwrap(),
                        options: WireOptions::empty(),
                    },
                );
                let replacement_stream = create_stream(&mut inner, "events");
                assert_eq!(
                    inner.resolve_stream_id(&WireIdentifier::named("events").unwrap()),
                    Some(replacement_stream)
                );
            }

            assert_eq!(inner.resolve_stream_id(&request.stream_id), Some(stream_id));
            assert_eq!(
                inner.resolve_topic_id(stream_id, &request.topic_id),
                Some(topic_id)
            );
            assert_eq!(
                original.confirmed_policy_in(&inner),
                iggy_common::Durability::Persisted
            );
        }
    }

    /// Applies `request` through its `StateHandler`, as a committed prepare
    /// would, and returns the reply body.
    fn commit(
        inner: &mut StreamsInner,
        request: &(impl StateHandler<State = StreamsInner> + std::fmt::Debug),
    ) -> bytes::Bytes {
        let reply = request.apply(inner, IggyTimestamp::default());
        assert_eq!(reply.code, 0, "{request:?} must commit");
        reply.body
    }

    fn create_stream(inner: &mut StreamsInner, name: &str) -> usize {
        let reply = commit(
            inner,
            &CreateStreamRequest {
                name: WireName::new(name).unwrap(),
                options: WireOptions::empty(),
            },
        );
        GetStreamResponse::decode_from(&reply).unwrap().stream.id as usize
    }

    fn create_topic(
        inner: &mut StreamsInner,
        stream: &str,
        name: &str,
        consensus_group_id: u64,
    ) -> usize {
        let reply = commit(
            inner,
            &CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: WireIdentifier::named(stream).unwrap(),
                    partitions_count: 1,
                    name: WireName::new(name).unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: vec![CreatedPartitionAssignment {
                    partition_id: 0,
                    consensus_group_id,
                }],
                created_view: 0,
            },
        );
        GetTopicResponse::decode_from(&reply).unwrap().topic.id as usize
    }

    #[test]
    fn completed_produce_checks_the_stored_topic_incarnation() {
        let mut inner = metadata::stm::stream::StreamsInner::default();
        let mut stream = metadata::stm::stream::Stream::default();
        let topic = metadata::stm::stream::Topic {
            partitions: vec![metadata::stm::stream::Partition::new(
                0,
                1,
                iggy_common::IggyTimestamp::default(),
                3,
                0,
            )],
            ..Default::default()
        };
        let topic_id = stream.topics.insert(topic);
        let stream_id = inner.items.insert(stream);
        let original = super::TopicDurability {
            stream_id,
            topic_id,
            created_revision: 3,
            durability: iggy_common::Durability::Persisted,
        };
        assert_eq!(
            original.confirmed_policy_in(&inner),
            iggy_common::Durability::Persisted
        );
        inner
            .items
            .get_mut(stream_id)
            .unwrap()
            .topics
            .get_mut(topic_id)
            .unwrap()
            .name = "renamed".into();
        assert_eq!(
            original.confirmed_policy_in(&inner),
            iggy_common::Durability::Persisted
        );
        inner
            .items
            .get_mut(stream_id)
            .unwrap()
            .topics
            .get_mut(topic_id)
            .unwrap()
            .partitions[0]
            .created_revision = 4;
        assert_eq!(
            original.confirmed_policy_in(&inner),
            iggy_common::Durability::Replicated
        );
        inner.items.remove(stream_id);
        assert_eq!(
            original.confirmed_policy_in(&inner),
            iggy_common::Durability::Replicated
        );
    }

    /// Root's user id, the caller every fixture below writes and reads as.
    const USER: u32 = 0;

    /// The gate exactly as [`await_metadata_read_frontier`] composes it: the
    /// per-user floor a committed control-plane reply left behind, against the
    /// node-wide applied frontier. Whether a read is HELD is decided by those
    /// two numbers and nothing else, so this is the plane's own contract:
    /// seeded floor, read held, commit that closes the gap serves it.
    ///
    /// The budget is a future that never completes, so a gate that failed to
    /// park would answer instead of hanging, and a gate that failed to wake
    /// would hang instead of answering. The end-to-end REST path (a follower
    /// that binds a forwarded epoch, then reads through axum) is
    /// `integration::server::http_read_your_writes`; the race it depends on
    /// cannot be forced from outside the process, which is why the hold is
    /// pinned here.
    #[compio::test]
    async fn given_a_recorded_floor_when_the_node_catches_up_should_serve_the_held_read() {
        let watermarks = MetadataWatermarks::default();
        let frontier = Arc::new(AppliedFrontier::default());
        frontier.advance(4);

        // A caller this node never wrote for waits for nothing.
        assert_eq!(
            hold_for_frontier(&frontier, watermarks.get(USER), pending()).await,
            Ok(FrontierWait::Ready),
            "an unseeded caller has no write to read back"
        );

        // The committed reply's op becomes the floor, which is above what this
        // node has applied: the read must not be answered yet.
        watermarks.record(USER, 9);
        let committer = Arc::clone(&frontier);
        compio::runtime::spawn(async move {
            compio::runtime::time::sleep(std::time::Duration::ZERO).await;
            committer.advance(9);
        })
        .detach();
        assert_eq!(
            hold_for_frontier(&frontier, watermarks.get(USER), pending()).await,
            Ok(FrontierWait::CaughtUp),
            "the read must be held until the node applies the caller's own op"
        );

        // Caught up: back to the fast path, with the floor still in place.
        assert_eq!(
            hold_for_frontier(&frontier, watermarks.get(USER), pending()).await,
            Ok(FrontierWait::Ready),
            "an applied floor must not cost a park on every later read"
        );
    }

    /// The refusal this plane renders when the frontier never arrives: the
    /// shared retryable 503, never a 2xx carrying the pre-write state.
    #[compio::test]
    async fn given_a_floor_the_node_never_reaches_when_gating_should_render_the_retryable_503() {
        let watermarks = MetadataWatermarks::default();
        watermarks.record(USER, 9);
        let frontier = AppliedFrontier::default();
        frontier.advance(4);

        let outcome = hold_for_frontier(&frontier, watermarks.get(USER), std::future::ready(()))
            .await
            .map_err(|_| ReadError::MetadataFrontierUnreached);
        assert!(
            matches!(outcome, Err(ReadError::MetadataFrontierUnreached)),
            "an unreached frontier must refuse the read, not serve it"
        );
    }

    /// The HTTP read routes share the binary dispatch's exclusion list, so this
    /// pins what that list means for the codes HTTP actually serves: every
    /// entity read is gated, and the static option catalog is not. Forking the
    /// list per plane is what this is here to catch.
    ///
    /// `GET_CLUSTER_METADATA` is deliberately absent: `/cluster/metadata` has
    /// its own local handler and never reaches [`read_local`], so asserting it
    /// here would pin a code this plane cannot produce. The shared predicate's
    /// own arm for it is covered where it is used, in the dispatch spine.
    #[test]
    fn given_the_http_read_codes_when_classified_should_gate_all_but_the_static_catalog() {
        for code in [
            GET_STREAMS_CODE,
            GET_STREAM_CODE,
            GET_TOPICS_CODE,
            GET_TOPIC_CODE,
            GET_USERS_CODE,
            GET_USER_CODE,
            GET_CONSUMER_GROUPS_CODE,
            GET_PERSONAL_ACCESS_TOKENS_CODE,
            GET_STATS_CODE,
        ] {
            assert!(
                read_needs_metadata_frontier(code),
                "code {code} answers from the metadata STM and must be gated"
            );
        }
        assert!(
            !read_needs_metadata_frontier(DESCRIBE_OPTIONS_CODE),
            "the option catalog is static; holding a read of it buys nothing"
        );
    }

    #[test]
    fn barrier_state_ready_when_no_barrier_armed() {
        assert_eq!(barrier_state(0, 0, false), BarrierWait::Ready);
        assert_eq!(barrier_state(0, 0, true), BarrierWait::Ready);
    }

    #[test]
    fn barrier_state_ready_when_commit_reached_barrier() {
        assert_eq!(barrier_state(5, 5, false), BarrierWait::Ready);
        assert_eq!(barrier_state(5, 6, false), BarrierWait::Ready);
    }

    #[test]
    fn barrier_state_pending_while_unmet_before_deadline() {
        assert_eq!(barrier_state(5, 3, false), BarrierWait::Pending);
    }

    #[test]
    fn barrier_state_expires_when_unmet_past_deadline() {
        // Red before the fail-loud change: an expired barrier used to serve the
        // read (a bare `()`), now an unmet barrier past its deadline is a
        // distinct terminal outcome the wait maps to a retryable 503.
        assert_eq!(barrier_state(5, 3, true), BarrierWait::Expired);
    }

    #[test]
    fn barrier_state_met_wins_over_expired_deadline() {
        assert_eq!(barrier_state(5, 5, true), BarrierWait::Ready);
    }

    /// The reply carries only the cap error code, so the HTTP create must
    /// name the caller and the cap itself.
    #[compio::test]
    async fn given_user_at_token_cap_when_creating_over_http_should_name_the_user_and_the_cap() {
        const OWNER: u32 = 7;
        const CLIENT: u128 = 9;
        const SESSION: u64 = 1;
        const TOKEN_CAP: u32 = 1;
        let state = read_state();
        let metadata = state.shard.plane.metadata();
        metadata.set_max_tokens_per_user(TOKEN_CAP);
        let token = CreatePersonalAccessTokenRequest {
            user_id: OWNER,
            name: WireName::new("taken").unwrap(),
            expiry: 0,
            token_hash: [b'a'; PAT_TOKEN_HASH_BYTES],
        };
        commit_user_request(
            &state,
            1,
            Operation::CreatePersonalAccessToken,
            &token.to_bytes(),
        );
        let attachment = {
            let mut client_table = metadata.client_table.borrow_mut();
            client_table
                .commit_register(CLIENT, OWNER, [0x5a; 32], register_reply(CLIENT, SESSION))
                .unwrap();
            client_table.attach_session(CLIENT, SESSION, OWNER).unwrap()
        };
        let session = HttpSession {
            attachment: RefCell::new(attachment),
            activity: Rc::new(SessionActivity::new(ConsumerSession {
                client_id: CLIENT,
                session: SESSION,
            })),
            key: String::new(),
            client_id: CLIENT,
            session: SESSION,
            user_id: OWNER,
            expiry: u64::MAX,
            gate: Mutex::new(FIRST_REQUEST_ID),
            data_gate: Mutex::new(FIRST_REQUEST_ID),
            partition_gates: RefCell::default(),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
        };

        let result = create_pat(
            State(state.clone()),
            Authenticated {
                session: SendWrapper::new(Rc::new(session)),
            },
            Json(CreatePersonalAccessToken {
                name: "fresh".to_owned(),
                ..CreatePersonalAccessToken::default()
            }),
        )
        .await;

        let Err(WriteError::Rejected(error)) = result else {
            panic!("a create at the cap must be rejected");
        };
        assert_eq!(
            error.to_string(),
            IggyError::PersonalAccessTokensLimitReached(OWNER, TOKEN_CAP).to_string()
        );
    }

    fn read_state() -> HttpState {
        read_state_on(None)
    }

    /// [`read_state`] whose shard sends its cross-shard requests to `mesh`,
    /// so a test can hold a read while it waits on another shard.
    fn read_state_on(mesh: Option<TaggedSender>) -> HttpState {
        install_default_crypto_provider();
        let config = Arc::new(ServerConfig::default());
        let prepared = crate::http::prepare(
            (Ipv4Addr::LOCALHOST, 0).into(),
            &config.http,
            &config.cluster,
        )
        .unwrap();
        let bus = Rc::new(IggyMessageBus::with_config(0, &config));
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            METADATA_GROUP,
            Rc::clone(&bus),
            LocalPipeline::new(),
        );
        consensus.init();
        let mux = ServerMuxStateMachine::default();
        mux.users().ensure_root_user("iggy", "hash");
        let metadata = ServerMetadata::new(Some(consensus), None, None, None, mux, None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: iggy_common::DEFAULT_MESSAGES_REQUIRED_TO_SAVE,
                size_of_messages_required_to_save: IggyByteSize::from(
                    iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
                ),
                validate_checksum: config.partition.validate_checksum,
                segment_size: IggyByteSize::from(iggy_common::DEFAULT_SEGMENT_SIZE),
                preallocate_segments: iggy_common::DEFAULT_PREALLOCATE_SEGMENTS,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let mut shard = ServerShard::without_inbox(
            ShardIdentity::new(0, "http-read-test".to_owned()),
            Rc::clone(&bus),
            metadata,
            partitions,
            PapayaShardsTable::new(),
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus),
        );
        if let Some(sender) = mesh {
            shard.attach_senders(vec![sender]);
        }
        let shard = Rc::new(shard);
        SendWrapper::new(Rc::new(HttpInner {
            session_liveness: Rc::default(),
            shard,
            jwt: prepared.jwt,
            server_config: config,
            sessions: RefCell::default(),
            registrations: RegistrationBarrier::default(),
            roster: Rc::new(ClusterRoster::disabled()),
            max_http_sessions: Default::default(),
            in_flight_writes: Cell::default(),
            forward: prepared.forward,
            metrics: HttpMetrics::init(&[]),
            metadata_watermarks: Rc::default(),
        }))
    }

    fn commit_user_request(state: &HttpState, timestamp: u64, operation: Operation, body: &[u8]) {
        let mut prepare = prepare_message(operation, 0, 0, body);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut prepare.as_mut_slice()[..size_of::<PrepareHeader>()],
        );
        header.timestamp = timestamp;
        header.op = timestamp;
        let reply = state
            .shard
            .plane
            .metadata()
            .mux_stm
            .gated_update(consensus::seal_prepare_checksum(prepare))
            .unwrap();
        assert_eq!(reply.code, 0, "{operation:?} must commit");
    }

    async fn authenticated_identity(state: &HttpState, user_id: u32, created_at: u64) -> Identity {
        let token = state
            .jwt
            .generate(user_id, IggyTimestamp::from(created_at))
            .unwrap();
        let (mut parts, ()) = Request::builder()
            .uri("/users/me")
            .header(AUTHORIZATION, format!("Bearer {}", token.access_token))
            .body(())
            .unwrap()
            .into_parts();
        let Ok(identity) = Identity::from_request_parts(&mut parts, state).await else {
            panic!("the active account's token must authenticate");
        };
        identity
    }
}
