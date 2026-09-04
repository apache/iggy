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
use crate::responses::{
    NonReplicatedResponse, build_non_replicated_response, resolve_stream_id, resolve_topic_id,
};

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
/// pairs it with the two waits a local read must serve behind. Callable on its
/// own only for a read that is NOT served from local state.
fn authorize_read(
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
        .authorize(|permissioner| rule(permissioner, identity.user_id))
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
/// same as `/metrics`) before the sync builder. An absent entity surfaces as
/// [`NonReplicatedResponse::Empty`], mapped to 404 here because every REST read
/// whose entity can be missing shares that not-found shape.
pub(in crate::http) async fn read_local(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
    code: u32,
    body: &[u8],
    rule: impl Fn(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<Bytes, ReadError> {
    gate_local_read(state, identity, consistency, code, rule).await?;
    let clients_count = if code == GET_STATS_CODE {
        u32::try_from(SendWrapper::new(state.shard.list_all_clients()).await.len())
            .unwrap_or(u32::MAX)
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
    let barrier = consensus.recovery_barrier();
    // Gate on commit_MIN (locally applied), not commit_max (known committed):
    // a StartView adoption advances commit_max first and only then walks the
    // journal applying ops, and this task interleaves with that walk at its
    // await points -- a commit_max gate would serve state from before the
    // suffix applied (e.g. a pre-restart password change not yet visible).
    if barrier_state(barrier, consensus.commit_min(), false) == BarrierWait::Ready {
        return Ok(());
    }
    let deadline = std::time::Instant::now() + consensus.recovery_deadline();
    loop {
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
/// not-found path renders the 404). Mirrors the TCP dispatch resolvers.
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
        .read(|inner| resolve_stream_id(inner, stream_id))
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
        .read(|inner| {
            let stream_id = resolve_stream_id(inner, stream_id)?;
            let topic_id = resolve_topic_id(inner, stream_id, topic_id)?;
            Some((stream_id, topic_id))
        })
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

#[cfg(test)]
mod tests {
    use super::{
        BarrierWait, FrontierWait, ReadError, barrier_state, hold_for_frontier,
        read_needs_metadata_frontier,
    };
    use crate::http::state::MetadataWatermarks;
    use iggy_binary_protocol::codes::{
        DESCRIBE_OPTIONS_CODE, GET_CONSUMER_GROUPS_CODE, GET_PERSONAL_ACCESS_TOKENS_CODE,
        GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE, GET_TOPIC_CODE, GET_TOPICS_CODE,
        GET_USER_CODE, GET_USERS_CODE,
    };
    use metadata::AppliedFrontier;
    use std::future::pending;
    use std::sync::Arc;

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
}
