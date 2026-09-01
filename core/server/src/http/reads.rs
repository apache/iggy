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
//! per-credential read-your-writes frontier), the local metadata-STM read
//! entry, and the wire/domain identifier resolvers the read and data-plane
//! routes ground their scopes through.

use crate::bootstrap::ServerShard;
use crate::dispatch::read_needs_metadata_frontier;
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

/// The two cross-cutting gates every authenticated read enforces before it
/// touches state. Factored out of [`read_local`] so the cross-shard client
/// reads (`get_clients` / `get_client`) - which serve from the shard session
/// managers, not the local STM, and so cannot use [`read_local`] - still pass
/// the identical gate. Keeping it in one place is what guarantees no read route
/// can silently skip authz or answer a linearizable request on a follower.
///
/// Per-op RBAC: run the route's `rule` against the caller's committed
/// permissions via the live permissioner. A denial (always `Unauthorized`)
/// renders 403 through the legacy `IggyError -> status` map; root holds every
/// grant, so its reads pass without a user-id short-circuit. A linearizable
/// read must come from the primary; on a follower it redirects (307) to the
/// primary's HTTP address when resolvable, else fails closed to a 503 (see
/// [`HttpInner::not_primary_read_error`]).
pub(in crate::http) fn authorize_read(
    state: &HttpInner,
    identity: &Identity,
    consistency: Consistency,
    rule: impl FnOnce(&Permissioner, u32) -> Result<(), IggyError>,
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
    rule: impl FnOnce(&Permissioner, u32) -> Result<(), IggyError>,
) -> Result<Bytes, ReadError> {
    await_recovery_barrier(&state.shard).await?;
    // Ahead of the frontier wait on purpose. `authorize_read` renders the
    // linearizable follower redirect, which must answer 307 immediately -
    // parking first would delay a request this node is not going to serve at
    // all - and an authorization denial is terminal, so holding the connection
    // for it buys nothing.
    authorize_read(state, identity, consistency, rule)?;
    if read_needs_metadata_frontier(code) {
        await_metadata_read_frontier(state, identity).await?;
    }
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

/// Poll cadence while a metadata read waits for this node's applied frontier.
/// The recovery barrier's cadence and the binary read gate's: what the wait is
/// usually short of is a single commit broadcast.
const READ_FRONTIER_POLL: std::time::Duration = std::time::Duration::from_millis(10);

/// Polls one held read is given before it fails retryable: 3s at the cadence
/// above. Matches the binary read gate's budget so both planes give up at the
/// same point, and stays far below the 30s the control-plane write path already
/// spends replaying a transient frame.
const READ_FRONTIER_MAX_POLLS: u32 = 300;

/// Hold a local metadata read until this node has applied everything the
/// presenting credential was told committed.
///
/// A committed control-plane reply hands the caller an op number; answering its
/// next read from a state machine below that op contradicts the response it is
/// holding. The lag is real on a node that is not the metadata primary: a
/// healthy backup FORWARDS a `Register` to the primary
/// (`dispatch::submit_register_local_or_forward`) and binds the committed epoch
/// while its own commit walk is still behind it - and a cluster without shared
/// bearer key material runs with HTTP forwarding off, so control-plane writes
/// stay on that backup instead of being relayed.
///
/// Adjacent to `?consistency=linearizable`, not in competition with it. That
/// asks for the freshest CLUSTER state and is answered by leaving this node
/// (307 to the primary), which [`authorize_read`] decides before this wait and
/// which this wait never sees. This gate makes an UNQUALIFIED read
/// read-your-writes for its own credential, at no redirect and no consensus
/// round trip.
///
/// Scope is this node's own view. A credential whose write this node relayed
/// over HTTP, or that wrote through a different node entirely, left no
/// watermark here; closing that needs the serving primary's commit op to reach
/// the reading node, which nothing in the response carries today.
async fn await_metadata_read_frontier(
    state: &HttpInner,
    identity: &Identity,
) -> Result<(), ReadError> {
    let metadata = state.shard.plane.metadata();
    hold_for_frontier(
        || metadata.applied_frontier(),
        state.metadata_watermark(&identity.session_key),
        READ_FRONTIER_MAX_POLLS,
    )
    .await
}

/// Poll `frontier` for `watermark` up to `max_polls` times, then give up
/// retryable. Split from its call site so the fast path, the park, the catch-up
/// and the expiry are all testable without a live shard - the same reason
/// [`barrier_state`] is split out below.
///
/// A caller with nothing to read back has `watermark == 0`, which the first
/// comparison satisfies: one `Acquire` load, no await, no allocation. Expiry is
/// loud and carries both numbers, so a frontier that stopped moving is visible
/// instead of showing up as latency.
async fn hold_for_frontier(
    frontier: impl Fn() -> u64,
    watermark: u64,
    max_polls: u32,
) -> Result<(), ReadError> {
    if frontier() >= watermark {
        return Ok(());
    }
    for _ in 0..max_polls {
        // `compio::time::sleep` like the recovery barrier below: this listener
        // is pinned to shard 0's compio thread, which has no blocking pool to
        // hand a wait to. Only this request parks.
        compio::time::sleep(READ_FRONTIER_POLL).await;
        if frontier() >= watermark {
            return Ok(());
        }
    }
    tracing::warn!(
        frontier = frontier(),
        watermark,
        "metadata read frontier unreached past deadline; failing read with retryable 503"
    );
    Err(ReadError::MetadataFrontierUnreached)
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
    const POLL: std::time::Duration = std::time::Duration::from_millis(10);

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
        BarrierWait, READ_FRONTIER_MAX_POLLS, ReadError, barrier_state, hold_for_frontier,
        read_needs_metadata_frontier,
    };
    use iggy_binary_protocol::codes::{
        DESCRIBE_OPTIONS_CODE, GET_CLUSTER_METADATA_CODE, GET_CONSUMER_GROUPS_CODE,
        GET_PERSONAL_ACCESS_TOKENS_CODE, GET_STATS_CODE, GET_STREAM_CODE, GET_STREAMS_CODE,
        GET_TOPIC_CODE, GET_TOPICS_CODE, GET_USER_CODE, GET_USERS_CODE,
    };
    use std::cell::Cell;

    /// A caller with nothing to read back (`watermark == 0`) and one whose
    /// watermark this node has already applied are the whole steady state, and
    /// neither may cost a park: exactly one load, no await. A gate that polled
    /// here would put 10ms on every REST read in the cluster.
    #[compio::test]
    async fn given_a_frontier_at_the_watermark_when_gating_should_serve_without_parking() {
        for (frontier_value, watermark) in [(0, 0), (7, 7), (9, 7)] {
            let loads = Cell::new(0u32);
            let outcome = hold_for_frontier(
                || {
                    loads.set(loads.get() + 1);
                    frontier_value
                },
                watermark,
                READ_FRONTIER_MAX_POLLS,
            )
            .await;
            assert!(
                outcome.is_ok(),
                "frontier {frontier_value} covers {watermark}"
            );
            assert_eq!(
                loads.get(),
                1,
                "frontier {frontier_value} covers {watermark}, so the read must not poll"
            );
        }
    }

    /// The gate's whole point: a read whose credential was told op 9 committed
    /// is held, not answered, while this node is still at op 4 - and it is
    /// answered as soon as the node catches up, rather than being failed.
    #[compio::test]
    async fn given_a_frontier_behind_the_watermark_when_gating_should_hold_until_it_catches_up() {
        const CATCH_UP_AFTER: u32 = 2;
        let polls = Cell::new(0u32);
        let outcome = hold_for_frontier(
            || {
                let seen = polls.get();
                polls.set(seen + 1);
                if seen >= CATCH_UP_AFTER { 9 } else { 4 }
            },
            9,
            8,
        )
        .await;

        assert!(
            outcome.is_ok(),
            "the read must be served once the node caught up"
        );
        assert!(
            polls.get() > CATCH_UP_AFTER,
            "the read was answered off the lagging frontier after {} loads",
            polls.get()
        );
    }

    /// A node can legitimately never catch up (a durably lagging replica), so
    /// the park is bounded - and the exit is a retryable refusal, never the
    /// stale answer. `MetadataFrontierUnreached` renders the shared 503 (see
    /// `error.rs`).
    #[compio::test]
    async fn given_a_frontier_that_never_catches_up_when_gating_should_fail_retryable() {
        let outcome = hold_for_frontier(|| 4, 9, 3).await;
        assert!(
            matches!(outcome, Err(ReadError::MetadataFrontierUnreached)),
            "an unreached frontier must refuse the read, not serve it"
        );
    }

    /// The HTTP read routes share the binary dispatch's exclusion list, so this
    /// pins what that list means for the codes HTTP actually serves: every
    /// entity read is gated, and the static option catalog is not. Forking the
    /// list per plane is what this is here to catch.
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
        for code in [DESCRIBE_OPTIONS_CODE, GET_CLUSTER_METADATA_CODE] {
            assert!(
                !read_needs_metadata_frontier(code),
                "code {code} answers from a static catalog or the roster; holding it buys nothing"
            );
        }
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
