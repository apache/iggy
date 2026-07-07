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

//! Read-path gates: the shared per-op RBAC + consistency check, the local
//! metadata-STM read entry, and the wire/domain identifier resolvers the read
//! and data-plane routes ground their scopes through.

use crate::bootstrap::ServerNgShard;
use bytes::Bytes;
use consensus::MetadataHandle;
use iggy_binary_protocol::WireIdentifier;
use iggy_common::wire_conversions::identifier_to_wire;
use iggy_common::{Identifier, IggyError};
use metadata::impls::metadata::StreamsFrontend;
use metadata::permissioner::Permissioner;
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
        return Err(state.not_primary_read_error(&identity.path_and_query));
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
/// is a pure STM read. It is synchronous, so this helper is too - no submit
/// await, no gate, no `SendWrapper`. An absent entity surfaces as
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
    await_recovery_barrier(&state.shard).await;
    authorize_read(state, identity, consistency, rule)?;
    match build_non_replicated_response(
        &state.shard,
        code,
        body,
        Some(identity.user_id),
        &state.roster,
    )
    .map_err(ReadError::Rejected)?
    {
        NonReplicatedResponse::Empty => Err(ReadError::NotFound),
        NonReplicatedResponse::Bytes(bytes) => Ok(bytes),
    }
}

/// Hold a local read while the recovered WAL suffix re-commits.
///
/// Recovery re-pipelines prepared-but-uncommitted ops that clients saw
/// committed before the restart; JWT-authenticated HTTP reads skip consensus
/// entirely, so without this wait they can observe state that rolls back
/// committed history in the first few hundred milliseconds after a restart.
/// No-op (`recovery_barrier() == 0`) outside that window. Bounded: the
/// suffix needs a backup quorum to re-commit, so serve anyway after the
/// deadline rather than wedging reads on a partitioned cluster.
pub(in crate::http) async fn await_recovery_barrier(shard: &Rc<ServerNgShard>) {
    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
    const POLL: std::time::Duration = std::time::Duration::from_millis(10);

    let Some(consensus) = shard.plane.metadata().consensus.as_ref() else {
        return;
    };
    let barrier = consensus.recovery_barrier();
    if barrier == 0 || consensus.commit_max() >= barrier {
        return;
    }
    let deadline = std::time::Instant::now() + DEADLINE;
    while consensus.commit_max() < barrier {
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                barrier,
                commit_max = consensus.commit_max(),
                "recovered suffix still uncommitted past deadline; serving read anyway"
            );
            return;
        }
        compio::time::sleep(POLL).await;
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
