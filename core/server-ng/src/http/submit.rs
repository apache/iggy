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

//! Write submission: the gate-serialized control-plane submit run on a
//! detached task, the awaited/fire-and-forget partition write paths, and the
//! session logout teardown.

use std::rc::Rc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::channel::oneshot;
use iggy_binary_protocol::consensus::Command2;
use iggy_binary_protocol::{GenericHeader, Operation, RequestHeader};
use iggy_common::IggyError;
use server_common::Message;
use tracing::warn;

use crate::bootstrap::ServerNgShard;
use crate::dispatch::{
    dispatch_partition_request, resolve_delete_segments_truncate, submit_client_request_on_owner,
    submit_logout_on_owner,
};
use crate::http::admission::admit_partition_write;
use crate::http::error::{PartitionWriteError, WriteError};
use crate::http::reply::{
    classify_partition_reply, committed_payload, eviction_error, transient_code,
};
use crate::http::session::HttpSession;
use crate::http::state::HttpInner;
use crate::http::wire::build_request_message;
use crate::pat::rewrite_pat_request_for_user;
use crate::users::maybe_rewrite_user_password_request;
use crate::wire::request_body;

/// Bound on a partition write's (produce / consumer-offset write) wait for its
/// committed reply. Long enough to ride out a view change (plus the dispatch
/// gates' own routable-wait budget), short enough not to pin HTTP connections
/// behind a dead consensus group. On expiry the caller gets 504 and must treat
/// the outcome as unknown: the partition plane is at-least-once and the prepare
/// may still commit after the wait gave up, so the server never retries on the
/// caller's behalf.
const PARTITION_WRITE_REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// Replay cadence for a control-plane write answered with the pre-consensus
/// `TransientNotCommitted` frame (not-caught-up primary, pipeline pressure,
/// or a view-change cancel). Mirrors the binary SDKs' in-client replay loop
/// (each TCP/QUIC/WS client's `NOT_READY_RETRY_INTERVAL`):
/// those transports absorb the frame client-side, HTTP has no SDK loop, so
/// the server replays here for transport parity.
const TRANSIENT_RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// Total replay budget for one control-plane write, mirroring the binary
/// SDKs' `RESPONSE_READ_TIMEOUT` bound on the same loop. On exhaustion the op
/// has still not entered consensus, so the caller gets a retryable 503, never
/// a terminal error.
const TRANSIENT_RETRY_DEADLINE: Duration = Duration::from_secs(30);

/// Run one authenticated control-plane write to commit and hand back the
/// committed reply `Message`, the request header, and any raw PAT token minted
/// along the way. Shared core of every HTTP write: [`submit_write`] decodes the
/// reply body for the stream/topic/user routes, while [`create_pat`] needs the
/// raw `Message` + request header to substitute the one-time token.
///
/// Control-plane writes are authorized in-apply on the metadata STM, so this
/// runs no pre-submit gate; it drives the gate-locked submit
/// ([`submit_gated`]) on a detached shard-0 task and awaits its outcome over a
/// oneshot. Axum drops this handler future the moment the HTTP client
/// disconnects; were the gate held here, that drop could land between the
/// pump-driven consensus commit and the id advance, leaving the committed op's
/// reply cached under an id the session still considers unused - the next
/// write would collide into `Duplicate` and silently receive the prior op's
/// reply. Detaching moves the whole critical section out of the cancellable
/// future; a disconnect now only drops the receiver half.
pub(in crate::http) async fn submit_committed(
    state: &HttpInner,
    session: &Rc<HttpSession>,
    operation: Operation,
    body: &[u8],
) -> Result<(RequestHeader, Message<GenericHeader>, Option<String>), WriteError> {
    // Control writes are authorized in-apply on the metadata STM: a denial
    // comes back as `Unauthorized` in the committed result section, which
    // `committed_payload` maps to a 403. No pre-submit gate here, so the
    // in-apply check is the single source of truth. Self-scoped PAT ops (which
    // the in-apply gate skips) reach here as any authenticated user, matching
    // legacy parity.
    let (result_slot, committed) = oneshot::channel();
    let shard = Rc::clone(&state.shard);
    let task_session = Rc::clone(session);
    let body = body.to_vec();
    // Detached so a client disconnect cannot split a commit from its id
    // advance; both happen inside the task regardless of handler liveness.
    compio::runtime::spawn(async move {
        let result = submit_gated(&shard, &task_session, operation, &body).await;
        // A failed send means the handler died mid-await. The id advance
        // already happened above, which is the invariant that matters.
        let _ = result_slot.send(result);
    })
    .detach();
    // `Canceled` = the task was dropped before sending (runtime teardown), a
    // transient server condition like an unanswered submit.
    let outcome = committed.await.map_err(|_| WriteError::Unavailable)?;
    // An eviction means this session's VSR slot is gone: forget the entry so
    // the caller's next request re-registers instead of 401-looping on it.
    if matches!(&outcome, Err(WriteError::Evicted(_))) {
        state.forget_session(session);
    }
    outcome
}

/// Gate-locked core of [`submit_committed`], run on a task the HTTP client
/// cannot cancel: serializes this session's writes behind its gate and holds
/// it across the submit so request ids reach the primary strictly in order.
/// The gate advances only on a genuine committed `Reply`: an unanswered
/// submit, a `TransientNotCommitted` frame, or an eviction leaves the id
/// free, which the depth-1 dedup requires (the next accepted request must be
/// `committed + 1`, so a consumed-but-uncommitted id would wedge the session
/// on `RequestGap`). A transient frame is replayed in place with the same id
/// on the binary SDKs' cadence (see [`TRANSIENT_RETRY_INTERVAL`]). An
/// eviction means the session is dead: mapped to 401, and [`submit_committed`]
/// then forgets the entry so the caller's retry re-registers cleanly.
///
/// Both shard-0 request rewrites run here before consensus, mirroring the TCP
/// dispatch path: the PAT rewrite mints a raw token and replicates only its hash
/// (`CreatePersonalAccessToken`), and the user-password rewrite hashes the new
/// password and, for `ChangePassword`, strips the current one and verifies it
/// against the stored hash (`CreateUser` / `ChangePassword`). Both are no-ops
/// for every other operation, so plaintext secrets never enter consensus on any
/// write. A third resolution handles `DeleteSegments`, which is not itself a
/// consensus op: it is rewritten to the metadata `TruncatePartition` that commits
/// the trim (see [`resolve_delete_segments_truncate`]), so the truncate rides
/// this session's gate id. On a rejection (a malformed body or an unresolved
/// delete-segments namespace) the gate is released without advancing, leaving
/// the id free. A failed current-password check is not a rejection here: the op
/// still commits with an emptied new-password sentinel that the replicated
/// apply grades to `InvalidCredentials`, so the id advances on the committed
/// reply (see `verify_and_rewrite_change_password`).
async fn submit_gated(
    shard: &Rc<ServerNgShard>,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<(RequestHeader, Message<GenericHeader>, Option<String>), WriteError> {
    let mut next_request_id = session.gate.lock().await;
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        *next_request_id,
        body,
    );
    let (message, raw_token) =
        rewrite_pat_request_for_user(session.user_id, message).map_err(WriteError::Rejected)?;
    let message =
        maybe_rewrite_user_password_request(shard, message).map_err(WriteError::Rejected)?;
    // `DeleteSegments` is not itself a consensus op: resolve it to the metadata
    // `TruncatePartition` that commits the trim before it reaches consensus,
    // mirroring the TCP dispatch. The truncate rides this session's gate id, so
    // the metadata request sequence stays contiguous; an unresolved namespace
    // releases the gate without advancing, like a rejected rewrite.
    let message = if message.header().operation == Operation::DeleteSegments {
        let template = *message.header();
        resolve_delete_segments_truncate(
            shard,
            &template,
            session.client_id,
            session.session,
            request_body(&message),
        )
        .await
        .map_err(WriteError::Rejected)?
    } else {
        message
    };
    let request_header = *message.header();
    let deadline = Instant::now() + TRANSIENT_RETRY_DEADLINE;
    let mut request = message;
    let mut saw_not_committed = false;
    let reply = loop {
        // The submit consumes the request; keep a byte-identical copy for a
        // possible replay. Re-running the rewrites instead would mint a fresh
        // PAT token / password hash and break same-id dedup idempotency.
        let retry_request = request.clone();
        let Some(reply) = submit_client_request_on_owner(shard, request).await else {
            return Err(WriteError::Unavailable);
        };
        let transient = (reply.header().command == Command2::Reply)
            .then(|| transient_code(&reply))
            .flatten();
        let Some(transient) = transient else {
            break reply;
        };
        saw_not_committed |= matches!(transient, IggyError::TransientNotCommitted);
        // Pre-consensus transient frame: replay the SAME request id, mirroring
        // the binary SDKs' in-client loop. Safe to replay - the dominant
        // emissions never entered the pipeline, and the view-change cancel is
        // dedup-idempotent (the client table serves the cached reply). The
        // gate stays held across the replay on purpose: the request keeps its
        // serialization turn, and a queued same-session write would only hit
        // the same transient.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // Budget exhausted: surface a retryable 503 (never the catch-all
            // 400), with the code sticky across frames. Once ANY frame was
            // `TransientNotCommitted` the op may still commit cluster-wide (a
            // view-change-canceled prepare can reach quorum and be inherited
            // by the new primary), so a later `TransientNotAccepted` frame -
            // this node losing the primary role mid-replay - must not
            // downgrade it: `TransientNotAccepted` licenses a forwarding
            // follower to re-issue at the new primary under a fresh session,
            // which would double-apply the possibly-committed op.
            return Err(WriteError::Rejected(if saw_not_committed {
                IggyError::TransientNotCommitted
            } else {
                transient
            }));
        }
        compio::time::sleep(TRANSIENT_RETRY_INTERVAL.min(remaining)).await;
        request = retry_request;
    };

    match reply.header().command {
        Command2::Reply => {
            *next_request_id += 1;
            drop(next_request_id);
            Ok((request_header, reply, raw_token))
        }
        Command2::Eviction => Err(WriteError::Evicted(eviction_error(&reply))),
        _ => Err(WriteError::Rejected(IggyError::InvalidCommand)),
    }
}

/// Run one authenticated control-plane write end to end and return the committed
/// reply's typed payload. Wraps [`submit_committed`] and decodes the reply body
/// via [`committed_payload`]: `create_stream` decodes the payload into an entity,
/// the update/delete/purge routes ignore it (it is empty) and answer 204.
pub(in crate::http) async fn submit_write(
    state: &HttpInner,
    session: &Rc<HttpSession>,
    operation: Operation,
    body: &[u8],
) -> Result<Bytes, WriteError> {
    let (_request_header, reply, _raw_token) =
        submit_committed(state, session, operation, body).await?;
    Ok(Bytes::copy_from_slice(committed_payload(&reply)?))
}

/// Tear down a caller's session for `DELETE /users/logout`: submit the VSR
/// `Logout` that releases its client-table slot on every replica (the commit a
/// TCP disconnect also submits), then forget the local session-table entry and
/// its reply target so neither leaks.
///
/// Best-effort: a transient submit failure is logged and the local entry is
/// dropped anyway, matching the disconnect path's contract that a logout never
/// blocks on peer slot release (the orphaned slot LRU-evicts). The bearer is not
/// revoked here - this listener's JWT half is issue+verify only, with no
/// revocation list - so the SDK dropping the token client-side is what ends the
/// credential; a caller that re-presents it just re-registers a fresh session.
pub(in crate::http) async fn logout_session(state: &HttpInner, session: &Rc<HttpSession>) {
    // Synthetic request id: the logout apply keys on (client, session) only and
    // is terminal for this session, so it needs no gate-issued contiguous id
    // (mirrors the disconnect path's `u64::MAX`).
    const LOGOUT_REQUEST_ID: u64 = u64::MAX;
    // Detached for the same reason as `submit_committed`: the submit drives
    // shared consensus machinery, and axum drops this handler future on
    // client disconnect. A cancel mid-await used to strand consensus state;
    // the detached task always drives the Logout to completion.
    let (result_slot, done) = oneshot::channel();
    let shard = Rc::clone(&state.shard);
    let vsr_client_id = session.client_id;
    let vsr_session = session.session;
    compio::runtime::spawn(async move {
        let result =
            submit_logout_on_owner(&shard, vsr_client_id, vsr_session, LOGOUT_REQUEST_ID).await;
        let _ = result_slot.send(result);
    })
    .detach();
    match done.await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => warn!(
            ?error,
            "server-ng HTTP: VSR Logout submit failed; slot lingers until eviction"
        ),
        Err(_canceled) => warn!(
            "server-ng HTTP: VSR Logout task dropped before replying; slot lingers until eviction"
        ),
    }
    state.forget_session(session);
}

/// Run one awaited partition write (produce / consumer-offset write) end to
/// end: admit against the in-flight caps, install the reply slot, dispatch
/// into the partition plane, and wait (bounded) for the committed reply.
///
/// Slot-before-dispatch is load-bearing: every pre-dispatch gate failure
/// inside [`dispatch_partition_request`] replies through `send_to_client`,
/// which fires an installed slot, so one slot catches every exit. The slot
/// guard borrows the registry, which is why this whole future runs inside
/// the caller's `SendWrapper` on shard 0.
pub(in crate::http) async fn partition_write_replicated(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
) -> Result<(), PartitionWriteError> {
    // Admission sits here rather than before body decode: axum's extractors
    // already buffered and deserialized the body (bounded by the router-wide
    // `DefaultBodyLimit`) before the handler ran, so the caps gate what is
    // actually unbounded - the slot install, the dispatch, and the parked
    // reply await that pins this request's buffers for up to the reply
    // timeout. Held across every exit below; released by `Drop`.
    let _in_flight = admit_partition_write(&session.in_flight_writes, &state.in_flight_writes)?;
    ensure_in_process_reply_target(state, session);
    let request_id = session.next_data_request_id();
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    let (guard, receiver) = state
        .shard
        .bus
        .clients()
        .install_reply_slot(session.client_id, request_id)
        .map_err(|error| {
            warn!(
                ?error,
                ?operation,
                "server-ng HTTP: partition write reply slot install failed"
            );
            PartitionWriteError::Unavailable
        })?;
    dispatch_partition_request(
        &state.shard,
        message,
        session.client_id,
        session.session,
        session.client_id,
        Some(session.user_id),
    )
    .await;
    let outcome = compio::time::timeout(PARTITION_WRITE_REPLY_TIMEOUT, receiver).await;
    // Removes the slot unless the reply already fired, so a late commit
    // reply after a timeout sheds at the bus instead of leaking a waiter.
    drop(guard);
    match outcome {
        Ok(Ok(reply)) => classify_partition_reply(&reply),
        // Cancelled (reply target torn down by session eviction mid-wait) or
        // elapsed: same caller contract either way - outcome unknown, 504.
        Ok(Err(_)) | Err(_) => Err(PartitionWriteError::Timeout(operation)),
    }
}

/// Fire-and-forget produce (`?ack=none`): installs no reply slot and never
/// awaits a commit. The commit still happens; its reply (and any gate-failure
/// reply) targets a request id with no slot installed and is shed at the bus by
/// design.
///
/// Admitted against the in-flight caps exactly like the acked path
/// ([`partition_write_replicated`]): being reply-less does not make it free.
/// The request still parks inside [`dispatch_partition_request`] for the
/// routable-wait budget while pinning its buffered body, so it must count
/// against the per-session and shard-global budgets or it would be an uncapped
/// bypass. The in-flight guard is held across dispatch and released by `Drop`.
/// A refusal is a synchronous 429/503, which is honest for `?ack=none`:
/// admission runs before dispatch, so it is not a commit reply the caller
/// opted out of - it is the request being turned away up front.
pub(in crate::http) async fn produce_unacked(
    state: &HttpInner,
    session: &HttpSession,
    body: &[u8],
) -> Result<(), PartitionWriteError> {
    let _in_flight = admit_partition_write(&session.in_flight_writes, &state.in_flight_writes)?;
    let request_id = session.next_data_request_id();
    let message = build_request_message(
        Operation::SendMessages,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    dispatch_partition_request(
        &state.shard,
        message,
        session.client_id,
        session.session,
        session.client_id,
        Some(session.user_id),
    )
    .await;
    Ok(())
}

/// Install this session's in-process reply target on first data-plane use.
///
/// The registry key is the session's shard-0 client id - the same id stamped
/// into `RequestHeader.client` - so a partition reply routed through
/// `send_to_client` lands on this entry and resolves the request-keyed slot.
/// `None` from the registry means the key is already occupied; treat it as
/// installed but leave the token unset so this session never tears down an
/// entry it does not own.
fn ensure_in_process_reply_target(state: &HttpInner, session: &HttpSession) {
    if session.registry_token.get().is_some() {
        return;
    }
    if let Some(token) = state
        .shard
        .bus
        .clients()
        .insert_in_process(session.client_id)
    {
        session.registry_token.set(Some(token));
    }
}
