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
use consensus::MetadataHandle;
use futures::channel::oneshot;
use iggy_binary_protocol::consensus::Command;
use iggy_binary_protocol::primitives::partition_history::PartitionContext;
use iggy_binary_protocol::{GenericHeader, Operation, ReplyHeader, RoutedRequestHeader};
use iggy_common::IggyError;
use metadata::impls::metadata::StreamsFrontend;
use server_common::{MESSAGE_ALIGN, Message, iobuf::Frozen};
use tracing::warn;

use crate::dispatch::partition::{dispatch_partition_request, resolve_delete_segments_truncate};
use crate::dispatch::session_ops::submit_logout_on_owner;
use crate::dispatch::submit::{committed_reply_commit, submit_client_request_on_owner};
use crate::http::admission::admit_partition_write;
use crate::http::error::{PartitionWriteError, WriteError};
use crate::http::reply::{classify_partition_reply, committed_payload, eviction_error};
use crate::http::session::HttpSession;
use crate::http::state::HttpInner;
use crate::http::wire::build_request_message;
use crate::reply_frame::transient_code;
use crate::rewrite::http_chain;
use crate::shell::ServerShard;
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
/// disconnects, and detaching keeps the whole gate-held critical section off
/// that cancellable future -- the submit always runs to completion and a
/// disconnect only drops the receiver half.
///
/// The collide-into-`Duplicate` hazard this originally guarded (a drop landing
/// between the consensus commit and the id advance, leaving a committed op's
/// reply cached under an id the session still thought unused) is now closed at
/// the source: [`submit_gated`] burns the id at stamp time, so no exit -- or
/// cancellation -- can leave it reusable. Detaching remains correct for the
/// other reason above.
pub(in crate::http) async fn submit_committed(
    state: &HttpInner,
    session: &Rc<HttpSession>,
    operation: Operation,
    body: Bytes,
) -> Result<(RoutedRequestHeader, Message<GenericHeader>, Option<String>), WriteError> {
    // Control writes are authorized in-apply on the metadata STM: a denial
    // comes back as `Unauthorized` in the committed result section, which
    // `committed_payload` maps to a 403. No pre-submit gate here, so the
    // in-apply check is the single source of truth. Self-scoped PAT ops (which
    // the in-apply gate skips) reach here as any authenticated user, matching
    // legacy parity.
    let (result_slot, committed) = oneshot::channel();
    let shard = Rc::clone(&state.shard);
    let task_session = Rc::clone(session);
    let watermarks = Rc::clone(&state.metadata_watermarks);
    let max_tokens_per_user = state.max_tokens_per_user;
    // Detached so a client disconnect cannot abandon the gate mid-submit;
    // the write runs to completion regardless of handler liveness.
    compio::runtime::spawn(async move {
        let result =
            submit_gated(&shard, &task_session, operation, max_tokens_per_user, &body).await;
        // Recorded here rather than after the await below, for the same reason
        // the submit is detached: a caller that disconnected mid-write still
        // committed the op, and its next request as this user must not be
        // served state older than what committed. Ordered before the wake, so a
        // read issued the instant the response lands already sees the mark.
        //
        // A follower with HTTP forwarding ON never runs this task: the
        // middleware relays the write and records the floor from the serving
        // primary's applied op instead (`http::forward::record_relayed_floor`).
        // One gap survives that split - a relayed 503 carrying
        // `TransientNotCommitted` is passed through untouched rather than
        // retried, because its op may still commit, and only a 2xx records a
        // floor. A write that did commit behind that code therefore leaves
        // none, until this caller's next committed write raises it.
        if let Ok((_, reply, _)) = &result
            && let Some(commit) = committed_reply_commit(reply)
        {
            watermarks.record(task_session.user_id, commit);
        }
        // A failed send means the handler died mid-await; the submit itself
        // already completed, which is the invariant that matters.
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
///
/// The id is BURNED as soon as it is stamped into a request: it advances once,
/// up front, and no exit path can hand it to a later operation. A transient
/// frame is still replayed in place under that same id (see
/// [`TRANSIENT_RETRY_INTERVAL`]) -- replaying the identical request is what
/// dedup exists for -- but once this call returns, the id is spent.
///
/// It used to advance only on a genuine committed `Reply`, leaving the id free
/// after an unanswered submit, a `TransientNotCommitted` frame, or an
/// eviction. Under the old depth-1 contiguity rule that reuse was mandatory:
/// the next accepted request had to be `committed + 1`, so a
/// consumed-but-uncommitted id wedged the session on `RequestGap`. The client
/// table now dedups on a watermark instead -- `request > watermark` is New, so
/// gaps are free and `RequestGap` no longer exists -- which inverts the
/// requirement. Reusing the id became a wrong-answer hazard: the frame that
/// released it may still have committed (a view-change-canceled prepare can
/// reach quorum and be inherited by the new primary, which is exactly why the
/// budget-exhausted arm reports `TransientNotCommitted`), and then the reused
/// id sits at or below the watermark, so the caller's NEXT and DIFFERENT
/// operation is answered from the dedup cache with the previous operation's
/// reply -- under a 2xx, since the create endpoints return 204 and never
/// inspect the payload.
///
/// An eviction still means the session is dead: mapped to 401, and
/// [`submit_committed`] then forgets the entry so the caller's retry
/// re-registers cleanly.
///
/// Both shard-0 request rewrites run here before consensus, mirroring the TCP
/// dispatch path: the PAT rewrite enforces the per-user token cap, then mints
/// a raw token and replicates only its hash
/// (`CreatePersonalAccessToken`), and the user-password rewrite hashes the new
/// password and, for `ChangePassword`, strips the current one and verifies it
/// against the stored hash (`CreateUser` / `ChangePassword`). Both are no-ops
/// for every other operation, so plaintext secrets never enter consensus on any
/// write. A third resolution handles `DeleteSegments`, which is not itself a
/// consensus op: it is rewritten to the metadata `TruncatePartition` that commits
/// the trim (see [`resolve_delete_segments_truncate`]), so the truncate rides
/// this session's gate id. A rejection (a malformed body, a caller at the
/// personal-access-token cap, or an unresolved delete-segments namespace) still
/// spends the id like any other exit. A failed current-password check is not a
/// rejection here: the op still commits with an emptied new-password sentinel
/// that the replicated apply grades to `InvalidCredentials` (see
/// `verify_and_rewrite_change_password`).
async fn submit_gated(
    shard: &Rc<ServerShard>,
    session: &HttpSession,
    operation: Operation,
    max_tokens_per_user: u32,
    body: &[u8],
) -> Result<(RoutedRequestHeader, Message<GenericHeader>, Option<String>), WriteError> {
    let mut next_request_id = session.gate.lock().await;
    // Burn the id at stamp time: every exit below (rewrite rejection,
    // unresolved delete-segments, unanswered submit, exhausted transient
    // budget, eviction) then leaves it spent rather than handing it to a
    // later, different operation. See the doc block for why reuse was
    // mandatory under contiguity and is a wrong-answer hazard under the
    // watermark.
    let request_id = *next_request_id;
    *next_request_id += 1;
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    let (message, raw_token) = http_chain(shard, session.user_id, max_tokens_per_user, message)
        .map_err(WriteError::Rejected)?;
    // `DeleteSegments` is not itself a consensus op: resolve it to the metadata
    // `TruncatePartition` that commits the trim before it reaches consensus,
    // mirroring the TCP dispatch. The truncate rides this session's burned
    // gate id; an unresolved namespace releases the gate with the id already
    // spent, like a rejected rewrite.
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
        // Retry the same rewritten body: a newly minted PAT token could differ
        // from the token hash protected by the first receipt.
        let retry_request = request.clone();
        let Some(reply) = submit_client_request_on_owner(shard, request).await else {
            return Err(WriteError::Unavailable);
        };
        let transient = (reply.header().command == Command::Reply)
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
        Command::Reply => {
            // Already burned at stamp time; release the gate so the next
            // write on this session can take its turn.
            drop(next_request_id);
            Ok((request_header, reply, raw_token))
        }
        Command::Eviction => Err(WriteError::Evicted(eviction_error(&reply))),
        _ => Err(WriteError::Rejected(IggyError::InvalidCommand)),
    }
}

/// Run one authenticated control-plane write end to end and return the committed
/// reply's typed payload. Wraps [`submit_committed`] and decodes the reply body
/// via [`committed_payload`]: `create_stream` decodes the payload into an entity,
/// the update/delete routes ignore it (it is empty) and answer 204.
pub(in crate::http) async fn submit_write(
    state: &HttpInner,
    session: &Rc<HttpSession>,
    operation: Operation,
    body: Bytes,
) -> Result<Bytes, WriteError> {
    let (_request_header, reply, _raw_token) =
        submit_committed(state, session, operation, body).await?;
    Ok(Bytes::copy_from_slice(committed_payload(&reply)?))
}

/// Tear down a caller's session for `DELETE /users/logout`: submit the VSR
/// `Logout` that ends its logical session on every replica, then forget the
/// local entry and reply target. Partition retirement releases the shared slot.
///
/// Best-effort: a transient submit failure is logged and the local entry is
/// dropped anyway. An orphaned slot remains until lease expiry and retirement.
/// The bearer is not
/// revoked here - this listener's JWT half is issue+verify only, with no
/// revocation list - so the SDK dropping the token client-side is what ends the
/// credential; a caller that re-presents it just re-registers a fresh session.
pub(in crate::http) async fn logout_session(state: &HttpInner, session: &Rc<HttpSession>) {
    // Synthetic request id: the logout apply keys on (client, session) only and
    // is terminal for this session, so it needs no gate-issued id at all
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
            "server HTTP: VSR Logout submit failed; session remains until lease expiry"
        ),
        Err(_canceled) => warn!(
            "server HTTP: VSR Logout task dropped before replying; session remains until lease expiry"
        ),
    }
    state.forget_session(session);
}

pub(in crate::http) async fn partition_write_replicated(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
    context: Option<PartitionContext>,
) -> Result<(Frozen<MESSAGE_ALIGN>, ReplyHeader), PartitionWriteError> {
    partition_write(state, session, operation, body, context, &mut None).await
}

async fn partition_write(
    state: &HttpInner,
    session: &HttpSession,
    operation: Operation,
    body: &[u8],
    context: Option<PartitionContext>,
    dispatched: &mut Option<oneshot::Sender<Result<(), PartitionWriteError>>>,
) -> Result<(Frozen<MESSAGE_ALIGN>, ReplyHeader), PartitionWriteError> {
    let deadline = Instant::now() + PARTITION_WRITE_REPLY_TIMEOUT;
    let namespace = crate::namespace::resolve_partition_request_namespace(
        &state.shard,
        operation,
        body,
        session.client_id,
    )
    .map_err(PartitionWriteError::Rejected)?;
    let attachment = session.attachment.borrow().clone();
    let captured = crate::dispatch::partition::capture_offset_attachment(
        state.shard.plane.metadata().mux_stm.streams(),
        server_common::sharding::IggyNamespace::from_raw(namespace),
        body,
        session.client_id,
        attachment,
        operation,
    )
    .map_err(PartitionWriteError::Rejected)?;
    let context = context.unwrap_or_else(|| {
        captured
            .metadata
            .context(state.shard.plane.metadata().applied_frontier().get())
    });
    let gate = session.partition_gate(namespace);
    // The group retains one receipt per session. Keep its lane occupied through
    // the reply wait, including when NoAck has already returned after dispatch.
    let _partition_guard = compio::time::timeout(
        deadline.saturating_duration_since(Instant::now()),
        gate.lock(),
    )
    .await
    .map_err(|_| PartitionWriteError::Rejected(IggyError::TransientNotAccepted))?;
    let _in_flight = admit_partition_write(&session.in_flight_writes, &state.in_flight_writes)?;
    ensure_in_process_reply_target(state, session);
    let request_id = {
        let mut next_id = session.data_gate.lock().await;
        let request_id = *next_id;
        *next_id = next_id
            .checked_add(1)
            .ok_or(PartitionWriteError::Unavailable)?;
        request_id
    };
    let message = build_request_message(
        operation,
        session.client_id,
        session.session,
        request_id,
        body,
    );
    let message = message.transmute_header(|header, routed: &mut RoutedRequestHeader| {
        *routed = header;
        routed.partition_incarnation = context.incarnation;
        routed.owner_generation = context.owner_generation;
        routed.minimum_metadata_op = context.metadata_op;
    });
    let (guard, receiver) = state
        .shard
        .bus
        .clients()
        .install_reply_slot(session.client_id, request_id)
        .map_err(|error| {
            warn!(
                ?error,
                ?operation,
                "server HTTP: partition write reply slot install failed"
            );
            PartitionWriteError::Unavailable
        })?;
    let attachment = session.attachment.borrow().clone();
    let outcome =
        compio::time::timeout(deadline.saturating_duration_since(Instant::now()), async {
            dispatch_partition_request(
                &state.shard,
                message,
                session.client_id,
                session.session,
                session.client_id,
                Some(session.user_id),
                Some((session.client_id, attachment)),
                Some(namespace),
            )
            .await;
            if let Some(dispatched) = dispatched.take() {
                let _ = dispatched.send(Ok(()));
            }
            receiver.await
        })
        .await;
    drop(guard);
    match outcome {
        Ok(Ok(reply)) => {
            let reply = reply.into_contiguous();
            classify_partition_reply(&reply).map(|header| (reply, header))
        }
        Ok(Err(_)) | Err(_) => Err(PartitionWriteError::Timeout(operation)),
    }
}

/// Return after dispatch while a detached task holds the partition gate until
/// the reply or deadline. A 202 still carries no commit or admission guarantee.
pub(in crate::http) async fn produce_unacked(
    state: &Rc<HttpInner>,
    session: &Rc<HttpSession>,
    body: Bytes,
    context: Option<PartitionContext>,
) -> Result<(), PartitionWriteError> {
    let state = Rc::clone(state);
    let session = Rc::clone(session);
    let (sent, dispatched) = oneshot::channel();
    compio::runtime::spawn(async move {
        let mut sent = Some(sent);
        let result = partition_write(
            &state,
            &session,
            Operation::SendMessages,
            &body,
            context,
            &mut sent,
        )
        .await;
        if let Some(sent) = sent {
            let _ = sent.send(result.map(|_| ()));
        } else if let Err(error) = result {
            tracing::debug!(
                ?error,
                "server HTTP: unacked produce resolved without a committed reply"
            );
        }
    })
    .detach();
    dispatched
        .await
        .map_err(|_| PartitionWriteError::Unavailable)?
}

/// Install this session's in-process reply target on first data-plane use.
///
/// The registry key is the session's shard-0 client id - the same id stamped
/// into `RoutedRequestHeader.client` - so a partition reply routed through
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
