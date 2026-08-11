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

//! Spec tests for clients-table durability across a node restart (IGGY-137).
//!
//! A client that keeps its `(client, session, request)` identity across a
//! node crash must be able to continue: a retry of an already-committed
//! request id must be answered from the dedup cache (never re-applied,
//! never silently dropped), and the next request id must be admitted.
//!
//! Server-side this rests on three landed pieces:
//!
//! 1. WAL-replay table recovery: `metadata::impls::recovery::recover`
//!    replays registers (minting the same epochs) and re-caches committed
//!    replies byte-identically, so a rebooted node remembers where each
//!    client left off. Sessions whose register fell below the snapshot
//!    floor are not recovered yet (no checkpoint artifact - IGGY-137
//!    remainder).
//! 2. Resume through the login path: a reconnecting client re-authenticates
//!    presenting its previous `client_id`. The rebind commits a Register --
//!    `submit_register_in_process` verifies the authenticated user owns the
//!    entry, then proposes -- so the entry's fence moves to the new register's
//!    commit op while keeping its watermark and reply ring. The client
//!    continues under the NEW epoch from the login reply; frames stamped with
//!    the pre-restart epoch are fenced zombies. There is deliberately no
//!    credential-free rebind; `given_live_session_when_unauthenticated_peer_*`
//!    pins that.
//! 3. A crash leaves no `Logout` behind. Every transport disconnect releases
//!    its session (`submit_disconnect_logout`), so what makes the entry
//!    survivable is that the process died with the connection still open --
//!    hence these tests restart before closing the socket. Holding the slot
//!    open past a graceful disconnect would make resume work there too, but
//!    it needs a timer of its own; see that function's rustdoc.
//!
//! The Rust SDK cannot drive this yet: it resets its `ConsensusSession` on
//! every disconnect and re-registers under a fresh identity (the
//! `sdk/vsr.rs` retry TODO). The frames are therefore hand-crafted on a raw
//! TCP socket, same technique as the protocol-version gate tests. SDK-side
//! identity stability (keep client id + request counter across reconnects,
//! retry replicated writes only under the same identity) is the remaining
//! client half.
//!
//! The topology matrix covers two distinct recovery paths:
//!
//! - **1 node**: the restarted node rebuilds the table from its own WAL and
//!   the client resumes against it (restart recovery).
//! - **3 nodes**: `restart_server` reboots node 0 only; the survivors elect
//!   a new primary, whose table was maintained by its own `commit_journal`
//!   applies all along. The client resumes against whichever node answers
//!   as primary — a follower cannot commit replicated TCP writes (no
//!   follower forwarding for TCP yet), so the continuation loop probes
//!   every node the way a leader-aware SDK re-routes (failover resume).
//!
//! These tests pin the resume contract: a client re-authenticates on the
//! fresh connection under its old `client_id` and the server binds it back to
//! the recovered entry rather than minting a new one. If the session-resume
//! work later settles on an explicit resume handshake, adjust `resume_request`
//! to speak it -- but it must stay credential-bearing.

#![cfg(feature = "vsr")]

use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::consensus::{
    Command2, Operation, ReplyHeader, RequestHeader, read_size_field, result_code,
    result_section_len,
};
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{ClientVersionInfo, HEADER_SIZE, IGGY_PROTOCOL_VERSION, WireName};
use integration::harness::TestHarness;
use integration::iggy_harness;
use secrecy::SecretString;
use std::mem::offset_of;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// Fixed wire identity so the post-restart frames are byte-identical to the
/// pre-restart ones; the SDK would randomize this on reconnect.
const CLIENT_ID: u128 = 0x1337_C0FFEE;

/// Budget for one committed round-trip (covers transient replays while the
/// single node elects itself after boot).
const COMMIT_BUDGET: Duration = Duration::from_secs(15);

/// Budget for the post-restart continuation attempts. Longer than
/// `COMMIT_BUDGET`: it also absorbs the listener coming back up.
const RESUME_BUDGET: Duration = Duration::from_secs(20);

/// Per-attempt reply wait. A server that silently drops the frame (the
/// `RequestGap` failure mode) answers nothing at all, so an unanswered read
/// is a verdict, not a reason to wait longer.
const REPLY_WAIT: Duration = Duration::from_secs(5);

const RETRY_PAUSE: Duration = Duration::from_millis(100);

#[iggy_harness(cluster_nodes = [1, 3])]
async fn given_committed_request_when_node_restarts_should_dedup_same_id_retry(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    let create_stream = create_stream_payload("iggy137-dedup");
    let committed = commit_request(&mut stream, session, 1, &create_stream).await;

    // Restart BEFORE dropping the socket, because the ordering is the scenario.
    // A crash takes the process down with the connection still open, so no
    // `Logout` is committed and the session is still in the WAL for replay to
    // rebuild. Closing first would model a graceful goodbye instead, and a
    // graceful disconnect ends the session by design
    // (`submit_disconnect_logout` releases the slot).
    //
    // Deterministic, not a race: the harness stops the node with SIGTERM, and
    // the per-connection cleanup in the bus installer skips `remove_client_meta`
    // once the bus token is triggered -- so a shutdown fires no
    // connection-lost callback for any still-open socket.
    harness.restart_server().await.unwrap();
    drop(stream);

    // The reply for request 1 was already delivered, but the client cannot
    // know that in the crash window; retrying the same id must converge on
    // the cached reply, never on a second apply or a silent drop. The
    // committed reply is passed in so the replay is proved byte-identical
    // rather than inferred from a duplicate-name rejection.
    let addrs = tcp_addrs(harness);
    resume_request(&addrs, session, 1, &create_stream, Some(&committed)).await;
}

#[iggy_harness(cluster_nodes = [1, 3])]
async fn given_bound_session_when_node_restarts_should_accept_next_request_id(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    commit_request(
        &mut stream,
        session,
        1,
        &create_stream_payload("iggy137-first"),
    )
    .await;

    // Crash ordering, see the sibling test.
    harness.restart_server().await.unwrap();
    drop(stream);

    // Continuation, not retry: the session advances to the next id. A node
    // that forgot the watermark sees request 2 on an unknown session and
    // either drops it as a gap or bounces the session entirely.
    let addrs = tcp_addrs(harness);
    // A continuation is a fresh op, so there is no cached reply to compare.
    resume_request(
        &addrs,
        session,
        2,
        &create_stream_payload("iggy137-second"),
        None,
    )
    .await;
}

/// Session resume must cost a credential.
///
/// An earlier revision rebound any unbound transport that merely presented a
/// matching `(client, session)`, treating the pair as a bearer token, and
/// logged the connection in as the entry's cached `user_id` -- a pre-auth
/// session takeover, trivially reachable because HTTP mints `client_id` from a
/// per-process counter seeded at 1.
///
/// This pins the contract: a transport that never authenticated gets the
/// unbound-transport fail-fast, never a binding, no matter what identity it
/// presents. Resume happens through the login path (see `resume_request`).
#[iggy_harness(cluster_nodes = 1)]
async fn given_live_session_when_unauthenticated_peer_presents_it_should_refuse_bind(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut owner, session) = register(addr).await;
    let payload = create_stream_payload("iggy137-auth-gap-owner");
    commit_request(&mut owner, session, 1, &payload).await;

    // Fresh connection, no login, presenting the live identity verbatim, and
    // asking for a write the owner never made -- so a bound impostor shows up
    // as an outright committed success rather than a name collision.
    let mut impostor = TcpStream::connect(addr).await.unwrap();
    let intruder_payload = create_stream_payload("iggy137-auth-gap-intruder");
    let header = request_header(Operation::CreateStream, session, 2, intruder_payload.len());
    let verdict = exchange(&mut impostor, &header, &intruder_payload)
        .await
        .verdict();
    assert!(
        matches!(
            verdict,
            Verdict::NoResultSection | Verdict::Evicted(_) | Verdict::Ignored
        ),
        "an unauthenticated peer presenting (client={CLIENT_ID:#x}, session={session}) \
         must not be bound; got {verdict:?}"
    );

    // Low request ids are equally refused: the guard is authentication, not
    // watermark position.
    let low = request_header(Operation::CreateStream, session, 1, payload.len());
    let verdict = exchange(&mut impostor, &low, &payload).await.verdict();
    assert!(
        !matches!(verdict, Verdict::Success(_)),
        "unauthenticated replay of a committed id must not succeed; got {verdict:?}"
    );

    // The owner's own session is untouched by the attempt.
    commit_request(
        &mut owner,
        session,
        2,
        &create_stream_payload("iggy137-auth-gap-live"),
    )
    .await;
}

pub(super) fn tcp_addr(harness: &TestHarness) -> SocketAddr {
    harness
        .server()
        .tcp_addr()
        .expect("server must expose a TCP address")
}

/// Every node's TCP address. The continuation loop probes all of them the
/// way a leader-aware SDK re-routes: after a node restart in a cluster the
/// primary may be any survivor, and only the primary commits (or answers
/// dedup for) replicated requests.
pub(super) fn tcp_addrs(harness: &TestHarness) -> Vec<SocketAddr> {
    (0..harness.cluster_size())
        .map(|node| {
            harness
                .node(node)
                .tcp_addr()
                .expect("every node must expose a TCP address")
        })
        .collect()
}

pub(super) fn create_stream_payload(name: &str) -> Bytes {
    CreateStreamRequest {
        name: WireName::new(name).unwrap(),
    }
    .to_bytes()
}

fn request_header(
    operation: Operation,
    session: u64,
    request: u64,
    body_len: usize,
) -> RequestHeader {
    RequestHeader {
        command: Command2::Request,
        operation,
        size: u32::try_from(HEADER_SIZE + body_len).unwrap(),
        client: CLIENT_ID,
        session,
        request,
        ..Default::default()
    }
}

/// Register `CLIENT_ID` as root and return the connection with its bound
/// session id. The session binds to THIS transport connection server-side,
/// so the pre-restart request must reuse the returned stream. Replays on
/// transient rejections: right after boot the single node may not have
/// elected itself yet.
pub(super) async fn register(addr: SocketAddr) -> (TcpStream, u64) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        if let Some(session) = login_on(&mut stream).await {
            return (stream, session);
        }
        assert!(
            Instant::now() < deadline,
            "register did not commit within {COMMIT_BUDGET:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Root login/register for `CLIENT_ID` on an already-connected socket.
/// `Some(session)` on a committed register, `None` on a transient rejection
/// (right after boot the node may not have elected itself yet).
async fn login_on(stream: &mut TcpStream) -> Option<u64> {
    let body = LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("iggy137-raw").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
    }
    .to_bytes();
    let header = request_header(Operation::Register, 0, 0, body.len());

    match exchange(stream, &header, &body).await.verdict() {
        Verdict::Success(reply) => {
            let response = LoginRegisterResponse::decode_from(&reply.payload)
                .expect("register payload must decode");
            assert_ne!(response.session, 0, "server must bind a nonzero session");
            Some(response.session)
        }
        Verdict::Rejected(code) if is_transient(code) => None,
        other => panic!("register did not commit: {other:?}"),
    }
}

/// Send one replicated metadata request on the registered connection and
/// require a committed success within `COMMIT_BUDGET`. Returns the committed
/// reply so a later replay can be compared against it byte for byte.
pub(super) async fn commit_request(
    stream: &mut TcpStream,
    session: u64,
    request: u64,
    body: &Bytes,
) -> CommittedReply {
    let header = request_header(Operation::CreateStream, session, request, body.len());
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        match exchange(stream, &header, body).await.verdict() {
            Verdict::Success(reply) => return reply,
            Verdict::Rejected(code) if is_transient(code) && Instant::now() < deadline => {
                sleep(RETRY_PAUSE).await;
            }
            other => panic!("request {request} did not commit: {other:?}"),
        }
    }
}

/// Post-restart continuation: re-authenticate under the OLD `client_id`, then
/// keep presenting the old identity, round-robin across every node, until one
/// commits (or serves the cached reply for) the request.
///
/// The re-login is the resume, and it is a rebind: the ownership-gated
/// Register commits, the recovered entry's fence moves to that register's op,
/// the login reply hands back the NEW epoch. Continuation frames must stamp
/// it -- the pre-restart epoch is a fenced zombie from here on. Watermark and
/// reply ring survive the rebind, which is what the dedup assertion rests on.
/// There is deliberately NO way to rebind without credentials -- an unbound
/// transport that merely presents `(client, session)` gets the empty-reply
/// fail-fast (see `given_unauthenticated_resume_*`).
///
/// Every attempt uses a fresh connection, both because the old one died with
/// the node and so an unanswered frame cannot desync the next attempt. Panics
/// with the last observed failure mode when the budget runs out.
pub(super) async fn resume_request(
    addrs: &[SocketAddr],
    session: u64,
    request: u64,
    body: &Bytes,
    expect_replay_of: Option<&CommittedReply>,
) {
    let deadline = Instant::now() + RESUME_BUDGET;
    let mut last_failure = "the listener never came back".to_string();
    let mut attempt = 0usize;
    while Instant::now() < deadline {
        let addr = addrs[attempt % addrs.len()];
        attempt += 1;
        let Ok(mut stream) = TcpStream::connect(addr).await else {
            sleep(RETRY_PAUSE).await;
            continue;
        };
        // Re-authenticate on the fresh connection. The rebind commits a
        // Register, so the epoch strictly advances past the pre-restart one
        // (op-derived; regression would mean the fence can be replayed into).
        let resumed = match login_on(&mut stream).await {
            Some(resumed) => {
                assert!(
                    resumed > session,
                    "rebind must move the fence above the pre-restart epoch \
                     (old {session}, got {resumed})"
                );
                resumed
            }
            None => {
                last_failure = "re-login did not commit".to_string();
                sleep(RETRY_PAUSE).await;
                continue;
            }
        };
        let header = request_header(Operation::CreateStream, resumed, request, body.len());
        match exchange(&mut stream, &header, body).await.verdict() {
            Verdict::Success(reply) => {
                if let Some(original) = expect_replay_of {
                    assert_replayed_from_cache(original, &reply, request);
                }
                return;
            }
            Verdict::Rejected(code) if is_transient(code) => {
                last_failure = format!("still transient (code {code})");
            }
            Verdict::Rejected(code) => {
                last_failure = format!(
                    "request {request} answered with committed code {code} (a \
                     duplicate-apply rejection means the dedup cache was lost)"
                );
            }
            Verdict::NoResultSection => {
                last_failure = format!(
                    "request {request} got the unbound-transport empty Reply on \
                     {addr}: that node does not recognize session {session}"
                );
            }
            Verdict::Ignored => {
                last_failure = format!(
                    "request {request} on session {session} was silently ignored for \
                     {REPLY_WAIT:?} (RequestGap-style drop: the node lost the \
                     request watermark)"
                );
            }
            Verdict::Evicted(reason) => {
                last_failure = format!(
                    "session {session} was evicted with reason {reason} instead of \
                     being rebound from the recovered table"
                );
            }
        }
        sleep(RETRY_PAUSE).await;
    }
    panic!(
        "session {session} did not survive the restart within {RESUME_BUDGET:?}: {last_failure}"
    );
}

/// Prove a retry was answered from the dedup cache rather than re-executed.
///
/// `build_reply_message` derives every reply field from the prepare header, and
/// recovery re-caches the committed reply from that same prepare, so a cached
/// replay is byte-identical to the reply the client originally received. A
/// re-apply cannot be: it commits at a fresh op, which changes `op`/`commit`
/// and therefore the frame `checksum`.
///
/// This is the direct proof. Without it the test rests on `CreateStream`
/// rejecting a duplicate name, which cannot distinguish "replayed the cached
/// reply" from "re-applied and rejected as a duplicate".
fn assert_replayed_from_cache(original: &CommittedReply, replayed: &CommittedReply, request: u64) {
    let field = |bytes: &[u8; HEADER_SIZE], offset: usize| {
        u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap())
    };
    let op_offset = offset_of!(ReplyHeader, op);
    let commit_offset = offset_of!(ReplyHeader, commit);
    assert_eq!(
        field(&replayed.header, op_offset),
        field(&original.header, op_offset),
        "request {request} was re-applied at a new op instead of replayed from cache"
    );
    assert_eq!(
        field(&replayed.header, commit_offset),
        field(&original.header, commit_offset),
        "request {request} replay carries a different commit than the original"
    );
    assert_eq!(
        replayed.header, original.header,
        "request {request} replay is not the cached bytes (headers differ)"
    );
    assert_eq!(
        replayed.payload, original.payload,
        "request {request} replay is not the cached bytes (payloads differ)"
    );
}

/// Everything one request/reply exchange can end in, spelled out so the
/// red-test panic names the exact failure mode instead of a decode error.
#[derive(Debug)]
enum Verdict {
    /// Committed success; carries the reply header plus the payload after the
    /// result section.
    Success(CommittedReply),
    /// Committed (or pre-consensus transient) rejection code.
    Rejected(u32),
    /// A Reply with no result section, i.e. the empty Reply the server emits
    /// for a replicated request on a transport it has no session for.
    NoResultSection,
    /// No frame within `REPLY_WAIT`.
    Ignored,
    /// Session-terminal Eviction frame; carries the wire reason byte.
    Evicted(u8),
}

/// A committed `Reply`, split into the wire header and the post-result-section
/// payload.
///
/// The header is boxed to keep it off the stack in `Verdict`/`Exchange`: at
/// `HEADER_SIZE` inline it dwarfs every other variant.
#[derive(Debug)]
pub(super) struct CommittedReply {
    header: Box<[u8; HEADER_SIZE]>,
    payload: Bytes,
}

enum Exchange {
    Reply {
        status: u32,
        /// Raw reply header, kept so a replay can be proved byte-identical to
        /// the original committed reply.
        header: Box<[u8; HEADER_SIZE]>,
        body: Bytes,
    },
    Eviction {
        reason: u8,
    },
    Ignored,
}

impl Exchange {
    fn verdict(self) -> Verdict {
        match self {
            Self::Ignored => Verdict::Ignored,
            Self::Eviction { reason } => Verdict::Evicted(reason),
            // A nonzero status is the pre-commit deny channel (authz etc.);
            // fold it into the rejection space, the codes are shared.
            Self::Reply { status, .. } if status != 0 => Verdict::Rejected(status),
            Self::Reply { header, body, .. } => match result_code(&body) {
                None => Verdict::NoResultSection,
                Some(0) => {
                    let payload_start = result_section_len(&body).unwrap();
                    Verdict::Success(CommittedReply {
                        header,
                        payload: body.slice(payload_start..),
                    })
                }
                Some(code) => Verdict::Rejected(code),
            },
        }
    }
}

/// Write one frame and read one frame off the lockstep connection.
async fn exchange(stream: &mut TcpStream, header: &RequestHeader, body: &Bytes) -> Exchange {
    stream.write_all(bytemuck::bytes_of(header)).await.unwrap();
    if !body.is_empty() {
        stream.write_all(body).await.unwrap();
    }

    let mut reply_header = [0u8; HEADER_SIZE];
    match timeout(REPLY_WAIT, stream.read_exact(&mut reply_header)).await {
        Err(_elapsed) => return Exchange::Ignored,
        Ok(read) => {
            read.expect("reply header read failed");
        }
    }

    let command_offset = offset_of!(RequestHeader, command);
    if reply_header[command_offset] == Command2::Eviction as u8 {
        return Exchange::Eviction {
            reason: reply_header[HEADER_SIZE - 1],
        };
    }
    assert_eq!(
        reply_header[command_offset],
        Command2::Reply as u8,
        "expected a Reply frame"
    );

    let status_offset = offset_of!(ReplyHeader, status);
    let status = u32::from_le_bytes(
        reply_header[status_offset..status_offset + 4]
            .try_into()
            .unwrap(),
    );

    let total_size = read_size_field(&reply_header).expect("reply size field") as usize;
    let mut body = vec![0u8; total_size - HEADER_SIZE];
    timeout(REPLY_WAIT, stream.read_exact(&mut body))
        .await
        .expect("reply body timed out")
        .expect("reply body read failed");
    Exchange::Reply {
        status,
        header: Box::new(reply_header),
        body: body.into(),
    }
}

fn is_transient(code: u32) -> bool {
    code == IggyError::TransientNotCommitted.as_code()
        || code == IggyError::TransientNotAccepted.as_code()
}
