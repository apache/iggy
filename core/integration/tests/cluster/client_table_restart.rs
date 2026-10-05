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
//! Metadata recovery restores the mandatory session-registry snapshot before
//! replaying the retained WAL. Matching registration preserves the original
//! epoch, and proof-bearing Bind authenticates a replacement connection.
//! Disconnect does not commit Logout. Authenticated activity renews the lease;
//! logout or expiry fences every attachment before ordered receipt retirement.
//!
//! Raw TCP frames retain explicit request identities and allow the fixture to
//! discard a reply before reconnecting. Rust SDK connections also retain their
//! logical identity and proof, but never mint another mutation after an
//! uncertain result.
//!
//! The topology matrix covers two distinct recovery paths:
//!
//! - **1 node**: the restarted node rebuilds the table from its own WAL and
//!   the client resumes against it (restart recovery).
//! - **3 nodes**: `restart_server` reboots node 0 only; the survivors elect
//!   a new primary, whose table was maintained by its own `commit_journal`
//!   applies all along. The client resumes against whichever node answers
//!   as primary. The continuation loop probes
//!   every node the way a leader-aware SDK re-routes (failover resume).
//!
//! These tests pin the resume contract: a client re-authenticates on the
//! fresh connection under its old `client_id` and the server binds it back to
//! the recovered entry rather than minting a new one. If the session-resume
//! work later settles on an explicit resume handshake, adjust `resume_request`
//! to speak it -- but it must stay credential-bearing.

use crate::server::raw_tcp::TEST_BIND_SECRET;
use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::codes::{GET_POLL_ROUTING_CODE, PING_CODE, POLL_MESSAGES_CODE};
use iggy_binary_protocol::consensus::{
    Command, Operation, ReplyHeader, RequestHeader, read_size_field, result_code,
    result_section_len,
};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_groups::JoinConsumerGroupRequest;
use iggy_binary_protocol::requests::messages::PollMessagesRequest;
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::system::{BindSessionRequest, SessionIdentity};
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::requests::users::login_register::BindSecret;
use iggy_binary_protocol::responses::messages::{PollMessagesResponse, PollRoutingResponse};
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, HEADER_SIZE, IGGY_PROTOCOL_VERSION, WireIdentifier, WireName, WireOptions,
};
use integration::harness::TestHarness;
use integration::iggy_harness;
use secrecy::SecretString;
use std::mem::offset_of;
use std::net::SocketAddr;
use std::str::FromStr;
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
async fn given_lost_login_reply_when_binding_should_resolve_the_original_epoch(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let mut original = TcpStream::connect(addr).await.unwrap();
    let login = login_body();
    let header = request_header(Operation::Register, 0, 0, login.len());
    original
        .write_all(bytemuck::bytes_of(&header))
        .await
        .unwrap();
    original.write_all(&login).await.unwrap();

    let mut request = BindSessionRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("iggy137-raw").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        identity: SessionIdentity {
            client_id: CLIENT_ID,
            session: 0,
            metadata_watermark: 0,
        },
        bind_secret: BindSecret::new(Box::new(TEST_BIND_SECRET)),
    };
    let body = request.to_bytes();
    let header = RequestHeader::for_request(
        iggy_binary_protocol::codes::BIND_SESSION_CODE,
        CLIENT_ID,
        0,
        0,
        &body,
    )
    .unwrap();
    let mut bound = TcpStream::connect(addr).await.unwrap();
    let deadline = Instant::now() + COMMIT_BUDGET;
    let session = loop {
        match exchange(&mut bound, &header, &body).await {
            Exchange::Reply {
                status: 0, body, ..
            } => {
                break LoginRegisterResponse::decode_from(&body).unwrap().session;
            }
            Exchange::Reply { status, .. } if is_transient(status) => {}
            other => panic!("uncertain login binding failed: {other:?}"),
        }
        assert!(
            Instant::now() < deadline,
            "binding never resolved registration"
        );
        sleep(RETRY_PAUSE).await;
    };
    assert_ne!(session, 0);
    drop(original);
    let (mut retried, retried_session) = register(addr).await;
    assert_eq!(
        retried_session, session,
        "login retry must retain the original epoch"
    );

    request.identity.session = session;
    request.identity.metadata_watermark = session;
    request.bind_secret = BindSecret::new(Box::new([0xbb; 32]));
    let body = request.to_bytes();
    let header = RequestHeader::for_request(
        iggy_binary_protocol::codes::BIND_SESSION_CODE,
        CLIENT_ID,
        0,
        session,
        &body,
    )
    .unwrap();
    let mut impostor = TcpStream::connect(addr).await.unwrap();
    assert!(matches!(exchange(&mut impostor, &header, &body).await,
        Exchange::Reply { status, .. } if status == IggyError::Unauthenticated.as_code()));

    let payload = create_stream_payload("lost-login-shared-binding");
    let original_result = commit_request(&mut bound, session, 1, &payload).await;
    let replay = commit_request(&mut retried, session, 1, &payload).await;
    assert_eq!(replay.header, original_result.header);
    assert_eq!(replay.payload, original_result.payload);
}

#[iggy_harness(cluster_nodes = [1, 3])]
async fn given_committed_request_when_node_restarts_should_dedup_same_id_retry(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr).await;
    let create_stream = create_stream_payload("iggy137-dedup");
    let committed = commit_request(&mut stream, session, 1, &create_stream).await;

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
        options: WireOptions::empty(),
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
        command: Command::Request,
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
    register_client(addr, CLIENT_ID).await
}

async fn register_client(addr: SocketAddr, client_id: u128) -> (TcpStream, u64) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        if let Some(session) = login_on(&mut stream, client_id).await {
            return (stream, session);
        }
        assert!(
            Instant::now() < deadline,
            "register {client_id:#x} did not commit within {COMMIT_BUDGET:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Root login/register on an already-connected socket.
/// `Some(session)` on a committed register, `None` on a transient rejection
/// (right after boot the node may not have elected itself yet).
async fn login_on(stream: &mut TcpStream, client_id: u128) -> Option<u64> {
    let body = login_body();
    let mut header = request_header(Operation::Register, 0, 0, body.len());
    header.client = client_id;

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

fn login_body() -> Bytes {
    LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("iggy137-raw").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
        bind_secret: BindSecret::new(Box::new(TEST_BIND_SECRET)),
    }
    .to_bytes()
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
    commit_request_header(stream, &header, body).await
}

async fn commit_request_header(
    stream: &mut TcpStream,
    header: &RequestHeader,
    body: &Bytes,
) -> CommittedReply {
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        match exchange(stream, header, body).await.verdict() {
            Verdict::Success(reply) => return reply,
            Verdict::Rejected(code) if is_transient(code) && Instant::now() < deadline => {
                sleep(RETRY_PAUSE).await;
            }
            other => panic!("request {} did not commit: {other:?}", header.request),
        }
    }
}

/// Post-restart continuation: re-authenticate under the OLD `client_id`, then
/// keep presenting the old identity, round-robin across every node, until one
/// commits (or serves the cached reply for) the request.
///
/// A matching Register authenticates the replacement connection while
/// preserving the recovered epoch, watermark, and reply ring. Continuation
/// frames keep that epoch so retries still find the original receipts.
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
    let mut authenticated_attempts = Vec::new();
    while Instant::now() < deadline {
        let addr = addrs[attempt % addrs.len()];
        attempt += 1;
        let Ok(mut stream) = TcpStream::connect(addr).await else {
            sleep(RETRY_PAUSE).await;
            continue;
        };
        // Keep the epoch so continuation retries use the recovered receipt identity.
        let resumed = match login_on(&mut stream, CLIENT_ID).await {
            Some(resumed) => {
                assert!(
                    resumed == session,
                    "rebind must preserve the recovered session \
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
        // Backups can forward Register even though they cannot serve the
        // following metadata write. Keep each authenticated socket alive so
        // its disconnect cleanup cannot race the next rebind with a Logout.
        authenticated_attempts.push(stream);
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

#[derive(Debug)]
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
    if reply_header[command_offset] == Command::Eviction as u8 {
        return Exchange::Eviction {
            reason: reply_header[HEADER_SIZE - 1],
        };
    }
    assert_eq!(
        reply_header[command_offset],
        Command::Reply as u8,
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

const LIVENESS_STREAM: &str = "session-liveness";
const LIVENESS_TOPIC: &str = "events";
const LIVENESS_GROUP: &str = "readers";
const LIVENESS_OBSERVATION: Duration = Duration::from_secs(12);

#[iggy_harness(server(
    metadata.clients_table_max = 4,
    heartbeat.enabled = false,
    consumer_group.heartbeat_interval = "500ms",
    consumer_group.session_timeout = "8s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_full_registry_when_new_clients_arrive_should_preserve_live_members_until_expiry(
    harness: &mut TestHarness,
) {
    const CAPACITY_PRESSURE: u32 = 12;
    let observer = harness.root_client_for_node(0).await.unwrap();
    create_liveness_group(&observer).await;
    let stream = Identifier::named(LIVENESS_STREAM).unwrap();
    let topic = Identifier::named(LIVENESS_TOPIC).unwrap();
    let group = Identifier::named(LIVENESS_GROUP).unwrap();
    let consumer = Consumer::group(group.clone());
    let mut messages = [
        IggyMessage::from_str("committed").unwrap(),
        IggyMessage::from_str("pending").unwrap(),
    ];
    observer
        .send_messages(
            &stream,
            &topic,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();
    let addr = harness.node(0).tcp_addr().unwrap();
    let (mut member, session) = register(addr).await;
    let join = JoinConsumerGroupRequest {
        stream_id: WireIdentifier::named(LIVENESS_STREAM).unwrap(),
        topic_id: WireIdentifier::named(LIVENESS_TOPIC).unwrap(),
        group_id: WireIdentifier::named(LIVENESS_GROUP).unwrap(),
    }
    .to_bytes();
    let header = request_header(Operation::JoinConsumerGroup, session, 1, join.len());
    assert!(matches!(
        exchange(&mut member, &header, &join).await.verdict(),
        Verdict::Success(_)
    ));
    let original = integration::harness::wait_for_consumer_group_assignment(
        &observer,
        &stream,
        &topic,
        &group,
        1,
        COMMIT_BUDGET,
    )
    .await;
    let mut poll_request = PollMessagesRequest {
        consumer: WireConsumer::consumer_group(WireIdentifier::named(LIVENESS_GROUP).unwrap()),
        stream_id: WireIdentifier::named(LIVENESS_STREAM).unwrap(),
        topic_id: WireIdentifier::named(LIVENESS_TOPIC).unwrap(),
        partition_id: Some(0),
        strategy: WirePollingStrategy::first(),
        count: 1,
        auto_commit: true,
    };
    let first_poll = poll_request.to_bytes();
    let mut routing_header = request_header(Operation::NonReplicated, session, 0, first_poll.len());
    routing_header.reserved[..size_of::<u32>()]
        .copy_from_slice(&GET_POLL_ROUTING_CODE.to_le_bytes());
    let Exchange::Reply {
        status: 0, body, ..
    } = exchange(&mut member, &routing_header, &first_poll).await
    else {
        panic!("the installed owner must discover its poll context");
    };
    let context = PollRoutingResponse::decode_from(&body).unwrap().context;
    let mut first_header = request_header(Operation::NonReplicated, session, 0, first_poll.len());
    first_header.reserved[..size_of::<u32>()].copy_from_slice(&POLL_MESSAGES_CODE.to_le_bytes());
    context.stamp(&mut first_header);
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        let Exchange::Reply {
            status: 0, body, ..
        } = exchange(&mut member, &first_header, &first_poll).await
        else {
            panic!("original member could not poll its assigned partition");
        };
        let mut response = PollMessagesResponse::decode(&body).unwrap();
        if let Some(message) = response.messages.next() {
            let message = message.unwrap();
            assert_eq!(message.offset, 0);
            assert_eq!(message.payload, messages[0].payload.as_ref());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "seeded messages were not readable"
        );
        sleep(RETRY_PAUSE).await;
    }
    assert_eq!(
        observer
            .get_consumer_offset(&consumer, &stream, &topic, Some(0))
            .await
            .unwrap()
            .unwrap()
            .stored_offset,
        0
    );
    let mut connections = Vec::new();
    let mut refusals = 0;
    for index in 1..=CAPACITY_PRESSURE {
        let mut connection = TcpStream::connect(addr).await.unwrap();
        if login_on(&mut connection, CLIENT_ID + u128::from(index))
            .await
            .is_some()
        {
            connections.push(connection);
        } else {
            refusals += 1;
        }
    }
    assert!(
        !connections.is_empty(),
        "pressure must occupy the remaining slots"
    );
    assert!(
        refusals > 0,
        "a full registry must refuse further identities"
    );
    poll_request.auto_commit = false;
    let poll = poll_request.to_bytes();
    let mut header = request_header(Operation::NonReplicated, session, 0, poll.len());
    header.reserved[..size_of::<u32>()].copy_from_slice(&POLL_MESSAGES_CODE.to_le_bytes());
    context.stamp(&mut header);
    let deadline = Instant::now() + LIVENESS_OBSERVATION;
    while Instant::now() < deadline {
        assert!(
            matches!(
                exchange(&mut member, &header, &poll).await,
                Exchange::Reply { status: 0, .. }
            ),
            "capacity pressure must preserve the active member's assignment"
        );
        assert_eq!(liveness_group(&observer).await.members_count, 1);
        sleep(RETRY_PAUSE).await;
    }
    let replacement = harness.root_client_for_node(0).await.unwrap();
    replacement
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    let waiting = liveness_group(&observer).await;
    assert_eq!(waiting.members_count, 2);
    assert_eq!(
        waiting
            .members
            .iter()
            .find(|member| member.id == original.members[0].id)
            .unwrap()
            .partitions,
        vec![0]
    );
    assert!(
        replacement
            .poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false
            )
            .await
            .unwrap()
            .messages
            .is_empty(),
        "replacement must wait while the original owns the only partition"
    );
    drop(member);
    let deadline = Instant::now() + RESUME_BUDGET;
    loop {
        let recovered = replacement
            .poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
        if let Some(message) = recovered.messages.first() {
            assert_eq!(message.header.offset, 1);
            assert_eq!(message.payload, messages[1].payload);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replacement never resumed after the inactive member expired"
        );
        sleep(RETRY_PAUSE).await;
    }
    let recovered = liveness_group(&observer).await;
    assert_eq!(recovered.id, original.id, "expiry must preserve the group");
    assert_eq!(recovered.members_count, 1);
    assert_ne!(recovered.members[0].id, original.members[0].id);
    assert_eq!(recovered.members[0].partitions, vec![0]);
    drop(connections);
}

#[iggy_harness(server(
    metadata.clients_table_max = "18",
    consumer_group.heartbeat_interval = "5s",
    consumer_group.session_timeout = "30s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_ended_sessions_when_registry_is_full_should_retire_more_than_one_per_heartbeat(
    harness: &mut TestHarness,
) {
    const CLIENTS: u128 = 16;
    const DRAIN_BUDGET: Duration = Duration::from_secs(4);
    const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
    const REGISTRATION_REPORT_WAIT: Duration =
        HEARTBEAT_INTERVAL.saturating_add(Duration::from_secs(1));
    let address = harness.node(0).tcp_addr().unwrap();
    for client_id in CLIENT_ID..CLIENT_ID + CLIENTS {
        let (mut stream, session) = register_client(address, client_id).await;
        let mut header = request_header(Operation::Logout, session, 1, 0);
        header.client = client_id;
        let logout = exchange(&mut stream, &header, &Bytes::new()).await;
        assert!(
            matches!(logout, Exchange::Reply { status: 0, .. }),
            "Logout must commit before its registry slot can retire: {logout:?}"
        );
    }
    sleep(REGISTRATION_REPORT_WAIT).await;
    timeout(DRAIN_BUDGET, async {
        for client_id in CLIENT_ID + CLIENTS..CLIENT_ID + CLIENTS * 2 {
            let _ = register_client(address, client_id).await;
        }
    })
    .await
    .expect("ended sessions kept registry slots beyond the first retirement sweep");
}

#[iggy_harness(server(
    heartbeat.enabled = false,
    consumer_group.heartbeat_interval = "5s",
    consumer_group.session_timeout = "22s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_many_recovered_members_when_expiring_should_drain_without_another_interval(
    harness: &mut TestHarness,
) {
    // Exceeds the per-pass logout cap, so recovery needs multiple passes.
    const MEMBER_COUNT: u32 = 300;
    // Below the 5s heartbeat interval. Every logout is a persisted, replicated
    // commit, so a slow disk stretches the drain. Only a pass that waits for
    // the next interval leaves the member count flat this long.
    const STALL_BUDGET: Duration = Duration::from_secs(4);
    const FIRST_EXPIRY_BUDGET: Duration = Duration::from_secs(45);
    let observer = harness.root_client_for_node(0).await.unwrap();
    create_liveness_group(&observer).await;
    let body = JoinConsumerGroupRequest {
        stream_id: WireIdentifier::named(LIVENESS_STREAM).unwrap(),
        topic_id: WireIdentifier::named(LIVENESS_TOPIC).unwrap(),
        group_id: WireIdentifier::named(LIVENESS_GROUP).unwrap(),
    }
    .to_bytes();
    let mut connections = Vec::with_capacity(MEMBER_COUNT as usize);
    for member in 0..MEMBER_COUNT {
        let client_id = CLIENT_ID + u128::from(member);
        let (mut connection, session) =
            register_client(harness.node(0).tcp_addr().unwrap(), client_id).await;
        let mut header = request_header(Operation::JoinConsumerGroup, session, 1, body.len());
        header.client = client_id;
        commit_request_header(&mut connection, &header, &body).await;
        connections.push(connection);
    }
    assert_eq!(liveness_group(&observer).await.members_count, MEMBER_COUNT);
    harness.kill_node(0).unwrap();
    drop(connections);
    harness.restart_node(0).unwrap();
    let observer = harness.root_client_for_node(0).await.unwrap();
    let deadline = Instant::now() + FIRST_EXPIRY_BUDGET;
    while liveness_group(&observer).await.members_count == MEMBER_COUNT {
        assert!(
            Instant::now() < deadline,
            "recovered members never began expiring"
        );
        sleep(RETRY_PAUSE).await;
    }
    let mut remaining = MEMBER_COUNT;
    let mut last_drop = Instant::now();
    loop {
        let current = liveness_group(&observer).await.members_count;
        if current == 0 {
            break;
        }
        if current < remaining {
            remaining = current;
            last_drop = Instant::now();
        }
        assert!(
            last_drop.elapsed() < STALL_BUDGET,
            "cleanup stalled for {STALL_BUDGET:?} with {remaining} members remaining"
        );
        sleep(RETRY_PAUSE).await;
    }
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    consumer_group.heartbeat_interval = "500ms",
    consumer_group.session_timeout = "8s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_backup_member_when_primary_restarts_should_preserve_membership(
    harness: &mut TestHarness,
) {
    let observer = harness.root_client_for_node(0).await.unwrap();
    let (mut connection, session, primary, backup) =
        bind_group_member_on_backup(harness, &observer).await;
    let before = liveness_group(&observer).await;
    keep_session_active(&mut connection, session, LIVENESS_OBSERVATION).await;
    let renewed = liveness_group(&observer).await;
    assert_eq!(renewed.members_count, 1);
    assert_eq!(renewed.members[0].id, before.members[0].id);

    harness.kill_node(primary).unwrap();
    harness.restart_node(primary).unwrap();
    keep_session_active(&mut connection, session, LIVENESS_OBSERVATION).await;
    let observer = harness.root_client_for_node(backup).await.unwrap();
    let after = liveness_group(&observer).await;
    assert_eq!(
        after.members_count, 1,
        "the new primary must honor a live backup's heartbeats"
    );
    assert_eq!(after.members[0].id, before.members[0].id);
    assert_eq!(after.members[0].partitions, before.members[0].partitions);
    drop(connection);
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    consumer_group.heartbeat_interval = "500ms",
    consumer_group.session_timeout = "8s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_backup_member_when_host_crashes_should_expire_membership(harness: &mut TestHarness) {
    let observer = harness.root_client_for_node(0).await.unwrap();
    let (connection, _, _, backup) = bind_group_member_on_backup(harness, &observer).await;
    assert_eq!(liveness_group(&observer).await.members_count, 1);
    harness.kill_node(backup).unwrap();
    let deadline = Instant::now() + RESUME_BUDGET;
    while liveness_group(&observer).await.members_count != 0 {
        assert!(
            Instant::now() < deadline,
            "dead backup member was not evicted without restarting its host"
        );
        sleep(RETRY_PAUSE).await;
    }
    drop(connection);
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    consumer_group.heartbeat_interval = "500ms",
    consumer_group.session_timeout = "8s",
    sharding.cpu_allocation = "0..1",
))]
async fn given_primary_member_when_host_crashes_should_expire_membership(
    harness: &mut TestHarness,
) {
    let observer = harness.root_client_for_node(0).await.unwrap();
    create_liveness_group(&observer).await;
    let primary = primary_index(harness, &observer).await;
    drop(observer);
    let stream = Identifier::named(LIVENESS_STREAM).unwrap();
    let topic = Identifier::named(LIVENESS_TOPIC).unwrap();
    let group = Identifier::named(LIVENESS_GROUP).unwrap();
    let member = harness.root_client_for_node(primary).await.unwrap();
    member
        .join_consumer_group(&stream, &topic, &group)
        .await
        .unwrap();
    let joined = liveness_group(&member).await;
    assert_eq!(joined.members_count, 1);
    let dead_member = joined.members[0].id;

    // The crashed host can no longer submit its member's disconnect Logout.
    harness.kill_node(primary).unwrap();
    drop(member);
    let survivor = (primary + 1) % harness.cluster_size();
    let deadline = Instant::now() + RESUME_BUDGET;
    let replacement = loop {
        if let Ok(client) = harness.root_client_for_node(survivor).await
            && client
                .join_consumer_group(&stream, &topic, &group)
                .await
                .is_ok()
        {
            break client;
        }
        assert!(
            Instant::now() < deadline,
            "no survivor accepted the replacement member"
        );
        sleep(RETRY_PAUSE).await;
    };

    let deadline = Instant::now() + RESUME_BUDGET;
    let expired = loop {
        let state = liveness_group(&replacement).await;
        if state.members_count == 1
            && state.members[0].id != dead_member
            && state.members[0].partitions_count == 1
        {
            break state;
        }
        assert!(
            Instant::now() < deadline,
            "dead primary's member kept its partition: {:?}",
            state.members
        );
        sleep(RETRY_PAUSE).await;
    };

    harness.restart_node(primary).unwrap();
    sleep(LIVENESS_OBSERVATION).await;
    let after = liveness_group(&replacement).await;
    assert_eq!(
        after.members_count, 1,
        "restarting the crashed host restored its member"
    );
    assert_eq!(after.members[0].id, expired.members[0].id);
}

async fn liveness_group(observer: &IggyClient) -> ConsumerGroupDetails {
    observer
        .get_consumer_group(
            &Identifier::named(LIVENESS_STREAM).unwrap(),
            &Identifier::named(LIVENESS_TOPIC).unwrap(),
            &Identifier::named(LIVENESS_GROUP).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn bind_group_member_on_backup(
    harness: &TestHarness,
    observer: &IggyClient,
) -> (TcpStream, u64, usize, usize) {
    create_liveness_group(observer).await;
    let stream = Identifier::named(LIVENESS_STREAM).unwrap();
    let topic = Identifier::named(LIVENESS_TOPIC).unwrap();
    let group = Identifier::named(LIVENESS_GROUP).unwrap();
    let primary = primary_index(harness, observer).await;
    let backup = (0..harness.cluster_size())
        .find(|&index| index != primary && index != 0)
        .unwrap();
    let (mut original, session) = register(harness.node(primary).tcp_addr().unwrap()).await;
    let body = JoinConsumerGroupRequest {
        stream_id: WireIdentifier::named(LIVENESS_STREAM).unwrap(),
        topic_id: WireIdentifier::named(LIVENESS_TOPIC).unwrap(),
        group_id: WireIdentifier::named(LIVENESS_GROUP).unwrap(),
    }
    .to_bytes();
    let header = request_header(Operation::JoinConsumerGroup, session, 1, body.len());
    commit_request_header(&mut original, &header, &body).await;

    // Closing one binding must preserve the shared logical session.
    let (backup_connection, new_session) = register(harness.node(backup).tcp_addr().unwrap()).await;
    assert_eq!(new_session, session);
    drop(original);
    let members = integration::harness::wait_for_consumer_group_assignment(
        observer,
        &stream,
        &topic,
        &group,
        1,
        COMMIT_BUDGET,
    )
    .await;
    assert_eq!(members.members_count, 1);
    (backup_connection, session, primary, backup)
}

async fn keep_session_active(connection: &mut TcpStream, session: u64, duration: Duration) {
    let body = Bytes::new();
    let header = RequestHeader::for_request(PING_CODE, CLIENT_ID, 0, session, &body).unwrap();
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        assert!(
            matches!(
                exchange(connection, &header, &body).await,
                Exchange::Reply { status: 0, .. }
            ),
            "active binding must accept a heartbeat"
        );
        sleep(RETRY_PAUSE).await;
    }
}

async fn primary_index(harness: &TestHarness, observer: &IggyClient) -> usize {
    let leader_port = observer
        .get_cluster_metadata()
        .await
        .unwrap()
        .nodes
        .iter()
        .find(|node| node.role == ClusterNodeRole::Leader)
        .unwrap()
        .endpoints
        .tcp;
    (0..harness.cluster_size())
        .find(|&index| harness.node(index).tcp_addr().unwrap().port() == leader_port)
        .unwrap()
}

async fn create_liveness_group(observer: &IggyClient) {
    observer.create_stream(LIVENESS_STREAM).await.unwrap();
    let stream = Identifier::named(LIVENESS_STREAM).unwrap();
    let topic = Identifier::named(LIVENESS_TOPIC).unwrap();
    observer
        .create_topic(
            &stream,
            LIVENESS_TOPIC,
            &TopicCreateOptions {
                partitions_count: Some(1),
                durability: Durability::Persisted,
                consumer_offset_durability: Durability::Persisted,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    observer
        .create_consumer_group(&stream, &topic, LIVENESS_GROUP)
        .await
        .unwrap();
}
