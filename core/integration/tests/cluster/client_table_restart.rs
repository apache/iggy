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
//! 2. Implicit rebind: a replicated request on an unbound transport whose
//!    `(client, session)` matches a live table entry rebinds the transport
//!    (`try_resume_session`); the same session arriving on two connections
//!    evicts the older binding (`SessionManager::bind_session`).
//! 3. Sessions survive transport disconnect: `submit_disconnect_logout`
//!    tears down only consumer-group members (the group must rebalance off
//!    a dead consumer); everything else keeps its slot for resume until an
//!    explicit `Logout` or capacity eviction.
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
//! These tests pin the implicit-rebind contract: a resumed client simply
//! keeps sending under its old `(client, session)` on a fresh connection and
//! the server rebinds the transport from the recovered table. If the
//! session-resume work settles on an explicit resume handshake instead,
//! adjust `resume_request` to speak it.

#![cfg(feature = "vsr")]

use bytes::Bytes;
use iggy::prelude::*;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::consensus::{
    Command2, Operation, ReplyHeader, RequestHeader, read_size_field, result_code,
    result_section_len,
};
use iggy_binary_protocol::namespace::METADATA_CONSENSUS_NAMESPACE;
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
    commit_request(&mut stream, session, 1, &create_stream).await;
    drop(stream);

    harness.restart_server().await.unwrap();

    // The reply for request 1 was already delivered, but the client cannot
    // know that in the crash window; retrying the same id must converge on
    // the cached reply, never on a second apply or a silent drop.
    let addrs = tcp_addrs(harness);
    resume_request(&addrs, session, 1, &create_stream).await;
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
    drop(stream);

    harness.restart_server().await.unwrap();

    // Continuation, not retry: the session advances to the next id. A node
    // that forgot the watermark sees request 2 on an unknown session and
    // either drops it as a gap or bounces the session entirely.
    let addrs = tcp_addrs(harness);
    resume_request(&addrs, session, 2, &create_stream_payload("iggy137-second")).await;
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
        namespace: match operation {
            Operation::Register => METADATA_CONSENSUS_NAMESPACE,
            _ => 0,
        },
        ..Default::default()
    }
}

/// Register `CLIENT_ID` as root and return the connection with its bound
/// session id. The session binds to THIS transport connection server-side,
/// so the pre-restart request must reuse the returned stream. Replays on
/// transient rejections: right after boot the single node may not have
/// elected itself yet.
pub(super) async fn register(addr: SocketAddr) -> (TcpStream, u64) {
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

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        match exchange(&mut stream, &header, &body).await.verdict() {
            Verdict::Success(payload) => {
                let response = LoginRegisterResponse::decode_from(&payload)
                    .expect("register payload must decode");
                assert_ne!(response.session, 0, "server must bind a nonzero session");
                return (stream, response.session);
            }
            Verdict::Rejected(code) if is_transient(code) && Instant::now() < deadline => {
                sleep(RETRY_PAUSE).await;
            }
            other => panic!("register did not commit: {other:?}"),
        }
    }
}

/// Send one replicated metadata request on the registered connection and
/// require a committed success within `COMMIT_BUDGET`.
pub(super) async fn commit_request(
    stream: &mut TcpStream,
    session: u64,
    request: u64,
    body: &Bytes,
) {
    let header = request_header(Operation::CreateStream, session, request, body.len());
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        match exchange(stream, &header, body).await.verdict() {
            Verdict::Success(_) => return,
            Verdict::Rejected(code) if is_transient(code) && Instant::now() < deadline => {
                sleep(RETRY_PAUSE).await;
            }
            other => panic!("request {request} did not commit: {other:?}"),
        }
    }
}

/// Post-restart continuation: keep presenting the old identity, round-robin
/// across every node, until some node commits (or serves the cached reply
/// for) the request. Every attempt uses a fresh connection, both because the
/// old one died with the node and so an unanswered frame cannot desync the
/// next attempt. Panics with the last observed failure mode when the budget
/// runs out.
pub(super) async fn resume_request(addrs: &[SocketAddr], session: u64, request: u64, body: &Bytes) {
    let header = request_header(Operation::CreateStream, session, request, body.len());
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
        match exchange(&mut stream, &header, body).await.verdict() {
            Verdict::Success(_) => return,
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

/// Everything one request/reply exchange can end in, spelled out so the
/// red-test panic names the exact failure mode instead of a decode error.
#[derive(Debug)]
enum Verdict {
    /// Committed success; carries the payload after the result section.
    Success(Bytes),
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

enum Exchange {
    Reply { status: u32, body: Bytes },
    Eviction { reason: u8 },
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
            Self::Reply { body, .. } => match result_code(&body) {
                None => Verdict::NoResultSection,
                Some(0) => {
                    let payload_start = result_section_len(&body).unwrap();
                    Verdict::Success(body.slice(payload_start..))
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
        body: body.into(),
    }
}

fn is_transient(code: u32) -> bool {
    code == IggyError::TransientNotCommitted.as_code()
        || code == IggyError::TransientNotAccepted.as_code()
}
