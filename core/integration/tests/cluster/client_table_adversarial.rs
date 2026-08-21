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

//! Adversarial specs against the VSR client table's at-most-once guarantees.
//!
//! Both assert the dedup contract a retrying client needs at the table's two
//! resource edges. The capacity one now passes; the reply-ring one is still a
//! RED SPEC, expected to FAIL.
//!
//! 1. Capacity: a full table evicts the entry with the oldest commit. Eviction
//!    keeps that client's request watermark (and the watermark's reply when the
//!    ring still held it), so a client that was merely quiet re-registers and
//!    its retry of an already-committed request id is answered, not re-executed.
//! 2. Reply ring: each entry retains only its `REPLY_RING_CAPACITY` most
//!    recent committed replies. A retry of a request whose reply aged out is
//!    refused with the terminal `RequestAlreadyApplied` and no result payload,
//!    indistinguishable from a rejection to the caller.
//!
//! The frames are hand-crafted on raw TCP sockets, same technique and frame
//! builders as the clients-table restart tests, because the churn needs
//! per-connection client identities and byte-level replay the SDK does not
//! expose. The builders here are parameterized by client id, which is why the
//! restart tests' fixed-identity helpers are not reused directly.

use bytes::Bytes;
use consensus::client_table::REPLY_RING_CAPACITY;
use iggy::prelude::*;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::consensus::{
    Command, Operation, ReplyHeader, RequestHeader, read_size_field, result_code,
    result_section_len,
};
use iggy_binary_protocol::requests::streams::CreateStreamRequest;
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::responses::users::LoginRegisterResponse;
use iggy_binary_protocol::{
    ClientVersionInfo, HEADER_SIZE, IGGY_PROTOCOL_VERSION, WireName, WireOptions,
};
use integration::harness::TestHarness;
use integration::iggy_harness;
use secrecy::SecretString;
use std::mem::offset_of;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// The client whose dedup state each test puts under pressure.
const CLIENT_A: u128 = 0xA11CE0001;

/// Churn identities that overflow the capacity-2 table and force the
/// eviction of `CLIENT_A`'s entry.
const CHURN_CLIENTS: [u128; 3] = [0xB0B0001, 0xB0B0002, 0xB0B0003];

/// Budget for one committed round-trip (covers transient replays while the
/// single node elects itself after boot).
const COMMIT_BUDGET: Duration = Duration::from_secs(15);

/// Per-attempt reply wait; an unanswered read is a verdict, not a reason to
/// wait longer.
const REPLY_WAIT: Duration = Duration::from_secs(5);

const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// Capacity eviction must not erase a live client's dedup watermark.
///
/// With the table floored at two slots, three fresh registrations evict
/// `CLIENT_A` (its commit is the oldest) while its connection is still open
/// and its request 1 is committed. The client then does exactly what the
/// resume contract tells a disconnected client to do: reconnect,
/// re-authenticate under its own identity, and retry the request it never saw
/// answered. The register finds no entry to rebind, so it restores the fence
/// eviction left and the retry is answered from it.
///
/// A committed duplicate-name rejection is the proof of re-execution: a dedup
/// hit replays the cached success bytes, so any committed rejection means the
/// state machine ran the operation a second time.
#[iggy_harness(cluster_nodes = 1, server(metadata.clients_table_max = "2"))]
async fn given_a_low_client_table_cap_when_connects_churn_should_keep_a_live_dedup_watermark(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream_a, session_a) = register(addr, CLIENT_A).await;
    let payload = create_stream_payload("adv-k-dedup");
    let committed = commit_request(&mut stream_a, CLIENT_A, session_a, 1, &payload).await;

    // Each register is a commit, so after the first churn client the table
    // holds [A, churn0]; the second churn register evicts A (oldest commit).
    // The sockets stay open so no Logout frees a slot and dodges the
    // capacity pressure.
    let mut churn_streams = Vec::with_capacity(CHURN_CLIENTS.len());
    for churn_client in CHURN_CLIENTS {
        churn_streams.push(register(addr, churn_client).await);
    }

    // The eviction is observable on the still-open connection: the entry is
    // gone, so the next request on it draws an eviction, not an answer. This
    // stages the scenario; the contract violation comes after the resume.
    let probe = request_header(CLIENT_A, session_a, 2, payload.len());
    match exchange(&mut stream_a, &probe, &payload).await.verdict() {
        Verdict::Evicted(reason) => {
            eprintln!("churn evicted CLIENT_A's entry (wire eviction reason byte {reason})");
        }
        Verdict::NoResultSection | Verdict::Ignored => {}
        other => panic!(
            "the churn must evict CLIENT_A's entry before the replay is meaningful; \
             its live connection still got {other:?}"
        ),
    }
    drop(stream_a);

    // Resume exactly as the contract prescribes: fresh connection,
    // credential-bearing re-register under the same client id, then retry
    // the committed request id under the new epoch.
    let (mut resumed_stream, resumed_session) = register(addr, CLIENT_A).await;
    let replay = replay_request(&mut resumed_stream, CLIENT_A, resumed_session, 1, &payload).await;

    match replay {
        Verdict::Success(replayed) => {
            assert_replayed_from_cache(&committed, &replayed, 1);
        }
        other => panic!(
            "capacity eviction erased a live client's dedup watermark: request 1 was \
             committed and its reply delivered, but after the table (capacity 2) evicted \
             the entry to admit churn registrations, the resume did not restore the fence, \
             so the retry of request 1 was re-executed by the state machine instead of \
             being answered from the dedup cache (at-most-once broken for any client the \
             table evicts while it is merely quiet); got {other:?}"
        ),
    }
}

/// A retry that arrives after more commits than the reply floor holds must
/// still replay its original result, not a terminal refusal.
///
/// Request 1 commits, then more requests than `REPLY_RING_CAPACITY` commit on
/// the same session. Retention past the floor is budgeted in bytes, so these
/// small replies all stay cached and the retry of request 1 answers with its
/// original bytes. Answering `RequestAlreadyApplied` instead tells the caller
/// its operation succeeded while handing back no result, which a slow retrier
/// cannot tell apart from a rejection.
#[iggy_harness(cluster_nodes = 1)]
async fn given_replays_past_the_reply_ring_when_a_request_id_falls_off_should_not_terminally_reject(
    harness: &mut TestHarness,
) {
    let addr = tcp_addr(harness);
    let (mut stream, session) = register(addr, CLIENT_A).await;
    let aged_payload = create_stream_payload("adv-l-aged");
    let committed = commit_request(&mut stream, CLIENT_A, session, 1, &aged_payload).await;

    // More commits than the unconditional floor holds, so passing depends on
    // the byte-budgeted retention rather than on the floor alone.
    let later_requests = REPLY_RING_CAPACITY as u64 + 1;
    for index in 0..later_requests {
        let payload = create_stream_payload(&format!("adv-l-filler-{index}"));
        commit_request(&mut stream, CLIENT_A, session, 2 + index, &payload).await;
    }

    let replay = replay_request(&mut stream, CLIENT_A, session, 1, &aged_payload).await;

    match replay {
        Verdict::Success(replayed) => {
            assert_replayed_from_cache(&committed, &replayed, 1);
        }
        other => panic!(
            "a committed request replayed past the reply floor lost its result: request 1 \
             was applied and confirmed, and {later_requests} newer commits of this size fit \
             the retention budget, so its reply must still replay; a terminal \
             RequestAlreadyApplied tells the client its operation succeeded while handing \
             back no result, which it cannot tell apart from a rejection; got {other:?}"
        ),
    }
}

fn tcp_addr(harness: &TestHarness) -> SocketAddr {
    harness
        .server()
        .tcp_addr()
        .expect("server must expose a TCP address")
}

fn create_stream_payload(name: &str) -> Bytes {
    CreateStreamRequest {
        name: WireName::new(name).unwrap(),
        options: WireOptions::empty(),
    }
    .to_bytes()
}

fn request_header(client: u128, session: u64, request: u64, body_len: usize) -> RequestHeader {
    RequestHeader {
        command: Command::Request,
        operation: Operation::CreateStream,
        size: u32::try_from(HEADER_SIZE + body_len).unwrap(),
        client,
        session,
        request,
        ..Default::default()
    }
}

/// Connect and register `client` as root, returning the connection with its
/// bound session id. Replays on transient rejections (right after boot the
/// single node may not have elected itself yet).
async fn register(addr: SocketAddr, client: u128) -> (TcpStream, u64) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        if let Some(session) = login_on(&mut stream, client).await {
            return (stream, session);
        }
        assert!(
            Instant::now() < deadline,
            "register of client {client:#x} did not commit within {COMMIT_BUDGET:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Root login/register for `client` on an already-connected socket.
/// `Some(session)` on a committed register, `None` on a transient rejection.
async fn login_on(stream: &mut TcpStream, client: u128) -> Option<u64> {
    let body = LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("adversarial-raw").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
    }
    .to_bytes();
    let header = RequestHeader {
        command: Command::Request,
        operation: Operation::Register,
        size: u32::try_from(HEADER_SIZE + body.len()).unwrap(),
        client,
        session: 0,
        request: 0,
        ..Default::default()
    };

    match exchange(stream, &header, &body).await.verdict() {
        Verdict::Success(reply) => {
            let response = LoginRegisterResponse::decode_from(&reply.payload)
                .expect("register payload must decode");
            assert_ne!(response.session, 0, "server must bind a nonzero session");
            Some(response.session)
        }
        Verdict::Rejected(code) if is_transient(code) => None,
        other => panic!("register of client {client:#x} did not commit: {other:?}"),
    }
}

/// Send one replicated request on the registered connection and require a
/// committed success within `COMMIT_BUDGET`. Returns the committed reply so a
/// later replay can be compared against it byte for byte.
async fn commit_request(
    stream: &mut TcpStream,
    client: u128,
    session: u64,
    request: u64,
    body: &Bytes,
) -> CommittedReply {
    let header = request_header(client, session, request, body.len());
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

/// Replay `request` and return the first non-transient verdict. Unlike
/// `commit_request` this never panics on a committed rejection: the rejection
/// IS the observation the red specs are after.
async fn replay_request(
    stream: &mut TcpStream,
    client: u128,
    session: u64,
    request: u64,
    body: &Bytes,
) -> Verdict {
    let header = request_header(client, session, request, body.len());
    let deadline = Instant::now() + COMMIT_BUDGET;
    loop {
        let verdict = exchange(stream, &header, body).await.verdict();
        match verdict {
            Verdict::Rejected(code) if is_transient(code) && Instant::now() < deadline => {
                sleep(RETRY_PAUSE).await;
            }
            other => return other,
        }
    }
}

/// Prove a retry was answered from the dedup cache rather than re-executed:
/// a cached replay is byte-identical to the original committed reply, while a
/// re-apply commits at a fresh op, which changes `op`/`commit` and the frame
/// checksum.
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
        replayed.payload, original.payload,
        "request {request} replay is not the cached bytes (payloads differ)"
    );
}

/// Everything one request/reply exchange can end in, spelled out so the
/// red-spec panics name the exact failure mode instead of a decode error.
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
/// payload. The header is boxed to keep it off the stack in `Verdict`.
#[derive(Debug)]
struct CommittedReply {
    header: Box<[u8; HEADER_SIZE]>,
    payload: Bytes,
}

enum Exchange {
    Reply {
        status: u32,
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
