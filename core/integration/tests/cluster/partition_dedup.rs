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

//! Spec tests for partition-plane request dedup (IGGY-274).
//!
//! Each partition consensus group keeps a slice of the VSR client table:
//! per-client request watermarks folded in at commit. A replay of an
//! already-committed `(client, request)` is answered with the empty success its
//! original earned instead of committing a second copy.
//!
//! The frames are hand-crafted on a raw TCP socket for the same reason
//! `client_table_restart` does it: the Rust SDK mints a fresh `client_id` and
//! request id per attempt, so it cannot express "the same request, twice" --
//! which is precisely the input under test. The SDK is still used for setup and
//! for reading the log back, where it is the more honest observer.

use bytes::{Bytes, BytesMut};
use futures::future::join_all;
use iggy::prelude::*;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::consensus::{
    Command, Operation, ReplyHeader, RequestHeader, read_size_field,
};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::messages::send_messages::{RawMessage, SendMessagesEncoder};
use iggy_binary_protocol::requests::users::LoginRegisterRequest;
use iggy_binary_protocol::{
    AckLevel, ClientVersionInfo, HEADER_SIZE, IGGY_PROTOCOL_VERSION, WireConsumer, WireIdentifier,
    WireName, WirePartitioning,
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

const STREAM_NAME: &str = "partition-dedup-stream";
const TOPIC_NAME: &str = "partition-dedup-topic";
const PARTITION_ID: u32 = 0;

/// Fixed wire identity, so the replay frame is byte-identical to the original.
/// The SDK would randomize this.
const CLIENT_ID: u128 = 0x0DED_1234_5678;

/// Second identity for liveness probes. The dedup watermark is a per-client
/// max, so a probe under [`CLIENT_ID`] would raise that client's watermark and
/// mask a missing transfer; the probe must not touch the identity under test.
const PROBE_CLIENT_ID: u128 = 0x0DED_9999_0001;

const REPLY_WAIT: Duration = Duration::from_secs(10);
const COMMIT_BUDGET: Duration = Duration::from_secs(20);
const RETRY_PAUSE: Duration = Duration::from_millis(100);

#[iggy_harness(server(system.sharding.cpu_allocation = "0..1"))]
async fn given_committed_send_when_replayed_should_absorb_without_a_second_copy(
    harness: &mut TestHarness,
) {
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;

    let addr = harness.node(0).tcp_addr().expect("node tcp address");
    let (mut stream, session) = register(addr).await;

    let body = send_messages_body(b"only-once");
    let header = request_header(Operation::SendMessages, session, 1, body.len());

    let original = exchange_until_committed(&mut stream, &header, &body).await;
    assert_eq!(original, 0, "the original send must commit");

    // Byte-identical replay: what a retry after a lost reply looks like.
    let replayed = exchange_until_committed(&mut stream, &header, &body).await;
    assert_eq!(
        replayed, 0,
        "an absorbed duplicate is a success, not an error"
    );

    let polled = poll_all(&client).await;
    assert_eq!(
        polled, 1,
        "the replayed send must not append a second copy (got {polled} messages)"
    );
}

#[iggy_harness(server(system.sharding.cpu_allocation = "0..1"))]
async fn given_committed_send_when_next_request_id_arrives_should_admit_it(
    harness: &mut TestHarness,
) {
    // The watermark must not wedge the client: the id above it still commits.
    // Without this, "dedup works" and "the plane is broken" look identical.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;

    let addr = harness.node(0).tcp_addr().expect("node tcp address");
    let (mut stream, session) = register(addr).await;

    for request in 1..=3u64 {
        let body = send_messages_body(format!("message-{request}").as_bytes());
        let header = request_header(Operation::SendMessages, session, request, body.len());
        let status = exchange_until_committed(&mut stream, &header, &body).await;
        assert_eq!(status, 0, "request {request} must commit");
    }

    let polled = poll_all(&client).await;
    assert_eq!(polled, 3, "each distinct request id must append once");
}

#[iggy_harness(server(system.sharding.cpu_allocation = "0..1"))]
async fn given_gapped_request_id_when_sent_should_commit(harness: &mut TestHarness) {
    // One client counter feeds every group it writes to, so a slice only ever
    // sees a subset of the ids minted. Gaps must be legal, not a wedge.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;

    let addr = harness.node(0).tcp_addr().expect("node tcp address");
    let (mut stream, session) = register(addr).await;

    for request in [1u64, 9, 40] {
        let body = send_messages_body(format!("gap-{request}").as_bytes());
        let header = request_header(Operation::SendMessages, session, request, body.len());
        let status = exchange_until_committed(&mut stream, &header, &body).await;
        assert_eq!(status, 0, "gapped request {request} must commit");
    }

    let polled = poll_all(&client).await;
    assert_eq!(polled, 3, "a gapped id is new, not a duplicate");
}

#[iggy_harness(server(system.sharding.cpu_allocation = "0..1"))]
async fn given_committed_consumer_offset_when_replayed_should_absorb(harness: &mut TestHarness) {
    // Dedup covers every replicated partition write, not just produces. A
    // replayed offset store must answer success rather than committing twice.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;

    let addr = harness.node(0).tcp_addr().expect("node tcp address");
    let (mut stream, session) = register(addr).await;

    // Seed a message so offset 0 is in range for the store.
    let produce = send_messages_body(b"seed");
    let produce_header = request_header(Operation::SendMessages, session, 1, produce.len());
    assert_eq!(
        exchange_until_committed(&mut stream, &produce_header, &produce).await,
        0,
        "the seed produce must commit"
    );

    let body = store_offset_body(0);
    let header = request_header(Operation::StoreConsumerOffset, session, 2, body.len());

    let original = exchange_until_committed(&mut stream, &header, &body).await;
    assert_eq!(original, 0, "the original offset store must commit");

    let replayed = exchange_until_committed(&mut stream, &header, &body).await;
    assert_eq!(
        replayed, 0,
        "a replayed offset store is absorbed as a success"
    );

    // The next id still gets through: the watermark must not wedge the client.
    let next = store_offset_body(0);
    let next_header = request_header(Operation::StoreConsumerOffset, session, 3, next.len());
    assert_eq!(
        exchange_until_committed(&mut stream, &next_header, &next).await,
        0,
        "the id above the watermark must still commit"
    );
}

/// More connections than the prepare queue holds, each with one write in
/// flight at the same instant, so the surplus parks in the request queue and is
/// promoted into a prepare slot at a later commit. Every one of them must be
/// answered: a write promoted without the reply sender it parked with commits
/// but leaves its connection waiting out a timeout, and the count is the
/// second discriminator (each writer's single id must commit exactly once).
const CONCURRENT_WRITERS: u64 = 40;

/// Distinct identity per writer, so each connection's watermark is its own and
/// the request ids can all be 1.
const WRITER_CLIENT_BASE: u128 = 0x0DED_C0DE_0000;

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.prepare_queue_depth = "4"
    )
)]
async fn given_more_writers_than_prepare_slots_when_all_send_at_once_should_answer_every_one(
    harness: &mut TestHarness,
) {
    // Three nodes so a prepare needs a replication round trip to commit and the
    // pipeline actually fills; a solo primary self-acks per frame and never
    // exposes the request queue. The queue depth is pinned low so forty writers
    // overflow it deterministically rather than by timing luck.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;

    let addr = harness.node(0).tcp_addr().expect("node tcp address");
    // Register every connection first so the writes race each other, not the
    // logins.
    let mut connections = Vec::with_capacity(CONCURRENT_WRITERS as usize);
    for writer in 0..CONCURRENT_WRITERS {
        let client_id = WRITER_CLIENT_BASE + u128::from(writer);
        let (stream, session) = register_client_with_budget(addr, client_id, COMMIT_BUDGET).await;
        connections.push((client_id, stream, session));
    }

    let sends = connections
        .iter_mut()
        .map(|(client_id, stream, session)| async move {
            let body = send_messages_body(format!("writer-{client_id:x}").as_bytes());
            let header =
                request_header_for(*client_id, Operation::SendMessages, *session, 1, body.len());
            exchange_with_budget(stream, &header, &body, COMMIT_BUDGET).await
        });
    let statuses = join_all(sends).await;
    for (writer, status) in statuses.into_iter().enumerate() {
        assert_eq!(
            status, 0,
            "writer {writer} must be answered with its commit (got status {status})"
        );
    }

    let polled = poll_all(&client).await;
    assert_eq!(
        u64::from(polled),
        CONCURRENT_WRITERS,
        "every writer's single request must commit exactly once"
    );
}

/// `StoreConsumerOffset` body for the raw connection's own consumer id.
fn store_offset_body(offset: u64) -> Bytes {
    StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
        stream_id: WireIdentifier::named(STREAM_NAME).expect("stream identifier"),
        topic_id: WireIdentifier::named(TOPIC_NAME).expect("topic identifier"),
        partition_id: Some(PARTITION_ID),
        offset,
        ack: AckLevel::Quorum,
    }
    .to_bytes()
}

/// State-transfer choreography end to end: rejoin, view changes, and the
/// final commits ride slow CI runners.
const TRANSFER_BUDGET: Duration = Duration::from_secs(60);
const FINAL_COMMIT_BUDGET: Duration = Duration::from_secs(120);
const MARKER_POLL: Duration = Duration::from_millis(200);
const INSTALL_MARKER: &str = "partition state transfer installed";

/// Pre-stop produces fold into every replica's slice live; the rest commit
/// while node 2 is down. Total must push the evicted ring (capacity 64) past
/// the rejoiner's durable end, or repair closes the gap and no transfer runs.
const PRE_STOP_SENDS: u64 = 40;
/// The identity under test stops sending here; everything after comes from the
/// filler client. The rejoiner's tail repair re-applies the ring window (the
/// LAST ~64 commits) through the ordinary commit path, and the watermark is a
/// per-client max -- so if the tested client appeared anywhere in that window,
/// repair alone would cover every lower id and the artifact would be
/// redundant. The filler pushes the tested client's last send out of the ring,
/// leaving the transferred artifact as node 2's ONLY source for it.
const TESTED_CLIENT_SENDS: u64 = 140;
const FILLER_SENDS: u64 = 100;
const TOTAL_SENDS: u64 = TESTED_CLIENT_SENDS + FILLER_SENDS;
/// Replayed id: the tested client's watermark itself. Absorbing it requires an
/// entry for that client, which only the transferred artifact can supply.
const REPLAYED_REQUEST: u64 = TESTED_CLIENT_SENDS;

/// Filler identity whose sends evict the tested client from the repair ring.
const FILLER_CLIENT_ID: u128 = 0x0DED_F111_E400;

/// Sentinel status for an Eviction frame: the connection's session is gone and
/// the caller must reconnect and re-register before retrying.
const EVICTED: u32 = u32::MAX;

/// Sentinel status for a socket the server closed mid-exchange (a node stopped
/// under the connection). Same contract as [`EVICTED`]: reconnect, re-register,
/// retry the identical frame.
const DISCONNECTED: u32 = u32::MAX - 1;

#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        partition.evicted_ring_capacity = "64"
    )
)]
async fn given_transferred_dedup_slice_when_old_request_replays_should_absorb(
    harness: &mut TestHarness,
) {
    // Phase 1: node 0 is every group's view-0 primary. Produce the pre-stop
    // window with node 2 live, then the rest with it stopped, so the second
    // window exists on node 2 only via state transfer.
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect a root client");
    seed_topic(&client).await;
    let addr = harness.node(0).tcp_addr().expect("node 0 tcp address");
    let (mut stream, session) = register(addr).await;
    raw_produce(&mut stream, session, 1..=PRE_STOP_SENDS, COMMIT_BUDGET).await;
    sleep(Duration::from_secs(1)).await;
    harness.stop_node(2).expect("stop node 2");
    raw_produce(
        &mut stream,
        session,
        (PRE_STOP_SENDS + 1)..=TESTED_CLIENT_SENDS,
        COMMIT_BUDGET,
    )
    .await;
    drop(stream);
    let (mut filler, filler_session) =
        register_client_with_budget(addr, FILLER_CLIENT_ID, COMMIT_BUDGET).await;
    raw_produce_for(
        &mut filler,
        FILLER_CLIENT_ID,
        filler_session,
        1..=FILLER_SENDS,
        COMMIT_BUDGET,
    )
    .await;
    drop(filler);
    drop(client);

    // Phase 2: the rejoin cannot repair past the survivors' evicted ring, so
    // it converts to state transfer; the install carries the dedup section.
    harness.restart_node(2).expect("restart node 2");
    await_marker(harness, 2, INSTALL_MARKER).await;

    // Phase 3: walk the primaries off node 0 and node 1 so the REPLAY is
    // admitted by the transferred node. Stopping node 0 elects node 1
    // (view 1); after node 0 rejoins, stopping node 1 elects node 2 (view 2)
    // with quorum {0, 2}.
    harness.stop_node(0).expect("stop node 0");
    sleep(Duration::from_secs(2)).await;
    harness.restart_node(0).expect("restart node 0");
    sleep(Duration::from_secs(2)).await;
    harness.stop_node(1).expect("stop node 1");

    // Phase 4, on node 2. The probe send goes FIRST and under a DIFFERENT
    // client: its commit proves the view settled on node 2 and the rejoined
    // node 0 is acking, and it pins the expected count -- while leaving
    // CLIENT_ID's watermark exactly what the transfer installed (the watermark
    // is a per-client max, so a same-client probe would mask a missing
    // transfer). Sends reconnect + re-register on eviction; dedup keys on the
    // client id and must hold across a re-register.
    let addr = harness.node(2).tcp_addr().expect("node 2 tcp address");
    let fresh = send_reconnecting(addr, PROBE_CLIENT_ID, 1, FINAL_COMMIT_BUDGET).await;
    assert_eq!(
        fresh, 0,
        "the probe client's send must commit on the new primary"
    );

    let replayed = send_reconnecting(addr, CLIENT_ID, REPLAYED_REQUEST, FINAL_COMMIT_BUDGET).await;
    assert_eq!(
        replayed, 0,
        "a replay of a transferred watermark is absorbed as a success"
    );

    // The count is the discriminator: an absorbed replay leaves it at
    // TOTAL_SENDS + 1; a re-execution (empty transferred slice) appends a
    // second copy of the replayed payload.
    let client = harness
        .root_client_for_node(2)
        .await
        .expect("connect a root client to node 2");
    let polled = poll_up_to(&client, (TOTAL_SENDS + 16) as u32).await;
    assert_eq!(
        u64::from(polled),
        TOTAL_SENDS + 1,
        "the transferred slice must absorb the replay instead of re-executing it"
    );
}

/// One send under `request`, surviving evictions: reconnect, re-register, and
/// retry the identical frame until it answers or the budget runs out.
async fn send_reconnecting(addr: SocketAddr, client: u128, request: u64, budget: Duration) -> u32 {
    let deadline = Instant::now() + budget;
    let body = send_messages_body(format!("send-{request}").as_bytes());
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            remaining > Duration::ZERO,
            "request {request} did not resolve within {budget:?}"
        );
        let (mut stream, session) = register_client_with_budget(addr, client, remaining).await;
        let header = request_header_for(
            client,
            Operation::SendMessages,
            session,
            request,
            body.len(),
        );
        let status = exchange_with_budget(&mut stream, &header, &body, remaining).await;
        if status != EVICTED && status != DISCONNECTED {
            return status;
        }
        sleep(RETRY_PAUSE).await;
    }
}

/// Produce one single-message batch per request id over the lockstep raw
/// connection, waiting out each commit.
async fn raw_produce(
    stream: &mut TcpStream,
    session: u64,
    requests: std::ops::RangeInclusive<u64>,
    budget: Duration,
) {
    raw_produce_for(stream, CLIENT_ID, session, requests, budget).await;
}

async fn raw_produce_for(
    stream: &mut TcpStream,
    client: u128,
    session: u64,
    requests: std::ops::RangeInclusive<u64>,
    budget: Duration,
) {
    for request in requests {
        let body = send_messages_body(format!("send-{request}").as_bytes());
        let header = request_header_for(
            client,
            Operation::SendMessages,
            session,
            request,
            body.len(),
        );
        let status = exchange_with_budget(stream, &header, &body, budget).await;
        assert_ne!(
            status, DISCONNECTED,
            "server closed the lockstep connection under request {request}"
        );
        assert_eq!(status, 0, "request {request} must commit");
    }
}

async fn await_marker(harness: &TestHarness, node: usize, marker: &str) {
    let deadline = Instant::now() + TRANSFER_BUDGET;
    while !harness.node(node).stdout_contains(marker) {
        assert!(
            Instant::now() < deadline,
            "node {node} never logged {marker:?} within {TRANSFER_BUDGET:?}"
        );
        sleep(MARKER_POLL).await;
    }
}

async fn seed_topic(client: &IggyClient) {
    client
        .create_stream(STREAM_NAME)
        .await
        .expect("create stream");
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                // Every commit flushes and ring-evicts, which is what marches
                // the repair floor past a rejoiner and forces the transfer the
                // transferred-slice spec depends on.
                messages_required_to_save: Some(1),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .expect("create topic");
}

async fn poll_all(client: &IggyClient) -> u32 {
    poll_up_to(client, 100).await
}

async fn poll_up_to(client: &IggyClient, max: u32) -> u32 {
    let stream_id = Identifier::named(STREAM_NAME).expect("stream identifier");
    let topic_id = Identifier::named(TOPIC_NAME).expect("topic identifier");
    client
        .poll_messages(
            &stream_id,
            &topic_id,
            Some(PARTITION_ID),
            &Consumer::new(Identifier::numeric(1).expect("consumer identifier")),
            &PollingStrategy::offset(0),
            max,
            false,
        )
        .await
        .expect("poll messages")
        .messages
        .len() as u32
}

/// Full `SendMessages` body: metadata prefix, batch header, one message.
fn send_messages_body(payload: &[u8]) -> Bytes {
    let stream_id = WireIdentifier::named(STREAM_NAME).expect("stream identifier");
    let topic_id = WireIdentifier::named(TOPIC_NAME).expect("topic identifier");
    let partitioning = WirePartitioning::PartitionId(PARTITION_ID);
    let messages = [RawMessage {
        // A fixed id keeps the replay byte-identical; a zero would be
        // server-stamped and the two frames would diverge.
        id: 0x5EED,
        origin_timestamp: 0,
        headers: None,
        payload,
    }];
    let size = SendMessagesEncoder::encoded_size(&stream_id, &topic_id, &partitioning, &messages);
    let mut buf = BytesMut::with_capacity(size);
    SendMessagesEncoder::encode(&mut buf, &stream_id, &topic_id, &partitioning, &messages)
        .expect("encode send_messages body");
    buf.freeze()
}

fn request_header(
    operation: Operation,
    session: u64,
    request: u64,
    body_len: usize,
) -> RequestHeader {
    request_header_for(CLIENT_ID, operation, session, request, body_len)
}

fn request_header_for(
    client: u128,
    operation: Operation,
    session: u64,
    request: u64,
    body_len: usize,
) -> RequestHeader {
    RequestHeader {
        command: Command::Request,
        operation,
        size: u32::try_from(HEADER_SIZE + body_len).unwrap(),
        client,
        session,
        request,
        ..Default::default()
    }
}

/// Exchange until the server stops answering transiently, returning the reply
/// status. A transient means the request was never admitted, so replaying it
/// keeps the same id -- exactly what the SDK's own retry loop does.
async fn exchange_until_committed(
    stream: &mut TcpStream,
    header: &RequestHeader,
    body: &Bytes,
) -> u32 {
    let status = exchange_with_budget(stream, header, body, COMMIT_BUDGET).await;
    assert_ne!(
        status, DISCONNECTED,
        "server closed the lockstep connection under request {}",
        header.request
    );
    status
}

async fn exchange_with_budget(
    stream: &mut TcpStream,
    header: &RequestHeader,
    body: &Bytes,
    budget: Duration,
) -> u32 {
    let deadline = Instant::now() + budget;
    loop {
        let status = exchange(stream, header, body).await;
        if !is_transient(status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "request {} stayed transient for {budget:?}",
            header.request
        );
        sleep(RETRY_PAUSE).await;
    }
}

/// Write one frame, read one frame, return the reply status. The connection is
/// lockstep, so the reply that comes back is this request's. A socket the
/// server closed (a node stopping under the connection) answers
/// [`DISCONNECTED`] rather than panicking, so the reconnecting callers can
/// treat it like an eviction; a reply that never comes is still a failure.
async fn exchange(stream: &mut TcpStream, header: &RequestHeader, body: &Bytes) -> u32 {
    if stream.write_all(bytemuck::bytes_of(header)).await.is_err() {
        return DISCONNECTED;
    }
    if !body.is_empty() && stream.write_all(body).await.is_err() {
        return DISCONNECTED;
    }

    let mut reply_header = [0u8; HEADER_SIZE];
    match timeout(REPLY_WAIT, stream.read_exact(&mut reply_header)).await {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return DISCONNECTED,
        Err(_) => panic!("reply header timed out"),
    }

    let command_offset = offset_of!(RequestHeader, command);
    if reply_header[command_offset] == Command::Eviction as u8 {
        // The session died (view change, epoch fence): the contract is
        // reconnect + re-register, and dedup must still hold because it keys
        // on the client id, not the session.
        return EVICTED;
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
    if total_size > HEADER_SIZE {
        let mut discard = vec![0u8; total_size - HEADER_SIZE];
        match timeout(REPLY_WAIT, stream.read_exact(&mut discard)).await {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => return DISCONNECTED,
            Err(_) => panic!("reply body timed out"),
        }
    }
    status
}

/// Register `CLIENT_ID` as root, returning the connection and its bound
/// session. The session binds to THIS socket server-side, so every frame in a
/// test must reuse the returned stream.
async fn register(addr: SocketAddr) -> (TcpStream, u64) {
    register_with_budget(addr, COMMIT_BUDGET).await
}

/// Fresh socket per attempt: a login refused mid-election may come back as an
/// eviction that poisons the connection.
async fn register_with_budget(addr: SocketAddr, budget: Duration) -> (TcpStream, u64) {
    register_client_with_budget(addr, CLIENT_ID, budget).await
}

async fn register_client_with_budget(
    addr: SocketAddr,
    client: u128,
    budget: Duration,
) -> (TcpStream, u64) {
    let deadline = Instant::now() + budget;
    loop {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        if let Some(session) = login_on(&mut stream, client).await {
            return (stream, session);
        }
        assert!(
            Instant::now() < deadline,
            "register did not commit within {budget:?}"
        );
        sleep(RETRY_PAUSE).await;
    }
}

async fn login_on(stream: &mut TcpStream, client: u128) -> Option<u64> {
    let body = LoginRegisterRequest {
        version_info: ClientVersionInfo {
            protocol_version: IGGY_PROTOCOL_VERSION,
            sdk_name: WireName::new("iggy274-raw").unwrap(),
            sdk_version: WireName::new("0.0.1").unwrap(),
        },
        username: WireName::new(DEFAULT_ROOT_USERNAME).unwrap(),
        password: SecretString::from(DEFAULT_ROOT_PASSWORD),
        client_context: None,
    }
    .to_bytes();
    let header = request_header_for(client, Operation::Register, 0, 0, body.len());

    stream.write_all(bytemuck::bytes_of(&header)).await.unwrap();
    stream.write_all(&body).await.unwrap();

    let mut reply_header = [0u8; HEADER_SIZE];
    let Ok(Ok(_)) = timeout(REPLY_WAIT, stream.read_exact(&mut reply_header)).await else {
        return None;
    };
    let command_offset = offset_of!(RequestHeader, command);
    if reply_header[command_offset] != Command::Reply as u8 {
        return None;
    }

    let status_offset = offset_of!(ReplyHeader, status);
    let status = u32::from_le_bytes(
        reply_header[status_offset..status_offset + 4]
            .try_into()
            .unwrap(),
    );
    let total_size = read_size_field(&reply_header).expect("login reply size") as usize;
    let mut reply_body = vec![0u8; total_size - HEADER_SIZE];
    let Ok(Ok(_)) = timeout(REPLY_WAIT, stream.read_exact(&mut reply_body)).await else {
        return None;
    };
    if status != 0 {
        return None;
    }
    let session_offset = offset_of!(ReplyHeader, commit);
    Some(u64::from_le_bytes(
        reply_header[session_offset..session_offset + 8]
            .try_into()
            .unwrap(),
    ))
}

fn is_transient(code: u32) -> bool {
    code == IggyError::TransientNotCommitted.as_code()
        || code == IggyError::TransientNotAccepted.as_code()
}
