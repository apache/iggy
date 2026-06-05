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

//! End-to-end replica auth handshake over real loopback TCP. The dialer
//! holds the lower id and dials the higher-id acceptor. The MAC algebra
//! (reflection, replay, swapped pair) is unit-tested in
//! `message_bus::replica::auth`; here we assert the full 3-message exchange
//! gates the registry insert: only a peer that proves possession of the
//! cluster PSK is installed (membership, not per-replica identity).

mod common;

use common::{install_replicas_locally, loopback};
use compio::net::TcpStream;
use iggy_binary_protocol::{Command2, GenericHeader, HEADER_SIZE};
use iggy_common::IggyError;
use message_bus::IggyMessageBus;
use message_bus::connector::start as start_connector;
use message_bus::framing::{self, MAX_MESSAGE_SIZE};
use message_bus::replica::auth::{self, HandshakeStatus, ReplicaAuth};
use message_bus::replica::listener::{MessageHandler, bind, run};
use server_common::Message;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0xCAFE;
const SECRET_A: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SECRET_B: &[u8] = b"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
const LONG_PERIOD: Duration = Duration::from_secs(30);
const GRACE: Duration = Duration::from_secs(10);

fn noop_handler() -> MessageHandler {
    Rc::new(|_peer, _msg| {})
}

/// Spawn replica 1's inbound listener with the given optional auth, returning
/// its bound address. Replica 1 is the acceptor (higher id) of the pair.
#[allow(clippy::future_not_send)]
async fn spawn_acceptor(
    bus: &Rc<IggyMessageBus>,
    auth: Option<ReplicaAuth>,
) -> std::net::SocketAddr {
    let (listener, addr) = bind(loopback()).await.expect("bind acceptor");
    let token = bus.token();
    let delegate = install_replicas_locally(bus.clone(), noop_handler());
    let handle = compio::runtime::spawn(async move {
        run(
            listener,
            token,
            CLUSTER,
            1,
            2,
            auth,
            delegate,
            message_bus::framing::MAX_MESSAGE_SIZE,
            GRACE,
        )
        .await;
    });
    bus.track_background(handle);
    addr
}

#[compio::test]
async fn authenticated_handshake_registers_verified_peer() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    start_connector(
        &dialer,
        CLUSTER,
        0,
        vec![(1, addr)],
        Some(ReplicaAuth::new(SECRET_A)),
        GRACE,
        install_replicas_locally(dialer.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;

    // Both ends register only after the mutual MAC verifies.
    wait_until(|| dialer.replicas().contains(1), Duration::from_secs(2)).await;
    wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn wrong_key_rejects_peer() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_B))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    start_connector(
        &dialer,
        CLUSTER,
        0,
        vec![(1, addr)],
        Some(ReplicaAuth::new(SECRET_A)),
        GRACE,
        install_replicas_locally(dialer.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;

    // Dialer rejects the acceptor's MAC; neither side completes the exchange.
    settle().await;
    assert!(
        !dialer.replicas().contains(1),
        "dialer must reject wrong key"
    );
    assert!(
        !acceptor.replicas().contains(0),
        "acceptor must not install an unfinished peer"
    );

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn enforcement_rejects_legacy_peer() {
    // Acceptor requires auth; dialer speaks the legacy (no-nonce) protocol.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    start_connector(
        &dialer,
        CLUSTER,
        0,
        vec![(1, addr)],
        None,
        GRACE,
        install_replicas_locally(dialer.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;

    settle().await;
    assert!(
        !acceptor.replicas().contains(0),
        "enforcement must reject an unauthenticated peer"
    );

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn dialer_times_out_when_acceptor_sends_no_challenge() {
    // Authenticated dialer vs a legacy acceptor that never answers with a
    // ReplicaChallenge: the dialer's handshake_grace fires and it drops without
    // registering. Exercises the mid-handshake timeout path.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, None).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    let short_grace = Duration::from_millis(200);
    start_connector(
        &dialer,
        CLUSTER,
        0,
        vec![(1, addr)],
        Some(ReplicaAuth::new(SECRET_A)),
        short_grace,
        install_replicas_locally(dialer.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;

    compio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        !dialer.replicas().contains(1),
        "dialer must time out waiting for the ReplicaChallenge and not register"
    );

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn acceptor_nacks_authenticated_dialer_on_cluster_mismatch() {
    // An authenticated dialer (sends a nonce) with the WRONG cluster id is
    // rejected before the acceptor's success ReplicaChallenge. Because it is a
    // waiting authenticated peer, the acceptor answers with a typed Nack (a
    // ReplicaChallenge carrying a nonzero status) instead of a bare close.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let nonce = auth::random_nonce().expect("nonce");
    framing::write_message(&mut stream, build_hello(0xDEAD, 0, Some(&nonce)))
        .await
        .expect("write hello");

    let resp = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read reject");
    assert_eq!(resp.header().command, Command2::ReplicaChallenge);
    assert_eq!(
        auth::read_status(&resp.header().reserved_command),
        HandshakeStatus::ClusterMismatch,
    );

    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn no_nack_to_unauthenticated_peer() {
    // Enforcement on; a legacy (no-nonce) ReplicaHello is rejected. Such a dialer
    // delegates its fd without reading, so the acceptor must NOT emit a Nack
    // (it would land in the VSR reader). The peer just sees EOF.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    framing::write_message(&mut stream, build_hello(CLUSTER, 0, None))
        .await
        .expect("write hello");

    let res = framing::read_message(&mut stream, MAX_MESSAGE_SIZE).await;
    assert!(
        matches!(res, Err(IggyError::ConnectionClosed)),
        "expected bare close on unauthenticated reject"
    );

    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn acceptor_rejects_wrong_command_on_frame3() {
    // After the acceptor sends ReplicaChallenge(Ok), the dialer must answer with
    // a ReplicaFinish. A third frame with any other command is rejected on the
    // command byte (before the MAC), the peer is not installed, and - because a
    // real dialer has stopped reading by now - no frame is sent back.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let nonce = auth::random_nonce().expect("nonce");
    framing::write_message(&mut stream, build_hello(CLUSTER, 0, Some(&nonce)))
        .await
        .expect("write hello");

    let challenge = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read challenge");
    assert_eq!(challenge.header().command, Command2::ReplicaChallenge);
    assert_eq!(
        auth::read_status(&challenge.header().reserved_command),
        HandshakeStatus::Ok,
    );

    // Wrong command in the finish slot (Prepare instead of ReplicaFinish).
    framing::write_message(&mut stream, build_raw(CLUSTER, 0, Command2::Prepare))
        .await
        .expect("write wrong finish");

    let res = framing::read_message(&mut stream, MAX_MESSAGE_SIZE).await;
    assert!(
        matches!(res, Err(IggyError::ConnectionClosed)),
        "expected bare close on wrong-command finish (no reject frame)"
    );
    assert!(
        !acceptor.replicas().contains(0),
        "acceptor must not install a peer that sent a wrong finish command"
    );

    acceptor.shutdown(Duration::from_secs(2)).await;
}

/// Build a raw `ReplicaHello` frame for the wire-level reject tests. With
/// `nonce` set it opens the authenticated handshake (`reserved_command[0..32]`);
/// without, it is a legacy plaintext announce.
#[allow(clippy::cast_possible_truncation)]
fn build_hello(
    cluster_id: u128,
    replica_id: u8,
    nonce: Option<&[u8; auth::NONCE_LEN]>,
) -> Message<GenericHeader> {
    Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
        h.command = Command2::ReplicaHello;
        h.cluster = cluster_id;
        h.replica = replica_id;
        h.size = HEADER_SIZE as u32;
        if let Some(nonce) = nonce {
            h.reserved_command[..auth::NONCE_LEN].copy_from_slice(nonce);
        }
    })
}

/// Build a raw frame with an arbitrary command for the wire-level tests (used to
/// send a wrong-command third frame in place of a `ReplicaFinish`).
#[allow(clippy::cast_possible_truncation)]
fn build_raw(cluster_id: u128, replica_id: u8, command: Command2) -> Message<GenericHeader> {
    Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
        h.command = command;
        h.cluster = cluster_id;
        h.replica = replica_id;
        h.size = HEADER_SIZE as u32;
    })
}

/// Let an in-flight (failing) handshake run to completion on loopback.
#[allow(clippy::future_not_send)]
async fn settle() {
    compio::time::sleep(Duration::from_millis(400)).await;
}

#[allow(clippy::future_not_send)]
async fn wait_until<F: Fn() -> bool>(cond: F, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("wait_until: condition not met within {timeout:?}");
}
