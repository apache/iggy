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

//! End-to-end replica auth handshake over real loopback TCP, through the
//! owning-shard install paths (shard 0 reads only the placement preamble,
//! the handshake runs in the install task). The dialer holds
//! the lower id and dials the higher-id acceptor. The MAC algebra
//! (reflection, replay, swapped pair) is unit-tested in
//! `message_bus::replica::auth`; here we assert the full 3-message
//! exchange gates the registry insert: only a peer that proves
//! possession of the cluster PSK is installed (membership, not
//! per-replica identity).

mod common;

use common::{
    install_dialed_replicas_locally, install_replicas_locally, loopback, set_replica_ctx,
};
use compio::net::TcpStream;
use iggy_binary_protocol::{Command, GenericHeader, HEADER_SIZE};
use iggy_common::IggyError;
use message_bus::connector::start as start_connector;
use message_bus::framing::{self, MAX_MESSAGE_SIZE};
use message_bus::replica::auth::{self, HandshakeStatus, LinkPlacement, ReplicaAuth};
use message_bus::replica::listener::{MessageHandler, bind, run};
use message_bus::replica::preamble::{Preamble, read_preamble, write_preamble};
use message_bus::{IggyMessageBus, MessageBus, MessageBusConfig};
use server_common::Message;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0xCAFE;
const SECRET_A: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const SECRET_B: &[u8] = b"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
const LONG_PERIOD: Duration = Duration::from_secs(30);
const PREAMBLE_WAIT: Duration = Duration::from_secs(2);
/// The placement of link 0 between one-link nodes.
const PLACEMENT: LinkPlacement = LinkPlacement { links: 1, link: 0 };
const LINK_0_OF_REPLICA_0: Preamble = Preamble { dialer: 0, link: 0 };

fn noop_handler() -> MessageHandler {
    Rc::new(|_peer, _msg| {})
}

/// Spawn replica 1's inbound listener with the given optional auth wired
/// into its bus ctx, returning its bound address. Replica 1 is the
/// acceptor (higher id) of the pair; the acceptor handshake runs in the
/// local install path, mirroring the production owning shard.
#[allow(clippy::future_not_send)]
async fn spawn_acceptor(
    bus: &Rc<IggyMessageBus>,
    auth: Option<ReplicaAuth>,
) -> std::net::SocketAddr {
    spawn_acceptor_as(bus, 1, 2, auth).await
}

/// [`spawn_acceptor`] for replica `self_id` of a `replica_count`-node
/// cluster.
#[allow(clippy::future_not_send)]
async fn spawn_acceptor_as(
    bus: &Rc<IggyMessageBus>,
    self_id: u8,
    replica_count: u8,
    auth: Option<ReplicaAuth>,
) -> std::net::SocketAddr {
    set_replica_ctx(bus, CLUSTER, self_id, replica_count, auth);
    let (listener, addr) = bind(loopback()).await.expect("bind acceptor");
    let token = bus.token();
    let delegate = install_replicas_locally(bus.clone(), noop_handler());
    let handle = compio::runtime::spawn(async move {
        run(listener, token, delegate).await;
    });
    bus.track_background(handle);
    addr
}

/// Build a dialer bus (replica 0) with the given auth and start its
/// connector toward `addr`.
#[allow(clippy::future_not_send)]
async fn spawn_dialer(
    bus: &Rc<IggyMessageBus>,
    auth: Option<ReplicaAuth>,
    addr: std::net::SocketAddr,
) {
    set_replica_ctx(bus, CLUSTER, 0, 2, auth);
    start_connector(
        bus,
        0,
        vec![(1, addr)],
        install_dialed_replicas_locally(bus.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;
}

#[compio::test]
async fn authenticated_handshake_registers_verified_peer() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer(&dialer, Some(ReplicaAuth::new(SECRET_A)), addr).await;

    // Both ends register only after the mutual MAC verifies.
    wait_until(|| dialer.replicas().contains(1), Duration::from_secs(2)).await;
    wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn acceptor_refuses_incompatible_build_with_and_without_auth() {
    for authenticated in [false, true] {
        let acceptor = Rc::new(IggyMessageBus::new(0));
        let addr =
            spawn_acceptor(&acceptor, authenticated.then(|| ReplicaAuth::new(SECRET_A))).await;
        let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
        let nonce = auth::random_nonce().expect("nonce");
        let hello = build_hello(CLUSTER, 0, authenticated.then_some(&nonce)).transmute_header(
            |old, header: &mut GenericHeader| {
                *header = old;
                header.reserved_command[auth::IDENTITY_OFFSET] ^= 1;
            },
        );
        framing::write_message(&mut stream, hello)
            .await
            .expect("write hello");
        let response = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
            .await
            .expect("read refusal");
        assert_eq!(
            auth::read_status(&response.header().reserved_command),
            HandshakeStatus::IncompatibleBuild
        );
        assert!(!acceptor.replicas().contains(0));
        acceptor.shutdown(Duration::from_secs(2)).await;
    }
}

#[compio::test]
async fn dialer_refuses_incompatible_build_with_and_without_auth() {
    for authenticated in [false, true] {
        let (listener, addr) = bind(loopback()).await.expect("bind");
        let responded = Rc::new(Cell::new(false));
        let observed = Rc::clone(&responded);
        let peer = compio::runtime::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let preamble = read_preamble(&mut stream, PREAMBLE_WAIT)
                .await
                .expect("read preamble");
            assert_eq!(
                Preamble::admit(preamble, 1, 2, PLACEMENT.links).expect("valid preamble"),
                LINK_0_OF_REPLICA_0
            );
            let hello = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
                .await
                .expect("read hello");
            assert_eq!(hello.header().command, Command::ReplicaHello);
            let challenge = build_raw(CLUSTER, 1, Command::ReplicaChallenge);
            framing::write_message(&mut stream, challenge)
                .await
                .expect("write incompatible challenge");
            observed.set(true);
        });
        let dialer = Rc::new(IggyMessageBus::new(0));
        spawn_dialer(
            &dialer,
            authenticated.then(|| ReplicaAuth::new(SECRET_A)),
            addr,
        )
        .await;
        settle().await;
        assert!(
            responded.get(),
            "dialer must have received the incompatible challenge"
        );
        assert!(!dialer.replicas().contains(1));
        peer.await.expect("peer task");
        dialer.shutdown(Duration::from_secs(2)).await;
    }
}

#[compio::test]
async fn wrong_key_rejects_peer() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_B))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer(&dialer, Some(ReplicaAuth::new(SECRET_A)), addr).await;

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
async fn rotation_window_registers_mid_roll_peers() {
    // Step-2 mid-roll of a PSK rotation: the acceptor already signs with the
    // new key, the dialer still signs with the old one, and each carries the
    // other key in its verify-only window. The mutual handshake must complete
    // in both directions.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(
        &acceptor,
        Some(ReplicaAuth::new(SECRET_B).with_previous_secret(SECRET_A)),
    )
    .await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer(
        &dialer,
        Some(ReplicaAuth::new(SECRET_A).with_previous_secret(SECRET_B)),
        addr,
    )
    .await;

    wait_until(|| dialer.replicas().contains(1), Duration::from_secs(2)).await;
    wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn enforcement_rejects_legacy_peer() {
    // Acceptor requires auth; dialer speaks the legacy (no-nonce) protocol.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;

    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer(&dialer, None, addr).await;

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
    // registering. Exercises the mid-handshake timeout path on the
    // (locally simulated) owning shard.
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, None).await;

    let short_grace = MessageBusConfig {
        handshake_grace: Duration::from_millis(200),
        ..MessageBusConfig::default()
    };
    let dialer = Rc::new(IggyMessageBus::with_tunables(0, short_grace));
    spawn_dialer(&dialer, Some(ReplicaAuth::new(SECRET_A)), addr).await;

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

    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    let nonce = auth::random_nonce().expect("nonce");
    framing::write_message(&mut stream, build_hello(0xDEAD, 0, Some(&nonce)))
        .await
        .expect("write hello");

    let resp = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read reject");
    assert_eq!(resp.header().command, Command::ReplicaChallenge);
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

    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
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

    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    let nonce = auth::random_nonce().expect("nonce");
    framing::write_message(&mut stream, build_hello(CLUSTER, 0, Some(&nonce)))
        .await
        .expect("write hello");

    let challenge = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read challenge");
    assert_eq!(challenge.header().command, Command::ReplicaChallenge);
    assert_eq!(
        auth::read_status(&challenge.header().reserved_command),
        HandshakeStatus::Ok,
    );

    // Wrong command in the finish slot (Prepare instead of ReplicaFinish).
    framing::write_message(&mut stream, build_raw(CLUSTER, 0, Command::Prepare))
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

#[compio::test]
async fn acceptor_refuses_a_hello_whose_placement_differs() {
    for (placement, status) in [
        (
            LinkPlacement {
                links: 2,
                ..PLACEMENT
            },
            HandshakeStatus::LinkCountMismatch,
        ),
        (
            LinkPlacement {
                link: 1,
                ..PLACEMENT
            },
            HandshakeStatus::PlacementMismatch,
        ),
    ] {
        let acceptor = Rc::new(IggyMessageBus::new(0));
        let addr = spawn_acceptor(&acceptor, None).await;
        let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
        let hello =
            build_hello(CLUSTER, 0, None).transmute_header(|old, header: &mut GenericHeader| {
                *header = old;
                placement.write(&mut header.reserved_command);
            });
        framing::write_message(&mut stream, hello)
            .await
            .expect("write hello");
        let response = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
            .await
            .expect("read refusal");
        assert_eq!(
            auth::read_status(&response.header().reserved_command),
            status,
            "{placement:?}"
        );
        assert!(!acceptor.replicas().contains(0));
        acceptor.shutdown(Duration::from_secs(2)).await;
    }
}

/// Shard 0 placed the link for the dialer its preamble named, so a hello
/// from another replica is refused even when that replica may dial.
#[compio::test]
async fn acceptor_refuses_a_hello_from_another_replica_than_its_preamble() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor_as(&acceptor, 2, 3, None).await;
    let mut stream = connect_link(addr, Preamble { dialer: 1, link: 0 }).await;
    framing::write_message(&mut stream, build_hello(CLUSTER, 0, None))
        .await
        .expect("write hello");
    let response = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read refusal");
    assert_eq!(
        auth::read_status(&response.header().reserved_command),
        HandshakeStatus::PlacementMismatch
    );
    assert!(!acceptor.replicas().contains(0));
    assert!(!acceptor.replicas().contains(1));
    acceptor.shutdown(Duration::from_secs(2)).await;
}

/// A dialer that sends no preamble, such as an older build, is closed by
/// shard 0 before any handshake frame is answered.
#[compio::test]
async fn acceptor_closes_a_link_without_a_preamble() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, None).await;
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    framing::write_message(&mut stream, build_hello(CLUSTER, 0, None))
        .await
        .expect("write hello");

    let res = framing::read_message(&mut stream, MAX_MESSAGE_SIZE).await;
    assert!(res.is_err(), "expected a close, got a response frame");
    assert!(!acceptor.replicas().contains(0));
    assert_eq!(acceptor.take_replica_link_stats().preamble_refusals, 1);
    acceptor.shutdown(Duration::from_secs(2)).await;
}

/// The preamble read on shard 0 and the handshake on the owner shard share
/// one `handshake_grace`, so a dialer that spends most of the grace before
/// its preamble and then sends no hello is closed one grace after it
/// connected, not two. Two windows would end at `PREAMBLE_DELAY` plus the
/// grace, past the bound below.
#[compio::test]
async fn acceptor_closes_a_slow_link_one_grace_after_connect() {
    const GRACE: Duration = Duration::from_secs(1);
    const PREAMBLE_DELAY: Duration = Duration::from_millis(800);
    let acceptor = Rc::new(IggyMessageBus::with_tunables(
        0,
        MessageBusConfig {
            handshake_grace: GRACE,
            ..MessageBusConfig::default()
        },
    ));
    let addr = spawn_acceptor(&acceptor, None).await;
    let connected = Instant::now();
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    compio::time::sleep(PREAMBLE_DELAY).await;
    write_preamble(&mut stream, LINK_0_OF_REPLICA_0)
        .await
        .expect("write preamble");

    let res = framing::read_message(&mut stream, MAX_MESSAGE_SIZE).await;
    let open_for = connected.elapsed();
    assert!(res.is_err(), "expected a close, got a response frame");
    assert!(
        open_for < GRACE + PREAMBLE_DELAY / 2,
        "link stayed open {open_for:?}, longer than one grace of {GRACE:?}"
    );
    assert!(!acceptor.replicas().contains(0));
    acceptor.shutdown(Duration::from_secs(2)).await;
}

/// Connect to `addr` and send `preamble`, the way the owner shard of a
/// dialer starts a link.
#[allow(clippy::future_not_send)]
async fn connect_link(addr: std::net::SocketAddr, preamble: Preamble) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    write_preamble(&mut stream, preamble)
        .await
        .expect("write preamble");
    stream
}

/// Build a raw `ReplicaHello` frame for link 0 for the wire-level reject
/// tests. With `nonce` set it opens the authenticated handshake
/// (`reserved_command[0..32]`); without, it is a legacy plaintext announce.
#[allow(clippy::cast_possible_truncation)]
fn build_hello(
    cluster_id: u128,
    replica_id: u8,
    nonce: Option<&[u8; auth::NONCE_LEN]>,
) -> Message<GenericHeader> {
    Message::<GenericHeader>::new(HEADER_SIZE).transmute_header(|_, h: &mut GenericHeader| {
        h.command = Command::ReplicaHello;
        h.cluster = cluster_id;
        h.replica = replica_id;
        h.size = HEADER_SIZE as u32;
        h.reserved_command[auth::IDENTITY_OFFSET..auth::IDENTITY_OFFSET + auth::IDENTITY_LEN]
            .copy_from_slice(&[0x5a; auth::IDENTITY_LEN]);
        if let Some(nonce) = nonce {
            h.reserved_command[..auth::NONCE_LEN].copy_from_slice(nonce);
        }
        PLACEMENT.write(&mut h.reserved_command);
    })
}

/// Build a raw frame with an arbitrary command for the wire-level tests (used to
/// send a wrong-command third frame in place of a `ReplicaFinish`).
#[allow(clippy::cast_possible_truncation)]
fn build_raw(cluster_id: u128, replica_id: u8, command: Command) -> Message<GenericHeader> {
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
