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
    TestRelease, install_dialed_replicas_locally, install_replicas_locally, loopback,
    release_identity, self_signed_replica_tls_ctx, set_replica_ctx, set_replica_ctx_of_release,
};
use compio::net::TcpStream;
use iggy_binary_protocol::{Command, GenericHeader, HEADER_SIZE};
use iggy_common::IggyError;
use message_bus::connector::start as start_connector;
use message_bus::framing::{self, MAX_MESSAGE_SIZE};
use message_bus::replica::auth::{
    self, HandshakeStatus, HeartbeatTimings, LinkPlacement, ReplicaAuth,
};
use message_bus::replica::handshake::MAX_COMPATIBLE_RELEASES;
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
const OLDER_RELEASE: &str = "1.0.0";
const NEWER_RELEASE: &str = "1.0.1";
const OTHER_RELEASE: &str = "0.9.0";

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
    spawn_listener(bus).await
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
    spawn_connector(bus, addr).await;
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
async fn releases_link_when_either_lists_the_other_on_every_channel() {
    for (authenticated, encrypted) in [(false, false), (true, false), (true, true)] {
        for (dialer_release, acceptor_release, dialer_lists, acceptor_lists) in [
            (OLDER_RELEASE, OLDER_RELEASE, &[][..], &[][..]),
            (NEWER_RELEASE, OLDER_RELEASE, &[OLDER_RELEASE], &[]),
            (OLDER_RELEASE, NEWER_RELEASE, &[], &[OLDER_RELEASE]),
            (
                NEWER_RELEASE,
                OLDER_RELEASE,
                &[OLDER_RELEASE],
                &[NEWER_RELEASE],
            ),
        ] {
            let acceptor = Rc::new(IggyMessageBus::new(0));
            set_replica_ctx_of_release(
                &acceptor,
                CLUSTER,
                1,
                2,
                authenticated.then(|| ReplicaAuth::new(SECRET_A)),
                encrypted.then(|| self_signed_replica_tls_ctx(2)),
                TestRelease {
                    release: acceptor_release,
                    compatible_with: acceptor_lists,
                },
            );
            let addr = spawn_listener(&acceptor).await;
            let dialer = Rc::new(IggyMessageBus::new(0));
            set_replica_ctx_of_release(
                &dialer,
                CLUSTER,
                0,
                2,
                authenticated.then(|| ReplicaAuth::new(SECRET_A)),
                encrypted.then(|| self_signed_replica_tls_ctx(2)),
                TestRelease {
                    release: dialer_release,
                    compatible_with: dialer_lists,
                },
            );
            spawn_connector(&dialer, addr).await;

            wait_until(|| dialer.replicas().contains(1), Duration::from_secs(2)).await;
            wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;
            assert_eq!(dialer.take_replica_link_stats().handshake_failures, 0);
            assert_eq!(acceptor.take_replica_link_stats().handshake_failures, 0);

            dialer.shutdown(Duration::from_secs(2)).await;
            acceptor.shutdown(Duration::from_secs(2)).await;
        }
    }
}

/// Two releases that list the same third release still refuse each other:
/// a row names the releases one release links with and implies nothing more.
#[compio::test]
async fn releases_that_do_not_list_each_other_refuse_the_link_with_and_without_auth() {
    for authenticated in [false, true] {
        let acceptor = Rc::new(IggyMessageBus::new(0));
        let addr = spawn_acceptor_of_release(
            &acceptor,
            authenticated.then(|| ReplicaAuth::new(SECRET_A)),
            OLDER_RELEASE,
            &[OTHER_RELEASE],
        )
        .await;
        let dialer = Rc::new(IggyMessageBus::new(0));
        spawn_dialer_of_release(
            &dialer,
            authenticated.then(|| ReplicaAuth::new(SECRET_A)),
            addr,
            NEWER_RELEASE,
            &[OTHER_RELEASE],
        )
        .await;

        wait_until(
            || dialer.take_replica_link_stats().handshake_failures > 0,
            PREAMBLE_WAIT,
        )
        .await;
        wait_until(
            || acceptor.take_replica_link_stats().handshake_failures > 0,
            PREAMBLE_WAIT,
        )
        .await;
        assert!(!dialer.replicas().contains(1));
        assert!(!acceptor.replicas().contains(0));

        dialer.shutdown(Duration::from_secs(2)).await;
        acceptor.shutdown(Duration::from_secs(2)).await;
    }
}

/// The acceptor answers a release it lists under the identity of that
/// release, the only one a node without the row accepts.
#[compio::test]
async fn acceptor_answers_a_release_it_lists_under_the_identity_of_that_release() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor_of_release(&acceptor, None, NEWER_RELEASE, &[OLDER_RELEASE]).await;
    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    framing::write_message(
        &mut stream,
        build_hello_of_release(
            release_identity(OLDER_RELEASE),
            &release_identity(NEWER_RELEASE),
        ),
    )
    .await
    .expect("write hello");

    let challenge = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read challenge");

    assert_eq!(
        auth::read_status(&challenge.header().reserved_command),
        HandshakeStatus::Ok
    );
    assert_eq!(
        auth::read_identity(&challenge.header().reserved_command),
        release_identity(OLDER_RELEASE)
    );
    wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

/// A hello from another release that lists the release of the acceptor is
/// answered under the identity of the acceptor.
#[compio::test]
async fn acceptor_answers_under_its_own_identity_when_the_hello_lists_its_release() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor_of_release(&acceptor, None, OLDER_RELEASE, &[]).await;
    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    framing::write_message(
        &mut stream,
        build_hello_of_release(
            release_identity(NEWER_RELEASE),
            &[
                release_identity(OTHER_RELEASE),
                release_identity(OLDER_RELEASE),
            ]
            .concat(),
        ),
    )
    .await
    .expect("write hello");

    let challenge = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read challenge");

    assert_eq!(
        auth::read_status(&challenge.header().reserved_command),
        HandshakeStatus::Ok
    );
    assert_eq!(
        auth::read_identity(&challenge.header().reserved_command),
        release_identity(OLDER_RELEASE)
    );
    wait_until(|| acceptor.replicas().contains(0), Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn acceptor_refuses_a_hello_whose_listed_identities_are_cut_short() {
    for release in [OLDER_RELEASE, NEWER_RELEASE, OTHER_RELEASE] {
        let acceptor = Rc::new(IggyMessageBus::new(0));
        let addr =
            spawn_acceptor_of_release(&acceptor, None, OLDER_RELEASE, &[OTHER_RELEASE]).await;
        let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
        let mut listed = release_identity(OLDER_RELEASE).to_vec();
        listed.push(0);
        framing::write_message(
            &mut stream,
            build_hello_of_release(release_identity(release), &listed),
        )
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
async fn acceptor_enforces_the_hello_identity_limit() {
    for count in [MAX_COMPATIBLE_RELEASES, MAX_COMPATIBLE_RELEASES + 1] {
        let acceptor = Rc::new(IggyMessageBus::with_tunables(
            0,
            MessageBusConfig {
                max_message_size: 2048,
                ..MessageBusConfig::default()
            },
        ));
        let addr = spawn_acceptor_of_release(&acceptor, None, OLDER_RELEASE, &[]).await;
        let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
        let listed = release_identity(OLDER_RELEASE).repeat(count);
        framing::write_message(
            &mut stream,
            build_hello_of_release(release_identity(NEWER_RELEASE), &listed),
        )
        .await
        .expect("write hello");
        let response = framing::read_message(&mut stream, MAX_MESSAGE_SIZE).await;
        if count == MAX_COMPATIBLE_RELEASES {
            let challenge = response.expect("read challenge");
            assert_eq!(
                auth::read_status(&challenge.header().reserved_command),
                HandshakeStatus::Ok
            );
            assert_eq!(
                auth::read_identity(&challenge.header().reserved_command),
                release_identity(OLDER_RELEASE)
            );
        } else {
            assert!(response.is_err());
            assert!(!acceptor.replicas().contains(0));
        }
        acceptor.shutdown(PREAMBLE_WAIT).await;
    }
}

/// The hello carries the identity of the dialer in its header and the
/// identities of the releases it lists as its body. The dialer then links
/// under a listed identity the acceptor answers with, and under no other.
#[compio::test]
async fn dialer_sends_the_identities_it_lists_and_links_under_one_of_them() {
    for (answered_release, links) in [(OLDER_RELEASE, true), ("1.0.2", false)] {
        let (listener, addr) = bind(loopback()).await.expect("bind");
        let peer = compio::runtime::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            read_preamble(&mut stream, PREAMBLE_WAIT)
                .await
                .expect("read preamble");
            let hello = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
                .await
                .expect("read hello");
            let challenge = build_raw(CLUSTER, 1, Command::ReplicaChallenge).transmute_header(
                |old, header: &mut GenericHeader| {
                    *header = old;
                    header.reserved_command
                        [auth::IDENTITY_OFFSET..auth::IDENTITY_OFFSET + auth::IDENTITY_LEN]
                        .copy_from_slice(&release_identity(answered_release));
                },
            );
            framing::write_message(&mut stream, challenge)
                .await
                .expect("write challenge");
            (hello, stream)
        });
        let dialer = Rc::new(IggyMessageBus::new(0));
        spawn_dialer_of_release(
            &dialer,
            None,
            addr,
            NEWER_RELEASE,
            &[OLDER_RELEASE, OTHER_RELEASE],
        )
        .await;

        let (hello, _held_link) = peer.await.expect("peer task");

        assert_eq!(
            auth::read_identity(&hello.header().reserved_command),
            release_identity(NEWER_RELEASE)
        );
        assert_eq!(
            hello.body(),
            [
                release_identity(OLDER_RELEASE),
                release_identity(OTHER_RELEASE),
            ]
            .concat()
        );
        if links {
            wait_until(|| dialer.replicas().contains(1), Duration::from_secs(2)).await;
        } else {
            wait_until(
                || dialer.take_replica_link_stats().handshake_failures > 0,
                PREAMBLE_WAIT,
            )
            .await;
            assert!(!dialer.replicas().contains(1), "{answered_release}");
        }
        dialer.shutdown(Duration::from_secs(2)).await;
    }
}

#[compio::test]
async fn dialer_rejects_a_challenge_with_a_body() {
    let (listener, addr) = bind(loopback()).await.unwrap();
    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer(&dialer, None, addr).await;
    let (mut stream, _) = listener.accept().await.unwrap();
    read_preamble(&mut stream, PREAMBLE_WAIT).await.unwrap();
    let hello = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .unwrap();
    assert_eq!(hello.header().command, Command::ReplicaHello);
    let mut fields = [0; iggy_binary_protocol::RESERVED_COMMAND_LEN];
    fields[auth::IDENTITY_OFFSET..auth::IDENTITY_OFFSET + auth::IDENTITY_LEN]
        .copy_from_slice(&auth::read_identity(&hello.header().reserved_command));
    let challenge = handshake_frame_with_body(Command::ReplicaChallenge, 1, fields);
    framing::write_message(&mut stream, challenge)
        .await
        .unwrap();

    wait_until(
        || dialer.take_replica_link_stats().handshake_failures > 0,
        PREAMBLE_WAIT,
    )
    .await;
    assert_handshake_closed(&mut stream).await;
    assert!(!dialer.replicas().contains(1));
    dialer.shutdown(PREAMBLE_WAIT).await;
}

#[compio::test]
async fn acceptor_rejects_an_authenticated_finish_with_a_body() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, Some(ReplicaAuth::new(SECRET_A))).await;
    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    let nonce_d = auth::random_nonce().unwrap();
    let hello = build_hello(CLUSTER, 0, Some(&nonce_d));
    let hello_hash = blake3::hash(hello.as_slice());
    framing::write_message(&mut stream, hello).await.unwrap();
    let challenge = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .unwrap();
    let reserved = &challenge.header().reserved_command;
    assert_eq!(auth::read_status(reserved), HandshakeStatus::Ok);
    let transcript = auth::Transcript {
        hello_hash,
        binary_identity: auth::read_identity(reserved),
        placement: PLACEMENT,
        cluster_id: CLUSTER,
        dialer_id: 0,
        acceptor_id: 1,
        nonce_d,
        nonce_a: auth::read_nonce(reserved),
        binding: auth::ChannelBinding::Plaintext,
    };
    let mut finish_fields = [0; iggy_binary_protocol::RESERVED_COMMAND_LEN];
    finish_fields[auth::IDENTITY_OFFSET..auth::IDENTITY_OFFSET + auth::IDENTITY_LEN]
        .copy_from_slice(&transcript.binary_identity);
    finish_fields[auth::NONCE_LEN..auth::NONCE_LEN + auth::MAC_LEN]
        .copy_from_slice(&ReplicaAuth::new(SECRET_A).dialer_mac(&transcript));
    let finish = handshake_frame_with_body(Command::ReplicaFinish, 0, finish_fields);
    framing::write_message(&mut stream, finish).await.unwrap();

    assert_handshake_closed(&mut stream).await;
    assert!(!acceptor.replicas().contains(0));
    acceptor.shutdown(PREAMBLE_WAIT).await;
}

#[test]
#[should_panic(expected = "release compatibility row exceeds the replica hello limit")]
fn oversized_release_row_is_rejected_at_installation() {
    let bus = IggyMessageBus::new(0);
    set_replica_ctx_of_release(
        &bus,
        CLUSTER,
        1,
        2,
        None,
        None,
        TestRelease {
            release: NEWER_RELEASE,
            compatible_with: &[OLDER_RELEASE; MAX_COMPATIBLE_RELEASES + 1],
        },
    );
}

/// A relay between two authenticated nodes drops one of the identities the
/// dialer lists. The acceptor still finds its own identity there and answers,
/// but its MAC covers the hello it received, so the dialer refuses it.
#[compio::test]
#[allow(clippy::cast_possible_truncation)]
async fn authenticated_handshake_fails_when_a_relay_alters_the_hello() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let acceptor_addr = spawn_acceptor_of_release(
        &acceptor,
        Some(ReplicaAuth::new(SECRET_A)),
        OLDER_RELEASE,
        &[],
    )
    .await;
    let (listener, relay_addr) = bind(loopback()).await.expect("bind relay");
    let relay = compio::runtime::spawn(async move {
        let (mut dialer_leg, _) = listener.accept().await.expect("accept");
        read_preamble(&mut dialer_leg, PREAMBLE_WAIT)
            .await
            .expect("read preamble");
        let mut acceptor_leg = connect_link(acceptor_addr, LINK_0_OF_REPLICA_0).await;
        let hello = framing::read_message(&mut dialer_leg, MAX_MESSAGE_SIZE)
            .await
            .expect("read hello");
        let kept = &hello.body()[..auth::IDENTITY_LEN];
        let size = HEADER_SIZE + kept.len();
        let mut altered = Message::<GenericHeader>::new(size).transmute_header(
            |_, header: &mut GenericHeader| {
                *header = *hello.header();
                header.size = size as u32;
            },
        );
        altered.as_mut_slice()[HEADER_SIZE..].copy_from_slice(kept);
        framing::write_message(&mut acceptor_leg, altered)
            .await
            .expect("forward the altered hello");
        let challenge = framing::read_message(&mut acceptor_leg, MAX_MESSAGE_SIZE)
            .await
            .expect("read challenge");
        let status = auth::read_status(&challenge.header().reserved_command);
        framing::write_message(&mut dialer_leg, challenge)
            .await
            .expect("forward challenge");
        let finish = framing::read_message(&mut dialer_leg, MAX_MESSAGE_SIZE).await;
        (status, finish.is_err())
    });
    let dialer = Rc::new(IggyMessageBus::new(0));
    spawn_dialer_of_release(
        &dialer,
        Some(ReplicaAuth::new(SECRET_A)),
        relay_addr,
        NEWER_RELEASE,
        &[OLDER_RELEASE, OTHER_RELEASE],
    )
    .await;

    let (status, dialer_closed) = relay.await.expect("relay task");

    assert_eq!(
        status,
        HandshakeStatus::Ok,
        "the acceptor must answer the altered hello, or this proves nothing"
    );
    assert!(
        dialer_closed,
        "the dialer answered a challenge for a hello it did not send"
    );
    assert!(!dialer.replicas().contains(1));
    assert!(!acceptor.replicas().contains(0));

    dialer.shutdown(Duration::from_secs(2)).await;
    acceptor.shutdown(Duration::from_secs(2)).await;
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

/// A node with another heartbeat value is refused. The refusal carries the
/// values of the acceptor, so the dialer names the option that differs.
#[compio::test]
async fn acceptor_refuses_a_hello_whose_heartbeat_values_differ() {
    let acceptor = Rc::new(IggyMessageBus::new(0));
    let addr = spawn_acceptor(&acceptor, None).await;
    let mut stream = connect_link(addr, LINK_0_OF_REPLICA_0).await;
    let dialer_timings = HeartbeatTimings {
        busy_timeout_ms: 6_000,
        ..HeartbeatTimings::default()
    };
    let hello =
        build_hello(CLUSTER, 0, None).transmute_header(|old, header: &mut GenericHeader| {
            *header = old;
            dialer_timings.write(&mut header.reserved_command);
        });
    framing::write_message(&mut stream, hello)
        .await
        .expect("write hello");

    let response = framing::read_message(&mut stream, MAX_MESSAGE_SIZE)
        .await
        .expect("read refusal");

    assert_eq!(
        auth::read_status(&response.header().reserved_command),
        HandshakeStatus::HeartbeatMismatch
    );
    let mismatch = dialer_timings
        .first_mismatch(HeartbeatTimings::read(&response.header().reserved_command))
        .expect("the refusal carries other values");
    assert_eq!(mismatch.option, "cluster.commit_busy_heartbeat_timeout");
    assert!(
        mismatch
            .to_string()
            .contains("cluster.commit_busy_heartbeat_timeout")
    );
    assert!(!acceptor.replicas().contains(0));
    acceptor.shutdown(Duration::from_secs(2)).await;
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

/// [`spawn_acceptor`] for a node that runs `release` and lists
/// `compatible_with`.
#[allow(clippy::future_not_send)]
async fn spawn_acceptor_of_release(
    bus: &Rc<IggyMessageBus>,
    auth: Option<ReplicaAuth>,
    release: &str,
    compatible_with: &[&str],
) -> std::net::SocketAddr {
    set_replica_ctx_of_release(
        bus,
        CLUSTER,
        1,
        2,
        auth,
        None,
        TestRelease {
            release,
            compatible_with,
        },
    );
    spawn_listener(bus).await
}

/// [`spawn_dialer`] for a node that runs `release` and lists
/// `compatible_with`.
#[allow(clippy::future_not_send)]
async fn spawn_dialer_of_release(
    bus: &Rc<IggyMessageBus>,
    auth: Option<ReplicaAuth>,
    addr: std::net::SocketAddr,
    release: &str,
    compatible_with: &[&str],
) {
    set_replica_ctx_of_release(
        bus,
        CLUSTER,
        0,
        2,
        auth,
        None,
        TestRelease {
            release,
            compatible_with,
        },
    );
    spawn_connector(bus, addr).await;
}

/// Run the inbound listener on `bus`, whose handshake context
/// is already set, and return its address.
#[allow(clippy::future_not_send)]
async fn spawn_listener(bus: &Rc<IggyMessageBus>) -> std::net::SocketAddr {
    let (listener, addr) = bind(loopback()).await.expect("bind acceptor");
    let token = bus.token();
    let delegate = install_replicas_locally(bus.clone(), noop_handler());
    let handle = compio::runtime::spawn(async move {
        run(listener, token, delegate).await;
    });
    bus.track_background(handle);
    addr
}

/// Start the connector of replica 0 on `bus`, whose handshake context is
/// already set, toward replica 1 at `addr`.
#[allow(clippy::future_not_send)]
async fn spawn_connector(bus: &Rc<IggyMessageBus>, addr: std::net::SocketAddr) {
    start_connector(
        bus,
        0,
        vec![(1, addr)],
        install_dialed_replicas_locally(bus.clone(), noop_handler()),
        LONG_PERIOD,
    )
    .await;
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

/// Build a raw unauthenticated `ReplicaHello` of replica 0 for link 0 that
/// carries `identity` in its header and `listed`, the identities of the
/// releases its sender lists, as its body.
#[allow(clippy::cast_possible_truncation)]
fn build_hello_of_release(
    identity: [u8; auth::IDENTITY_LEN],
    listed: &[u8],
) -> Message<GenericHeader> {
    let size = HEADER_SIZE + listed.len();
    let mut hello =
        Message::<GenericHeader>::new(size).transmute_header(|_, h: &mut GenericHeader| {
            h.command = Command::ReplicaHello;
            h.cluster = CLUSTER;
            h.replica = 0;
            h.size = size as u32;
            h.reserved_command[auth::IDENTITY_OFFSET..auth::IDENTITY_OFFSET + auth::IDENTITY_LEN]
                .copy_from_slice(&identity);
            PLACEMENT.write(&mut h.reserved_command);
        });
    hello.as_mut_slice()[HEADER_SIZE..].copy_from_slice(listed);
    hello
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

fn handshake_frame_with_body(
    command: Command,
    replica: u8,
    reserved: [u8; iggy_binary_protocol::RESERVED_COMMAND_LEN],
) -> Message<GenericHeader> {
    let size = HEADER_SIZE + 32;
    Message::<GenericHeader>::new(size).transmute_header(|_, header: &mut GenericHeader| {
        header.command = command;
        header.replica = replica;
        header.cluster = CLUSTER;
        header.size = u32::try_from(size).unwrap();
        header.reserved_command = reserved;
    })
}

#[allow(clippy::future_not_send)]
async fn assert_handshake_closed(stream: &mut TcpStream) {
    let response = compio::time::timeout(
        PREAMBLE_WAIT,
        framing::read_message(stream, MAX_MESSAGE_SIZE),
    )
    .await
    .expect("peer closed before deadline");
    assert!(matches!(
        response,
        Err(IggyError::ConnectionClosed | IggyError::TcpError)
    ));
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
