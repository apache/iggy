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

//! `replica_io::start_on_shard_zero` only wires TCP I/O when the bus runs
//! on shard 0. Non-zero shards keep a working bus instance but never bind
//! a listener, never dial peers, and never register background tasks.

mod common;

use common::{
    install_clients_locally, install_quic_clients_locally, install_replicas_locally,
    install_ws_clients_locally, loopback, test_token_source,
};
use message_bus::IggyMessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::connector::DEFAULT_RECONNECT_PERIOD;
use message_bus::replica_io::{QuicServerCredentials, start_on_shard_zero};
use message_bus::replica_listener::{MessageHandler, bind, run};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xF00D;

#[compio::test]
async fn shard_zero_binds_listener_and_starts_connector() {
    // Peer (replica 1) stands up its own bus + inbound listener so the
    // directional dial from replica 0 succeeds.
    let peer_bus = Rc::new(IggyMessageBus::new(0));
    let peer_handler: MessageHandler = Rc::new(|_, _| {});
    let (peer_listener, peer_addr) = bind(loopback()).await.expect("bind peer listener");
    let peer_token = peer_bus.token();
    let peer_nonces = peer_bus.replica_nonces();
    let peer_accept = install_replicas_locally(peer_bus.clone(), peer_handler.clone());
    let peer_listen_handle = compio::runtime::spawn(async move {
        run(
            peer_listener,
            peer_token,
            CLUSTER,
            1,
            2,
            peer_accept,
            message_bus::framing::MAX_MESSAGE_SIZE,
            test_token_source(),
            peer_nonces,
        )
        .await;
    });
    peer_bus.track_background(peer_listen_handle);

    // Shard 0 bus under test. For this gating test we wire the
    // install-locally delegates so shard 0 registers inbound / outbound
    // peers on its own registry (mimicking a single-shard deployment).
    let bus_zero = Rc::new(IggyMessageBus::new(0));
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let accepted_replica = install_replicas_locally(bus_zero.clone(), on_message.clone());
    let accepted_client = install_clients_locally(bus_zero.clone(), on_request);

    let bound = start_on_shard_zero(
        &bus_zero,
        loopback(),
        loopback(),
        None,
        None,
        None,
        CLUSTER,
        0,
        2,
        vec![(1u8, peer_addr)],
        accepted_replica,
        accepted_client,
        None,
        None,
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await
    .expect("start_on_shard_zero must succeed on shard 0");

    let bound = bound.expect("shard 0 must return bound listeners");
    let replica_addr: SocketAddr = bound.replica;
    let client_addr: SocketAddr = bound.client;
    assert_ne!(replica_addr.port(), 0, "replica port must be assigned");
    assert_ne!(client_addr.port(), 0, "client port must be assigned");
    assert_ne!(
        replica_addr.port(),
        client_addr.port(),
        "replica and client must bind distinct ports"
    );

    // Wait for the outbound dial to replica 1 to land.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus_zero.replicas().contains(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "shard 0 did not register the outbound connection to peer 1"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(bus_zero.replicas().len(), 1);

    bus_zero.shutdown(Duration::from_secs(2)).await;
    peer_bus.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn non_zero_shard_skips_io() {
    let bus_one = Rc::new(IggyMessageBus::new(1));
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let accepted_replica = install_replicas_locally(bus_one.clone(), on_message);
    let accepted_client = install_clients_locally(bus_one.clone(), on_request);

    let dead_peer: SocketAddr = "127.0.0.1:1".parse().unwrap();

    let outcome = start_on_shard_zero(
        &bus_one,
        loopback(),
        loopback(),
        None,
        None,
        None,
        CLUSTER,
        1,
        2,
        vec![(0u8, dead_peer)],
        accepted_replica,
        accepted_client,
        None,
        None,
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await
    .expect("start_on_shard_zero must succeed on non-zero shard (no-op)");

    assert!(outcome.is_none(), "non-zero shard must not bind a listener");
    assert_eq!(bus_one.replicas().len(), 0);
    assert_eq!(bus_one.clients().len(), 0);

    let drained = bus_one.shutdown(Duration::from_millis(100)).await;
    assert_eq!(drained.clean, 0, "no peer entries should have been drained");
    assert_eq!(
        drained.force, 0,
        "no peer entries should have been force-cancelled"
    );
    assert_eq!(
        drained.background_clean, 0,
        "helper must not register any background tasks on non-zero shards"
    );
    assert_eq!(
        drained.background_force, 0,
        "helper must not register any background tasks on non-zero shards"
    );
}

fn self_signed() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).expect("rcgen");
    let cert_der = CertificateDer::from(cert.cert);
    let key_der: PrivateKeyDer<'static> =
        PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into();
    (cert_der, key_der)
}

#[compio::test]
async fn shard_zero_binds_all_four_planes_when_configured() {
    // Idempotent across same-process retries.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let bus_zero = Rc::new(IggyMessageBus::new(0));
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let accepted_replica = install_replicas_locally(bus_zero.clone(), on_message.clone());
    let accepted_client = install_clients_locally(bus_zero.clone(), on_request.clone());
    let accepted_ws = install_ws_clients_locally(bus_zero.clone(), on_request.clone());
    let accepted_quic = install_quic_clients_locally(bus_zero.clone(), on_request);

    let (cert, key) = self_signed();
    let creds = QuicServerCredentials {
        cert_chain: vec![cert],
        key_der: key,
    };

    let bound = start_on_shard_zero(
        &bus_zero,
        loopback(),
        loopback(),
        Some(loopback()),
        Some(loopback()),
        Some(creds),
        CLUSTER,
        0,
        1,
        vec![],
        accepted_replica,
        accepted_client,
        Some(accepted_ws),
        Some(accepted_quic),
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await
    .expect("start_on_shard_zero must succeed");

    let bound = bound.expect("shard 0 must return bound listeners");
    assert_ne!(bound.replica.port(), 0, "replica must bind a port");
    assert_ne!(bound.client.port(), 0, "client must bind a port");
    let ws = bound.ws.expect("ws plane must be bound");
    let quic = bound.quic.expect("quic plane must be bound");
    assert_ne!(ws.port(), 0, "ws must bind a port");
    assert_ne!(quic.port(), 0, "quic must bind a port");

    // Ports must be pairwise distinct on the TCP-family slots.
    let tcp_ports = [bound.replica.port(), bound.client.port(), ws.port()];
    for i in 0..tcp_ports.len() {
        for j in (i + 1)..tcp_ports.len() {
            assert_ne!(
                tcp_ports[i], tcp_ports[j],
                "TCP-family listeners must use distinct ports",
            );
        }
    }

    bus_zero.shutdown(Duration::from_secs(2)).await;
}
