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

//! MITM-relay regression for the PSK channel binding.
//!
//! In `cluster.tls.self_signed` mode the dialer accepts ANY certificate,
//! so an on-path attacker can terminate TLS on both legs and pipe the
//! plaintext through. The PSK handshake MACs are bound to the TLS
//! exporter of the session they ran over; the relay's two legs are two
//! distinct sessions with two distinct exporters, so the dialer's
//! verification of `mac_a` fails and neither side installs the peer.
//! If the binding ever regresses to nothing (or to a value the relay
//! can reproduce), this test fails by observing a successful install.

mod common;

use common::{
    install_dialed_replicas_locally, install_replicas_locally, loopback,
    self_signed_replica_tls_ctx, set_replica_ctx_with_tls,
};
use compio::io::compat::AsyncStream;
use compio::net::{TcpListener, TcpStream};
use futures::AsyncReadExt;
use message_bus::IggyMessageBus;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica::auth::ReplicaAuth;
use message_bus::replica::handshake::ReplicaTlsCtx;
use message_bus::replica::listener::{MessageHandler, bind, run};
use rustls::pki_types::ServerName;
use std::cell::Cell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0x4174;
const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

#[compio::test]
async fn tls_relay_mitm_is_rejected_by_channel_binding() {
    // Victim acceptor: replica 1, TLS self-signed + PSK auth.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx_with_tls(
        &bus1,
        CLUSTER,
        1,
        2,
        Some(ReplicaAuth::new(SECRET)),
        self_signed_replica_tls_ctx(2),
    );
    let on_message1: MessageHandler = Rc::new(|_, _| {});
    let (l1, addr1) = bind(loopback()).await.expect("bind acceptor");
    let token_for_l1 = bus1.token();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1);
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate_1).await;
    });
    bus1.track_background(l1_handle);

    // Attacker relay: terminates TLS towards the dialer with its OWN
    // throwaway certificate (the accept-any verifier swallows it),
    // dials the real acceptor as an accept-any TLS client, and pipes
    // the decrypted plaintext between the legs.
    let relay_listener = TcpListener::bind(loopback()).await.expect("bind relay");
    let relay_addr: SocketAddr = relay_listener.local_addr().expect("relay addr");
    let relay_tls = self_signed_replica_tls_ctx(2);
    let relayed_sessions = Rc::new(Cell::new(0u32));
    let relayed_for_task = Rc::clone(&relayed_sessions);
    let relay_handle = compio::runtime::spawn(async move {
        run_relay(relay_listener, addr1, relay_tls, relayed_for_task).await;
    });

    // Victim dialer: replica 0 pointed at the relay instead of the peer.
    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx_with_tls(
        &bus0,
        CLUSTER,
        0,
        2,
        Some(ReplicaAuth::new(SECRET)),
        self_signed_replica_tls_ctx(2),
    );
    let on_message0: MessageHandler = Rc::new(|_, _| {});
    let dial_delegate_0 = install_dialed_replicas_locally(bus0.clone(), on_message0);
    start_connector(
        &bus0,
        0,
        vec![(1, relay_addr)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    // Both TLS legs succeed and the PSK frames flow through the relay;
    // only the exporter mismatch stops the handshake. Give it ample
    // time to (wrongly) install before asserting it did not.
    compio::time::sleep(Duration::from_millis(700)).await;
    assert!(
        relayed_sessions.get() >= 1,
        "relay never established both TLS legs; the test would pass vacuously"
    );
    assert!(
        !bus0.replicas().contains(1),
        "dialer installed through a TLS relay MITM: channel binding is broken"
    );
    assert!(
        !bus1.replicas().contains(0),
        "acceptor installed through a TLS relay MITM: channel binding is broken"
    );

    drop(relay_handle);
    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}

/// Accept one connection at a time, terminate TLS on both legs, and
/// shuttle plaintext between them until either leg closes. Bumps
/// `relayed` once both legs are established (the attack reached the
/// PSK layer).
#[allow(clippy::future_not_send)]
async fn run_relay(
    listener: TcpListener,
    target: SocketAddr,
    tls: Rc<ReplicaTlsCtx>,
    relayed: Rc<Cell<u32>>,
) {
    loop {
        let Ok((inbound, _)) = listener.accept().await else {
            return;
        };
        let Ok(outbound) = TcpStream::connect(target).await else {
            return;
        };
        let Ok(victim_leg) = futures_rustls::TlsAcceptor::from(tls.server.clone())
            .accept(Box::pin(AsyncStream::new(inbound)))
            .await
        else {
            continue;
        };
        let server_name: ServerName<'static> = "localhost".try_into().expect("server name");
        let Ok(target_leg) = futures_rustls::TlsConnector::from(tls.client.clone())
            .connect(server_name, Box::pin(AsyncStream::new(outbound)))
            .await
        else {
            continue;
        };
        relayed.set(relayed.get() + 1);

        let (victim_read, mut victim_write) = victim_leg.split();
        let (target_read, mut target_write) = target_leg.split();
        let forward = futures::io::copy(victim_read, &mut target_write);
        let backward = futures::io::copy(target_read, &mut victim_write);
        let _ = futures::join!(forward, backward);
    }
}
