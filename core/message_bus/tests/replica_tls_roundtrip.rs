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

//! Replica-to-replica traffic over TLS-wrapped connections, in the
//! production `cluster.tls.self_signed` shape (per-node throwaway
//! certificate, accept-any dialer verifier, PSK auth on). The dialer
//! and acceptor each run the TLS wrap plus the PSK handshake on their
//! owning shard, then the consensus frames flow through the TLS pump.

mod common;

use async_channel::Receiver;
use common::{
    header_only, install_dialed_replicas_locally, install_replicas_locally, loopback,
    self_signed_replica_tls_ctx, set_replica_ctx, set_replica_ctx_with_tls,
};
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica::auth::ReplicaAuth;
use message_bus::replica::listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0x715;
const SECRET: &[u8] = b"0123456789abcdef0123456789abcdef";

#[compio::test]
async fn two_replicas_exchange_prepare_and_ack_over_tls() {
    // Replica 1: inbound listener; TLS acceptor + PSK acceptor half run
    // on the (test-local) owning shard.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx_with_tls(
        &bus1,
        CLUSTER,
        1,
        2,
        Some(ReplicaAuth::new(SECRET)),
        self_signed_replica_tls_ctx(2),
    );
    let (tx1, rx1) = async_channel::bounded::<u8>(8);
    let on_message1: MessageHandler = Rc::new(move |peer, msg| {
        tx1.try_send(msg.header().command as u8).ok();
        let _ = peer;
    });
    let (l1, addr1) = bind(loopback()).await.expect("bind r1");

    let token_for_l1 = bus1.token();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1.clone());
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate_1).await;
    });
    bus1.track_background(l1_handle);

    // Replica 0: outbound connector dials replica 1 with its own
    // (distinct) self-signed certificate.
    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx_with_tls(
        &bus0,
        CLUSTER,
        0,
        2,
        Some(ReplicaAuth::new(SECRET)),
        self_signed_replica_tls_ctx(2),
    );
    let (tx0, rx0) = async_channel::bounded::<u8>(8);
    let on_message0: MessageHandler = Rc::new(move |peer, msg| {
        tx0.try_send(msg.header().command as u8).ok();
        let _ = peer;
    });
    let dial_delegate_0 = install_dialed_replicas_locally(bus0.clone(), on_message0);

    start_connector(
        &bus0,
        0,
        vec![(1, addr1)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    wait_until(|| bus0.replicas().contains(1), Duration::from_secs(2)).await;

    let prepare = header_only(Command2::Prepare, CLUSTER, 0);
    bus0.send_to_replica(1, prepare.into_frozen())
        .await
        .expect("send prepare");

    let cmd = expect_recv(&rx1, Duration::from_secs(2)).await;
    assert_eq!(cmd, Command2::Prepare as u8);

    let ack = header_only(Command2::PrepareOk, CLUSTER, 1);
    bus1.send_to_replica(0, ack.into_frozen())
        .await
        .expect("send ack");

    let cmd = expect_recv(&rx0, Duration::from_secs(2)).await;
    assert_eq!(cmd, Command2::PrepareOk as u8);

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn tls_dialer_against_plaintext_acceptor_never_installs() {
    // Mixed-mode misconfiguration: the acceptor speaks plaintext, the
    // dialer wraps in TLS. The acceptor reads the ClientHello as a
    // garbage frame and drops the connection; the dialer's handshake
    // fails. Neither side may register the peer.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx(&bus1, CLUSTER, 1, 2, None);
    let on_message1: MessageHandler = Rc::new(|_, _| {});
    let (l1, addr1) = bind(loopback()).await.expect("bind r1");

    let token_for_l1 = bus1.token();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1);
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate_1).await;
    });
    bus1.track_background(l1_handle);

    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx_with_tls(&bus0, CLUSTER, 0, 2, None, self_signed_replica_tls_ctx(2));
    let on_message0: MessageHandler = Rc::new(|_, _| {});
    let dial_delegate_0 = install_dialed_replicas_locally(bus0.clone(), on_message0);

    start_connector(
        &bus0,
        0,
        vec![(1, addr1)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    compio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !bus0.replicas().contains(1),
        "TLS dialer must not install against a plaintext acceptor"
    );
    assert!(
        !bus1.replicas().contains(0),
        "plaintext acceptor must not install a TLS dialer"
    );

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn plaintext_dialer_against_tls_acceptor_never_installs() {
    // Reverse mixed-mode: the acceptor expects a TLS ClientHello but
    // receives a plaintext ReplicaHello frame; the TLS accept fails and
    // the connection drops. The authenticated dialer then fails its
    // challenge read. Neither side may register the peer.
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
    let (l1, addr1) = bind(loopback()).await.expect("bind r1");

    let token_for_l1 = bus1.token();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1);
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate_1).await;
    });
    bus1.track_background(l1_handle);

    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx(&bus0, CLUSTER, 0, 2, Some(ReplicaAuth::new(SECRET)));
    let on_message0: MessageHandler = Rc::new(|_, _| {});
    let dial_delegate_0 = install_dialed_replicas_locally(bus0.clone(), on_message0);

    start_connector(
        &bus0,
        0,
        vec![(1, addr1)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    compio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !bus0.replicas().contains(1),
        "plaintext dialer must not install against a TLS acceptor"
    );
    assert!(
        !bus1.replicas().contains(0),
        "TLS acceptor must not install a plaintext dialer"
    );

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}

#[allow(clippy::future_not_send)]
async fn wait_until<F: Fn() -> bool>(cond: F, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("wait_until: condition not met within {timeout:?}");
}

#[allow(clippy::future_not_send)]
async fn expect_recv(rx: &Receiver<u8>, timeout: Duration) -> u8 {
    compio::time::timeout(timeout, rx.recv())
        .await
        .expect("expect_recv: timeout")
        .expect("expect_recv: channel closed")
}
