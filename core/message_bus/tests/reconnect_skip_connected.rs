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

//! Once a peer is registered on the cluster the periodic reconnect
//! sweep must NOT dial it again. A redial would produce a second TCP
//! connection that either races the live one or flaps the mapping
//! across shards.

mod common;

use common::{
    install_dialed_replicas_locally, install_replicas_locally, loopback, set_replica_ctx,
};
use message_bus::IggyMessageBus;
use message_bus::connector::start as start_connector;
use message_bus::replica::listener::{MessageHandler, bind, run};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

const CLUSTER: u128 = 0xC2C2;

#[compio::test]
async fn periodic_reconnect_skips_already_connected_peer() {
    let accept_count = Arc::new(AtomicU32::new(0));
    let bus1 = Rc::new(IggyMessageBus::new(0));
    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx(&bus1, CLUSTER, 1, 2, None);
    set_replica_ctx(&bus0, CLUSTER, 0, 2, None);

    // Listener on bus1: count accepted TCP connections via a wrapper
    // around the install delegate.
    let (l1, addr1) = bind(loopback()).await.unwrap();
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let accept_inner = install_replicas_locally(bus1.clone(), on_message.clone());
    let accept_counter = Arc::clone(&accept_count);
    let accept_delegate: message_bus::AcceptedReplicaFn = Rc::new(move |stream| {
        accept_counter.fetch_add(1, Ordering::SeqCst);
        accept_inner(stream);
    });
    let token_for_l1 = bus1.token();
    let l1_handle = compio::runtime::spawn(async move {
        run(l1, token_for_l1, accept_delegate).await;
    });
    bus1.track_background(l1_handle);

    // bus0 connector: very short reconnect period so the sweep fires
    // multiple times within the test window. If the skip guard is
    // missing, bus1's listener will see N extra accepts.
    let period = Duration::from_millis(50);
    let dial_delegate = install_dialed_replicas_locally(bus0.clone(), on_message.clone());
    start_connector(&bus0, 0, vec![(1u8, addr1)], dial_delegate, period).await;

    // Wait for the initial connection to settle on both sides.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if bus0.replicas().contains(1) && accept_count.load(Ordering::SeqCst) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "initial connect failed: bus0.contains(1)={}, accepts={}",
            bus0.replicas().contains(1),
            accept_count.load(Ordering::SeqCst),
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        accept_count.load(Ordering::SeqCst),
        1,
        "initial connect should produce exactly one accept"
    );

    // Let the periodic sweep fire at least ~5 times.
    compio::time::sleep(period * 6).await;

    // The connection is still live AND no extra accepts happened.
    assert!(
        bus0.replicas().contains(1),
        "peer should still be connected"
    );
    assert_eq!(
        accept_count.load(Ordering::SeqCst),
        1,
        "periodic sweep must not redial a live peer"
    );

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}

/// Redial-race regression: post-refactor, delegation returns before the
/// owning shard's handshake completes, so neither the registry nor the
/// owner table has an entry yet. The sweep must consult the
/// pending-dial set in that window or it would open a second connection
/// to the same peer.
#[compio::test]
async fn periodic_reconnect_skips_dial_pending_peer() {
    let accept_count = Arc::new(AtomicU32::new(0));
    let bus0 = Rc::new(IggyMessageBus::new(0));
    set_replica_ctx(&bus0, CLUSTER, 0, 2, None);

    // Raw listener counting connection attempts; never speaks the
    // protocol, so nothing ever registers on bus0 - only the
    // pending-dial entry can stop redials.
    let listener = compio::net::TcpListener::bind(loopback()).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_counter = Arc::clone(&accept_count);
    let held: Rc<std::cell::RefCell<Vec<compio::net::TcpStream>>> =
        Rc::new(std::cell::RefCell::new(Vec::new()));
    let held_clone = Rc::clone(&held);
    let accept_handle = compio::runtime::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            accept_counter.fetch_add(1, Ordering::SeqCst);
            held_clone.borrow_mut().push(stream);
        }
    });

    // Simulate the in-flight window: the dialed callback marks the peer
    // pending but never completes a handshake (it just holds the
    // stream), exactly like a delegated handshake still running on the
    // owning shard.
    let bus0_for_dial = Rc::clone(&bus0);
    let dialed_streams: Rc<std::cell::RefCell<Vec<compio::net::TcpStream>>> =
        Rc::new(std::cell::RefCell::new(Vec::new()));
    let dialed_streams_clone = Rc::clone(&dialed_streams);
    let on_dialed: message_bus::DialedReplicaFn = Rc::new(move |stream, peer_id| {
        bus0_for_dial.mark_dial_pending(peer_id);
        dialed_streams_clone.borrow_mut().push(stream);
    });

    let period = Duration::from_millis(50);
    start_connector(&bus0, 0, vec![(1u8, addr)], on_dialed, period).await;

    // First sweep dials once; subsequent sweeps must skip the pending
    // peer even though it never registers.
    compio::time::sleep(period * 6).await;
    assert_eq!(
        accept_count.load(Ordering::SeqCst),
        1,
        "sweep must not redial a peer whose delegated handshake is pending"
    );

    // Clearing the pending entry (the handshake-outcome ack) re-enables
    // the redial on the next sweep.
    bus0.clear_dial_pending(1);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while accept_count.load(Ordering::SeqCst) < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "cleared pending entry must re-enable the redial"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    drop(accept_handle);
    bus0.shutdown(Duration::from_secs(2)).await;
}
