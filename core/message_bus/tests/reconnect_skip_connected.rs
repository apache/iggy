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

use common::{install_replicas_locally, loopback};
use message_bus::IggyMessageBus;
use message_bus::connector::start as start_connector;
use message_bus::replica_listener::{MessageHandler, bind, run};
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

    // Listener on bus1: count accepted TCP connections via a wrapper
    // around the install delegate.
    let (l1, addr1) = bind(loopback()).await.unwrap();
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let accept_inner = install_replicas_locally(bus1.clone(), on_message.clone());
    let accept_counter = Arc::clone(&accept_count);
    let accept_delegate: message_bus::AcceptedReplicaFn = Rc::new(move |stream, peer_id| {
        accept_counter.fetch_add(1, Ordering::SeqCst);
        accept_inner(stream, peer_id);
    });
    let token_for_l1 = bus1.token();
    let l1_handle = compio::runtime::spawn(async move {
        run(
            l1,
            token_for_l1,
            CLUSTER,
            1,
            2,
            accept_delegate,
            message_bus::framing::MAX_MESSAGE_SIZE,
        )
        .await;
    });
    bus1.track_background(l1_handle);

    // bus0 connector: very short reconnect period so the sweep fires
    // multiple times within the test window. If the skip guard is
    // missing, bus1's listener will see N extra accepts.
    let period = Duration::from_millis(50);
    let dial_delegate = install_replicas_locally(bus0.clone(), on_message.clone());
    start_connector(&bus0, CLUSTER, 0, vec![(1u8, addr1)], dial_delegate, period).await;

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
