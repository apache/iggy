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

//! Verify that a slow peer cannot stall sends to other peers.
//!
//! Setup: a sender bus has two replica connections. Peer A reads normally;
//! peer B's listener accepts but then drops the read half (so the sender's
//! kernel TCP send buffer eventually fills, blocking peer B's writer task).
//! With the per-peer queue model, sends to peer A must remain O(microseconds)
//! regardless of how blocked peer B is.

mod common;

use common::{header_only, install_replicas_locally, loopback};
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus};
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

const CLUSTER: u128 = 0xF00D;

#[compio::test]
async fn slow_peer_does_not_block_other_peers() {
    // Receiving bus 1 (peer A): drains messages normally.
    let bus_a = Rc::new(IggyMessageBus::new(0));
    let received_a = Rc::new(Cell::new(0usize));
    let received_a_clone = received_a.clone();
    let on_a: MessageHandler = Rc::new(move |_, _| {
        received_a_clone.set(received_a_clone.get() + 1);
    });
    let (la, addr_a) = bind(loopback()).await.unwrap();
    let token_a = bus_a.token();
    let accept_a = install_replicas_locally(bus_a.clone(), on_a.clone());
    let la_handle = compio::runtime::spawn(async move {
        run(
            la,
            token_a,
            CLUSTER,
            1,
            3,
            accept_a,
            message_bus::framing::MAX_MESSAGE_SIZE,
        )
        .await;
    });
    bus_a.track_background(la_handle);

    // Receiving bus 2 (peer B): handler is a no-op, but the listener stays
    // up. The kernel TCP recv buffer is small, so once we send enough to
    // peer B without it draining, peer B's writer task on the sender side
    // will eventually block on writev.
    let bus_b = Rc::new(IggyMessageBus::new(0));
    let on_b: MessageHandler = Rc::new(|_, _| {});
    let (lb, addr_b) = bind(loopback()).await.unwrap();
    let token_b = bus_b.token();
    let accept_b = install_replicas_locally(bus_b.clone(), on_b);
    let lb_handle = compio::runtime::spawn(async move {
        run(
            lb,
            token_b,
            CLUSTER,
            2,
            3,
            accept_b,
            message_bus::framing::MAX_MESSAGE_SIZE,
        )
        .await;
    });
    bus_b.track_background(lb_handle);

    // Sender bus 0 dials both A and B.
    let bus0 = Rc::new(IggyMessageBus::with_capacity(0, 16));
    let dial_0: MessageHandler = Rc::new(|_, _| {});
    let dial_delegate = install_replicas_locally(bus0.clone(), dial_0);
    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1, addr_a), (2, addr_b)],
        dial_delegate,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    let deadline = Instant::now() + Duration::from_secs(2);
    while !(bus0.replicas().contains(1) && bus0.replicas().contains(2)) {
        assert!(Instant::now() < deadline, "both replicas must connect");
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    // Time a single send_to_replica to the FAST peer A. With per-peer
    // queues this must complete in microseconds even if peer B is busy.
    let send_a_start = Instant::now();
    bus0.send_to_replica(1, header_only(Command2::Prepare, 0, 0).into_frozen())
        .await
        .expect("send to A");
    let send_a_elapsed = send_a_start.elapsed();
    assert!(
        send_a_elapsed < Duration::from_millis(50),
        "send_to_replica(A) took {send_a_elapsed:?}; head-of-line blocking?"
    );

    // Even after issuing many sends to B (which will eventually pile up in
    // the kernel buffer and the per-peer queue), sends to A must stay fast.
    for _ in 0..50 {
        let _ = bus0
            .send_to_replica(2, header_only(Command2::Prepare, 0, 0).into_frozen())
            .await;
    }
    let send_a2_start = Instant::now();
    bus0.send_to_replica(1, header_only(Command2::Prepare, 0, 0).into_frozen())
        .await
        .expect("send to A after B saturation");
    let send_a2_elapsed = send_a2_start.elapsed();
    assert!(
        send_a2_elapsed < Duration::from_millis(50),
        "second send_to_replica(A) took {send_a2_elapsed:?}; HOL after B saturation"
    );

    // Wait for peer A to receive at least our two messages.
    let deadline = Instant::now() + Duration::from_secs(2);
    while received_a.get() < 2 {
        assert!(
            Instant::now() < deadline,
            "peer A received only {}/2",
            received_a.get()
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    bus0.shutdown(Duration::from_secs(2)).await;
    bus_a.shutdown(Duration::from_secs(2)).await;
    bus_b.shutdown(Duration::from_secs(2)).await;
}
