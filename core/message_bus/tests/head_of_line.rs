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
use compio::net::TcpListener;
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus, SendError};
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

    // Peer B: raw TCP listener that accepts connections but never reads
    // from them. This is how we force head-of-line blocking: bus0's dial
    // completes (TCP connect + Ping write into the peer's kernel recv
    // buffer), but subsequent Prepare frames pile up. Once the kernel send
    // buffer on bus0's side and the per-peer queue are both full,
    // send_to_replica(2) returns Backpressure.
    let lb = TcpListener::bind(loopback()).await.unwrap();
    let addr_b = lb.local_addr().unwrap();
    let held_streams: Rc<std::cell::RefCell<Vec<compio::net::TcpStream>>> =
        Rc::new(std::cell::RefCell::new(Vec::new()));
    let held_streams_clone = held_streams.clone();
    let accept_b_handle = compio::runtime::spawn(async move {
        while let Ok((stream, _)) = lb.accept().await {
            held_streams_clone.borrow_mut().push(stream);
        }
    });

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

    // Baseline: a single send to A before any saturation. Both peers are
    // healthy, both sends take try_send fast-path time.
    bus0.send_to_replica(1, header_only(Command2::Prepare, 0, 0).into_frozen())
        .await
        .expect("send to A (baseline)");

    // Saturate peer B until the per-peer queue returns Backpressure. Once
    // that happens, we know the kernel send buffer + the 16-slot queue are
    // both full and any further send-to-B completes synchronously with the
    // Backpressure error (not by blocking on writev).
    let mut b_saturated = false;
    for _ in 0..100_000 {
        match bus0
            .send_to_replica(2, header_only(Command2::Prepare, 0, 0).into_frozen())
            .await
        {
            Ok(()) => {}
            Err(SendError::Backpressure) => {
                b_saturated = true;
                break;
            }
            Err(other) => panic!("unexpected error while saturating peer B: {other:?}"),
        }
    }
    assert!(
        b_saturated,
        "peer B should return Backpressure once kernel + queue are full"
    );

    // Property under test: with peer B's queue fully backpressured,
    // sends to peer A must NOT block on peer B. Compare A's send latency
    // against a freshly-timed B send that we expect to return Backpressure
    // instantly. HOL would manifest as A being as slow as a blocked writev.
    let send_a_start = Instant::now();
    bus0.send_to_replica(1, header_only(Command2::Prepare, 0, 0).into_frozen())
        .await
        .expect("send to A while B is saturated");
    let send_a_elapsed = send_a_start.elapsed();

    let send_b_start = Instant::now();
    let send_b_result = bus0
        .send_to_replica(2, header_only(Command2::Prepare, 0, 0).into_frozen())
        .await;
    let send_b_elapsed = send_b_start.elapsed();
    let b_backpressured = matches!(send_b_result, Err(SendError::Backpressure));

    assert!(
        b_backpressured,
        "peer B should still be backpressured, got {send_b_result:?}"
    );
    // Both the A-send and the Backpressure-B-send are fast-path sync
    // operations; HOL blocking would push A into millisecond territory
    // while a saturated B-send stays at Backpressure speed. Allow 100x
    // slack so this holds across loaded CI without tuning thresholds.
    assert!(
        send_a_elapsed <= send_b_elapsed.saturating_mul(100),
        "HOL: send_to_replica(A)={send_a_elapsed:?}, backpressured send_to_replica(B)={send_b_elapsed:?}"
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
    drop(accept_b_handle);
    drop(held_streams);
}
