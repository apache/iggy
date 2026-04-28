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

//! Verify that the per-peer queue applies backpressure: with a tiny capacity
//! and a peer whose receiving end never drains, `send_to_replica` returns
//! `SendError::Backpressure` once the queue is full.
//!
//! This proves the bus is fire-and-forget and that VSR can rely on
//! `Backpressure` as a drop signal rather than a blocking await.

mod common;

use common::{header_only, install_replicas_locally, loopback};
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus, SendError};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xDEAD;

#[compio::test]
async fn try_send_returns_backpressure_when_queue_full() {
    // Tiny per-peer queue so it fills after a few sends.
    let bus0 = Rc::new(IggyMessageBus::with_capacity(0, 4));

    // Peer 1: a real listener that NEVER reads its socket. The kernel TCP
    // recv buffer fills, then the writer task on bus0 blocks on its next
    // writev, which leaves the per-peer mpsc filling up until try_send
    // returns Full.
    let bus1 = Rc::new(IggyMessageBus::with_capacity(0, 4));
    let on_message1: MessageHandler = Rc::new(|_, _| {
        // Block the read loop forever by sleeping. The handler is sync,
        // so no actual blocking is possible from here; instead we just do
        // nothing and rely on the kernel TCP buffer + bus0's tiny queue
        // to apply backpressure.
    });
    let (l1, addr1) = bind(loopback()).await.unwrap();
    let token_for_l1 = bus1.token();
    let accept_1 = install_replicas_locally(bus1.clone(), on_message1);
    let l1_handle = compio::runtime::spawn(async move {
        run(
            l1,
            token_for_l1,
            CLUSTER,
            1,
            2,
            accept_1,
            message_bus::framing::MAX_MESSAGE_SIZE,
        )
        .await;
    });
    bus1.track_background(l1_handle);

    // bus0 dials bus1.
    let on_message0: MessageHandler = Rc::new(|_, _| {});
    let dial_0 = install_replicas_locally(bus0.clone(), on_message0);
    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1, addr1)],
        dial_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;

    // Wait for the connection to register.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus0.replicas().contains(1) {
        assert!(std::time::Instant::now() < deadline, "connect timeout");
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    // Hammer the bus with messages much bigger than the kernel TCP send
    // buffer plus the per-peer queue. With queue capacity 4 and the writer
    // task absorbing some before getting blocked on the wire, we should
    // see Backpressure within a few thousand sends. We are not measuring
    // the exact threshold, only that it is finite and reachable.
    let mut hit_backpressure = false;
    for _ in 0..100_000 {
        let msg = header_only(Command2::Prepare, CLUSTER, 0);
        match bus0.send_to_replica(1, msg.into_frozen()).await {
            Ok(()) => {}
            Err(SendError::Backpressure) => {
                hit_backpressure = true;
                break;
            }
            Err(other) => panic!("unexpected error: {other}"),
        }
    }
    assert!(
        hit_backpressure,
        "expected SendError::Backpressure under sustained send load"
    );

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}
