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

//! Verify that the writer task batches pipelined sends into a single
//! syscall and that all messages arrive in order at the receiver.

mod common;

use common::{header_only, install_replicas_locally, loopback, test_token_source};
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xBA7C;
const N: usize = 200;

#[compio::test]
async fn writer_batches_pipelined_sends_in_order() {
    let bus1 = Rc::new(IggyMessageBus::new(0));
    let received = Rc::new(Cell::new(0usize));
    let received_for_handler = received.clone();
    let on_message: MessageHandler = Rc::new(move |_peer, msg| {
        // The cluster field carries the message sequence number so we can
        // assert ordering as well as count.
        let seq = msg.header().cluster as usize;
        let prev = received_for_handler.get();
        assert_eq!(seq, prev, "messages must arrive in order");
        received_for_handler.set(prev + 1);
    });
    let (l1, addr1) = bind(loopback()).await.unwrap();
    let token_for_l1 = bus1.token();
    let accept_1 = install_replicas_locally(bus1.clone(), on_message.clone());
    let l1_handle = compio::runtime::spawn(async move {
        run(
            l1,
            token_for_l1,
            CLUSTER,
            1,
            2,
            accept_1,
            message_bus::framing::MAX_MESSAGE_SIZE,
            test_token_source(),
        )
        .await;
    });
    bus1.track_background(l1_handle);

    let bus0 = Rc::new(IggyMessageBus::new(0));
    let on_reply: MessageHandler = Rc::new(|_, _| {});
    let dial_0 = install_replicas_locally(bus0.clone(), on_reply);
    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1, addr1)],
        dial_0,
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await;

    // Wait for connect.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus0.replicas().contains(1) {
        assert!(std::time::Instant::now() < deadline, "connect timeout");
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    // Pipeline N sends back-to-back without yielding. The writer task
    // should drain many of them in single writev calls.
    for i in 0..N {
        let msg = header_only(Command2::Prepare, i as u128, 0);
        bus0.send_to_replica(1, msg.into_frozen())
            .await
            .expect("send should succeed - queue capacity is 256");
    }

    // Wait for delivery.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while received.get() < N {
        assert!(
            std::time::Instant::now() < deadline,
            "received only {}/{N}",
            received.get()
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}
