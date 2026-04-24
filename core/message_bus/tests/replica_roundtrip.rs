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

//! Two real `IggyMessageBus` instances act as replica 0 and replica 1.
//! Replica 0 dials replica 1; the Ping handshake completes; replica 0 sends
//! a Prepare; replica 1's `on_message` callback receives it; replica 1
//! responds with `PrepareOk` over its own bus; replica 0 receives it.

mod common;

use async_channel::Receiver;
use common::{header_only, install_replicas_locally, loopback, test_token_source};
use iggy_binary_protocol::Command2;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xCAFE;

#[compio::test]
async fn two_replicas_exchange_prepare_and_ack() {
    // Bring up replica 1's inbound listener first so replica 0's connect succeeds.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    let (tx1, rx1) = async_channel::bounded::<u8>(8);
    let on_message1: MessageHandler = Rc::new(move |peer, msg| {
        tx1.try_send(msg.header().command as u8).ok();
        let _ = peer;
    });
    let (l1, addr1) = bind(loopback()).await.expect("bind r1");

    let token_for_l1 = bus1.token();
    let nonces_for_l1 = bus1.replica_nonces();
    let accept_delegate_1 = install_replicas_locally(bus1.clone(), on_message1.clone());
    let l1_handle = compio::runtime::spawn(async move {
        run(
            l1,
            token_for_l1,
            CLUSTER,
            1,
            2,
            accept_delegate_1,
            message_bus::framing::MAX_MESSAGE_SIZE,
            test_token_source(),
            nonces_for_l1,
        )
        .await;
    });
    bus1.track_background(l1_handle);

    // Replica 0: outbound connector dials replica 1.
    let bus0 = Rc::new(IggyMessageBus::new(0));
    let (tx0, rx0) = async_channel::bounded::<u8>(8);
    let on_message0: MessageHandler = Rc::new(move |peer, msg| {
        tx0.try_send(msg.header().command as u8).ok();
        let _ = peer;
    });
    let dial_delegate_0 = install_replicas_locally(bus0.clone(), on_message0);

    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1, addr1)],
        dial_delegate_0,
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await;

    // Wait until replica 0 has the connection registered.
    wait_until(|| bus0.replicas().contains(1), Duration::from_secs(2)).await;

    // Replica 0 sends a Prepare to replica 1.
    let prepare = header_only(Command2::Prepare, CLUSTER, 0);
    bus0.send_to_replica(1, prepare.into_frozen())
        .await
        .expect("send prepare");

    // Replica 1 should observe the Prepare via its on_message callback.
    let cmd = expect_recv(&rx1, Duration::from_secs(2)).await;
    assert_eq!(cmd, Command2::Prepare as u8);

    // Replica 1 responds with PrepareOk.
    let ack = header_only(Command2::PrepareOk, CLUSTER, 1);
    bus1.send_to_replica(0, ack.into_frozen())
        .await
        .expect("send ack");

    // Replica 0 should observe the PrepareOk on its outbound read loop.
    let cmd = expect_recv(&rx0, Duration::from_secs(2)).await;
    assert_eq!(cmd, Command2::PrepareOk as u8);

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
