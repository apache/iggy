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

//! Integration coverage for the authenticated replica handshake.
//!
//! Complements the in-crate unit tests in `core/message_bus/src/auth.rs`
//! by exercising the full end-to-end path (dial -> Ping frame -> listener
//! verify) through `replica_listener::run` + `connector::start`.

mod common;

use common::{install_replicas_locally, loopback};
use message_bus::IggyMessageBus;
use message_bus::auth::{StaticSharedSecret, TokenSource};
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xCAFE;

/// Matching secrets on both sides: handshake succeeds, peer registers.
#[compio::test]
async fn handshake_with_matching_secret_registers_peer() {
    let bus_listener = Rc::new(IggyMessageBus::new(0));
    let bus_dialer = Rc::new(IggyMessageBus::new(0));

    let secret: Rc<dyn TokenSource> = Rc::new(StaticSharedSecret::new([7u8; 32]));

    let on_msg: MessageHandler = Rc::new(|_, _| {});

    let (listener, addr) = bind(loopback()).await.unwrap();
    let token_for_listener = bus_listener.token();
    let accept = install_replicas_locally(bus_listener.clone(), on_msg.clone());
    let secret_for_listener = Rc::clone(&secret);
    let listener_handle = compio::runtime::spawn(async move {
        run(
            listener,
            token_for_listener,
            CLUSTER,
            1,
            2,
            accept,
            message_bus::framing::MAX_MESSAGE_SIZE,
            secret_for_listener,
        )
        .await;
    });
    bus_listener.track_background(listener_handle);

    let dial = install_replicas_locally(bus_dialer.clone(), on_msg);
    start_connector(
        &bus_dialer,
        CLUSTER,
        0,
        vec![(1u8, addr)],
        dial,
        DEFAULT_RECONNECT_PERIOD,
        Rc::clone(&secret),
    )
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus_dialer.replicas().contains(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "matching-secret handshake did not register peer"
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    bus_dialer.shutdown(Duration::from_secs(2)).await;
    bus_listener.shutdown(Duration::from_secs(2)).await;
}

/// Mismatched secrets: the listener rejects the dialed Ping, so neither
/// side ends up with a registered peer within the test window.
#[compio::test]
async fn handshake_with_mismatched_secret_rejects_peer() {
    let bus_listener = Rc::new(IggyMessageBus::new(0));
    let bus_dialer = Rc::new(IggyMessageBus::new(0));

    let listener_secret: Rc<dyn TokenSource> = Rc::new(StaticSharedSecret::new([1u8; 32]));
    let dialer_secret: Rc<dyn TokenSource> = Rc::new(StaticSharedSecret::new([2u8; 32]));

    let on_msg: MessageHandler = Rc::new(|_, _| {});

    let (listener, addr) = bind(loopback()).await.unwrap();
    let token_for_listener = bus_listener.token();
    let accept = install_replicas_locally(bus_listener.clone(), on_msg.clone());
    let listener_handle = compio::runtime::spawn(async move {
        run(
            listener,
            token_for_listener,
            CLUSTER,
            1,
            2,
            accept,
            message_bus::framing::MAX_MESSAGE_SIZE,
            listener_secret,
        )
        .await;
    });
    bus_listener.track_background(listener_handle);

    let dial = install_replicas_locally(bus_dialer.clone(), on_msg);
    start_connector(
        &bus_dialer,
        CLUSTER,
        0,
        vec![(1u8, addr)],
        dial,
        Duration::from_millis(50),
        dialer_secret,
    )
    .await;

    compio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        !bus_dialer.replicas().contains(1),
        "mismatched-secret handshake must not register the peer"
    );
    assert!(
        !bus_listener.replicas().contains(0),
        "mismatched-secret handshake must not register the peer on the listener either"
    );

    bus_dialer.shutdown(Duration::from_secs(2)).await;
    bus_listener.shutdown(Duration::from_secs(2)).await;
}
