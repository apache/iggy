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

//! Both replicas spin up their inbound listener AND their connector at the
//! same time. The directional rule (lower id dials, higher id accepts)
//! must yield exactly one TCP connection between them, regardless of
//! scheduling.

mod common;

use common::{install_replicas_locally, loopback};
use message_bus::IggyMessageBus;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica::listener::{MessageHandler, bind, run};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xBEEF;

#[compio::test]
async fn lower_id_dials_higher_id_accepts() {
    let bus0 = Rc::new(IggyMessageBus::new(0));
    let bus1 = Rc::new(IggyMessageBus::new(0));

    let on_message: MessageHandler = Rc::new(|_, _| {});

    // Both buses bind their inbound listener.
    let (l0, addr0) = bind(loopback()).await.unwrap();
    let (l1, addr1) = bind(loopback()).await.unwrap();

    let token_for_l0 = bus0.token();
    let accept_0 = install_replicas_locally(bus0.clone(), on_message.clone());
    let l0_handle = compio::runtime::spawn(async move {
        run(
            l0,
            token_for_l0,
            CLUSTER,
            0,
            2,
            accept_0,
            message_bus::framing::MAX_MESSAGE_SIZE,
            Duration::from_secs(10),
        )
        .await;
    });
    bus0.track_background(l0_handle);

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
            Duration::from_secs(10),
        )
        .await;
    });
    bus1.track_background(l1_handle);

    // Both buses start their connector with the full peer list.
    let peers = vec![(0u8, addr0), (1u8, addr1)];

    let dial_0 = install_replicas_locally(bus0.clone(), on_message.clone());
    start_connector(
        &bus0,
        CLUSTER,
        0,
        peers.clone(),
        dial_0,
        DEFAULT_RECONNECT_PERIOD,
    )
    .await;
    let dial_1 = install_replicas_locally(bus1.clone(), on_message.clone());
    start_connector(&bus1, CLUSTER, 1, peers, dial_1, DEFAULT_RECONNECT_PERIOD).await;

    // Wait for the directional connection to settle.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !(bus0.replicas().contains(1) && bus1.replicas().contains(0)) {
        assert!(
            std::time::Instant::now() < deadline,
            "replicas did not connect: bus0={} bus1={}",
            bus0.replicas().len(),
            bus1.replicas().len()
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    // Each side has exactly one peer connection.
    assert_eq!(bus0.replicas().len(), 1);
    assert_eq!(bus1.replicas().len(), 1);
    // bus0 must NOT have dialed itself, and must have peer 1.
    assert!(bus0.replicas().contains(1));
    // bus1 must NOT have dialed peer 0 (directional rule); it has the
    // inbound entry for 0.
    assert!(bus1.replicas().contains(0));

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}
