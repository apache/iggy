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

//! The dialer's connector keeps retrying after an initial connect failure.
//! Once the listener comes up on the same port, the connector picks it up
//! within one reconnect period.

mod common;

use common::{install_replicas_locally, loopback, test_token_source};
use message_bus::IggyMessageBus;
use message_bus::connector::start as start_connector;
use message_bus::replica_listener::{MessageHandler, bind, run};
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xABCD;

#[compio::test]
async fn periodic_retry_picks_up_late_listener() {
    // Reserve a free port by binding then dropping.
    let (probe, addr) = bind(loopback()).await.unwrap();
    drop(probe);

    // Bus 0 starts its connector targeting that addr - nothing is listening
    // yet, so the first attempt fails. The reconnect period is short so the
    // test does not have to wait long.
    let bus0 = Rc::new(IggyMessageBus::new(0));
    let on_message: MessageHandler = Rc::new(|_, _| {});
    let period = Duration::from_millis(100);
    let dial_delegate = install_replicas_locally(bus0.clone(), on_message.clone());
    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1, addr)],
        dial_delegate,
        period,
        test_token_source(),
    )
    .await;
    assert!(!bus0.replicas().contains(1), "first connect should fail");

    // Bring bus 1 online on the same address.
    let bus1 = Rc::new(IggyMessageBus::new(0));
    let (l1, _) = bind(addr).await.unwrap();
    let token_for_l1 = bus1.token();
    let accept_delegate = install_replicas_locally(bus1.clone(), on_message.clone());
    let l1_handle = compio::runtime::spawn(async move {
        run(
            l1,
            token_for_l1,
            CLUSTER,
            1,
            2,
            accept_delegate,
            message_bus::framing::MAX_MESSAGE_SIZE,
            test_token_source(),
        )
        .await;
    });
    bus1.track_background(l1_handle);

    // Within one or two reconnect periods, bus 0 should connect.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus0.replicas().contains(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "reconnect periodic did not establish the connection"
        );
        compio::time::sleep(Duration::from_millis(20)).await;
    }

    bus0.shutdown(Duration::from_secs(2)).await;
    bus1.shutdown(Duration::from_secs(2)).await;
}
