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

//! Both the writer task and the reader task observe an abnormal
//! connection close. They MUST emit exactly one `connection_lost`
//! notification per peer disconnect so shard 0 doesn't churn the
//! mapping broadcast twice in a row.

mod common;

use common::{install_replicas_locally, loopback, test_token_source};
use message_bus::IggyMessageBus;
use message_bus::connector::{DEFAULT_RECONNECT_PERIOD, start as start_connector};
use message_bus::replica_listener::{MessageHandler, bind, run};
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

const CLUSTER: u128 = 0xFEED;

#[compio::test]
async fn connection_lost_fires_exactly_once_per_peer_disconnect() {
    // bus0 dials bus1 (directional rule: lower id dials higher id).
    let bus0 = Rc::new(IggyMessageBus::new(0));
    let bus1 = Rc::new(IggyMessageBus::new(0));

    let counter: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    let notified_peer: Rc<Cell<Option<u8>>> = Rc::new(Cell::new(None));
    {
        let counter = Rc::clone(&counter);
        let notified_peer = Rc::clone(&notified_peer);
        bus0.set_connection_lost_fn(Rc::new(move |peer_id| {
            counter.set(counter.get() + 1);
            notified_peer.set(Some(peer_id));
        }));
    }

    let on_message: MessageHandler = Rc::new(|_, _| {});

    // bus1 listens; bus0 dials.
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

    let dial_0 = install_replicas_locally(bus0.clone(), on_message.clone());
    start_connector(
        &bus0,
        CLUSTER,
        0,
        vec![(1u8, addr1)],
        dial_0,
        DEFAULT_RECONNECT_PERIOD,
        test_token_source(),
    )
    .await;

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !bus0.replicas().contains(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "bus0 failed to establish connection to bus1"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    // Drop bus1 entirely: this closes the listener and every accepted
    // connection's fd, so bus0's reader sees EOF and bus0's writer sees
    // a broken pipe on its next flush / close notification.
    bus1.shutdown(Duration::from_secs(2)).await;
    drop(bus1);

    // Wait for bus0 to observe the loss and eject the peer.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while bus0.replicas().contains(1) {
        assert!(
            std::time::Instant::now() < deadline,
            "bus0 never observed the disconnect"
        );
        compio::time::sleep(Duration::from_millis(10)).await;
    }

    // Small grace window: the second half (reader or writer) may still
    // be winding down. If the guard works, it will run through the
    // post-exit block and skip the notify.
    compio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        counter.get(),
        1,
        "notify_connection_lost must fire exactly once per peer disconnect, got {}",
        counter.get(),
    );
    assert_eq!(notified_peer.get(), Some(1u8));

    bus0.shutdown(Duration::from_secs(2)).await;
}
