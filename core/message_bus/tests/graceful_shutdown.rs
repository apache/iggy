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

//! Open several real client connections, call `bus.shutdown(timeout)`, and
//! verify that all read tasks exit cleanly within the timeout.

mod common;

use common::{install_clients_locally, loopback};
use compio::net::TcpStream;
use message_bus::client_listener::{RequestHandler, bind, run};
use message_bus::{IggyMessageBus, MessageBus, SendError};
use std::rc::Rc;
use std::time::Duration;

#[compio::test]
async fn drains_all_clients_within_timeout() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let (listener, addr) = bind(loopback()).await.unwrap();

    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept);

    // Open N clients. The streams are kept alive in `clients` so the
    // listener observes 5 live connections; we never read from them.
    #[allow(clippy::collection_is_never_read)]
    let mut clients: Vec<TcpStream> = Vec::with_capacity(5);
    for _ in 0..5 {
        let stream = TcpStream::connect(addr).await.unwrap();
        clients.push(stream);
    }

    // Wait for the listener to register all of them.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while bus.clients().len() < 5 {
        assert!(
            std::time::Instant::now() < deadline,
            "listener never registered all clients"
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(outcome.force, 0, "no connection should need force-cancel");
    // Each connection now has two tasks (writer + reader) so 5 conns -> 10.
    assert_eq!(outcome.clean, 10);
    assert!(bus.clients().is_empty());
    assert!(bus.is_shutting_down());

    // Sends after shutdown must fail with the right variant.
    let dummy = common::header_only(iggy_binary_protocol::Command2::Reply, 0, 0);
    let err = bus
        .send_to_client(1u128 << 112, dummy.into_frozen())
        .await
        .unwrap_err();
    assert!(matches!(err, SendError::BusShuttingDown));
}

/// Regression: a misbehaving background task that ignores the shutdown
/// token and outlives the total budget must not starve connection drain.
/// Connection drain runs BEFORE background drain so writer tasks still
/// flush cleanly when a rogue background task exists.
#[compio::test]
async fn connection_drain_precedes_slow_background() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let (listener, addr) = bind(loopback()).await.unwrap();

    let token = bus.token();
    let accept_delegate = install_clients_locally(bus.clone(), on_request);
    let accept = compio::runtime::spawn(async move {
        run(listener, token, accept_delegate).await;
    });
    bus.track_background(accept);

    // Rogue background task: sleeps 3s without observing the shutdown
    // token. Under the old ordering this would consume the entire
    // 1s shutdown budget before connection drain starts. Under the new
    // ordering connections drain first and the rogue task is forced.
    let rogue = compio::runtime::spawn(async {
        compio::time::sleep(Duration::from_secs(3)).await;
    });
    bus.track_background(rogue);

    #[allow(clippy::collection_is_never_read)]
    let mut clients: Vec<TcpStream> = Vec::with_capacity(2);
    for _ in 0..2 {
        clients.push(TcpStream::connect(addr).await.unwrap());
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while bus.clients().len() < 2 {
        assert!(
            std::time::Instant::now() < deadline,
            "listener never registered clients"
        );
        compio::time::sleep(Duration::from_millis(5)).await;
    }

    let outcome = bus.shutdown(Duration::from_secs(1)).await;
    // Clients still drained cleanly: writer + reader per connection.
    assert_eq!(
        outcome.force, 0,
        "connection drain must not be forced by rogue background task"
    );
    assert_eq!(outcome.clean, 4, "2 clients * (writer + reader)");
    // The rogue background task (plus the accept loop, which exits on
    // the shutdown token) is accounted for in background counters. At
    // least one background task was forced.
    assert!(
        outcome.background_force >= 1,
        "rogue background task should be force-cancelled, got {outcome:?}"
    );
    assert!(bus.clients().is_empty());
}
