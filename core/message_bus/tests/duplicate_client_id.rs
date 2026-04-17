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

//! Shard 0 mints monotonic client ids so the registry should never see a
//! collision. If one leaks in anyway (bad foreign mint, wrap at 2^112),
//! the installer must drop the duplicate fd instead of panicking. This
//! test forces the collision and verifies the first entry survives while
//! the second is dropped cleanly.

mod common;

use common::{header_only, loopback};
use compio::net::{TcpListener, TcpStream};
use iggy_binary_protocol::Command2;
use message_bus::IggyMessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::framing::write_message;
use message_bus::installer::install_client_stream;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

#[allow(clippy::future_not_send)]
async fn loopback_pair() -> TcpStream {
    let listener = TcpListener::bind(loopback()).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let accept = compio::runtime::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        stream
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    let _server = accept.await.expect("accept task");
    client
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn duplicate_install_drops_second_fd_without_panic() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});

    let client_id: u128 = 0x42;
    let first = loopback_pair().await;
    install_client_stream(&bus, client_id, first, on_request.clone());
    assert!(bus.clients().contains(client_id));
    assert_eq!(bus.clients().len(), 1);

    // Collision: same client_id, second stream. Must not panic, must not
    // double-register.
    let second = loopback_pair().await;
    install_client_stream(&bus, client_id, second, on_request);
    assert!(bus.clients().contains(client_id));
    assert_eq!(
        bus.clients().len(),
        1,
        "duplicate install must not inflate the registry"
    );

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(
        outcome.force, 0,
        "duplicate's writer task must exit cleanly via dropped sender"
    );
}

#[allow(clippy::future_not_send)]
async fn loopback_peer_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(loopback()).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let accept = compio::runtime::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        stream
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    let server = accept.await.expect("accept task");
    (client, server)
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn orphan_reader_from_losing_install_does_not_invoke_on_request() {
    // An orphan reader spawned by the losing side of a duplicate-client-id
    // install must NOT route inbound requests via `on_request`: the
    // registry entry that `client_id` keys is the winner's, so anything
    // forwarded through the winner's entry would land on the wrong
    // consensus path. The aborted flag set in `install_client_stream` on
    // `AlreadyRegistered` stops the read loop before it dispatches.
    let bus = Rc::new(IggyMessageBus::new(0));

    let on_request_count: Rc<Cell<u32>> = Rc::new(Cell::new(0));
    let on_request: RequestHandler = {
        let c = Rc::clone(&on_request_count);
        Rc::new(move |_, _| {
            c.set(c.get() + 1);
        })
    };

    let client_id: u128 = 0xdead_beef;

    // First install wins the slot.
    let (first_local, _first_peer) = loopback_peer_pair().await;
    install_client_stream(&bus, client_id, first_local, on_request.clone());
    assert!(bus.clients().contains(client_id));

    // Second install loses the `AlreadyRegistered` race. Its spawned
    // reader is holding `second_local`'s read half; we drive that reader
    // by writing a `Request` frame from `second_peer`.
    let (second_local, mut second_peer) = loopback_peer_pair().await;
    install_client_stream(&bus, client_id, second_local, on_request);

    // Push a valid Request frame down the orphan reader's wire. Before
    // the fix, the orphan would call `on_request(client_id, msg)` and
    // bump the counter; with the fix the `aborted` guard skips it.
    let msg = header_only(Command2::Request, 0, 0);
    write_message(&mut second_peer, msg)
        .await
        .expect("write Request");
    // Give the orphan reader a tick to observe the bytes and hit the
    // aborted check.
    compio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        on_request_count.get(),
        0,
        "aborted orphan reader must not invoke on_request"
    );
    assert!(
        bus.clients().contains(client_id),
        "winner entry must still be present"
    );

    // Close the orphan's peer side so its reader observes EOF and runs
    // its post-loop block. With the fix, it must NOT evict the winner.
    drop(second_peer);
    compio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        bus.clients().contains(client_id),
        "aborted orphan reader must not close_peer on the winner"
    );

    let outcome = bus.shutdown(Duration::from_secs(2)).await;
    assert_eq!(outcome.force, 0);
}
