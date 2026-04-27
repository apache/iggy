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

//! End-to-end: a real WebSocket client connects to the consensus WS
//! pre-upgrade listener on shard 0; the listener's callback dups the
//! TCP fd and (on the same shard, locally) hands it to
//! `install_client_ws_fd`, which runs `compio_ws::accept_hdr_async`
//! with the iggy.consensus.v1 subprotocol callback before installing
//! the WS connection.
//!
//! Coverage:
//!
//! - **Positive**: client requests `iggy.consensus.v1`, handshake
//!   succeeds, the server-side handler observes a Request, and the
//!   server's `bus.send_to_client` reply lands on the client's
//!   reader. Verifies the full bidirectional plane through the
//!   reader / writer two-task split.
//! - **Negative (missing)**: client omits the `Sec-WebSocket-Protocol`
//!   header; server returns HTTP 400; handshake fails on the client.
//! - **Negative (wrong)**: client requests an unrelated subprotocol;
//!   server returns HTTP 400.

mod common;

use common::{header_only, install_ws_clients_locally, loopback};
use compio::net::TcpStream;
use iggy_binary_protocol::Command2;
use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener_ws::{bind, run};
use message_bus::transports::ws::{WS_SUBPROTOCOL, WsTransportConn};
use message_bus::transports::{TransportConn, TransportReader, TransportWriter};
use message_bus::{IggyMessageBus, MessageBus, framing};
use std::rc::Rc;
use std::time::Duration;

#[compio::test]
async fn handshake_succeeds_and_round_trip_completes() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler echoes a Reply back to the originating client_id via the
    // bus's send_to_client surface — same path a real dispatcher would
    // take. Spawned because the handler signature is synchronous; the
    // bus surface returns Ready-on-first-poll (I2) so the spawned task
    // completes within the same runtime tick.
    let bus_for_handler = Rc::clone(&bus);
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let bus = Rc::clone(&bus_for_handler);
        compio::runtime::spawn(async move {
            let reply = header_only(Command2::Reply, 42, 0).into_frozen();
            bus.send_to_client(client_id, reply)
                .await
                .expect("server send_to_client");
        })
        .detach();
    });

    let (listener, server_addr) = bind(loopback()).await.expect("bind");
    let token = bus.token();
    let on_accepted = install_ws_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    // Dial as a real WS client with the iggy.consensus.v1 subprotocol.
    let client_tcp = TcpStream::connect(server_addr).await.unwrap();
    let url = format!("ws://{server_addr}/");
    let req = tungstenite::ClientRequestBuilder::new(url.parse().unwrap())
        .with_sub_protocol(WS_SUBPROTOCOL);
    let (ws_client, _resp) = compio_ws::client_async(req, client_tcp)
        .await
        .expect("ws handshake");

    let conn = WsTransportConn::new_client(ws_client);
    let (mut reader, mut writer) = conn.into_split();

    let request = header_only(Command2::Request, 42, 0).into_frozen();
    let mut batch: Vec<Frozen<MESSAGE_ALIGN>> = vec![request];
    writer.send_batch(&mut batch).await.expect("client send");
    assert!(batch.is_empty(), "send_batch must drain the Vec");

    // Read the server's Reply off the WS reader within 2 s.
    let reply = compio::time::timeout(
        Duration::from_secs(2),
        reader.read_message(framing::MAX_MESSAGE_SIZE),
    )
    .await
    .expect("client must receive reply within 2 s")
    .expect("reply frame");
    assert_eq!(reply.header().command, Command2::Reply);

    bus.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn missing_subprotocol_rejected_at_handshake() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler should never run (handshake fails before install).
    let on_request: RequestHandler = Rc::new(|_, _| {
        panic!("handler must not be called when subprotocol is missing");
    });

    let (listener, server_addr) = bind(loopback()).await.expect("bind");
    let token = bus.token();
    let on_accepted = install_ws_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    // Dial without the subprotocol header.
    let client_tcp = TcpStream::connect(server_addr).await.unwrap();
    let url = format!("ws://{server_addr}/");
    let result = compio_ws::client_async(url, client_tcp).await;
    assert!(
        result.is_err(),
        "WS handshake should fail when subprotocol is absent"
    );

    bus.shutdown(Duration::from_secs(1)).await;
}

#[compio::test]
async fn wrong_subprotocol_rejected_at_handshake() {
    let bus = Rc::new(IggyMessageBus::new(0));

    let on_request: RequestHandler = Rc::new(|_, _| {
        panic!("handler must not be called when subprotocol is wrong");
    });

    let (listener, server_addr) = bind(loopback()).await.expect("bind");
    let token = bus.token();
    let on_accepted = install_ws_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    let client_tcp = TcpStream::connect(server_addr).await.unwrap();
    let url = format!("ws://{server_addr}/");
    let req = tungstenite::ClientRequestBuilder::new(url.parse().unwrap())
        .with_sub_protocol("definitely.not.iggy");
    let result = compio_ws::client_async(req, client_tcp).await;
    assert!(
        result.is_err(),
        "WS handshake should fail when subprotocol is wrong"
    );

    bus.shutdown(Duration::from_secs(1)).await;
}
