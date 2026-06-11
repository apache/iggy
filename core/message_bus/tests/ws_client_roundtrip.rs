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
//! `install_client_ws_fd`, which runs `compio_ws::accept_async` before
//! installing the WS connection.
//!
//! Coverage: client connects without any subprotocol header, handshake
//! succeeds, the server-side handler observes a Request, and the
//! server's `bus.send_to_client` reply lands on the client's reader.
//! Verifies the full bidirectional plane through the reader / writer
//! two-task split. Pre-LOGIN command gating is the caller's
//! responsibility (server-ng) and is not exercised here.

mod common;

use bytes::Bytes;
use common::{header_only, install_ws_clients_locally, loopback};
use compio::net::TcpStream;
use compio_ws::WebSocketStream;
use compio_ws::tungstenite::Message as WsMessage;
use iggy_binary_protocol::Command2;
use iggy_binary_protocol::GenericHeader;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener::ws::{bind, run};
use message_bus::{IggyMessageBus, MessageBus};
use server_common::iobuf::Owned;
use server_common::{MESSAGE_ALIGN, Message};
use std::rc::Rc;
use std::time::Duration;

/// Decode one consensus frame from a raw WS Binary payload. Mirrors the
/// transport's internal `decode_consensus_frame`, reimplemented here
/// because that helper is crate-private and the production WS transport
/// is server-role only (tests drive the client side with raw ws ops).
fn decode_frame(body: &[u8]) -> Message<GenericHeader> {
    let owned = Owned::<MESSAGE_ALIGN>::copy_from_slice(body);
    Message::<GenericHeader>::try_from(owned).expect("decode client frame")
}

/// Raw-read one consensus frame off a client `WebSocketStream`.
#[allow(clippy::future_not_send)]
async fn raw_recv(ws: &mut WebSocketStream<TcpStream>) -> Message<GenericHeader> {
    loop {
        match ws.read().await.expect("client raw read") {
            WsMessage::Binary(bytes) => return decode_frame(&bytes),
            WsMessage::Ping(_) | WsMessage::Pong(_) => {}
            other => panic!("unexpected client ws frame: {other:?}"),
        }
    }
}

#[compio::test]
async fn handshake_succeeds_and_round_trip_completes() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Handler echoes a Reply back to the originating client_id via the
    // bus's send_to_client surface — same path a real dispatcher would
    // take. Spawned because the handler signature is synchronous; the
    // bus surface returns Ready-on-first-poll so the spawned task
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

    // Dial as a real WS client; no subprotocol negotiation required.
    // The production WS transport is server-role only, so drive the
    // client side with raw ws ops: send one Request, read the Reply.
    let client_tcp = TcpStream::connect(server_addr).await.unwrap();
    let url = format!("ws://{server_addr}/");
    let (mut ws_client, _resp) = compio_ws::client_async(url, client_tcp)
        .await
        .expect("ws handshake");

    let request = header_only(Command2::Request, 42, 0).into_frozen();
    ws_client
        .send(WsMessage::Binary(Bytes::from_owner(request)))
        .await
        .expect("client send");

    let reply = compio::time::timeout(Duration::from_secs(2), raw_recv(&mut ws_client))
        .await
        .expect("client must receive reply within 2 s");
    assert_eq!(reply.header().command, Command2::Reply);

    bus.shutdown(Duration::from_secs(2)).await;
}

#[compio::test]
async fn handshake_succeeds_without_subprotocol_header() {
    // Without the subprotocol gate, a client that sends NO
    // Sec-WebSocket-Protocol header must still complete the upgrade.
    // Pre-LOGIN command gating is enforced by the caller (server-ng),
    // not at the bus layer.
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});

    let (listener, server_addr) = bind(loopback()).await.expect("bind");
    let token = bus.token();
    let on_accepted = install_ws_clients_locally(bus.clone(), on_request);
    let accept_handle = compio::runtime::spawn(async move {
        run(listener, token, on_accepted).await;
    });
    bus.track_background(accept_handle);

    let client_tcp = TcpStream::connect(server_addr).await.unwrap();
    let url = format!("ws://{server_addr}/");
    let result = compio_ws::client_async(url, client_tcp).await;
    assert!(
        result.is_ok(),
        "WS handshake must succeed without subprotocol negotiation"
    );

    bus.shutdown(Duration::from_secs(1)).await;
}
