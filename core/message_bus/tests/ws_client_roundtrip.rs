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
//!   succeeds, a Request frame reaches the server-side handler.
//!   Caveat: a full Request -> Reply round trip would deadlock
//!   the per-connection WS dispatcher (`transports::ws.rs`); its
//!   phase-alternating drain-then-read loop blocks Phase B on
//!   `stream.read().await` and cannot react to outbound queue
//!   activity, so the server's Reply never reaches the wire while
//!   the client is parked in Phase B waiting for it. Tracked as a
//!   Phase 7 follow-up in the silverhand transport-plan; the design
//!   note in the dispatcher comment overstates its request/reply
//!   support.
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
use message_bus::IggyMessageBus;
use message_bus::client_listener::RequestHandler;
use message_bus::client_listener_ws::{bind, run};
use message_bus::transports::ws::{WS_SUBPROTOCOL, WsTransportConn};
use message_bus::transports::{TransportConn, TransportWriter};
use std::rc::Rc;
use std::time::Duration;

#[compio::test]
async fn handshake_succeeds_and_request_reaches_handler() {
    let bus = Rc::new(IggyMessageBus::new(0));

    // Side-channel signal: handler fires once when it sees the Request.
    let (tx_seen, rx_seen) = async_channel::bounded::<u128>(1);
    let on_request: RequestHandler = Rc::new(move |client_id, msg| {
        assert_eq!(msg.header().command, Command2::Request);
        let _ = tx_seen.try_send(client_id);
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

    // Reuse the existing WsTransportConn split for client-side framing.
    // Drop the client's reader half; we only verify that the server saw
    // the Request. A full round trip is blocked by the dispatcher
    // limitation called out in the module-level comment.
    let conn = WsTransportConn::new(ws_client);
    let (_reader, mut writer) = conn.into_split();

    let request = header_only(Command2::Request, 42, 0).into_frozen();
    let mut batch: Vec<Frozen<MESSAGE_ALIGN>> = vec![request];
    writer.send_batch(&mut batch).await.expect("client send");
    assert!(batch.is_empty(), "send_batch must drain the Vec");

    // Wait for the handler to observe the Request.
    let observed = compio::time::timeout(Duration::from_secs(2), rx_seen.recv())
        .await
        .expect("handler must fire within 2 s")
        .expect("handler must signal");
    assert_eq!(
        observed >> 112,
        0,
        "client_id top 16 bits must encode shard 0"
    );

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
