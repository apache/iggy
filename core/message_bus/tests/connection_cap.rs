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

//! A client socket keeps its slot in the node's connection cap for as long
//! as the installed connection holds the socket, and frees it on every exit:
//! a peer close, and a WebSocket upgrade that fails before any install.
//!
//! One test, because the count is process-wide and the test harness runs
//! tests in parallel.

mod common;

use common::{loopback, test_client_meta};
use compio::io::AsyncWriteExt;
use compio::net::{TcpListener, TcpStream};
use message_bus::client_listener::RequestHandler;
use message_bus::{
    ClientTransportKind, ConnectionCap, ConnectionInstaller, IggyMessageBus, fd_transfer,
};
use std::rc::Rc;
use std::time::Duration;

const RELEASE_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[allow(clippy::future_not_send)]
async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind(loopback()).await.unwrap();
    let (connected, accepted) = futures::join!(
        TcpStream::connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    (accepted.unwrap().0, connected.unwrap())
}

#[allow(clippy::future_not_send)]
async fn wait_for_live(cap: &ConnectionCap, live: usize, context: &str) {
    compio::time::timeout(RELEASE_TIMEOUT, async {
        while cap.live() != live {
            compio::time::sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{context}: live stayed {} instead of {live}", cap.live()));
}

#[compio::test]
#[allow(clippy::future_not_send)]
async fn given_capped_sockets_when_they_close_or_fail_the_upgrade_should_free_their_slots() {
    let bus = Rc::new(IggyMessageBus::new(0));
    let on_request: RequestHandler = Rc::new(|_, _| {});
    let cap = ConnectionCap::new(Some(2));

    let (tcp_server, tcp_peer) = tcp_pair().await;
    let fd = fd_transfer::dup_fd(&tcp_server).expect("dup TCP fd");
    drop(tcp_server);
    bus.install_client_fd(
        fd,
        test_client_meta(1, ClientTransportKind::Tcp),
        cap.try_acquire(),
        on_request.clone(),
    );

    let (ws_server, mut ws_peer) = tcp_pair().await;
    let fd = fd_transfer::dup_fd(&ws_server).expect("dup WS fd");
    drop(ws_server);
    bus.install_client_ws_fd(
        fd,
        test_client_meta(2, ClientTransportKind::Ws),
        cap.try_acquire(),
        on_request,
    );

    assert_eq!(cap.live(), 2);
    assert!(cap.try_acquire().is_none(), "both slots are taken");

    let (result, _) = ws_peer.write_all(b"not an upgrade\r\n\r\n").await.into();
    result.expect("write to the WS listener");
    wait_for_live(&cap, 1, "a failed WS upgrade").await;

    compio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(cap.live(), 1, "the installed TCP connection keeps its slot");
    drop(tcp_peer);
    wait_for_live(&cap, 0, "a TCP peer close").await;
}
