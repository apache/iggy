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

//! The node-wide `[message_bus] connections_max` cap. Idle sockets to the TCP,
//! WebSocket and HTTP listeners share one count. A socket past the cap is
//! closed at accept on every listener, and closing a held socket frees its
//! slot, also when another shard owns the connection.

use std::net::SocketAddr;
use std::time::Duration;

use integration::iggy_harness;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::{Instant, sleep, timeout};

/// An admitted socket gets no bytes before its first request, so a read that
/// stays pending this long means the server holds the socket open.
const OPEN_WAIT: Duration = Duration::from_millis(300);
const CLOSE_WAIT: Duration = Duration::from_secs(5);
const RELEASE_BUDGET: Duration = Duration::from_secs(10);
const RETRY_PAUSE: Duration = Duration::from_millis(100);

async fn connect(addr: SocketAddr) -> TcpStream {
    TcpStream::connect(addr)
        .await
        .unwrap_or_else(|error| panic!("connect to {addr}: {error}"))
}

/// `true` when the server keeps the socket open, `false` when it closes it.
async fn stays_open(stream: &mut TcpStream) -> bool {
    let mut byte = [0_u8; 1];
    match timeout(OPEN_WAIT, stream.read(&mut byte)).await {
        Err(_elapsed) => true,
        Ok(Ok(0) | Err(_)) => false,
        Ok(Ok(_)) => panic!("the server sent bytes before any request"),
    }
}

async fn assert_closed_at_accept(addr: SocketAddr, listener: &str) {
    let mut stream = connect(addr).await;
    let mut byte = [0_u8; 1];
    let read = timeout(CLOSE_WAIT, stream.read(&mut byte))
        .await
        .unwrap_or_else(|_| panic!("the {listener} listener kept a socket past the cap"));
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "the {listener} listener must close a socket past the cap, read {read:?}"
    );
}

#[iggy_harness(
    cluster_nodes = 1,
    server(
        message_bus.connections_max = "3",
        message_bus.handshake_grace = "60 s",
        sharding.cpu_allocation = "0..2"
    )
)]
async fn given_connections_cap_when_sockets_fill_it_should_close_new_ones_until_one_closes(
    harness: &TestHarness,
) {
    let server = harness.server();
    let tcp = server.tcp_addr().expect("TCP listener");
    let websocket = server.websocket_addr().expect("WebSocket listener");
    let http = server.http_addr().expect("HTTP listener");

    let mut held = Vec::new();
    for (addr, listener) in [(tcp, "TCP"), (websocket, "WebSocket"), (http, "HTTP")] {
        let mut stream = connect(addr).await;
        assert!(
            stays_open(&mut stream).await,
            "the {listener} socket fits under the cap"
        );
        held.push(stream);
    }

    for (addr, listener) in [(tcp, "TCP"), (websocket, "WebSocket"), (http, "HTTP")] {
        assert_closed_at_accept(addr, listener).await;
    }

    drop(held.remove(0));
    let deadline = Instant::now() + RELEASE_BUDGET;
    let mut replacement = loop {
        let mut stream = connect(tcp).await;
        if stays_open(&mut stream).await {
            break stream;
        }
        assert!(
            Instant::now() < deadline,
            "closing a held socket must free its slot"
        );
        sleep(RETRY_PAUSE).await;
    };

    assert_closed_at_accept(http, "HTTP").await;
    assert!(
        stays_open(&mut replacement).await,
        "the replacement socket keeps its slot"
    );
}
