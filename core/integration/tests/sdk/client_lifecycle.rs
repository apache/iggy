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

//! A client that is shut down stays shut down, and an explicit disconnect
//! stays in effect until the caller connects again. These tests pin that
//! contract.

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Short, so that several heartbeats run within `HEARTBEATS_WINDOW`.
const FAST_HEARTBEAT: &str = "100ms";
/// Long enough for several heartbeats, any of which would reconnect the client.
const HEARTBEATS_WINDOW: Duration = Duration::from_secs(1);
/// Long, so that no heartbeat after the first one runs during a test.
const SLOW_HEARTBEAT: &str = "1h";
/// The heartbeat pings once right after `connect()`. Waiting this long lets that
/// ping finish, so it cannot race with the `disconnect()` that follows.
const FIRST_HEARTBEAT_SETTLE: Duration = Duration::from_millis(500);
/// A request on a disconnected client must fail on the SDK side,
/// with no dial and no implicit wait, so it has to fail well within this bound.
const SDK_SIDE_FAILURE: Duration = Duration::from_millis(500);
/// A disconnect closes local resources only, so it must return well within
/// this bound. On QUIC, a redial that lands inside the drain of the old
/// connection blocks `disconnect()` in `endpoint.wait_idle()` for good, so the
/// tests bound the call to fail instead of hang. That redial depends on timing,
/// and a connect already past the caller-intent check can still make it
/// (problem 6 of #4287).
const DISCONNECT_BOUND: Duration = Duration::from_secs(5);

#[iggy_harness]
async fn given_a_shut_down_websocket_client_when_connecting_should_fail(harness: &TestHarness) {
    let client = harness.websocket_new_client().await.unwrap();
    client.shutdown().await.unwrap();

    let connect = client.connect().await;

    assert!(
        matches!(connect, Err(IggyError::ClientShutdown)),
        "a client that is shut down must not open a new connection, got {connect:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_a_shut_down_client_when_disconnected_should_stay_shut_down(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client.shutdown().await.unwrap();
    client.disconnect().await.unwrap();

    let connect = client.connect().await;

    assert!(
        matches!(connect, Err(IggyError::ClientShutdown)),
        "disconnect after shutdown must not make the client reusable, got {connect:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_an_auto_login_client_when_explicitly_disconnected_should_stay_disconnected(
    harness: &TestHarness,
) {
    let client = auto_login_client(harness, FAST_HEARTBEAT);
    client.connect().await.unwrap();
    client
        .get_me()
        .await
        .expect("auto-login signs in on connect");

    timeout(DISCONNECT_BOUND, client.disconnect())
        .await
        .expect("disconnect must return while the heartbeat runs")
        .unwrap();
    sleep(HEARTBEATS_WINDOW).await;

    let me = client.get_me().await.map(|_| ());
    assert!(
        matches!(me, Err(IggyError::Disconnected)),
        "a heartbeat after an explicit disconnect must not reconnect the client \
         and replay its auto-login, got {me:?}"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_a_disconnected_auto_login_client_when_sending_requests_should_fail_without_reconnecting(
    harness: &TestHarness,
) {
    let client = disconnected_auto_login_client(harness).await;

    let ping = timeout(SDK_SIDE_FAILURE, client.ping()).await;
    assert!(
        matches!(ping, Ok(Err(IggyError::NotConnected))),
        "ping after an explicit disconnect must fail at once, without a reconnect, got {ping:?}"
    );

    let stats = timeout(SDK_SIDE_FAILURE, client.get_stats())
        .await
        .map(|result| result.map(|_| ()));
    assert!(
        matches!(stats, Ok(Err(IggyError::NotConnected))),
        "get_stats after a failed ping must fail at once too, without a reconnect, got {stats:?}"
    );

    let me = client.get_me().await.map(|_| ());
    assert!(
        matches!(me, Err(IggyError::Disconnected)),
        "the failed requests must not reconnect the client, got {me:?}"
    );
}

/// Builds a client whose auto-login credentials live in its configuration.
/// After `disconnect()`, every transport reconnects only with these configured
/// credentials, and TCP also drops a manual `login_user()` sign-in. The
/// reconnect cooldown is zero, so a reconnect never hides behind a pause.
fn auto_login_client(harness: &TestHarness, heartbeat_interval: &str) -> IggyClient {
    let server = harness.server();
    let transport = harness.transport().unwrap();
    let (address, cooldown_key) = match transport {
        TransportProtocol::Tcp => (server.tcp_addr(), "reestablish_after"),
        TransportProtocol::WebSocket => (server.websocket_addr(), "reestablish_after"),
        TransportProtocol::Quic => (server.quic_addr(), "reconnection_reestablish_after"),
        TransportProtocol::Http => panic!("HTTP has no connection to disconnect"),
    };
    let connection_string = format!(
        "iggy+{transport}://{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}@{}\
         ?heartbeat_interval={heartbeat_interval}&{cooldown_key}=0s",
        address.unwrap()
    );
    IggyClient::from_connection_string(&connection_string).unwrap()
}

/// Connects and then disconnects an auto-login client, after its first
/// heartbeat has finished.
async fn disconnected_auto_login_client(harness: &TestHarness) -> IggyClient {
    let client = auto_login_client(harness, SLOW_HEARTBEAT);
    client.connect().await.unwrap();
    sleep(FIRST_HEARTBEAT_SETTLE).await;
    client.disconnect().await.unwrap();
    client
}
