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
//! contract and fail on the current SDK (#4287), so they stay ignored until the
//! fixes land.

use iggy::prelude::*;
use iggy_common::Credentials;
use integration::harness::TestHarness;
use integration::iggy_harness;
use secrecy::SecretString;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const HEARTBEAT_INTERVAL: &str = "100ms";
/// Long enough for several heartbeats, any of which would reconnect the client.
const HEARTBEATS_WINDOW: Duration = Duration::from_secs(1);
/// The heartbeat pings once right after `connect()`. Waiting this long lets that
/// ping finish, so it cannot race with the `disconnect()` that follows.
const FIRST_HEARTBEAT_SETTLE: Duration = Duration::from_millis(500);
/// A request on a disconnected client must fail on the SDK side,
/// with no dial and no implicit wait, so it has to fail well within this bound.
const SDK_SIDE_FAILURE: Duration = Duration::from_millis(500);

#[iggy_harness]
#[ignore = "fails until #4287 is fixed: WebSocket connects after shutdown"]
async fn given_a_shut_down_websocket_client_when_connecting_should_fail(harness: &TestHarness) {
    // TODO: in `WebSocketClient::connect_inner`, return `ClientShutdown` in the
    // `Shutdown` state before any dial, as `TcpClient` and `QuicClient` already
    // do. The `set_state` guard of the next test only stops the state change,
    // not the dial.
    let client = harness.websocket_new_client().await.unwrap();
    client.shutdown().await.unwrap();

    assert!(
        matches!(client.connect().await, Err(IggyError::ClientShutdown)),
        "a client that is shut down must not open a new connection"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
#[ignore = "fails until #4287 is fixed: disconnect after shutdown makes the client reusable"]
async fn given_a_shut_down_client_when_disconnected_should_stay_shut_down(harness: &TestHarness) {
    // TODO: add one guard in `set_state` of each transport, so that no write
    // moves a client out of `Shutdown`. A guard in each `disconnect()` is not
    // enough: other paths also write `Disconnected`, for example a timeout.
    // With the guard, `connect_inner` returns `ClientShutdown` as it does for a
    // client that was never disconnected.
    let client = harness.new_client().await.unwrap();
    client.shutdown().await.unwrap();
    client.disconnect().await.unwrap();

    assert!(
        matches!(client.connect().await, Err(IggyError::ClientShutdown)),
        "disconnect after shutdown must not make the client reusable"
    );
}

#[iggy_harness]
#[ignore = "fails until #4287 is fixed: the heartbeat undoes an explicit disconnect"]
async fn given_an_auto_login_client_when_explicitly_disconnected_should_stay_disconnected(
    harness: &TestHarness,
) {
    // TODO: stop the heartbeat task in `IggyClient::disconnect`, and let
    // `connect` start it again. That alone is not enough: the heartbeat ping
    // reconnects through the same path as a direct `ping()`, so this test also
    // needs the caller-intent flag of the ping and get_stats tests below. The
    // `IggyClient`, Python and C++ docs describe the current behavior, so they
    // must change with the fix.
    let config = TcpClientConfig {
        server_address: harness.server().raw_tcp_addr().unwrap(),
        heartbeat_interval: NonZeroIggyDuration::from_str(HEARTBEAT_INTERVAL).unwrap(),
        auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
            DEFAULT_ROOT_USERNAME.to_string(),
            SecretString::from(DEFAULT_ROOT_PASSWORD),
        )),
        ..TcpClientConfig::default()
    };
    let client = IggyClient::create(
        ClientWrapper::Tcp(TcpClient::create(Arc::new(config)).unwrap()),
        None,
        None,
    );
    client.connect().await.unwrap();
    client
        .get_me()
        .await
        .expect("auto-login signs in on connect");

    client.disconnect().await.unwrap();
    sleep(HEARTBEATS_WINDOW).await;

    assert!(
        matches!(client.get_me().await, Err(IggyError::Disconnected)),
        "a heartbeat after an explicit disconnect must not reconnect the client \
         and replay its auto-login"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
#[ignore = "fails until #4287 is fixed: ping after disconnect reconnects an auto-login client"]
async fn given_a_disconnected_auto_login_client_when_pinging_should_fail_without_reconnecting(
    harness: &TestHarness,
) {
    // TODO: `Client::disconnect` sets a caller-intent flag, and `connect()`
    // clears it. While the flag is set, `send_raw_with_response` fails on the
    // SDK side instead of reconnecting with the configured credentials. Do not
    // read the intent from `get_state() == Disconnected`: a socket error or a
    // timeout also leaves the client `Disconnected`, and that loss must still
    // heal on its own.
    let client = disconnected_auto_login_client(harness).await;

    let ping = timeout(SDK_SIDE_FAILURE, client.ping()).await;

    assert!(
        matches!(ping, Ok(Err(_))),
        "ping after an explicit disconnect must fail at once, without a reconnect"
    );
    assert!(
        client.get_me().await.is_err(),
        "the failed ping must not reconnect the client"
    );
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
#[ignore = "fails until #4287 is fixed: get_stats after disconnect reconnects an auto-login client"]
async fn given_a_disconnected_auto_login_client_when_getting_stats_should_fail_without_reconnecting(
    harness: &TestHarness,
) {
    // TODO: same fix as for ping: while the caller-intent flag that
    // `Client::disconnect` sets is on, `send_raw_with_response` fails on the
    // SDK side until the next `connect()`.
    let client = disconnected_auto_login_client(harness).await;

    let stats = timeout(SDK_SIDE_FAILURE, client.get_stats()).await;

    assert!(
        matches!(stats, Ok(Err(_))),
        "get_stats after an explicit disconnect must fail at once, without a reconnect"
    );
    assert!(
        client.get_me().await.is_err(),
        "the failed get_stats must not reconnect the client"
    );
}

/// Connects and then disconnects a client whose auto-login credentials live in
/// its configuration. `disconnect()` forgets a `login_user()` sign-in, but not
/// these credentials, so the client still has them to reconnect with.
async fn disconnected_auto_login_client(harness: &TestHarness) -> IggyClient {
    let server = harness.server();
    let credentials = format!("{DEFAULT_ROOT_USERNAME}:{DEFAULT_ROOT_PASSWORD}");
    let connection_string = match harness.transport().unwrap() {
        TransportProtocol::Tcp => {
            format!("iggy+tcp://{credentials}@{}", server.tcp_addr().unwrap())
        }
        TransportProtocol::WebSocket => {
            format!(
                "iggy+ws://{credentials}@{}",
                server.websocket_addr().unwrap()
            )
        }
        TransportProtocol::Quic => {
            format!("iggy+quic://{credentials}@{}", server.quic_addr().unwrap())
        }
        other => panic!("no auto-login client for the {other} transport"),
    };
    let client = IggyClient::from_connection_string(&connection_string).unwrap();

    client.connect().await.unwrap();

    sleep(FIRST_HEARTBEAT_SETTLE).await;

    client.disconnect().await.unwrap();
    client
}
