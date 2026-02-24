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

use crate::server::scenarios::{
    authentication_scenario, bench_scenario, consumer_timestamp_polling_scenario,
    create_message_payload, invalid_consumer_offset_scenario, message_headers_scenario,
    permissions_scenario, snapshot_scenario, stream_size_validation_scenario, system_scenario,
    user_scenario,
};
use integration::iggy_harness;

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn authentication(harness: &TestHarness) {
    authentication_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn system(harness: &TestHarness) {
    system_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn user(harness: &TestHarness) {
    user_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn permissions(harness: &TestHarness) {
    permissions_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn message_headers(harness: &TestHarness) {
    message_headers_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn create_message_payload_scenario(harness: &TestHarness) {
    create_message_payload::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn stream_size_validation(harness: &TestHarness) {
    stream_size_validation_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn bench(harness: &TestHarness) {
    bench_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn consumer_timestamp_polling(harness: &TestHarness) {
    consumer_timestamp_polling_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn snapshot(harness: &TestHarness) {
    snapshot_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn invalid_consumer_offset(harness: &TestHarness) {
    invalid_consumer_offset_scenario::run(harness).await;
}
