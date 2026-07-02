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
    authentication_scenario, consumer_timestamp_polling_scenario, create_message_payload,
    message_headers_scenario,
};
#[cfg(not(feature = "vsr"))]
use crate::server::scenarios::{
    bench_scenario, invalid_consumer_offset_scenario, permissions_scenario, snapshot_scenario,
    stream_size_validation_scenario, system_scenario, user_scenario,
};
use integration::iggy_harness;

#[iggy_harness(
    test_client_transport = [Tcp, Http, Quic, WebSocket],
    server(
        segment.size = "1MiB",
        tcp.socket.override_defaults = true,
        tcp.socket.nodelay = true,
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn authentication(harness: &TestHarness) {
    authentication_scenario::run(harness).await;
}

// Blocked under vsr: asserts on responses server-ng still stubs --
// `MaxTopicSize::ServerDefault` is echoed instead of resolved against
// server config, and stats/cluster-metadata fields are hardcoded zeros.
// Re-enable as the response wiring lands.
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr: the startup-hang is fixed and root `created_at` now
// resolves, but the scenario issues several sequential `login_user` calls on
// one connection. Under vsr login == register, and the SDK's one-shot
// `ConsensusSession` only resets on the reconnect/replay path, so the second
// deliberate re-login panics `register_request_id already called`
// (sdk/src/session.rs). Needs an SDK login-lifecycle fix (reset the session
// on each fresh login).
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr, but no longer for RBAC: server-ng now enforces the
// caller's permissions on the metadata apply path, and that matrix is
// covered by `server::http_rbac`. This scenario stays gated because it
// drives get_stats/get_clients/snapshot across all four transports
// (Tcp/Http/Quic/WebSocket), and QUIC/WebSocket control-plane parity is
// not yet verified on server-ng.
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr: stream/topic size accounting is not surfaced into
// the get_stream/get_topic responses yet (sizes report 0).
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr: pushes 8 MiB through the data plane, which drains the
// in-memory partition journal to disk segments; benchmarks are out of
// scope for the vsr test pass.
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr: the snapshot-file feature (GET_SNAPSHOT_FILE) is
// not implemented in server-ng.
#[cfg(not(feature = "vsr"))]
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

// Blocked under vsr (unsolved): expects typed errors (`InvalidOffset`)
// for invalid offset stores, but the partition plane neither validates
// stored offsets nor has a wire vehicle for per-request errors (the vsr
// Reply carries no status; Eviction is session-terminal). Needs an
// error-reply design on the partition plane.
#[cfg(not(feature = "vsr"))]
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
