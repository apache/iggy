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
    consumer_group_auto_commit_reconnection_scenario,
    consumer_group_duplicate_name_create_scenario, consumer_group_join_scenario,
    consumer_group_new_messages_after_restart_scenario, consumer_group_offset_cleanup_scenario,
    consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario,
};
use integration::iggy_harness;

// Consumer group scenarios do not support HTTP (stateful operations).

// Every spec here pins a 60s server heartbeat because harness clients never
// ping on their own: the SDK pinger is spawned by `IggyClient::connect`, which
// the harness builder does not call. At the shipped 30s interval a group member
// that idles through another client's setup is reaped by the server's verifier,
// and the failure surfaces as a short member count instead of anything about
// the scenario under test.

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn join(harness: &TestHarness) {
    consumer_group_join_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn single_client(harness: &TestHarness) {
    consumer_group_with_single_client_polling_messages_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn multiple_clients(harness: &TestHarness) {
    consumer_group_with_multiple_clients_polling_messages_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn auto_commit_reconnection(harness: &TestHarness) {
    consumer_group_auto_commit_reconnection_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn new_messages_after_restart(harness: &TestHarness) {
    consumer_group_new_messages_after_restart_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn offset_cleanup(harness: &TestHarness) {
    consumer_group_offset_cleanup_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn duplicate_name_create_preserves_live_group(harness: &TestHarness) {
    consumer_group_duplicate_name_create_scenario::run(harness).await;
}
