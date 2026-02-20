/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::server::scenarios::{
    message_size_scenario, segment_rotation_race_scenario, single_message_per_batch_scenario,
    tcp_tls_scenario, websocket_tls_scenario,
};
use integration::iggy_harness;

#[iggy_harness(
    test_client_transport = TcpTlsGenerated,
    server(tls = generated)
)]
async fn tcp_tls_scenario_should_be_valid(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    tcp_tls_scenario::run(&client).await;
}

#[iggy_harness(
    test_client_transport = TcpTlsSelfSigned,
    server(tls = self_signed)
)]
async fn tcp_tls_self_signed_scenario_should_be_valid(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    tcp_tls_scenario::run(&client).await;
}

#[iggy_harness(
    test_client_transport = WebSocketTlsGenerated,
    server(websocket_tls = generated)
)]
async fn websocket_tls_scenario_should_be_valid(harness: &TestHarness) {
    let client = harness.root_client().await.unwrap();
    websocket_tls_scenario::run(&client).await;
}

#[iggy_harness]
async fn message_size_scenario(harness: &TestHarness) {
    message_size_scenario::run(harness).await;
}

#[iggy_harness(server(partition.messages_required_to_save = "10000"))]
async fn should_handle_single_message_per_batch_with_delayed_persistence(harness: &TestHarness) {
    single_message_per_batch_scenario::run(harness, 5).await;
}

/// This test configures the server to trigger frequent segment rotations and runs
/// multiple concurrent producers across all protocols (TCP, HTTP, QUIC, WebSocket)
/// to maximize the chance of hitting the race condition between persist_messages_to_disk
/// and handle_full_segment.
///
/// Server configuration:
/// - Very small segment size (512B) to trigger frequent rotations
/// - Short message_saver interval (1s) to add concurrent persist operations
/// - Small messages_required_to_save (32) to trigger more frequent saves
/// - cache_indexes = none to trigger clear_active_indexes path
///
/// Test configuration:
/// - 8 producers total (2 per protocol: TCP, HTTP, QUIC, WebSocket)
/// - All producers write to the same partition for maximum lock contention
#[iggy_harness(server(
    segment.size = "512B",
    message_saver.interval = "1s",
    partition.messages_required_to_save = "32",
    segment.cache_indexes = "none",
    tcp.socket_migration = false,
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn segment_rotation_scenario(harness: &TestHarness) {
    segment_rotation_race_scenario::run(harness).await;
}
