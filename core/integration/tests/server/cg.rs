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
    consumer_group_auto_commit_reconnection_scenario, consumer_group_join_scenario,
    consumer_group_offset_cleanup_scenario,
    consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario,
};
use integration::iggy_harness;

// Consumer group scenarios do not support HTTP (stateful operations).
// TODO: Add QUIC support.

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn join(harness: &TestHarness) {
    consumer_group_join_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn single_client(harness: &TestHarness) {
    consumer_group_with_single_client_polling_messages_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn multiple_clients(harness: &TestHarness) {
    consumer_group_with_multiple_clients_polling_messages_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn auto_commit_reconnection(harness: &TestHarness) {
    consumer_group_auto_commit_reconnection_scenario::run(harness).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(tcp.socket.override_defaults = true, tcp.socket.nodelay = true)
)]
async fn offset_cleanup(harness: &TestHarness) {
    consumer_group_offset_cleanup_scenario::run(harness).await;
}
