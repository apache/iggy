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

//! VSR connected-client reads: `get_me` (own connection, from the local
//! `SessionManager`) and `get_clients` (cluster-wide, scatter-gathered
//! across all shards). Run on a 3-node cluster so the gather genuinely
//! crosses shards.
//!
//! TODO(remove-when-scenarios-vsr): these are a focused stopgap. The
//! canonical coverage lives in `server::scenarios::authentication_scenario`
//! (`get_clients`/`get_me`/`get_client`), but that whole suite is
//! `#[cfg(not(feature = "vsr"))]` because it also does real
//! `send_messages` / `poll_messages`, which are not wired under VSR yet
//! (data plane / partition reconciliation). Once the data plane lands and
//! the `server` scenario suite is un-gated under VSR, it subsumes these
//! tests -- delete this file then.

#![cfg(feature = "vsr")]

use iggy::prelude::*;
use integration::iggy_harness;

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn get_me_returns_own_connection(harness: &TestHarness) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let me = client.get_me().await.expect("get_me");
    // Sourced from the per-shard SessionManager: authenticated user id +
    // a real transport label + a non-empty peer address.
    assert_eq!(me.user_id, Some(0), "root is user id 0 in server-ng");
    assert!(!me.transport.is_empty(), "transport label must be set");
    assert!(!me.address.is_empty(), "peer address must be set");

    client.logout_user().await.unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn get_clients_gathers_all_connections(harness: &TestHarness) {
    // Two extra clients alongside the harness client; on a 3-node cluster
    // their connections are spread across shards, so a complete list
    // exercises the cross-shard gather.
    let a = harness.new_client().await.unwrap();
    a.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    let b = harness.new_client().await.unwrap();
    b.login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let clients = a.get_clients().await.expect("get_clients");
    // At least the two we just logged in (the gather may also include
    // other harness/bookkeeping connections).
    assert!(
        clients.len() >= 2,
        "expected >= 2 connected clients, got {}",
        clients.len()
    );
    // Records are real, not the old empty stub: transport + address set.
    for info in &clients {
        assert!(!info.transport.is_empty(), "transport label must be set");
        assert!(!info.address.is_empty(), "peer address must be set");
    }

    a.logout_user().await.unwrap();
    b.logout_user().await.unwrap();
}
