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

use crate::server::scenarios::{message_size_scenario, single_message_per_batch_scenario};
use crate::server::scenarios::{reconnect_after_restart_scenario, restart_offset_skip_scenario};
use crate::server::scenarios::{
    segment_rotation_race_scenario, tcp_tls_scenario, websocket_tls_scenario,
};
use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use std::collections::HashSet;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

const TLS_CLIENTS_PER_SHARD: usize = 2;
const TLS_TEST_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[iggy_harness(
    test_client_transport = TcpTlsGenerated,
    server(tls = generated, logging.level = "info")
)]
async fn tcp_tls_scenario_should_be_valid(harness: &TestHarness) {
    let shard_count = tls_test_shard_count(harness).await;
    let slow_clients = stalled_tls_clients(harness, shard_count).await;
    let clients = harness
        .root_clients(shard_count * TLS_CLIENTS_PER_SHARD)
        .await
        .unwrap();
    for client in &clients {
        tcp_tls_scenario::run(client).await;
    }
    drop(slow_clients);
    assert_tls_client_sharding_and_cleanup(harness, &clients, "TCP-TLS", shard_count).await;
}

#[iggy_harness(
    test_client_transport = TcpTlsSelfSigned,
    server(tls = self_signed, logging.level = "info")
)]
async fn tcp_tls_self_signed_scenario_should_be_valid(harness: &TestHarness) {
    let shard_count = tls_test_shard_count(harness).await;
    let slow_clients = stalled_tls_clients(harness, shard_count).await;
    let clients = harness
        .root_clients(shard_count * TLS_CLIENTS_PER_SHARD)
        .await
        .unwrap();
    for client in &clients {
        tcp_tls_scenario::run(client).await;
    }
    drop(slow_clients);
    assert_tls_client_sharding_and_cleanup(harness, &clients, "TCP-TLS", shard_count).await;
}

#[iggy_harness(
    test_client_transport = WebSocketTlsGenerated,
    server(websocket_tls = generated, logging.level = "info")
)]
async fn websocket_tls_scenario_should_be_valid(harness: &TestHarness) {
    let shard_count = tls_test_shard_count(harness).await;
    let slow_clients = stalled_tls_clients(harness, shard_count).await;
    let clients = harness
        .root_clients(shard_count * TLS_CLIENTS_PER_SHARD)
        .await
        .unwrap();
    for client in &clients {
        websocket_tls_scenario::run(client).await;
    }
    drop(slow_clients);
    assert_tls_client_sharding_and_cleanup(harness, &clients, "WSS", shard_count).await;
}

#[iggy_harness]
async fn message_size_scenario(harness: &TestHarness) {
    message_size_scenario::run(harness).await;
}

#[iggy_harness]
async fn should_handle_single_message_per_batch_with_delayed_persistence(harness: &TestHarness) {
    single_message_per_batch_scenario::run(harness, 5).await;
}

#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn producer_reconnect_after_server_restart(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_producer(harness).await;
}

// QUIC is excluded on an SDK gap: after the restart the QUIC client redirects
// to the new leader, reconnects, and signs in, but the long-lived consumer's
// polls then return nothing for the whole window -- the post-reconnect request
// path wedges (QUIC also lacks the TCP client's mid-connection failover). TCP
// and WebSocket run.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket],
    server(
        quic.max_idle_timeout = "500s",
        quic.keep_alive_interval = "15s"
    )
)]
async fn consumer_reconnect_after_server_restart(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_consumer(harness).await;
}

#[iggy_harness]
async fn single_message_restart_offset_zero(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_single_message_offset_zero_restart(harness).await;
}

// Exercises the rejoin probe's election fallback across all replicas, which a
// plain single-node restart does not reach.
#[iggy_harness]
async fn full_cluster_restart_recovers_and_serves(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_full_cluster_restart(harness).await;
}

// Exercises `RangeEvicted` + the commit floor: the rejoin window exceeds the
// peers' evicted ring, so journal repair alone cannot cover it.
#[iggy_harness(server(partition.evicted_ring_capacity = 4096))]
async fn rejoin_window_exceeding_evicted_ring(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_ring_overflow_rejoin(harness).await;
}

#[iggy_harness]
async fn consumer_offset_ahead_after_crash(harness: &mut TestHarness) {
    reconnect_after_restart_scenario::run_consumer_offset_ahead_after_crash(harness).await;
}

/// Regression test: consumer offset skip after server restart during concurrent
/// produce+consume. Reproduces the exact scenario from issue #2924/#2715:
/// send messages, restart server, produce+consume concurrently, verify no offset
/// gaps.
///
/// Config: high messages_required_to_save so post-restart messages accumulate in
/// the journal (exposing the base_offset=0 bug).
#[iggy_harness]
async fn restart_offset_skip(harness: &mut TestHarness) {
    restart_offset_skip_scenario::run(harness).await;
}

/// This test configures the server to trigger frequent segment rotations and runs
/// multiple concurrent producers across all protocols (TCP, HTTP, QUIC, WebSocket)
/// to maximize the chance of hitting the race condition between persist_messages_to_disk
/// and handle_full_segment.
///
/// Server configuration:
/// - Smallest segment size a topic may declare (1 MiB), plus a payload sized
///   to keep rotations frequent at that floor (~240 rolls per run)
/// - Small messages_required_to_save (32) to trigger more frequent saves
///
/// Test configuration:
/// - 8 producers total (2 per protocol: TCP, HTTP, QUIC, WebSocket)
/// - All producers write to the same partition for maximum lock contention
// Concurrency race test: runs over the three VSR transports (TCP/QUIC/
// WebSocket -- HTTP/REST carries no VSR framing).
#[iggy_harness]
async fn segment_rotation_scenario(harness: &TestHarness) {
    segment_rotation_race_scenario::run(harness).await;
}

async fn tls_test_shard_count(harness: &TestHarness) -> usize {
    tokio::time::timeout(TLS_TEST_TIMEOUT, async {
        loop {
            let logs = harness.server().stdout_plain();
            if let Some(line) = logs
                .lines()
                .find(|line| line.contains("server bootstrap dispatched; awaiting shard runtimes"))
            {
                let shard_count = line
                    .split_whitespace()
                    .find_map(|field| field.strip_prefix("shards_count="))
                    .expect("bootstrap log must carry shards_count")
                    .parse::<usize>()
                    .expect("bootstrap shards_count must be numeric");
                assert!(shard_count > 0, "server must start at least one shard");
                return shard_count;
            }
            tokio::time::sleep(TLS_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("server must report its resolved shard count")
}

async fn assert_tls_client_sharding_and_cleanup(
    harness: &TestHarness,
    clients: &[IggyClient],
    transport: &str,
    shard_count: usize,
) {
    assert_tls_client_sharding(harness, clients, transport, shard_count).await;
    for client in clients {
        client.disconnect().await.unwrap();
    }

    // Each fresh observer has no partition connection and lives on a different
    // shard. Seeing every observer proves that the gather includes every shard.
    let observers = harness.root_clients(shard_count).await.unwrap();
    let observer_ids =
        assert_tls_client_sharding(harness, &observers, transport, shard_count).await;
    tokio::time::timeout(TLS_TEST_TIMEOUT, async {
        loop {
            let connected = observers[0].get_clients().await.unwrap();
            let connected_ids: HashSet<_> =
                connected.iter().map(|client| client.client_id).collect();
            if connected.len() == observers.len() && connected_ids == observer_ids {
                break;
            }
            tokio::time::sleep(TLS_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("every shard must report only its observer after client disconnect");
    for observer in observers {
        observer.disconnect().await.unwrap();
    }
}

async fn assert_tls_client_sharding(
    harness: &TestHarness,
    clients: &[IggyClient],
    transport: &str,
    shard_count: usize,
) -> HashSet<u32> {
    let mut client_ids = HashSet::with_capacity(clients.len());
    for client in clients {
        client_ids.insert(client.get_me().await.unwrap().client_id);
    }
    assert_eq!(client_ids.len(), clients.len(), "client IDs must be unique");

    // The wire exposes only the sequence tail. Match it to the router's
    // full ID and thread name to prove execution placement after handoff.
    let install_marker = format!("installing delegated {transport} client fd");
    tokio::time::timeout(TLS_TEST_TIMEOUT, async {
        loop {
            let logs = harness.server().stdout_plain();
            let mut counts = vec![0; shard_count];
            let mut installed = HashSet::new();
            for line in logs.lines().filter(|line| line.contains(&install_marker)) {
                let fields: Vec<_> = line.split_whitespace().collect();
                let full_id: u128 = fields
                    .iter()
                    .find_map(|field| field.strip_prefix("client_id="))
                    .expect("install log must carry client_id")
                    .parse()
                    .unwrap();
                let wire_id = u32::try_from(full_id & u128::from(u32::MAX)).unwrap();
                if !client_ids.contains(&wire_id) {
                    continue;
                }
                let owner = usize::try_from(full_id >> 112).unwrap();
                assert!(owner < shard_count, "invalid owner: {line}");
                assert!(
                    fields.contains(&format!("shard={owner}").as_str()),
                    "{line}"
                );
                assert!(
                    fields.contains(&format!("shard-{owner}").as_str()),
                    "{line}"
                );
                assert!(installed.insert(wire_id), "client installed twice: {line}");
                counts[owner] += 1;
            }
            if installed == client_ids {
                assert_eq!(counts, vec![clients.len() / shard_count; shard_count]);
                break;
            }
            tokio::time::sleep(TLS_POLL_INTERVAL).await;
        }
    })
    .await
    .expect("every encrypted client must be installed on its owning shard thread");
    client_ids
}

async fn stalled_tls_clients(harness: &TestHarness, shard_count: usize) -> Vec<TcpStream> {
    let address = match harness.transport().unwrap() {
        TransportProtocol::Tcp => harness.server().tcp_addr().unwrap(),
        TransportProtocol::WebSocket => harness.server().websocket_addr().unwrap(),
        transport => panic!("expected TCP-TLS or WSS, got {transport:?}"),
    };
    let mut clients = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        clients.push(TcpStream::connect(address).await.unwrap());
    }
    for _ in 0..shard_count {
        let mut invalid = TcpStream::connect(address).await.unwrap();
        invalid.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    }
    clients
}
