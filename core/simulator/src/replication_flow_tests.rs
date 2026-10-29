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

//! Flow control to a backup must not hold produces for a consensus tick.
//!
//! The primary stops sending prepares to a backup while a progress probe is
//! out, and when the bytes sent since the last probe reply reach
//! `REPLICATION_BATCH_BYTES_MAX`. A timer also probes each backup about once
//! per second. A stop that ends one probe round trip later costs little on a
//! real network. A stop that waits for the next tick, to send the probe or to
//! resume the prepares, costs up to 10 ms per wait. Steady produce repeats the
//! stops, so these waits show in the publish tail.
//!
//! The network is ordered, like the TCP link between two replicas, and each hop
//! takes one tick. A produce takes four hops: the request, the prepare, the
//! prepare ok and the reply. A probe round trip adds two hops. A produce that
//! takes longer waited for a tick.

use bytes::Bytes;
use consensus::REPLICATION_BATCH_BYTES_MAX;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::PacketSimulatorOptions;

const SEED: u64 = 0x5245_504C;
const REPLICAS: u8 = 3;
/// Leads view 0.
const PRIMARY: u8 = 0;
const ONE_WAY_DELAY_TICKS: u64 = 1;
/// The request, the prepare, the prepare ok and the reply.
const PRODUCE_HOPS: u64 = 4;
/// A probe and its reply.
const PROBE_HOPS: u64 = 2;
const LATENCY_TICKS_MAX: u64 = (PRODUCE_HOPS + PROBE_HOPS) * ONE_WAY_DELAY_TICKS;
/// A client has one produce in flight, so one produce per tick needs more
/// clients than the slowest produce takes ticks.
const CLIENTS: u8 = 16;

#[test]
fn given_steady_produce_when_a_backup_window_fills_should_answer_within_one_probe_round_trip() {
    const REQUEST_BYTES: u64 = 512 * 1024;
    /// Bytes sent to each backup, counted in windows.
    const WINDOWS: u64 = 3;
    assert_produce_latency(
        REQUEST_BYTES,
        WINDOWS * REPLICATION_BATCH_BYTES_MAX / REQUEST_BYTES,
    );
}

#[test]
fn given_steady_produce_when_the_progress_timer_probes_a_backup_should_answer_within_one_probe_round_trip()
 {
    /// Never fills a window, so only the timer sends probes.
    const REQUEST_BYTES: u64 = 1024;
    /// A backup is probed 1 s (`PROGRESS_INTERVAL_MICROS`) after the first
    /// prepare that follows the last probe reply: 100 ticks of 10 ms.
    const PROBE_INTERVAL_TICKS: u64 = 100;
    /// Four intervals, so the timer probes each backup more than once.
    const TICKS: u64 = 4 * PROBE_INTERVAL_TICKS;
    assert_produce_latency(REQUEST_BYTES, TICKS);
}

/// Sends one produce per tick for `ticks` ticks, like a producer at a fixed
/// rate, and asserts that each one is answered within [`LATENCY_TICKS_MAX`].
fn assert_produce_latency(request_bytes: u64, ticks: u64) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let network = PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: CLIENTS,
        seed: SEED,
        one_way_delay_min: ONE_WAY_DELAY_TICKS,
        ordered: true,
        ..PacketSimulatorOptions::default()
    };
    let client_ids = 1..=u128::from(CLIENTS);
    let mut sim = Simulator::new(usize::from(REPLICAS), client_ids.clone(), network);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    let clients: Vec<SimClient> = client_ids.map(SimClient::new).collect();
    for client in &clients {
        sim.register_client_with_primary(client);
    }

    let payload = Bytes::from(vec![0; usize::try_from(request_bytes).unwrap()]);
    let mut sent_at: Vec<Option<u64>> = vec![None; clients.len()];
    let mut late = Vec::new();
    // The produces sent last get the whole bound to finish.
    for tick in 0..ticks + LATENCY_TICKS_MAX {
        if tick < ticks
            && let Some(idle) = sent_at.iter().position(Option::is_none)
        {
            let request = clients[idle].send_messages(namespace, std::slice::from_ref(&payload));
            sim.submit_request(clients[idle].client_id(), PRIMARY, request.into_generic());
            sent_at[idle] = Some(tick);
        }
        for (_, reply) in sim.step_with_senders() {
            let header = reply.header();
            assert_eq!(header.status, 0, "produce failed at tick {tick}");
            let client = clients
                .iter()
                .position(|client| client.client_id() == header.client)
                .expect("reply to a test client");
            let sent = sent_at[client].take().expect("one reply per produce");
            if tick - sent > LATENCY_TICKS_MAX {
                late.push((sent, Some(tick - sent)));
            }
        }
    }
    late.extend(sent_at.into_iter().flatten().map(|sent| (sent, None)));
    late.sort_unstable();
    assert!(
        late.is_empty(),
        "every produce must be answered within {LATENCY_TICKS_MAX} ticks: {PRODUCE_HOPS} hops \
         and one probe round trip of {PROBE_HOPS}, so a slower one waited for a timer tick. \
         (tick sent, ticks to the reply or None without one): {late:?}"
    );
}
