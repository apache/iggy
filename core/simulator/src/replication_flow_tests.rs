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

//! Flow control must not hold back the prepares of a backup that keeps up.
//!
//! The primary charges each prepare it sends to a backup against a window of
//! `REPLICATION_BATCH_BYTES_MAX` bytes. A progress probe follows the prepares on
//! the same ordered link, and its reply returns the credit of the bytes sent
//! before it. Probes go out at half a window and about once per second, and no
//! prepare waits for a probe reply. So a backup that keeps up never fills its
//! window, and no prepare goes out late or by replay.
//!
//! The network is ordered, like the TCP link between two replicas, and each hop
//! takes one tick. A produce takes four hops: the request, the prepare, the
//! prepare ok and the reply. A produce that takes longer waited for flow
//! control, and the other backup can hide that wait. So the flow counters must
//! also stay at zero.

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
const LATENCY_TICKS_MAX: u64 = PRODUCE_HOPS * ONE_WAY_DELAY_TICKS;
/// A client has one produce in flight, so one produce per tick needs more
/// clients than the slowest produce takes ticks.
const CLIENTS: u8 = 16;

#[test]
fn given_steady_produce_when_it_passes_several_windows_should_answer_without_flow_control_waits() {
    const REQUEST_BYTES: u64 = 512 * 1024;
    /// Bytes sent to each backup, counted in windows.
    const WINDOWS: u64 = 3;
    assert_produce_latency(
        REQUEST_BYTES,
        WINDOWS * REPLICATION_BATCH_BYTES_MAX / REQUEST_BYTES,
    );
}

#[test]
fn given_steady_produce_when_the_progress_timer_probes_a_backup_should_answer_without_flow_control_waits()
 {
    /// Never fills a window, so only the timer sends probes.
    const REQUEST_BYTES: u64 = 1024;
    /// The timer probes a backup 1 s (`PROGRESS_INTERVAL_MICROS`) after its
    /// last probe reply: 100 ticks of 10 ms.
    const PROBE_INTERVAL_TICKS: u64 = 100;
    /// Four intervals, so the timer probes each backup more than once.
    const TICKS: u64 = 4 * PROBE_INTERVAL_TICKS;
    assert_produce_latency(REQUEST_BYTES, TICKS);
}

/// Sends one produce per tick for `ticks` ticks, like a producer at a fixed
/// rate. Asserts that each one is answered within [`LATENCY_TICKS_MAX`], and
/// that flow control refused no live prepare and replayed none.
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
        "every produce must be answered within its {PRODUCE_HOPS} hops, so a slower one waited \
         for flow control. (tick sent, ticks to the reply or None without one): {late:?}"
    );
    let shards = sim
        .replicas
        .iter()
        .flat_map(|replica| replica.shards.iter());
    let (refusals, replayed) = shards.fold((0, 0), |(refusals, replayed), shard| {
        let metrics = shard.metrics();
        (
            refusals + metrics.replication_live_refusals_value(),
            replayed + metrics.replication_replayed_prepares_value(),
        )
    });
    assert_eq!(
        (refusals, replayed),
        (0, 0),
        "flow control must not refuse or replay prepares while every backup keeps up: \
         (live refusals, replayed prepares)"
    );
}
