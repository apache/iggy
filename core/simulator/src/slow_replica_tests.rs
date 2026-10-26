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

//! One slow replica must not keep the two healthy replicas changing views.
//!
//! Replica 2 is cut off for a moment, long enough to start a view change of its
//! own, and every message it sends from then on takes longer than the
//! view-change status timeout to arrive. Its `StartViewChange` for the next
//! view therefore lands after it already timed out into the view after that.
//! Replicas 0 and 1 are Normal and commit together, yet each such
//! `StartViewChange` takes them out of their working view, and the primary
//! keeps moving between them. Nothing but latency is injected after the cut
//! heals: no packet is dropped.

use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::packet::ProcessId;
use crate::partition_repair_driver_tests::cluster;
use crate::workload::{
    self, Workload,
    invariants::Invariants,
    options::{ActionWeights, WorkloadOptions},
};

const SEED: u64 = 0x5052_494D;
const SLOW: u8 = 2;
/// Groups the rebalancer spreads across the replicas, so each replica leads
/// one before the fault.
const GROUPS: usize = 3;
/// Ticks for the rebalancer to spread the primaries.
const SPREAD_TICKS: u64 = 10_000;
/// Past `NORMAL_HEARTBEAT_TICKS` (500): the cut lasts long enough that the
/// group replica 2 led fails over and replica 2 starts a view change itself.
const CUT_TICKS: u64 = 600;
/// One-way delay on every link out of replica 2, just past
/// `VIEW_CHANGE_STATUS_TICKS` (500). Below that bound the healthy pair adopts
/// replica 2's view before it times out again, and the views settle.
const EGRESS_DELAY_TICKS: u64 = 520;
/// Measured after the heal: 300 s of virtual time at 10 ms per tick.
const MEASURE_TICKS: u64 = 30_000;
/// The heal itself may cost a few view changes while replica 2 rejoins: 450
/// ticks of delay costs up to 3 here, 520 ticks costs about 60.
const VIEW_CHANGES_MAX: u32 = 5;

#[test]
#[ignore = "known bug: a replica slower than the view-change status timeout keeps the healthy pair leaving its working view; run with --ignored"]
fn given_a_replica_slower_than_the_view_change_timeout_when_it_rejoins_should_not_keep_the_healthy_pair_changing_views()
 {
    let (mut sim, client) = cluster(SEED);
    let namespaces: Vec<_> = (0..GROUPS)
        .map(|partition| IggyNamespace::new(1, 1, partition))
        .collect();
    for &namespace in &namespaces {
        sim.init_partition(namespace);
    }
    sim.register_client_with_primary(&client);
    let mut options = WorkloadOptions::new(SEED, sim.replica_count, namespaces.clone());
    options.weights = ActionWeights::partition_only();
    let mut workload = Workload::new(options);
    let clients = [client];
    let mut invariants = Invariants::new();
    let mut tick = |sim: &mut Simulator| {
        workload::run(sim, &mut workload, &clients, 1, u64::MAX, &mut invariants)
    };

    for _ in 0..SPREAD_TICKS {
        tick(&mut sim);
    }
    set_peer_links(&mut sim, false);
    for _ in 0..CUT_TICKS {
        tick(&mut sim);
    }
    set_peer_links(&mut sim, true);
    for peer in (0..sim.replica_count).filter(|&peer| peer != SLOW) {
        *sim.network
            .link_extra_delay(ProcessId::Replica(SLOW), ProcessId::Replica(peer)) =
            EGRESS_DELAY_TICKS;
    }

    let first_views: Vec<u32> = namespaces
        .iter()
        .map(|&namespace| healthy_view(&sim, namespace))
        .collect();
    let mut views = first_views.clone();
    let mut changes = vec![0_u32; GROUPS];
    let mut replies = 0;
    for _ in 0..MEASURE_TICKS {
        replies += tick(&mut sim);
        for (index, &namespace) in namespaces.iter().enumerate() {
            let view = healthy_view(&sim, namespace);
            if view != views[index] {
                changes[index] += 1;
                views[index] = view;
            }
        }
    }
    assert!(
        replies > 0,
        "the workload must keep committing after the heal"
    );
    for (index, &namespace) in namespaces.iter().enumerate() {
        assert!(
            changes[index] <= VIEW_CHANGES_MAX,
            "replicas 0 and 1 changed view {} times for {namespace:?} in {MEASURE_TICKS} ticks \
             (view {} -> {}), while only replica 2 was slow: one replica whose messages \
             arrive later than the view-change status timeout keeps the healthy pair leaving \
             its working view (changes per group {changes:?})",
            changes[index],
            first_views[index],
            views[index],
        );
    }
}

/// Cuts or restores every link between the slow replica and its peers.
fn set_peer_links(sim: &mut Simulator, enabled: bool) {
    for peer in (0..sim.replica_count).filter(|&peer| peer != SLOW) {
        sim.network
            .set_link_filter(ProcessId::Replica(SLOW), ProcessId::Replica(peer), enabled);
        sim.network
            .set_link_filter(ProcessId::Replica(peer), ProcessId::Replica(SLOW), enabled);
    }
}

/// The highest view replicas 0 and 1 hold for `namespace`.
fn healthy_view(sim: &Simulator, namespace: IggyNamespace) -> u32 {
    (0..sim.replica_count)
        .filter(|&replica| replica != SLOW)
        .filter_map(|replica| sim.partition_consensus_state(usize::from(replica), namespace))
        .map(|state| state.view)
        .max()
        .unwrap_or(0)
}
