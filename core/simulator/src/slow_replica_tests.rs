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
//! view-change status timeout to arrive. A replica that times out into the
//! next view before its `StartViewChange` lands stays one view ahead of every
//! vote it sends, and each vote that lands takes replicas 0 and 1 out of their
//! working view while they commit together. Nothing but latency is injected
//! after the cut heals: no packet is dropped.
//!
//! Replica 2 can also hear a single peer late while every other link stays
//! fast. On a real network a slow egress of replica 2 delays the TCP
//! acknowledgements of the prepares replica 1 forwards to it down the chain, so
//! they queue on that link, and every frame replica 1 sends after them waits in
//! the same queue. Replica 2 then gives up on each view replica 1 leads before
//! its `StartView` arrives, and the rebalancer must not keep handing primaries
//! back to replica 1.
//!
//! A replica that stops hearing a primary its peers still hear must not depose
//! it, on either plane, while a primary that crashed or was cut off is still
//! replaced as fast as before.

use bytes::Bytes;
use consensus::{PartitionsHandle, Status, TimeoutManager};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, RequestPreparesHeader};
use server_common::sharding::IggyNamespace;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::{Packet, ProcessId};
use crate::partition_repair_driver_tests::cluster;
use crate::workload::{
    self, Workload,
    invariants::Invariants,
    options::{ActionWeights, WorkloadOptions},
};

const SEED: u64 = 0x5052_494D;
const SLOW: u8 = 2;
/// Leads the single group of [`Run::single`] and the metadata plane.
const LEADER: u8 = 0;
/// The peer replica 2 hears late: its predecessor in the chain of every group
/// that replica 0 or 1 leads.
const LAGGING_PEER: u8 = 1;
/// Groups the rebalancer spreads across the replicas, so each replica leads
/// one before the fault.
const GROUPS: usize = 3;
/// Ticks for the rebalancer to spread the primaries.
const SPREAD_TICKS: u64 = 10_000;
/// Ticks for a single group to settle under the workload before a fault, as
/// long as the spread, so the rebalancer had the same time to act.
const SETTLE_TICKS: u64 = SPREAD_TICKS;
/// Ten view-change status timeouts: a replica cut off this long times out
/// again and again.
const PARTITION_TICKS: u64 = 10 * TimeoutManager::VIEW_CHANGE_STATUS_TICKS;
/// A backup times out a heartbeat timeout after the last heartbeat it heard,
/// and that heartbeat left the primary at most one heartbeat interval before
/// the fault. Every backup heard one in that interval, so waiting for the
/// backup that times out last adds nothing to the bound. The view change
/// itself must finish within one retransmit interval of its votes.
const ELECTION_TICKS_MAX: u64 = TimeoutManager::NORMAL_HEARTBEAT_TICKS
    + TimeoutManager::COMMIT_MESSAGE_TICKS
    + TimeoutManager::START_VIEW_CHANGE_MESSAGE_TICKS;
/// Past `NORMAL_HEARTBEAT_TICKS` (500): the cut lasts long enough that the
/// group replica 2 led fails over and replica 2 starts a view change itself.
const CUT_TICKS: u64 = 600;
/// One-way delay on every link out of replica 2, just past
/// `VIEW_CHANGE_STATUS_TICKS` (500). Below that bound the healthy pair adopts
/// replica 2's view before it times out again, and the views settle.
const EGRESS_DELAY_TICKS: u64 = 520;
/// One-way delay from the lagging peer to replica 2: 20 s, inside the 15 to
/// 55 s the queue took on a real cluster. Any delay past
/// `VIEW_CHANGE_STATUS_TICKS` (500) loses the `StartView` of that peer.
const LAGGING_PEER_DELAY_TICKS: u64 = 2_000;
/// Measured after the fault starts: 300 s of virtual time at 10 ms per tick.
const MEASURE_TICKS: u64 = 30_000;
/// The heal itself may cost a few view changes while replica 2 rejoins: up to 2
/// per group here with 450, 520 or 1200 ticks of delay.
const VIEW_CHANGES_MAX: u32 = 5;
/// Summed over the groups, because the rebalancer backs off per target: one
/// view change when replica 2 leaves the view replica 1 leads at the start,
/// then two for each primary moved to replica 1 that replica 2 cannot follow.
/// The waits after the failed moves, 30, 60 and 120 s, leave room for four such
/// moves in the window, and a fifth would need 450 s. Without the backoff a
/// move fails every few seconds.
const LAGGING_PEER_VIEW_CHANGES_MAX: u32 = 9;

#[test]
#[allow(clippy::too_many_lines)]
fn given_a_missed_committed_prepare_when_pull_repair_is_unavailable_should_rejoin_live_delivery_under_load()
 {
    const WRITES: usize = 24;
    const TICKS_PER_WRITE: usize = 250;
    static GROUP: AtomicU64 = AtomicU64::new(0);
    static MISSING: AtomicU64 = AtomicU64::new(0);
    static WITHHOLD: AtomicBool = AtomicBool::new(true);
    static DROPPED: AtomicU64 = AtomicU64::new(0);

    fn withhold_prepare(packet: &Packet) -> bool {
        if !WITHHOLD.load(Ordering::Relaxed) || packet.message.header().command != Command::Prepare
        {
            return false;
        }
        let message = packet.message.try_as_typed::<PrepareHeader>().unwrap();
        let header = message.header();
        if header.group == GROUP.load(Ordering::Relaxed)
            && header.op == MISSING.load(Ordering::Relaxed)
        {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        false
    }

    fn block_pull_repair(packet: &Packet) -> bool {
        packet.message.header().command == Command::RequestPrepares
            && packet
                .message
                .try_as_typed::<RequestPreparesHeader>()
                .unwrap()
                .header()
                .group
                == GROUP.load(Ordering::Relaxed)
    }

    let (mut sim, client) = cluster(SEED);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);
    let warmup = client
        .send_messages(namespace, &[Bytes::from_static(b"warmup")])
        .into_generic();
    let reply = sim.await_setup_reply(
        &client,
        LEADER,
        &warmup,
        Operation::SendMessages,
        "warmup prefix",
    );
    assert_eq!(reply.header().status, 0);
    assert!((0..500).any(|_| {
        sim.step();
        sim.partition_consensus_state(usize::from(SLOW), namespace)
            .is_some_and(|state| state.commit_min >= 1)
    }));
    GROUP.store(namespace.inner(), Ordering::Relaxed);
    MISSING.store(2, Ordering::Relaxed);
    WITHHOLD.store(true, Ordering::Relaxed);
    DROPPED.store(0, Ordering::Relaxed);
    *sim.network
        .link_drop_packet_fn(ProcessId::Replica(LEADER), ProcessId::Replica(SLOW)) =
        Some(withhold_prepare);
    for peer in [0, 1] {
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(SLOW), ProcessId::Replica(peer)) =
            Some(block_pull_repair);
    }
    let repair_prepares = sim.network.command_counts()[Command::RepairPrepare as usize];
    let request = client
        .send_messages(namespace, &[Bytes::from_static(b"missed")])
        .into_generic();
    let reply = sim.await_setup_reply(
        &client,
        LEADER,
        &request,
        Operation::SendMessages,
        "missed prepare setup",
    );
    assert_eq!(reply.header().status, 0);
    assert!(DROPPED.load(Ordering::Relaxed) > 0);
    assert!(
        sim.replicas[usize::from(SLOW)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .unwrap()
            .consensus()
            .commit_min()
            < 2
    );
    WITHHOLD.store(false, Ordering::Relaxed);

    let mut caught_under_load = false;
    for index in 0..WRITES {
        let request = client
            .send_messages(namespace, &[Bytes::from(format!("live-{index}"))])
            .into_generic();
        let reply = sim.await_setup_reply(
            &client,
            LEADER,
            &request,
            Operation::SendMessages,
            "continued load",
        );
        assert_eq!(reply.header().status, 0);
        for _ in 0..TICKS_PER_WRITE {
            sim.step();
        }
        let state = sim
            .partition_consensus_state(usize::from(SLOW), namespace)
            .unwrap();
        caught_under_load |= index < WRITES - 1 && state.commit_min >= reply.header().op;
        assert_eq!((state.status, state.view), (Status::Normal, 0));
    }
    assert!(
        caught_under_load,
        "the replica must catch up before writes stop: source {:?}, backup {:?}, progress {:?}",
        sim.partition_consensus_state(usize::from(LEADER), namespace),
        sim.partition_consensus_state(usize::from(SLOW), namespace),
        sim.replicas[usize::from(SLOW)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .map(|partition| (
                partition.held_through(),
                partition.replication_progress(),
                partition.view_fenced(),
                partition.repair
            )),
    );
    assert!(sim.network.delivered_any(Command::PrepareProgress));
    assert_eq!(
        sim.network.command_counts()[Command::RepairPrepare as usize],
        repair_prepares,
        "the committed hole must heal through ordered live replay while pull repair is blocked"
    );
}

#[test]
fn given_a_replica_slower_than_the_view_change_timeout_when_it_rejoins_should_not_keep_the_healthy_pair_changing_views()
 {
    let mut run = Run::spread();
    set_peer_links(&mut run.sim, SLOW, false);
    for _ in 0..CUT_TICKS {
        run.tick();
    }
    set_peer_links(&mut run.sim, SLOW, true);
    for peer in (0..run.sim.replica_count).filter(|&peer| peer != SLOW) {
        *run.sim
            .network
            .link_extra_delay(ProcessId::Replica(SLOW), ProcessId::Replica(peer)) =
            EGRESS_DELAY_TICKS;
    }
    let changes = run.measure_view_changes();
    for (namespace, &count) in run.namespaces.iter().zip(&changes) {
        assert!(
            count <= VIEW_CHANGES_MAX,
            "replicas 0 and 1 changed view {count} times for {namespace:?} in {MEASURE_TICKS} \
             ticks, while only replica 2 was slow: one replica whose messages arrive later \
             than the view-change status timeout keeps the healthy pair leaving its working \
             view (changes per group {changes:?})"
        );
    }
}

#[test]
fn given_a_replica_that_hears_one_peer_late_when_primaries_are_rebalanced_should_not_keep_the_healthy_pair_changing_views()
 {
    let mut run = Run::spread();
    *run.sim
        .network
        .link_extra_delay(ProcessId::Replica(LAGGING_PEER), ProcessId::Replica(SLOW)) =
        LAGGING_PEER_DELAY_TICKS;
    let changes = run.measure_view_changes();
    let total: u32 = changes.iter().sum();
    assert!(
        total <= LAGGING_PEER_VIEW_CHANGES_MAX,
        "replicas 0 and 1 changed view {total} times in {MEASURE_TICKS} ticks, while replica 2 \
         heard replica 1 late: replica 2 leaves every view replica 1 leads before it hears of \
         it, and each primary moved back to replica 1 starts the view changes again (changes \
         per group {changes:?})"
    );
}

#[test]
fn given_a_replica_that_hears_the_primary_late_should_not_depose_it() {
    let mut run = Run::single();
    let metadata_view = healthy_metadata_view(&run.sim);
    *run.sim
        .network
        .link_extra_delay(ProcessId::Replica(LEADER), ProcessId::Replica(SLOW)) =
        LAGGING_PEER_DELAY_TICKS;

    let changes = run.measure_view_changes();

    assert_eq!(
        (changes[0], healthy_metadata_view(&run.sim)),
        (0, metadata_view),
        "replica 2 heard replica 0 {LAGGING_PEER_DELAY_TICKS} ticks late, and replicas 0 and 1, \
         which hear it in time, followed replica 2 out of its view: (partition view changes, \
         metadata view)"
    );
}

#[test]
fn given_a_backup_cut_off_for_many_view_change_timeouts_when_it_heals_should_not_change_the_view() {
    let mut run = Run::single();
    let metadata_view = healthy_metadata_view(&run.sim);
    let view = healthy_view(&run.sim, run.namespaces[0]);
    set_peer_links(&mut run.sim, SLOW, false);
    for _ in 0..PARTITION_TICKS {
        run.tick();
    }
    set_peer_links(&mut run.sim, SLOW, true);

    let changes = run.measure_view_changes();

    assert_eq!(
        (
            healthy_view(&run.sim, run.namespaces[0]),
            changes[0],
            healthy_metadata_view(&run.sim)
        ),
        (view, 0, metadata_view),
        "replica 2 was cut off for {PARTITION_TICKS} ticks, and after the heal replicas 0 and 1 \
         left the view of a primary they never stopped hearing: (partition view, partition view \
         changes after the heal, metadata view)"
    );
}

#[test]
fn given_a_crashed_primary_should_elect_a_new_one_within_the_election_bound() {
    let mut run = Run::single();
    run.sim.replica_crash(LEADER);

    assert!(
        run.others_lead_within_election_bound(LEADER),
        "replicas 1 and 2 did not elect a partition and a metadata primary within \
         {ELECTION_TICKS_MAX} ticks of the crash of replica 0"
    );
}

#[test]
fn given_a_primary_cut_off_should_be_replaced_and_rejoin_as_a_backup() {
    let mut run = Run::single();
    let namespace = run.namespaces[0];
    set_peer_links(&mut run.sim, LEADER, false);

    assert!(
        run.others_lead_within_election_bound(LEADER),
        "replicas 1 and 2 did not elect a partition and a metadata primary within \
         {ELECTION_TICKS_MAX} ticks of cutting off replica 0"
    );
    let views = run.views_of(LEADER + 1, namespace);
    set_peer_links(&mut run.sim, LEADER, true);
    for _ in 0..PARTITION_TICKS {
        run.tick();
    }

    assert_eq!(
        (
            run.views_of(LEADER + 1, namespace),
            run.views_of(LEADER, namespace)
        ),
        (views, views),
        "after the heal replica 0 must adopt the views replicas 1 and 2 elected without \
         another view change: ((partition, metadata) of replica 1, of replica 0)"
    );
    let rejoined = run
        .sim
        .partition_consensus_state(usize::from(LEADER), namespace)
        .is_some_and(|state| state.status == Status::Normal && !state.is_primary);
    assert!(rejoined, "replica 0 must rejoin the partition as a backup");
}

/// The groups after the rebalancer spread them, with the workload that keeps
/// them committing.
struct Run {
    sim: Simulator,
    workload: Workload,
    clients: [SimClient; 1],
    invariants: Invariants,
    namespaces: Vec<IggyNamespace>,
}

impl Run {
    fn spread() -> Self {
        Self::new(GROUPS, SPREAD_TICKS)
    }

    /// One group that replica 0 leads, like the metadata plane.
    fn single() -> Self {
        let run = Self::new(1, SETTLE_TICKS);
        assert_eq!(
            (
                run.sim.primary_index(run.namespaces[0]),
                run.sim.metadata_primary_index()
            ),
            (Some(LEADER), Some(LEADER)),
            "replica 0 must lead the group and the metadata plane before the fault"
        );
        run
    }

    fn new(groups: usize, settle_ticks: u64) -> Self {
        let (mut sim, client) = cluster(SEED);
        let namespaces: Vec<_> = (0..groups)
            .map(|partition| IggyNamespace::new(1, 1, partition))
            .collect();
        for &namespace in &namespaces {
            sim.init_partition(namespace);
        }
        sim.register_client_with_primary(&client);
        let mut options = WorkloadOptions::new(SEED, sim.replica_count, namespaces.clone());
        options.weights = ActionWeights::partition_only();
        let mut run = Self {
            sim,
            workload: Workload::new(options),
            clients: [client],
            invariants: Invariants::new(),
            namespaces,
        };
        for _ in 0..settle_ticks {
            run.tick();
        }
        run
    }

    fn tick(&mut self) -> u64 {
        workload::run(
            &mut self.sim,
            &mut self.workload,
            &self.clients,
            1,
            u64::MAX,
            &mut self.invariants,
        )
    }

    /// Runs `MEASURE_TICKS` under the fault already in place and returns how
    /// often replicas 0 and 1 changed view in each group.
    fn measure_view_changes(&mut self) -> Vec<u32> {
        let mut views: Vec<u32> = self
            .namespaces
            .iter()
            .map(|&namespace| healthy_view(&self.sim, namespace))
            .collect();
        let mut changes = vec![0_u32; self.namespaces.len()];
        let mut replies = 0;
        for _ in 0..MEASURE_TICKS {
            replies += self.tick();
            for (index, &namespace) in self.namespaces.iter().enumerate() {
                let view = healthy_view(&self.sim, namespace);
                if view != views[index] {
                    changes[index] += 1;
                    views[index] = view;
                }
            }
        }
        assert!(
            replies > 0,
            "the workload must keep committing under the fault"
        );
        changes
    }

    /// Whether a replica other than `failed` becomes a `Normal` primary of the
    /// group and of the metadata plane within `ELECTION_TICKS_MAX`.
    fn others_lead_within_election_bound(&mut self, failed: u8) -> bool {
        (0..ELECTION_TICKS_MAX).any(|_| {
            self.tick();
            let mut others = (0..self.sim.replica_count).filter(|&replica| replica != failed);
            let namespace = self.namespaces[0];
            others.clone().any(|replica| {
                self.sim
                    .partition_consensus_state(usize::from(replica), namespace)
                    .is_some_and(|state| state.is_primary && state.status == Status::Normal)
            }) && others.any(|replica| {
                self.sim
                    .metadata_consensus(usize::from(replica))
                    .is_some_and(|consensus| {
                        consensus.is_primary() && consensus.status() == Status::Normal
                    })
            })
        })
    }

    /// The partition and metadata views of `replica`.
    fn views_of(&self, replica: u8, namespace: IggyNamespace) -> (Option<u32>, Option<u32>) {
        (
            self.sim
                .partition_consensus_state(usize::from(replica), namespace)
                .map(|state| state.view),
            self.sim
                .metadata_consensus(usize::from(replica))
                .map(consensus::VsrConsensus::view),
        )
    }
}

/// Cuts or restores every link between `replica` and its peers.
fn set_peer_links(sim: &mut Simulator, replica: u8, enabled: bool) {
    for peer in (0..sim.replica_count).filter(|&peer| peer != replica) {
        sim.network.set_link_filter(
            ProcessId::Replica(replica),
            ProcessId::Replica(peer),
            enabled,
        );
        sim.network.set_link_filter(
            ProcessId::Replica(peer),
            ProcessId::Replica(replica),
            enabled,
        );
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

/// The highest metadata view replicas 0 and 1 hold.
fn healthy_metadata_view(sim: &Simulator) -> u32 {
    (0..sim.replica_count)
        .filter(|&replica| replica != SLOW)
        .filter_map(|replica| sim.metadata_consensus(usize::from(replica)))
        .map(consensus::VsrConsensus::view)
        .max()
        .unwrap_or(0)
}
