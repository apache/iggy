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

//! A replica must never commit a prepare the cluster did not commit.
//!
//! The old primary journals an op, is cut off before any peer sees it, and the
//! other two replicas elect a new view that commits a DIFFERENT prepare at the
//! same op. Whether the old primary rejoins that view or is later elected over
//! it, its stale prepare sits below the view's canonical headers, and it must
//! end up serving the view's entry at that offset, not its own.

use bytes::Bytes;
use consensus::{PartitionsHandle, Status};
use futures::FutureExt;
use iggy_common::PollingStrategy;
use partitions::{PollingArgs, PollingConsumer};
use server_common::send_messages::{BatchIntegrity, decode_batch_slice_with};
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::ProcessId;
use crate::partition_repair_driver_tests::{cluster, group_state};

const REPLICAS: u8 = 3;
const OLD_PRIMARY: u8 = 0;
const STEPS_PER_SEND: usize = 12;
/// Well past `NORMAL_HEARTBEAT_TICKS` (500), so the majority elects.
const ELECTION_STEPS: usize = 5_000;
const SETTLE_STEPS: usize = 5_000;
/// Long enough for a lone survivor's view changes to rotate onto the old primary.
const REJOIN_STEPS: usize = 40_000;

fn send(sim: &mut Simulator, client: &SimClient, namespace: IggyNamespace, to: u8, payload: &str) {
    let request = client.send_messages(namespace, &[Bytes::from(payload.to_owned())]);
    sim.submit_request(client.client_id(), to, request.into_generic());
    for _ in 0..STEPS_PER_SEND {
        sim.step();
    }
}

fn isolate(sim: &mut Simulator, replica: u8, isolated: bool) {
    for peer in (0..REPLICAS).filter(|peer| *peer != replica) {
        sim.network.set_link_filter(
            ProcessId::Replica(replica),
            ProcessId::Replica(peer),
            !isolated,
        );
        sim.network.set_link_filter(
            ProcessId::Replica(peer),
            ProcessId::Replica(replica),
            !isolated,
        );
    }
}

/// `(offset, payload)` for every message a consumer can poll from `replica`,
/// read through the ordinary client poll path.
fn polled(sim: &mut Simulator, replica: u8, namespace: IggyNamespace) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    loop {
        let next = out.last().map_or(0, |(offset, _)| offset + 1);
        let args = PollingArgs::new(PollingStrategy::offset(next), 1_000, false);
        let poll = sim.poll_messages(
            usize::from(replica),
            namespace,
            PollingConsumer::Consumer(1, 0),
            &args,
        );
        let (tx, rx) = shard::channel(1);
        sim.executor.spawn(async move {
            let _ = tx.try_send(poll.await);
        });
        sim.run_pumps();
        let fragments = rx
            .recv()
            .now_or_never()
            .expect("poll completes")
            .expect("reply channel open")
            .expect("poll accepted");
        let before = out.len();
        for fragment in &fragments {
            let mut bytes = fragment.as_slice();
            while let Ok(batch) = decode_batch_slice_with(bytes, BatchIntegrity::LayoutOnly) {
                for message in &batch {
                    out.push((
                        batch.header.base_offset + u64::from(message.header.offset_delta),
                        String::from_utf8_lossy(message.payload).into_owned(),
                    ));
                }
                let Some(rest) = bytes.get(batch.header.total_size()..) else {
                    break;
                };
                bytes = rest;
            }
        }
        if out.len() == before {
            return out;
        }
    }
}

#[test]
fn given_an_old_primary_holding_an_uncommitted_prepare_when_it_rejoins_a_view_that_committed_another_prepare_at_that_op_should_not_commit_its_own()
 {
    let (mut sim, client) = cluster(0x5EED_57A1);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);

    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("warmup-{index}"),
        );
    }
    let (_, _, warm_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert_eq!(warm_commit, 3, "warmup did not commit on the old primary");

    // Cut the old primary off, then hand it a request: it journals op 4 and
    // can never replicate it.
    isolate(&mut sim, OLD_PRIMARY, true);
    send(&mut sim, &client, namespace, OLD_PRIMARY, "stale");

    // The majority elects a new view without it.
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = primary_of(&sim, 1, namespace, new_view);
    assert_ne!(new_primary, OLD_PRIMARY);

    // The new view commits different prepares at op 4 and beyond.
    for index in 0..4 {
        send(
            &mut sim,
            &client,
            namespace,
            new_primary,
            &format!("fresh-{index}"),
        );
    }
    let (_, _, majority_commit, _) = group_state(&sim, new_primary, namespace);
    assert!(
        majority_commit >= 5,
        "the new view committed only through {majority_commit}"
    );

    isolate(&mut sim, OLD_PRIMARY, false);
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }
    let (_, _, rejoined_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert!(
        rejoined_commit >= majority_commit,
        "the old primary never caught up: committed {rejoined_commit} of {majority_commit}"
    );

    let reference = polled(&mut sim, new_primary, namespace);
    let rejoined = polled(&mut sim, OLD_PRIMARY, namespace);
    assert!(
        !reference.iter().any(|(_, payload)| payload == "stale"),
        "the new view committed the isolated primary's request: {reference:?}"
    );
    assert_eq!(
        rejoined, reference,
        "the rejoined replica serves a committed log that differs from the view's"
    );
}

/// The primary-elect twin: the old primary never adopts the majority's view. It is
/// elected later, with its stale op under the merged log's commit point, where the
/// merged headers do not reach.
#[test]
fn given_an_old_primary_holding_an_uncommitted_prepare_when_it_is_elected_over_a_log_that_committed_another_prepare_should_not_commit_its_own()
 {
    let (mut sim, client) = cluster(0x5EED_57A1);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);
    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("warmup-{index}"),
        );
    }
    isolate(&mut sim, OLD_PRIMARY, true);
    send(&mut sim, &client, namespace, OLD_PRIMARY, "stale");
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = primary_of(&sim, 1, namespace, new_view);
    for index in 0..4 {
        send(
            &mut sim,
            &client,
            namespace,
            new_primary,
            &format!("fresh-{index}"),
        );
    }
    let (_, _, majority_commit, _) = group_state(&sim, new_primary, namespace);
    assert!(
        majority_commit >= 5,
        "the new view committed only through {majority_commit}"
    );

    // Cut the new primary off and keep the old one apart until the survivor's view
    // changes reach a view the old primary leads; then let those two meet.
    let survivor = (0..REPLICAS)
        .find(|replica| *replica != OLD_PRIMARY && *replica != new_primary)
        .expect("three replicas");
    isolate(&mut sim, new_primary, true);
    (0..REJOIN_STEPS)
        .find(|_| {
            sim.step();
            let (_, view, _, _) = group_state(&sim, survivor, namespace);
            primary_of(&sim, survivor, namespace, view) == OLD_PRIMARY
        })
        .expect("the survivor never reached a view the old primary leads");
    connect(&mut sim, OLD_PRIMARY, survivor);
    let led_view = (0..SETTLE_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, OLD_PRIMARY, namespace);
            (status == Status::Normal
                && view > new_view
                && primary_of(&sim, OLD_PRIMARY, namespace, view) == OLD_PRIMARY)
                .then_some(view)
        })
        .expect("the old primary never led a view: the primary-elect path did not run");

    isolate(&mut sim, new_primary, false);
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }
    let reference = polled(&mut sim, survivor, namespace);
    let elected = polled(&mut sim, OLD_PRIMARY, namespace);
    assert!(
        !reference.iter().any(|(_, payload)| payload == "stale"),
        "view {led_view} committed the isolated primary's request: {reference:?}"
    );
    assert_eq!(
        elected, reference,
        "the elected old primary serves a committed log that differs from the survivor's"
    );
}

/// A rejoining replica fences everything above its commit point, and while fenced
/// its commit point cannot move. Repair must still walk a window wider than one
/// served chunk instead of re-requesting the first chunk forever.
#[test]
fn given_a_fenced_window_wider_than_a_repair_chunk_when_the_old_primary_rejoins_should_catch_up() {
    const CHUNK: u64 = 2;
    let (mut sim, client) = cluster(0x5EED_57A1);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);
    for replica in &sim.replicas {
        for shard in &replica.shards {
            shard.set_repair_chunk_max(CHUNK);
        }
    }
    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("warmup-{index}"),
        );
    }
    isolate(&mut sim, OLD_PRIMARY, true);
    send(&mut sim, &client, namespace, OLD_PRIMARY, "stale");
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = primary_of(&sim, 1, namespace, new_view);
    for index in 0..12 {
        send(
            &mut sim,
            &client,
            namespace,
            new_primary,
            &format!("fresh-{index}"),
        );
    }
    let (_, _, majority_commit, _) = group_state(&sim, new_primary, namespace);
    assert!(
        majority_commit > 3 + 4 * CHUNK,
        "the new view committed only through {majority_commit}"
    );

    isolate(&mut sim, OLD_PRIMARY, false);
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }
    let (_, _, rejoined_commit, _) = group_state(&sim, OLD_PRIMARY, namespace);
    assert!(
        rejoined_commit >= majority_commit,
        "the old primary never caught up: committed {rejoined_commit} of {majority_commit}"
    );
    let reference = polled(&mut sim, new_primary, namespace);
    let rejoined = polled(&mut sim, OLD_PRIMARY, namespace);
    assert_eq!(
        rejoined, reference,
        "the rejoined replica serves a committed log that differs from the view's"
    );
}

/// A proof that breaks cuts the stale suffix from the broken op, and the cut
/// window must be re-requested at once instead of waiting out the repair retry
/// timer. Two re-request sites exist: the `RepairDone` of a request already in
/// flight, and the stall sweep when nothing is in flight. Seed `0x5EED_57A2`
/// takes the first, seed `0x2` the second.
#[test]
fn given_an_old_primary_holding_a_stale_prepare_when_the_proof_cuts_it_should_refetch_the_window_at_once()
 {
    for seed in [0x5EED_57A2, 0x2] {
        let steps = cut_to_catch_up(seed).unwrap_or_else(|| {
            panic!("seed {seed:#x} left the old primary no stale prepare for the proof to cut")
        });
        assert!(
            steps < usize::try_from(partitions::REPAIR_RETRY_TICKS).unwrap(),
            "seed {seed:#x}: catching up took {steps} steps after the cut, so the refetch \
             waited out the repair retry timer"
        );
    }
}

/// Runs the stale-prepare rejoin under `seed` and returns how many steps the
/// old primary took to catch up after its proof cut the held run, or `None`
/// when no cut happened (the isolated primary journaled nothing stale).
fn cut_to_catch_up(seed: u64) -> Option<usize> {
    let (mut sim, client) = cluster(seed);
    let namespace = IggyNamespace::new(1, 1, 0);
    sim.init_partition(namespace);
    sim.register_client_with_primary(&client);
    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("warmup-{index}"),
        );
    }
    isolate(&mut sim, OLD_PRIMARY, true);
    for index in 0..3 {
        send(
            &mut sim,
            &client,
            namespace,
            OLD_PRIMARY,
            &format!("stale-{index}"),
        );
    }
    let new_view = (0..ELECTION_STEPS)
        .find_map(|_| {
            sim.step();
            let (status, view, _, _) = group_state(&sim, 1, namespace);
            (status == Status::Normal && view > 0).then_some(view)
        })
        .expect("replicas 1 and 2 never elected a new view");
    let new_primary = primary_of(&sim, 1, namespace, new_view);
    for index in 0..6 {
        send(
            &mut sim,
            &client,
            namespace,
            new_primary,
            &format!("fresh-{index}"),
        );
    }
    let (_, _, majority_commit, _) = group_state(&sim, new_primary, namespace);

    isolate(&mut sim, OLD_PRIMARY, false);
    // The adoption, not the heal, starts the clock: how long the old primary
    // takes to learn of the new view is the view probe's business.
    (0..SETTLE_STEPS)
        .find(|_| {
            sim.step();
            group_state(&sim, OLD_PRIMARY, namespace).1 >= new_view
        })
        .expect("the old primary never adopted the new view");
    // The held run only shrinks when a proof breaks and cuts it. Within the
    // step of the cut a re-request may already have gone out, so the request
    // is either still pending (`None`) or starts at or below the cut.
    let mut held = held_through(&sim, OLD_PRIMARY, namespace);
    let mut cut_at = None;
    let mut caught_up = None;
    for step in 0..SETTLE_STEPS {
        if group_state(&sim, OLD_PRIMARY, namespace).2 >= majority_commit {
            caught_up = Some(step);
            break;
        }
        let now_held = held_through(&sim, OLD_PRIMARY, namespace);
        if now_held < held && cut_at.is_none() {
            cut_at = Some(step);
            let requested =
                repair_session(&sim, OLD_PRIMARY, namespace).map(|session| session.requested_from);
            assert!(
                requested.is_some_and(|from| from.is_none_or(|from| from <= now_held + 1)),
                "seed {seed:#x}: the cut left held entries through {now_held} but the \
                 repair request stands at {requested:?}"
            );
        }
        held = now_held;
        sim.step();
    }
    let caught_up = caught_up.unwrap_or_else(|| {
        panic!("seed {seed:#x}: the old primary never caught up to {majority_commit}")
    });
    for _ in 0..SETTLE_STEPS {
        sim.step();
    }
    let reference = polled(&mut sim, new_primary, namespace);
    let rejoined = polled(&mut sim, OLD_PRIMARY, namespace);
    assert!(
        !reference
            .iter()
            .any(|(_, payload)| payload.starts_with("stale")),
        "seed {seed:#x}: the new view committed the isolated primary's requests: {reference:?}"
    );
    assert_eq!(
        rejoined, reference,
        "seed {seed:#x}: the rejoined replica serves a committed log that differs from the view's"
    );
    cut_at.map(|cut_at| caught_up - cut_at)
}

/// Highest op through which `replica` holds every entry above its commit point.
fn held_through(sim: &Simulator, replica: u8, namespace: IggyNamespace) -> u64 {
    sim.replicas[usize::from(replica)]
        .partition_shard(namespace)
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .expect("replica hosts the group")
        .held_through()
}

/// `replica`'s repair session for the group, if one is armed.
fn repair_session(
    sim: &Simulator,
    replica: u8,
    namespace: IggyNamespace,
) -> Option<partitions::RepairSession> {
    sim.replicas[usize::from(replica)]
        .partition_shard(namespace)
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .expect("replica hosts the group")
        .repair
}

fn primary_of(sim: &Simulator, replica: u8, namespace: IggyNamespace, view: u32) -> u8 {
    sim.replicas[usize::from(replica)]
        .partition_shard(namespace)
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .expect("replica hosts the group")
        .consensus()
        .primary_index(view)
}

fn connect(sim: &mut Simulator, left: u8, right: u8) {
    sim.network
        .set_link_filter(ProcessId::Replica(left), ProcessId::Replica(right), true);
    sim.network
        .set_link_filter(ProcessId::Replica(right), ProcessId::Replica(left), true);
}
