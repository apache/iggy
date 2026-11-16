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

//! Three replicas of a partition that places its batches.
//!
//! A send goes to the object store first. The primary then commits where
//! its messages sit, and the group replicates that and not the messages. So
//! every replica must hold the same placed batches, and a poll of any of
//! them must return the messages the producers sent, read back from the one
//! store they share.
//!
//! The partitions here are told to place their batches by hand, after they
//! are built. Nothing in a topic's metadata says so yet, so a replica that
//! restarts builds a partition of whole batches. The scenarios therefore
//! stop short of a restart and of a state transfer.

use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;

use bytes::Bytes;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use iggy_binary_protocol::{GenericHeader, ReplyHeader, RoutedRequestHeader};
use iggy_common::PollingStrategy;
use iggy_object_store::{
    InlineTierExecutor, KeyLayout, MemoryStore, TierExecutor, TierJobHandle, TierJobRequest,
};
use partitions::{
    ExecutorFreshUploader, ExecutorObjects, PollingArgs, PollingConsumer, RemoteSegments,
};
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::client::SimClient;
use crate::packet::PacketSimulatorOptions;

const REPLICAS: u8 = 3;
/// Leads view 0.
const PRIMARY: u8 = 0;
const CLIENT_ID: u128 = 1;
/// Ticks one send gets: its upload window, the upload, and the commit.
const SEND_TICKS: usize = 2000;
/// Ticks the replicas get to elect a primary, or to all commit what one did.
const SETTLE_TICKS: usize = 5000;

/// One executor for every shard of every replica, so all reach one store.
#[derive(Clone)]
struct Shared(Rc<InlineTierExecutor<MemoryStore>>);

impl TierExecutor for Shared {
    type Store = MemoryStore;

    fn submit<T, F, Fut>(&self, request: TierJobRequest, job: F) -> TierJobHandle<T>
    where
        T: 'static,
        F: FnOnce(Rc<Self::Store>) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        self.0.submit(request, job)
    }
}

fn layout() -> KeyLayout {
    KeyLayout::new("simulated").expect("a valid root")
}

fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 0)
}

/// Three replicas whose partition [`namespace`] places its batches, with one
/// object store behind all of them.
fn placing_cluster(seed: u64) -> (Simulator, SimClient, Shared) {
    placing_cluster_on(PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: 1,
        seed,
        ..PacketSimulatorOptions::default()
    })
}

/// [`placing_cluster`] on a network of the caller's choice.
fn placing_cluster_on(network: PacketSimulatorOptions) -> (Simulator, SimClient, Shared) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let mut sim = Simulator::new(usize::from(REPLICAS), std::iter::once(CLIENT_ID), network);
    sim.init_partition(namespace());
    let client = SimClient::new(CLIENT_ID);
    sim.register_client_with_primary(&client);
    let store = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    for replica in &sim.replicas {
        for shard in &replica.shards {
            shard.set_fresh_uploader(Some(Rc::new(ExecutorFreshUploader::new(
                store.clone(),
                layout(),
            ))));
            shard
                .plane
                .partitions()
                .set_remote_segments(Rc::new(RemoteSegments::new(
                    Rc::new(ExecutorObjects::new(store.clone())),
                    layout(),
                )));
        }
        let owner = replica.partition_shard(namespace());
        let partition = owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .expect("every replica holds the partition");
        partition.set_places_batches(true);
    }
    (sim, client, store)
}

/// Where the answer to a send arrives: `None` until it does, and then what
/// the shard answered.
type Answer = Rc<RefCell<Option<Option<Message<GenericHeader>>>>>;

/// Hand one send of `payload` to `replica`, as its server does for a
/// producer.
fn submit_send(sim: &mut Simulator, client: &SimClient, replica: u8, payload: &[u8]) -> Answer {
    let shard = Rc::clone(sim.replicas[usize::from(replica)].partition_shard(namespace()));
    let request = client.send_messages(namespace(), &[Bytes::copy_from_slice(payload)]);
    // The watermark is the server's to stamp, on the node that takes the send.
    let watermark = shard.plane.metadata().applied_frontier().get();
    let request = request.transmute_header(|header, stamped: &mut RoutedRequestHeader| {
        *stamped = header;
        stamped.metadata_watermark = watermark;
    });
    let ticket = shard
        .partition_submit(namespace(), request)
        .expect("the owning shard takes the send");
    let answer = Answer::default();
    let arrived = Rc::clone(&answer);
    sim.executor.spawn(async move {
        *arrived.borrow_mut() = Some(shard.await_partition_submit(ticket).await);
    });
    answer
}

/// Step until `answer` arrived, and return whether the send was committed.
fn wait_for(sim: &mut Simulator, answer: &Answer) -> bool {
    for _ in 0..SEND_TICKS {
        if let Some(arrived) = answer.borrow().as_ref() {
            return arrived.is_some();
        }
        sim.step();
    }
    panic!("a send got no answer in {SEND_TICKS} ticks");
}

/// Whether the answer that arrived says the send committed, and not that it
/// was refused.
fn sent_was_committed(answer: &Answer) -> bool {
    answer
        .borrow()
        .as_ref()
        .and_then(Option::as_ref)
        .and_then(|reply| reply.try_as_typed::<ReplyHeader>().ok())
        .is_some_and(|reply| reply.header().status == 0)
}

/// Run `future` on the simulator's executor. Its output must be ready once
/// the pumps drain.
fn complete_now<T: 'static>(sim: &mut Simulator, future: impl Future<Output = T> + 'static) -> T {
    let output = Rc::new(RefCell::new(None));
    let ready = Rc::clone(&output);
    sim.executor.spawn(async move {
        *ready.borrow_mut() = Some(future.await);
    });
    sim.run_pumps();
    output
        .borrow_mut()
        .take()
        .expect("the future completes once the pumps drain")
}

/// Every message `replica` returns from offset 0 on, as the bytes of the
/// batches a poll hands out.
fn polled(sim: &mut Simulator, replica: u8) -> Vec<u8> {
    let args = PollingArgs::new(PollingStrategy::offset(0), 1000, false);
    let poll = sim.poll_messages(
        usize::from(replica),
        namespace(),
        PollingConsumer::Consumer(7, 0),
        &args,
    );
    complete_now(sim, poll)
        .expect("the poll is served")
        .iter()
        .flat_map(|fragment| fragment.as_slice().to_vec())
        .collect()
}

fn holds(polled: &[u8], payload: &[u8]) -> usize {
    polled
        .windows(payload.len())
        .filter(|bytes| *bytes == payload)
        .count()
}

/// The live replica that leads the partition in normal status, if one does.
fn primary(sim: &Simulator) -> Option<u8> {
    (0..REPLICAS).find(|replica| {
        !sim.is_crashed(*replica)
            && sim.replicas[usize::from(*replica)]
                .partition_shard(namespace())
                .plane
                .partitions()
                .get_by_ns(&namespace())
                .is_some_and(|partition| {
                    let consensus = partition.consensus();
                    consensus.is_primary() && consensus.is_normal()
                })
    })
}

/// The offset each live replica committed up to.
fn commits(sim: &Simulator) -> Vec<Option<u64>> {
    (0..REPLICAS)
        .filter(|replica| !sim.is_crashed(*replica))
        .map(|replica| {
            sim.offsets(usize::from(replica), namespace())
                .map(|offsets| offsets.commit_offset)
        })
        .collect()
}

/// Step until every live replica committed up to `offset`.
fn settle_at(sim: &mut Simulator, offset: u64) {
    for _ in 0..SETTLE_TICKS {
        if commits(sim).iter().all(|commit| *commit == Some(offset)) {
            return;
        }
        sim.step();
    }
    panic!(
        "the live replicas did not all commit {offset}: {:?}",
        commits(sim)
    );
}

/// The payload of the `index`-th send, which no other send carries.
fn payload(index: usize) -> Vec<u8> {
    format!("placed-message-{index:04}-end").into_bytes()
}

#[test]
fn given_three_replicas_when_sends_are_placed_should_hold_and_serve_the_same_messages_everywhere() {
    const SENDS: usize = 12;
    let (mut sim, client, store) = placing_cluster(0x504C_4143);

    for index in 0..SENDS {
        let answer = submit_send(&mut sim, &client, PRIMARY, &payload(index));
        assert!(
            wait_for(&mut sim, &answer) && sent_was_committed(&answer),
            "send {index} is answered as committed"
        );
    }
    settle_at(&mut sim, SENDS as u64 - 1);

    let objects = store.0.store().keys();
    assert!(
        !objects.is_empty() && objects.len() <= SENDS,
        "the messages are in fresh objects: {objects:?}"
    );
    let from_primary = polled(&mut sim, PRIMARY);
    for index in 0..SENDS {
        assert_eq!(
            holds(&from_primary, &payload(index)),
            1,
            "the primary serves send {index} once"
        );
    }
    for replica in 0..REPLICAS {
        assert_eq!(
            polled(&mut sim, replica),
            from_primary,
            "replica {replica} serves what the primary serves"
        );
    }
}

/// The primary stops with one send answered to nobody. A backup takes over
/// and the producers go on with it. Every send that was answered as
/// committed is served once by the replicas that are left, and the one that
/// was not is served once or not at all.
#[test]
fn given_a_primary_that_stops_mid_send_when_a_backup_takes_over_should_keep_every_answered_send() {
    const BEFORE: usize = 6;
    const AFTER: usize = 6;
    let (mut sim, client, _store) = placing_cluster(0x4641_494C);
    for index in 0..BEFORE {
        let answer = submit_send(&mut sim, &client, PRIMARY, &payload(index));
        assert!(wait_for(&mut sim, &answer), "send {index} is committed");
    }
    // Its upload window is open, or its upload is on the way.
    let _unanswered = submit_send(&mut sim, &client, PRIMARY, &payload(BEFORE));
    sim.step();
    sim.step();

    sim.replica_crash(PRIMARY);
    let mut successor = None;
    for _ in 0..SETTLE_TICKS {
        successor = primary(&sim);
        if successor.is_some() {
            break;
        }
        sim.step();
    }
    let successor = successor.expect("a backup takes over");
    assert_ne!(successor, PRIMARY);

    let mut answered = BEFORE;
    for index in BEFORE + 1..=BEFORE + AFTER {
        // A send that arrives while the new view settles is answered as not
        // accepted, and the producer sends it again.
        let committed = (0..50).any(|_| {
            let answer = submit_send(&mut sim, &client, successor, &payload(index));
            wait_for(&mut sim, &answer) && sent_was_committed(&answer)
        });
        assert!(committed, "send {index} is committed by the successor");
        answered += 1;
    }
    let last = commits(&sim)
        .into_iter()
        .flatten()
        .max()
        .expect("the successor committed");
    settle_at(&mut sim, last);

    let from_successor = polled(&mut sim, successor);
    for index in (0..BEFORE).chain(BEFORE + 1..=BEFORE + AFTER) {
        assert_eq!(
            holds(&from_successor, &payload(index)),
            1,
            "send {index} was answered as committed"
        );
    }
    assert!(
        holds(&from_successor, &payload(BEFORE)) <= 1,
        "the send nobody was answered for is served once or not at all"
    );
    assert_eq!(answered, BEFORE + AFTER);
    let live: Vec<u8> = (0..REPLICAS)
        .filter(|replica| !sim.is_crashed(*replica))
        .collect();
    assert_eq!(live.len(), 2);
    for replica in live {
        assert_eq!(
            polled(&mut sim, replica),
            from_successor,
            "replica {replica} serves what the successor serves"
        );
    }
}

/// The network drops and repeats packets between the replicas. The sends
/// still commit, and every replica ends with the same placed batches.
#[test]
fn given_a_network_that_drops_and_repeats_packets_when_sends_are_placed_should_converge() {
    const SENDS: usize = 20;
    let (mut sim, client, _store) = placing_cluster_on(PacketSimulatorOptions {
        node_count: REPLICAS,
        client_count: 1,
        seed: 0x4C4F_5353,
        packet_loss_probability: 0.1,
        replay_probability: 0.1,
        ..PacketSimulatorOptions::default()
    });

    let mut committed = Vec::new();
    for index in 0..SENDS {
        let leader = primary(&sim).unwrap_or(PRIMARY);
        let answer = submit_send(&mut sim, &client, leader, &payload(index));
        if wait_for(&mut sim, &answer) && sent_was_committed(&answer) {
            committed.push(index);
        }
    }
    assert!(
        committed.len() > SENDS / 2,
        "most sends commit on a lossy network: {committed:?}"
    );
    let last = commits(&sim).into_iter().flatten().max().unwrap();
    settle_at(&mut sim, last);

    let leader = primary(&sim).expect("the partition has a primary");
    let from_leader = polled(&mut sim, leader);
    for index in &committed {
        assert_eq!(
            holds(&from_leader, &payload(*index)),
            1,
            "send {index} was answered as committed"
        );
    }
    for index in 0..SENDS {
        assert!(holds(&from_leader, &payload(index)) <= 1, "send {index}");
    }
    for replica in 0..REPLICAS {
        assert_eq!(polled(&mut sim, replica), from_leader, "replica {replica}");
    }
}
