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
//! Nothing in a topic's metadata says that its partitions place their
//! batches yet, so the simulator is told which ones do and builds them so
//! each time, as boot does from a topic's options. A replica that restarts
//! gets its store back from [`restart`], the way a server's boot wires it.
//!
//! No scenario ends in a state transfer: the simulator's partitions keep
//! their log in memory, and a transfer copies segment files.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use consensus::{Consensus, MetadataHandle, PartitionsHandle, Sequencer};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_binary_protocol::placement::PlacedBatch;
use iggy_binary_protocol::{Command, GenericHeader, Operation, ReplyHeader, RoutedRequestHeader};
use iggy_common::{IggyTimestamp, PollingStrategy};
use iggy_object_store::{
    FreshIndex, FreshObjectId, FreshProofRow, InlineTierExecutor, KeyLayout, MemoryStore,
    ObjectStore, PutBody, PutOptions, SegmentObject, TierExecutor, TierJobHandle, TierJobRequest,
    TierScopeToken,
};
use partitions::{
    ExecutorFreshSweeper, ExecutorFreshUploader, ExecutorLongTermWriter, ExecutorObjects,
    FRESH_OBJECT_GRACE, IggyPartition, LongTermWriter, PollingArgs, PollingConsumer, ProvenBatch,
    RemoteSegments, RewriteOutcome, SegmentRewrite,
};
use server_common::Message;
use server_common::sharding::IggyNamespace;

use crate::Simulator;
use crate::bus::SharedSimOutbox;
use crate::client::SimClient;
use crate::deps::SimSuperblock;
use crate::packet::{Packet, PacketSimulatorOptions, ProcessId};

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

/// A partition that the second shard owns on a replica of two shards, so a
/// scenario with two shards has work on both.
fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 1)
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
    placing_cluster_of(1, network)
}

/// [`placing_cluster_on`] with `shards` shards on each replica.
fn placing_cluster_of(
    shards: u16,
    network: PacketSimulatorOptions,
) -> (Simulator, SimClient, Shared) {
    server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
        enabled: false,
        size: iggy_common::IggyByteSize::from(0u64),
        bucket_capacity: 1,
    });
    let mut sim = Simulator::with_shards(
        usize::from(REPLICAS),
        shards,
        std::iter::once(CLIENT_ID),
        network,
    );
    sim.init_partition(namespace());
    sim.place_batches(namespace());
    let client = SimClient::new(CLIENT_ID);
    sim.register_client_with_primary(&client);
    let store = Shared(Rc::new(InlineTierExecutor::new(MemoryStore::new())));
    for replica in 0..REPLICAS {
        wire_store(&sim, replica, &store);
    }
    (sim, client, store)
}

/// A wait for proof rows longer than any scenario, so that the store holds
/// only the objects that the sends of a scenario fill.
const NO_CONFIRMATION: Duration = Duration::from_secs(3600);

/// Give every shard of `replica` the store: for its uploads, its sweeps and
/// the reads of its polls.
fn wire_store(sim: &Simulator, replica: u8, store: &Shared) {
    for shard in &sim.replicas[usize::from(replica)].shards {
        shard.set_fresh_confirm_after(NO_CONFIRMATION);
        shard.set_fresh_uploader(Some(Rc::new(ExecutorFreshUploader::new(
            store.clone(),
            layout(),
        ))));
        shard.set_fresh_sweeper(Some(Rc::new(ExecutorFreshSweeper::new(
            store.clone(),
            layout(),
        ))));
        shard.plane.partitions().set_remote_segments(Rc::new(
            RemoteSegments::new(Rc::new(ExecutorObjects::new(store.clone())), layout())
                .with_runtime(Rc::new(shard.bus.clone())),
        ));
    }
}

/// Restart `replica`, which is down, with the store wired to its new shards.
fn restart(sim: &mut Simulator, replica: u8, store: &Shared) {
    sim.replica_restart(replica);
    wire_store(sim, replica, store);
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
                .peek_by_ns(&namespace())
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

/// Ticks a sweep gets to list, hear from every shard, and delete.
const SWEEP_TICKS: usize = 50;

/// The ids of the fresh objects in the store, ascending.
fn fresh_objects(store: &Shared) -> Vec<u128> {
    let mut objects: Vec<_> = store
        .0
        .store()
        .keys()
        .iter()
        .filter_map(|key| layout().parse_fresh_key(key))
        .collect();
    objects.sort_unstable();
    objects
}

/// Store a fresh object under an id of `replica` that no placed batch
/// names, as a shard of an earlier life of that replica left it.
fn store_unnamed_object(store: &Shared, replica: u8) -> u128 {
    let object = FreshObjectId::new(replica, 0, 1, 0)
        .expect("a valid id")
        .get();
    store
        .0
        .store()
        .put(
            &layout().fresh_key(object),
            PutBody::from_bytes(Bytes::from_static(b"IGGYFRS1 never committed")),
            &PutOptions::default(),
        )
        .now_or_never()
        .expect("a memory store answers at once")
        .expect("the store takes the object");
    object
}

/// Run a sweep of `replica`'s fresh objects at `now`, and return whether
/// every shard of the replica answered it.
fn sweep(sim: &mut Simulator, replica: u8, now: IggyTimestamp) -> bool {
    let first = Rc::clone(&sim.replicas[usize::from(replica)].shards[0]);
    let settled = first.fresh_sweeps_settled();
    first.request_sweep_fresh_objects(now);
    let mut answered = false;
    for _ in 0..SWEEP_TICKS {
        sim.step();
        answered |= first.fresh_sweeps_settled() > settled;
    }
    answered
}

/// A time one grace period and a second after `now`.
fn past_grace(now: IggyTimestamp) -> IggyTimestamp {
    let grace = u64::try_from(FRESH_OBJECT_GRACE.as_micros()).expect("the grace fits");
    IggyTimestamp::from(now.as_micros() + grace + 1_000_000)
}

/// The first shard of a replica sweeps for all of them. The partition here
/// is on the second shard, so what keeps its objects is that shard's
/// answer, and what lets the unnamed object go is that both answered.
#[test]
fn given_a_partition_on_the_second_shard_when_the_first_sweeps_should_keep_what_it_names() {
    const SENDS: usize = 3;
    let (mut sim, client, store) = placing_cluster_of(
        2,
        PacketSimulatorOptions {
            node_count: REPLICAS,
            client_count: 1,
            seed: 0x5357_4550,
            ..PacketSimulatorOptions::default()
        },
    );
    let replica = &sim.replicas[usize::from(PRIMARY)];
    assert!(
        !Rc::ptr_eq(replica.partition_shard(namespace()), &replica.shards[0]),
        "the partition is not on the shard that sweeps"
    );
    for index in 0..SENDS {
        let answer = submit_send(&mut sim, &client, PRIMARY, &payload(index));
        assert!(wait_for(&mut sim, &answer) && sent_was_committed(&answer));
    }
    settle_at(&mut sim, SENDS as u64 - 1);
    let named = fresh_objects(&store);
    let unnamed = store_unnamed_object(&store, PRIMARY);
    let other_replica = store_unnamed_object(&store, PRIMARY + 1);
    let now = IggyTimestamp::from(1_700_000_000_000_000);

    assert!(sweep(&mut sim, PRIMARY, now));
    assert!(fresh_objects(&store).contains(&unnamed), "the grace runs");
    assert!(sweep(&mut sim, PRIMARY, past_grace(now)));

    let left = fresh_objects(&store);
    assert!(!left.contains(&unnamed));
    assert!(
        left.contains(&other_replica),
        "another replica judges its own"
    );
    for object in &named {
        assert!(left.contains(object), "a placed batch names {object:032x}");
    }
    assert_eq!(left.len(), named.len() + 1);
    let served = polled(&mut sim, PRIMARY);
    for index in 0..SENDS {
        assert_eq!(holds(&served, &payload(index)), 1, "send {index}");
    }
}

/// A replica that lost its primary and cannot elect another does not know
/// what its group holds. Its sweep deletes nothing, however long an object
/// went unnamed, and the same sweep deleted such an object while the
/// partition was in normal status.
#[test]
fn given_a_replica_in_a_view_change_when_it_sweeps_should_delete_nothing() {
    const BACKUP: u8 = 1;
    const OTHER_BACKUP: u8 = 2;
    let (mut sim, client, store) = placing_cluster_of(
        2,
        PacketSimulatorOptions {
            node_count: REPLICAS,
            client_count: 1,
            seed: 0x5649_4557,
            ..PacketSimulatorOptions::default()
        },
    );
    let answer = submit_send(&mut sim, &client, PRIMARY, &payload(0));
    assert!(wait_for(&mut sim, &answer) && sent_was_committed(&answer));
    settle_at(&mut sim, 0);
    let now = IggyTimestamp::from(1_700_000_000_000_000);

    let swept = store_unnamed_object(&store, BACKUP);
    assert!(sweep(&mut sim, BACKUP, now));
    assert!(sweep(&mut sim, BACKUP, past_grace(now)));
    assert!(!fresh_objects(&store).contains(&swept));

    // The other backup stays up, so this one starts a view change. It
    // cannot end it: once it left normal status, nothing passes between
    // the two.
    sim.replica_crash(PRIMARY);
    let in_normal_status = |sim: &Simulator| {
        sim.replicas[usize::from(BACKUP)]
            .partition_shard(namespace())
            .plane
            .partitions()
            .peek_by_ns(&namespace())
            .is_some_and(|partition| partition.consensus().is_normal())
    };
    for _ in 0..SETTLE_TICKS {
        if !in_normal_status(&sim) {
            break;
        }
        sim.step();
    }
    assert!(!in_normal_status(&sim), "the backup misses its primary");
    for (from, to) in [(BACKUP, OTHER_BACKUP), (OTHER_BACKUP, BACKUP)] {
        *sim.network
            .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
            Some(|_: &Packet| true);
    }

    let kept = store_unnamed_object(&store, BACKUP);
    let later = past_grace(now);
    assert!(!sweep(&mut sim, BACKUP, later));
    assert!(!sweep(&mut sim, BACKUP, past_grace(later)));
    assert!(!sweep(&mut sim, BACKUP, past_grace(past_grace(later))));

    assert!(!in_normal_status(&sim));
    assert!(fresh_objects(&store).contains(&kept));
}

/// The seeds every scenario below runs under, one cluster for each.
const SCENARIO_SEEDS: [u64; 4] = [0x5245_5354, 0x5245_5355, 0x0BAD_5EED, 7];

/// Run `read` on the partition as `replica` holds it.
fn with_partition<T>(
    sim: &Simulator,
    replica: u8,
    read: impl FnOnce(&IggyPartition<SharedSimOutbox, SimSuperblock>) -> T,
) -> T {
    let partitions = sim.replicas[usize::from(replica)]
        .partition_shard(namespace())
        .plane
        .partitions();
    read(
        partitions
            .peek_by_ns(&namespace())
            .expect("the replica holds the partition"),
    )
}

/// The fresh objects the placed batches of `replica` name above its
/// long-term floor.
fn named(sim: &mut Simulator, replica: u8) -> Vec<u128> {
    let read = with_partition(sim, replica, |partition| {
        partition
            .fresh_objects_in_use()
            .expect("the partition places its batches")
    });
    complete_now(sim, read.read())
        .expect("the placed batches are read")
        .into_iter()
        .collect()
}

/// Send `payload(index)` to whichever replica leads until one answers it as
/// committed. A send that arrives while a view settles is answered as not
/// accepted, and the producer sends it again.
fn send_until_committed(sim: &mut Simulator, client: &SimClient, index: usize) {
    for _ in 0..50 {
        let leader = await_primary(sim);
        let answer = submit_send(sim, client, leader, &payload(index));
        if wait_for(sim, &answer) && sent_was_committed(&answer) {
            return;
        }
    }
    panic!("send {index} was not committed");
}

/// Step until a replica leads the partition, and return it.
fn await_primary(sim: &mut Simulator) -> u8 {
    for _ in 0..SETTLE_TICKS {
        if let Some(leader) = primary(sim) {
            return leader;
        }
        sim.step();
    }
    panic!("no replica leads the partition");
}

/// Step until every replica is up, in normal status, and committed up to the
/// highest offset one of them committed.
fn settle_everywhere(sim: &mut Simulator) -> u64 {
    for _ in 0..SETTLE_TICKS {
        let commits = commits(sim);
        let normal = (0..REPLICAS)
            .all(|replica| with_partition(sim, replica, |held| held.consensus().is_normal()));
        if let Some(last) = commits.iter().flatten().max().copied()
            && normal
            && commits.len() == usize::from(REPLICAS)
            && commits.iter().all(|commit| *commit == Some(last))
        {
            return last;
        }
        sim.step();
    }
    panic!("the replicas did not all commit: {:?}", commits(sim));
}

/// Every replica holds placed batches that name the same objects, and
/// serves each of the sends `0..sends` once, as the others do.
fn assert_same_everywhere(sim: &mut Simulator, sends: usize, seed: u64) {
    let from_first = polled(sim, 0);
    let named_by_first = named(sim, 0);
    assert!(!named_by_first.is_empty(), "seed {seed:#x}");
    for index in 0..sends {
        assert_eq!(
            holds(&from_first, &payload(index)),
            1,
            "send {index} is served once, seed {seed:#x}"
        );
    }
    for replica in 1..REPLICAS {
        assert!(
            with_partition(sim, replica, IggyPartition::places_batches),
            "replica {replica} places its batches, seed {seed:#x}"
        );
        assert_eq!(
            named(sim, replica),
            named_by_first,
            "replica {replica} names the objects the first one names, seed {seed:#x}"
        );
        assert_eq!(
            polled(sim, replica),
            from_first,
            "replica {replica} serves what the first one serves, seed {seed:#x}"
        );
    }
}

/// A replica loses power between two sends of a stream and comes back from
/// its WAL at once. It must come back as a partition that places its
/// batches: one of whole batches refuses every placed prepare, and never
/// catches up.
fn restart_mid_stream(restarted: u8, seed: u64) {
    const BEFORE: usize = 5;
    const AFTER: usize = 5;
    let (mut sim, client, store) = placing_cluster(seed);
    for index in 0..BEFORE {
        send_until_committed(&mut sim, &client, index);
    }
    // Its upload window is open, or its upload is on the way.
    let in_flight = submit_send(&mut sim, &client, PRIMARY, &payload(BEFORE));
    sim.step();
    sim.step();

    sim.replica_crash(restarted);
    restart(&mut sim, restarted, &store);
    assert!(
        with_partition(&sim, restarted, IggyPartition::places_batches),
        "the rebuilt partition places its batches, seed {seed:#x}"
    );
    let in_flight_committed = wait_for(&mut sim, &in_flight) && sent_was_committed(&in_flight);
    for index in BEFORE + 1..=BEFORE + AFTER {
        send_until_committed(&mut sim, &client, index);
    }
    let last = settle_everywhere(&mut sim);

    let sends = BEFORE + AFTER + usize::from(in_flight_committed);
    assert!(last + 1 >= sends as u64, "seed {seed:#x}");
    let served = polled(&mut sim, restarted);
    for index in (0..BEFORE).chain(BEFORE + 1..=BEFORE + AFTER) {
        assert_eq!(
            holds(&served, &payload(index)),
            1,
            "send {index} was answered as committed, seed {seed:#x}"
        );
    }
    assert!(holds(&served, &payload(BEFORE)) <= 1, "seed {seed:#x}");
    if in_flight_committed {
        assert_eq!(holds(&served, &payload(BEFORE)), 1, "seed {seed:#x}");
    }
    assert_same_everywhere(&mut sim, 0, seed);
}

#[test]
fn given_a_backup_that_restarts_mid_stream_when_the_sends_go_on_should_hold_and_serve_what_the_others_do()
 {
    for seed in SCENARIO_SEEDS {
        restart_mid_stream(PRIMARY + 1, seed);
    }
}

#[test]
fn given_a_primary_that_restarts_mid_stream_when_the_sends_go_on_should_hold_and_serve_what_the_others_do()
 {
    for seed in SCENARIO_SEEDS {
        restart_mid_stream(PRIMARY, seed);
    }
}

/// A backup is down while the others commit more sends than a retransmit
/// brings back. After its restart it asks its peers for the prepares it
/// missed, which are placed batches, and ends with what they hold.
#[test]
fn given_a_backup_that_was_down_for_many_sends_when_it_restarts_should_catch_up_by_repair() {
    const BEFORE: usize = 3;
    const MISSED: usize = 12;
    const LAGGING: u8 = 2;
    for seed in SCENARIO_SEEDS {
        let (mut sim, client, store) = placing_cluster(seed);
        for index in 0..BEFORE {
            send_until_committed(&mut sim, &client, index);
        }
        settle_at(&mut sim, BEFORE as u64 - 1);

        sim.replica_crash(LAGGING);
        for index in BEFORE..BEFORE + MISSED {
            send_until_committed(&mut sim, &client, index);
        }
        restart(&mut sim, LAGGING, &store);
        assert_eq!(
            sim.offsets(usize::from(LAGGING), namespace())
                .map(|offsets| offsets.commit_offset),
            Some(BEFORE as u64 - 1),
            "the restart recovered what the backup had committed, seed {seed:#x}"
        );

        let mut repaired = false;
        for _ in 0..SETTLE_TICKS {
            repaired |= with_partition(&sim, LAGGING, |held| held.repair().is_some());
            if commits(&sim)
                .iter()
                .all(|commit| *commit == Some((BEFORE + MISSED) as u64 - 1))
            {
                break;
            }
            sim.step();
        }
        assert!(repaired, "the backup asked for a repair, seed {seed:#x}");
        assert_eq!(
            settle_everywhere(&mut sim),
            (BEFORE + MISSED) as u64 - 1,
            "seed {seed:#x}"
        );
        assert_same_everywhere(&mut sim, BEFORE + MISSED, seed);
    }
}

/// Ticks a rewrite gets to store its segment and commit it: a pass that
/// found another primary's segment waits before the next one.
const REWRITE_TICKS: usize = 10_000;

/// Rewrites into the store. While `holds` is set it stores the segment and
/// never reports it, so the primary that started the rewrite sends no
/// command for it.
struct HeldWriter {
    writer: ExecutorLongTermWriter<Shared>,
    holds: Rc<Cell<bool>>,
}

impl LongTermWriter for HeldWriter {
    fn write(
        &self,
        rewrite: SegmentRewrite,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, RewriteOutcome> {
        let write = self.writer.write(rewrite, scope);
        let holds = Rc::clone(&self.holds);
        Box::pin(async move {
            let outcome = write.await;
            if holds.get() {
                std::future::pending::<()>().await;
            }
            outcome
        })
    }
}

/// Give every shard of `replica` a writer of long-term segments into the
/// store.
fn wire_writer(sim: &Simulator, replica: u8, store: &Shared, holds: &Rc<Cell<bool>>) {
    for shard in &sim.replicas[usize::from(replica)].shards {
        shard.set_long_term_writer(Some(Rc::new(HeldWriter {
            writer: ExecutorLongTermWriter::new(store.clone(), layout()),
            holds: Rc::clone(holds),
        })));
    }
}

/// [`placing_cluster`] whose replicas rewrite into the store too.
fn rewriting_cluster(seed: u64) -> (Simulator, SimClient, Shared, Rc<Cell<bool>>) {
    let (sim, client, store) = placing_cluster(seed);
    let held = Rc::new(Cell::new(false));
    for replica in 0..REPLICAS {
        wire_writer(&sim, replica, &store, &held);
    }
    (sim, client, store, held)
}

/// Ask `replica` for the first rewrite pass over the partition, as the
/// segment cleaner does. Its tick starts the passes after that one.
fn request_rewrite(sim: &Simulator, replica: u8) {
    let revision = with_partition(sim, replica, IggyPartition::created_revision);
    sim.replicas[usize::from(replica)]
        .partition_shard(namespace())
        .request_rewrite_segments(namespace(), revision);
}

/// The long-term floor of every live replica.
fn floors(sim: &Simulator) -> Vec<Option<u64>> {
    (0..REPLICAS)
        .filter(|replica| !sim.is_crashed(*replica))
        .map(|replica| with_partition(sim, replica, IggyPartition::long_term_floor))
        .collect()
}

/// Step until the long-term floor of every live replica is `floor`.
fn settle_floors_at(sim: &mut Simulator, floor: u64) {
    for _ in 0..REWRITE_TICKS {
        if floors(sim).iter().all(|held| *held == Some(floor)) {
            return;
        }
        sim.step();
    }
    panic!(
        "the long-term floors did not all reach {floor}: {:?}",
        floors(sim)
    );
}

/// The first and the last offset of each segment in the long-term log of
/// `replica`.
fn long_term_log(sim: &Simulator, replica: u8) -> Vec<(u64, u64)> {
    with_partition(sim, replica, |partition| {
        partition
            .long_term_segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset))
            .collect()
    })
}

/// The first offsets of the long-term segments the store holds, ascending.
fn stored_segments(store: &Shared) -> Vec<u64> {
    let mut starts: Vec<_> = store
        .0
        .store()
        .keys()
        .iter()
        .filter_map(|key| layout().parse_long_term_key(key))
        .filter(|key| key.object == SegmentObject::Log)
        .map(|key| key.start_offset)
        .collect();
    starts.sort_unstable();
    starts
}

/// Crash the primary, wait for a backup to take over, and bring the crashed
/// replica back with its store and its writer.
fn change_primary(
    sim: &mut Simulator,
    store: &Shared,
    holds: &Rc<Cell<bool>>,
    leader: u8,
    seed: u64,
) -> u8 {
    sim.replica_crash(leader);
    let successor = await_primary(sim);
    assert_ne!(successor, leader, "seed {seed:#x}");
    restart(sim, leader, store);
    wire_writer(sim, leader, store, holds);
    successor
}

/// The primary changes at each step of a rewrite: once its segment is in
/// the store and no command names it, once the command went out and did not
/// commit, and once it committed. Whoever leads next goes on from what the
/// group committed and the store holds, so the long-term log ends as one run
/// of offsets with no gap and no offset twice.
#[test]
#[allow(clippy::too_many_lines)]
fn given_a_primary_change_at_each_step_of_a_rewrite_when_the_next_primary_goes_on_should_leave_no_gap_and_no_repeat()
 {
    const SENDS_PER_ROUND: usize = 3;
    for seed in SCENARIO_SEEDS {
        let (mut sim, client, store, held) = rewriting_cluster(seed);
        let mut sent = 0;
        let mut send_round = |sim: &mut Simulator| {
            for index in sent..sent + SENDS_PER_ROUND {
                send_until_committed(sim, &client, index);
            }
            sent += SENDS_PER_ROUND;
            assert_eq!(settle_everywhere(sim), sent as u64 - 1, "seed {seed:#x}");
            sent as u64 - 1
        };

        // The segment is stored, and the primary stops before it sends the
        // command. The next primary rewrites more than the stored segment
        // holds: its key names another range, so the stored one is left for
        // the delete once the floor passes its end.
        let stored_up_to = send_round(&mut sim);
        let leader = await_primary(&mut sim);
        held.set(true);
        request_rewrite(&sim, leader);
        for _ in 0..REWRITE_TICKS {
            if !stored_segments(&store).is_empty() {
                break;
            }
            sim.step();
        }
        assert_eq!(stored_segments(&store), [0], "seed {seed:#x}");
        assert!(
            floors(&sim).iter().all(Option::is_none),
            "no command names the stored segment, seed {seed:#x}"
        );
        let leader = change_primary(&mut sim, &store, &held, leader, seed);
        held.set(false);
        let committed = send_round(&mut sim);
        request_rewrite(&sim, leader);
        settle_floors_at(&mut sim, committed);
        assert!(stored_up_to < committed, "seed {seed:#x}");
        assert_eq!(
            long_term_log(&sim, leader),
            [(0, committed)],
            "the next primary rewrote from the floor, seed {seed:#x}"
        );

        // The command reaches the backups, and the primary stops before it
        // hears from them. The next primary commits it with its view. The
        // registration of the segment commits before the primary stores it,
        // so the backups stop answering only after that.
        let committed = send_round(&mut sim);
        let leader = await_primary(&mut sim);
        let backups: Vec<u8> = (0..REPLICAS).filter(|replica| *replica != leader).collect();
        request_rewrite(&sim, leader);
        let registered = |sim: &Simulator| {
            with_partition(sim, leader, |partition| {
                partition.long_term_log().is_some_and(|log| {
                    log.registrations()
                        .iter()
                        .any(|registration| registration.start_offset == log.next_start())
                })
            })
        };
        for _ in 0..REWRITE_TICKS {
            if registered(&sim) {
                break;
            }
            sim.step();
        }
        assert!(
            registered(&sim),
            "the registration of the segment commits, seed {seed:#x}"
        );
        assert!(
            floors(&sim).iter().all(|floor| *floor < Some(committed)),
            "nothing is published before the backups stop answering, seed {seed:#x}"
        );
        for backup in &backups {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(*backup), ProcessId::Replica(leader)) =
                Some(|packet: &Packet| packet.message.header().command == Command::PrepareOk);
        }
        let command_sent = |sim: &Simulator| {
            let op = with_partition(sim, leader, |partition| {
                partition
                    .unpublished_segment()
                    .map(|_| partition.consensus().sequencer().current_sequence())
            });
            op.is_some_and(|op| {
                backups.iter().all(|backup| {
                    sim.partition_journaled_header(usize::from(*backup), namespace(), op)
                        .is_some_and(|header| header.operation == Operation::StateMachineCommand)
                })
            })
        };
        for _ in 0..REWRITE_TICKS {
            if command_sent(&sim) {
                break;
            }
            sim.step();
        }
        assert!(
            command_sent(&sim),
            "the backups hold the command, seed {seed:#x}"
        );
        assert!(
            floors(&sim).iter().all(|floor| *floor < Some(committed)),
            "the command did not commit, seed {seed:#x}"
        );
        for backup in &backups {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(*backup), ProcessId::Replica(leader)) =
                None;
        }
        let leader = change_primary(&mut sim, &store, &held, leader, seed);
        settle_everywhere(&mut sim);
        settle_floors_at(&mut sim, committed);

        // The command commits, and then the primary stops.
        let committed = send_round(&mut sim);
        request_rewrite(&sim, leader);
        settle_floors_at(&mut sim, committed);
        let leader = change_primary(&mut sim, &store, &held, leader, seed);
        let committed = send_round(&mut sim);
        request_rewrite(&sim, leader);
        settle_floors_at(&mut sim, committed);

        let log = long_term_log(&sim, leader);
        assert_eq!(log.first().map(|first| first.0), Some(0), "seed {seed:#x}");
        assert_eq!(
            log.last().map(|last| last.1),
            Some(committed),
            "seed {seed:#x}"
        );
        for pair in log.windows(2) {
            assert_eq!(
                pair[1].0,
                pair[0].1 + 1,
                "the segments follow each other, seed {seed:#x}: {log:?}"
            );
        }
        let stored = stored_segments(&store);
        for (start, _) in &log {
            assert!(
                stored.contains(start),
                "the store holds the segment from {start}, seed {seed:#x}: {stored:?}"
            );
        }
        for replica in 0..REPLICAS {
            assert_eq!(
                long_term_log(&sim, replica),
                log,
                "replica {replica}, seed {seed:#x}"
            );
        }
        let served = polled(&mut sim, leader);
        for index in 0..sent {
            assert_eq!(
                holds(&served, &payload(index)),
                1,
                "send {index}, seed {seed:#x}"
            );
        }
    }
}

/// Drop every packet between `replica` and the other two, or let them pass
/// again.
fn cut_off(sim: &mut Simulator, replica: u8, cut: bool) {
    for other in (0..REPLICAS).filter(|other| *other != replica) {
        for (from, to) in [(replica, other), (other, replica)] {
            *sim.network
                .link_drop_packet_fn(ProcessId::Replica(from), ProcessId::Replica(to)) =
                cut.then_some(|_: &Packet| true);
        }
    }
}

/// The replica that wrote a fresh object asks only its own partitions
/// whether to keep it. A backup that is cut off holds the placed batches and
/// not the command that moved the floor past them, and nothing tells the
/// writer so. What the backup has is the grace period: the writer keeps the
/// objects that long after its own floor passed them, and no longer.
///
/// A backup that stays away for longer names objects the store no longer
/// holds. It serves no poll while it is cut off, and what brings it back is
/// the group's log, which holds the command too. So it ends with the floor
/// of the others and serves the messages from the long-term segment.
#[test]
fn given_a_backup_cut_off_for_longer_than_the_grace_when_the_writer_sweeps_should_delete_the_objects_and_still_bring_the_backup_back()
 {
    const SENDS: usize = 3;
    const LAGGING: u8 = 2;
    for seed in SCENARIO_SEEDS {
        let (mut sim, client, store, _held) = rewriting_cluster(seed);
        for index in 0..SENDS {
            send_until_committed(&mut sim, &client, index);
        }
        let last = settle_everywhere(&mut sim);
        let objects = fresh_objects(&store);
        assert!(!objects.is_empty(), "seed {seed:#x}");
        assert_eq!(named(&mut sim, LAGGING), objects, "seed {seed:#x}");

        cut_off(&mut sim, LAGGING, true);
        request_rewrite(&sim, PRIMARY);
        for _ in 0..REWRITE_TICKS {
            if with_partition(&sim, PRIMARY, IggyPartition::long_term_floor) == Some(last) {
                break;
            }
            sim.step();
        }
        assert_eq!(
            with_partition(&sim, PRIMARY, IggyPartition::long_term_floor),
            Some(last),
            "seed {seed:#x}"
        );
        assert_eq!(
            named(&mut sim, PRIMARY),
            Vec::<u128>::new(),
            "seed {seed:#x}"
        );
        let behind = |sim: &mut Simulator| {
            with_partition(sim, LAGGING, IggyPartition::long_term_floor).is_none()
                && named(sim, LAGGING) == objects
        };
        assert!(behind(&mut sim), "seed {seed:#x}");

        let unnamed_since = IggyTimestamp::from(1_700_000_000_000_000);
        let grace = u64::try_from(FRESH_OBJECT_GRACE.as_micros()).expect("the grace fits");
        assert!(sweep(&mut sim, PRIMARY, unnamed_since));
        assert!(sweep(
            &mut sim,
            PRIMARY,
            IggyTimestamp::from(unnamed_since.as_micros() + grace - 1)
        ));
        assert_eq!(
            fresh_objects(&store),
            objects,
            "the backup can read the objects until the grace period ends, seed {seed:#x}"
        );

        assert!(sweep(&mut sim, PRIMARY, past_grace(unnamed_since)));
        assert_eq!(
            fresh_objects(&store),
            Vec::<u128>::new(),
            "the writer deletes the objects without asking the backup, seed {seed:#x}"
        );
        assert!(
            behind(&mut sim),
            "the backup still names them, seed {seed:#x}"
        );

        cut_off(&mut sim, LAGGING, false);
        assert_eq!(settle_everywhere(&mut sim), last, "seed {seed:#x}");
        settle_floors_at(&mut sim, last);
        assert_eq!(
            named(&mut sim, LAGGING),
            Vec::<u128>::new(),
            "seed {seed:#x}"
        );
        let served = polled(&mut sim, LAGGING);
        for index in 0..SENDS {
            assert_eq!(
                holds(&served, &payload(index)),
                1,
                "send {index}, seed {seed:#x}"
            );
        }
        for replica in 0..REPLICAS {
            assert_eq!(
                polled(&mut sim, replica),
                served,
                "replica {replica}, seed {seed:#x}"
            );
        }
    }
}

/// A fresh object in the store, read whole and decoded under its key, as a
/// reader of the store alone finds it.
struct Stored {
    id: FreshObjectId,
    index: FreshIndex,
    bytes: Bytes,
}

fn stored_objects(store: &Shared) -> Vec<Stored> {
    fresh_objects(store)
        .into_iter()
        .map(|id| {
            let key = layout().fresh_key(id);
            let memory = store.0.store();
            let size = memory
                .head(&key)
                .now_or_never()
                .expect("a memory store answers at once")
                .expect("the object is there")
                .size;
            let bytes = memory
                .get_range(&key, 0..size)
                .now_or_never()
                .expect("a memory store answers at once")
                .expect("the object is there")
                .bytes;
            let id = FreshObjectId::from_raw(id);
            let index = FreshIndex::decode(&bytes, id).expect("every object decodes");
            Stored { id, index, bytes }
        })
        .collect()
}

/// The distinct proof rows of the partition across `objects`, by the offset
/// of their batch. Panics on two different rows for one batch.
fn proven(objects: &[Stored]) -> BTreeMap<u64, FreshProofRow> {
    let mut rows = BTreeMap::new();
    for object in objects {
        let Some(row) = object.index.partition(namespace().inner()) else {
            continue;
        };
        for proof in object.index.proofs_of(row) {
            if let Some(earlier) = rows.insert(proof.base_offset, *proof) {
                assert_eq!(
                    earlier, *proof,
                    "two rows for the batch at {}",
                    proof.base_offset
                );
            }
        }
    }
    rows
}

/// The messages of the batch that `proof` names, from the object that
/// holds them, once the rows rebuild the batch the partition committed.
fn proven_blob<'a>(objects: &'a [Stored], proof: &FreshProofRow) -> &'a [u8] {
    let source = objects
        .iter()
        .find(|object| object.id == proof.object_id)
        .expect("the object a row names is in the store");
    let row = source
        .index
        .partition(namespace().inner())
        .expect("the object holds the partition");
    let placed = PlacedBatch::try_from(
        ProvenBatch::find(proof, row, &source.index).expect("the row names a batch of it"),
    )
    .expect("the rows rebuild the committed placed batch");
    let start = usize::try_from(placed.placement.position).expect("a position fits");
    let end = start + usize::try_from(placed.placement.length).expect("a length fits");
    let blob = &source.bytes[start..end];
    assert!(placed.placement.matches(blob));
    blob
}

/// Every replica proves at the pace of a server.
fn confirm_proofs(sim: &Simulator) {
    for replica in &sim.replicas {
        for shard in &replica.shards {
            shard.set_fresh_confirm_after(shard::FRESH_CONFIRM_AFTER);
        }
    }
}

/// A primary cut off from its backups takes more sends and stores their
/// messages in objects of its own, which never commit. The backups elect a
/// successor that commits more. No proof row in the store names a batch
/// that did not commit, and once the cut heals the store proves every
/// committed batch exactly once, so a reader of the store alone finds each
/// committed message and no other.
#[test]
fn given_a_primary_cut_off_while_it_uploads_when_a_successor_takes_over_should_prove_only_and_all_of_what_committed()
 {
    const BEFORE: usize = 4;
    const ORPHANS: usize = 3;
    const AFTER: usize = 4;
    for seed in SCENARIO_SEEDS {
        let (mut sim, client, store) = placing_cluster(seed);
        confirm_proofs(&sim);
        for index in 0..BEFORE {
            send_until_committed(&mut sim, &client, index);
        }
        settle_everywhere(&mut sim);

        cut_off(&mut sim, PRIMARY, true);
        let before_cut = fresh_objects(&store);
        let _orphans: Vec<Answer> = (BEFORE..BEFORE + ORPHANS)
            .map(|index| {
                let answer = submit_send(&mut sim, &client, PRIMARY, &payload(index));
                for _ in 0..50 {
                    sim.step();
                }
                answer
            })
            .collect();
        let uploaded_while_cut = fresh_objects(&store)
            .into_iter()
            .filter(|id| {
                !before_cut.contains(id) && FreshObjectId::from_raw(*id).replica() == PRIMARY
            })
            .count();
        assert!(
            uploaded_while_cut > 0,
            "the cut-off primary stored objects of its own, seed {seed:#x}"
        );

        let mut successor = None;
        for _ in 0..SETTLE_TICKS {
            successor = (0..REPLICAS)
                .filter(|replica| *replica != PRIMARY)
                .find(|replica| {
                    with_partition(&sim, *replica, |partition| {
                        partition.consensus().is_primary() && partition.consensus().is_normal()
                    })
                });
            if successor.is_some() {
                break;
            }
            sim.step();
        }
        let successor = successor.expect("a backup takes over");
        for index in BEFORE + ORPHANS..BEFORE + ORPHANS + AFTER {
            let committed = (0..50).any(|_| {
                let answer = submit_send(&mut sim, &client, successor, &payload(index));
                wait_for(&mut sim, &answer) && sent_was_committed(&answer)
            });
            assert!(committed, "send {index} commits, seed {seed:#x}");
        }
        cut_off(&mut sim, PRIMARY, false);
        let last = settle_everywhere(&mut sim);

        let committed: Vec<usize> = (0..BEFORE)
            .chain(BEFORE + ORPHANS..BEFORE + ORPHANS + AFTER)
            .collect();
        assert_eq!(last + 1, committed.len() as u64, "seed {seed:#x}");
        let mut rows = BTreeMap::new();
        for _ in 0..SETTLE_TICKS {
            rows = proven(&stored_objects(&store));
            if rows.len() as u64 > last {
                break;
            }
            sim.step();
        }
        let objects = stored_objects(&store);
        assert_eq!(
            rows.keys().copied().collect::<Vec<_>>(),
            (0..=last).collect::<Vec<_>>(),
            "every committed batch is proven, seed {seed:#x}"
        );
        let served = polled(&mut sim, successor);
        for (offset, proof) in &rows {
            let blob = proven_blob(&objects, proof);
            let matching: Vec<usize> = (0..BEFORE + ORPHANS + AFTER)
                .filter(|index| holds(blob, &payload(*index)) == 1)
                .collect();
            assert_eq!(matching.len(), 1, "the batch at {offset}, seed {seed:#x}");
            assert!(
                committed.contains(&matching[0]),
                "the row at {offset} names send {}, which never committed, seed {seed:#x}",
                matching[0]
            );
            assert_eq!(holds(&served, &payload(matching[0])), 1, "seed {seed:#x}");
        }
    }
}

/// The proof rows `replica` holds, by the offset of their batch, once they
/// hold every committed batch that may lack a stored proof.
fn covering_rows(sim: &Simulator, replica: u8) -> Option<BTreeMap<u64, FreshProofRow>> {
    with_partition(sim, replica, |partition| {
        let committed_end = partition.offset.load(std::sync::atomic::Ordering::Acquire) + 1;
        partition
            .fresh_proofs()
            .filter(|proofs| proofs.covers(committed_end, partition.long_term_floor()))
            .map(|proofs| {
                proofs
                    .rows_from(0)
                    .map(|row| (row.base_offset, *row))
                    .collect()
            })
    })
}

/// The primary restarts with batches it committed whose proofs it may not
/// have stored, and no commit of them runs again. It rebuilds their rows
/// from what it holds, the same rows the replicas that stayed up hold, and
/// once it leads again the store proves every committed batch.
#[test]
fn given_a_primary_that_restarts_with_unproven_batches_when_it_leads_again_should_prove_every_committed_batch()
 {
    const SENDS: usize = 4;
    for seed in SCENARIO_SEEDS {
        let (mut sim, client, store) = placing_cluster(seed);
        for index in 0..SENDS {
            send_until_committed(&mut sim, &client, index);
        }
        let last = settle_everywhere(&mut sim);

        sim.replica_crash(PRIMARY);
        restart(&mut sim, PRIMARY, &store);
        confirm_proofs(&sim);
        assert!(
            with_partition(&sim, PRIMARY, |partition| partition
                .fresh_proofs()
                .is_none()),
            "the restart lost the rows, seed {seed:#x}"
        );
        assert_eq!(settle_everywhere(&mut sim), last, "seed {seed:#x}");
        let mut rebuilt = None;
        for _ in 0..SETTLE_TICKS {
            rebuilt = covering_rows(&sim, PRIMARY);
            if rebuilt.is_some() {
                break;
            }
            sim.step();
        }
        let rebuilt = rebuilt.expect("the restarted replica rebuilds its rows");
        let others = (0..REPLICAS)
            .filter(|replica| *replica != PRIMARY)
            .filter_map(|replica| covering_rows(&sim, replica));
        for held in others {
            for (offset, row) in &rebuilt {
                if let Some(other) = held.get(offset) {
                    assert_eq!(row, other, "the batch at {offset}, seed {seed:#x}");
                }
            }
        }

        for replica in (0..REPLICAS).filter(|replica| *replica != PRIMARY) {
            sim.replica_crash(replica);
            restart(&mut sim, replica, &store);
        }
        confirm_proofs(&sim);
        await_primary(&mut sim);
        settle_everywhere(&mut sim);
        let mut rows = BTreeMap::new();
        for _ in 0..SETTLE_TICKS {
            rows = proven(&stored_objects(&store));
            if rows.len() as u64 > last {
                break;
            }
            sim.step();
        }
        assert_eq!(
            rows.keys().copied().collect::<Vec<_>>(),
            (0..=last).collect::<Vec<_>>(),
            "every committed batch is proven, seed {seed:#x}"
        );
    }
}
