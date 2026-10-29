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

#![allow(clippy::future_not_send)]

use std::time::Duration;

use super::*;
use consensus::{ClientTable, PartitionsHandle, build_reply_message_with};
use iggy_binary_protocol::requests::partitions::InstallConsumerGroupOwnerRequest;
use iggy_binary_protocol::{Operation, PrepareHeader};
use iggy_common::{
    ConsumerGroupId, ConsumerKind, ConsumerOffset, DEFAULT_POLL_MAX_BYTES, IggyByteSize,
    IggyTimestamp,
};
use metadata::IggyMetadata;
use metadata::stm::consumer_group::{ConsumerGroup, ConsumerGroupMember};
use metadata::stm::stream::{Partition as MetadataPartition, Stream, StreamsInner, Topic};
use metadata::stm::user::Users;
use partitions::{Fragment, IggyPartitions};
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{PartitionLocation, ShardId};

use crate::poll::completion::PollCompletionLane;
use crate::poll::test_support::{
    PollTestMetadata, commit_messages, install_group_owner, partition_with_messages,
};
use crate::poll::timeout_tests::{PollTestBus, evict_messages_for_disk_dispatch};
use crate::shards_table::PapayaShardsTable;
use crate::{
    LifecycleFrame, PARTITION_READ_TIMEOUT, PartitionConsensusConfig, Receiver, ReplicaTopology,
    ShardFrame, ShardIdentity, channel, shard_channel,
};

const DEADLINE: u64 = 100;
const REQUEST_DEADLINE: u64 = DEADLINE + iggy_common::MIN_DEFERRED_POLL_REPLY_MARGIN_US;
const CLIENT: u128 = 41;
/// The metadata op that installs the fixture's consumer group owners.
const METADATA_OP: u64 = 1;
fn namespace() -> IggyNamespace {
    partition_namespace(0)
}

fn partition_namespace(partition: usize) -> IggyNamespace {
    IggyNamespace::new(0, 0, partition)
}
type TestShard = IggyShard<PollTestBus, (), (), PollTestMetadata, PapayaShardsTable>;

async fn owner(payloads: &[&str]) -> TestShard {
    owner_of(&[payloads]).await
}

/// One owner shard with a partition for each entry of `partitions`.
async fn owner_of(partitions: &[&[&str]]) -> TestShard {
    shard_of(0, PollTestBus::default(), partitions).await
}

/// Shard `shard` that owns a partition for each entry of `partitions`.
async fn shard_of(shard: u16, bus: PollTestBus, partitions: &[&[&str]]) -> TestShard {
    let mut plane = None;
    let mut topic = Topic::default();
    let routes = PapayaShardsTable::new();
    for (id, payloads) in partitions.iter().enumerate() {
        let namespace = partition_namespace(id);
        let (partition, config) = partition_with_messages(&bus, namespace, payloads).await;
        plane
            .get_or_insert_with(|| IggyPartitions::new(ShardId::new(shard), config))
            .insert(namespace, partition);
        topic.partitions.push(MetadataPartition::new(
            id,
            namespace.inner(),
            IggyTimestamp::default(),
            1,
            0,
        ));
        routes.insert(namespace, PartitionLocation::new(ShardId::new(shard), 1));
    }
    let partitions_plane = plane.expect("fixture requires a partition");
    let mut inner = StreamsInner::default();
    let mut stream = Stream::default();
    let mut group = ConsumerGroup::new(0, Arc::from("group"));
    let mut member = ConsumerGroupMember::new(0, CLIENT);
    member.session = Some(1);
    group.members.insert(member);
    group.rebalance_members(&topic.partitions, METADATA_OP, 1);
    let config = partitions_plane.config().clone();
    for id in 0..partitions.len() {
        let assignment = &group.assignments[&id];
        let pending = assignment.pending.unwrap();
        let installation = InstallConsumerGroupOwnerRequest {
            incarnation: assignment.incarnation,
            group_id: 0,
            owner: pending.owner,
            metadata_op: pending.metadata_op,
        };
        let partition = partitions_plane
            .get_mut_by_ns(&partition_namespace(id))
            .unwrap();
        let op = install_group_owner(partition, &config, installation).await;
        assert!(group.complete_revocation(id, &installation, op));
    }
    topic.consumer_groups.insert(0, group);
    stream.topics.insert(topic);
    inner.items.insert(stream);
    let users = Users::default();
    users.ensure_root_user("iggy", "password-hash");
    let metadata = IggyMetadata::new(
        None,
        None,
        None,
        None,
        PollTestMetadata::new((users, (inner.into(), ()))),
        None,
    );
    TestShard::without_inbox(
        ShardIdentity::new(shard, "deferred-test".to_string()),
        bus.clone(),
        metadata,
        partitions_plane,
        routes,
        PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus),
    )
}

fn request(owner: &TestShard, count: u32, group: bool, auto_commit: bool) -> DeferredPollRequest {
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace(), group.then_some(0), CLIENT)
        .unwrap();
    let mut request = DeferredPollRequest::new(
        if group {
            PollingConsumer::ConsumerGroup(0, 0)
        } else {
            PollingConsumer::Consumer(0, 0)
        },
        PollingArgs {
            kind: PollingKind::Next,
            value: 0,
            count,
            auto_commit,
        },
        metadata,
        DeferredPollContext {
            session: None,
            user_id: 0,
            user_created_at: None,
            quota: PollQuotaIdentity::Session(CLIENT),
            primary: false,
        },
        iggy_common::DeferredPollOptions {
            max_wait: DEADLINE.into(),
            min_count: count,
            request_timeout: REQUEST_DEADLINE.into(),
            ..Default::default()
        },
    );
    request.deadline = DEADLINE;
    request.request_deadline = REQUEST_DEADLINE;
    request
}

async fn submit(owner: &TestShard, request: DeferredPollRequest) -> Receiver<PartitionReadReply> {
    let (reply, replies) = channel(1);
    owner.admit_deferred_poll(namespace(), request, &reply);
    owner.service_deferred_polls().await;
    replies
}

fn offsets(replies: &Receiver<PartitionReadReply>) -> Vec<u64> {
    let PartitionReadReply::Poll { fragments, .. } = replies.try_recv().expect("poll completed")
    else {
        panic!("poll was rejected");
    };
    let bytes: Vec<u8> = fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let mut bytes = bytes.as_slice();
    let mut offsets = Vec::new();
    while !bytes.is_empty() {
        let batch = decode_batch_slice(bytes).unwrap();
        offsets.extend(
            batch
                .iter()
                .map(|message| batch.header.base_offset + u64::from(message.header.offset_delta)),
        );
        bytes = &bytes[batch.header.total_size()..];
    }
    offsets
}

fn pending(replies: &Receiver<PartitionReadReply>) {
    assert!(matches!(
        replies.try_recv(),
        Err(crossfire::TryRecvError::Empty)
    ));
}

/// What a disk read of the fixture's `request` reserves.
fn read_bytes(owner: &TestShard) -> usize {
    owner
        .deferred_polls
        .read_bytes(request(owner, 1, false, false).options.max_bytes)
}

/// Fills the read budget with reservations the size of a fixture read, so
/// that dropping one makes room for exactly one such read.
fn hold_read_budget(owner: &TestShard) -> Vec<ReadReservation> {
    let bytes = read_bytes(owner);
    std::iter::from_fn(|| owner.deferred_polls.reserve_read(bytes).ok()).collect()
}

#[compio::test]
async fn deferred_completion_capacity_is_distinct_from_a_closed_lane() {
    let owner = owner(&["first"]).await;
    let lane = PollCompletionLane::new(1, &owner.metrics);
    let reserve = || {
        owner
            .deferred_polls
            .reserve_read(read_bytes(&owner))
            .unwrap()
    };
    let active = lane
        .try_reserve_deferred(namespace(), 1, reserve())
        .unwrap();
    assert!(matches!(
        lane.try_reserve_deferred(namespace(), 2, reserve()),
        Err(IggyError::DeferredPollCapacityExceeded)
    ));
    lane.close();
    assert!(matches!(
        lane.try_reserve_deferred(namespace(), 3, reserve()),
        Err(IggyError::TransientNotAccepted)
    ));
    drop(active);
    assert_eq!(
        owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
        0
    );
}

#[compio::test]
async fn resident_poll_completes_while_disk_reads_hold_the_whole_read_budget() {
    let owner = owner(&["first", "second"]).await;
    let held = hold_read_budget(&owner);
    assert!(!held.is_empty());
    assert!(matches!(
        owner.deferred_polls.reserve_read(read_bytes(&owner)),
        Err(IggyError::DeferredPollCapacityExceeded)
    ));
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    assert_eq!(offsets(&replies), [0, 1]);
}

#[compio::test]
async fn freed_read_budget_starts_refused_disk_reads_in_refusal_order() {
    let owner = owner(&["first"]).await;
    // One message is below `min_count`, so the older waiter parks after a
    // resident read and needs a disk read once the journal drops the batch.
    let older = submit(&owner, request(&owner, 2, false, false)).await;
    pending(&older);
    evict_messages_for_disk_dispatch(&owner, namespace());
    let mut held = hold_read_budget(&owner);
    let refused = submit(&owner, request(&owner, 2, false, false)).await;
    pending(&refused);
    assert_eq!(
        owner.deferred_polls.state.borrow().read_queue,
        BTreeSet::from([(0, 2)])
    );

    drop(held.pop());
    owner.bus.now.set(DEADLINE);
    owner.service_deferred_polls().await;
    {
        let state = owner.deferred_polls.state.borrow();
        assert!(state.waiters[&2].running, "the read refused first starts");
        assert!(
            !state.waiters[&1].running,
            "an older waiter queues behind a refused read"
        );
        assert_eq!(state.read_queue, BTreeSet::from([(DEADLINE, 1)]));
        assert!(state.ready.is_empty(), "a full budget wakes nobody");
    }

    // Dropping the captured read frees its completion slot and its
    // reservation, as a finished read does.
    owner.bus.spawned_tasks.borrow_mut().clear();
    assert!(
        owner
            .deferred_polls
            .poll_ready(&Context::from_waker(Waker::noop()))
            .is_ready(),
        "freed room wakes the pump"
    );
    owner.service_deferred_polls().await;
    let state = owner.deferred_polls.state.borrow();
    assert!(state.waiters[&1].running, "freed room wakes the next read");
    assert!(state.read_queue.is_empty());
}

#[compio::test]
async fn freed_read_budget_starts_a_refused_read_ahead_of_other_wakes() {
    let owner = owner(&["first"]).await;
    evict_messages_for_disk_dispatch(&owner, namespace());
    let mut held = hold_read_budget(&owner);
    // Admitted first, these waits take the lower ids, so a maintenance wake
    // queues them ahead of the refused read and fills a whole service turn.
    let waits: Vec<_> = (0..SERVICE_BUDGET)
        .map(|_| {
            wait(
                &owner,
                vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
            )
        })
        .collect();
    let refused = submit(&owner, request(&owner, 2, false, false)).await;
    let refused_id = u64::try_from(SERVICE_BUDGET).unwrap() + 1;
    assert_eq!(
        owner.deferred_polls.state.borrow().read_queue,
        BTreeSet::from([(0, refused_id)])
    );

    drop(held.pop());
    owner.deferred_polls.maintenance();
    owner.service_deferred_polls().await;
    pending(&refused);
    let state = owner.deferred_polls.state.borrow();
    assert!(
        state.waiters[&refused_id].running,
        "freed room goes to the refused read first"
    );
    assert!(state.read_queue.is_empty());
    assert_eq!(state.readiness.len(), waits.len());
}

#[compio::test]
async fn cancelled_refused_read_does_not_hold_back_later_reads() {
    let owner = owner(&["first"]).await;
    evict_messages_for_disk_dispatch(&owner, namespace());
    let mut held = hold_read_budget(&owner);
    let cancelled = submit(&owner, request(&owner, 2, false, false)).await;
    let later = submit(&owner, request(&owner, 2, false, false)).await;
    assert_eq!(owner.deferred_polls.state.borrow().read_queue.len(), 2);

    drop(cancelled);
    drop(held.pop());
    owner.service_deferred_polls().await;
    pending(&later);
    let state = owner.deferred_polls.state.borrow();
    assert!(!state.waiters.contains_key(&1));
    assert!(state.waiters[&2].running);
    assert!(state.read_queue.is_empty());
}

#[compio::test]
async fn disk_reads_reserve_three_parts_of_their_max_bytes_or_one_chunk() {
    // A small max_bytes must not let more reads start than chunk-sized parts fit.
    for (max_bytes, part) in [
        (DEFAULT_POLL_MAX_BYTES, DEFAULT_POLL_MAX_BYTES as usize),
        (4 << 10, READ_PART_MIN_BYTES),
    ] {
        let mut owner = owner(&["first"]).await;
        let per_read = READ_BUDGET_PARTS * part;
        let reads = owner.deferred_polls.config.borrow().max_inflight_bytes / per_read;
        owner.poll_completions = PollCompletionLane::new(reads + 1, &owner.metrics);
        evict_messages_for_disk_dispatch(&owner, namespace());
        let mut replies = Vec::with_capacity(reads + 1);
        for _ in 0..=reads {
            let mut request = request(&owner, 2, false, false);
            request.options.max_bytes = max_bytes;
            replies.push(submit(&owner, request).await);
        }
        replies.iter().for_each(pending);
        let state = owner.deferred_polls.state.borrow();
        assert_eq!(
            state
                .waiters
                .values()
                .filter(|waiter| waiter.running)
                .count(),
            reads,
            "every read that fits the budget starts, max_bytes {max_bytes}"
        );
        assert_eq!(
            state.read_queue.len(),
            1,
            "the read past the budget waits, max_bytes {max_bytes}"
        );
        assert_eq!(
            owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
            reads * per_read,
            "max_bytes {max_bytes}"
        );
    }
}

#[compio::test]
async fn disk_read_reserves_at_most_max_read_bytes() {
    let mut owner = owner(&["first"]).await;
    let max_read_bytes = owner.deferred_polls.config.borrow().max_read_bytes;
    let ceiling = u32::try_from(max_read_bytes.div_ceil(READ_BUDGET_PARTS)).unwrap();
    let cases = [
        (ceiling - 1, READ_BUDGET_PARTS * (ceiling - 1) as usize),
        (ceiling, max_read_bytes),
        (u32::MAX, max_read_bytes),
    ];
    owner.poll_completions = PollCompletionLane::new(cases.len(), &owner.metrics);
    evict_messages_for_disk_dispatch(&owner, namespace());
    let mut replies = Vec::with_capacity(cases.len());
    for (max_bytes, reserved) in cases {
        let before = owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed);
        let mut request = request(&owner, 2, false, false);
        request.options.max_bytes = max_bytes;
        replies.push(submit(&owner, request).await);
        assert_eq!(
            owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed) - before,
            reserved,
            "max_bytes {max_bytes}"
        );
    }
    replies.iter().for_each(pending);
    let held = owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed);
    assert_eq!(
        owner.deferred_polls.metrics.inflight_bytes.get(),
        i64::try_from(held).unwrap()
    );

    // Dropping the captured reads releases each reservation by its own size.
    owner.bus.spawned_tasks.borrow_mut().clear();
    assert_eq!(
        owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
        0
    );
    assert_eq!(owner.deferred_polls.metrics.inflight_bytes.get(), 0);
}

#[compio::test]
async fn refused_large_read_keeps_smaller_reads_behind_it_waiting() {
    let mut owner = owner(&["first"]).await;
    owner.poll_completions = PollCompletionLane::new(2, &owner.metrics);
    evict_messages_for_disk_dispatch(&owner, namespace());
    let config = *owner.deferred_polls.config.borrow();
    // Room for a fixture read, but not for one at `max_read_bytes`.
    let held = owner
        .deferred_polls
        .reserve_read(config.max_inflight_bytes - read_bytes(&owner))
        .unwrap();
    let mut large = request(&owner, 2, false, false);
    large.options.max_bytes = u32::try_from(config.max_read_bytes).unwrap();
    let large = submit(&owner, large).await;
    let small = submit(&owner, request(&owner, 2, false, false)).await;
    {
        let state = owner.deferred_polls.state.borrow();
        assert_eq!(state.read_queue, BTreeSet::from([(0, 1), (0, 2)]));
        assert!(
            !state.waiters[&2].running,
            "a smaller read that fits does not overtake the refused one"
        );
    }
    let context = Context::from_waker(Waker::noop());
    assert!(
        owner.deferred_polls.poll_ready(&context).is_pending(),
        "room for a smaller read wakes nobody"
    );

    drop(held);
    assert!(
        owner.deferred_polls.poll_ready(&context).is_ready(),
        "room for the head read wakes the pump"
    );
    owner.service_deferred_polls().await;
    pending(&large);
    pending(&small);
    let state = owner.deferred_polls.state.borrow();
    assert!(state.waiters[&1].running);
    assert!(state.waiters[&2].running);
    assert!(state.read_queue.is_empty());
}

/// Parks `request` after a first disk read that selected only offset 0, with
/// that message evicted from the journal.
async fn parked_after_disk_read(
    owner: &TestShard,
    request: DeferredPollRequest,
) -> Receiver<PartitionReadReply> {
    let first_read = resident_read(owner, request.consumer, request.args.count);
    evict_messages_for_disk_dispatch(owner, namespace());
    let replies = submit(owner, request).await;
    assert_eq!(owner.bus.spawned_tasks.borrow().len(), 1, "a disk read");
    // Dropping the captured read frees its completion slot and reservation,
    // and the result arrives as a read of the evicted message would.
    owner.bus.spawned_tasks.borrow_mut().clear();
    owner.on_deferred_poll_completed(1, first_read).await;
    pending(&replies);
    replies
}

fn resident_read(owner: &TestShard, consumer: PollingConsumer, count: u32) -> PollReadResult {
    owner
        .plane
        .partitions()
        .build_poll_snapshot(
            &namespace(),
            consumer,
            &PollingArgs::new(PollingStrategy::offset(0), count, false),
        )
        .unwrap()
        .execute_resident()
}

async fn commit(owner: &TestShard, payloads: &[&str], request: u64) {
    commit_to(owner, 0, payloads, request).await;
}

async fn commit_to(owner: &TestShard, partition: usize, payloads: &[&str], request: u64) {
    let namespace = partition_namespace(partition);
    let partitions = owner.plane.partitions();
    let mut stored = partitions.remove(&namespace).unwrap();
    commit_messages(
        &mut stored,
        partitions.config(),
        namespace,
        payloads,
        (request, METADATA_OP),
    )
    .await;
    partitions.insert(namespace, stored);
}

/// Commits `payloads` and, like a persisted topic on one replica, can drop
/// them from the journal in the same step.
async fn commit_and_evict(owner: &TestShard, payloads: &[&str], request: u64, evict: bool) {
    commit(owner, payloads, request).await;
    if evict {
        evict_messages_for_disk_dispatch(owner, namespace());
    }
}

/// Routes timestamp polls to the disk, as segment bytes do.
fn persist_segment_bytes(owner: &TestShard) {
    let partitions = owner.plane.partitions();
    let mut partition = partitions.remove(&namespace()).unwrap();
    partition.log.active_segment_mut().size = IggyByteSize::from(1);
    partitions.insert(namespace(), partition);
}

#[compio::test]
async fn waiter_reads_from_disk_again_only_once_new_messages_can_reach_min_count() {
    for evict in [false, true] {
        let owner = owner(&["first"]).await;
        let replies = parked_after_disk_read(&owner, request(&owner, 3, false, false)).await;

        commit_and_evict(&owner, &["second"], 2, evict).await;
        owner.service_deferred_polls().await;
        pending(&replies);
        assert!(
            owner.bus.spawned_tasks.borrow().is_empty(),
            "two of three messages start no read, evicted: {evict}"
        );
        assert_eq!(
            owner.deferred_polls.state.borrow().waiters[&1]
                .last_visibility
                .map(|visibility| visibility.offset),
            Some(Some(1))
        );

        commit_and_evict(&owner, &["third"], 3, evict).await;
        owner.service_deferred_polls().await;
        assert_eq!(
            owner.bus.spawned_tasks.borrow().len(),
            1,
            "three messages start a read, evicted: {evict}"
        );
    }
}

#[compio::test]
async fn timestamp_waiter_reads_again_only_once_new_messages_can_reach_min_count() {
    let owner = owner(&["first"]).await;
    let mut request = request(&owner, 3, false, false);
    request.args = PollingArgs::new(PollingStrategy::timestamp(IggyTimestamp::from(0)), 3, false);
    persist_segment_bytes(&owner);
    let replies = parked_after_disk_read(&owner, request).await;

    commit(&owner, &["second"], 2).await;
    owner.service_deferred_polls().await;
    pending(&replies);
    assert!(
        owner.bus.spawned_tasks.borrow().is_empty(),
        "two of three messages start no read"
    );

    commit(&owner, &["third"], 3).await;
    owner.service_deferred_polls().await;
    assert_eq!(owner.bus.spawned_tasks.borrow().len(), 1);
}

#[compio::test]
async fn waiter_below_min_count_reads_again_once_new_bytes_reach_the_byte_limit() {
    // Payloads of one length keep every batch at `batch_bytes`.
    let owner = owner(&["first"]).await;
    let mut request = request(&owner, 100, false, false);
    let batch_bytes = resident_read(&owner, request.consumer, 1).encoded_bytes();
    request.options.max_bytes =
        u32::try_from(POLL_RESPONSE_HEADER_SIZE + batch_bytes * 5 / 2).unwrap();
    let replies = parked_after_disk_read(&owner, request).await;

    commit(&owner, &["third"], 2).await;
    owner.service_deferred_polls().await;
    pending(&replies);
    assert!(
        owner.bus.spawned_tasks.borrow().is_empty(),
        "two batches stay below the byte limit"
    );

    commit(&owner, &["fifth"], 3).await;
    owner.service_deferred_polls().await;
    assert_eq!(
        owner.bus.spawned_tasks.borrow().len(),
        1,
        "three batches reach the byte limit before min_count"
    );
}

#[compio::test]
async fn commit_below_min_count_probes_no_parked_waiter() {
    // One waiter more than a pump turn serves, so a probe of each would leave
    // one queued.
    const WAITERS: usize = SERVICE_BUDGET + 1;
    for reach_min_count in [true, false] {
        let owner = owner(&["first"]).await;
        let mut waiters = Vec::with_capacity(WAITERS);
        for _ in 0..WAITERS {
            waiters.push(submit(&owner, request(&owner, 3, false, false)).await);
        }

        commit(&owner, &["second"], 2).await;
        owner.service_deferred_polls().await;
        waiters.iter().for_each(pending);
        {
            let state = owner.deferred_polls.state.borrow();
            assert!(
                state.ready.is_empty(),
                "two of three messages probe no waiter"
            );
            assert!(
                state.waiters.values().all(|waiter| {
                    waiter.last_visibility.map(|visibility| visibility.offset) == Some(Some(1))
                }),
                "each waiter counts the second message"
            );
        }

        if reach_min_count {
            commit(&owner, &["third"], 3).await;
        } else {
            owner.bus.now.set(DEADLINE);
        }
        owner.service_deferred_polls().await;
        owner.service_deferred_polls().await;
        let expected: &[u64] = if reach_min_count { &[0, 1, 2] } else { &[0, 1] };
        for replies in &waiters {
            assert_eq!(
                offsets(replies),
                expected,
                "min_count reached: {reach_min_count}"
            );
        }
    }
}

#[compio::test]
async fn replacement_history_rejects_parked_poll_on_its_next_signal() {
    let owner = owner(&["old"]).await;
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    let (replacement, _) = partition_with_messages(&owner.bus, namespace(), &["new"]).await;
    owner.plane.partitions().insert(namespace(), replacement);
    // A partition publishes its visibility when it replaces its history.
    owner.deferred_polls.signals.borrow_mut().mark(namespace());
    owner.service_deferred_polls().await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
}

#[compio::test]
async fn read_budget_caps_a_resident_reply_after_its_first_message() {
    let owner = owner(&["first", "second"]).await;
    owner.set_deferred_poll_config(DeferredPollConfig {
        max_read_bytes: 1,
        ..Default::default()
    });
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    assert_eq!(offsets(&replies), [0]);
}

#[compio::test]
async fn full_result_completes_before_deadline() {
    let owner = owner(&["first", "second"]).await;
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    assert_eq!(offsets(&replies), [0, 1]);
    assert!(owner.deferred_polls.state.borrow().waiters.is_empty());
    assert_eq!(
        owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
        0
    );
}

#[compio::test]
async fn partial_result_waits_until_deadline_without_progress() {
    for group in [false, true] {
        let owner = owner(&["first", "second"]).await;
        let request = request(&owner, 3, group, false);
        let consumer = request.consumer;
        let replies = submit(&owner, request).await;
        pending(&replies);
        assert_eq!(
            owner
                .plane
                .partitions()
                .consumer_offset_read(&namespace(), consumer)
                .unwrap()
                .0,
            None
        );
        owner.bus.now.set(DEADLINE - 1);
        owner.service_deferred_polls().await;
        pending(&replies);
        owner.bus.now.set(DEADLINE);
        owner.service_deferred_polls().await;
        assert_eq!(offsets(&replies), [0, 1]);
        assert!(
            owner.bus.spawned_tasks.borrow().is_empty(),
            "resident expiry does not require IO"
        );
        assert!(owner.deferred_polls.state.borrow().deadlines.is_empty());
    }
}

#[compio::test]
async fn readiness_expiry_returns_committed_data_even_after_the_wait() {
    for arrival in [DEADLINE - 1, DEADLINE, DEADLINE + 1] {
        let owner = owner(&["first"]).await;
        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap()
            .set_offset_space_used(false);
        let replies = submit(&owner, request(&owner, 1, false, true)).await;
        pending(&replies);
        owner.bus.now.set(arrival);
        owner
            .plane
            .partitions()
            .get_mut_by_ns(&namespace())
            .unwrap()
            .set_offset_space_used(true);
        owner.service_deferred_polls().await;
        assert_eq!(offsets(&replies), vec![0]);
    }
}

#[compio::test]
async fn expired_or_cancelled_waiter_discards_queued_read_without_progress() {
    for cancel in [false, true] {
        let owner = owner(&["first"]).await;
        let request = request(&owner, 2, false, true);
        let consumer = request.consumer;
        let replies = submit(&owner, request).await;
        let id = *owner
            .deferred_polls
            .state
            .borrow()
            .waiters
            .keys()
            .next()
            .unwrap();
        let result = owner
            .plane
            .partitions()
            .build_poll_snapshot(
                &namespace(),
                consumer,
                &PollingArgs {
                    kind: PollingKind::First,
                    value: 0,
                    count: 1,
                    auto_commit: true,
                },
            )
            .unwrap()
            .execute_resident();
        // A read dispatched before expiry still owns its byte reservation after cancellation.
        let reservation = owner
            .deferred_polls
            .reserve_read(read_bytes(&owner))
            .unwrap();
        owner
            .poll_completions
            .try_reserve_deferred(namespace(), id, reservation)
            .unwrap()
            .complete(result);
        owner.bus.now.set(DEADLINE);
        if cancel {
            drop(replies);
        } else {
            owner.service_deferred_polls().await;
            assert_eq!(offsets(&replies), [0]);
        }
        assert!(owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed) > 0);
        let completion = owner.poll_completions.try_recv().unwrap();
        assert!(owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed) > 0);
        owner.on_poll_completed(*completion).await;
        assert!(owner.deferred_polls.state.borrow().waiters.is_empty());
        assert_eq!(
            owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
            0
        );
        if cancel {
            assert_eq!(
                owner
                    .plane
                    .partitions()
                    .consumer_offset_read(&namespace(), consumer)
                    .unwrap()
                    .0,
                None
            );
        }
    }
}

#[compio::test]
async fn replacement_history_invalidates_pending_manual_poll() {
    let owner = owner(&["old"]).await;
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    let (replacement, _) = partition_with_messages(&owner.bus, namespace(), &["new"]).await;
    owner.plane.partitions().insert(namespace(), replacement);
    owner.bus.now.set(DEADLINE);
    owner.service_deferred_polls().await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
}

/// Applies an explicit offset store of another session. Unlike the commit of
/// a poll, it can move the offset back.
fn store_offset(owner: &TestShard, consumer: PollingConsumer, offset: u64) {
    owner
        .plane
        .partitions()
        .with_partition(&namespace(), |partition| match consumer {
            PollingConsumer::Consumer(id, _) => {
                let stored = ConsumerOffset::new(
                    ConsumerKind::Consumer,
                    u32::try_from(id).unwrap(),
                    offset,
                    String::new(),
                );
                partition.consumer_offsets.pin().insert(id, stored);
            }
            PollingConsumer::ConsumerGroup(id, _) => {
                let stored = ConsumerOffset::new(
                    ConsumerKind::ConsumerGroup,
                    u32::try_from(id).unwrap(),
                    offset,
                    String::new(),
                );
                partition
                    .consumer_group_offsets
                    .pin()
                    .insert(ConsumerGroupId(id), stored);
            }
        })
        .unwrap();
}

fn stored_offset(owner: &TestShard, consumer: PollingConsumer) -> Option<u64> {
    owner
        .plane
        .partitions()
        .consumer_offset_read(&namespace(), consumer)
        .unwrap()
        .0
}

#[compio::test]
async fn offset_that_another_session_stores_rejects_parked_next_poll() {
    // A rewind asks for messages again, and a store ahead marks them read.
    for stored in [0, 2] {
        for group in [false, true] {
            for exit in ["message", "deadline"] {
                let owner = owner(&["a", "b", "c", "d"]).await;
                let poll = request(&owner, 3, group, true);
                let consumer = poll.consumer;
                store_offset(&owner, consumer, 1);
                // Two of the three messages it needs, so it parks at offset 2.
                let replies = submit(&owner, poll).await;
                pending(&replies);

                store_offset(&owner, consumer, stored);
                if exit == "message" {
                    commit(&owner, &["e"], 2).await;
                } else {
                    owner.bus.now.set(DEADLINE);
                }
                owner.service_deferred_polls().await;

                let reply = replies.try_recv();
                assert!(
                    matches!(
                        reply,
                        Ok(PartitionReadReply::Rejected(
                            IggyError::TransientNotAccepted
                        ))
                    ),
                    "a read from offset 2 passes over the store of {stored}, exit {exit}: {reply:?}"
                );
                assert_eq!(
                    stored_offset(&owner, consumer),
                    Some(stored),
                    "the refused poll commits nothing"
                );
                assert!(owner.deferred_polls.state.borrow().waiters.is_empty());
                if stored == 0 {
                    assert_eq!(
                        offsets(&submit(&owner, request(&owner, 3, group, true)).await),
                        [1, 2, 3],
                        "the poll again reads the rewound messages"
                    );
                }
            }
        }
    }
}

#[compio::test]
async fn offset_that_another_session_stores_again_lets_parked_next_poll_finish() {
    let owner = owner(&["a", "b", "c", "d"]).await;
    let poll = request(&owner, 3, false, true);
    let consumer = poll.consumer;
    store_offset(&owner, consumer, 1);
    let replies = submit(&owner, poll).await;
    pending(&replies);

    store_offset(&owner, consumer, 1);
    commit(&owner, &["e"], 2).await;
    owner.service_deferred_polls().await;

    assert_eq!(offsets(&replies), [2, 3, 4]);
    assert_eq!(stored_offset(&owner, consumer), Some(4));
}

#[compio::test]
async fn full_shard_quota_rejects_another_session_without_poll_progress() {
    let owner = owner(&["first"]).await;
    owner.set_deferred_poll_config(DeferredPollConfig {
        max_pending: 1,
        max_pending_per_session: 1,
        ..Default::default()
    });
    let first = submit(&owner, request(&owner, 2, false, false)).await;
    let mut other = request(&owner, 2, false, true);
    other.context.quota = PollQuotaIdentity::Session(CLIENT + 1);
    let consumer = other.consumer;
    let refused = submit(&owner, other).await;
    assert!(matches!(
        refused.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::DeferredPollCapacityExceeded
        ))
    ));
    assert_eq!(
        owner
            .plane
            .partitions()
            .consumer_offset_read(&namespace(), consumer)
            .unwrap()
            .0,
        None
    );
    drop(first);
    owner.deferred_polls.maintenance();
    owner.service_deferred_polls().await;
    assert_released(&owner);
}

#[compio::test]
async fn session_quota_and_cancellation_release_all_indices() {
    let owner = owner(&["first"]).await;
    owner.set_deferred_poll_config(DeferredPollConfig {
        max_pending_per_session: 1,
        ..Default::default()
    });
    let first = submit(&owner, request(&owner, 2, false, false)).await;
    let second = submit(&owner, request(&owner, 2, false, false)).await;
    assert!(matches!(
        second.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::DeferredPollCapacityExceeded
        ))
    ));
    drop(first);
    owner.deferred_polls.maintenance();
    owner.service_deferred_polls().await;
    let state = owner.deferred_polls.state.borrow();
    assert!(state.waiters.is_empty());
    assert!(state.sessions.is_empty());
    assert!(state.partitions.is_empty());
    assert!(state.deadlines.is_empty());
    assert!(state.ready.is_empty());
}

#[compio::test]
async fn readiness_minimum_is_independent_of_batch_count() {
    let owner = owner(&["available"]).await;
    let mut request = request(&owner, 100, false, false);
    request.options.min_count = 1;
    let replies = submit(&owner, request).await;
    assert_eq!(offsets(&replies), [0]);
}

#[compio::test]
async fn read_crossing_readiness_deadline_finishes_within_request_budget() {
    for completed_at in [DEADLINE + 1, REQUEST_DEADLINE] {
        let owner = owner(&["read-from-snapshot"]).await;
        let request = request(&owner, 2, false, true);
        let consumer = request.consumer;
        let replies = submit(&owner, request).await;
        let id = *owner
            .deferred_polls
            .state
            .borrow()
            .waiters
            .keys()
            .next()
            .unwrap();
        owner
            .deferred_polls
            .state
            .borrow_mut()
            .waiters
            .get_mut(&id)
            .unwrap()
            .running = true;
        let result = owner
            .plane
            .partitions()
            .build_poll_snapshot(
                &namespace(),
                consumer,
                &PollingArgs {
                    kind: PollingKind::First,
                    value: 0,
                    count: 1,
                    auto_commit: true,
                },
            )
            .unwrap()
            .execute_resident();
        let reservation = owner
            .deferred_polls
            .reserve_read(read_bytes(&owner))
            .unwrap();
        let completion = owner
            .poll_completions
            .try_reserve_deferred(namespace(), id, reservation)
            .unwrap();
        owner.bus.now.set(DEADLINE);
        owner.service_deferred_polls().await;
        pending(&replies);
        owner.bus.now.set(completed_at);
        completion.complete(result);
        let completion = owner.poll_completions.try_recv().unwrap();
        owner.on_poll_completed(*completion).await;
        if completed_at < REQUEST_DEADLINE {
            assert_eq!(offsets(&replies), [0]);
        } else {
            assert!(matches!(
                replies.try_recv(),
                Ok(PartitionReadReply::Rejected(
                    IggyError::TransientNotCommitted
                ))
            ));
            assert_eq!(
                owner
                    .plane
                    .partitions()
                    .consumer_offset_read(&namespace(), consumer)
                    .unwrap()
                    .0,
                None
            );
        }
        assert!(owner.deferred_polls.state.borrow().waiters.is_empty());
        assert_eq!(
            owner.deferred_polls.inflight_bytes.load(Ordering::Relaxed),
            0
        );
    }
}

#[compio::test]
async fn poll_admission_is_independent_of_resident_journal_size() {
    // 1.5 MiB resident, one message polled: the read budget bounds the reply,
    // not how much the partition holds.
    let payload = "x".repeat(768 * 1024);
    let owner = owner(&[payload.as_str(); 2]).await;
    let replies = submit(&owner, request(&owner, 1, false, false)).await;
    assert_eq!(offsets(&replies), [0]);
}

fn readiness_target(
    owner: &TestShard,
    partition: usize,
    strategy: PollingStrategy,
) -> ReadinessTarget {
    let namespace = partition_namespace(partition);
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace, None, CLIENT)
        .unwrap();
    ReadinessTarget {
        namespace,
        partition_id: u32::try_from(partition).unwrap(),
        consumer: PollingConsumer::Consumer(0, partition),
        strategy,
        metadata,
    }
}

fn readiness_caller() -> DeferredPollContext {
    DeferredPollContext {
        session: None,
        user_id: 0,
        user_created_at: None,
        quota: PollQuotaIdentity::Session(CLIENT),
        primary: true,
    }
}

fn wait(owner: &TestShard, targets: Vec<ReadinessTarget>) -> Receiver<PartitionReadReply> {
    let (reply, replies) = channel(2);
    owner.admit_readiness_wait(
        ReadinessWait {
            context: readiness_caller(),
            targets,
            deadline: DEADLINE,
            min_count: ANY_MESSAGE,
        },
        &reply,
        None,
    );
    replies
}

/// The parked waiter and the ready partitions of the next answer.
fn answer(replies: &Receiver<PartitionReadReply>) -> (Option<u64>, Vec<u32>) {
    let Ok(PartitionReadReply::Ready {
        shard: 0,
        parked,
        partitions,
    }) = replies.try_recv()
    else {
        panic!("expected a readiness answer from shard 0");
    };
    (parked, ids(&partitions))
}

fn ids(ready: &[ReadyTarget]) -> Vec<u32> {
    ready.iter().map(|target| target.partition_id).collect()
}

fn set_committed(owner: &TestShard, partition: usize, committed: bool) {
    owner
        .plane
        .partitions()
        .get_mut_by_ns(&partition_namespace(partition))
        .unwrap()
        .set_offset_space_used(committed);
}

fn assert_released(owner: &TestShard) {
    let state = owner.deferred_polls.state.borrow();
    assert!(state.readiness.is_empty());
    assert!(state.sessions.is_empty());
    assert!(state.partitions.is_empty());
    assert!(state.deadlines.is_empty());
    assert!(state.ready.is_empty());
    assert!(state.read_queue.is_empty());
}

#[compio::test]
async fn readiness_answers_ready_targets_at_admission_without_parking() {
    let owner = owner_of(&[&["first"], &["first", "second"]]).await;
    let replies = wait(
        &owner,
        vec![
            readiness_target(&owner, 0, PollingStrategy::offset(1)),
            readiness_target(&owner, 1, PollingStrategy::offset(1)),
        ],
    );
    assert_eq!(answer(&replies), (None, vec![1]));
    assert_released(&owner);
}

#[compio::test]
async fn expired_readiness_answers_at_admission_without_parking() {
    let owner = owner(&["first"]).await;
    owner.bus.now.set(DEADLINE);
    let replies = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
    );
    assert_eq!(answer(&replies), (None, Vec::new()));
    assert_released(&owner);
}

#[compio::test]
async fn parked_readiness_reports_every_ready_target_once() {
    let owner = owner_of(&[&["first"], &["first"]]).await;
    set_committed(&owner, 0, false);
    set_committed(&owner, 1, false);
    let replies = wait(
        &owner,
        vec![
            readiness_target(&owner, 0, PollingStrategy::offset(0)),
            readiness_target(&owner, 1, PollingStrategy::offset(0)),
        ],
    );
    let (parked, partitions) = answer(&replies);
    assert!(parked.is_some() && partitions.is_empty());
    owner.service_deferred_polls().await;
    pending(&replies);
    set_committed(&owner, 0, true);
    set_committed(&owner, 1, true);
    owner.service_deferred_polls().await;
    assert_eq!(answer(&replies), (None, vec![0, 1]));
    assert!(
        matches!(
            replies.try_recv(),
            Err(crossfire::TryRecvError::Disconnected)
        ),
        "a waiter answers once"
    );
    assert_released(&owner);
}

#[compio::test]
async fn next_readiness_starts_after_the_stored_offset() {
    let owner = owner(&["first"]).await;
    let before = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::next())],
    );
    assert_eq!(answer(&before), (None, vec![0]));
    assert_eq!(
        offsets(&submit(&owner, request(&owner, 1, false, true)).await),
        [0]
    );
    let after = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::next())],
    );
    let (parked, partitions) = answer(&after);
    assert!(parked.is_some() && partitions.is_empty());
}

#[compio::test]
async fn readiness_waiter_leaves_on_cancel_disconnect_or_deadline() {
    for exit in ["cancel", "disconnect", "deadline"] {
        let owner = owner(&["first"]).await;
        let replies = wait(
            &owner,
            vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
        );
        let (Some(id), _) = answer(&replies) else {
            panic!("nothing is ready, so the waiter parks");
        };
        match exit {
            "cancel" => owner.cancel_readiness_wait(id),
            "disconnect" => {
                drop(replies);
                owner.deferred_polls.maintenance();
                owner.service_deferred_polls().await;
            }
            _ => {
                owner.bus.now.set(DEADLINE);
                owner.service_deferred_polls().await;
                assert!(
                    matches!(
                        replies.try_recv(),
                        Err(crossfire::TryRecvError::Disconnected)
                    ),
                    "the origin answers an expired wait"
                );
            }
        }
        assert_released(&owner);
    }
}

#[compio::test]
async fn replacement_history_rejects_parked_readiness() {
    let owner = owner(&["old"]).await;
    let replies = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
    );
    assert!(answer(&replies).0.is_some());
    let (replacement, _) =
        partition_with_messages(&owner.bus, namespace(), &["new", "newer"]).await;
    owner.plane.partitions().insert(namespace(), replacement);
    owner.deferred_polls.maintenance();
    owner.service_deferred_polls().await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_released(&owner);
}

/// An owner whose partitions each hold one message, and a parked wait for
/// the next message of every partition.
async fn parked_wait_on(partitions: usize) -> (TestShard, Receiver<PartitionReadReply>) {
    let owner = owner_of(&vec![&["first"][..]; partitions]).await;
    let targets = (0..partitions)
        .map(|partition| readiness_target(&owner, partition, PollingStrategy::offset(1)))
        .collect();
    let replies = wait(&owner, targets);
    assert!(answer(&replies).0.is_some());
    (owner, replies)
}

/// The partition refuses reads but marks nothing, so only an evaluation of
/// its target finds it.
fn begin_state_transfer(owner: &TestShard, partition: usize) {
    owner
        .plane
        .partitions()
        .with_partition(&partition_namespace(partition), |partition| {
            partition.consensus().begin_state_transfer_await();
        })
        .unwrap();
}

#[compio::test]
async fn commit_evaluates_only_the_readiness_target_of_its_partition() {
    let (owner, replies) = parked_wait_on(MAINTENANCE_BUDGET + 1).await;
    begin_state_transfer(&owner, MAINTENANCE_BUDGET);
    commit(&owner, &["second"], 2).await;
    owner.service_deferred_polls().await;
    assert_eq!(answer(&replies), (None, vec![0]));
    assert_released(&owner);
}

#[compio::test]
async fn maintenance_evaluates_a_large_readiness_wait_one_budget_per_tick() {
    let (owner, replies) = parked_wait_on(MAINTENANCE_BUDGET + 1).await;
    let tick = async || {
        owner.deferred_polls.maintenance();
        owner.service_deferred_polls().await;
    };
    // The first pass takes two ticks. The second starts again at the first
    // target, so it reaches the last one on the fourth tick.
    tick().await;
    tick().await;
    begin_state_transfer(&owner, MAINTENANCE_BUDGET);
    tick().await;
    pending(&replies);
    tick().await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_released(&owner);
}

#[compio::test]
async fn stale_session_rejects_parked_readiness_on_a_commit() {
    let owner = owner(&["first"]).await;
    let mut table = ClientTable::new(1);
    let registration = PrepareHeader {
        client: CLIENT,
        operation: Operation::Register,
        op: 1,
        ..Default::default()
    };
    table
        .commit_register(
            CLIENT,
            0,
            [0x5a; 32],
            build_reply_message_with(&registration, 0, |_| {}),
        )
        .unwrap();
    let session = table.attach_session(CLIENT, 1, 0).unwrap();
    let (reply, replies) = channel(2);
    owner.admit_readiness_wait(
        ReadinessWait {
            context: DeferredPollContext {
                session: Some(session),
                ..readiness_caller()
            },
            targets: vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
            deadline: DEADLINE,
            min_count: ANY_MESSAGE,
        },
        &reply,
        None,
    );
    assert!(answer(&replies).0.is_some());
    drop(table);
    commit(&owner, &["second"], 2).await;
    owner.service_deferred_polls().await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(IggyError::StaleClient))
    ));
    assert_released(&owner);
}

#[compio::test]
async fn readiness_refuses_targets_of_more_than_one_topic() {
    let owner = owner(&["first"]).await;
    let mut other_topic = readiness_target(&owner, 0, PollingStrategy::offset(1));
    other_topic.namespace = IggyNamespace::new(0, 1, 0);
    let replies = wait(
        &owner,
        vec![
            readiness_target(&owner, 0, PollingStrategy::offset(1)),
            other_topic,
        ],
    );
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(IggyError::InvalidCommand))
    ));
    assert_released(&owner);
}

#[compio::test]
async fn readiness_quota_counts_parked_waiters_only() {
    let owner = owner(&["first"]).await;
    owner.set_deferred_poll_config(DeferredPollConfig {
        max_pending_per_session: 1,
        ..Default::default()
    });
    let parked = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
    );
    assert!(answer(&parked).0.is_some());
    for refused in [
        wait(
            &owner,
            vec![readiness_target(&owner, 0, PollingStrategy::offset(1))],
        ),
        submit(&owner, request(&owner, 2, false, false)).await,
    ] {
        assert!(matches!(
            refused.try_recv(),
            Ok(PartitionReadReply::Rejected(
                IggyError::DeferredPollCapacityExceeded
            ))
        ));
    }
    let ready = wait(
        &owner,
        vec![readiness_target(&owner, 0, PollingStrategy::offset(0))],
    );
    assert_eq!(answer(&ready), (None, vec![0]));
}

#[compio::test]
async fn fan_out_merges_admissions_and_cancels_what_stays_parked() {
    for (later, duration) in [
        (false, PARTITION_READ_TIMEOUT),
        // A zero wait still collects every admission answer.
        (false, Duration::ZERO),
        (true, PARTITION_READ_TIMEOUT),
    ] {
        let mut owner = owner_of(&[&["first"], &["first"]]).await;
        let (local, local_inbox, _) = shard_channel(0, 4, 1);
        let (remote, remote_inbox, _) = shard_channel(1, 4, 1);
        owner.attach_senders(vec![local, remote]);
        owner.shards_table.insert(
            partition_namespace(1),
            PartitionLocation::new(ShardId::new(1), 1),
        );
        // Partition 0 is ready now unless the remote shard answers later.
        let start = u64::from(later);
        let targets = vec![
            readiness_target(&owner, 0, PollingStrategy::offset(start)),
            readiness_target(&owner, 1, PollingStrategy::offset(0)),
        ];
        let owners = async {
            let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait { wait, reply, watch })) =
                local_inbox.recv().await
            else {
                panic!("shard 0 holds partition 0");
            };
            owner.admit_readiness_wait(*wait, &reply, watch);
            let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait { wait, reply, watch })) =
                remote_inbox.recv().await
            else {
                panic!("shard 1 holds partition 1");
            };
            assert_eq!(wait.targets.len(), 1);
            assert_eq!(
                watch.is_some(),
                !duration.is_zero(),
                "only a wait that can park starts a watch"
            );
            let ready = ReadyTarget {
                partition_id: 1,
                available: 1,
                known_bytes: None,
            };
            for answer in [
                Some((Some(7), Vec::new())),
                later.then(|| (None, vec![ready])),
            ]
            .into_iter()
            .flatten()
            {
                reply
                    .try_send(PartitionReadReply::Ready {
                        shard: 1,
                        parked: answer.0,
                        partitions: answer.1,
                    })
                    .unwrap();
            }
        };
        let (ready, ()) = futures::join!(
            owner.wait_for_messages(readiness_caller(), targets, duration, ANY_MESSAGE),
            owners
        );
        if later {
            assert_eq!(ids(&ready.unwrap()), [1]);
            assert!(
                local_inbox.try_recv().is_err(),
                "the local waiter ends in place, without a cancel"
            );
            assert!(
                remote_inbox.try_recv().is_err(),
                "the remote waiter answered"
            );
        } else {
            assert_eq!(ids(&ready.unwrap()), [0]);
            assert!(local_inbox.try_recv().is_err(), "nothing parked locally");
            assert!(matches!(
                remote_inbox.try_recv(),
                Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessCancel {
                    waiter: 7
                }))
            ));
        }
        assert_released(&owner);
    }
}

const WATCHER: u16 = 0;
const OWNER: u16 = 1;
/// Room for the frames of one test step.
const INBOX: usize = 4;

struct Inbox {
    main: Receiver<ShardFrame>,
    replies: Receiver<ShardFrame>,
}

/// Shard `WATCHER` serves the waits, and shard `OWNER` holds a partition for
/// each entry of `partitions`. Both read one clock.
async fn watched_pair(partitions: &[&[&str]]) -> (TestShard, TestShard, [Inbox; 2]) {
    let (watcher, mut owners, inboxes) = watched_by(1, partitions).await;
    let Ok(inboxes) = <[Inbox; 2]>::try_from(inboxes) else {
        panic!("one watcher and one owner");
    };
    (watcher, owners.remove(0), inboxes)
}

/// Shard `WATCHER` serves the waits, and each of the `owners` shards after
/// it holds a partition for each entry of `partitions`. Partition `p` is the
/// one of shard `OWNER + p % owners`. All read one clock, and the inbox of
/// a shard is at its id.
async fn watched_by(
    owners: u16,
    partitions: &[&[&str]],
) -> (TestShard, Vec<TestShard>, Vec<Inbox>) {
    let bus = PollTestBus::default();
    // Boxed: one shard held across the build of another outgrows the
    // `large_futures` cap.
    let mut shards = vec![Box::pin(shard_of(WATCHER, bus.clone(), partitions)).await];
    for owner in OWNER..OWNER + owners {
        shards.push(Box::pin(shard_of(owner, bus.clone(), partitions)).await);
    }
    for partition in 0..partitions.len() {
        let namespace = partition_namespace(partition);
        assert!(shards[0].plane.partitions().remove(&namespace).is_some());
        let owner = OWNER + u16::try_from(partition).unwrap() % owners;
        shards[0]
            .shards_table
            .insert(namespace, PartitionLocation::new(ShardId::new(owner), 1));
    }
    let (senders, inboxes): (Vec<_>, Vec<_>) = (WATCHER..OWNER + owners)
        .map(|shard| {
            let (sender, main, replies) = shard_channel(shard, INBOX, INBOX);
            (sender, Inbox { main, replies })
        })
        .unzip();
    for shard in &mut shards {
        shard.attach_senders(senders.clone());
    }
    let watcher = shards.remove(0);
    (watcher, shards, inboxes)
}

/// Runs the frames of `inbox` as the pump of `shard` does, then its service
/// loop and its flush.
async fn pump(shard: &TestShard, inbox: &Inbox) {
    while let Ok(frame) = inbox.replies.try_recv().or_else(|_| inbox.main.try_recv()) {
        let ShardFrame::Lifecycle(frame) = frame else {
            panic!("a readiness test sends lifecycle frames only");
        };
        match frame {
            LifecycleFrame::ReadinessWait { wait, reply, watch } => {
                shard.admit_readiness_wait(*wait, &reply, watch);
            }
            LifecycleFrame::ReadinessCancel { waiter } => shard.cancel_readiness_wait(waiter),
            LifecycleFrame::ReadinessUpdate { from, states } => {
                shard.apply_readiness_update(from, states);
            }
            LifecycleFrame::ReadinessUnwatch { from, watches } => {
                shard.unwatch_readiness(from, watches);
            }
            LifecycleFrame::PartitionRead {
                namespace,
                read,
                reply,
            } => shard.on_partition_read(namespace, read, reply).await,
            _ => panic!("a readiness test sends readiness frames and reads only"),
        }
    }
    shard.service_deferred_polls().await;
    shard.flush_readiness_pushes();
}

/// A first wait for `partition` goes to the owner. The owner answers it and
/// pushes the state, which the watcher then mirrors.
async fn mirror(watcher: &TestShard, owner: &TestShard, inboxes: &[Inbox], partition: usize) {
    let target = readiness_target(watcher, partition, PollingStrategy::offset(0));
    mirror_on(watcher, owner, inboxes, target).await;
}

/// [`mirror`] through the ready `target`, so that the pushes carry the
/// installed owner of its group too.
async fn mirror_on(
    watcher: &TestShard,
    owner: &TestShard,
    inboxes: &[Inbox],
    target: ReadinessTarget,
) {
    let namespace = target.namespace;
    let partition = target.partition_id;
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(
            readiness_caller(),
            vec![target],
            PARTITION_READ_TIMEOUT,
            ANY_MESSAGE
        ),
        async {
            let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait {
                wait,
                reply,
                watch: Some(watch),
            })) = inboxes[usize::from(owner.id)].main.try_recv()
            else {
                panic!("the first wait goes to the owner and starts a watch");
            };
            owner.admit_readiness_wait(*wait, &reply, Some(watch));
            owner.flush_readiness_pushes();
        }
    );
    assert_eq!(ids(&ready.unwrap()), [partition]);
    pump(watcher, &inboxes[usize::from(WATCHER)]).await;
    assert!(
        watcher.deferred_polls.mirrors.borrow().entries[&namespace]
            .state
            .is_some()
    );
}

/// Parks a wait for offset 1 of partition 0 on the mirror of the watcher,
/// makes `change` on the owner, and passes on what the owner pushes.
async fn wait_through(
    watcher: &TestShard,
    owner: &TestShard,
    inboxes: &[Inbox; 2],
    change: impl AsyncFnOnce(),
) -> Result<Vec<ReadyTarget>, IggyError> {
    let target = readiness_target(watcher, 0, PollingStrategy::offset(1));
    wait_through_on(watcher, owner, inboxes, target, change).await
}

/// [`wait_through`] for `target`.
async fn wait_through_on(
    watcher: &TestShard,
    owner: &TestShard,
    inboxes: &[Inbox; 2],
    target: ReadinessTarget,
    change: impl AsyncFnOnce(),
) -> Result<Vec<ReadyTarget>, IggyError> {
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(
            readiness_caller(),
            vec![target],
            PARTITION_READ_TIMEOUT,
            ANY_MESSAGE
        ),
        async {
            pump(watcher, &inboxes[0]).await;
            assert_eq!(
                watcher.deferred_polls.state.borrow().readiness.len(),
                1,
                "the wait parks on the watcher"
            );
            change().await;
            pump(owner, &inboxes[1]).await;
            pump(watcher, &inboxes[0]).await;
            assert!(
                watcher.deferred_polls.state.borrow().readiness.is_empty(),
                "the push ends the wait"
            );
        }
    );
    ready
}

#[compio::test]
async fn wait_on_a_mirror_sends_no_frame_to_the_owner() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let ready = wait_through(&watcher, &owner, &inboxes, async || {
        commit(&owner, &["second"], 2).await;
    })
    .await;
    assert_eq!(ids(&ready.unwrap()), [0]);
    assert!(
        inboxes[1].main.try_recv().is_err(),
        "the wait and its end stay on the watcher"
    );
    assert!(
        inboxes[0].main.try_recv().is_err(),
        "an answered waiter needs no cancel"
    );
    assert_eq!(owner.metrics.deferred_polls.readiness_pushes.get(), 2);
    assert_eq!(owner.metrics.deferred_polls.readiness_watches.get(), 1);
    assert_released(&watcher);
    assert_released(&owner);
}

#[compio::test]
async fn zero_wait_or_next_target_asks_the_owner_without_a_watch() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    for (strategy, duration) in [
        (PollingStrategy::offset(0), Duration::ZERO),
        (PollingStrategy::next(), PARTITION_READ_TIMEOUT),
    ] {
        let target = readiness_target(&watcher, 0, strategy);
        let (ready, ()) = futures::join!(
            watcher.wait_for_messages(readiness_caller(), vec![target], duration, ANY_MESSAGE),
            async {
                let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait {
                    wait,
                    reply,
                    watch: None,
                })) = inboxes[1].main.try_recv()
                else {
                    panic!("the owner evaluates the target without a watch");
                };
                owner.admit_readiness_wait(*wait, &reply, None);
            }
        );
        assert_eq!(ids(&ready.unwrap()), [0]);
    }
}

#[compio::test]
async fn mirror_that_refuses_a_target_sends_it_to_the_owner() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    begin_state_transfer(&owner, 0);
    // No notifier reports a state transfer, so the sweep pushes it.
    owner.sweep_readiness_watches();
    pump(&owner, &inboxes[1]).await;
    pump(&watcher, &inboxes[0]).await;
    let target = readiness_target(&watcher, 0, PollingStrategy::offset(1));
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(
            readiness_caller(),
            vec![target],
            PARTITION_READ_TIMEOUT,
            ANY_MESSAGE
        ),
        async {
            let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait {
                wait,
                reply,
                watch: Some(watch),
            })) = inboxes[1].main.try_recv()
            else {
                panic!("the owner answers the error, and the watch goes on");
            };
            owner.admit_readiness_wait(*wait, &reply, Some(watch));
        }
    );
    assert!(matches!(ready, Err(IggyError::TransientNotAccepted)));
}

#[compio::test]
async fn frames_from_before_a_renewed_watch_change_nothing() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let stale = watcher.deferred_polls.mirrors.borrow().entries[&namespace()].seq;
    let renewed = watcher.next_readiness_watch();
    watcher.mirror_partitions(OWNER, renewed.seq, &[namespace()], 0);
    owner.watch_partitions(
        renewed,
        &[readiness_target(&owner, 0, PollingStrategy::offset(0))],
    );
    watcher.apply_readiness_update(
        OWNER,
        vec![WatchedState {
            namespace: namespace(),
            seq: stale,
            state: None,
        }],
    );
    assert!(
        watcher.deferred_polls.mirrors.borrow().entries[&namespace()]
            .state
            .is_some(),
        "a stale push keeps the mirror"
    );
    assert!(inboxes[1].main.try_recv().is_err(), "and unwatches nothing");
    owner.unwatch_readiness(WATCHER, vec![(namespace(), stale)]);
    assert_eq!(
        owner.deferred_polls.state.borrow().watches[&namespace()].watchers,
        [renewed],
        "a stale unwatch keeps the renewed watch"
    );
}

#[compio::test]
async fn unused_mirror_ends_its_watch() {
    for commits in [true, false] {
        let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
        mirror(&watcher, &owner, &inboxes, 0).await;
        watcher.bus.now.set(MIRROR_IDLE_US);
        if commits {
            commit(&owner, &["second"], 2).await;
            pump(&owner, &inboxes[1]).await;
            pump(&watcher, &inboxes[0]).await;
        } else {
            // A partition without commits pushes nothing.
            watcher.sweep_readiness_watches();
        }
        assert!(watcher.deferred_polls.mirrors.borrow().entries.is_empty());
        pump(&owner, &inboxes[1]).await;
        assert!(owner.deferred_polls.state.borrow().watches.is_empty());
        assert_eq!(owner.metrics.deferred_polls.readiness_watches.get(), 0);
        set_committed(&owner, 0, true);
        assert!(
            owner.deferred_polls.signals.borrow().dirty.is_empty(),
            "the notifier goes with the last watch"
        );
    }
}

#[compio::test]
async fn finished_poll_keeps_the_notifier_of_a_watched_partition() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let replies = submit(&owner, request(&owner, 2, false, false)).await;
    commit(&owner, &["second"], 2).await;
    pump(&owner, &inboxes[1]).await;
    assert_eq!(offsets(&replies), [0, 1]);
    commit(&owner, &["third"], 3).await;
    pump(&owner, &inboxes[1]).await;
    pump(&watcher, &inboxes[0]).await;
    assert!(matches!(
        &watcher.deferred_polls.mirrors.borrow().entries[&namespace()].state,
        Some(state) if state.visibility.offset == Some(2)
    ));
}

#[compio::test]
async fn owner_close_rejects_a_wait_parked_on_its_mirror() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let ready = wait_through(&watcher, &owner, &inboxes, async || {
        owner.deferred_polls.close(OWNER, &owner.senders);
    })
    .await;
    assert!(matches!(ready, Err(IggyError::TransientNotAccepted)));
    assert_released(&watcher);
}

#[compio::test]
async fn replacement_history_in_a_push_rejects_a_parked_wait() {
    let (watcher, owner, inboxes) = watched_pair(&[&["old"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let ready = wait_through(&watcher, &owner, &inboxes, async || {
        let (replacement, _) =
            partition_with_messages(&owner.bus, namespace(), &["new", "newer"]).await;
        owner.plane.partitions().insert(namespace(), replacement);
        // The rebuilt partition has no notifier, so the sweep finds it.
        owner.sweep_readiness_watches();
    })
    .await;
    assert!(matches!(ready, Err(IggyError::TransientNotAccepted)));
    set_committed(&owner, 0, true);
    assert!(
        owner
            .deferred_polls
            .signals
            .borrow()
            .queued
            .contains(&namespace()),
        "the sweep installs the notifier again"
    );
    assert_released(&watcher);
}

#[compio::test]
async fn refused_push_goes_at_the_next_flush() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    let lane = owner.senders[usize::from(WATCHER)].reply_sender();
    while lane
        .try_send(ShardFrame::lifecycle(LifecycleFrame::ReadinessUpdate {
            from: OWNER,
            states: Vec::new(),
        }))
        .is_ok()
    {}
    commit(&owner, &["second"], 2).await;
    pump(&owner, &inboxes[1]).await;
    assert!(
        !owner.deferred_polls.state.borrow().pushes.is_empty(),
        "a full lane keeps the push"
    );
    while inboxes[0].replies.try_recv().is_ok() {}
    owner.flush_readiness_pushes();
    let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessUpdate { states, .. })) =
        inboxes[0].replies.try_recv()
    else {
        panic!("the next flush sends the push");
    };
    assert!(matches!(
        &states[..],
        [WatchedState { state: Some(state), .. }] if state.visibility.offset == Some(1)
    ));
    assert!(owner.deferred_polls.state.borrow().pushes.is_empty());
}

/// The messages that a fetch read asks for, more than any test commits.
const FETCH_COUNT: u32 = 16;

/// A target of the group of the fixture, whose owner the partitions install.
fn group_target(shard: &TestShard, partition: usize, strategy: PollingStrategy) -> ReadinessTarget {
    let namespace = partition_namespace(partition);
    ReadinessTarget {
        consumer: PollingConsumer::ConsumerGroup(0, partition),
        metadata: shard
            .plane
            .metadata()
            .mux_stm
            .streams()
            .poll_metadata(namespace, Some(0), CLIENT)
            .unwrap(),
        ..readiness_target(shard, partition, strategy)
    }
}

/// A read of a fetch from `start` of `partition`.
fn fetch_read(
    shard: &TestShard,
    partition: usize,
    start: u64,
    max_bytes: u32,
    auto_commit: bool,
) -> DeferredPollRequest {
    let target = readiness_target(shard, partition, PollingStrategy::offset(start));
    DeferredPollRequest::new(
        target.consumer,
        PollingArgs::new(target.strategy, FETCH_COUNT, auto_commit),
        target.metadata,
        readiness_caller(),
        DeferredPollOptions {
            max_bytes,
            ..Default::default()
        },
    )
}

/// Changes the mirror of partition 0 as a push would, and marks it.
fn change_mirror(watcher: &TestShard, change: impl FnOnce(&mut ServedState)) {
    change(
        watcher
            .deferred_polls
            .mirrors
            .borrow_mut()
            .entries
            .get_mut(&namespace())
            .and_then(|mirror| mirror.state.as_mut())
            .expect("the watcher mirrors partition 0"),
    );
    watcher
        .deferred_polls
        .signals
        .borrow_mut()
        .mark(namespace());
}

/// Lets the other futures of a `join!` run once.
async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|context| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        context.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

fn parked_waits(shard: &TestShard) -> usize {
    shard.deferred_polls.state.borrow().readiness.len()
}

#[compio::test]
async fn wait_fires_at_the_min_count_sum_of_targets_on_two_owners() {
    let (watcher, owners, inboxes) = watched_by(2, &[&["first"], &["first"]]).await;
    for (partition, owner) in owners.iter().enumerate() {
        mirror(&watcher, owner, &inboxes, partition).await;
    }
    let targets = (0..owners.len())
        .map(|partition| readiness_target(&watcher, partition, PollingStrategy::offset(1)))
        .collect();
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(readiness_caller(), targets, PARTITION_READ_TIMEOUT, 2),
        async {
            pump(&watcher, &inboxes[usize::from(WATCHER)]).await;
            for (partition, owner) in owners.iter().enumerate() {
                assert_eq!(parked_waits(&watcher), 1, "the sum is below min_count");
                commit_to(owner, partition, &["second"], 2).await;
                pump(owner, &inboxes[usize::from(owner.id)]).await;
                pump(&watcher, &inboxes[usize::from(WATCHER)]).await;
            }
        }
    );
    let ready = ready.unwrap();
    assert_eq!(ids(&ready), [0, 1]);
    assert!(ready.iter().all(|target| target.available == 1));
    assert_released(&watcher);
}

#[compio::test]
async fn expired_wait_returns_the_targets_below_min_count_or_nothing() {
    for commits in [true, false] {
        let (watcher, owner, inboxes) = watched_pair(&[&["first"], &["first"]]).await;
        mirror(&watcher, &owner, &inboxes, 0).await;
        mirror(&watcher, &owner, &inboxes, 1).await;
        let targets = (0..2)
            .map(|partition| readiness_target(&watcher, partition, PollingStrategy::offset(1)))
            .collect();
        let (ready, ()) = futures::join!(
            watcher.wait_for_messages(readiness_caller(), targets, PARTITION_READ_TIMEOUT, 3),
            async {
                pump(&watcher, &inboxes[0]).await;
                // The wait takes its admission answer before the clock moves.
                yield_now().await;
                if commits {
                    commit(&owner, &["second"], 2).await;
                    pump(&owner, &inboxes[1]).await;
                    pump(&watcher, &inboxes[0]).await;
                    assert_eq!(parked_waits(&watcher), 1, "one message is below min_count");
                }
                watcher
                    .bus
                    .now
                    .set(u64::try_from(PARTITION_READ_TIMEOUT.as_micros()).unwrap());
                pump(&watcher, &inboxes[0]).await;
            }
        );
        let ready = ready.unwrap();
        if commits {
            assert_eq!(ids(&ready), [0]);
            assert_eq!(ready[0].available, 1);
        } else {
            assert_eq!(ready, []);
        }
        assert_eq!(watcher.metrics.deferred_polls.expired.get(), 1);
        assert_released(&watcher);
    }
}

#[compio::test]
async fn known_bytes_are_what_a_read_from_the_start_returns() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"], &["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    mirror(&watcher, &owner, &inboxes, 1).await;
    // Partition 0 waits at its end, and partition 1 behind its last offset.
    let targets = vec![
        readiness_target(&watcher, 0, PollingStrategy::offset(1)),
        readiness_target(&watcher, 1, PollingStrategy::offset(0)),
    ];
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(readiness_caller(), targets, PARTITION_READ_TIMEOUT, 4),
        async {
            pump(&watcher, &inboxes[0]).await;
            commit(&owner, &["second", "third"], 2).await;
            commit_to(&owner, 1, &["second"], 2).await;
            pump(&owner, &inboxes[1]).await;
            pump(&watcher, &inboxes[0]).await;
        }
    );
    let ready = ready.unwrap();
    let [at_end, behind] = &ready[..] else {
        panic!("both targets hold messages");
    };
    assert_eq!((at_end.partition_id, at_end.available), (0, 2));
    assert_eq!(
        (behind.partition_id, behind.available, behind.known_bytes),
        (1, 2, None)
    );
    let known = at_end
        .known_bytes
        .expect("the start was the next offset to commit");
    let room = u32::try_from(known).unwrap() + KNOWN_BYTES_SLACK;
    // The slack is exact: one byte less cuts the batch after its first message.
    for (max_bytes, next) in [(room, 3), (room - 1, 2)] {
        let read = fetch_read(&watcher, 0, 1, max_bytes, false);
        let (reply, ()) = futures::join!(
            watcher.deferred_read(namespace(), read, REQUEST_DEADLINE),
            pump(&owner, &inboxes[1])
        );
        let Some(PartitionReadReply::Poll {
            fragments,
            next_offset,
            ..
        }) = reply
        else {
            panic!("the read returns messages");
        };
        let bytes = fragments.iter().map(Fragment::len).sum::<usize>() as u64;
        assert_eq!(
            bytes == known,
            next == 3,
            "{bytes} bytes against {known} known"
        );
        assert_eq!(next_offset, Some(next));
    }
}

#[compio::test]
async fn one_commit_gives_one_push_one_fire_and_no_cancel() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"], &["first"]]).await;
    mirror(&watcher, &owner, &inboxes, 0).await;
    mirror(&watcher, &owner, &inboxes, 1).await;
    let pushes = owner.metrics.deferred_polls.readiness_pushes.get();
    let targets = (0..2)
        .map(|partition| readiness_target(&watcher, partition, PollingStrategy::offset(1)))
        .collect();
    let (ready, ()) = futures::join!(
        watcher.wait_for_messages(readiness_caller(), targets, PARTITION_READ_TIMEOUT, 1),
        async {
            pump(&watcher, &inboxes[0]).await;
            commit(&owner, &["second"], 2).await;
            pump(&owner, &inboxes[1]).await;
            pump(&watcher, &inboxes[0]).await;
        }
    );
    assert_eq!(ids(&ready.unwrap()), [0]);
    assert_eq!(
        owner.metrics.deferred_polls.readiness_pushes.get(),
        pushes + 1
    );
    assert_eq!(watcher.metrics.deferred_polls.filled.get(), 1);
    assert!(
        watcher
            .metrics
            .scrape()
            .lines()
            .any(|line| line == "readiness_push_to_fire_seconds_count 1")
    );
    assert!(
        inboxes[1].main.try_recv().is_err(),
        "nothing goes to the owner"
    );
    assert!(inboxes[0].main.try_recv().is_err(), "no cancel");
    assert_released(&watcher);
}

#[compio::test]
async fn only_a_mirror_that_lags_an_owner_install_is_not_ready() {
    for cause in ["install", "unstable", "view", "incarnation"] {
        let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
        // A first wait of the group starts pushes that carry its owner.
        mirror_on(
            &watcher,
            &owner,
            &inboxes,
            group_target(&watcher, 0, PollingStrategy::offset(0)),
        )
        .await;
        let target = group_target(&watcher, 0, PollingStrategy::offset(1));
        let ready = wait_through_on(&watcher, &owner, &inboxes, target, async || match cause {
            "install" => {
                change_mirror(&watcher, |state| {
                    state.visibility.offset = Some(1);
                    state.owners = vec![(0, None)];
                });
                pump(&watcher, &inboxes[0]).await;
                assert_eq!(parked_waits(&watcher), 1, "a lagging install waits");
                commit(&owner, &["second"], 2).await;
            }
            "unstable" => {
                begin_state_transfer(&owner, 0);
                owner.sweep_readiness_watches();
            }
            "view" => change_mirror(&watcher, |state| state.status.view_started = false),
            _ => change_mirror(&watcher, |state| state.status.incarnation = None),
        })
        .await;
        if cause == "install" {
            assert_eq!(ids(&ready.unwrap()), [0]);
        } else {
            assert!(
                matches!(ready, Err(IggyError::TransientNotAccepted)),
                "{cause} fails the wait"
            );
        }
        assert_released(&watcher);
    }
}

#[compio::test]
async fn deferred_read_commits_only_before_its_request_deadline() {
    for late in [false, true] {
        let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
        let read = fetch_read(&watcher, 0, 0, DEFAULT_POLL_MAX_BYTES, true);
        let consumer = read.consumer;
        let (reply, ()) = futures::join!(
            watcher.deferred_read(namespace(), read, REQUEST_DEADLINE),
            async {
                if late {
                    owner.bus.now.set(REQUEST_DEADLINE);
                }
                pump(&owner, &inboxes[1]).await;
            }
        );
        if late {
            assert!(matches!(
                reply,
                Some(PartitionReadReply::Rejected(
                    IggyError::TransientNotCommitted
                ))
            ));
            assert_eq!(stored_offset(&owner, consumer), None);
        } else {
            assert!(matches!(
                reply,
                Some(PartitionReadReply::Poll {
                    next_offset: Some(1),
                    ..
                })
            ));
            assert_eq!(stored_offset(&owner, consumer), Some(0));
        }
    }
    let (watcher, _owner, inboxes) = watched_pair(&[&["first"]]).await;
    watcher.bus.now.set(REQUEST_DEADLINE);
    let read = fetch_read(&watcher, 0, 0, DEFAULT_POLL_MAX_BYTES, true);
    assert!(
        watcher
            .deferred_read(namespace(), read, REQUEST_DEADLINE)
            .await
            .is_none()
    );
    assert!(
        inboxes[1].main.try_recv().is_err(),
        "a read without time stays"
    );
}

#[compio::test]
async fn deferred_read_at_the_end_names_its_start_as_next_offset() {
    let (watcher, owner, inboxes) = watched_pair(&[&["first"]]).await;
    let read = fetch_read(&watcher, 0, 1, DEFAULT_POLL_MAX_BYTES, true);
    let (reply, ()) = futures::join!(
        watcher.deferred_read(namespace(), read, REQUEST_DEADLINE),
        pump(&owner, &inboxes[1])
    );
    let Some(PartitionReadReply::Poll {
        fragments,
        next_offset,
        ..
    }) = reply
    else {
        panic!("a read without messages still returns a poll");
    };
    assert!(fragments.is_empty());
    assert_eq!(next_offset, Some(1));
}

#[compio::test]
async fn empty_read_before_the_retained_offsets_moves_to_the_first_one() {
    let owner = owner(&["first", "second"]).await;
    evict_messages_for_disk_dispatch(&owner, namespace());
    // As after retention deleted offsets 0 and 1: the log starts at offset 2.
    let partitions = owner.plane.partitions();
    let mut partition = partitions.remove(&namespace()).unwrap();
    partition.log.active_segment_mut().start_offset = 2;
    partitions.insert(namespace(), partition);
    let mut read = request(&owner, 1, false, false);
    read.args = PollingArgs::new(PollingStrategy::offset(0), 1, false);
    let consumer = read.consumer;
    let replies = submit(&owner, read).await;
    assert_eq!(owner.bus.spawned_tasks.borrow().len(), 1, "a disk read");
    owner.bus.spawned_tasks.borrow_mut().clear();
    owner.bus.now.set(DEADLINE);
    // What a disk read of the empty active segment finds.
    let nothing = partitions
        .build_poll_snapshot(
            &namespace(),
            consumer,
            &PollingArgs::new(PollingStrategy::offset(2), 1, false),
        )
        .unwrap()
        .execute_resident();
    owner.on_deferred_poll_completed(1, nothing).await;
    let Ok(PartitionReadReply::Poll {
        fragments,
        next_offset,
        ..
    }) = replies.try_recv()
    else {
        panic!("the expired poll returns what it read");
    };
    assert!(fragments.is_empty());
    assert_eq!(next_offset, Some(2));
}
