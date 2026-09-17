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

use super::*;
use consensus::PartitionsHandle;
use iggy_common::IggyTimestamp;
use metadata::IggyMetadata;
use metadata::stm::consumer_group::{ConsumerGroup, ConsumerGroupMember};
use metadata::stm::stream::{Partition as MetadataPartition, Stream, StreamsInner, Topic};
use metadata::stm::user::Users;
use partitions::IggyPartitions;
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{PartitionLocation, ShardId};

use crate::poll::test_support::{PollTestMetadata, partition_with_messages};
use crate::poll::timeout_tests::PollTestBus;
use crate::shards_table::PapayaShardsTable;
use crate::{PartitionConsensusConfig, Receiver, ReplicaTopology, ShardIdentity, channel};

const DEADLINE: u64 = 100;
const CLIENT: u128 = 41;
fn namespace() -> IggyNamespace {
    IggyNamespace::new(0, 0, 0)
}
type TestShard = IggyShard<PollTestBus, (), (), PollTestMetadata, PapayaShardsTable>;

async fn owner(payloads: &[&str]) -> TestShard {
    let bus = PollTestBus::default();
    let (partition, config) = partition_with_messages(&bus, namespace(), payloads).await;
    let partitions = IggyPartitions::new(ShardId::new(0), config);
    partitions.insert(namespace(), partition);
    let mut inner = StreamsInner::default();
    let mut stream = Stream::default();
    let mut topic = Topic::default();
    topic.partitions.push(MetadataPartition::new(
        0,
        namespace().inner(),
        IggyTimestamp::default(),
        1,
        0,
    ));
    let mut group = ConsumerGroup::new(0, Arc::from("group"));
    group.members.insert(ConsumerGroupMember::new(0, CLIENT));
    group.rebalance_members(&[0]);
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
    let routes = PapayaShardsTable::new();
    routes.insert(namespace(), PartitionLocation::new(ShardId::new(0), 1));
    TestShard::without_inbox(
        ShardIdentity::new(0, "deferred-test".to_string()),
        bus.clone(),
        metadata,
        partitions,
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
            strategy: PollingStrategy::next(),
            count,
            auto_commit,
        },
        DeferredPollContext {
            session: None,
            metadata,
            user_id: 0,
            quota: PollQuotaIdentity::Session(CLIENT),
            primary: false,
        },
        iggy_common::DeferredPollOptions {
            max_wait: DEADLINE.into(),
            min_count: count,
            request_timeout: (DEADLINE * 2).into(),
            ..Default::default()
        },
    );
    request.deadline = DEADLINE;
    request.request_deadline = DEADLINE * 2;
    request
}

async fn submit(owner: &TestShard, request: DeferredPollRequest) -> Receiver<PartitionReadReply> {
    let (reply, replies) = channel(1);
    owner.admit_deferred_poll(namespace(), request, reply).await;
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
                    strategy: PollingStrategy::first(),
                    count: 1,
                    auto_commit: true,
                },
            )
            .unwrap()
            .execute_resident();
        // A read dispatched before expiry still owns its byte reservation after cancellation.
        let reservation = owner.deferred_polls.reserve_read().unwrap();
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
            IggyError::TransientNotAccepted
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
    for completed_at in [DEADLINE + 1, DEADLINE * 2] {
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
                    strategy: PollingStrategy::first(),
                    count: 1,
                    auto_commit: true,
                },
            )
            .unwrap()
            .execute_resident();
        let reservation = owner.deferred_polls.reserve_read().unwrap();
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
        if completed_at < DEADLINE * 2 {
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
