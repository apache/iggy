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

use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake};

use consensus::PartitionsHandle;
use iggy_common::{IggyError, PollingStrategy};
use journal::prepare_journal::PrepareJournal;
use message_bus::IggyMessageBus;
use metadata::IggyMetadata;
use metadata::impls::metadata::IggySnapshot;
use partitions::{IggyPartitions, PartitionsConfig, PollingArgs, PollingConsumer};
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::test_support::{PollTestMetadata, partition_with_messages};
use crate::metrics::ShardMetrics;
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    IggyShard, LifecycleFrame, PartitionConsensusConfig, PartitionRead, PartitionReadReply,
    Receiver, ReplicaTopology, ShardFrame, ShardIdentity, TaggedSender, channel, shard_channel,
};

/// Replacement can reuse every message offset from the old history. The pump
/// must reject the old completion by history, then accept a fresh completion
/// through the same completion lane without inheriting stale group progress.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_pending_group_read_when_partition_is_replaced_should_reject_stale_completion_through_owner_pump()
 {
    let namespace = IggyNamespace::new(1, 1, 0);
    let group_id = 7;
    let consumer = PollingConsumer::ConsumerGroup(
        usize::try_from(group_id).expect("group id fits the consumer key"),
        0,
    );
    let bus = Rc::new(IggyMessageBus::new(0));
    let old_payloads = ["old zero", "old one", "old two"];
    let (old_partition, config) = partition_with_messages(&bus, namespace, &old_payloads).await;
    let (owner, _owner_sender) = owner_with_inbox(&bus, config, namespace);
    let partitions = owner.plane.partitions();
    partitions.insert(namespace, old_partition);
    let poll_args = PollingArgs {
        strategy: PollingStrategy::offset(0),
        count: 3,
        auto_commit: true,
    };

    // Read the committed old batch but hold its result before owner acceptance.
    // Resident bytes make the release ordering explicit without disk timing.
    let (stale_reply_sender, stale_replies) = channel(1);
    let old_completion = owner
        .poll_completions
        .try_reserve(namespace, stale_reply_sender, owner.metrics().clone())
        .expect("reserve the old read before executing it");
    let old_plan = partitions
        .build_poll_snapshot(&namespace, consumer, &poll_args)
        .expect("old partition has a read snapshot");
    assert!(!old_plan.needs_off_pump_io());
    let delayed_result = old_plan.execute_resident();

    // Reuse offsets 0 through 2 in a different history. Checking only that the
    // old offset fits the current partition would wrongly accept this result.
    // The pump has not started, so no partition borrow can span replacement.
    let fresh_payloads = ["fresh zero", "fresh one", "fresh two"];
    let (replacement, _) = partition_with_messages(&bus, namespace, &fresh_payloads).await;
    drop(
        partitions
            .remove(&namespace)
            .expect("remove the old history"),
    );
    partitions.insert(namespace, replacement);
    let (last_polled, committed) = partitions.group_offset_state(&namespace, group_id).unwrap();
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);

    // Keep stop open and drive the real pump only after each completion is
    // queued. Dropping the pump at the end avoids an unrelated shutdown flush.
    let (_stop_sender, stop_receiver) = channel(1);
    let pump = owner.run_message_pump(stop_receiver, Arc::new(AtomicBool::new(false)));
    futures::pin_mut!(pump);
    old_completion.complete(delayed_result);
    assert_eq!(
        owner.poll_completion_inbox_len(),
        1,
        "stale result is queued for owner validation"
    );
    assert!(
        matches!(
            stale_replies.try_recv(),
            Err(crossfire::TryRecvError::Empty)
        ),
        "the sender must leave rejection to the owner"
    );
    assert!(futures::poll!(pump.as_mut()).is_pending());
    let stale_reply = stale_replies
        .try_recv()
        .expect("owner processed the stale completion");

    // Check both offsets before a fresh read can hide a stale update. These
    // assertions also expose nonempty stale admission if the history check fails.
    let (last_polled, committed) = partitions.group_offset_state(&namespace, group_id).unwrap();
    assert_eq!(
        last_polled, None,
        "stale completion must not restore the group's last polled offset"
    );
    assert_eq!(
        committed, None,
        "stale completion must not admit an automatic commit"
    );
    assert!(
        matches!(
            stale_reply,
            PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
        ),
        "old history must be rejected, got {stale_reply:?}"
    );

    // A fresh result takes the same completion lane and pump. Distinct
    // payloads prove the reply belongs to the replacement at the reused offsets.
    let (fresh_reply_sender, fresh_replies) = channel(1);
    let fresh_completion = owner
        .poll_completions
        .try_reserve(namespace, fresh_reply_sender, owner.metrics().clone())
        .expect("reserve the fresh read before executing it");
    let fresh_plan = partitions
        .build_poll_snapshot(&namespace, consumer, &poll_args)
        .expect("replacement has a read snapshot");
    assert!(!fresh_plan.needs_off_pump_io());
    let fresh_result = fresh_plan.execute_resident();
    fresh_completion.complete(fresh_result);
    assert_eq!(
        owner.poll_completion_inbox_len(),
        1,
        "fresh result uses the same completion lane"
    );
    assert!(
        matches!(
            fresh_replies.try_recv(),
            Err(crossfire::TryRecvError::Empty)
        ),
        "the sender must leave success to the owner"
    );
    assert!(futures::poll!(pump.as_mut()).is_pending());
    let PartitionReadReply::Poll {
        fragments,
        current_offset,
    } = fresh_replies
        .try_recv()
        .expect("owner processed the fresh completion")
    else {
        panic!("fresh history should produce a successful poll reply");
    };
    assert_eq!(current_offset, 2);
    let bytes: Vec<u8> = fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&bytes).expect("decode the fresh batch");
    let offsets: Vec<u64> = batch
        .iter()
        .map(|message| batch.header.base_offset + u64::from(message.header.offset_delta))
        .collect();
    let payloads: Vec<&[u8]> = batch.iter().map(|message| message.payload).collect();
    assert_eq!(offsets, vec![0, 1, 2]);
    assert_eq!(payloads, fresh_payloads.map(str::as_bytes));
    let (last_polled, committed) = partitions.group_offset_state(&namespace, group_id).unwrap();
    assert_eq!(last_polled, Some(2));
    assert_eq!(
        committed,
        Some(2),
        "fresh acceptance advances the stored offset locally"
    );
}

/// A full ordinary inbox must not refuse a completed read. Interleaving offset
/// queries with two reads also proves neither lane drains its whole backlog
/// before giving the other lane a turn.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_full_owner_inbox_when_reserved_reads_complete_should_interleave_both_lanes() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let group_id = 7;
    let consumer = PollingConsumer::ConsumerGroup(
        usize::try_from(group_id).expect("group id fits the consumer key"),
        0,
    );
    let bus = Rc::new(IggyMessageBus::new(0));
    let payloads = ["first completion", "second completion"];
    let (partition, config) = partition_with_messages(&bus, namespace, &payloads).await;
    let (owner, owner_sender) = owner_with_inbox(&bus, config, namespace);
    let partitions = owner.plane.partitions();
    partitions.insert(namespace, partition);
    let mut delayed_reads = Vec::new();
    let mut poll_replies = Vec::new();

    // Reserve both reads before executing them. Each nonempty result advances
    // the same group's progress by one offset, making acceptance order visible.
    for offset in [0, 1] {
        let (reply_sender, replies) = channel(1);
        let completion = owner
            .poll_completions
            .try_reserve(namespace, reply_sender, owner.metrics().clone())
            .expect("reserve completion capacity before reading");
        let plan = partitions
            .build_poll_snapshot(
                &namespace,
                consumer,
                &PollingArgs {
                    strategy: PollingStrategy::offset(offset),
                    count: 1,
                    auto_commit: false,
                },
            )
            .expect("fixture partition has a read snapshot");
        assert!(!plan.needs_off_pump_io());
        delayed_reads.push((completion, plan.execute_resident()));
        poll_replies.push(replies);
    }

    // The fixture's ordinary inbox has two slots. These queries fill it before
    // either completed read is returned, reproducing the former refusal path.
    let mut progress_replies = Vec::new();
    for _ in 0..2 {
        let (reply, replies) = channel(1);
        assert!(
            owner_sender
                .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionRead {
                    namespace,
                    read: PartitionRead::GroupOffsetState { group_id },
                    reply,
                }))
                .is_ok()
        );
        progress_replies.push(replies);
    }
    assert!(matches!(
        owner_sender.try_send(ShardFrame::lifecycle(LifecycleFrame::ReconcileApply)),
        Err(crossfire::TrySendError::Full(_))
    ));
    for (completion, result) in delayed_reads {
        completion.complete(result);
    }
    assert_eq!(owner.inbox_len(), 2, "ordinary work remains queued");
    assert_eq!(owner.poll_completion_inbox_len(), 2);
    assert_eq!(owner.metrics().frame_drops_value(), 0);
    for replies in &poll_replies {
        assert!(matches!(
            replies.try_recv(),
            Err(crossfire::TryRecvError::Empty)
        ));
    }

    let (_stop_sender, stop_receiver) = channel(1);
    let pump = owner.run_message_pump(stop_receiver, Arc::new(AtomicBool::new(false)));
    futures::pin_mut!(pump);
    assert!(futures::poll!(pump.as_mut()).is_pending());

    // The first query runs before either read is accepted. The second runs
    // after exactly one acceptance: each lane yields while the other has work.
    for (replies, expected_last_polled) in progress_replies.iter().zip([None, Some(0)]) {
        let PartitionReadReply::GroupOffsetState {
            last_polled,
            committed,
        } = replies.try_recv().expect("ordinary query was processed")
        else {
            panic!("expected the group's progress at this pump turn");
        };
        assert_eq!(last_polled, expected_last_polled);
        assert_eq!(committed, None, "automatic commits were disabled");
    }

    // Both accepted results contain their requested message, so an empty read
    // or an early rejection cannot make the progress observations pass.
    for (expected_offset, replies) in poll_replies.iter().enumerate() {
        let PartitionReadReply::Poll { fragments, .. } = replies
            .try_recv()
            .expect("completed read reached its caller")
        else {
            panic!("a full ordinary inbox must not reject a reserved completion");
        };
        let bytes: Vec<u8> = fragments
            .iter()
            .flat_map(|fragment| fragment.as_slice().iter().copied())
            .collect();
        let batch = decode_batch_slice(&bytes).expect("decode the completed read");
        let offsets: Vec<u64> = batch
            .iter()
            .map(|message| batch.header.base_offset + u64::from(message.header.offset_delta))
            .collect();
        let returned_payloads: Vec<&[u8]> = batch.iter().map(|message| message.payload).collect();
        assert_eq!(offsets, vec![expected_offset as u64]);
        assert_eq!(
            returned_payloads,
            vec![payloads[expected_offset].as_bytes()]
        );
    }
    let (last_polled, committed) = partitions.group_offset_state(&namespace, group_id).unwrap();
    assert_eq!(last_polled, Some(1));
    assert_eq!(committed, None);
    assert_eq!(owner.inbox_len(), 0);
    assert_eq!(owner.poll_completion_inbox_len(), 0);
}

#[compio::test]
async fn given_owner_processed_completion_when_shutdown_arrives_should_wake_and_drain_queued_completion()
 {
    let namespace = IggyNamespace::new(1, 1, 0);
    let bus = Rc::new(IggyMessageBus::new(0));
    let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
    let (owner, _owner_sender) = owner_with_inbox(&bus, config, namespace);
    owner.plane.partitions().insert(namespace, partition);
    let (stop_sender, stop_receiver) = channel(1);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let wake_observer = Arc::new(PumpWakeObserver::default());
    let waker = Arc::clone(&wake_observer).into();
    let mut context = Context::from_waker(&waker);
    let pump = owner.run_message_pump(stop_receiver, Arc::clone(&shutdown_flag));
    futures::pin_mut!(pump);

    // Processing a completion advances the pump into another wait. Shutdown
    // must still wake it after a completion branch has already won.
    let first_reply = queue_resident_poll(&owner, namespace);
    assert!(pump.as_mut().poll(&mut context).is_pending());
    assert_single_message_reply(&first_reply);
    wake_observer.notified.store(false, Ordering::Relaxed);
    stop_sender.try_send(()).expect("signal shutdown");
    assert!(wake_observer.notified.load(Ordering::Relaxed));

    // Both shutdown and a completion are ready before the pump resumes.
    // Graceful shutdown must drain the completion before the final flush.
    let queued_reply = queue_resident_poll(&owner, namespace);
    assert!(matches!(
        pump.as_mut().poll(&mut context),
        Poll::Ready(None)
    ));
    assert_single_message_reply(&queued_reply);
    assert_eq!(owner.inbox_len(), 0);
    assert_eq!(owner.poll_completion_inbox_len(), 0);
    assert!(!shutdown_flag.load(Ordering::Relaxed));
}

#[compio::test]
async fn given_owner_processed_completion_when_shutdown_sender_drops_should_wake_and_stop() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let bus = Rc::new(IggyMessageBus::new(0));
    let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
    let (owner, _owner_sender) = owner_with_inbox(&bus, config, namespace);
    owner.plane.partitions().insert(namespace, partition);
    let (stop_sender, stop_receiver) = channel(1);
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let wake_observer = Arc::new(PumpWakeObserver::default());
    let waker = Arc::clone(&wake_observer).into();
    let mut context = Context::from_waker(&waker);
    let pump = owner.run_message_pump(stop_receiver, Arc::clone(&shutdown_flag));
    futures::pin_mut!(pump);

    let reply = queue_resident_poll(&owner, namespace);
    assert!(pump.as_mut().poll(&mut context).is_pending());
    assert_single_message_reply(&reply);

    // Losing the final shutdown sender must wake an otherwise idle owner;
    // manually polling to completion alone would miss a lost notification.
    wake_observer.notified.store(false, Ordering::Relaxed);
    drop(stop_sender);
    assert!(wake_observer.notified.load(Ordering::Relaxed));
    assert!(matches!(
        pump.as_mut().poll(&mut context),
        Poll::Ready(None)
    ));
    assert_eq!(owner.inbox_len(), 0);
    assert_eq!(owner.poll_completion_inbox_len(), 0);
    assert!(!shutdown_flag.load(Ordering::Relaxed));
}

type CompletionTestShard = IggyShard<
    Rc<IggyMessageBus>,
    PrepareJournal,
    IggySnapshot,
    PollTestMetadata,
    PapayaShardsTable,
>;

fn owner_with_inbox(
    bus: &Rc<IggyMessageBus>,
    config: PartitionsConfig,
    namespace: IggyNamespace,
) -> (CompletionTestShard, TaggedSender) {
    let shard_id = ShardId::new(0);
    let partitions = IggyPartitions::new(shard_id, config);
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None);
    let (sender, inbox, replies) = shard_channel(0, 2, 1);
    let routes = PapayaShardsTable::new();
    routes.insert(namespace, PartitionLocation::new(shard_id, 0));
    let owner = CompletionTestShard::new(
        ShardIdentity::new(0, "poll-completion-test".to_string()),
        bus.clone(),
        Rc::new(|_, _| {}),
        Rc::new(|_, _| {}),
        Rc::new(|_| {}),
        Rc::new(|_| {}),
        metadata,
        partitions,
        vec![sender.clone()],
        inbox,
        replies,
        2,
        routes,
        PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus.clone()),
        None,
        ShardMetrics::for_shard(),
    )
    .expect("valid owner inbox wiring");
    (owner, sender)
}

fn queue_resident_poll(
    owner: &CompletionTestShard,
    namespace: IggyNamespace,
) -> Receiver<PartitionReadReply> {
    let plan = owner
        .plane
        .partitions()
        .build_poll_snapshot(
            &namespace,
            PollingConsumer::Consumer(1, 0),
            &PollingArgs {
                strategy: PollingStrategy::offset(0),
                count: 1,
                auto_commit: false,
            },
        )
        .expect("fixture has a read snapshot");
    assert!(!plan.needs_off_pump_io());
    let (reply_sender, replies) = channel(1);
    owner
        .poll_completions
        .try_reserve(namespace, reply_sender, owner.metrics().clone())
        .expect("reserve capacity before completing the read")
        .complete(plan.execute_resident());
    assert_eq!(
        owner.poll_completion_inbox_len(),
        1,
        "completion awaits owner acceptance"
    );
    assert!(matches!(
        replies.try_recv(),
        Err(crossfire::TryRecvError::Empty)
    ));
    replies
}

fn assert_single_message_reply(replies: &Receiver<PartitionReadReply>) {
    let PartitionReadReply::Poll {
        fragments,
        current_offset,
    } = replies.try_recv().expect("owner replied to the completion")
    else {
        panic!("the fixture's read should succeed");
    };
    assert_eq!(current_offset, 0);
    let bytes: Vec<u8> = fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&bytes).expect("decode the reply");
    let payloads: Vec<&[u8]> = batch.iter().map(|message| message.payload).collect();
    assert_eq!(payloads, vec![b"message".as_slice()]);
    assert!(matches!(
        replies.try_recv(),
        Err(crossfire::TryRecvError::Disconnected)
    ));
}

#[derive(Default)]
struct PumpWakeObserver {
    notified: AtomicBool,
}

impl Wake for PumpWakeObserver {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Relaxed);
    }
}
