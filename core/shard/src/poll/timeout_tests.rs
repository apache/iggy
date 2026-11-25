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

use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use consensus::{LocalPipeline, MetadataHandle, PartitionsHandle, VsrConsensus};
use futures::channel::oneshot;
use iggy_binary_protocol::requests::messages::FETCH_REPLY_MARGIN_US;
use iggy_common::{Durability, IggyError, PartitionStats, PollingStrategy};
use message_bus::{
    BusMessage, ConnectionLostFn, JoinHandle, MessageBus, ReplicaForwardFn, SendError,
};
use metadata::impls::metadata::StreamsFrontend;
use metadata::{AppliedFrontier, IggyMetadata};
use partitions::{IggyPartition, IggyPartitions, PollFragments, PollingArgs, PollingConsumer};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::completion::PollCompletionLane;
use super::test_support::{
    PollTestMetadata, partition_with_messages, partitions_config, primary_with_pending_view_write,
};
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    ANY_MESSAGE, ConsumerOffsetRead, DeferredPollContext, DeferredPollRequest, IggyShard,
    LifecycleFrame, MAX_HELD_READS, MAX_HELD_READS_PER_NAMESPACE, OffsetReadDeadlines,
    PARTITION_BUILD_HOLD, PARTITION_READ_TIMEOUT, PartitionConsensusConfig, PartitionRead,
    PartitionReadReply, PollQuotaIdentity, ReadHold, ReadinessTarget, ReadyTarget, Receiver,
    ReconcileOp, ReplicaTopology, ShardFrame, ShardIdentity, channel, shard_channel,
    shard_mesh_channels,
};

/// Expire the requester before releasing a nonempty completion to the owner.
/// The closed reply channel must prevent admission so a later Next can return
/// the unseen messages. Timeout racing with admission is a separate case.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_auto_commit_poll_when_completion_arrives_after_timeout_should_return_unseen_messages_on_next()
 {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consumer = PollingConsumer::Consumer(7, 0);
    let (expire_timeout, timeout_elapsed) = oneshot::channel();
    let bus = PollTestBus {
        next_timeout: Rc::new(RefCell::new(Some(timeout_elapsed))),
        ..Default::default()
    };
    let mut owner = owner_with_messages(&bus, namespace).await;
    let (owner_sender, owner_inbox, _owner_reply_lane) = shard_channel(0, 2, 1);
    owner.attach_senders(vec![owner_sender.clone()]);
    let partitions = owner.plane.partitions();
    let (stored_offset, partition_offset) = partitions
        .consumer_offset_read(&namespace, consumer)
        .expect("fixture partition exists");
    assert_eq!(stored_offset, None);
    assert_eq!(
        partition_offset, 3,
        "four messages are available at offsets 0 through 3"
    );

    // Start a real routed request, but let the test control when its completed
    // read reaches the owner. The fourth message will identify the next batch.
    let first_poll = owner.partition_read(
        namespace,
        PartitionRead::Poll {
            metadata: None,
            consumer,
            args: PollingArgs::new(PollingStrategy::next(), 3, true),
        },
    );
    futures::pin_mut!(first_poll);
    assert!(futures::poll!(first_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace: requested_namespace,
        read:
            PartitionRead::Poll {
                metadata: None,
                consumer: requested_consumer,
                args,
            },
        reply,
        ..
    }) = owner_inbox
        .try_recv()
        .expect("poll reached the owner inbox")
    else {
        panic!("expected the routed poll request");
    };
    let late_reply = reply.clone();
    let completion = owner
        .poll_completions
        .try_reserve(requested_namespace, reply, None)
        .expect("reserve the read before the requester times out");
    let read_plan = partitions
        .build_poll_snapshot(&requested_namespace, requested_consumer, &args)
        .expect("poll has a read snapshot");
    // Resident bytes keep disk scheduling out of this test. Holding the result
    // here models the delay before acceptance shared with detached disk reads.
    assert!(!read_plan.needs_off_pump_io());
    let delayed_result = read_plan.execute_resident();

    // The reply sender is still alive, so None must come from the timeout.
    expire_timeout
        .send(())
        .expect("requester is waiting on its timer");
    assert!(
        first_poll.await.is_none(),
        "the caller received no successful poll reply"
    );
    assert!(
        matches!(
            late_reply.try_send(PartitionReadReply::NotFound),
            Err(crossfire::TrySendError::Disconnected(_))
        ),
        "timeout closed the reply channel before completion"
    );
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(
        stored_offset, None,
        "reading alone has not accepted progress"
    );

    // The reservation survives requester timeout and follows the late result
    // into the completion lane. The owner discards it before admitting progress.
    completion.complete(delayed_result);
    let completion = owner
        .poll_completions
        .try_recv()
        .expect("late completion reached its reserved lane");
    owner.on_poll_completed(*completion).await;
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(
        stored_offset, None,
        "the late result must leave progress unset for the three unseen messages"
    );
    partitions
        .with_partition(&namespace, |partition| {
            assert!(
                partition.consensus().pipeline_is_empty(),
                "the late completion must not assign an automatic commit"
            );
            assert_eq!(
                partition.consensus().request_queue_len(),
                0,
                "the late completion must not queue an automatic commit"
            );
        })
        .expect("fixture partition exists");

    // A new Next must include the three unseen messages and the fourth message.
    // Disabling its automatic commit keeps the final cursor attributable to
    // the timed out poll alone.
    let next_poll = owner.partition_read(
        namespace,
        PartitionRead::Poll {
            metadata: None,
            consumer,
            args: PollingArgs::new(PollingStrategy::next(), 4, false),
        },
    );
    futures::pin_mut!(next_poll);
    assert!(futures::poll!(next_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace,
        read,
        reply,
        hold,
    }) = owner_inbox
        .try_recv()
        .expect("subsequent Next reached the owner inbox")
    else {
        panic!("expected the subsequent poll request");
    };
    owner.on_partition_read(namespace, read, reply, hold).await;
    let Some(PartitionReadReply::Poll { fragments, .. }) = next_poll.await else {
        panic!("subsequent Next must return messages successfully");
    };
    assert_eq!(
        message_offsets(&fragments),
        vec![0, 1, 2, 3],
        "Next must include offsets 0 through 2, which the caller never received"
    );
    let (stored_offset, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .unwrap();
    assert_eq!(stored_offset, None);
}

/// Admission is checked against a synthetic disk plan: the fixture's journal
/// bytes are evicted and spawned futures are captured without execution. This
/// proves dispatch ordering; the partition tests separately cover real disk I/O.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_reserved_completion_capacity_when_disk_polls_arrive_should_reject_until_owner_dequeues()
 {
    let namespace = IggyNamespace::new(1, 1, 0);
    let group_id = 7;
    let consumer = PollingConsumer::ConsumerGroup(7, 0);
    let bus = PollTestBus::default();
    let mut owner = owner_with_messages(&bus, namespace).await;
    owner.poll_completions = PollCompletionLane::new(1, owner.metrics());
    let (owner_sender, _owner_inbox, _owner_replies) = shard_channel(0, 2, 1);
    owner.attach_senders(vec![owner_sender]);
    let partitions = owner.plane.partitions();
    let args = PollingArgs::new(PollingStrategy::offset(0), 3, true);
    let resident_plan = partitions
        .build_poll_snapshot(&namespace, consumer, &args)
        .expect("fixture has messages before eviction");
    assert!(!resident_plan.needs_off_pump_io());
    let delayed_result = resident_plan.execute_resident();
    evict_messages_for_disk_dispatch(&owner, namespace);
    assert!(
        partitions
            .build_poll_snapshot(&namespace, consumer, &args)
            .expect("evicted messages have a disk plan")
            .needs_off_pump_io()
    );
    assert!(bus.spawned_tasks.borrow().is_empty());

    // A running read owns the sole slot even while its queue is empty.
    let (held_reply, _held_replies) = channel(1);
    let reservation = owner
        .poll_completions
        .try_reserve(namespace, held_reply, None)
        .expect("reserve the only slot");
    assert_eq!(owner.poll_completion_inbox_len(), 0);
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            namespace,
            PartitionRead::Poll {
                metadata: None,
                consumer,
                args: args.clone(),
            },
            reply,
            None,
        )
        .await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert!(
        bus.spawned_tasks.borrow().is_empty(),
        "full capacity must reject before spawning I/O"
    );

    // Returning the result transfers its reservation into queue occupancy.
    // A finished read cannot admit a replacement until the owner takes it out.
    reservation.complete(delayed_result);
    assert_eq!(owner.poll_completion_inbox_len(), 1);
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            namespace,
            PartitionRead::Poll {
                metadata: None,
                consumer,
                args: args.clone(),
            },
            reply,
            None,
        )
        .await;
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert!(
        bus.spawned_tasks.borrow().is_empty(),
        "queued results still consume capacity"
    );
    let (last_polled, committed) = partitions.group_offset_state(&namespace, group_id).unwrap();
    assert_eq!(
        last_polled, None,
        "rejected dispatch must not advance group progress"
    );
    assert_eq!(
        committed, None,
        "rejected dispatch must not commit an offset"
    );
    partitions
        .with_partition(&namespace, |partition| {
            assert!(partition.consensus().pipeline_is_empty());
            assert_eq!(partition.consensus().request_queue_len(), 0);
        })
        .expect("fixture partition exists");

    // Discarding the dequeued result frees the slot without accepting its data.
    drop(
        owner
            .poll_completions
            .try_recv()
            .expect("owner dequeues held result"),
    );
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            namespace,
            PartitionRead::Poll {
                consumer,
                args,
                metadata: None,
            },
            reply,
            None,
        )
        .await;
    assert_eq!(
        bus.spawned_tasks.borrow().len(),
        1,
        "a newly available slot permits disk dispatch"
    );
    assert!(matches!(
        replies.try_recv(),
        Err(crossfire::TryRecvError::Empty)
    ));
    assert_eq!(
        owner.poll_completion_inbox_len(),
        0,
        "captured disk task has not run"
    );
    bus.spawned_tasks.borrow_mut().clear();
}

#[compio::test]
async fn given_missing_owner_route_when_disk_poll_arrives_should_reject_before_dispatch() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let bus = PollTestBus::default();
    let owner = owner_with_messages(&bus, namespace).await;
    evict_messages_for_disk_dispatch(&owner, namespace);
    let consumer = PollingConsumer::ConsumerGroup(7, 0);
    let args = PollingArgs::new(PollingStrategy::offset(0), 3, true);
    assert!(
        owner
            .plane
            .partitions()
            .build_poll_snapshot(&namespace, consumer, &args)
            .expect("fixture must enter the disk path")
            .needs_off_pump_io()
    );
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(
            namespace,
            PartitionRead::Poll {
                consumer,
                args,
                metadata: None,
            },
            reply,
            None,
        )
        .await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert!(bus.spawned_tasks.borrow().is_empty());
    assert_eq!(owner.poll_completion_inbox_len(), 0);
    assert_eq!(owner.metrics().frame_drops_value(), 1);
    let (last_polled, committed) = owner
        .plane
        .partitions()
        .group_offset_state(&namespace, 7)
        .unwrap();
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
}

#[compio::test]
async fn given_disconnected_owner_when_disk_poll_arrives_should_reject_before_dispatch() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let bus = PollTestBus::default();
    let mut owner = owner_with_messages(&bus, namespace).await;
    let (owner_sender, owner_inbox, owner_replies) = shard_channel(0, 2, 1);
    owner.attach_senders(vec![owner_sender]);
    drop(owner_inbox);
    drop(owner_replies);
    evict_messages_for_disk_dispatch(&owner, namespace);
    let consumer = PollingConsumer::ConsumerGroup(7, 0);
    let args = PollingArgs::new(PollingStrategy::offset(0), 3, true);
    assert!(
        owner
            .plane
            .partitions()
            .build_poll_snapshot(&namespace, consumer, &args)
            .expect("fixture must enter the disk path")
            .needs_off_pump_io()
    );
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(
            namespace,
            PartitionRead::Poll {
                consumer,
                args,
                metadata: None,
            },
            reply,
            None,
        )
        .await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert!(bus.spawned_tasks.borrow().is_empty());
    assert_eq!(owner.poll_completion_inbox_len(), 0);
    assert_eq!(owner.metrics().frame_drops_value(), 1);
    let (last_polled, committed) = owner
        .plane
        .partitions()
        .group_offset_state(&namespace, 7)
        .unwrap();
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
}

#[compio::test]
async fn given_unbuilt_partition_when_reading_primary_should_answer_after_insert() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);

    let replies = read_primary(&owner, namespace).await;
    assert!(replies.try_recv().is_err(), "the read waits for the build");
    assert_eq!(owner.held_read_count(), 1);

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Primary {
            view: 0,
            primary: 0
        })
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A delete and re-create between the read and the build leaves another
/// incarnation under the same namespace, which must not answer the read.
#[compio::test]
async fn given_held_read_when_revision_changes_should_answer_not_found() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let replies = read_primary(&owner, namespace).await;
    assert_eq!(owner.held_read_count(), 1);

    insert_built_partition(&owner, &bus, namespace, created_revision + 1);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

#[compio::test]
async fn given_held_read_when_hold_expires_should_answer_not_found() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let replies = read_primary(&owner, namespace).await;
    drop(read_primary(&owner, namespace).await);
    assert_eq!(owner.held_read_count(), 2);
    let hold = u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap();

    bus.now.set(hold - 1);
    owner.expire_held_reads();
    assert!(replies.try_recv().is_err(), "the hold has not ended yet");
    assert_eq!(
        owner.held_read_count(),
        1,
        "the read whose requester left is dropped"
    );

    bus.now.set(hold);
    owner.expire_held_reads();
    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
    assert_eq!(
        owner.metrics().partition_held_reads_expired_value(),
        1,
        "only the read that was answered counts"
    );
}

#[compio::test]
async fn given_missing_row_when_reading_primary_should_route_by_hash() {
    const CANDIDATE_PARTITIONS: usize = 64;
    let bus = PollTestBus::default();
    let (mut owner, _) = owner_with_unbuilt_partition(&bus, IggyNamespace::new(0, 0, 0), &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(2, 1, 1);
    owner.attach_senders(senders);
    let peer_inbox = inboxes[1].take().unwrap();
    let namespace = (0..CANDIDATE_PARTITIONS)
        .map(|id| IggyNamespace::new(0, 0, id))
        .find(|namespace| owner.partition_shards().shard_for(namespace) == 1)
        .unwrap();
    assert!(owner.shards_table.shard_for(namespace).is_none());

    let read = owner.partition_read(namespace, PartitionRead::Primary);
    futures::pin_mut!(read);
    assert!(futures::poll!(read.as_mut()).is_pending());

    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace: routed,
        read: PartitionRead::Primary,
        ..
    }) = peer_inbox
        .try_recv()
        .expect("the read reached the hash owner")
    else {
        panic!("expected the routed primary read");
    };
    assert_eq!(routed, namespace);
}

fn offset_poll() -> PartitionRead {
    PartitionRead::Poll {
        consumer: PollingConsumer::Consumer(7, 0),
        args: PollingArgs::new(PollingStrategy::offset(0), 1, false),
        metadata: None,
    }
}

/// A poll that follows a create can arrive before the reconciler of its shard
/// seeds the row, and the owner can already serve it.
#[compio::test]
async fn given_missing_row_when_polling_a_stored_partition_should_route_by_hash() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    assert!(owner.shards_table.shard_for(namespace).is_none());

    let read = owner.partition_read(namespace, offset_poll());
    futures::pin_mut!(read);
    assert!(futures::poll!(read.as_mut()).is_pending());

    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace: routed,
        read: PartitionRead::Poll { .. },
        ..
    }) = owner_inbox
        .try_recv()
        .expect("the poll reached the hash owner")
    else {
        panic!("expected the routed poll");
    };
    assert_eq!(routed, namespace);
}

/// A client reaches this node through a lookup that another member answered
/// after its own build, so a poll can arrive before the build here.
#[compio::test]
async fn given_unbuilt_partition_when_polling_on_the_owner_should_run_after_insert() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    assert!(replies.try_recv().is_err(), "the poll waits for the build");
    assert_eq!(owner.held_read_count(), 1);

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert_eq!(owner.held_read_count(), 0);
    let resubmitted = std::iter::from_fn(|| owner_inbox.try_recv().ok()).any(|frame| {
        matches!(
            frame,
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                namespace: routed,
                read: PartitionRead::Poll { .. },
                ..
            }) if routed == namespace
        )
    });
    assert!(resubmitted, "the held poll went back to the inbox");
}

/// Dispatch tests the freshness of metadata before a poll reaches the owner,
/// and a hold can end after that proof does. The owner then cannot tell
/// whether the session and the permission of the poll still stand.
#[compio::test]
async fn given_held_poll_when_its_partition_is_built_without_fresh_metadata_should_refuse_it() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) =
        unbuilt_partition_owner(&bus, namespace, &[], frontier_without_fresh_metadata(&bus));
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    assert_eq!(owner.held_read_count(), 1);

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    let resubmitted = std::iter::from_fn(|| owner_inbox.try_recv().ok()).any(|frame| {
        matches!(
            frame,
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { .. })
        )
    });
    assert!(
        !resubmitted,
        "the held poll must not run without fresh metadata"
    );
    assert_eq!(owner.held_read_count(), 0);
}

/// A poll without metadata has no fence of its own. A delete and a create
/// between two ticks put another incarnation where it waited, and there it
/// must not run, because it can store an offset.
#[compio::test]
async fn given_held_poll_when_another_incarnation_is_built_should_refuse_it() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    assert_eq!(owner.held_read_count(), 1);

    insert_built_partition(&owner, &bus, namespace, created_revision + 1);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    let resubmitted = std::iter::from_fn(|| owner_inbox.try_recv().ok()).any(|frame| {
        matches!(
            frame,
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { .. })
        )
    });
    assert!(
        !resubmitted,
        "the held poll must not run on the other incarnation"
    );
    assert_eq!(owner.held_read_count(), 0);
}

/// The build ends the hold and hands the poll back to the inbox. A delete and
/// a create can put another incarnation of its ids there before the pump
/// takes the poll from the inbox, and the poll must not run on that one.
#[compio::test]
async fn given_held_poll_handed_back_when_another_incarnation_is_built_before_it_runs_should_refuse_it()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    insert_built_partition(&owner, &bus, namespace, created_revision);
    let (read, reply, hold) = std::iter::from_fn(|| owner_inbox.try_recv().ok())
        .find_map(|frame| match frame {
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                read, reply, hold, ..
            }) => Some((read, reply, hold)),
            _ => None,
        })
        .expect("the build hands the held poll back to the inbox");
    assert_eq!(
        hold.and_then(|hold| hold.created_revision),
        Some(created_revision),
        "the poll carries the incarnation that its hold waited for"
    );

    begin_teardown(&owner, namespace);
    confirm_remove(&owner, namespace);
    insert_built_partition(&owner, &bus, namespace, created_revision + 1);
    owner.on_partition_read(namespace, read, reply, hold).await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A poll that a hold handed back can find its partition gone and the next
/// incarnation of its ids due. It must not wait for that one.
#[compio::test]
async fn given_held_poll_handed_back_when_another_incarnation_is_due_should_refuse_it_without_a_hold()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let waited_for = created_revision
        .checked_sub(1)
        .expect("the seed commits above revision 0");
    let (reply, replies) = channel(1);
    let hold = ReadHold {
        deadline: u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap(),
        created_revision: Some(waited_for),
    };

    owner
        .on_partition_read(namespace, offset_poll(), reply, Some(hold))
        .await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A delete and a create that reuse the ids of a partition commit while this
/// owner still has the old incarnation. A create waits on this lookup, so it
/// must get the primary of the new one, also through the teardown of the old
/// one. The other reads are not held.
#[compio::test]
async fn given_older_incarnation_here_when_reading_primary_should_answer_after_the_committed_one_is_built()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let old_revision = created_revision
        .checked_sub(1)
        .expect("the seed commits above revision 0");
    insert_built_partition(&owner, &bus, namespace, old_revision);
    let (reply, polls) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    let poll = polls.try_recv();
    assert!(
        matches!(poll, Ok(PartitionReadReply::Poll { .. })),
        "a poll is not held: {poll:?}"
    );
    assert_eq!(owner.held_read_count(), 0);

    let replies = read_primary(&owner, namespace).await;
    let early = replies.try_recv();
    assert!(
        early.is_err(),
        "the lookup waits for the committed incarnation: {early:?}"
    );
    assert_eq!(owner.held_read_count(), 1);
    owner.expire_held_reads();
    assert_eq!(
        owner.held_read_count(),
        1,
        "the tick keeps the hold while the old incarnation serves"
    );
    begin_teardown(&owner, namespace);
    owner.expire_held_reads();
    assert_eq!(
        owner.held_read_count(),
        1,
        "the tick keeps the hold through the teardown"
    );
    confirm_remove(&owner, namespace);
    assert_eq!(
        owner.held_read_count(),
        1,
        "the removal of the old incarnation keeps the hold"
    );

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Primary {
            view: 0,
            primary: 0
        })
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// The teardown of an older incarnation hides it before the reconciler builds
/// the committed one, so the reads of the namespace wait for that build.
#[compio::test]
async fn given_older_incarnation_in_teardown_when_reading_should_hold_until_the_committed_one_is_built()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let old_revision = created_revision
        .checked_sub(1)
        .expect("the seed commits above revision 0");
    insert_built_partition(&owner, &bus, namespace, old_revision);
    begin_teardown(&owner, namespace);

    let lookups = read_primary(&owner, namespace).await;
    let (reply, polls) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    let early = (lookups.try_recv(), polls.try_recv());
    assert!(
        early.0.is_err() && early.1.is_err(),
        "both reads wait for the build: {early:?}"
    );
    assert_eq!(owner.held_read_count(), 2);
    confirm_remove(&owner, namespace);
    assert_eq!(
        owner.held_read_count(),
        2,
        "the removal of the old incarnation keeps the holds"
    );

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        lookups.try_recv(),
        Ok(PartitionReadReply::Primary {
            view: 0,
            primary: 0
        })
    ));
    assert_eq!(owner.held_read_count(), 0);
    let resubmitted = std::iter::from_fn(|| owner_inbox.try_recv().ok()).any(|frame| {
        matches!(
            frame,
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                namespace: routed,
                read: PartitionRead::Poll { .. },
                ..
            }) if routed == namespace
        )
    });
    assert!(resubmitted, "the held poll went back to the inbox");
}

/// A fence of the committed incarnation never lifts, so a hold there could
/// only end at its deadline.
#[compio::test]
async fn given_fence_of_the_committed_incarnation_when_reading_primary_should_answer_not_found_at_once()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    owner.plane.partitions().fence(namespace, created_revision);

    let replies = read_primary(&owner, namespace).await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// The reconciler removes the files that a fence of an older incarnation
/// guards, and then builds the committed one, so a lookup waits for that
/// build.
#[compio::test]
async fn given_fence_of_an_older_incarnation_when_reading_primary_should_answer_after_the_committed_one_is_built()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let old_revision = created_revision
        .checked_sub(1)
        .expect("the seed commits above revision 0");
    owner.plane.partitions().fence(namespace, old_revision);

    let replies = read_primary(&owner, namespace).await;
    let early = replies.try_recv();
    assert!(
        early.is_err(),
        "the lookup waits for the committed incarnation: {early:?}"
    );
    assert_eq!(owner.held_read_count(), 1);
    confirm_remove(&owner, namespace);
    assert_eq!(
        owner.held_read_count(),
        1,
        "the removal of the fenced files keeps the hold"
    );

    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Primary {
            view: 0,
            primary: 0
        })
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// Whether the reconciler builds a namespace again after a tombstone without
/// a partition depends on records of its own, which this shard cannot see.
#[compio::test]
async fn given_tombstone_without_a_partition_when_reading_primary_should_answer_not_found_at_once()
{
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    owner.plane.partitions().tombstone(namespace);

    let replies = read_primary(&owner, namespace).await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// HTTP waits for a primary in a loop of its own, which tests the deadline of
/// the request. Its lookup must come back at once, also for a partition that
/// this shard is due to build.
#[compio::test]
async fn given_unbuilt_partition_when_looking_up_the_primary_now_should_answer_at_once() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(namespace, PartitionRead::PrimaryNow, reply, None)
        .await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A delete and a create that reuse the ids of a partition commit while this
/// owner still has the old incarnation. HTTP serves at the primary that this
/// lookup names, and the owner refuses a write of the committed incarnation
/// there, after HTTP answered a send without ack.
#[compio::test]
async fn given_older_incarnation_here_when_looking_up_the_primary_now_should_name_no_primary() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let old_revision = created_revision
        .checked_sub(1)
        .expect("the seed commits above revision 0");
    insert_built_partition(&owner, &bus, namespace, old_revision);
    let look_up = async || {
        let (reply, replies) = channel(1);
        owner
            .on_partition_read(namespace, PartitionRead::PrimaryNow, reply, None)
            .await;
        replies.try_recv()
    };

    let superseded = look_up().await;

    assert!(
        matches!(
            superseded,
            Ok(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ),
        "the old incarnation must not name a primary: {superseded:?}"
    );
    assert_eq!(owner.held_read_count(), 0);

    begin_teardown(&owner, namespace);
    confirm_remove(&owner, namespace);
    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        look_up().await,
        Ok(PartitionReadReply::Primary {
            view: 0,
            primary: 0
        })
    ));
}

/// The requester of a deferred poll stops its wait at the request deadline.
/// The hold must end one reply margin before it, so that the requester gets
/// the refusal of the owner and not a spent reply budget.
#[compio::test]
async fn given_held_deferred_poll_when_its_request_deadline_nears_should_reject_as_transient() {
    const REQUEST_DEADLINE: u64 = 1_000_000;
    const HOLD_END: u64 = REQUEST_DEADLINE - FETCH_REPLY_MARGIN_US;
    assert!(Duration::from_micros(REQUEST_DEADLINE) < PARTITION_BUILD_HOLD);
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let mut request = deferred_poll(&owner, namespace);
    request.request_deadline = REQUEST_DEADLINE;
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            namespace,
            PartitionRead::DeferredPoll(Box::new(request)),
            reply,
            None,
        )
        .await;
    bus.now.set(HOLD_END - 1);
    owner.expire_held_reads();
    assert_eq!(owner.held_read_count(), 1);

    bus.now.set(HOLD_END);
    owner.expire_held_reads();

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A held read of a fetch keeps back the replies of the reads beside it, so
/// the fetch clears the hold of such a read.
#[compio::test]
async fn given_deferred_poll_that_must_not_wait_when_its_partition_is_unbuilt_should_answer_at_once()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let mut request = deferred_poll(&owner, namespace);
    // Time for a whole hold, so only the switch keeps the read out of it.
    request.request_deadline = u64::MAX;
    request.holdable = false;
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(
            namespace,
            PartitionRead::DeferredPoll(Box::new(request)),
            reply,
            None,
        )
        .await;

    // The admission refuses it: this owner has no partition to read from.
    assert!(
        matches!(replies.try_recv(), Ok(PartitionReadReply::Rejected(_))),
        "the read must not wait for the build"
    );
    assert_eq!(owner.held_read_count(), 0);
}

/// A fetch asks every owner without a hold first, so that a partition that
/// cannot serve yet does not delay its other targets.
#[compio::test]
async fn given_offset_batch_whose_hold_end_passed_when_reading_should_answer_it_at_once() {
    const NOW: u64 = 1_000;
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    bus.now.set(NOW);
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let read = ConsumerOffsetRead {
        namespace,
        consumer: Some(PollingConsumer::Consumer(7, 0)),
        metadata: owner
            .plane
            .metadata()
            .mux_stm
            .streams()
            .poll_metadata(namespace, None, 7)
            .expect("metadata names the partition"),
    };
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(
            namespace,
            PartitionRead::ConsumerOffsetsOnPrimary {
                reads: vec![read],
                hold_until: NOW,
            },
            reply,
            None,
        )
        .await;

    let Ok(PartitionReadReply::ConsumerOffsets(offsets)) = replies.try_recv() else {
        panic!("a batch that leaves no time for a hold must be answered at once");
    };
    assert!(matches!(
        offsets.as_slice(),
        [PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        )]
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// HTTP maps the `NotFound` of an owner to a permanent 404, so a poll whose
/// partition did not arrive must stay a miss that the client retries.
#[compio::test]
async fn given_held_poll_when_hold_expires_should_reject_as_transient() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    assert_eq!(owner.held_read_count(), 1);

    bus.now
        .set(u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap());
    owner.expire_held_reads();

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A build can land after the end of a hold and before the sweep that
/// expires it. The requester of that poll no longer waits for a run, which
/// could commit an offset whose messages nobody receives.
#[compio::test]
async fn given_held_poll_when_its_partition_is_built_after_the_hold_ended_should_reject_as_transient()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;
    assert_eq!(owner.held_read_count(), 1);

    bus.now
        .set(u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap());
    insert_built_partition(&owner, &bus, namespace, created_revision);

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
    let resubmitted = std::iter::from_fn(|| owner_inbox.try_recv().ok()).any(|frame| {
        matches!(
            frame,
            ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { .. })
        )
    });
    assert!(!resubmitted, "a poll whose hold ended must not run");
}

/// The hop target of an HTTP relay stops its wait at the end of its budget.
/// The hold of its read must end one reply margin before that, and never
/// later than a hold without a budget. The wait for the reply ends with the
/// budget, and never later than a read without one.
#[compio::test]
async fn given_a_requester_budget_when_reading_an_unbuilt_partition_should_end_the_hold_before_it()
{
    const SHORT_BUDGET: Duration = Duration::from_secs(1);
    let namespace = IggyNamespace::new(0, 0, 0);
    let build_hold = u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap();
    let short_hold = u64::try_from(SHORT_BUDGET.as_micros()).unwrap() - FETCH_REPLY_MARGIN_US;
    for (budget, hold, reply_wait) in [
        (SHORT_BUDGET, short_hold, SHORT_BUDGET),
        (
            PARTITION_BUILD_HOLD * 2,
            build_hold,
            PARTITION_BUILD_HOLD * 2,
        ),
        (
            PARTITION_READ_TIMEOUT * 2,
            build_hold,
            PARTITION_READ_TIMEOUT,
        ),
    ] {
        let bus = PollTestBus::default();
        // The bus refuses a reply timer of another duration.
        bus.read_budget.set(Some(reply_wait));
        let (mut owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
        let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
        owner.attach_senders(senders);
        let owner_inbox = inboxes[0].take().unwrap();

        let read = owner.partition_read_budgeted(namespace, offset_poll(), budget);
        futures::pin_mut!(read);
        assert!(futures::poll!(read.as_mut()).is_pending());
        let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
            namespace: routed,
            read: poll,
            reply,
            hold: limits,
        }) = owner_inbox.try_recv().expect("the read reached the owner")
        else {
            panic!("expected the routed poll");
        };
        owner.on_partition_read(routed, poll, reply, limits).await;
        assert_eq!(owner.held_read_count(), 1, "budget {budget:?}");

        bus.now.set(hold - 1);
        owner.expire_held_reads();
        assert!(
            futures::poll!(read.as_mut()).is_pending(),
            "the hold of budget {budget:?} has not ended yet"
        );
        bus.now.set(hold);
        owner.expire_held_reads();
        assert!(
            matches!(
                futures::poll!(read.as_mut()),
                Poll::Ready(Some(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted
                )))
            ),
            "the hold of budget {budget:?} must end {hold} us after the read"
        );
    }
}

/// A fence for a rebuild leaves the partition in metadata and removes it
/// here. A poll that arrives by hash must stay a miss that the client retries.
#[compio::test]
async fn given_tombstoned_partition_when_polling_on_the_owner_should_reject_as_transient() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    owner.plane.partitions().tombstone(namespace);
    let (reply, replies) = channel(1);

    owner
        .on_partition_read(namespace, offset_poll(), reply, None)
        .await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A partition created in a view above 0 starts with a pending view write.
/// The create reply and the first send wait on this lookup, so it must end
/// when the primary can serve, not at the insert.
#[compio::test]
async fn given_primary_with_a_pending_view_write_when_reading_primary_should_answer_after_it_lands()
{
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let replies = read_primary(&owner, namespace).await;
    assert_eq!(owner.held_read_count(), 1);

    owner.enqueue_reconcile_op(ReconcileOp::InsertOwned {
        namespace,
        partition: Box::new(primary_with_pending_view_write(
            bus.clone(),
            namespace,
            created_revision,
        )),
        epoch: created_revision,
    });
    owner.apply_reconcile_ops();
    assert!(
        replies.try_recv().is_err(),
        "the insert of a primary that still starts ends no hold"
    );
    owner.expire_held_reads();
    assert_eq!(owner.held_read_count(), 1);

    let landed = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace)
        .unwrap()
        .persist_superblock_if_needed()
        .await;
    assert!(landed);
    owner.expire_held_reads();

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Primary { .. })
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// HTTP serves a read at the primary that its lookup names. A primary here
/// that still starts refuses that read after the hold, so the lookup must
/// name no primary until the view write lands.
#[compio::test]
async fn given_primary_with_a_pending_view_write_when_looking_up_the_primary_now_should_name_it_after_the_write_lands()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    owner.enqueue_reconcile_op(ReconcileOp::InsertOwned {
        namespace,
        partition: Box::new(primary_with_pending_view_write(
            bus.clone(),
            namespace,
            created_revision,
        )),
        epoch: created_revision,
    });
    owner.apply_reconcile_ops();
    let lookup = async || {
        let (reply, replies) = channel(1);
        owner
            .on_partition_read(namespace, PartitionRead::PrimaryNow, reply, None)
            .await;
        replies.try_recv()
    };

    assert!(matches!(
        lookup().await,
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);

    let landed = owner
        .plane
        .partitions()
        .get_mut_by_ns(&namespace)
        .unwrap()
        .persist_superblock_if_needed()
        .await;
    assert!(landed);

    assert!(matches!(
        lookup().await,
        Ok(PartitionReadReply::Primary { .. })
    ));
}

/// A batch is held for one partition that cannot serve. Its requester stops
/// the wait before the build hold ends, so the hold must end at the moment
/// that the requester named, and the other entries must get their offsets.
#[compio::test]
async fn given_held_offset_batch_when_its_hold_deadline_passes_should_refuse_only_the_unbuilt_entry()
 {
    const HOLD_UNTIL: u64 = 400_000;
    assert!(Duration::from_micros(HOLD_UNTIL) < PARTITION_BUILD_HOLD);
    let unbuilt = IggyNamespace::new(0, 0, 0);
    let ready = IggyNamespace::new(1, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, unbuilt, &[]);
    let streams = owner.plane.metadata().mux_stm.streams();
    streams.seed_namespace_on(ready, ready.inner(), 0, Durability::Persisted, &[]);
    let ready_revision = streams
        .created_revision_for_namespace(ready)
        .expect("the seed committed the partition");
    insert_built_partition(&owner, &bus, ready, ready_revision);
    let read = |namespace| ConsumerOffsetRead {
        namespace,
        consumer: Some(PollingConsumer::Consumer(7, 0)),
        metadata: streams
            .poll_metadata(namespace, None, 7)
            .expect("metadata names the partition"),
    };
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            unbuilt,
            PartitionRead::ConsumerOffsetsOnPrimary {
                reads: vec![read(unbuilt), read(ready)],
                hold_until: HOLD_UNTIL,
            },
            reply,
            None,
        )
        .await;
    bus.now.set(HOLD_UNTIL - 1);
    owner.expire_held_reads();
    assert_eq!(owner.held_read_count(), 1);

    bus.now.set(HOLD_UNTIL);
    owner.expire_held_reads();

    let Ok(PartitionReadReply::ConsumerOffsets(offsets)) = replies.try_recv() else {
        panic!("a batch whose hold ended must be answered entry by entry");
    };
    assert!(
        matches!(
            offsets.as_slice(),
            [
                PartitionReadReply::Rejected(IggyError::TransientNotAccepted),
                PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: 0
                }
            ]
        ),
        "only the entry of the unbuilt partition is refused, not {offsets:?}"
    );
    assert_eq!(owner.held_read_count(), 0);
}

/// The end of a hold answers a batch without a new pass through the inbox,
/// so that answer needs the freshness test of its own.
#[compio::test]
async fn given_held_offset_batch_when_its_hold_ends_without_fresh_metadata_should_refuse_it() {
    const HOLD_UNTIL: u64 = 400_000;
    assert!(Duration::from_micros(HOLD_UNTIL) < PARTITION_BUILD_HOLD);
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) =
        unbuilt_partition_owner(&bus, namespace, &[], frontier_without_fresh_metadata(&bus));
    let read = ConsumerOffsetRead {
        namespace,
        consumer: Some(PollingConsumer::Consumer(7, 0)),
        metadata: owner
            .plane
            .metadata()
            .mux_stm
            .streams()
            .poll_metadata(namespace, None, 7)
            .expect("metadata names the partition"),
    };
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            namespace,
            PartitionRead::ConsumerOffsetsOnPrimary {
                reads: vec![read],
                hold_until: HOLD_UNTIL,
            },
            reply,
            None,
        )
        .await;
    assert_eq!(owner.held_read_count(), 1);

    bus.now.set(HOLD_UNTIL);
    owner.expire_held_reads();

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A batch is held for its first entry that cannot serve. When that entry
/// settles, the batch goes back to the inbox and is held for the next entry.
/// That hold must end where the first one did, or the requester of a batch of
/// many new partitions waits a whole hold for each of them.
#[compio::test]
async fn given_offset_batch_held_again_for_another_entry_when_its_first_hold_ends_should_answer_it()
{
    const SETTLED_AT: u64 = 1_000_000;
    let hold = u64::try_from(PARTITION_BUILD_HOLD.as_micros()).unwrap();
    assert!(SETTLED_AT < hold);
    let settles = IggyNamespace::new(0, 0, 0);
    let stays_unbuilt = IggyNamespace::new(1, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, settled_revision) = owner_with_unbuilt_partition(&bus, settles, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 4, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let streams = owner.plane.metadata().mux_stm.streams();
    streams.seed_namespace_on(
        stays_unbuilt,
        stays_unbuilt.inner(),
        0,
        Durability::Persisted,
        &[],
    );
    let read = |namespace| ConsumerOffsetRead {
        namespace,
        consumer: Some(PollingConsumer::Consumer(7, 0)),
        metadata: streams
            .poll_metadata(namespace, None, 7)
            .expect("metadata names the partition"),
    };
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            settles,
            PartitionRead::ConsumerOffsetsOnPrimary {
                reads: vec![read(settles), read(stays_unbuilt)],
                hold_until: u64::MAX,
            },
            reply,
            None,
        )
        .await;
    assert_eq!(owner.held_read_count(), 1);

    bus.now.set(SETTLED_AT);
    insert_built_partition(&owner, &bus, settles, settled_revision);
    assert_eq!(
        owner.held_read_count(),
        0,
        "the settled entry hands the batch back to the inbox"
    );
    while let Ok(frame) = owner_inbox.try_recv() {
        if let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
            namespace,
            read,
            reply,
            hold,
        }) = frame
        {
            owner.on_partition_read(namespace, read, reply, hold).await;
        }
    }
    assert_eq!(
        owner.held_read_count(),
        1,
        "the batch is held for its other entry"
    );

    bus.now.set(hold - 1);
    owner.expire_held_reads();
    assert!(
        replies.try_recv().is_err(),
        "the first hold has not ended yet"
    );
    assert_eq!(
        owner.metrics().partition_held_reads_value(),
        2,
        "the gauge counts both reads of the one held batch"
    );

    bus.now.set(hold);
    owner.expire_held_reads();

    let Ok(PartitionReadReply::ConsumerOffsets(offsets)) = replies.try_recv() else {
        panic!("a batch held again must be answered when its first hold ends");
    };
    assert!(
        matches!(
            offsets.as_slice(),
            [
                PartitionReadReply::ConsumerOffset {
                    stored: None,
                    current_offset: 0
                },
                PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
            ]
        ),
        "only the entry that stays unbuilt is refused, not {offsets:?}"
    );
    assert_eq!(owner.held_read_count(), 0);
}

/// A batch stores each of its offset reads, so each of them counts against
/// the shard cap. A batch that does not fit is answered without the wait.
#[compio::test]
async fn given_offset_batch_above_the_shard_hold_cap_when_reading_should_answer_it_at_once() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let streams = owner.plane.metadata().mux_stm.streams();
    let batch = |reads: usize| {
        let read = ConsumerOffsetRead {
            namespace,
            consumer: Some(PollingConsumer::Consumer(7, 0)),
            metadata: streams
                .poll_metadata(namespace, None, 7)
                .expect("metadata names the partition"),
        };
        PartitionRead::ConsumerOffsetsOnPrimary {
            reads: vec![read; reads],
            hold_until: u64::MAX,
        }
    };
    let (held_reply, held_replies) = channel(1);
    owner
        .on_partition_read(namespace, batch(MAX_HELD_READS - 1), held_reply, None)
        .await;
    assert_eq!(owner.held_read_count(), 1);

    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, batch(2), reply, None)
        .await;

    let Ok(PartitionReadReply::ConsumerOffsets(offsets)) = replies.try_recv() else {
        panic!("a batch above the cap must be answered at once");
    };
    assert!(matches!(
        offsets.as_slice(),
        [
            PartitionReadReply::Rejected(IggyError::TransientNotAccepted),
            PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
        ]
    ));
    assert_eq!(owner.held_read_count(), 1);
    assert!(
        held_replies.try_recv().is_err(),
        "the first batch stays held"
    );
    assert_eq!(
        owner.metrics().partition_held_reads_refused_value(),
        1,
        "the refused batch counts once, the held one not at all"
    );
}

/// The fetch of a `Next` consumer reads its offsets through this batch.
#[compio::test]
async fn given_unbuilt_partition_when_reading_offsets_on_primary_should_send_the_batch_to_the_owner()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (mut owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace, None, 7)
        .expect("metadata names the partition");

    let reads = owner.consumer_offsets_on_primary(
        vec![ConsumerOffsetRead {
            namespace,
            consumer: Some(PollingConsumer::Consumer(7, 0)),
            metadata,
        }],
        OffsetReadDeadlines {
            hold_until: u64::MAX,
            answer_by: u64::MAX,
        },
    );
    futures::pin_mut!(reads);
    assert!(futures::poll!(reads.as_mut()).is_pending());

    assert!(matches!(
        owner_inbox.try_recv(),
        Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
            read: PartitionRead::ConsumerOffsetsOnPrimary { .. },
            ..
        }))
    ));
}

/// The first stored-offset reads of a fetch leave no time for a hold, and a
/// `Next` reader sends them with every fetch. Its own partitions must not
/// take the lookups of a hold, nor a trip through the inbox.
#[compio::test]
async fn given_unbuilt_partition_when_reading_offsets_whose_hold_end_passed_should_answer_in_place()
{
    const NOW: u64 = 1_000;
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    bus.now.set(NOW);
    let (mut owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace, None, 7)
        .expect("metadata names the partition");

    let reads = owner.consumer_offsets_on_primary(
        vec![ConsumerOffsetRead {
            namespace,
            consumer: Some(PollingConsumer::Consumer(7, 0)),
            metadata,
        }],
        OffsetReadDeadlines {
            hold_until: NOW,
            answer_by: u64::MAX,
        },
    );
    futures::pin_mut!(reads);

    let Poll::Ready(offsets) = futures::poll!(reads.as_mut()) else {
        panic!("a read that no owner can hold must not wait for the inbox");
    };
    assert!(
        matches!(
            offsets.as_slice(),
            [Some(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))]
        ),
        "the partition that cannot serve refuses its read, not {offsets:?}"
    );
    assert!(
        owner_inbox.try_recv().is_err(),
        "the read takes no trip through the inbox"
    );
}

/// A fetch needs the stored offsets by its read deadline. An owner that does
/// not answer by then must not cost the answers of the other owners.
#[compio::test]
async fn given_one_silent_owner_when_reading_offsets_on_primary_should_keep_the_answers_of_the_others()
 {
    const ANSWER_BY: u64 = 250_000;
    const SILENT_OWNER: u16 = 1;
    const ANSWERING_OWNER: u16 = 2;
    let silent = IggyNamespace::new(0, 0, 0);
    let answering = IggyNamespace::new(1, 0, 0);
    let (answer_by_passed, timeout) = oneshot::channel();
    let bus = PollTestBus {
        next_timeout: Rc::new(RefCell::new(Some(timeout))),
        ..PollTestBus::default()
    };
    bus.read_budget.set(Some(Duration::from_micros(ANSWER_BY)));
    let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, silent, &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(3, 1, 1);
    owner.attach_senders(senders);
    let streams = owner.plane.metadata().mux_stm.streams();
    streams.seed_namespace_on(answering, answering.inner(), 0, Durability::Persisted, &[]);
    for (namespace, shard) in [(silent, SILENT_OWNER), (answering, ANSWERING_OWNER)] {
        owner.shards_table.insert(
            namespace,
            PartitionLocation::new(ShardId::new(shard), created_revision),
        );
    }
    let read = |namespace| ConsumerOffsetRead {
        namespace,
        consumer: Some(PollingConsumer::Consumer(7, 0)),
        metadata: streams
            .poll_metadata(namespace, None, 7)
            .expect("metadata names the partition"),
    };

    let offsets = owner.consumer_offsets_on_primary(
        vec![read(silent), read(answering)],
        OffsetReadDeadlines {
            hold_until: 0,
            answer_by: ANSWER_BY,
        },
    );
    futures::pin_mut!(offsets);
    assert!(futures::poll!(offsets.as_mut()).is_pending());
    let Ok(ShardFrame::Lifecycle(LifecycleFrame::PartitionRead { reply, .. })) = inboxes
        [usize::from(ANSWERING_OWNER)]
    .take()
    .unwrap()
    .try_recv() else {
        panic!("the answering owner gets its frame");
    };
    reply
        .try_send(PartitionReadReply::ConsumerOffsets(vec![
            PartitionReadReply::ConsumerOffset {
                stored: Some(4),
                current_offset: 9,
            },
        ]))
        .unwrap();
    assert!(
        futures::poll!(offsets.as_mut()).is_pending(),
        "the silent owner has until the moment the caller named"
    );

    answer_by_passed.send(()).unwrap();

    let offsets = offsets.await;
    assert!(
        matches!(
            offsets.as_slice(),
            [
                None,
                Some(PartitionReadReply::ConsumerOffset {
                    stored: Some(4),
                    current_offset: 9
                })
            ]
        ),
        "only the read of the silent owner stays unanswered, not {offsets:?}"
    );
}

/// A fetch reads its targets by its read deadline. An owner that does not
/// answer the admission of a wait is awaited for `admission_wait` at most.
/// The wait then returns the ready targets of the other owners, and fails
/// when it has none.
#[compio::test]
async fn given_one_silent_owner_when_waiting_for_messages_should_end_at_the_admission_wait_with_the_ready_targets_of_the_others()
 {
    const ADMISSION_WAIT: Duration = Duration::from_millis(250);
    const SILENT_OWNER: u16 = 1;
    const ANSWERING_OWNER: u16 = 2;
    const CONSUMER: u128 = 7;
    let silent = IggyNamespace::new(0, 0, 0);
    let answering = IggyNamespace::new(1, 0, 0);
    for ready in [true, false] {
        let bus = PollTestBus::default();
        bus.read_budget.set(Some(ADMISSION_WAIT));
        let (mut owner, created_revision) = owner_with_unbuilt_partition(&bus, silent, &[]);
        let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(3, 1, 1);
        owner.attach_senders(senders);
        let streams = owner.plane.metadata().mux_stm.streams();
        streams.seed_namespace_on(answering, answering.inner(), 0, Durability::Persisted, &[]);
        for (namespace, shard) in [(silent, SILENT_OWNER), (answering, ANSWERING_OWNER)] {
            owner.shards_table.insert(
                namespace,
                PartitionLocation::new(ShardId::new(shard), created_revision),
            );
        }
        let target = |namespace, partition_id| ReadinessTarget {
            namespace,
            partition_id,
            consumer: PollingConsumer::Consumer(7, 0),
            strategy: PollingStrategy::offset(0),
            metadata: streams
                .poll_metadata(namespace, None, CONSUMER)
                .expect("metadata names the partition"),
        };
        let found = ReadyTarget {
            partition_id: 1,
            available: 1,
            known_bytes: None,
        };

        let wait = owner.wait_for_messages(
            DeferredPollContext {
                session: None,
                user_id: 0,
                credential: None,
                quota: PollQuotaIdentity::Session(CONSUMER),
                primary: true,
            },
            vec![target(silent, 0), target(answering, 1)],
            Duration::ZERO,
            ANY_MESSAGE,
            Some(ADMISSION_WAIT),
        );
        futures::pin_mut!(wait);
        assert!(futures::poll!(wait.as_mut()).is_pending());
        let Ok(ShardFrame::Lifecycle(LifecycleFrame::ReadinessWait { reply, .. })) = inboxes
            [usize::from(ANSWERING_OWNER)]
        .take()
        .unwrap()
        .try_recv() else {
            panic!("the answering owner gets its frame");
        };
        reply
            .try_send(PartitionReadReply::Ready {
                shard: ANSWERING_OWNER,
                parked: None,
                partitions: if ready { vec![found] } else { Vec::new() },
            })
            .unwrap();
        let (admission_wait_passed, timeout) = oneshot::channel();
        *bus.next_timeout.borrow_mut() = Some(timeout);
        assert!(
            futures::poll!(wait.as_mut()).is_pending(),
            "the silent owner has until the end of the admission wait"
        );

        admission_wait_passed.send(()).unwrap();

        let outcome = wait.await;
        if ready {
            assert_eq!(outcome.expect("an owner answered with messages"), [found]);
        } else {
            assert!(matches!(outcome, Err(IggyError::ShardCommunicationError)));
        }
    }
}

#[compio::test]
async fn given_missing_row_when_polling_an_unknown_namespace_should_reject_as_transient() {
    let bus = PollTestBus::default();
    let (mut owner, _) = owner_with_unbuilt_partition(&bus, IggyNamespace::new(0, 0, 0), &[]);
    let (senders, mut inboxes, _reply_inboxes) = shard_mesh_channels(1, 1, 1);
    owner.attach_senders(senders);
    let owner_inbox = inboxes[0].take().unwrap();

    let reply = owner
        .partition_read(IggyNamespace::new(0, 0, 1), offset_poll())
        .await;

    assert!(matches!(
        reply,
        Some(PartitionReadReply::Rejected(
            IggyError::TransientNotAccepted
        ))
    ));
    assert!(
        owner_inbox.try_recv().is_err(),
        "a poll of a namespace that metadata does not hold reaches no owner"
    );
}

#[compio::test]
async fn given_unknown_namespace_when_reading_primary_should_answer_not_found_at_once() {
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, IggyNamespace::new(0, 0, 0), &[]);

    let replies = read_primary(&owner, IggyNamespace::new(0, 0, 1)).await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// A node that does not store the partition never builds it, so a hold
/// there could only end at its deadline.
#[compio::test]
async fn given_partition_stored_on_other_nodes_when_reading_primary_should_answer_not_found_at_once()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, _) = owner_with_unbuilt_partition(&bus, namespace, &[1, 2]);

    let replies = read_primary(&owner, namespace).await;

    assert!(matches!(
        replies.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), 0);
}

/// The cap is what bounds the memory of the holds.
#[compio::test]
async fn given_namespace_at_its_hold_cap_when_reading_primary_should_answer_not_found_at_once() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = PollTestBus::default();
    let (owner, created_revision) = owner_with_unbuilt_partition(&bus, namespace, &[]);
    let mut held = Vec::with_capacity(MAX_HELD_READS_PER_NAMESPACE);
    for _ in 0..MAX_HELD_READS_PER_NAMESPACE {
        held.push(read_primary(&owner, namespace).await);
    }

    let refused = read_primary(&owner, namespace).await;

    assert!(matches!(
        refused.try_recv(),
        Ok(PartitionReadReply::NotFound)
    ));
    assert_eq!(owner.held_read_count(), MAX_HELD_READS_PER_NAMESPACE);
    assert_eq!(
        owner.metrics().partition_held_reads_refused_value(),
        1,
        "only the read above the cap counts"
    );
    insert_built_partition(&owner, &bus, namespace, created_revision);
    assert!(
        held.iter()
            .all(|replies| matches!(replies.try_recv(), Ok(PartitionReadReply::Primary { .. })))
    );
}

/// A shard whose metadata holds `namespace` as committed on `members`, every
/// node when empty, and whose reconciler has not built it yet. Returns the
/// committed revision too.
fn owner_with_unbuilt_partition(
    bus: &PollTestBus,
    namespace: IggyNamespace,
    members: &[u8],
) -> (PollTestShard, u64) {
    unbuilt_partition_owner(bus, namespace, members, AppliedFrontier::default())
}

/// [`owner_with_unbuilt_partition`] on a node with `frontier`.
fn unbuilt_partition_owner(
    bus: &PollTestBus,
    namespace: IggyNamespace,
    members: &[u8],
    frontier: AppliedFrontier,
) -> (PollTestShard, u64) {
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None)
        .with_applied_frontier(Arc::new(frontier));
    let owner = PollTestShard::without_inbox(
        ShardIdentity::new(0, "held-read-test".to_string()),
        bus.clone(),
        metadata,
        IggyPartitions::new(ShardId::new(0), partitions_config()),
        PapayaShardsTable::new(),
        PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus.clone()),
    );
    let streams = owner.plane.metadata().mux_stm.streams();
    streams.seed_namespace_on(
        namespace,
        namespace.inner(),
        0,
        Durability::Persisted,
        members,
    );
    let created_revision = streams
        .created_revision_for_namespace(namespace)
        .expect("the seed committed the partition");
    (owner, created_revision)
}

/// The frontier of a node that must prove fresh metadata and has no proof:
/// nothing credited it, which reads the same as a proof that ended.
fn frontier_without_fresh_metadata(bus: &PollTestBus) -> AppliedFrontier {
    const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
    AppliedFrontier::default().with_partition_freshness(bus.monotonic_now(), HEARTBEAT_TIMEOUT)
}

/// Sends a `Primary` read to the pump handler of `owner`.
#[allow(clippy::future_not_send)]
async fn read_primary(
    owner: &PollTestShard,
    namespace: IggyNamespace,
) -> Receiver<PartitionReadReply> {
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, PartitionRead::Primary, reply, None)
        .await;
    replies
}

/// Inserts a partition of `namespace` built at `created_revision`, through
/// the op that the reconciler stages.
/// A deferred poll of the first message of `namespace`, without a deadline.
fn deferred_poll(owner: &PollTestShard, namespace: IggyNamespace) -> DeferredPollRequest {
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(namespace, None, 7)
        .expect("metadata names the partition");
    DeferredPollRequest::new(
        PollingConsumer::Consumer(7, 0),
        PollingArgs::new(PollingStrategy::offset(0), 1, false),
        metadata,
        DeferredPollContext {
            session: None,
            user_id: 0,
            credential: None,
            quota: PollQuotaIdentity::Session(7),
            primary: true,
        },
        iggy_common::DeferredPollOptions::default(),
    )
}

fn insert_built_partition(
    owner: &PollTestShard,
    bus: &PollTestBus,
    namespace: IggyNamespace,
    created_revision: u64,
) {
    let consensus = VsrConsensus::new(
        1,
        0,
        3,
        namespace.inner(),
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        partitions_config().segment_size,
    );
    partition.set_created_revision(created_revision);
    owner.enqueue_reconcile_op(ReconcileOp::InsertOwned {
        namespace,
        partition: Box::new(partition),
        epoch: created_revision,
    });
    owner.apply_reconcile_ops();
}

/// The first step of a teardown by the reconciler: the tombstone and the
/// removal of the routing row, before the disk delete.
fn begin_teardown(owner: &PollTestShard, namespace: IggyNamespace) {
    owner.plane.partitions().tombstone(namespace);
    owner.shards_table.remove(&namespace);
}

/// The last step of that teardown, through the op that drops the partition.
fn confirm_remove(owner: &PollTestShard, namespace: IggyNamespace) {
    owner.enqueue_reconcile_op(ReconcileOp::ConfirmRemove { namespace });
    owner.apply_reconcile_ops();
}

/// Remove the fixture's resident batches to select disk dispatch without
/// creating files. Captured tasks must stay unpolled: these tests establish
/// admission behavior, while real disk reads are covered in partition tests.
pub(super) fn evict_messages_for_disk_dispatch(owner: &PollTestShard, namespace: IggyNamespace) {
    let partitions = owner.plane.partitions();
    let partition = partitions
        .remove(&namespace)
        .expect("fixture partition exists");
    partition.log.journal().inner.evict_prefix(usize::MAX);
    assert!(
        partition
            .log
            .journal()
            .inner
            .oldest_resident_offset()
            .is_none()
    );
    partitions.insert(namespace, partition);
}

fn message_offsets(fragments: &PollFragments) -> Vec<u64> {
    // A partial batch has separate header and payload fragments. Reassemble
    // the fixture's single batch before decoding its actual message offsets.
    let bytes: Vec<u8> = fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&bytes).expect("decode polled batch");
    batch
        .iter()
        .map(|message| batch.header.base_offset + u64::from(message.header.offset_delta))
        .collect()
}

type PollTestShard = IggyShard<PollTestBus, (), (), PollTestMetadata, PapayaShardsTable>;

/// Commit four messages at offsets 0 through 3 while retaining their bytes in
/// the resident journal. The regression controls completion acceptance, so
/// setup needs neither disk files nor a running shard pump.
#[allow(clippy::future_not_send)]
async fn owner_with_messages(bus: &PollTestBus, namespace: IggyNamespace) -> PollTestShard {
    let shard_id = ShardId::new(0);
    let (partition, config) = partition_with_messages(bus, namespace, &["x"; 4]).await;
    let partitions = IggyPartitions::new(shard_id, config);
    partitions.insert(namespace, partition);
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None);
    let routes = PapayaShardsTable::new();
    routes.insert(namespace, PartitionLocation::new(shard_id, 0));
    PollTestShard::without_inbox(
        ShardIdentity::new(0, "poll-timeout-test".to_string()),
        bus.clone(),
        metadata,
        partitions,
        routes,
        PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus.clone()),
    )
}

pub type CapturedTask = Pin<Box<dyn Future<Output = ()>>>;

/// The first configured timer expires on demand; other timers stay pending.
/// Detached tasks are captured so dispatch tests can observe admission without
/// executing their synthetic disk plans.
#[derive(Clone, Default)]
pub struct PollTestBus {
    pub(super) now: Rc<Cell<u64>>,
    next_timeout: Rc<RefCell<Option<oneshot::Receiver<()>>>>,
    /// The budget of each timer when it is not `PARTITION_READ_TIMEOUT`.
    read_budget: Rc<Cell<Option<Duration>>>,
    pub(crate) spawned_tasks: Rc<RefCell<Vec<CapturedTask>>>,
}

#[allow(clippy::future_not_send)]
impl MessageBus for PollTestBus {
    fn monotonic_micros(&self) -> u64 {
        self.now.get()
    }

    fn spawn(&self, future: impl Future<Output = ()> + 'static) {
        self.spawned_tasks.borrow_mut().push(Box::pin(future));
    }

    fn sleep(&self, duration: Duration) -> impl Future<Output = ()> {
        assert_eq!(
            duration,
            self.read_budget.get().unwrap_or(PARTITION_READ_TIMEOUT)
        );
        let timeout = self.next_timeout.borrow_mut().take();
        async move {
            if let Some(timeout) = timeout {
                timeout
                    .await
                    .expect("test controls when the timeout expires");
            } else {
                std::future::pending::<()>().await;
            }
        }
    }

    async fn send_to_client(
        &self,
        _client_id: u128,
        _data: impl Into<BusMessage>,
    ) -> Result<(), SendError> {
        panic!("partition reads reply through their channel");
    }

    fn send_to_replica(
        &self,
        _replica: u8,
        _data: Frozen<MESSAGE_ALIGN>,
    ) -> impl Future<Output = Result<(), SendError>> {
        // This test observes local progress, without acknowledging replication.
        std::future::ready(Ok(()))
    }

    fn set_connection_lost_fn(&self, _f: ConnectionLostFn) {}
    fn set_replica_forward_fn(&self, _f: ReplicaForwardFn) {}
    fn track_background(&self, _handle: JoinHandle<()>) {}
}
