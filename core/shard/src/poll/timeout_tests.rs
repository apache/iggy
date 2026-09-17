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
use std::time::Duration;

use consensus::PartitionsHandle;
use futures::channel::oneshot;
use iggy_common::{IggyError, PollingStrategy};
use message_bus::{
    BusMessage, ClientForwardFn, ConnectionLostFn, JoinHandle, MessageBus, ReplicaForwardFn,
    SendError,
};
use metadata::IggyMetadata;
use partitions::{IggyPartitions, PollFragments, PollingArgs, PollingConsumer};
use server_common::MESSAGE_ALIGN;
use server_common::iobuf::Frozen;
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::completion::PollCompletionLane;
use super::test_support::{PollTestMetadata, partition_with_messages};
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    IggyShard, LifecycleFrame, PARTITION_READ_TIMEOUT, PartitionConsensusConfig, PartitionRead,
    PartitionReadReply, ReplicaTopology, ShardFrame, ShardIdentity, channel, shard_channel,
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
            consumer,
            args: PollingArgs {
                strategy: PollingStrategy::next(),
                count: 3,
                auto_commit: true,
            },
        },
    );
    futures::pin_mut!(first_poll);
    assert!(futures::poll!(first_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace: requested_namespace,
        read:
            PartitionRead::Poll {
                consumer: requested_consumer,
                args,
            },
        reply,
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
            late_reply.try_send(PartitionReadReply::Ack),
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
            consumer,
            args: PollingArgs {
                strategy: PollingStrategy::next(),
                count: 4,
                auto_commit: false,
            },
        },
    );
    futures::pin_mut!(next_poll);
    assert!(futures::poll!(next_poll.as_mut()).is_pending());
    let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
        namespace,
        read,
        reply,
    }) = owner_inbox
        .try_recv()
        .expect("subsequent Next reached the owner inbox")
    else {
        panic!("expected the subsequent poll request");
    };
    owner.on_partition_read(namespace, read, reply).await;
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
    evict_messages_for_disk_dispatch(&owner, namespace).await;
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
                consumer,
                args: args.clone(),
            },
            reply,
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
                consumer,
                args: args.clone(),
            },
            reply,
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
        .on_partition_read(namespace, PartitionRead::Poll { consumer, args }, reply)
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
    evict_messages_for_disk_dispatch(&owner, namespace).await;
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
        .on_partition_read(namespace, PartitionRead::Poll { consumer, args }, reply)
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
    evict_messages_for_disk_dispatch(&owner, namespace).await;
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
        .on_partition_read(namespace, PartitionRead::Poll { consumer, args }, reply)
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

/// Remove the fixture's only resident batch to select disk dispatch without
/// creating files. Captured tasks must stay unpolled: these tests establish
/// admission behavior, while real disk reads are covered in partition tests.
#[allow(clippy::future_not_send)]
async fn evict_messages_for_disk_dispatch(owner: &PollTestShard, namespace: IggyNamespace) {
    let partitions = owner.plane.partitions();
    let partition = partitions
        .remove(&namespace)
        .expect("fixture partition exists");
    let retained = partition.log.journal().inner.evict_prefix(1).await;
    assert!(retained.is_empty(), "fixture has exactly one batch");
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

type CapturedTask = Pin<Box<dyn Future<Output = ()>>>;

/// The first configured timer expires on demand; other timers stay pending.
/// Detached tasks are captured so dispatch tests can observe admission without
/// executing their synthetic disk plans.
#[derive(Clone, Default)]
pub(super) struct PollTestBus {
    pub(super) now: Rc<Cell<u64>>,
    next_timeout: Rc<RefCell<Option<oneshot::Receiver<()>>>>,
    pub(super) spawned_tasks: Rc<RefCell<Vec<CapturedTask>>>,
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
        assert_eq!(duration, PARTITION_READ_TIMEOUT);
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
    fn set_client_forward_fn(&self, _f: ClientForwardFn) {}
    fn track_background(&self, _handle: JoinHandle<()>) {}
}
