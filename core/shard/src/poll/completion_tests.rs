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

use std::collections::HashSet;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake};

use consensus::{
    ClientTable, MetadataHandle, PartitionsHandle, Sequencer, SessionEnd, build_reply_message_with,
};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::topics::{DeleteTopicRequest, PurgeTopicRequest};
use iggy_binary_protocol::{AckLevel, ReplyHeader, RoutedRequestHeader, WireConsumer};
use iggy_binary_protocol::{Command, Operation, PrepareHeader, WireEncode, WireIdentifier};
use iggy_common::{IggyError, IggyTimestamp, PollingStrategy};
use journal::prepare_journal::PrepareJournal;
use message_bus::IggyMessageBus;
use metadata::IggyMetadata;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::stm::StateMachine;
use metadata::stm::consumer_group::{ConsumerGroup, ConsumerGroupMember, JoinConsumerGroupRequest};
use metadata::stm::stream::{Partition, Stream, StreamsInner, Topic};
use metadata::stm::user::Users;
use partitions::{IggyPartitions, PartitionsConfig, PollingArgs, PollingConsumer};
use server_common::Message;
use server_common::send_messages::decode_batch_slice;
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::test_support::{PollTestMetadata, partition_with_messages};
use crate::metrics::ShardMetrics;
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    ConsumerAttachment, IggyShard, LifecycleFrame, PartitionConsensusConfig, PartitionRead,
    PartitionReadReply, Receiver, ReplicaTopology, ShardFrame, ShardIdentity, TaggedSender,
    channel, shard_channel,
};

#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_pending_attached_poll_when_metadata_changes_should_fence_only_affected_reads() {
    const CLIENT: u128 = 41;
    const OTHER_CLIENT: u128 = 42;
    const USER: u32 = 7;
    const GROUP: u64 = 7;
    #[derive(Debug, Clone, Copy)]
    enum Change {
        Logout,
        Leave,
        OtherLeave,
        MissingLeave,
        Rejoin,
        Purge,
        Delete,
    }
    for change in [
        Change::Logout,
        Change::Leave,
        Change::OtherLeave,
        Change::MissingLeave,
        Change::Rejoin,
        Change::Purge,
        Change::Delete,
    ] {
        let namespace = IggyNamespace::new(0, 0, 0);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
        let mut inner = StreamsInner::default();
        let mut stream = Stream::default();
        let mut topic = Topic::default();
        topic.partitions.push(Partition::new(
            0,
            namespace.inner(),
            IggyTimestamp::default(),
            1,
            0,
        ));
        for (group_id, client_id) in [(GROUP, CLIENT), (GROUP + 1, OTHER_CLIENT)] {
            let mut group = ConsumerGroup::new(group_id, Arc::from(format!("group-{group_id}")));
            group.members.insert(ConsumerGroupMember::new(0, client_id));
            group.rebalance_members(&[0]);
            topic.consumer_groups.insert(group_id, group);
        }
        stream.topics.insert(topic);
        inner.items.insert(stream);
        let metadata = PollTestMetadata::new((Users::default(), (inner.into(), ())));
        let (owner, _owner_sender) = owner_with_metadata(&bus, config, namespace, metadata);
        owner
            .shards_table
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 1));
        let partitions = owner.plane.partitions();
        partitions.insert(namespace, partition);
        let mut table = ClientTable::new(1);
        let header = PrepareHeader {
            client: CLIENT,
            user_id: USER,
            operation: Operation::Register,
            op: 1,
            ..Default::default()
        };
        table.commit_register(CLIENT, USER, build_reply_message_with(&header, 0, |_| {}));
        let (_stop, stop) = channel(1);
        let pump = owner.run_message_pump(stop, Arc::new(AtomicBool::new(false)));
        futures::pin_mut!(pump);

        let session = table.attach_session(CLIENT, header.op, USER).unwrap();
        let streams = owner.plane.metadata().mux_stm.streams();
        if matches!(change, Change::Purge) {
            let (reply, replies) = channel(1);
            owner
                .on_partition_read(
                    namespace,
                    PartitionRead::PollOnPrimary {
                        consumer: PollingConsumer::Consumer(USER as usize, 0),
                        args: PollingArgs {
                            strategy: PollingStrategy::first(),
                            count: 1,
                            auto_commit: false,
                        },
                        attachment: ConsumerAttachment {
                            session: session.clone(),
                            metadata: streams.poll_metadata(namespace, None, CLIENT).unwrap(),
                        },
                    },
                    reply,
                )
                .await;
            assert_single_message_reply(&replies);
        }
        let mut completions = Vec::new();
        for group in [None, Some(GROUP)] {
            let attachment = ConsumerAttachment {
                session: session.clone(),
                metadata: streams.poll_metadata(namespace, group, CLIENT).unwrap(),
            };
            let (reply, replies) = channel(1);
            let completion = owner
                .poll_completions
                .try_reserve(namespace, reply, Some(attachment))
                .expect("reserve the attached read");
            let plan = partitions
                .build_poll_snapshot(
                    &namespace,
                    group.map_or(PollingConsumer::Consumer(USER as usize, 0), |group| {
                        PollingConsumer::ConsumerGroup(usize::try_from(group).unwrap(), 0)
                    }),
                    &PollingArgs {
                        strategy: PollingStrategy::first(),
                        count: 1,
                        auto_commit: true,
                    },
                )
                .expect("the group has an unread message");
            let result = plan.execute_resident();
            completions.push((group, completion, replies, result));
        }
        match change {
            Change::Logout => {
                table.remove_client(CLIENT, USER, SessionEnd::Explicit);
            }
            Change::Leave => streams.remove_consumer_group_member(CLIENT, IggyTimestamp::default()),
            Change::OtherLeave => {
                streams.remove_consumer_group_member(OTHER_CLIENT, IggyTimestamp::default());
            }
            Change::MissingLeave => {
                streams.remove_consumer_group_member(OTHER_CLIENT + 1, IggyTimestamp::default());
            }
            Change::Rejoin => apply_poll_metadata(
                &owner,
                Operation::JoinConsumerGroup,
                JoinConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    group_id: WireIdentifier::numeric(u32::try_from(GROUP).unwrap()),
                    client_id: CLIENT,
                    in_flight: Vec::new(),
                }
                .to_bytes(),
            ),
            Change::Purge => apply_poll_metadata(
                &owner,
                Operation::PurgeTopic,
                PurgeTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                }
                .to_bytes(),
            ),
            Change::Delete => apply_poll_metadata(
                &owner,
                Operation::DeleteTopic,
                DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                }
                .to_bytes(),
            ),
        }
        for (group, completion, replies, result) in completions {
            let stale = matches!(change, Change::Logout | Change::Purge | Change::Delete)
                || (matches!(change, Change::Leave) && group.is_some());
            completion.complete(result);
            assert!(futures::poll!(pump.as_mut()).is_pending());
            let reply = replies
                .try_recv()
                .expect("the owner must validate the completion");
            if stale {
                assert!(
                    matches!(
                        reply,
                        PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
                    ),
                    "{change:?}, group {group:?}: stale authorization must reject, got {reply:?}"
                );
            } else {
                assert!(
                    matches!(
                        reply,
                        PartitionReadReply::Poll {
                            current_offset: 0,
                            ..
                        }
                    ),
                    "fresh authorization must accept, got {reply:?}"
                );
            }
            if group.is_some() {
                let expected = if stale {
                    (None, None)
                } else {
                    (Some(0), Some(0))
                };
                assert_eq!(
                    partitions.group_offset_state(&namespace, GROUP).unwrap(),
                    expected,
                    "{change:?}: completion must preserve group progress"
                );
            }
        }
        if matches!(change, Change::Purge) {
            let (reply, replies) = channel(1);
            owner
                .on_partition_read(
                    namespace,
                    PartitionRead::PollOnPrimary {
                        consumer: PollingConsumer::Consumer(USER as usize, 0),
                        args: PollingArgs {
                            strategy: PollingStrategy::first(),
                            count: 1,
                            auto_commit: true,
                        },
                        attachment: ConsumerAttachment {
                            session: session.clone(),
                            metadata: streams.poll_metadata(namespace, None, CLIENT).unwrap(),
                        },
                    },
                    reply,
                )
                .await;
            assert!(
                matches!(
                    replies.try_recv().unwrap(),
                    PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
                ),
                "new polls must wait until the committed purge is materialized"
            );
        }
    }
}

fn apply_poll_metadata(owner: &CompletionTestShard, operation: Operation, body: impl AsRef<[u8]>) {
    let body = body.as_ref();
    let size = size_of::<PrepareHeader>() + body.len();
    let mut message = Message::<PrepareHeader>::new(size);
    let header = PrepareHeader {
        command: Command::Prepare,
        operation,
        size: u32::try_from(size).unwrap(),
        ..Default::default()
    };
    message.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(body);
    let message = message.transmute_header::<PrepareHeader>(|_, target| *target = header);
    assert_eq!(
        owner.plane.metadata().mux_stm.update(message).unwrap().code,
        0
    );
}

#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_queued_offset_write_when_parent_or_history_changes_should_fence_admission() {
    const PARENT: u128 = 41;
    const DATA_CLIENT: u128 = 51;
    const USER: u32 = 7;
    const GROUP: u64 = 7;
    #[derive(Clone, Copy, Debug)]
    enum Change {
        None,
        PendingRevocation,
        Logout,
        Reregister,
        Leave,
        Purge,
        Delete,
        Replaced,
        Unmaterialized,
    }
    for change in [
        Change::None,
        Change::PendingRevocation,
        Change::Logout,
        Change::Reregister,
        Change::Leave,
        Change::Purge,
        Change::Delete,
        Change::Replaced,
        Change::Unmaterialized,
    ] {
        let namespace = IggyNamespace::new(0, 0, 1);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
        let mut inner = StreamsInner::default();
        let mut stream = Stream::default();
        let mut topic = Topic::default();
        for partition_id in [0, 1] {
            let namespace = IggyNamespace::new(0, 0, partition_id);
            topic.partitions.push(Partition::new(
                partition_id,
                namespace.inner(),
                IggyTimestamp::default(),
                1,
                0,
            ));
        }
        let mut group = ConsumerGroup::new(GROUP, Arc::from("offset-group"));
        group.members.insert(ConsumerGroupMember::new(0, PARENT));
        group.rebalance_members(&[0, 1]);
        if matches!(change, Change::PendingRevocation) {
            group
                .members
                .insert(ConsumerGroupMember::new(1, PARENT + 1));
            group.rebalance_cooperative(&[0, 1], &HashSet::from([1]), 1);
            assert_eq!(group.pending_revocations().len(), 1);
        }
        topic.consumer_groups.insert(GROUP, group);
        stream.topics.insert(topic);
        inner.items.insert(stream);
        let metadata = PollTestMetadata::new((Users::default(), (inner.into(), ())));
        let (owner, _sender) = owner_with_metadata(&bus, config.clone(), namespace, metadata);
        owner
            .shards_table
            .insert(namespace, PartitionLocation::new(ShardId::new(0), 1));
        let partitions = owner.plane.partitions();
        partitions.insert(namespace, partition);
        let mut table = ClientTable::new(1);
        let mut registration = PrepareHeader {
            client: PARENT,
            user_id: USER,
            operation: Operation::Register,
            op: 1,
            ..Default::default()
        };
        table.commit_register(
            PARENT,
            USER,
            build_reply_message_with(&registration, 0, |_| {}),
        );
        let streams = owner.plane.metadata().mux_stm.streams();
        if matches!(change, Change::PendingRevocation) {
            assert!(
                streams
                    .poll_metadata(namespace, Some(GROUP), PARENT)
                    .is_none()
            );
        }
        let attachment = ConsumerAttachment {
            session: table.attach_session(PARENT, registration.op, USER).unwrap(),
            metadata: streams
                .consumer_offset_metadata(namespace, Some(GROUP), PARENT)
                .unwrap(),
        };
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer_group(WireIdentifier::numeric(
                u32::try_from(GROUP).unwrap(),
            )),
            stream_id: WireIdentifier::numeric(0),
            topic_id: WireIdentifier::numeric(0),
            partition_id: Some(1),
            offset: 0,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let size = size_of::<RoutedRequestHeader>() + body.len();
        let mut request = Message::<RoutedRequestHeader>::new(size);
        request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
        let request = request.transmute_header::<RoutedRequestHeader>(|_, header| {
            *header = RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::StoreConsumerOffset,
                size: u32::try_from(size).unwrap(),
                cluster: 1,
                group: namespace.inner(),
                client: DATA_CLIENT,
                user_id: USER,
                session: 1,
                request: 1,
                ..Default::default()
            }
        });
        let ticket = owner
            .partition_submit_attached(namespace, request, Some(attachment))
            .unwrap();
        match change {
            Change::None | Change::PendingRevocation => {}
            Change::Logout => {
                table.remove_client(PARENT, USER, SessionEnd::Explicit);
            }
            Change::Reregister => {
                registration.op += 1;
                table.commit_register(
                    PARENT,
                    USER,
                    build_reply_message_with(&registration, 0, |_| {}),
                );
            }
            Change::Leave => streams.remove_consumer_group_member(PARENT, IggyTimestamp::default()),
            Change::Purge => apply_poll_metadata(
                &owner,
                Operation::PurgeTopic,
                PurgeTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                }
                .to_bytes(),
            ),
            Change::Delete => apply_poll_metadata(
                &owner,
                Operation::DeleteTopic,
                DeleteTopicRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                }
                .to_bytes(),
            ),
            Change::Replaced => {
                owner
                    .shards_table
                    .insert(namespace, PartitionLocation::new(ShardId::new(0), 2));
            }
            Change::Unmaterialized => {
                partitions.remove(&namespace);
            }
        }
        let (_stop, stop) = channel(1);
        let pump = owner.run_message_pump(stop, Arc::new(AtomicBool::new(false)));
        futures::pin_mut!(pump);
        assert!(futures::poll!(pump.as_mut()).is_pending());
        let admitted = matches!(change, Change::None | Change::PendingRevocation);
        if let Some(partition) = partitions.get_mut_by_ns(&namespace) {
            assert_eq!(
                partition.consensus().sequencer().current_sequence(),
                if admitted { 2 } else { 1 },
                "{change:?}"
            );
            if admitted {
                partition.consensus().advance_commit_max(2);
                partition.commit_journal(&config).await;
            }
        }
        let reply = owner
            .await_partition_submit(ticket)
            .await
            .unwrap()
            .try_into_typed::<ReplyHeader>()
            .unwrap();
        let header = reply.header();
        let status = if admitted {
            0
        } else if matches!(change, Change::Logout | Change::Reregister) {
            IggyError::StaleClient.as_code()
        } else {
            IggyError::TransientNotAccepted.as_code()
        };
        assert_eq!(header.status, status, "{change:?}");
        assert_eq!(
            header.client, DATA_CLIENT,
            "the parent must not replace the write's deduplication identity"
        );
        if !matches!(change, Change::Unmaterialized) {
            assert_eq!(
                partitions.group_offset_state(&namespace, GROUP).unwrap().1,
                admitted.then_some(0),
                "{change:?}"
            );
        }
    }
}

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
        .try_reserve(namespace, stale_reply_sender, None)
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
        .try_reserve(namespace, fresh_reply_sender, None)
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
            .try_reserve(namespace, reply_sender, None)
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
    owner_with_metadata(bus, config, namespace, PollTestMetadata::default())
}

fn owner_with_metadata(
    bus: &Rc<IggyMessageBus>,
    config: PartitionsConfig,
    namespace: IggyNamespace,
    metadata: PollTestMetadata,
) -> (CompletionTestShard, TaggedSender) {
    let shard_id = ShardId::new(0);
    let partitions = IggyPartitions::new(shard_id, config);
    let metadata = IggyMetadata::new(None, None, None, None, metadata, None);
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
        .try_reserve(namespace, reply_sender, None)
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
