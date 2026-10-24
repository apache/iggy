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

use std::cell::RefCell;
use std::collections::HashSet;
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake};

use consensus::{
    ClientTable, Consensus, LocalPipeline, MetadataHandle, PartitionsHandle,
    STATE_TRANSFER_MAX_DECODE_RETRIES, STATE_TRANSFER_MAX_STALL_RETRIES, Sequencer, StateArtifact,
    StateTransferStage, VsrConsensus, artifact_kind, build_reply_message_with,
    encode_state_manifest,
};
use iggy_binary_protocol::primitives::permissions::{WireGlobalPermissions, WirePermissions};
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::system::SessionIdentity;
use iggy_binary_protocol::requests::topics::DeleteTopicRequest;
use iggy_binary_protocol::requests::users::{CreateUserRequest, UpdatePermissionsRequest};
use iggy_binary_protocol::{
    AckLevel, CommitHeader, RepairRangeReplyHeader, ReplyHeader, RequestPreparesHeader,
    RequestStateChunkHeader, RequestStateTransferHeader, RoutedRequestHeader,
    StartViewChangeHeader, StateTransferTargetHeader, WireConsumer,
};
use iggy_binary_protocol::{
    Command, ConsensusHeader, Operation, PrepareHeader, WireEncode, WireIdentifier, WireName,
    WireOptions,
};
use iggy_common::{IggyError, IggyTimestamp, PartitionStats, PollingStrategy, UserStatus};
use journal::Journal;
use journal::prepare_journal::PrepareJournal;
use message_bus::IggyMessageBus;
use metadata::IggyMetadata;
use metadata::impls::metadata::{IggySnapshot, StreamsFrontend};
use metadata::stm::StateMachine;
use metadata::stm::consumer_group::{ConsumerGroup, ConsumerGroupMember, JoinConsumerGroupRequest};
use metadata::stm::stream::{Partition, Stream, StreamsInner, Topic};
use metadata::stm::user::Users;
use partitions::state_transfer::PartitionTransferSession;
use partitions::{
    IggyPartition, IggyPartitions, PartitionsConfig, PollingArgs, PollingConsumer, RepairSession,
};
use server_common::MESSAGE_ALIGN;
use server_common::Message;
use server_common::iobuf::{Frozen, Owned};
use server_common::send_messages::{
    COMMAND_HEADER_SIZE, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    decode_batch_slice,
};
use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};

use super::test_support::{
    PollTestMetadata, delete_history, install_group_owner, partition_with_messages,
    partition_with_messages_at_revision, partitions_config,
};
use crate::metrics::ShardMetrics;
use crate::shards_table::{PapayaShardsTable, ShardsTable};
use crate::{
    ConsumerAttachment, IggyShard, LifecycleFrame, NoopHost, PartitionConsensusConfig,
    PartitionRead, PartitionReadReply, REPAIR_CHUNK_MAX, Receiver, ReplicaTopology, ShardFrame,
    ShardIdentity, TaggedSender, channel, shard_channel,
};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::partitions::InstallConsumerGroupOwnerRequest;

#[compio::test]
async fn given_a_primary_with_a_deferred_view_change_when_reading_should_refuse_old_history() {
    let (mut fixture, _) = BoundPoll::activated().await;
    let partition = fixture.partition.as_mut().unwrap();
    let message = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
        .transmute_header(|_, header: &mut StartViewChangeHeader| {
            header.command = Command::StartViewChange;
            header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
            header.view = 1;
            header.replica = 1;
        });
    partition.defer_view_transition(server_common::MessageBag::StartViewChange(message));
    assert!(partition.consensus().is_primary());
    assert!(partition.consensus().is_normal());
    assert!(matches!(
        fixture.poll().await,
        PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
    ));
    assert_eq!(fixture.group_offsets(), (None, None));
    let (owner, ..) = fixture.served.as_ref().unwrap();
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .poll_metadata(fixture.namespace, None, BOUND_CLIENT)
        .unwrap();
    let (reply, replies) = channel(1);
    owner
        .on_partition_read(
            fixture.namespace,
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: None,
                metadata,
            },
            reply,
        )
        .await;
    assert!(matches!(
        replies.try_recv().unwrap(),
        PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
    ));
}

#[compio::test]
async fn given_deleted_history_when_retirement_read_names_old_incarnation_should_refuse_replacement()
 {
    const ORIGINAL_REVISION: u64 = 7;
    const REPLACEMENT_REVISION: u64 = 8;
    let bus = Rc::new(IggyMessageBus::new(0));
    let namespace = IggyNamespace::new(0, 0, 0);
    let config = partitions_config();
    let metadata = PollTestMetadata::new((Users::default(), (StreamsInner::default().into(), ())));
    let (owner, _sender) = owner_with_metadata(&bus, config.clone(), namespace, metadata);
    let identity = SessionIdentity {
        client_id: 1,
        session: 1,
        metadata_watermark: 100,
    };
    for revision in [ORIGINAL_REVISION, REPLACEMENT_REVISION] {
        let (mut partition, _) =
            partition_with_messages_at_revision(&bus, namespace, &["message"], revision).await;
        delete_history(&mut partition, &config, 10).await;
        owner.plane.partitions().remove(&namespace);
        owner.plane.partitions().insert(namespace, partition);
        let (reply, replies) = channel(1);
        owner
            .on_partition_read(
                namespace,
                PartitionRead::SessionRetired {
                    identity,
                    created_revision: ORIGINAL_REVISION,
                },
                reply,
            )
            .await;
        assert!(matches!(
            replies.try_recv().unwrap(),
            PartitionReadReply::SessionRetired(retired) if retired == (revision == ORIGINAL_REVISION)
        ));
    }
}

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
        Delete,
        RevokePermissions,
    }
    for (change, unbound) in [
        Change::Logout,
        Change::Leave,
        Change::OtherLeave,
        Change::MissingLeave,
        Change::Rejoin,
        Change::Delete,
        Change::RevokePermissions,
    ]
    .into_iter()
    .flat_map(|change| [(change, false), (change, true)])
    {
        let namespace = IggyNamespace::new(0, 0, 0);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
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
            let mut member = ConsumerGroupMember::new(0, client_id);
            member.session = Some(1);
            group.members.insert(member);
            group.rebalance_members(&topic.partitions, group_id, 1);
            let assignment = &group.assignments[&0];
            let pending = assignment.pending.unwrap();
            let installation = InstallConsumerGroupOwnerRequest {
                incarnation: assignment.incarnation,
                group_id,
                owner: pending.owner,
                metadata_op: pending.metadata_op,
            };
            let op = install_group_owner(&mut partition, &config, installation).await;
            assert!(group.complete_revocation(0, &installation, op));
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
        table
            .commit_register(
                CLIENT,
                USER,
                [0x5a; 32],
                build_reply_message_with(&header, 0, |_| {}),
            )
            .unwrap();
        let (_stop, stop) = channel(1);
        let pump = owner.run_message_pump(stop, Arc::new(AtomicBool::new(false)));
        futures::pin_mut!(pump);

        let session = table.attach_session(CLIENT, header.op, USER).unwrap();
        let streams = owner.plane.metadata().mux_stm.streams();
        let mut completions = Vec::new();
        for group in [None, Some(GROUP)]
            .into_iter()
            .filter(|group| !unbound || group.is_none())
        {
            let attachment = ConsumerAttachment {
                session: session.clone(),
                metadata: streams.poll_metadata(namespace, group, CLIENT).unwrap(),
            };
            let attachment = if unbound {
                super::PollAttachment::Unbound {
                    user_id: USER,
                    user_created_at: 0,
                    metadata: attachment.metadata,
                }
            } else {
                super::PollAttachment::Bound(attachment)
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
                    &PollingArgs::new(PollingStrategy::first(), 1, true),
                )
                .expect("the group has an unread message");
            let result = plan.execute_resident();
            completions.push((group, completion, replies, result));
        }
        match change {
            Change::Logout => {
                table.end_session(CLIENT, USER, table.get_epoch(CLIENT).unwrap(), 100);
            }
            Change::Leave => {
                streams.remove_consumer_group_member(CLIENT, IggyTimestamp::default(), 0);
            }
            Change::OtherLeave => {
                streams.remove_consumer_group_member(OTHER_CLIENT, IggyTimestamp::default(), 0);
            }
            Change::MissingLeave => {
                streams.remove_consumer_group_member(OTHER_CLIENT + 1, IggyTimestamp::default(), 0);
            }
            Change::Rejoin => apply_poll_metadata(
                &owner,
                Operation::JoinConsumerGroup,
                JoinConsumerGroupRequest {
                    stream_id: WireIdentifier::numeric(0),
                    topic_id: WireIdentifier::numeric(0),
                    group_id: WireIdentifier::numeric(u32::try_from(GROUP).unwrap()),
                    client_id: CLIENT,

                    session: 1,
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
            Change::RevokePermissions => apply_poll_metadata(
                &owner,
                Operation::UpdatePermissions,
                UpdatePermissionsRequest {
                    user_id: WireIdentifier::numeric(USER),
                    permissions: None,
                }
                .to_bytes(),
            ),
        }
        for (group, completion, replies, result) in completions {
            let stale = matches!(change, Change::Delete | Change::RevokePermissions)
                || (matches!(change, Change::Logout) && !unbound)
                || (matches!(change, Change::Leave) && group.is_some());
            completion.complete(result);
            assert!(futures::poll!(pump.as_mut()).is_pending());
            let reply = replies
                .try_recv()
                .expect("the owner must validate the completion");
            if stale {
                let expected = if matches!(change, Change::RevokePermissions) {
                    IggyError::Unauthorized
                } else {
                    IggyError::TransientNotAccepted
                };
                assert!(
                    matches!(
                        reply,
                        PartitionReadReply::Rejected(ref error) if *error == expected
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
            } else {
                let (stored, _) = partitions
                    .consumer_offset_read(&namespace, PollingConsumer::Consumer(USER as usize, 0))
                    .unwrap();
                assert_eq!(
                    stored,
                    (!stale).then_some(0),
                    "{change:?}: completion must preserve consumer progress"
                );
            }
        }
    }
}

fn apply_poll_metadata(owner: &CompletionTestShard, operation: Operation, body: impl AsRef<[u8]>) {
    let body = body.as_ref();
    let size = size_of::<PrepareHeader>() + body.len();
    let mut message = Message::<PrepareHeader>::new(size);
    let header = PrepareHeader {
        command: Command::Prepare,
        op: 100,
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

// Wider than u64, so a refusal that truncated it would name another client.
const BOUND_CLIENT: u128 = (1 << 64) + 41;
const OTHER_CLIENT: u128 = 42;
const BOUND_USER: u32 = 7;
const BOUND_GROUP: u64 = 7;

/// Another node's rebalance moved the partition to a newer owner, and this
/// node's metadata has not applied it. The stale owner's auto-commit would
/// otherwise be stamped with the new owner's generation.
#[compio::test]
async fn given_newer_installed_owner_when_stale_owner_polls_on_primary_should_refuse_without_progress()
 {
    let (mut fixture, activated) = BoundPoll::activated().await;
    fixture
        .install(InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: OTHER_CLIENT,
                session: 1,
                generation: activated.owner.generation + 1,
            },
            metadata_op: activated.metadata_op + 1,
            ..activated
        })
        .await;

    let reply = fixture.poll().await;
    fixture.commit().await;
    assert_eq!(
        fixture.group_offsets(),
        (None, None),
        "the stale owner's read must not move the new owner's progress"
    );
    assert_not_owned(&reply);
}

/// The last member left, so the partition installed an unassigned owner. An
/// automatic commit under it fails at apply on every replica.
#[compio::test]
async fn given_unassigned_installed_owner_when_stale_owner_polls_on_primary_should_refuse_without_fatal_commit()
 {
    let (mut fixture, activated) = BoundPoll::activated().await;
    fixture
        .install(InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: 0,
                session: 0,
                generation: activated.owner.generation + 1,
            },
            metadata_op: activated.metadata_op + 1,
            ..activated
        })
        .await;

    let reply = fixture.poll().await;
    fixture.commit().await;
    assert_eq!(fixture.group_offsets(), (None, None));
    assert_not_owned(&reply);
}

/// Metadata activates an owner only after its install committed, but this
/// replica can apply the install later. The owner retries until it does.
#[compio::test]
async fn given_unapplied_install_when_new_owner_polls_on_primary_should_retry_until_it_applies() {
    let (mut fixture, _) = BoundPoll::activated().await;
    let successor = fixture.reconnect_owner();

    let reply = fixture.poll().await;
    assert!(
        matches!(
            reply,
            PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
        ),
        "the partition has not applied generation {}, got {reply:?}",
        successor.owner.generation
    );
    assert_eq!(fixture.group_offsets(), (None, None));

    fixture.install(successor).await;
    let reply = fixture.poll().await;
    fixture.commit().await;
    assert_served(&reply, successor.owner.generation);
    assert_eq!(fixture.group_offsets(), (Some(0), Some(0)));
}

#[compio::test]
async fn given_installed_owner_when_it_polls_on_primary_should_serve_its_generation() {
    let (mut fixture, activated) = BoundPoll::activated().await;

    let reply = fixture.poll().await;
    fixture.commit().await;
    assert_served(&reply, activated.owner.generation);
    assert_eq!(fixture.group_offsets(), (Some(0), Some(0)));
}

#[compio::test]
async fn given_a_captured_offset_read_when_partition_is_replaced_should_not_read_new_history() {
    const REPLACEMENT_REVISION: u64 = 2;
    let (mut fixture, _) = BoundPoll::activated().await;
    fixture.poll().await;
    fixture.commit().await;
    let (owner, ..) = fixture.served.as_ref().unwrap();
    let namespace = fixture.namespace;
    let metadata = owner
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_offset_metadata(namespace, None, BOUND_CLIENT)
        .unwrap();

    for replaced in [false, true] {
        if replaced {
            let (replacement, _) = partition_with_messages_at_revision(
                &fixture.bus,
                namespace,
                &["new history"],
                REPLACEMENT_REVISION,
            )
            .await;
            owner.plane.partitions().remove(&namespace).unwrap();
            owner.plane.partitions().insert(namespace, replacement);
            owner.shards_table.insert(
                namespace,
                PartitionLocation::new(ShardId::new(0), REPLACEMENT_REVISION),
            );
        }
        for read in [
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: Some(PollingConsumer::ConsumerGroup(
                    usize::try_from(BOUND_GROUP).unwrap(),
                    0,
                )),
                metadata,
            },
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: None,
                metadata,
            },
            PartitionRead::ExternalGroupOffsetOnPrimary {
                group_id: u32::try_from(BOUND_GROUP).unwrap(),
                metadata,
            },
        ] {
            let has_offset = matches!(
                &read,
                PartitionRead::ConsumerOffsetOnPrimary {
                    consumer: Some(_),
                    ..
                }
            );
            let (reply, replies) = channel(1);
            owner.on_partition_read(namespace, read, reply).await;
            let reply = replies.try_recv().unwrap();
            if replaced {
                assert!(
                    matches!(
                        reply,
                        PartitionReadReply::Rejected(IggyError::TransientNotAccepted)
                    ),
                    "captured read must not read the replacement, got {reply:?}"
                );
            } else {
                let PartitionReadReply::ConsumerOffset { stored, .. } = reply else {
                    panic!("the original history should answer the read, got {reply:?}");
                };
                assert_eq!(stored, has_offset.then_some(0));
            }
        }
    }
}

/// Equal generations alone must not bind the poller: the partition's owner
/// at that generation is another client.
#[compio::test]
async fn given_other_client_installed_at_same_generation_when_owner_polls_on_primary_should_refuse()
{
    let (mut fixture, activated) = BoundPoll::activated_without_install().await;
    fixture
        .install(InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: OTHER_CLIENT,
                ..activated.owner
            },
            ..activated
        })
        .await;

    let reply = fixture.poll().await;
    fixture.commit().await;
    assert_eq!(fixture.group_offsets(), (None, None));
    assert_not_owned(&reply);
}

/// A normal backup names the primary of its view, so the poller or offset
/// writer goes there at once. A backup in transfer cannot vouch for the view
/// and stays transient.
#[compio::test]
async fn given_backup_when_polled_on_primary_should_name_the_primary_unless_transferring() {
    const BACKUP_REPLICA: u8 = 1;
    const REPLICA_COUNT: u8 = 3;
    const CREATED_REVISION: u64 = 1;
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = Rc::new(IggyMessageBus::new(0));
    let config = partitions_config();
    let consensus = VsrConsensus::new(
        1,
        BACKUP_REPLICA,
        REPLICA_COUNT,
        namespace.inner(),
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        config.segment_size,
    );
    partition.set_created_revision(CREATED_REVISION);
    let mut topic = Topic::default();
    topic.partitions.push(Partition::new(
        0,
        namespace.inner(),
        IggyTimestamp::default(),
        CREATED_REVISION,
        0,
    ));
    let mut stream = Stream::default();
    stream.topics.insert(topic);
    let mut inner = StreamsInner::default();
    inner.items.insert(stream);
    let metadata = PollTestMetadata::new((Users::default(), (inner.into(), ())));
    let (owner, _inbox) = owner_with_metadata(&bus, config, namespace, metadata);
    owner.shards_table.insert(
        namespace,
        PartitionLocation::new(ShardId::new(0), CREATED_REVISION),
    );
    owner.plane.partitions().insert(namespace, partition);
    let mut table = ClientTable::new(1);
    let registration = PrepareHeader {
        client: BOUND_CLIENT,
        user_id: BOUND_USER,
        operation: Operation::Register,
        op: 1,
        ..Default::default()
    };
    table
        .commit_register(
            BOUND_CLIENT,
            BOUND_USER,
            [0x5a; 32],
            build_reply_message_with(&registration, 0, |_| {}),
        )
        .unwrap();

    let (reply, replies) = channel(1);
    owner
        .on_partition_read(namespace, PartitionRead::Primary, reply)
        .await;
    let reply = replies.try_recv().expect("the owner answers the read");
    assert!(
        matches!(
            reply,
            PartitionReadReply::Primary {
                view: 0,
                primary: 0
            }
        ),
        "the backup must name the primary of view 0, got {reply:?}"
    );

    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        if transferring {
            owner
                .plane
                .partitions()
                .with_partition(&namespace, |partition| {
                    partition.consensus().begin_state_transfer_await();
                })
                .unwrap();
        }
        let attachment = ConsumerAttachment {
            session: table.attach_session(BOUND_CLIENT, 1, BOUND_USER).unwrap(),
            metadata: owner
                .plane
                .metadata()
                .mux_stm
                .streams()
                .poll_metadata(namespace, None, BOUND_CLIENT)
                .expect("metadata names the partition"),
        };
        let metadata = attachment.metadata;
        for read in [
            PartitionRead::PollOnPrimary {
                consumer: PollingConsumer::Consumer(1, 0),
                args: PollingArgs::new(PollingStrategy::first(), 1, true),
                attachment,
            },
            PartitionRead::UnboundPollOnPrimary {
                consumer: PollingConsumer::Consumer(1, 0),
                args: PollingArgs::new(PollingStrategy::next(), 1, false),
                user_id: BOUND_USER,
                user_created_at: 1,
                metadata,
            },
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: Some(PollingConsumer::Consumer(1, 0)),
                metadata,
            },
            PartitionRead::ConsumerOffsetOnPrimary {
                consumer: None,
                metadata,
            },
            PartitionRead::ExternalGroupOffsetOnPrimary {
                group_id: 1,
                metadata,
            },
        ] {
            let (reply, replies) = channel(1);
            owner.on_partition_read(namespace, read, reply).await;
            let reply = replies.try_recv().expect("the owner answers the read");
            assert!(
                matches!(&reply, PartitionReadReply::Rejected(error) if *error == expected),
                "transferring {transferring}: expected {expected:?}, got {reply:?}"
            );
        }

        let attachment = ConsumerAttachment {
            session: table.attach_session(BOUND_CLIENT, 1, BOUND_USER).unwrap(),
            metadata: owner
                .plane
                .metadata()
                .mux_stm
                .streams()
                .consumer_offset_metadata(namespace, None, BOUND_CLIENT)
                .expect("metadata names the partition"),
        };
        assert_eq!(
            owner.validate_partition_attachment(&consumer_offset_store(namespace), &attachment),
            Err(expected),
            "transferring {transferring}: an attached offset write"
        );
    }
}

fn consumer_offset_store(namespace: IggyNamespace) -> Message<RoutedRequestHeader> {
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::numeric(1)),
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
        partition_id: Some(0),
        offset: 0,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut request = Message::<RoutedRequestHeader>::new(size);
    request.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
    request.transmute_header::<RoutedRequestHeader>(|_, header| {
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::StoreConsumerOffset,
            size: u32::try_from(size).unwrap(),
            group: namespace.inner(),
            client: BOUND_CLIENT,
            session: 1,
            request: 1,
            ..Default::default()
        }
    })
}

fn assert_not_owned(reply: &PartitionReadReply) {
    assert!(
        matches!(
            reply,
            PartitionReadReply::Rejected(IggyError::ConsumerGroupPartitionNotOwned(
                BOUND_CLIENT,
                0
            ))
        ),
        "the poller, client {BOUND_CLIENT}, is not the installed owner, got {reply:?}"
    );
}

fn assert_served(reply: &PartitionReadReply, generation: u64) {
    let PartitionReadReply::Poll {
        context,
        current_offset: 0,
        ..
    } = reply
    else {
        panic!("the installed owner must be served, got {reply:?}");
    };
    assert_eq!(context.owner_generation, generation);
}

/// Bound group polls of `BOUND_CLIENT`, driven through the owner's read gate
/// as a binary poll on the partition primary arrives. Metadata and the
/// partition apply owners independently, so either one can lag the other.
struct BoundPoll {
    namespace: IggyNamespace,
    bus: Rc<IggyMessageBus>,
    config: PartitionsConfig,
    group: ConsumerGroup,
    topic: Topic,
    partition: Option<Box<partitions::IggyPartition<Rc<IggyMessageBus>>>>,
    served: Option<(CompletionTestShard, TaggedSender, ClientTable)>,
}

#[allow(clippy::future_not_send)]
impl BoundPoll {
    /// Metadata and the partition both name `BOUND_CLIENT`.
    async fn activated() -> (Self, InstallConsumerGroupOwnerRequest) {
        let (mut fixture, installation) = Self::pending().await;
        let op = fixture.install(installation).await;
        assert!(fixture.group.complete_revocation(0, &installation, op));
        (fixture, installation)
    }

    /// Metadata names `BOUND_CLIENT`, and the partition installed no owner.
    async fn activated_without_install() -> (Self, InstallConsumerGroupOwnerRequest) {
        let (mut fixture, installation) = Self::pending().await;
        assert!(fixture.group.complete_revocation(0, &installation, 2));
        (fixture, installation)
    }

    async fn pending() -> (Self, InstallConsumerGroupOwnerRequest) {
        let namespace = IggyNamespace::new(0, 0, 0);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
        let mut topic = Topic::default();
        topic.partitions.push(Partition::new(
            0,
            namespace.inner(),
            IggyTimestamp::default(),
            partition.created_revision(),
            0,
        ));
        let mut group = ConsumerGroup::new(BOUND_GROUP, Arc::from("bound-group"));
        let mut member = ConsumerGroupMember::new(0, BOUND_CLIENT);
        member.session = Some(1);
        group.members.insert(member);
        group.rebalance_members(&topic.partitions, 1, 1);
        let pending = group.assignments[&0].pending.unwrap();
        let installation = InstallConsumerGroupOwnerRequest {
            incarnation: group.assignments[&0].incarnation,
            group_id: BOUND_GROUP,
            owner: pending.owner,
            metadata_op: pending.metadata_op,
        };
        let fixture = Self {
            namespace,
            bus,
            config,
            group,
            topic,
            partition: Some(Box::new(partition)),
            served: None,
        };
        (fixture, installation)
    }

    /// The owner reconnected with a new session. Metadata activates the new
    /// session's owner as if its install committed on another replica.
    fn reconnect_owner(&mut self) -> InstallConsumerGroupOwnerRequest {
        for (_, member) in &mut self.group.members {
            member.session = Some(2);
        }
        self.group.rebalance_members(&self.topic.partitions, 3, 2);
        let pending = self.group.assignments[&0].pending.unwrap();
        let installation = InstallConsumerGroupOwnerRequest {
            incarnation: self.group.assignments[&0].incarnation,
            group_id: BOUND_GROUP,
            owner: pending.owner,
            metadata_op: pending.metadata_op,
        };
        assert!(self.group.complete_revocation(0, &installation, 99));
        installation
    }

    async fn install(&mut self, installation: InstallConsumerGroupOwnerRequest) -> u64 {
        let config = self.config.clone();
        if let Some(partition) = self.partition.as_mut() {
            return install_group_owner(partition, &config, installation).await;
        }
        let (owner, ..) = self.served.as_ref().unwrap();
        let partition = owner
            .plane
            .partitions()
            .get_mut_by_ns(&self.namespace)
            .unwrap();
        install_group_owner(partition, &config, installation).await
    }

    /// Freeze metadata on the first poll, then serve every poll from it.
    async fn poll(&mut self) -> PartitionReadReply {
        if let Some(partition) = self.partition.take() {
            let mut stream = Stream::default();
            let mut topic = self.topic.clone();
            topic
                .consumer_groups
                .insert(BOUND_GROUP, self.group.clone());
            stream.topics.insert(topic);
            let mut inner = StreamsInner::default();
            inner.items.insert(stream);
            let metadata = PollTestMetadata::new((Users::default(), (inner.into(), ())));
            let (owner, inbox) =
                owner_with_metadata(&self.bus, self.config.clone(), self.namespace, metadata);
            owner.shards_table.insert(
                self.namespace,
                PartitionLocation::new(ShardId::new(0), partition.created_revision()),
            );
            owner.plane.partitions().insert(self.namespace, *partition);
            let mut table = ClientTable::new(1);
            let registration = PrepareHeader {
                client: BOUND_CLIENT,
                user_id: BOUND_USER,
                operation: Operation::Register,
                op: 1,
                ..Default::default()
            };
            table
                .commit_register(
                    BOUND_CLIENT,
                    BOUND_USER,
                    [0x5a; 32],
                    build_reply_message_with(&registration, 0, |_| {}),
                )
                .unwrap();
            self.served = Some((owner, inbox, table));
        }
        let (owner, _, table) = self.served.as_mut().unwrap();
        let attachment = ConsumerAttachment {
            session: table.attach_session(BOUND_CLIENT, 1, BOUND_USER).unwrap(),
            metadata: owner
                .plane
                .metadata()
                .mux_stm
                .streams()
                .poll_metadata(self.namespace, Some(BOUND_GROUP), BOUND_CLIENT)
                .expect("metadata names the poller as the group's owner"),
        };
        let (reply, replies) = channel(1);
        owner
            .on_partition_read(
                self.namespace,
                PartitionRead::PollOnPrimary {
                    consumer: PollingConsumer::ConsumerGroup(
                        usize::try_from(BOUND_GROUP).unwrap(),
                        0,
                    ),
                    args: PollingArgs::new(PollingStrategy::first(), 1, true),
                    attachment,
                },
                reply,
            )
            .await;
        replies.try_recv().expect("the owner answers the read")
    }

    /// Commit what the poll prepared. A store that apply refuses would make
    /// every replica fatal, since each applies the same op to the same owner.
    async fn commit(&self) {
        let (owner, ..) = self.served.as_ref().unwrap();
        let partition = owner
            .plane
            .partitions()
            .get_mut_by_ns(&self.namespace)
            .unwrap();
        let op = partition.consensus().sequencer().current_sequence();
        partition.consensus().advance_commit_max(op);
        partition.commit_journal(&self.config).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    }

    fn group_offsets(&self) -> (Option<u64>, Option<u64>) {
        let (owner, ..) = self.served.as_ref().unwrap();
        owner
            .plane
            .partitions()
            .group_offset_state(&self.namespace, BOUND_GROUP)
            .unwrap()
    }
}

/// A replica applies the delete fence from its partition log, and its metadata
/// can apply the delete later. Until then that metadata authorizes reads of the
/// old incarnation, on a backup or on a primary elected after the fence. The
/// partition refuses them before reading, and refuses a read captured before
/// the fence when it completes.
#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_deleted_history_when_metadata_still_lists_the_partition_should_refuse_every_poll() {
    const CLIENT: u128 = 41;
    const USER: u32 = 7;
    const DELETE_METADATA_OP: u64 = 10;
    let namespace = IggyNamespace::new(0, 0, 0);
    let bus = Rc::new(IggyMessageBus::new(0));
    let (partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
    let created_revision = partition.created_revision();
    let mut topic = Topic::default();
    topic.partitions.push(Partition::new(
        0,
        namespace.inner(),
        IggyTimestamp::default(),
        created_revision,
        0,
    ));
    let mut stream = Stream::default();
    stream.topics.insert(topic);
    let mut inner = StreamsInner::default();
    inner.items.insert(stream);
    let metadata = PollTestMetadata::new((Users::default(), (inner.into(), ())));
    let (owner, _owner_sender) = owner_with_metadata(&bus, config.clone(), namespace, metadata);
    owner.shards_table.insert(
        namespace,
        PartitionLocation::new(ShardId::new(0), created_revision),
    );
    let partitions = owner.plane.partitions();
    partitions.insert(namespace, partition);
    let mut table = ClientTable::new(1);
    let registration = PrepareHeader {
        client: CLIENT,
        user_id: USER,
        operation: Operation::Register,
        op: 1,
        ..Default::default()
    };
    table
        .commit_register(
            CLIENT,
            USER,
            [0x5a; 32],
            build_reply_message_with(&registration, 0, |_| {}),
        )
        .unwrap();
    let streams = owner.plane.metadata().mux_stm.streams();
    let consumer = PollingConsumer::Consumer(USER as usize, 0);
    let first = |auto_commit| PollingArgs::new(PollingStrategy::first(), 1, auto_commit);

    let (late_reply, late_replies) = channel(1);
    let late = owner
        .poll_completions
        .try_reserve(namespace, late_reply, None)
        .expect("reserve the read before the fence");
    let late_result = partitions
        .build_poll_snapshot(&namespace, consumer, &first(false))
        .expect("the partition holds a message")
        .execute_resident();
    let fence_op = delete_history(
        partitions
            .get_mut_by_ns(&namespace)
            .expect("partition exists"),
        &config,
        DELETE_METADATA_OP,
    )
    .await;
    late.complete(late_result);
    let completion = owner
        .poll_completions
        .try_recv()
        .expect("the late read reached its owner");
    owner.on_poll_completed(*completion).await;
    let reply = late_replies.try_recv();
    assert!(
        matches!(
            reply,
            Ok(PartitionReadReply::Rejected(IggyError::HistoryUnavailable))
        ),
        "a read captured before the fence must not return the deleted history, got {reply:?}"
    );

    for disk in [false, true] {
        if disk {
            partitions
                .with_partition(&namespace, |partition| {
                    partition.log.journal().inner.evict_prefix(1);
                })
                .expect("partition exists");
            assert!(
                partitions
                    .build_poll_snapshot(&namespace, consumer, &first(false))
                    .expect("partition exists")
                    .needs_off_pump_io()
            );
        }
        for auto_commit in [false, true] {
            let metadata = streams
                .poll_metadata(namespace, None, CLIENT)
                .expect("metadata still lists the partition");
            let reads = [
                (
                    "Poll",
                    PartitionRead::Poll {
                        consumer,
                        args: first(auto_commit),
                        metadata: Some(metadata),
                    },
                ),
                (
                    "PollOnPrimary",
                    PartitionRead::PollOnPrimary {
                        consumer,
                        args: first(auto_commit),
                        attachment: ConsumerAttachment {
                            session: table.attach_session(CLIENT, 1, USER).unwrap(),
                            metadata,
                        },
                    },
                ),
                (
                    "ConsumerOffsetOnPrimary",
                    PartitionRead::ConsumerOffsetOnPrimary {
                        consumer: Some(consumer),
                        metadata,
                    },
                ),
                (
                    "MissingGroupOffsetOnPrimary",
                    PartitionRead::ConsumerOffsetOnPrimary {
                        consumer: None,
                        metadata,
                    },
                ),
                (
                    "ExternalGroupOffsetOnPrimary",
                    PartitionRead::ExternalGroupOffsetOnPrimary {
                        group_id: 1,
                        metadata,
                    },
                ),
            ];
            for (shape, read) in reads {
                let (reply, replies) = channel(1);
                owner.on_partition_read(namespace, read, reply).await;
                let reply = replies.try_recv();
                assert!(
                    matches!(
                        reply,
                        Ok(PartitionReadReply::Rejected(IggyError::HistoryUnavailable))
                    ),
                    "{shape}, disk {disk}, auto_commit {auto_commit}: the read must be refused \
                     before reading, got {reply:?}"
                );
            }
        }
    }
    let (stored, _) = partitions
        .consumer_offset_read(&namespace, consumer)
        .expect("partition exists");
    assert_eq!(stored, None);
    partitions
        .with_partition(&namespace, |partition| {
            let consensus = partition.consensus();
            assert_eq!(consensus.sequencer().current_sequence(), fence_op);
            assert!(consensus.pipeline_is_empty());
            assert_eq!(consensus.request_queue_len(), 0);
        })
        .expect("partition exists");
}

#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_queued_offset_write_when_parent_or_history_changes_should_fence_admission() {
    const PARENT: u128 = 41;
    const DATA_CLIENT: u128 = PARENT;
    const USER: u32 = 7;
    const GROUP: u64 = 7;
    const OWNER_OP: u64 = 10;
    #[derive(Clone, Copy, Debug)]
    enum Change {
        None,
        PendingRevocation,
        Logout,
        SessionReplacement,
        Leave,
        Delete,
        Replaced,
        Unmaterialized,
        Materialized,
        LogoutWhileParked,
    }
    for change in [
        Change::None,
        Change::PendingRevocation,
        Change::Logout,
        Change::SessionReplacement,
        Change::Leave,
        Change::Delete,
        Change::Replaced,
        Change::Unmaterialized,
        Change::Materialized,
        Change::LogoutWhileParked,
    ] {
        let namespace = IggyNamespace::new(0, 0, 1);
        let bus = Rc::new(IggyMessageBus::new(0));
        let (mut partition, config) = partition_with_messages(&bus, namespace, &["message"]).await;
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
        let mut member = ConsumerGroupMember::new(0, PARENT);
        member.session = Some(1);
        group.members.insert(member);
        group.rebalance_members(&topic.partitions, OWNER_OP, 1);
        for partition_id in [0, 1] {
            let assignment = &group.assignments[&partition_id];
            let pending = assignment.pending.unwrap();
            let installation = InstallConsumerGroupOwnerRequest {
                incarnation: assignment.incarnation,
                group_id: GROUP,
                owner: pending.owner,
                metadata_op: pending.metadata_op,
            };
            let op = if partition_id == 1 {
                install_group_owner(&mut partition, &config, installation).await
            } else {
                2
            };
            assert!(group.complete_revocation(partition_id, &installation, op));
        }
        if matches!(change, Change::PendingRevocation) {
            let mut member = ConsumerGroupMember::new(1, PARENT + 1);
            member.session = Some(2);
            group.members.insert(member);
            group.rebalance_members(&topic.partitions, 11, 2);
            assert!(group.assignments[&1].pending.is_some());
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
        table
            .commit_register(
                PARENT,
                USER,
                [0x5a; 32],
                build_reply_message_with(&registration, 0, |_| {}),
            )
            .unwrap();
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
                partition_incarnation: 1,
                owner_generation: OWNER_OP,
                metadata_watermark: OWNER_OP,
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
            Change::None
            | Change::PendingRevocation
            | Change::Unmaterialized
            | Change::Materialized
            | Change::LogoutWhileParked => {}
            Change::Logout => {
                table.end_session(PARENT, USER, table.get_epoch(PARENT).unwrap(), 100);
            }
            Change::SessionReplacement => {
                assert!(table.end_session(PARENT, USER, registration.op, registration.op + 1));
                assert!(table.finalize_session(
                    iggy_binary_protocol::requests::system::SessionIdentity {
                        client_id: PARENT,
                        session: registration.op,
                        metadata_watermark: registration.op + 1,
                    }
                ));
                registration.op += 1;
                table
                    .commit_register(
                        PARENT,
                        USER,
                        [0x5a; 32],
                        build_reply_message_with(&registration, 0, |_| {}),
                    )
                    .unwrap();
            }
            Change::Leave => {
                streams.remove_consumer_group_member(PARENT, IggyTimestamp::default(), 0);
            }
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
        }
        let parked = matches!(
            change,
            Change::Unmaterialized | Change::Materialized | Change::LogoutWhileParked
        );
        let removed_partition = parked.then(|| partitions.remove(&namespace).unwrap());
        let (_stop, stop) = channel(1);
        let pump = owner.run_message_pump(stop, Arc::new(AtomicBool::new(false)));
        futures::pin_mut!(pump);
        assert!(futures::poll!(pump.as_mut()).is_pending());
        if parked {
            assert_eq!(owner.parked_frame_count(namespace), 1, "{change:?}");
            if matches!(change, Change::Unmaterialized) {
                for _ in 0..=crate::MAX_PARKED_PASSES {
                    owner.age_parked_partition_frames(namespace);
                }
                assert_eq!(owner.parked_frame_count(namespace), 0);
            } else {
                if matches!(change, Change::LogoutWhileParked) {
                    assert!(table.end_session(PARENT, USER, registration.op, 100));
                }
                partitions.insert(namespace, removed_partition.unwrap());
                assert!(owner.redispatch_parked_frames(namespace, 1));
                assert!(futures::poll!(pump.as_mut()).is_pending());
            }
        }
        let admitted = matches!(
            change,
            Change::None | Change::PendingRevocation | Change::Materialized
        );
        if let Some(partition) = partitions.get_mut_by_ns(&namespace) {
            assert_eq!(
                partition.consensus().sequencer().current_sequence(),
                if admitted { 3 } else { 2 },
                "{change:?}"
            );
            if admitted {
                partition.consensus().advance_commit_max(3);
                partition.commit_journal(&config).await;
            }
        }
        let futures::future::Either::Left((reply, _)) = futures::future::select(
            Box::pin(owner.await_partition_submit(ticket)),
            pump.as_mut(),
        )
        .await
        else {
            panic!("pump stopped before the admitted offset write completed");
        };
        let reply = reply.unwrap().try_into_typed::<ReplyHeader>().unwrap();
        let header = reply.header();
        let status = if admitted {
            0
        } else if matches!(
            change,
            Change::Logout | Change::SessionReplacement | Change::LogoutWhileParked
        ) {
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
    let (mut old_partition, config) = partition_with_messages(&bus, namespace, &old_payloads).await;
    install_test_group_owner(&mut old_partition, &config, group_id).await;
    let (owner, _owner_sender) = owner_with_inbox(&bus, config, namespace);
    let partitions = owner.plane.partitions();
    partitions.insert(namespace, old_partition);
    let poll_args = PollingArgs::new(PollingStrategy::offset(0), 3, true);

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
    let (mut replacement, replacement_config) =
        partition_with_messages_at_revision(&bus, namespace, &fresh_payloads, 2).await;
    install_test_group_owner(&mut replacement, &replacement_config, group_id).await;
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
        ..
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
    let (mut partition, config) = partition_with_messages(&bus, namespace, &payloads).await;
    install_test_group_owner(&mut partition, &config, group_id).await;
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
                &PollingArgs::new(PollingStrategy::offset(offset), 1, false),
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

#[compio::test]
#[allow(clippy::too_many_lines)]
async fn given_metadata_transfer_when_target_is_unavailable_should_retry_only_unaccepted_transient()
{
    const GENERATION: u64 = 37;
    const PEER: u8 = 0;
    const RETRY_TICKS: u32 = 2;
    const SNAPSHOT_BYTES: &[u8] = b"snapshot contents";
    for (target_accepted, transient) in [(false, true), (false, false), (true, true)] {
        let bus = Rc::new(IggyMessageBus::new(0));
        let sent = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&sent);
        bus.set_replica_forward_fn(Box::new(move |peer, _, frame| {
            captured.borrow_mut().push((peer, frame));
            Ok(())
        }));
        assert!(bus.owner_table().try_claim(PEER, 1));
        let dir = tempfile::tempdir().unwrap();
        let journal = PrepareJournal::open(&dir.path().join("metadata.wal"), 0)
            .await
            .unwrap();
        let consensus = VsrConsensus::new(
            1,
            1,
            3,
            server_common::sharding::METADATA_GROUP,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        consensus.advance_commit_max(GENERATION);
        let metadata = IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            PollTestMetadata::default(),
            None,
        );
        let (owner, _sender) = owner_with_metadata_plane(
            &bus,
            partitions_config(),
            IggyNamespace::new(0, 0, 0),
            metadata,
        );
        owner.set_repair_retry_ticks(RETRY_TICKS);
        let consensus = owner.plane.metadata().consensus.as_ref().unwrap();
        consensus.set_state_transfer_stage(StateTransferStage::AwaitingTarget);
        owner.arm_metadata_transfer(consensus, PEER).await;
        let nonce = owner.metadata_transfer.borrow().as_ref().unwrap().nonce;
        assert_eq!(sent.borrow().len(), 1);
        sent.borrow_mut().clear();
        let descriptor = metadata_descriptor(
            nonce,
            GENERATION,
            &[
                StateArtifact::for_bytes(
                    artifact_kind::METADATA_SNAPSHOT,
                    GENERATION,
                    SNAPSHOT_BYTES,
                ),
                StateArtifact::for_bytes(artifact_kind::CLIENT_TABLE, GENERATION, b"client table"),
            ],
        );
        if target_accepted {
            owner.on_state_transfer_target(&descriptor).await;
            assert_eq!(
                consensus.state_transfer_stage(),
                StateTransferStage::Fetching
            );
            assert!(
                owner
                    .metadata_transfer
                    .borrow()
                    .as_ref()
                    .unwrap()
                    .target_accepted
            );
            assert_eq!(sent.borrow().len(), 1);
            sent.borrow_mut().clear();
        }
        let unavailable =
            Message::<StateTransferTargetHeader>::new(size_of::<StateTransferTargetHeader>())
                .transmute_header(|_, header: &mut StateTransferTargetHeader| {
                    header.command = Command::StateTransferTarget;
                    header.cluster = 1;
                    header.replica = PEER;
                    header.group = server_common::sharding::METADATA_GROUP;
                    header.nonce = nonce;
                    header.size = u32::try_from(size_of::<StateTransferTargetHeader>()).unwrap();
                    header.unavailable_transient = u8::from(transient);
                    header.seal();
                });

        if transient && !target_accepted {
            // One stall round short of the budget. A busy reply is no progress,
            // so it must not refill this: the next stall round still spends it.
            let charged = STATE_TRANSFER_MAX_STALL_RETRIES - 1;
            owner.metadata_transfer_attempts.set(charged);
            for _ in 0..=STATE_TRANSFER_MAX_STALL_RETRIES {
                owner
                    .metadata_transfer
                    .borrow_mut()
                    .as_mut()
                    .unwrap()
                    .idle_ticks = RETRY_TICKS - 1;

                owner.on_state_transfer_target(&unavailable).await;

                let transfer = owner.metadata_transfer.borrow();
                let session = transfer
                    .as_ref()
                    .expect("checkpoint contention preserves the session");
                assert_eq!(session.nonce, nonce);
                assert_eq!(session.peer, PEER);
                assert_eq!(session.idle_ticks, 0);
                assert!(!session.target_accepted);
                assert!(session.artifacts.is_empty());
                assert_eq!(owner.metadata_transfer_attempts.get(), charged);
                assert_eq!(
                    consensus.state_transfer_stage(),
                    StateTransferStage::AwaitingTarget
                );
                assert!(owner.metadata_repair.borrow().is_none());
                assert!(
                    sent.borrow().is_empty(),
                    "a busy reply must not trigger another request"
                );
            }
            owner.tick_metadata().await;
            assert!(
                sent.borrow().is_empty(),
                "descriptor retry waits for the configured interval"
            );
            owner.tick_metadata().await;
            assert_eq!(sent.borrow().len(), 1);
            let (peer, frame) = sent.borrow_mut().pop().unwrap();
            let request = Message::<RequestStateTransferHeader>::try_from(Owned::copy_from_slice(
                frame.as_slice(),
            ))
            .unwrap();
            assert_eq!(peer, PEER);
            assert_eq!(request.header().command, Command::RequestStateTransfer);
            assert_eq!(request.header().nonce, nonce);
            assert_eq!(
                owner.metadata_transfer_attempts.get(),
                STATE_TRANSFER_MAX_STALL_RETRIES
            );

            owner.on_state_transfer_target(&descriptor).await;

            assert_eq!(
                consensus.state_transfer_stage(),
                StateTransferStage::Fetching
            );
            let transfer = owner.metadata_transfer.borrow();
            let session = transfer.as_ref().unwrap();
            assert_eq!(session.nonce, nonce);
            assert!(session.target_accepted);
            assert_eq!(session.generation, GENERATION);
            assert_eq!(session.artifacts.len(), 2);
            assert_eq!(sent.borrow().len(), 1);
            let (peer, frame) = sent.borrow_mut().pop().unwrap();
            let request = Message::<RequestStateChunkHeader>::try_from(Owned::copy_from_slice(
                frame.as_slice(),
            ))
            .unwrap();
            assert_eq!(peer, PEER);
            assert_eq!(request.header().command, Command::RequestStateChunk);
            assert_eq!(request.header().nonce, nonce);
            assert_eq!(request.header().artifact, 0);
            assert_eq!(request.header().offset, 0);
            assert_eq!(request.header().len as usize, SNAPSHOT_BYTES.len());
        } else {
            owner.on_state_transfer_target(&unavailable).await;

            assert!(
                owner.metadata_transfer.borrow().is_none(),
                "accepted={target_accepted}, transient={transient}"
            );
            assert_eq!(consensus.state_transfer_stage(), StateTransferStage::Idle);
            let repair = owner.metadata_repair.borrow();
            let repair = repair
                .as_ref()
                .expect("unavailable history falls back to journal repair");
            assert_eq!(repair.peer, PEER);
            assert_eq!((repair.from_op, repair.to_op), (1, GENERATION));
            assert_eq!(sent.borrow().len(), 1);
            let (peer, frame) = sent.borrow_mut().pop().unwrap();
            let request = Message::<RequestPreparesHeader>::try_from(Owned::copy_from_slice(
                frame.as_slice(),
            ))
            .unwrap();
            assert_eq!(peer, PEER);
            assert_eq!(request.header().command, Command::RequestPrepares);
            assert_eq!(request.header().nonce, repair.nonce);
            assert_eq!(
                (request.header().from_op, request.header().to_op),
                (1, GENERATION)
            );
        }
    }
}

#[compio::test]
async fn given_accepted_descriptor_when_install_fails_should_exhaust_its_generation() {
    const GENERATION: u64 = 37;
    const UNKNOWN_ARTIFACT_KIND: u8 = u8::MAX;
    let bus = Rc::new(IggyMessageBus::new(0));
    let dir = tempfile::tempdir().unwrap();
    let journal = PrepareJournal::open(&dir.path().join("metadata.wal"), 0)
        .await
        .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    let metadata = IggyMetadata::new(
        Some(consensus),
        Some(journal),
        None,
        None,
        PollTestMetadata::default(),
        None,
    );
    let (owner, _sender) = owner_with_metadata_plane(
        &bus,
        partitions_config(),
        IggyNamespace::new(0, 0, 0),
        metadata,
    );
    let consensus = owner.plane.metadata().consensus.as_ref().unwrap();
    consensus.set_state_transfer_stage(StateTransferStage::AwaitingTarget);
    owner.arm_metadata_transfer(consensus, 0).await;
    for attempt in 1..=STATE_TRANSFER_MAX_DECODE_RETRIES + 1 {
        let nonce = owner.metadata_transfer.borrow().as_ref().unwrap().nonce;
        let target = invalid_metadata_descriptor(nonce, GENERATION, UNKNOWN_ARTIFACT_KIND);

        owner.on_state_transfer_target(&target).await;

        assert_eq!(
            owner.metadata_transfer_decode_failures.get(),
            Some((GENERATION, attempt)),
            "install failure must charge the accepted snapshot even before scanning its artifact"
        );
    }
    consensus.set_state_transfer_stage(StateTransferStage::AwaitingTarget);
    owner.arm_metadata_transfer(consensus, 0).await;
    let nonce = owner.metadata_transfer.borrow().as_ref().unwrap().nonce;
    owner
        .on_state_transfer_target(&invalid_metadata_descriptor(
            nonce,
            GENERATION,
            UNKNOWN_ARTIFACT_KIND,
        ))
        .await;
    assert!(
        !owner
            .metadata_transfer
            .borrow()
            .as_ref()
            .unwrap()
            .target_accepted,
        "an exhausted generation must be refused before allocating artifact buffers"
    );
    assert_eq!(
        consensus.state_transfer_stage(),
        StateTransferStage::AwaitingTarget
    );

    owner
        .on_state_transfer_target(&invalid_metadata_descriptor(
            nonce,
            GENERATION + 1,
            UNKNOWN_ARTIFACT_KIND,
        ))
        .await;
    assert_eq!(
        owner.metadata_transfer_decode_failures.get(),
        Some((GENERATION + 1, 1)),
        "a new checkpoint generation starts a fresh decode budget"
    );
}

const LAGGING_NONCE: u128 = 0x5eed;
const LAGGING_LOG_VIEW: u32 = 11;
const LAGGING_VIEW: u32 = 58;
const LAGGING_COMMIT_MAX: u64 = 53_052;
const GROUP_VIEW: u32 = 20;
const GROUP_COMMIT: u64 = 55_013;
const GROUP_PRIMARY: u8 = 2;

/// What a partition transfer descriptor answers.
#[derive(Clone, Copy)]
enum DescriptorReply {
    Offer,
    TransientRefusal,
    HardRefusal,
}

/// Replica 0 of three, awaiting a transfer descriptor from `GROUP_PRIMARY`. Its
/// log was last adopted in `LAGGING_LOG_VIEW` while elections nobody heard
/// raised its own view to `LAGGING_VIEW`, so `primary_index(view())` names a
/// different replica than the group's real primary.
fn lagging_replica_awaiting_target(namespace: IggyNamespace) -> CompletionTestShard {
    let owner = lagging_replica_in_cluster(namespace, 3);
    {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        assert_ne!(consensus.primary_index(LAGGING_VIEW), GROUP_PRIMARY);
        assert_eq!(consensus.primary_index(GROUP_VIEW), GROUP_PRIMARY);
    }
    owner
}

/// Replica 0 of `replica_count` in the lagging state above, with a transfer
/// session awaiting a descriptor from `GROUP_PRIMARY`.
fn lagging_replica_in_cluster(namespace: IggyNamespace, replica_count: u8) -> CompletionTestShard {
    let bus = Rc::new(IggyMessageBus::new(0));
    let config = partitions_config();
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None);
    let (owner, _sender) =
        owner_in_cluster(&bus, config.clone(), namespace, metadata, replica_count);
    let mut consensus = VsrConsensus::new(
        1,
        0,
        replica_count,
        namespace.inner(),
        bus,
        LocalPipeline::new(),
    );
    consensus.init();
    consensus.set_view(LAGGING_VIEW);
    consensus.set_log_view(LAGGING_LOG_VIEW);
    consensus.advance_commit_max(LAGGING_COMMIT_MAX);
    consensus.set_state_transfer_stage(StateTransferStage::AwaitingTarget);
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        config.segment_size,
    );
    arm_lagging_transfer(&mut partition, GROUP_PRIMARY);
    owner.plane.partitions().insert(namespace, partition);
    owner
}

fn arm_lagging_transfer<B, S>(partition: &mut IggyPartition<B, S>, peer: u8)
where
    B: message_bus::MessageBus,
{
    partition.transfer = Some(PartitionTransferSession {
        nonce: LAGGING_NONCE,
        peer,
        commit_op: 0,
        artifacts: Vec::new(),
        target_accepted: false,
        idle_ticks: 0,
    });
}

/// A descriptor from `GROUP_PRIMARY` at `GROUP_VIEW`: an offer at
/// `GROUP_COMMIT` when `offer` is set, a transient refusal otherwise.
fn group_primary_descriptor(
    namespace: IggyNamespace,
    offer: bool,
) -> Message<StateTransferTargetHeader> {
    let reply = if offer {
        DescriptorReply::Offer
    } else {
        DescriptorReply::TransientRefusal
    };
    lagging_descriptor(namespace, GROUP_PRIMARY, GROUP_VIEW, reply)
}

/// A descriptor answering the lagging replica's transfer session, sent by
/// `replica` at `view`.
fn lagging_descriptor(
    namespace: IggyNamespace,
    replica: u8,
    view: u32,
    reply: DescriptorReply,
) -> Message<StateTransferTargetHeader> {
    let manifest = if matches!(reply, DescriptorReply::Offer) {
        encode_state_manifest(&[StateArtifact::for_bytes(
            artifact_kind::SEGMENT_LOG,
            GROUP_COMMIT,
            b"segment",
        )])
    } else {
        Vec::new()
    };
    let size = size_of::<StateTransferTargetHeader>() + manifest.len();
    let mut message = Message::<StateTransferTargetHeader>::new(size);
    message.as_mut_slice()[size_of::<StateTransferTargetHeader>()..].copy_from_slice(&manifest);
    message.transmute_header(|_, header: &mut StateTransferTargetHeader| {
        header.command = Command::StateTransferTarget;
        header.cluster = 1;
        header.replica = replica;
        header.view = view;
        header.group = namespace.inner();
        header.nonce = LAGGING_NONCE;
        header.size = u32::try_from(size).unwrap();
        header.commit_max = GROUP_COMMIT;
        match reply {
            DescriptorReply::Offer => {
                header.available = 1;
                header.commit_op = GROUP_COMMIT;
            }
            DescriptorReply::TransientRefusal => header.unavailable_transient = 1,
            DescriptorReply::HardRefusal => {}
        }
        header.seal();
    })
}

/// A partition `Commit` heartbeat from `replica` as the primary of `view`.
fn primary_commit(
    namespace: IggyNamespace,
    replica: u8,
    view: u32,
    commit: u64,
) -> Message<CommitHeader> {
    Message::<CommitHeader>::new(size_of::<CommitHeader>()).transmute_header(
        |_, header: &mut CommitHeader| {
            header.command = Command::Commit;
            header.cluster = 1;
            header.replica = replica;
            header.view = view;
            header.commit = commit;
            header.group = namespace.inner();
            header.size = u32::try_from(size_of::<CommitHeader>()).unwrap();
            header.seal();
        },
    )
}

/// Restart the lagging replica's transfer against `peer` after a re-arm.
fn rearm_lagging_transfer_to(owner: &CompletionTestShard, namespace: IggyNamespace, peer: u8) {
    let partitions = owner.plane.partitions();
    let partition = partitions
        .get_mut_by_ns(&namespace)
        .expect("the partition stays registered");
    partition.transfer_rearm = None;
    arm_lagging_transfer(partition, peer);
}

/// The peer the lagging replica's next transfer attempt is scheduled against.
fn scheduled_rearm_peer(owner: &CompletionTestShard, namespace: IggyNamespace) -> u8 {
    owner
        .plane
        .partitions()
        .get_by_ns(&namespace)
        .expect("the partition stays registered")
        .transfer_rearm
        .expect("a refusal schedules a re-arm")
        .peer
}

/// A replica that needs partition state transfer cannot finish a view change,
/// and with `persisted` durability it cannot vote in one either, so its own
/// `view` climbs past the group's. Refusing the group primary's offer as "from
/// a replica behind this one" keeps it behind for good.
#[compio::test]
async fn given_the_primary_offer_when_this_replica_ratcheted_its_view_alone_should_accept_it() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_awaiting_target(namespace);

    owner
        .on_partition_state_transfer_target(&group_primary_descriptor(namespace, true))
        .await;

    let partitions = owner.plane.partitions();
    let session = partitions
        .get_by_ns(&namespace)
        .expect("the partition stays registered")
        .transfer
        .as_ref();
    assert!(
        session.is_some_and(|session| session.target_accepted),
        "the group primary's offer holds more committed state than this replica \
         knows, so it must be accepted"
    );
}

/// Only the primary serves a partition transfer, so a transient refusal from
/// it must be retried against it, not against the replica this node's
/// inflated view would name. Its heartbeats carry a view below this replica's,
/// which the partition view filter drops, yet they still name the primary.
#[compio::test]
async fn given_a_transient_refusal_from_the_primary_when_this_replica_ratcheted_its_view_alone_should_retry_the_primary()
 {
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_awaiting_target(namespace);
    owner
        .on_commit(&primary_commit(
            namespace,
            GROUP_PRIMARY,
            GROUP_VIEW,
            GROUP_COMMIT,
        ))
        .await;

    owner
        .on_partition_state_transfer_target(&group_primary_descriptor(namespace, false))
        .await;

    assert_eq!(
        scheduled_rearm_peer(&owner, namespace),
        GROUP_PRIMARY,
        "the next request must go to the primary this replica heard from"
    );
}

/// A refusal names no primary: without a heartbeat from the refusing peer,
/// staying on it may spend every round on a backup.
#[compio::test]
async fn given_a_transient_refusal_from_an_unheard_peer_when_lagging_should_rotate_away() {
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_awaiting_target(namespace);

    owner
        .on_partition_state_transfer_target(&group_primary_descriptor(namespace, false))
        .await;

    assert_ne!(scheduled_rearm_peer(&owner, namespace), GROUP_PRIMARY);
}

/// A backup can only refuse, so staying on it spends every round on nothing.
#[compio::test]
async fn given_a_transient_refusal_from_a_backup_of_the_group_view_when_lagging_should_rotate_away()
{
    const BACKUP: u8 = 1;
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_awaiting_target(namespace);
    assert_ne!(BACKUP, GROUP_PRIMARY);

    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            BACKUP,
            GROUP_VIEW,
            DescriptorReply::TransientRefusal,
        ))
        .await;

    assert_ne!(scheduled_rearm_peer(&owner, namespace), BACKUP);
}

/// The sender was primary of a view older than the one this replica's log was
/// adopted in, so the group has provably moved past it.
#[compio::test]
async fn given_a_transient_refusal_from_the_primary_of_a_view_below_log_view_when_lagging_should_rotate_away()
 {
    const STALE_VIEW: u32 = 8;
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_awaiting_target(namespace);
    let stale_primary = {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        assert!(STALE_VIEW < consensus.log_view());
        consensus.primary_index(STALE_VIEW)
    };

    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            stale_primary,
            STALE_VIEW,
            DescriptorReply::TransientRefusal,
        ))
        .await;

    assert_ne!(scheduled_rearm_peer(&owner, namespace), stale_primary);
}

/// Five replicas, so the primary this replica's inflated view names (3), the
/// group's primary (1) and the failed backup (2) are all distinct. A rotation
/// steered by the inflated view asks replica 3, which can only refuse.
#[compio::test]
async fn given_a_hard_refusal_from_a_backup_when_this_replica_ratcheted_its_view_alone_should_rotate_to_the_group_primary()
 {
    const GROUP_VIEW_OF_FIVE: u32 = 21;
    const FAILED_BACKUP: u8 = 2;
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_in_cluster(namespace, 5);
    let group_primary = {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        let group_primary = consensus.primary_index(GROUP_VIEW_OF_FIVE);
        let inflated_primary = consensus.primary_index(LAGGING_VIEW);
        assert_eq!(
            HashSet::from([0, group_primary, inflated_primary, FAILED_BACKUP]).len(),
            4
        );
        group_primary
    };
    owner
        .on_commit(&primary_commit(
            namespace,
            group_primary,
            GROUP_VIEW_OF_FIVE,
            GROUP_COMMIT,
        ))
        .await;
    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            group_primary,
            GROUP_VIEW_OF_FIVE,
            DescriptorReply::TransientRefusal,
        ))
        .await;
    assert_eq!(scheduled_rearm_peer(&owner, namespace), group_primary);
    {
        let partitions = owner.plane.partitions();
        let partition = partitions
            .get_mut_by_ns(&namespace)
            .expect("the partition stays registered");
        partition.transfer_rearm = None;
        arm_lagging_transfer(partition, FAILED_BACKUP);
    }

    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            FAILED_BACKUP,
            GROUP_VIEW_OF_FIVE,
            DescriptorReply::HardRefusal,
        ))
        .await;

    assert_eq!(
        scheduled_rearm_peer(&owner, namespace),
        group_primary,
        "the rotation must follow the primary the group was last heard from"
    );
}

/// A second lagging replica whose view climbed alone to a view it is primary
/// of refuses transiently, stamping that view. Taken as the primary, it would
/// pin every later re-arm to itself.
#[compio::test]
async fn given_a_lagging_peer_refusing_at_its_inflated_view_when_the_group_primary_was_heard_should_rearm_to_the_group_primary()
 {
    const GROUP_VIEW_OF_FIVE: u32 = 21;
    const INFLATED_PEER: u8 = 4;
    const INFLATED_VIEW: u32 = 59;
    const FAILED_BACKUP: u8 = 2;
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_in_cluster(namespace, 5);
    let group_primary = {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        assert_eq!(consensus.primary_index(INFLATED_VIEW), INFLATED_PEER);
        consensus.primary_index(GROUP_VIEW_OF_FIVE)
    };
    owner
        .on_commit(&primary_commit(
            namespace,
            group_primary,
            GROUP_VIEW_OF_FIVE,
            GROUP_COMMIT,
        ))
        .await;
    rearm_lagging_transfer_to(&owner, namespace, INFLATED_PEER);

    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            INFLATED_PEER,
            INFLATED_VIEW,
            DescriptorReply::TransientRefusal,
        ))
        .await;
    assert_eq!(scheduled_rearm_peer(&owner, namespace), group_primary);

    rearm_lagging_transfer_to(&owner, namespace, group_primary);
    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            group_primary,
            GROUP_VIEW_OF_FIVE,
            DescriptorReply::TransientRefusal,
        ))
        .await;
    assert_eq!(scheduled_rearm_peer(&owner, namespace), group_primary);

    rearm_lagging_transfer_to(&owner, namespace, FAILED_BACKUP);
    owner
        .on_partition_state_transfer_target(&lagging_descriptor(
            namespace,
            FAILED_BACKUP,
            GROUP_VIEW_OF_FIVE,
            DescriptorReply::HardRefusal,
        ))
        .await;
    assert_eq!(scheduled_rearm_peer(&owner, namespace), group_primary);
    assert_eq!(
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the partition stays registered")
            .consensus()
            .primary_hint(),
        Some(group_primary)
    );
}

/// The primary this replica heard from goes silent. Kept as the hint, it
/// would draw every rotation back to itself, so the ring walk would only ever
/// reach the next replica and never the new primary beyond it.
#[compio::test]
async fn given_the_heard_primary_failing_when_rotating_should_walk_the_ring_to_the_new_primary() {
    const DEAD_PRIMARY_VIEW: u32 = 21;
    const NEW_PRIMARY_VIEW: u32 = 23;
    let namespace = IggyNamespace::new(0, 0, 0);
    let owner = lagging_replica_in_cluster(namespace, 5);
    let (dead_primary, new_primary) = {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        (
            consensus.primary_index(DEAD_PRIMARY_VIEW),
            consensus.primary_index(NEW_PRIMARY_VIEW),
        )
    };
    assert_eq!((dead_primary, new_primary), (1, 3));
    owner
        .on_commit(&primary_commit(
            namespace,
            dead_primary,
            DEAD_PRIMARY_VIEW,
            GROUP_COMMIT,
        ))
        .await;

    let mut tried = Vec::new();
    let mut peer = dead_primary;
    for _ in 0..2 {
        rearm_lagging_transfer_to(&owner, namespace, peer);
        {
            let partitions = owner.plane.partitions();
            let partition = partitions
                .get_mut_by_ns(&namespace)
                .expect("the partition stays registered");
            owner
                .abandon_or_rearm_partition_transfer(partition, peer)
                .await;
        }
        peer = scheduled_rearm_peer(&owner, namespace);
        tried.push(peer);
    }

    assert_eq!(tried, vec![2, new_primary]);
}

/// Frames the bus forwarded, by destination replica.
type SentFrames = Rc<RefCell<Vec<(u8, Frozen<MESSAGE_ALIGN>)>>>;

/// A backup at view 1 of five, behind its own `commit_max`.
fn gap_stopped_backup(
    namespace: IggyNamespace,
    commit_max: u64,
) -> (CompletionTestShard, SentFrames) {
    let bus = Rc::new(IggyMessageBus::new(0));
    let sent = Rc::new(RefCell::new(Vec::new()));
    let captured = Rc::clone(&sent);
    bus.set_replica_forward_fn(Box::new(move |peer, _, frame| {
        captured.borrow_mut().push((peer, frame));
        Ok(())
    }));
    for peer in 1..5 {
        assert!(bus.owner_table().try_claim(peer, 1));
    }
    let config = partitions_config();
    let metadata = IggyMetadata::new(None, None, None, None, PollTestMetadata::default(), None);
    let (owner, _sender) = owner_in_cluster(&bus, config.clone(), namespace, metadata, 5);
    let mut consensus = VsrConsensus::new(1, 0, 5, namespace.inner(), bus, LocalPipeline::new());
    consensus.init();
    consensus.set_view(1);
    consensus.set_log_view(1);
    consensus.advance_commit_max(commit_max);
    let partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        config.segment_size,
    );
    owner.plane.partitions().insert(namespace, partition);
    owner.set_repair_gap_debounce_ticks(1);
    owner.set_repair_retry_ticks(1);
    (owner, sent)
}

/// Tick until the partition's repair session targets a peer other than
/// `previous`, and return it.
#[allow(clippy::future_not_send)]
async fn tick_until_repair_peer_changes(
    owner: &CompletionTestShard,
    namespace: IggyNamespace,
    previous: Option<u8>,
) -> u8 {
    let mut scratch = Vec::new();
    for _ in 0..64 {
        assert!(owner.tick_partitions(&mut scratch).await.is_none());
        scratch.clear();
        let peer = owner
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the partition stays registered")
            .repair
            .map(|session| session.peer);
        if let Some(peer) = peer
            && Some(peer) != previous
        {
            return peer;
        }
    }
    panic!("the repair session never moved off {previous:?}");
}

/// The gap-repair arm and the stall rotation both aim at the primary this
/// replica last heard from, which is not the primary of its own view.
#[compio::test]
async fn given_a_primary_heard_at_a_newer_view_when_the_partition_gap_stops_should_repair_from_it()
{
    const HEARD_VIEW: u32 = 3;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, _sent) = gap_stopped_backup(namespace, 5);
    let (own_view_primary, heard_primary) = {
        let partitions = owner.plane.partitions();
        let consensus = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus();
        (
            consensus.primary_index(consensus.view()),
            consensus.primary_index(HEARD_VIEW),
        )
    };
    assert_eq!((own_view_primary, heard_primary), (1, 3));
    let heartbeat = primary_commit(namespace, heard_primary, HEARD_VIEW, 5);
    owner.on_commit(&heartbeat).await;

    let armed = tick_until_repair_peer_changes(&owner, namespace, None).await;
    assert_eq!(armed, heard_primary, "the gap arm asks the heard primary");

    let rotated = tick_until_repair_peer_changes(&owner, namespace, Some(armed)).await;
    assert_eq!(
        rotated, own_view_primary,
        "a stalled heard primary is forgotten, leaving the primary of this view"
    );

    owner.on_commit(&heartbeat).await;
    let back = tick_until_repair_peer_changes(&owner, namespace, Some(rotated)).await;
    assert_eq!(
        back, heard_primary,
        "the stall rotation goes to the primary heard again"
    );
}

/// A gap-stopped metadata backup of a five-replica cluster at view 1.
#[allow(clippy::future_not_send)]
async fn gap_stopped_metadata_backup(commit_max: u64) -> (CompletionTestShard, tempfile::TempDir) {
    let bus = Rc::new(IggyMessageBus::new(0));
    bus.set_replica_forward_fn(Box::new(|_, _, _| Ok(())));
    for peer in 1..5 {
        assert!(bus.owner_table().try_claim(peer, 1));
    }
    let dir = tempfile::tempdir().unwrap();
    let journal = PrepareJournal::open(&dir.path().join("metadata.wal"), 0)
        .await
        .unwrap();
    let mut consensus = VsrConsensus::new(
        1,
        0,
        5,
        server_common::sharding::METADATA_GROUP,
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    consensus.set_view(1);
    consensus.set_log_view(1);
    consensus.advance_commit_max(commit_max);
    let metadata = IggyMetadata::new(
        Some(consensus),
        Some(journal),
        None,
        None,
        PollTestMetadata::default(),
        None,
    );
    let (owner, _sender) = owner_in_cluster(
        &bus,
        partitions_config(),
        IggyNamespace::new(0, 0, 0),
        metadata,
        5,
    );
    owner.set_repair_gap_debounce_ticks(1);
    owner.set_repair_retry_ticks(1);
    (owner, dir)
}

/// Tick until the metadata repair session targets a peer other than
/// `previous`, and return it.
#[allow(clippy::future_not_send)]
async fn tick_until_metadata_repair_peer_changes(
    owner: &CompletionTestShard,
    previous: Option<u8>,
) -> u8 {
    for _ in 0..64 {
        owner.tick_metadata().await;
        let peer = owner
            .metadata_repair
            .borrow()
            .as_ref()
            .map(|session| session.peer);
        if let Some(peer) = peer
            && Some(peer) != previous
        {
            return peer;
        }
    }
    panic!("the metadata repair session never moved off {previous:?}");
}

/// The metadata plane aims its gap-repair arm and stall rotation at the
/// primary it last heard from, as the partition plane does.
#[compio::test]
async fn given_a_primary_heard_at_a_newer_view_when_the_metadata_gap_stops_should_repair_from_it() {
    const HEARD_VIEW: u32 = 3;
    let (owner, _dir) = gap_stopped_metadata_backup(5).await;
    let (own_view_primary, heard_primary) = {
        let metadata = owner.plane.metadata();
        let consensus = metadata
            .consensus
            .as_ref()
            .expect("the fixture builds metadata consensus");
        (
            consensus.primary_index(consensus.view()),
            consensus.primary_index(HEARD_VIEW),
        )
    };
    assert_eq!((own_view_primary, heard_primary), (1, 3));
    let heartbeat = primary_commit(
        IggyNamespace::from_raw(server_common::sharding::METADATA_GROUP),
        heard_primary,
        HEARD_VIEW,
        5,
    );
    owner.on_commit(&heartbeat).await;

    let armed = tick_until_metadata_repair_peer_changes(&owner, None).await;
    assert_eq!(armed, heard_primary, "the gap arm asks the heard primary");

    let rotated = tick_until_metadata_repair_peer_changes(&owner, Some(armed)).await;
    assert_eq!(
        rotated, own_view_primary,
        "a stalled heard primary is forgotten, leaving the primary of this view"
    );

    owner.on_commit(&heartbeat).await;
    let back = tick_until_metadata_repair_peer_changes(&owner, Some(rotated)).await;
    assert_eq!(
        back, heard_primary,
        "the stall rotation goes to the primary heard again"
    );
}

/// The incarnation fence runs before the heartbeat names a primary, so a
/// stale incarnation's heartbeat cannot steer repair toward its replica.
#[compio::test]
async fn given_a_heartbeat_of_another_incarnation_when_it_names_a_newer_primary_should_keep_the_hint()
 {
    const HEARD_VIEW: u32 = 3;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, _sent) = gap_stopped_backup(namespace, 5);
    let primary_hint = || {
        owner
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition")
            .consensus()
            .primary_hint()
    };
    let (own_view_primary, heard_primary, hosted_incarnation) = {
        let partitions = owner.plane.partitions();
        let partition = partitions
            .get_by_ns(&namespace)
            .expect("the fixture registers the partition");
        let consensus = partition.consensus();
        (
            consensus.primary_index(consensus.view()),
            consensus.primary_index(HEARD_VIEW),
            partition.created_revision(),
        )
    };
    assert_ne!(own_view_primary, heard_primary);
    let stale = primary_commit(namespace, heard_primary, HEARD_VIEW, 5).transmute_header(
        |heartbeat, header: &mut CommitHeader| {
            *header = heartbeat;
            header.partition_incarnation = hosted_incarnation + 1;
            header.seal();
        },
    );

    owner.on_commit(&stale).await;
    assert_eq!(primary_hint(), Some(own_view_primary));

    owner
        .on_commit(&primary_commit(namespace, heard_primary, HEARD_VIEW, 5))
        .await;
    assert_eq!(primary_hint(), Some(heard_primary));
}

/// A floor holds the walk and the served chunk above it carries no message, so
/// nothing anchors the window. Pulling again from `commit_min + 1` would
/// re-serve that same chunk forever.
#[compio::test]
async fn given_a_floor_holding_the_walk_when_a_chunk_without_messages_lands_should_pull_the_next_chunk()
 {
    const PEER: u8 = 1;
    const NONCE: u128 = 0xf100;
    const FLOOR: u64 = 5;
    const FIRST_CHUNK_LAST: u64 = FLOOR + REPAIR_CHUNK_MAX;
    const TO_OP: u64 = FIRST_CHUNK_LAST + 10;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, sent) = gap_stopped_backup(namespace, TO_OP);
    {
        let partitions = owner.plane.partitions();
        let partition = partitions
            .get_mut_by_ns(&namespace)
            .expect("the fixture registers the partition");
        partition.repair = Some(RepairSession {
            nonce: NONCE,
            view: partition.consensus().view(),
            from_op: partition.consensus().commit_min() + 1,
            commit_to_op: TO_OP,
            fetch_to_op: TO_OP,
            floor: Some(FLOOR),
            peer: PEER,
            idle_ticks: 0,
            floor_pulled_from: 0,
            requested_from: Some(partition.consensus().commit_min() + 1),
        });
        for op in FLOOR + 1..=FIRST_CHUNK_LAST {
            partition
                .log
                .journal()
                .inner
                .append(control_prepare(namespace, op).into_frozen())
                .await
                .expect("journal append");
        }
    }
    let repair_done = |served_through: u64| {
        Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
            .transmute_header(|_, header: &mut RepairRangeReplyHeader| {
                header.command = Command::RepairDone;
                header.size = u32::try_from(size_of::<RepairRangeReplyHeader>()).unwrap();
                header.group = namespace.inner();
                header.cluster = 1;
                header.replica = PEER;
                header.nonce = NONCE;
                header.op = served_through;
                header.seal();
            })
    };

    owner
        .on_repair_range_reply(&repair_done(FIRST_CHUNK_LAST))
        .await;

    assert_eq!(
        drain_prepare_requests(&sent),
        vec![(PEER, FIRST_CHUNK_LAST + 1, TO_OP)]
    );

    owner
        .on_repair_range_reply(&repair_done(FIRST_CHUNK_LAST))
        .await;
    assert!(
        sent.borrow().is_empty(),
        "a reply that delivered nothing new leaves the rest to the stall retry"
    );

    let mut scratch = Vec::new();
    assert!(owner.tick_partitions(&mut scratch).await.is_none());
    assert_eq!(
        drain_prepare_requests(&sent),
        vec![(PEER, FIRST_CHUNK_LAST + 1, TO_OP)],
        "the stall retry starts past the resident chunk too"
    );
}

/// A floor holds the walk and every op up to `fetch_to_op` is resident, so the
/// pull start lies past the window. Only a `RepairDone` concludes the session.
/// No batch above the floor anchors it, so that conclusion is a refusal that
/// hands recovery to state transfer.
#[allow(clippy::future_not_send)]
async fn resident_window_backup(
    namespace: IggyNamespace,
    peer: u8,
    nonce: u128,
    floor: u64,
    to_op: u64,
) -> (CompletionTestShard, SentFrames) {
    resident_window_backup_with(namespace, peer, nonce, floor, to_op, |op| {
        control_prepare(namespace, op)
    })
    .await
}

/// [`resident_window_backup`] with the window's ops built by `prepare_at`.
#[allow(clippy::future_not_send)]
async fn resident_window_backup_with(
    namespace: IggyNamespace,
    peer: u8,
    nonce: u128,
    floor: u64,
    to_op: u64,
    prepare_at: impl Fn(u64) -> Message<PrepareHeader>,
) -> (CompletionTestShard, SentFrames) {
    let (owner, sent) = gap_stopped_backup(namespace, to_op);
    {
        let partitions = owner.plane.partitions();
        let partition = partitions
            .get_mut_by_ns(&namespace)
            .expect("the fixture registers the partition");
        partition.repair = Some(RepairSession {
            nonce,
            view: partition.consensus().view(),
            from_op: partition.consensus().commit_min() + 1,
            commit_to_op: to_op,
            fetch_to_op: to_op,
            floor: Some(floor),
            peer,
            idle_ticks: 0,
            floor_pulled_from: 0,
            requested_from: Some(partition.consensus().commit_min() + 1),
        });
        for op in floor + 1..=to_op {
            partition
                .log
                .journal()
                .inner
                .append(prepare_at(op).into_frozen())
                .await
                .expect("journal append");
        }
    }
    (owner, sent)
}

#[compio::test]
async fn given_a_resident_window_when_the_last_repair_done_is_lost_should_re_request_and_convert_to_state_transfer()
 {
    const PEER: u8 = 1;
    const NONCE: u128 = 0xf100;
    const FLOOR: u64 = 5;
    const TO_OP: u64 = FLOOR + 10;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, sent) = resident_window_backup(namespace, PEER, NONCE, FLOOR, TO_OP).await;

    re_request_lost_repair_done(&owner, &sent, namespace, PEER, NONCE, TO_OP).await;

    let partitions = owner.plane.partitions();
    let partition = partitions
        .get_by_ns(&namespace)
        .expect("the partition stays registered");
    assert!(
        partition.repair.is_none(),
        "the re-served RepairDone concludes the session"
    );
    assert_ne!(
        partition.consensus().state_transfer_stage(),
        consensus::StateTransferStage::Idle,
        "an unanchored floor over a complete window converts to state transfer"
    );
    assert_eq!(
        partition.transfer.as_ref().map(|session| session.peer),
        Some(PEER)
    );
    assert_eq!(partition.consensus().commit_min(), 0);
}

/// The first op above the floor is a batch at offset 0, which connects to
/// the empty log, so the re-served `RepairDone` accepts the floor and the walk
/// commits the window.
#[compio::test]
async fn given_a_connecting_resident_window_when_the_last_repair_done_is_lost_should_re_request_and_commit_it()
 {
    const PEER: u8 = 1;
    const NONCE: u128 = 0xf100;
    const FLOOR: u64 = 5;
    const TO_OP: u64 = FLOOR + 10;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, sent) = resident_window_backup_with(namespace, PEER, NONCE, FLOOR, TO_OP, |op| {
        if op == FLOOR + 1 {
            send_messages_prepare(namespace, op, 0)
        } else {
            control_prepare(namespace, op)
        }
    })
    .await;

    re_request_lost_repair_done(&owner, &sent, namespace, PEER, NONCE, TO_OP).await;

    let partitions = owner.plane.partitions();
    let partition = partitions
        .get_by_ns(&namespace)
        .expect("the partition stays registered");
    assert_eq!(partition.consensus().commit_min(), TO_OP);
    assert!(
        partition.repair.is_none(),
        "the re-served RepairDone concludes the session"
    );
    assert_eq!(
        partition.consensus().state_transfer_stage(),
        consensus::StateTransferStage::Idle
    );
}

/// Tick until the stall retry asks again for `to_op`, the last op of a fully
/// resident window, then deliver the `RepairDone` that request draws.
#[allow(clippy::future_not_send)]
async fn re_request_lost_repair_done(
    owner: &CompletionTestShard,
    sent: &SentFrames,
    namespace: IggyNamespace,
    peer: u8,
    nonce: u128,
    to_op: u64,
) {
    let mut scratch = Vec::new();
    assert!(owner.tick_partitions(&mut scratch).await.is_none());
    assert_eq!(
        drain_prepare_requests(sent),
        vec![(peer, to_op, to_op)],
        "the stall retry asks again for the last op to draw a fresh RepairDone"
    );

    let repair_done = Message::<RepairRangeReplyHeader>::new(size_of::<RepairRangeReplyHeader>())
        .transmute_header(|_, header: &mut RepairRangeReplyHeader| {
            header.command = Command::RepairDone;
            header.size = u32::try_from(size_of::<RepairRangeReplyHeader>()).unwrap();
            header.group = namespace.inner();
            header.cluster = 1;
            header.replica = peer;
            header.nonce = nonce;
            header.op = to_op;
            header.seal();
        });
    owner.on_repair_range_reply(&repair_done).await;
}

#[compio::test]
async fn given_a_resident_window_when_the_serving_peer_is_dead_should_rotate_to_another_peer() {
    const PEER: u8 = 1;
    let namespace = IggyNamespace::new(0, 0, 0);
    let (owner, _sent) = resident_window_backup(namespace, PEER, 0xf100, 5, 15).await;

    let rotated = tick_until_repair_peer_changes(&owner, namespace, Some(PEER)).await;
    assert_ne!(rotated, PEER);
}

/// The `RequestPrepares` frames the bus forwarded, as `(peer, from_op, to_op)`.
fn drain_prepare_requests(sent: &SentFrames) -> Vec<(u8, u64, u64)> {
    sent.borrow_mut()
        .drain(..)
        .filter_map(|(peer, frame)| {
            let request = Message::<RequestPreparesHeader>::try_from(Owned::copy_from_slice(
                frame.as_slice(),
            ))
            .ok()?;
            (request.header().command == Command::RequestPrepares)
                .then(|| (peer, request.header().from_op, request.header().to_op))
        })
        .collect()
}

/// A `SendMessages` prepare at `op` carrying one message at `base_offset`.
fn send_messages_prepare(
    namespace: IggyNamespace,
    op: u64,
    base_offset: u64,
) -> Message<PrepareHeader> {
    let payload = b"repaired";
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload: payload.to_vec().into(),
        user_headers: None,
    });
    let mut batch =
        SendMessagesOwned::from_messages(namespace, &messages).expect("encode the batch");
    batch.header.base_offset = base_offset;
    batch.header.batch_checksum = batch.header.checksum_for_blob(&batch.blob);
    let body_start = size_of::<PrepareHeader>();
    let size = body_start + COMMAND_HEADER_SIZE + batch.blob.len();
    let mut prepare = Message::<PrepareHeader>::new(size);
    let body = &mut prepare.as_mut_slice()[body_start..];
    batch.header.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    body[COMMAND_HEADER_SIZE..].copy_from_slice(&batch.blob);
    prepare.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = op;
        header.op = op;
        header.operation = Operation::SendMessages;
        header.group = namespace.inner();
        header.size = u32::try_from(size).unwrap();
    })
}

/// A bare non-`SendMessages` prepare at `op`, as the commit walk skips it.
fn control_prepare(namespace: IggyNamespace, op: u64) -> Message<PrepareHeader> {
    let size = size_of::<PrepareHeader>();
    Message::<PrepareHeader>::new(size).transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.retry_capacity = u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
        header.client = message_bus::AUTO_COMMIT_CLIENT_ID;
        header.session = 1;
        header.request = op;
        header.op = op;
        header.operation = Operation::CreateStream;
        header.group = namespace.inner();
        header.size = u32::try_from(size).unwrap();
    })
}

fn invalid_metadata_descriptor(
    nonce: u128,
    generation: u64,
    unknown_kind: u8,
) -> Message<StateTransferTargetHeader> {
    metadata_descriptor(
        nonce,
        generation,
        &[
            StateArtifact::for_bytes(unknown_kind, generation, &[]),
            StateArtifact::for_bytes(artifact_kind::METADATA_SNAPSHOT, generation, &[]),
            StateArtifact::for_bytes(artifact_kind::CLIENT_TABLE, generation, &[]),
        ],
    )
}

fn metadata_descriptor(
    nonce: u128,
    generation: u64,
    artifacts: &[StateArtifact],
) -> Message<StateTransferTargetHeader> {
    let manifest = encode_state_manifest(artifacts);
    let size = size_of::<StateTransferTargetHeader>() + manifest.len();
    let mut message = Message::<StateTransferTargetHeader>::new(size);
    message.as_mut_slice()[size_of::<StateTransferTargetHeader>()..].copy_from_slice(&manifest);
    message.transmute_header(|_, header: &mut StateTransferTargetHeader| {
        header.command = Command::StateTransferTarget;
        header.cluster = 1;
        header.group = server_common::sharding::METADATA_GROUP;
        header.nonce = nonce;
        header.size = u32::try_from(size).unwrap();
        header.available = 1;
        header.commit_op = generation;
        header.seal();
    })
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
    let metadata = IggyMetadata::new(None, None, None, None, metadata, None);
    owner_with_metadata_plane(bus, config, namespace, metadata)
}

fn owner_with_metadata_plane(
    bus: &Rc<IggyMessageBus>,
    config: PartitionsConfig,
    namespace: IggyNamespace,
    metadata: IggyMetadata<
        consensus::VsrConsensus<Rc<IggyMessageBus>>,
        PrepareJournal,
        IggySnapshot,
        PollTestMetadata,
    >,
) -> (CompletionTestShard, TaggedSender) {
    owner_in_cluster(bus, config, namespace, metadata, 3)
}

fn owner_in_cluster(
    bus: &Rc<IggyMessageBus>,
    config: PartitionsConfig,
    namespace: IggyNamespace,
    metadata: IggyMetadata<
        consensus::VsrConsensus<Rc<IggyMessageBus>>,
        PrepareJournal,
        IggySnapshot,
        PollTestMetadata,
    >,
    replica_count: u8,
) -> (CompletionTestShard, TaggedSender) {
    let shard_id = ShardId::new(0);
    let partitions = IggyPartitions::new(shard_id, config);
    let (sender, inbox, replies) = shard_channel(0, 2, 1);
    let routes = PapayaShardsTable::new();
    routes.insert(namespace, PartitionLocation::new(shard_id, 0));
    let owner = CompletionTestShard::new(
        ShardIdentity::new(0, "poll-completion-test".to_string()),
        bus.clone(),
        Rc::new(NoopHost),
        metadata,
        partitions,
        vec![sender.clone()],
        inbox,
        replies,
        2,
        None,
        routes,
        PartitionConsensusConfig::new(1, ReplicaTopology::new(0, replica_count), bus.clone()),
        None,
        ShardMetrics::for_shard(),
    )
    .expect("valid owner inbox wiring");
    owner
        .plane
        .metadata()
        .mux_stm
        .users()
        .ensure_root_user("root", "hash");
    for user_id in 1..=BOUND_USER {
        apply_poll_metadata(
            &owner,
            Operation::CreateUser,
            CreateUserRequest {
                username: WireName::new(format!("reader-{user_id}")).unwrap(),
                password: "hash".to_owned(),
                status: UserStatus::Active.as_code(),
                permissions: Some(WirePermissions {
                    global: WireGlobalPermissions {
                        manage_servers: false,
                        read_servers: false,
                        manage_users: false,
                        read_users: false,
                        manage_streams: false,
                        read_streams: false,
                        manage_topics: false,
                        read_topics: false,
                        poll_messages: true,
                        send_messages: false,
                    },
                    streams: Vec::new(),
                }),
                options: WireOptions::empty(),
            }
            .to_bytes(),
        );
    }
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
            &PollingArgs::new(PollingStrategy::offset(0), 1, false),
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
        ..
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

#[allow(clippy::future_not_send)]
async fn install_test_group_owner(
    partition: &mut partitions::IggyPartition<Rc<IggyMessageBus>>,
    config: &PartitionsConfig,
    group_id: u64,
) {
    let installation = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id,
        owner: iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner {
            client_id: 1,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    install_group_owner(partition, config, installation).await;
}
