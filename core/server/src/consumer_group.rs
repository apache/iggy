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

//! Consumer-group Join/Leave request enrichment.
//!
//! The wire `Join`/`Leave` requests carry only the stream/topic/group
//! identifiers. The replicated metadata `apply` additionally needs the
//! joining client's VSR id (which member) and, for Join, the topic's
//! partition count (to seed the group's partition list) -- and it cannot
//! read the Streams STM from inside the consumer-group apply. So the
//! primary enriches the op here before replication, mirroring the PAT mint
//! in [`crate::pat`] and the password hash in [`crate::users`].

use crate::responses::{resolve_offset_group_id, resolve_partition_namespace};
use crate::shell::{ShellBus, ShellShard};
use crate::wire::{request_body, rewrite_request_body};
use consensus::MetadataHandle;
use iggy_binary_protocol::PrepareHeader;
use iggy_binary_protocol::codec::{WireDecode, WireEncode};
use iggy_binary_protocol::requests::consumer_groups::{
    JoinConsumerGroupRequest as WireJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as WireLeaveConsumerGroupRequest,
};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{KIND_CONSUMER_GROUP, Operation, RoutedRequestHeader, WireIdentifier};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::consumer_group::{
    JoinConsumerGroupRequest as ReplicatedJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as ReplicatedLeaveConsumerGroupRequest,
};
use server_common::Message;
use shard::{PartitionRead, PartitionReadReply};
use std::rc::Rc;

/// Rewrite a `Join`/`Leave` request body into the replicated form carrying the
/// client's VSR id (which the apply can't read from the consensus header).
///
/// For `Join` the home shard also gathers `in_flight` -- the group's partitions
/// with uncommitted polled data -- by reading each partition's poll/commit state
/// (via the partition-read mesh), so the cooperative rebalance pending-revokes
/// only those and hands off never-polled/drained partitions synchronously at
/// join. Every other operation passes through.
pub async fn maybe_rewrite_consumer_group_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
) -> Result<Message<RoutedRequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let operation = request.header().operation;
    let client_id = request.header().client;
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::JoinConsumerGroup => {
            let wire = WireJoinConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            let in_flight =
                gather_in_flight(shard, &wire.stream_id, &wire.topic_id, &wire.group_id).await?;
            ReplicatedJoinConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
                in_flight,
            }
            .to_bytes()
        }
        Operation::LeaveConsumerGroup => {
            let wire = WireLeaveConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            ReplicatedLeaveConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
            }
            .to_bytes()
        }
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

/// Gather the group's in-flight partitions (`last_polled` present and
/// `committed < last_polled`) for the cooperative-rebalance classification. A
/// not-yet-created group, an unresolved topic, or an absent partition reply
/// is treated as not-in-flight (eager handoff, allowing at-least-once replay).
/// An explicit rejection aborts the join before replication: missing local
/// materialization cannot establish whether an existing owner has drained.
/// A retry gathers ownership and offsets again before clearing stale marks.
async fn gather_in_flight<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    stream_id: &WireIdentifier,
    topic_id: &WireIdentifier,
    group_id: &WireIdentifier,
) -> Result<Vec<u32>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let streams = shard.plane.metadata().mux_stm.streams();
    let Some(monotonic_group_id) = streams.resolve_consumer_group_id(stream_id, topic_id, group_id)
    else {
        // Fresh group (e.g. create-if-not-exists): nothing polled yet.
        return Ok(Vec::new());
    };
    let Some(partition_ids) = streams.topic_partition_ids(stream_id, topic_id) else {
        return Ok(Vec::new());
    };
    // Partitions a live member currently owns. A `last_polled` past the commit
    // only means in-flight work when a live member still holds the partition;
    // for an unowned one it is the residue of a member removed on disconnect
    // (the reconnect case), which must be reassigned and re-read, not protected.
    let assigned = streams
        .consumer_group_assigned_partitions(stream_id, topic_id, group_id)
        .unwrap_or_default();
    // Resolve namespaces up front (sync), then fire every partition's
    // `GroupOffsetState` read concurrently. The reads are independent, so a
    // wide-topic join must not serialize N cross-shard round-trips before the
    // join op can even be proposed.
    let targets: Vec<(u32, _)> = partition_ids
        .into_iter()
        .filter_map(|partition_id| {
            resolve_partition_namespace(shard, stream_id, topic_id, Some(partition_id))
                .ok()
                .map(|ns| (partition_id, ns))
        })
        .collect();
    let results = futures::future::join_all(targets.iter().map(|&(partition_id, ns)| async move {
        let reply = shard
            .partition_read(
                ns,
                PartitionRead::GroupOffsetState {
                    group_id: monotonic_group_id,
                },
            )
            .await;
        (partition_id, ns, reply)
    }))
    .await;

    let mut in_flight = Vec::new();
    let mut stale_clears = Vec::new();
    for (partition_id, ns, reply) in results {
        if let Some(PartitionReadReply::Rejected(error)) = reply {
            return Err(error);
        }
        let Some(PartitionReadReply::GroupOffsetState {
            last_polled: Some(polled),
            committed,
        }) = reply
        else {
            continue;
        };
        if committed.is_some_and(|c| c >= polled) {
            continue;
        }
        if assigned.contains(&partition_id) {
            in_flight.push(partition_id);
        } else {
            // Stale mark from a removed member: drop it so a later join in this
            // same restart does not misread it once the partition is reassigned.
            stale_clears.push(shard.partition_read(
                ns,
                PartitionRead::ClearGroupLastPolled {
                    group_id: monotonic_group_id,
                },
            ));
        }
    }
    // Fire the stale-mark clears concurrently too; the result is unused.
    futures::future::join_all(stale_clears).await;
    Ok(in_flight)
}

/// Rewrite a group consumer-offset op so its consumer id is the group's
/// monotonic id rather than the wire name. The partition plane keys group
/// offsets by that numeric id (decoded from `WireIdentifier::Numeric`), so the
/// read path -- which resolves the same id from metadata -- and the reconciler
/// purge agree, and a re-created group (new id) never inherits a stale offset.
/// Individual-consumer ops and every other operation pass through untouched.
#[allow(clippy::cast_possible_truncation)]
pub fn maybe_rewrite_consumer_offset_request<B, MJ, S, SB>(
    shard: &Rc<ShellShard<B, MJ, S, SB>>,
    request: Message<RoutedRequestHeader>,
) -> Result<Message<RoutedRequestHeader>, IggyError>
where
    B: ShellBus,
    MJ: JournalHandle + 'static,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    S: 'static,
    SB: SuperblockStore + 'static,
{
    let operation = request.header().operation;
    if !matches!(
        operation,
        Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
    ) {
        return Ok(request);
    }
    let body = request_body(&request);
    // The store/delete ops differ only in the decode type; this collapses
    // their identical decode -> resolve group id -> rewrite consumer id ->
    // re-encode bodies. Individual consumers pass through. A group identifier
    // that metadata cannot resolve is rejected before it can create a raw file
    // in the group-offset directory.
    macro_rules! rewrite_group_offset {
        ($ty:ty) => {{
            let mut wire = <$ty>::decode_from(body).map_err(|_| IggyError::InvalidCommand)?;
            if wire.consumer.kind != KIND_CONSUMER_GROUP {
                return Ok(request);
            }
            let group_id = resolve_offset_group_id(
                shard.plane.metadata().mux_stm.streams(),
                &wire.stream_id,
                &wire.topic_id,
                &wire.consumer.id,
            )?;
            // The partition-plane group-offset key is u32 (see the documented
            // ceiling on `Topic::next_consumer_group_id`). Clamp on the
            // ~4-billion-creates overflow rather than panic this live
            // client-driven path, matching `iggy_partition.rs`'s identical cast.
            wire.consumer.id = WireIdentifier::Numeric(u32::try_from(group_id).unwrap_or(u32::MAX));
            wire.to_bytes()
        }};
    }
    let rewritten = match operation {
        Operation::StoreConsumerOffset => rewrite_group_offset!(StoreConsumerOffsetRequest),
        Operation::DeleteConsumerOffset => rewrite_group_offset!(DeleteConsumerOffsetRequest),
        // The outer `matches!` already filtered to the 2 ops above, but the
        // match is over the 37-variant `Operation`, so a catch-all is required.
        _ => return Ok(request),
    };

    rewrite_request_body(&request, &rewritten)
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    use bytes::Bytes;
    use consensus::{Consensus, LocalPipeline, PartitionsHandle, Sequencer, VsrConsensus};
    use futures::future::{Either, select};
    use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
    use iggy_binary_protocol::primitives::ack_level::AckLevel;
    use iggy_binary_protocol::primitives::consumer::WireConsumer;
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{WireName, WireOptions};
    use iggy_common::{
        ConsumerGroupId, ConsumerGroupOffsets, ConsumerKind, ConsumerOffset, ConsumerOffsets,
        IggyByteSize, PartitionStats,
    };
    use metadata::IggyMetadata;
    use metadata::stm::StateMachine;
    use partitions::state_transfer::mark_materialization_missing;
    use partitions::{
        IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter, PartitionPathLayout,
        PartitionsConfig,
    };
    use server_common::SegmentStorage;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
    use shard::metrics::ShardMetrics;
    use shard::shards_table::{PapayaShardsTable, ShardsTable};
    use shard::{
        LifecycleFrame, PartitionConsensusConfig, ReplicaTopology, ShardFrame, ShardIdentity,
        channel, shard_channel,
    };

    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestMux, TestShard, prepare_message, request_message,
    };

    const STREAM_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const TOPIC_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const GROUP_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const FIRST_CLIENT: u128 = 11;
    const SECOND_CLIENT: u128 = 22;
    const STALE_PARTITION: u32 = 0;
    const RECOVERING_PARTITION: u32 = 1;
    const PARTITION_COUNT: u32 = 2;
    const INBOX_CAPACITY: usize = 16;
    const LAST_POLLED_OFFSET: u64 = 10;
    const COMMITTED_OFFSET: u64 = 5;

    #[compio::test]
    async fn given_rejected_join_when_recovered_should_retry_without_stale_revocations() {
        let shard = group_shard();
        let directory = tempfile::tempdir().unwrap();
        let stale_namespace = namespace(&shard, STALE_PARTITION);
        let recovering_namespace = namespace(&shard, RECOVERING_PARTITION);
        let stale = partition(&shard, STALE_PARTITION, &directory.path().join("stale"));
        record_last_polled(&stale);
        shard.plane.partitions().insert(stale_namespace, stale);
        let recovering_dir = directory.path().join("recovering");
        let mut recovering = partition(&shard, RECOVERING_PARTITION, &recovering_dir);
        mark_materialization_missing(recovering_dir.to_str().unwrap(), 0)
            .await
            .unwrap();
        recovering.open_persistence().await.unwrap();
        assert!(recovering.requires_state_transfer());
        shard
            .plane
            .partitions()
            .insert(recovering_namespace, recovering);

        let streams = shard.plane.metadata().mux_stm.streams();
        let before = streams
            .consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID)
            .unwrap();
        let rejected = run_with_partition_message_pump(
            &shard,
            maybe_rewrite_consumer_group_request(&shard, join_request(FIRST_CLIENT)),
        )
        .await;
        assert!(matches!(rejected, Err(IggyError::TransientNotAccepted)));
        assert_eq!(
            streams.consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID),
            Some(before)
        );
        assert_eq!(
            shard
                .plane
                .partitions()
                .group_offset_state(&stale_namespace, 0),
            Some((Some(LAST_POLLED_OFFSET), None)),
            "rejected join must not execute queued stale clears"
        );

        recover_partition(&shard, &directory.path().join("donor")).await;
        let accepted = run_with_partition_message_pump(
            &shard,
            maybe_rewrite_consumer_group_request(&shard, join_request(FIRST_CLIENT)),
        )
        .await
        .unwrap();
        assert!(
            ReplicatedJoinConsumerGroupRequest::decode_from(request_body(&accepted))
                .unwrap()
                .in_flight
                .is_empty()
        );
        apply_join(&shard, &accepted);
        assert_eq!(
            shard
                .plane
                .partitions()
                .group_offset_state(&stale_namespace, 0),
            Some((None, None)),
            "successful retry clears the orphan's last-polled mark"
        );
        assert_eq!(
            streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT)
                .unwrap()
                .1,
            vec![STALE_PARTITION, RECOVERING_PARTITION]
        );

        let next_join = run_with_partition_message_pump(
            &shard,
            maybe_rewrite_consumer_group_request(&shard, join_request(SECOND_CLIENT)),
        )
        .await
        .unwrap();
        assert!(
            ReplicatedJoinConsumerGroupRequest::decode_from(request_body(&next_join))
                .unwrap()
                .in_flight
                .is_empty(),
            "the next join must not misclassify the previously orphaned partition"
        );
        apply_join(&shard, &next_join);
        assert!(!streams.has_pending_revocations());
        assert_eq!(
            streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, SECOND_CLIENT)
                .unwrap()
                .1,
            vec![RECOVERING_PARTITION]
        );
    }

    #[compio::test]
    async fn given_transferring_partition_when_joining_should_preserve_commit_ownership() {
        for missing in [true, false] {
            let shard = group_shard();
            apply_initial_join(&shard);
            let directory = tempfile::tempdir().unwrap();
            let mut recovering = partition(&shard, RECOVERING_PARTITION, directory.path());
            record_last_polled(&recovering);
            recovering.consumer_group_offsets.pin().insert(
                ConsumerGroupId(0),
                ConsumerOffset::new(
                    ConsumerKind::ConsumerGroup,
                    0,
                    COMMITTED_OFFSET,
                    String::new(),
                ),
            );
            if missing {
                mark_materialization_missing(directory.path().to_str().unwrap(), 0)
                    .await
                    .unwrap();
                recovering.open_persistence().await.unwrap();
            } else {
                recovering.consensus().begin_state_transfer_await();
            }
            assert!(recovering.consensus().is_transferring());
            assert_eq!(recovering.requires_state_transfer(), missing);
            shard
                .plane
                .partitions()
                .insert(namespace(&shard, RECOVERING_PARTITION), recovering);
            let streams = shard.plane.metadata().mux_stm.streams();
            let before = streams
                .consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID)
                .unwrap();
            let assignment = streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT)
                .unwrap();

            let rewritten = run_with_partition_message_pump(
                &shard,
                maybe_rewrite_consumer_group_request(&shard, join_request(SECOND_CLIENT)),
            )
            .await;
            if missing {
                assert!(matches!(rewritten, Err(IggyError::TransientNotAccepted)));
                assert_eq!(
                    streams.consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID),
                    Some(before)
                );
                assert_eq!(
                    streams.consumer_group_member_assignment(
                        &STREAM_ID,
                        &TOPIC_ID,
                        &GROUP_ID,
                        FIRST_CLIENT
                    ),
                    Some(assignment),
                    "rejection must preserve generation and the existing owner's assignment"
                );
                assert!(
                    streams
                        .consumer_group_member_assignment(
                            &STREAM_ID,
                            &TOPIC_ID,
                            &GROUP_ID,
                            SECOND_CLIENT
                        )
                        .is_none()
                );
                assert!(!streams.has_pending_revocations());
            } else {
                let rewritten = rewritten.unwrap();
                assert_eq!(
                    ReplicatedJoinConsumerGroupRequest::decode_from(request_body(&rewritten))
                        .unwrap()
                        .in_flight,
                    vec![RECOVERING_PARTITION]
                );
                apply_join(&shard, &rewritten);
                assert!(streams.has_pending_revocations());
                assert_eq!(
                    streams.consumer_group_fence(
                        &STREAM_ID,
                        &TOPIC_ID,
                        &GROUP_ID,
                        FIRST_CLIENT,
                        RECOVERING_PARTITION,
                        true
                    ),
                    None,
                    "cooperative revocation stops new polls while the owner drains"
                );
            }
            assert_eq!(
                streams.consumer_group_fence(
                    &STREAM_ID,
                    &TOPIC_ID,
                    &GROUP_ID,
                    FIRST_CLIENT,
                    RECOVERING_PARTITION,
                    false
                ),
                Some(0),
                "the existing owner must retain permission to commit its in-flight batch"
            );
            assert_eq!(
                streams.consumer_group_fence(
                    &STREAM_ID,
                    &TOPIC_ID,
                    &GROUP_ID,
                    SECOND_CLIENT,
                    RECOVERING_PARTITION,
                    false
                ),
                None
            );
        }
    }

    #[compio::test]
    async fn given_unroutable_partition_when_joining_should_preserve_ownership() {
        let monotonic_group_id = 0;
        let shard = group_shard();
        apply_initial_join(&shard);
        let streams = shard.plane.metadata().mux_stm.streams();
        let group_before_join = streams
            .consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID)
            .unwrap();

        // The first member owns both partitions. Remove one route so gathering
        // its progress is explicitly refused before any owner receives that read.
        let unroutable_namespace = namespace(&shard, RECOVERING_PARTITION);
        shard.shards_table().remove(&unroutable_namespace);
        assert!(matches!(
            shard
                .partition_read(
                    unroutable_namespace,
                    PartitionRead::GroupOffsetState {
                        group_id: monotonic_group_id
                    },
                )
                .await,
            Some(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted
            ))
        ));

        // A refusal aborts the second member's join before it can be replicated.
        let join_result = run_with_partition_message_pump(
            &shard,
            maybe_rewrite_consumer_group_request(&shard, join_request(SECOND_CLIENT)),
        );
        assert!(matches!(
            join_result.await,
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(
            streams.consumer_group_details(&STREAM_ID, &TOPIC_ID, &GROUP_ID),
            Some(group_before_join),
            "a refused progress read must leave membership and assignments unchanged"
        );

        // Committing previously polled work remains allowed for the existing owner.
        let require_pollable = false;
        assert_eq!(
            streams.consumer_group_fence(
                &STREAM_ID,
                &TOPIC_ID,
                &GROUP_ID,
                FIRST_CLIENT,
                RECOVERING_PARTITION,
                require_pollable
            ),
            Some(monotonic_group_id),
            "the existing owner must retain permission to commit"
        );
        assert!(
            !streams.has_pending_revocations(),
            "the rejected join must not start a handoff"
        );
    }

    #[compio::test]
    async fn given_dropped_or_not_found_group_replies_when_joining_should_allow_eager_handoff() {
        let mut shard = group_shard();
        apply_initial_join(&shard);

        // Routes exist but no partitions have been installed. The real owner
        // pump answers NotFound, establishing the missing partition case.
        let missing_partition_reply = run_with_partition_message_pump(
            &shard,
            shard.partition_read(
                namespace(&shard, RECOVERING_PARTITION),
                PartitionRead::GroupOffsetState { group_id: 0 },
            ),
        )
        .await;
        assert!(matches!(
            missing_partition_reply,
            Some(PartitionReadReply::NotFound)
        ));

        // Receive both join reads through a controlled owner inbox. Lose one
        // reply after submission and answer NotFound for the other partition.
        let (sender, owner_inbox, _owner_replies) =
            shard_channel(0, INBOX_CAPACITY, INBOX_CAPACITY);
        Rc::get_mut(&mut shard)
            .unwrap()
            .attach_senders(vec![sender]);
        let unanswered_namespace = namespace(&shard, STALE_PARTITION);
        let missing_namespace = namespace(&shard, RECOVERING_PARTITION);
        let owner_task = compio::runtime::spawn(async move {
            let mut reply_was_dropped = false;
            let mut not_found_was_sent = false;
            for _ in 0..PARTITION_COUNT {
                let ShardFrame::Lifecycle(LifecycleFrame::PartitionRead {
                    namespace,
                    read: PartitionRead::GroupOffsetState { group_id: 0 },
                    reply,
                }) = owner_inbox.recv().await.unwrap()
                else {
                    panic!("the group read must have reached the owner");
                };
                if namespace == unanswered_namespace {
                    drop(reply);
                    reply_was_dropped = true;
                } else {
                    assert_eq!(namespace, missing_namespace);
                    reply.try_send(PartitionReadReply::NotFound).unwrap();
                    not_found_was_sent = true;
                }
            }
            assert!(reply_was_dropped, "one submitted read must lose its reply");
            assert!(
                not_found_was_sent,
                "the other read must report a missing partition"
            );
        });

        // Neither outcome establishes outstanding work to drain. The join
        // therefore permits immediate reassignment under the existing policy.
        let join_for_replication =
            maybe_rewrite_consumer_group_request(&shard, join_request(SECOND_CLIENT))
                .await
                .unwrap();
        owner_task.await.expect("the owner task must finish");
        let replicated_join =
            ReplicatedJoinConsumerGroupRequest::decode_from(request_body(&join_for_replication))
                .unwrap();
        assert!(
            replicated_join.in_flight.is_empty(),
            "neither partition should wait for its previous owner to drain"
        );
        apply_join(&shard, &join_for_replication);
        let streams = shard.plane.metadata().mux_stm.streams();
        assert!(!streams.has_pending_revocations());
        let (_generation, assigned_partitions) = streams
            .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, SECOND_CLIENT)
            .unwrap();
        assert_eq!(
            assigned_partitions,
            vec![RECOVERING_PARTITION],
            "the new member can poll its partition immediately after the join is applied"
        );
    }

    /// Create a group and routes for two partitions, with no members or local
    /// partitions. Tests install partition state and apply joins explicitly.
    fn group_shard() -> Rc<TestShard> {
        let shard = partition_read_shard();
        let mux = &shard.plane.metadata().mux_stm;
        mux.update(prepare_message(
            Operation::CreateStream,
            FIRST_CLIENT,
            1,
            &CreateStreamRequest {
                name: WireName::new("stream").unwrap(),
                options: WireOptions::empty(),
            }
            .to_bytes(),
        ))
        .unwrap();
        mux.update(prepare_message(
            Operation::CreateTopicWithAssignments,
            FIRST_CLIENT,
            2,
            &CreateTopicWithAssignmentsRequest {
                request: CreateTopicRequest {
                    stream_id: STREAM_ID,
                    partitions_count: PARTITION_COUNT,
                    name: WireName::new("topic").unwrap(),
                    options: WireOptions::empty(),
                },
                derived_options: WireOptions::empty(),
                partitions: (0..PARTITION_COUNT)
                    .map(|partition_id| CreatedPartitionAssignment {
                        partition_id,
                        consensus_group_id: u64::from(partition_id) + 1,
                    })
                    .collect(),
                created_view: 0,
            }
            .to_bytes(),
        ))
        .unwrap();
        mux.update(prepare_message(
            Operation::CreateConsumerGroup,
            FIRST_CLIENT,
            3,
            &CreateConsumerGroupRequest {
                stream_id: STREAM_ID,
                topic_id: TOPIC_ID,
                name: WireName::new("group").unwrap(),
            }
            .to_bytes(),
        ))
        .unwrap();
        for partition_id in 0..PARTITION_COUNT {
            shard.shards_table().insert(
                namespace(&shard, partition_id),
                PartitionLocation::new(ShardId::new(0), 0),
            );
        }
        shard
    }

    /// Build a shard with its own inbox so tests can serve group progress reads
    /// and clears through the production message pump.
    fn partition_read_shard() -> Rc<TestShard> {
        let bus = SpyBus::default();
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            server_common::sharding::METADATA_GROUP,
            bus.clone(),
            LocalPipeline::new(),
        );
        consensus.set_incarnation(1);
        consensus.init();
        let metadata =
            IggyMetadata::new(Some(consensus), None, None, None, TestMux::default(), None);
        let partitions = IggyPartitions::new(
            ShardId::new(0),
            PartitionsConfig {
                messages_required_to_save: 1,
                size_of_messages_required_to_save: IggyByteSize::from(1024_u64),
                validate_checksum: true,
                segment_size: IggyByteSize::from(1_048_576_u64),
                preallocate_segments: false,
                encryptor: None,
                path_layout: PartitionPathLayout::default(),
            },
        );
        let (sender, inbox, replies) = shard_channel(0, INBOX_CAPACITY, INBOX_CAPACITY);
        Rc::new(
            TestShard::new(
                ShardIdentity::new(0, "consumer-group-test".to_string()),
                bus.clone(),
                Rc::new(|_, _| {}),
                Rc::new(|_, _| {}),
                Rc::new(|_| {}),
                Rc::new(|_| {}),
                metadata,
                partitions,
                vec![sender],
                inbox,
                replies,
                PapayaShardsTable::new(),
                PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 3), bus),
                None,
                ShardMetrics::for_shard(),
            )
            .unwrap(),
        )
    }

    fn namespace(shard: &Rc<TestShard>, partition_id: u32) -> IggyNamespace {
        resolve_partition_namespace(shard, &STREAM_ID, &TOPIC_ID, Some(partition_id)).unwrap()
    }

    fn partition(
        shard: &Rc<TestShard>,
        partition_id: u32,
        directory: &Path,
    ) -> IggyPartition<SpyBus> {
        let consensus = VsrConsensus::new(
            1,
            0,
            3,
            namespace(shard, partition_id).inner(),
            SpyBus::default(),
            LocalPipeline::new(),
        );
        consensus.init();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            shard.plane.partitions().config().segment_size,
        );
        let consumers = directory.join("offsets/consumers");
        let groups = directory.join("offsets/groups");
        std::fs::create_dir_all(&consumers).unwrap();
        std::fs::create_dir_all(&groups).unwrap();
        partition.set_partition_dir(directory.to_string_lossy().into_owned());
        partition.configure_consumer_offset_storage(
            consumers.to_string_lossy().into_owned(),
            groups.to_string_lossy().into_owned(),
            ConsumerOffsets::with_capacity(0),
            ConsumerGroupOffsets::with_capacity(1),
        );
        partition
    }

    fn record_last_polled(partition: &IggyPartition<SpyBus>) {
        partition.last_polled_offsets.pin().insert(
            ConsumerGroupId(0),
            ConsumerOffset::new(
                ConsumerKind::ConsumerGroup,
                0,
                LAST_POLLED_OFFSET,
                String::new(),
            ),
        );
    }

    fn join_request(client: u128) -> Message<RoutedRequestHeader> {
        request_message(
            Operation::JoinConsumerGroup,
            client,
            1,
            1,
            &WireJoinConsumerGroupRequest {
                stream_id: STREAM_ID,
                topic_id: TOPIC_ID,
                group_id: GROUP_ID,
            }
            .to_bytes(),
        )
    }

    fn apply_initial_join(shard: &Rc<TestShard>) {
        shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::JoinConsumerGroup,
                FIRST_CLIENT,
                4,
                &ReplicatedJoinConsumerGroupRequest {
                    stream_id: STREAM_ID,
                    topic_id: TOPIC_ID,
                    group_id: GROUP_ID,
                    client_id: FIRST_CLIENT,
                    in_flight: Vec::new(),
                }
                .to_bytes(),
            ))
            .unwrap();
    }

    fn apply_join(shard: &Rc<TestShard>, request: &Message<RoutedRequestHeader>) {
        shard
            .plane
            .metadata()
            .mux_stm
            .update(prepare_message(
                Operation::JoinConsumerGroup,
                request.header().client,
                request.header().request,
                request_body(request),
            ))
            .unwrap();
    }

    /// Poll the shard's message pump alongside the operation until it finishes.
    /// This serves progress reads and also executes any requested stale clears.
    async fn run_with_partition_message_pump<T>(
        shard: &Rc<TestShard>,
        operation: impl Future<Output = T>,
    ) -> T {
        let (_stop, stop) = channel(1);
        let serve = shard.run_message_pump(stop, Arc::new(AtomicBool::new(false)));
        match select(Box::pin(operation), Box::pin(serve)).await {
            Either::Left((result, _)) => result,
            Either::Right(_) => {
                unreachable!("partition read service runs until the request completes")
            }
        }
    }

    async fn recover_partition(shard: &Rc<TestShard>, donor_dir: &Path) {
        let mut donor = partition(shard, RECOVERING_PARTITION, donor_dir);
        attach_segment_files(&mut donor, donor_dir).await;
        let body = StoreConsumerOffsetRequest {
            consumer: WireConsumer::consumer_group(GROUP_ID),
            stream_id: STREAM_ID,
            topic_id: TOPIC_ID,
            partition_id: Some(RECOVERING_PARTITION),
            offset: COMMITTED_OFFSET,
            ack: AckLevel::Quorum,
        }
        .to_bytes();
        let config = shard.plane.partitions().config();
        commit_donor_prepare(&mut donor, config, Operation::StoreConsumerOffset, &body).await;
        let mut messages =
            IggyMessages::with_capacity(usize::try_from(COMMITTED_OFFSET + 1).unwrap());
        for _ in 0..=COMMITTED_OFFSET {
            messages.push(IggyMessage {
                header: IggyMessageHeader::default(),
                payload: Bytes::from_static(b"recovered"),
                user_headers: None,
            });
        }
        let batch =
            SendMessagesOwned::from_messages(namespace(shard, RECOVERING_PARTITION), &messages)
                .unwrap();
        let mut body = vec![0; batch.header.total_size()];
        batch.header.encode_into(&mut body);
        body[BATCH_HEADER_SIZE..].copy_from_slice(&batch.blob);
        commit_donor_prepare(&mut donor, config, Operation::SendMessages, &body).await;
        assert_eq!(donor.group_offset_state(0).1, Some(COMMITTED_OFFSET));
        let offer = donor.state_transfer_offer(config).await.unwrap();
        assert_eq!(offer.segments.len(), 1);
        let recovering_namespace = namespace(shard, RECOVERING_PARTITION);
        let recovering = shard
            .plane
            .partitions()
            .get_mut_by_ns(&recovering_namespace)
            .unwrap();
        let segment = &offer.segments[0];
        let staged = recovering
            .spill_transfer_segment(&segment.entry, std::fs::read(&segment.log_path).unwrap())
            .await
            .unwrap();
        recovering
            .install_state_transfer(config, offer.commit_op, vec![staged], &offer.offsets.1, 0)
            .await
            .unwrap();
        assert!(!recovering.requires_state_transfer());
        assert_eq!(recovering.group_offset_state(0).1, Some(COMMITTED_OFFSET));
    }

    async fn commit_donor_prepare(
        donor: &mut IggyPartition<SpyBus>,
        config: &PartitionsConfig,
        operation: Operation,
        body: &[u8],
    ) {
        let op = donor.consensus().sequencer().current_sequence() + 1;
        let prepare = prepare_message(operation, FIRST_CLIENT, op, body).transmute_header(
            |original, header: &mut PrepareHeader| {
                *header = original;
                header.cluster = 1;
                header.group = donor.consensus().group();
                header.op = op;
                header.parent = donor.consensus().last_prepare_checksum();
            },
        );
        let prepare = consensus::seal_prepare_checksum(prepare);
        donor.consensus().sequencer().set_sequence(op);
        donor
            .consensus()
            .set_last_prepare_checksum(prepare.header().checksum);
        donor.on_replicate(prepare).await;
        donor.consensus().advance_commit_max(op);
        donor.commit_journal(config).await;
        assert_eq!(donor.consensus().commit_min(), op);
    }

    async fn attach_segment_files(partition: &mut IggyPartition<SpyBus>, directory: &Path) {
        let start_offset = partition.log.active_segment().start_offset;
        let messages_path = directory
            .join(format!("{start_offset:020}.log"))
            .to_string_lossy()
            .into_owned();
        let index_path = directory
            .join(format!("{start_offset:020}.index"))
            .to_string_lossy()
            .into_owned();
        let storage = SegmentStorage::new(&messages_path, &index_path, 0, 0, false)
            .await
            .unwrap();
        let messages_size = storage.messages_writer.as_ref().unwrap().size_counter();
        let index_size = storage.index_writer.as_ref().unwrap().size_counter();
        partition.log.messages_writers_mut()[0] = Some(Rc::new(
            MessagesWriter::new(&messages_path, messages_size, false, false, None)
                .await
                .unwrap(),
        ));
        partition.log.index_writers_mut()[0] = Some(Rc::new(
            IggyIndexWriter::new(&index_path, index_size, false, false)
                .await
                .unwrap(),
        ));
        *partition.log.active_storage_mut() = storage;
    }
}
