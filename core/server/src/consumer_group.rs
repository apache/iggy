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
//! The metadata primary attaches the authenticated client and session to each
//! membership operation. Ordered metadata apply chooses pending assignments;
//! durable partition-log installations authorize their activation.

pub mod lease;
pub mod liveness;

use crate::namespace::resolve_offset_group_id;
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
use iggy_binary_protocol::{
    KIND_CONSUMER_GROUP, KIND_EXTERNAL_GROUP, Operation, RoutedRequestHeader, WireIdentifier,
};
use iggy_common::IggyError;
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::consumer_group::{
    JoinConsumerGroupRequest as ReplicatedJoinConsumerGroupRequest,
    LeaveConsumerGroupRequest as ReplicatedLeaveConsumerGroupRequest,
};
use server_common::Message;
use std::rc::Rc;

/// Retain the authenticated member identity in the replicated join or leave.
/// Ownership is activated only by the partition installation protocol.
pub fn maybe_rewrite_consumer_group_request(
    request: Message<RoutedRequestHeader>,
) -> Result<Message<RoutedRequestHeader>, IggyError> {
    let operation = request.header().operation;
    let client_id = request.header().client;
    let body = request_body(&request);
    let rewritten = match operation {
        Operation::JoinConsumerGroup => {
            let wire = WireJoinConsumerGroupRequest::decode_from(body)
                .map_err(|_| IggyError::InvalidCommand)?;
            ReplicatedJoinConsumerGroupRequest {
                stream_id: wire.stream_id,
                topic_id: wire.topic_id,
                group_id: wire.group_id,
                client_id,
                session: request.header().session,
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

/// Rewrite a group or external-group consumer-offset op so its consumer id is
/// the group's monotonic id rather than the wire name. The partition plane keys group
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
            if !matches!(
                wire.consumer.kind,
                KIND_CONSUMER_GROUP | KIND_EXTERNAL_GROUP
            ) {
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
    use super::*;
    use crate::dispatch::test_support::{
        SpyBus, TestMux, TestShard, prepare_message, request_message,
    };
    use crate::namespace::resolve_partition_namespace;
    use consensus::{LocalPipeline, VsrConsensus};
    use futures::future::{Either, select};
    use iggy_binary_protocol::primitives::partition_assignment::CreatedPartitionAssignment;
    use iggy_binary_protocol::requests::consumer_groups::CreateConsumerGroupRequest;
    use iggy_binary_protocol::requests::streams::CreateStreamRequest;
    use iggy_binary_protocol::requests::topics::{
        CreateTopicRequest, CreateTopicWithAssignmentsRequest,
    };
    use iggy_binary_protocol::{WireName, WireOptions};
    use iggy_common::IggyByteSize;
    use metadata::IggyMetadata;
    use metadata::stm::StateMachine;
    use metadata::stm::consumer_group::CompleteConsumerGroupRevocationRequest;
    use partitions::{IggyPartitions, PartitionPathLayout, PartitionsConfig};
    use server_common::sharding::{IggyNamespace, PartitionLocation, ShardId};
    use shard::metrics::ShardMetrics;
    use shard::shards_table::{PapayaShardsTable, ShardsTable};
    use shard::{
        NoopHost, PartitionConsensusConfig, ReplicaTopology, ShardIdentity, channel, shard_channel,
    };
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    const STREAM_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const TOPIC_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const GROUP_ID: WireIdentifier = WireIdentifier::Numeric(0);
    const FIRST_CLIENT: u128 = 11;
    const SECOND_CLIENT: u128 = 22;
    const PARTITION_COUNT: u32 = 2;
    const INBOX_CAPACITY: usize = 16;

    #[compio::test]
    async fn given_unavailable_partitions_when_joining_should_wait_for_exact_installations() {
        let shard = group_shard();
        let mux = &shard.plane.metadata().mux_stm;
        let streams = mux.streams();
        let join = maybe_rewrite_consumer_group_request(join_request(FIRST_CLIENT)).unwrap();
        let identity =
            ReplicatedJoinConsumerGroupRequest::decode_from(request_body(&join)).unwrap();
        assert_eq!(identity.client_id, FIRST_CLIENT);
        assert_eq!(identity.session, 1);
        mux.update(prepare_message(
            Operation::JoinConsumerGroup,
            FIRST_CLIENT,
            4,
            request_body(&join),
        ))
        .unwrap();
        assert_eq!(
            streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT)
                .unwrap()
                .1,
            [] as [u32; 0]
        );
        let transitions = streams.consumer_group_pending_revocations();
        assert_eq!(transitions.len(), PARTITION_COUNT as usize);
        for (index, transition) in transitions.into_iter().enumerate() {
            let completion = CompleteConsumerGroupRevocationRequest {
                stream_id: STREAM_ID,
                topic_id: TOPIC_ID,
                partition_id: transition.partition_id,
                installation: transition.installation,
                partition_op: 1,
            };
            mux.update(prepare_message(
                Operation::CompleteConsumerGroupRevocation,
                FIRST_CLIENT,
                5 + index as u64,
                &completion.to_bytes(),
            ))
            .unwrap();
        }
        assert_eq!(
            streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT)
                .unwrap()
                .1,
            vec![0, 1]
        );

        shard.shards_table().remove(&namespace(&shard, 1));
        let join = maybe_rewrite_consumer_group_request(join_request(SECOND_CLIENT)).unwrap();
        mux.update(prepare_message(
            Operation::JoinConsumerGroup,
            SECOND_CLIENT,
            7,
            request_body(&join),
        ))
        .unwrap();
        assert!(streams.has_pending_revocations());
        assert_eq!(
            streams
                .consumer_group_member_assignment(&STREAM_ID, &TOPIC_ID, &GROUP_ID, SECOND_CLIENT)
                .unwrap()
                .1,
            [] as [u32; 0]
        );
        assert_eq!(
            streams.consumer_group_fence(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT, 1, true),
            None
        );
        assert_eq!(
            streams.consumer_group_fence(&STREAM_ID, &TOPIC_ID, &GROUP_ID, FIRST_CLIENT, 1, false),
            Some(0)
        );
        assert_eq!(
            streams.consumer_group_fence(&STREAM_ID, &TOPIC_ID, &GROUP_ID, SECOND_CLIENT, 1, false),
            None
        );
    }
    /// Create a group and routes for two partitions, with no members or local
    /// partitions. Tests install partition state and apply joins explicitly.
    fn group_shard() -> Rc<TestShard> {
        let shard = partition_read_shard(3);
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
    pub(super) fn partition_read_shard(replica_count: u8) -> Rc<TestShard> {
        // These tests do not dispatch disk polls, so keep that lane minimal.
        const POLL_COMPLETION_CAPACITY: usize = 1;

        let bus = SpyBus::default();
        let consensus = VsrConsensus::new(
            1,
            0,
            replica_count,
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
                Rc::new(NoopHost),
                metadata,
                partitions,
                vec![sender],
                inbox,
                replies,
                POLL_COMPLETION_CAPACITY,
                None,
                PapayaShardsTable::new(),
                PartitionConsensusConfig::new(1, ReplicaTopology::new(0, replica_count), bus),
                None,
                ShardMetrics::for_shard(),
            )
            .unwrap(),
        )
    }

    fn namespace(shard: &Rc<TestShard>, partition_id: u32) -> IggyNamespace {
        resolve_partition_namespace(shard, &STREAM_ID, &TOPIC_ID, Some(partition_id)).unwrap()
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

    /// Poll the shard's message pump alongside the operation until it finishes.
    /// This serves progress reads and also executes any requested stale clears.
    pub(super) async fn run_with_partition_message_pump<T>(
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
}
