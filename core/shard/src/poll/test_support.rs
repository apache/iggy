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

use std::sync::Arc;

use consensus::{LocalPipeline, Sequencer, VsrConsensus, oneshot_channel};
use futures::FutureExt;
use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, RoutedRequestHeader, WireConsumer, WireEncode, WireIdentifier,
};
use iggy_common::{IggyByteSize, PartitionStats, variadic};
use message_bus::MessageBus;
use metadata::MuxStateMachine;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use partitions::{
    IggyPartition, Partition, PartitionPathLayout, PartitionsConfig, PollingConsumer,
};
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::IggyNamespace;

pub(super) type PollTestMetadata = MuxStateMachine<variadic!(Users, Streams)>;

pub(super) fn partitions_config() -> PartitionsConfig {
    let segment_size = IggyByteSize::from(1_048_576_u64);
    PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: segment_size,
        validate_checksum: true,
        segment_size,
        preallocate_segments: false,
        key_context: Arc::default(),
        path_layout: PartitionPathLayout::default(),
    }
}
const SEGMENT_FLOOR: u64 = 1024 * 1024;

/// Commit one batch starting at offset zero and keep it resident. Each call
/// creates an independent partition history, even for the same namespace.
#[allow(clippy::future_not_send)]
pub(super) async fn partition_with_messages<B: MessageBus + Clone>(
    bus: &B,
    namespace: IggyNamespace,
    payloads: &[&str],
) -> (IggyPartition<B>, PartitionsConfig) {
    partition_with_messages_at_revision(bus, namespace, payloads, 1).await
}

#[allow(clippy::future_not_send)]
pub(super) async fn partition_with_messages_at_revision<B: MessageBus + Clone>(
    bus: &B,
    namespace: IggyNamespace,
    payloads: &[&str],
    created_revision: u64,
) -> (IggyPartition<B>, PartitionsConfig) {
    let cluster_id = 1;
    let replica_id = 0;
    let replica_count = 3;
    // The batch must fit one segment or it never materialises, and it must stay
    // under the flush threshold or it stops being resident. Doubling the payload
    // total covers framing; the floor keeps small fixtures where they were.
    let payload_bytes: u64 = payloads.iter().map(|payload| payload.len() as u64).sum();
    let segment_size = IggyByteSize::from(SEGMENT_FLOOR.max(payload_bytes.saturating_mul(2)));
    let config = PartitionsConfig {
        segment_size,
        size_of_messages_required_to_save: segment_size,
        ..partitions_config()
    };
    let consensus = VsrConsensus::new(
        cluster_id,
        replica_id,
        replica_count,
        namespace.inner(),
        bus.clone(),
        LocalPipeline::new(),
    );
    consensus.init();
    let mut partition = Box::new(IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        segment_size,
    ));
    partition.set_created_revision(created_revision);
    partition.set_runtime_options(iggy_common::TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        consumer_offset_durability: iggy_common::Durability::Persisted,
        ..Default::default()
    });

    commit_messages(&mut partition, &config, namespace, payloads, (1, 0)).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(partition.offsets().commit_offset, payloads.len() as u64 - 1);

    (*partition, config)
}

/// Append `payloads` as one batch from the fixture client and commit it.
/// `request` numbers the batch among the client's requests, and
/// `metadata_watermark` must reach the partition's metadata fences.
#[allow(clippy::future_not_send)]
pub(super) async fn commit_messages<B: MessageBus + Clone>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    payloads: &[&str],
    (request, metadata_watermark): (u64, u64),
) {
    assert!(!payloads.is_empty(), "fixture requires messages");
    let mut messages = IggyMessages::with_capacity(payloads.len());
    for (index, payload) in payloads.iter().enumerate() {
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: index as u128 + 1,
                payload_length: u32::try_from(payload.len()).expect("fixture payload fits u32"),
                ..Default::default()
            },
            payload: payload.as_bytes().to_vec().into(),
            user_headers: None,
        });
    }
    let append = SendMessagesOwned::from_messages(namespace, &messages)
        .expect("encode fixture messages")
        .encode_request(RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: 1,
            session: 1,
            request,
            group: namespace.inner(),
            partition_incarnation: partition.created_revision(),
            metadata_watermark,
            ..Default::default()
        })
        .expect("encode append request");
    let (append_reply, append_result) = oneshot_channel();
    let op = partition.consensus().sequencer().current_sequence() + 1;
    partition.on_request(append, Some(append_reply)).await;
    assert_eq!(
        partition.consensus().sequencer().current_sequence(),
        op,
        "fixture append was prepared"
    );
    partition.consensus().advance_commit_max(op);
    partition.commit_journal(config).await;
    assert!(
        matches!(append_result.now_or_never(), Some(Ok(_))),
        "fixture append was committed"
    );
}

/// The client of a [`store_consumer_offset`] of a standalone consumer, apart
/// from the one that appends.
const STORING_CLIENT: u128 = 2;

/// Stores `offset` for `consumer` as a client store does, through the commit
/// of the partition with `ack`. `request` numbers the store among the
/// storing client's requests.
#[allow(clippy::future_not_send)]
pub(super) async fn store_consumer_offset<B: MessageBus + Clone>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    namespace: IggyNamespace,
    (consumer, offset, ack): (PollingConsumer, u64, AckLevel),
    (request, metadata_watermark): (u64, u64),
) {
    // A group offset takes a store only from its installed owner.
    let (wire_consumer, client, owner_generation) = match consumer {
        PollingConsumer::Consumer(id, _) => (
            WireConsumer::consumer(WireIdentifier::numeric(u32::try_from(id).unwrap())),
            STORING_CLIENT,
            0,
        ),
        PollingConsumer::ConsumerGroup(group, _) => {
            let owner = partition
                .consumer_group_owner(group as u64)
                .expect("the fixture installs the group owner");
            (
                WireConsumer::consumer_group(WireIdentifier::numeric(
                    u32::try_from(group).unwrap(),
                )),
                owner.client_id,
                owner.generation,
            )
        }
    };
    let body = StoreConsumerOffsetRequest {
        consumer: wire_consumer,
        stream_id: WireIdentifier::numeric(u32::try_from(namespace.stream_id()).unwrap()),
        topic_id: WireIdentifier::numeric(u32::try_from(namespace.topic_id()).unwrap()),
        partition_id: Some(u32::try_from(namespace.partition_id()).unwrap()),
        offset,
        ack,
    }
    .to_bytes();
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut message = server_common::Message::<RoutedRequestHeader>::new(size);
    message.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(&body);
    let message = message.transmute_header::<RoutedRequestHeader>(|_, header| {
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::StoreConsumerOffset,
            client,
            session: 1,
            request,
            size: u32::try_from(size).unwrap(),
            group: namespace.inner(),
            partition_incarnation: partition.created_revision(),
            owner_generation,
            metadata_watermark,
            ..Default::default()
        };
    });
    let (reply, stored) = oneshot_channel();
    partition.on_request(message, Some(reply)).await;
    if ack != AckLevel::NoAck {
        let op = partition.consensus().sequencer().current_sequence();
        partition.consensus().advance_commit_max(op);
        partition.commit_journal(config).await;
    }
    let stored = stored.now_or_never();
    assert!(
        matches!(&stored, Some(Ok(reply)) if reply.header().status == 0),
        "the fixture store {consumer:?} with {ack:?} was committed: {:?}",
        stored.map(|reply| reply.map(|reply| reply.header().status))
    );
    assert_eq!(partition.get_consumer_offset(consumer), Some(offset));
}

#[allow(clippy::future_not_send)]
pub(super) async fn install_group_owner<B: MessageBus + Clone>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    installation: InstallConsumerGroupOwnerRequest,
) -> u64 {
    let op = commit_lifecycle_fence(
        partition,
        config,
        Operation::InstallConsumerGroupOwner,
        &installation.to_bytes(),
        installation.incarnation,
        installation.metadata_op,
    )
    .await;
    assert_eq!(
        partition.installed_consumer_group_owner(&installation),
        Some(op)
    );
    op
}

/// Commit the delete fence that closes the partition's current incarnation.
#[allow(clippy::future_not_send)]
pub(super) async fn delete_history<B: MessageBus + Clone>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    metadata_op: u64,
) -> u64 {
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op,
    };
    let op = commit_lifecycle_fence(
        partition,
        config,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        transition.incarnation,
        metadata_op,
    )
    .await;
    assert_eq!(
        partition.installed_history_transition(&transition),
        Some(op)
    );
    op
}

#[allow(clippy::future_not_send)]
async fn commit_lifecycle_fence<B: MessageBus + Clone>(
    partition: &mut IggyPartition<B>,
    config: &PartitionsConfig,
    operation: Operation,
    body: &[u8],
    incarnation: u64,
    metadata_op: u64,
) -> u64 {
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut message = server_common::Message::<RoutedRequestHeader>::new(size);
    message.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
    let message = message.transmute_header::<RoutedRequestHeader>(|_, header| {
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation,
            client: u128::MAX,
            session: 1,
            request: metadata_op,
            size: u32::try_from(size).unwrap(),
            partition_incarnation: incarnation,
            metadata_watermark: metadata_op,
            ..Default::default()
        };
    });
    partition.on_request(message, None).await;
    let op = partition.consensus().sequencer().current_sequence();
    partition.consensus().advance_commit_max(op);
    partition.commit_journal(config).await;
    op
}
