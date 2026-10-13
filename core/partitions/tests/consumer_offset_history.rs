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

use consensus::{LocalPipeline, Sequencer, VsrConsensus, oneshot_channel};
use futures::FutureExt;
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::requests::partitions::{
    InstallConsumerGroupOwnerRequest, TransitionPartitionHistoryRequest,
};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareOkHeader, ReplyHeader, RoutedRequestHeader, WireConsumer,
    WireEncode, WireIdentifier,
};
use iggy_common::{ConsumerKind, IggyByteSize, IggyError, PartitionStats};
use message_bus::{AUTO_COMMIT_CLIENT_ID, IggyMessageBus};
use partitions::{
    IggyPartition, Partition, PartitionPathLayout, PartitionsConfig, PollingConsumer,
};
use server_common::Message;
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::IggyNamespace;
use std::rc::Rc;
use std::sync::Arc;

const CONSUMER_ID: u32 = 7;
const METADATA_WATERMARK: u64 = 100;
const DELETE_METADATA_OP: u64 = 2;
const CLUSTER_REPLICAS: u8 = 3;
type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

#[compio::test]
async fn given_admitted_offset_store_when_deleting_should_drain_before_the_fence() {
    for client_id in [42, AUTO_COMMIT_CLIENT_ID] {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let (mut partition, config, _directory) =
                Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
            let consumer = polling_consumer(kind);
            partition
                .on_request(append(&partition, 2, b"admitted record"), None)
                .await;
            let delayed = [
                store(&partition, client_id, 2, kind, 0),
                delete(&partition, kind, 3),
                append(&partition, 4, b"delayed record"),
            ];
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(&partition, client_id, 1, kind, 0), Some(sender))
                .await;
            assert_eq!(partition.consensus().request_queue_len(), 1);

            assert_fence_waits(&mut partition).await;
            let (sender, refused) = oneshot_channel();
            partition
                .on_request(append(&partition, 5, b"arrived during drain"), Some(sender))
                .await;
            assert_status(refused, IggyError::TransientNotAccepted.as_code());
            commit_pending(&mut partition, &config).await;
            assert_success(receiver);
            assert_eq!(partition.get_consumer_offset(consumer), Some(0));

            delete_history(&mut partition, &config).await;
            let before = partition.consensus().sequencer().current_sequence();
            for request in delayed {
                let (sender, receiver) = oneshot_channel();
                partition.on_request(request, Some(sender)).await;
                assert_status(receiver, IggyError::HistoryUnavailable.as_code());
            }
            assert_eq!(partition.consensus().sequencer().current_sequence(), before);
            assert_eq!(partition.get_consumer_offset(consumer), Some(0));
        }
    }
}

#[compio::test]
async fn given_unassigned_owner_when_automatic_offsets_arrive_should_only_allow_matching_cleanup() {
    let (mut partition, config, _directory) =
        Box::pin(partition_with_old_record(CLUSTER_REPLICAS)).await;
    let consumer = polling_consumer(ConsumerKind::ConsumerGroup);
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            store(&partition, 42, 1, ConsumerKind::ConsumerGroup, 0),
            Some(sender),
        )
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    let stale_cleanup = delete(&partition, ConsumerKind::ConsumerGroup, 2).transmute_header(
        |old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.client = AUTO_COMMIT_CLIENT_ID;
        },
    );
    let installation = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: u64::from(CONSUMER_ID),
        owner: ConsumerGroupOwner {
            client_id: 0,
            session: 0,
            generation: 2,
        },
        metadata_op: 2,
    };
    install_owner_record(&mut partition, &config, installation).await;
    let automatic_store = store(
        &partition,
        AUTO_COMMIT_CLIENT_ID,
        3,
        ConsumerKind::ConsumerGroup,
        0,
    );
    let before = partition.consensus().sequencer().current_sequence();
    for request in [automatic_store, stale_cleanup] {
        let (sender, receiver) = oneshot_channel();
        partition.on_request(request, Some(sender)).await;
        assert_status(
            receiver,
            IggyError::ConsumerGroupPartitionNotOwned(CONSUMER_ID, 0).as_code(),
        );
        assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    }
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);

    let cleanup = delete(&partition, ConsumerKind::ConsumerGroup, 4).transmute_header(
        |old, header: &mut RoutedRequestHeader| {
            *header = old;
            header.client = AUTO_COMMIT_CLIENT_ID;
        },
    );
    let delayed_cleanup = cleanup.deep_copy();
    let (sender, receiver) = oneshot_channel();
    partition.on_request(cleanup, Some(sender)).await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    assert_eq!(partition.get_consumer_offset(consumer), None);

    install_owner_record(
        &mut partition,
        &config,
        InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: 77,
                session: 1,
                generation: 3,
            },
            metadata_op: 3,
            ..installation
        },
    )
    .await;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            store(&partition, 77, 1, ConsumerKind::ConsumerGroup, 0),
            Some(sender),
        )
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    let (sender, receiver) = oneshot_channel();
    partition.on_request(delayed_cleanup, Some(sender)).await;
    assert_status(
        receiver,
        IggyError::ConsumerGroupPartitionNotOwned(CONSUMER_ID, 0).as_code(),
    );
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
}

#[allow(clippy::future_not_send)]
async fn install_owner_record(
    partition: &mut TestPartition,
    config: &PartitionsConfig,
    installation: InstallConsumerGroupOwnerRequest,
) {
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(
            offset_request(
                partition,
                Operation::InstallConsumerGroupOwner,
                AUTO_COMMIT_CLIENT_ID,
                installation.metadata_op,
                &installation.to_bytes(),
                0,
            ),
            Some(sender),
        )
        .await;
    commit_pending(partition, config).await;
    assert_success(receiver);
    assert!(
        partition
            .installed_consumer_group_owner(&installation)
            .is_some()
    );
}

fn delete_fence(partition: &TestPartition) -> Message<RoutedRequestHeader> {
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: DELETE_METADATA_OP,
    };
    offset_request(
        partition,
        Operation::TransitionPartitionHistory,
        AUTO_COMMIT_CLIENT_ID,
        transition.metadata_op,
        &transition.to_bytes(),
        0,
    )
}

#[allow(clippy::future_not_send)]
async fn assert_fence_waits(partition: &mut TestPartition) {
    let before = partition.consensus().sequencer().current_sequence();
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(delete_fence(partition), Some(sender))
        .await;
    assert_status(receiver, IggyError::TransientNotAccepted.as_code());
    assert_eq!(partition.consensus().sequencer().current_sequence(), before);
}

#[allow(clippy::future_not_send)]
async fn delete_history(partition: &mut TestPartition, config: &PartitionsConfig) {
    assert_eq!(partition.consensus().request_queue_len(), 0);
    let fence_op = partition.consensus().commit_min() + 1;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(delete_fence(partition), Some(sender))
        .await;
    commit_pending(partition, config).await;
    assert_success(receiver);
    assert!(partition.history_deleted());
    assert_eq!(partition.consensus().commit_min(), fence_op);
}

#[allow(clippy::future_not_send)]
async fn partition_with_old_record(
    replica_count: u8,
) -> (TestPartition, PartitionsConfig, tempfile::TempDir) {
    let directory = tempfile::tempdir().unwrap();
    let segment_size = IggyByteSize::from(1_048_576_u64);
    let config = PartitionsConfig {
        messages_required_to_save: 100,
        size_of_messages_required_to_save: segment_size,
        validate_checksum: true,
        segment_size,
        preallocate_segments: false,
        encryptor: None,
        path_layout: PartitionPathLayout::default(),
    };
    let consensus = VsrConsensus::new(
        1,
        0,
        replica_count,
        namespace().inner(),
        Rc::new(IggyMessageBus::new(0)),
        LocalPipeline::with_capacities(1, 8),
    );
    consensus.init();
    let mut partition = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        segment_size,
    );
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let installation = InstallConsumerGroupOwnerRequest {
        incarnation: partition.created_revision(),
        group_id: u64::from(CONSUMER_ID),
        owner: ConsumerGroupOwner {
            client_id: 42,
            session: 1,
            generation: 1,
        },
        metadata_op: 1,
    };
    install_owner_record(&mut partition, &config, installation).await;
    partition
        .on_request(append(&partition, 1, b"old record"), None)
        .await;
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 2);
    (partition, config, directory)
}

// Drive the actual commit and queue-promotion paths with deterministic quorum
// completion. Remote replicas and timing do not decide when a prepare advances.
#[allow(clippy::future_not_send)]
async fn commit_pending(partition: &mut TestPartition, config: &PartitionsConfig) {
    for _ in 0..16 {
        process_self_acknowledgments(partition, config).await;
        let current = partition.consensus().sequencer().current_sequence();
        if partition.consensus().commit_min() == current {
            return;
        }
        partition.consensus().advance_commit_max(current);
        partition.commit_journal(config).await;
    }
    panic!("the bounded request queue did not drain");
}

#[allow(clippy::future_not_send)]
async fn process_self_acknowledgments(partition: &mut TestPartition, config: &PartitionsConfig) {
    let mut queued = Vec::new();
    partition.consensus().drain_loopback_into(&mut queued);
    for message in queued {
        let mut typed = Message::<PrepareOkHeader>::new(message.as_slice().len());
        typed.as_mut_slice().copy_from_slice(message.as_slice());
        partition.on_ack(typed, config).await;
    }
}

fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 0)
}

const fn polling_consumer(kind: ConsumerKind) -> PollingConsumer {
    match kind {
        ConsumerKind::Consumer => PollingConsumer::Consumer(CONSUMER_ID as usize, 0),
        ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(CONSUMER_ID as usize, 0),
        ConsumerKind::ExternalGroup => panic!("an external group is never polled"),
    }
}

fn append(partition: &TestPartition, request: u64, payload: &[u8]) -> Message<RoutedRequestHeader> {
    let mut messages = IggyMessages::with_capacity(1);
    messages.push(IggyMessage {
        header: IggyMessageHeader {
            id: u128::from(request),
            payload_length: u32::try_from(payload.len()).unwrap(),
            ..Default::default()
        },
        payload: payload.to_vec().into(),
        user_headers: None,
    });
    SendMessagesOwned::from_messages(namespace(), &messages)
        .unwrap()
        .encode_request(RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: u128::from(request),
            session: 1,
            request,
            group: namespace().inner(),
            partition_incarnation: partition.created_revision(),
            metadata_watermark: METADATA_WATERMARK,
            ..Default::default()
        })
        .unwrap()
}

fn store(
    partition: &TestPartition,
    client: u128,
    request: u64,
    kind: ConsumerKind,
    offset: u64,
) -> Message<RoutedRequestHeader> {
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::numeric(CONSUMER_ID),
        },
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(1),
        partition_id: Some(0),
        offset,
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    offset_request(
        partition,
        Operation::StoreConsumerOffset,
        client,
        request,
        &body,
        owner_generation(partition, kind),
    )
}

fn delete(
    partition: &TestPartition,
    kind: ConsumerKind,
    request: u64,
) -> Message<RoutedRequestHeader> {
    let body = DeleteConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: kind.as_code(),
            id: WireIdentifier::numeric(CONSUMER_ID),
        },
        stream_id: WireIdentifier::numeric(1),
        topic_id: WireIdentifier::numeric(1),
        partition_id: Some(0),
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    offset_request(
        partition,
        Operation::DeleteConsumerOffset,
        42,
        request,
        &body,
        owner_generation(partition, kind),
    )
}

fn offset_request(
    partition: &TestPartition,
    operation: Operation,
    client: u128,
    request: u64,
    body: &[u8],
    owner_generation: u64,
) -> Message<RoutedRequestHeader> {
    let size = size_of::<RoutedRequestHeader>() + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(size);
    message.as_mut_slice()[size_of::<RoutedRequestHeader>()..].copy_from_slice(body);
    message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        *header = RoutedRequestHeader {
            command: Command::Request,
            operation,
            size: u32::try_from(size).unwrap(),
            client,
            session: 1,
            request,
            group: namespace().inner(),
            owner_generation,
            partition_incarnation: partition.created_revision(),
            metadata_watermark: METADATA_WATERMARK,
            ..Default::default()
        };
    })
}

fn owner_generation(partition: &TestPartition, kind: ConsumerKind) -> u64 {
    match kind {
        ConsumerKind::Consumer | ConsumerKind::ExternalGroup => 0,
        ConsumerKind::ConsumerGroup => {
            partition
                .consumer_group_owner(u64::from(CONSUMER_ID))
                .unwrap()
                .generation
        }
    }
}

fn assert_success(receiver: consensus::Receiver<Message<ReplyHeader>>) {
    assert_status(receiver, 0);
}

fn assert_status(receiver: consensus::Receiver<Message<ReplyHeader>>, status: u32) {
    let reply = receiver
        .now_or_never()
        .expect("the request must finish")
        .unwrap();
    assert_eq!(reply.header().status, status);
}
