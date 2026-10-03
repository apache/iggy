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
use iggy_binary_protocol::batch::{BatchIntegrity, decode_batch_slice_with};
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffsetRequest, StoreConsumerOffsetRequest,
};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareOkHeader, ReplyHeader, RoutedRequestHeader, WireConsumer,
    WireEncode, WireIdentifier,
};
use iggy_common::{ConsumerKind, IggyByteSize, IggyError, PartitionStats, PollingStrategy};
use message_bus::{AUTO_COMMIT_CLIENT_ID, IggyMessageBus};
use partitions::{
    IggyPartition, IggyPartitions, Partition, PartitionPathLayout, PartitionsConfig, PollingArgs,
    PollingConsumer,
};
use server_common::Message;
use server_common::send_messages::{
    IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
};
use server_common::sharding::{IggyNamespace, ShardId};
use std::rc::Rc;
use std::sync::Arc;

const CONSUMER_ID: u32 = 7;
const REPLACEMENT: &[u8] = b"replacement record";
type TestPartition = IggyPartition<Rc<IggyMessageBus>>;

#[compio::test]
async fn given_queued_offset_store_when_purged_should_not_skip_replacement_records() {
    for client_id in [42, AUTO_COMMIT_CLIENT_ID] {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let (mut partition, config, _directory) = partition_with_old_record().await;
            let consumer = polling_consumer(kind);
            let (returned, delivered) = poll(
                partition,
                &config,
                consumer,
                &PollingArgs::new(PollingStrategy::first(), 1, false),
            );
            partition = returned;
            assert_eq!(delivered, vec![b"old record".to_vec()]);
            assert_eq!(partition.get_consumer_offset(consumer), None);

            partition
                .on_request(append(2, b"old pending prepare"), None)
                .await;
            assert!(partition.consensus().pipeline_is_full());
            partition.on_request(append(3, REPLACEMENT), None).await;
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(client_id, 1, kind, 0), Some(sender))
                .await;
            assert_eq!(partition.consensus().request_queue_len(), 2);

            partition.purge(&config, 1).await.unwrap();
            assert_eq!(partition.purge_floor_op(), 2);
            commit_pending(&mut partition, &config).await;

            assert_eq!(
                partition.get_consumer_offset(consumer),
                None,
                "a store admitted before purge must not checkpoint replacement history ({client_id}, {kind:?})",
            );
            let reply = receiver
                .now_or_never()
                .expect("the queued store must finish")
                .unwrap();
            assert_eq!(reply.header().status, IggyError::InvalidOffset(0).as_code());
            assert_eq!(
                partition.consensus().sequencer().current_sequence(),
                3,
                "reject the obsolete store before assigning an operation"
            );
            let (partition, next) = poll(
                partition,
                &config,
                consumer,
                &PollingArgs::new(PollingStrategy::next(), 10, false),
            );
            let (_, first) = poll(
                partition,
                &config,
                PollingConsumer::Consumer(8, 0),
                &PollingArgs::new(PollingStrategy::first(), 10, false),
            );
            assert_eq!(
                first,
                vec![REPLACEMENT.to_vec()],
                "the unrelated queued publish survives"
            );
            assert_eq!(next, first, "Next must not skip the replacement record");
        }
    }
}

#[compio::test]
async fn given_queued_automatic_commit_when_purged_should_keep_replacement_records_unread() {
    let (mut partition, config, _directory) = partition_with_old_record().await;
    partition
        .on_request(append(2, b"old pending prepare"), None)
        .await;
    partition.on_request(append(3, REPLACEMENT), None).await;
    let consumer = polling_consumer(ConsumerKind::Consumer);
    let (mut partition, delivered) = poll(
        partition,
        &config,
        consumer,
        &PollingArgs::new(PollingStrategy::first(), 1, true),
    );
    assert_eq!(delivered, vec![b"old record".to_vec()]);
    assert_eq!(partition.consensus().request_queue_len(), 2);
    partition.purge(&config, 1).await.unwrap();
    assert_eq!(partition.consensus().request_queue_len(), 1);
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.get_consumer_offset(consumer), None);
    let (_, next) = poll(
        partition,
        &config,
        consumer,
        &PollingArgs::new(PollingStrategy::next(), 10, false),
    );
    assert_eq!(next, vec![REPLACEMENT.to_vec()]);
}

#[compio::test]
async fn given_queued_explicit_store_when_history_is_unchanged_should_allow_rewind() {
    for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
        let (mut partition, config, _directory) = partition_with_old_record().await;
        partition
            .on_request(append(2, b"second record"), None)
            .await;
        commit_pending(&mut partition, &config).await;
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(store(42, 1, kind, 1), Some(sender))
            .await;
        commit_pending(&mut partition, &config).await;
        assert_success(receiver);
        assert_eq!(
            partition.get_consumer_offset(polling_consumer(kind)),
            Some(1)
        );

        partition.on_request(append(3, b"third record"), None).await;
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(store(42, 2, kind, 0), Some(sender))
            .await;
        assert_eq!(partition.consensus().request_queue_len(), 1);
        commit_pending(&mut partition, &config).await;
        assert_success(receiver);
        assert_eq!(
            partition.get_consumer_offset(polling_consumer(kind)),
            Some(0),
            "history protection must not turn an explicit store into a monotone automatic commit"
        );
    }
}

#[compio::test]
async fn given_queued_offset_delete_when_history_changes_should_reject_without_replay() {
    for purge in [false, true] {
        for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
            let (mut partition, config, _directory) = partition_with_old_record().await;
            let (sender, receiver) = oneshot_channel();
            partition
                .on_request(store(42, 1, kind, 0), Some(sender))
                .await;
            commit_pending(&mut partition, &config).await;
            assert_success(receiver);
            partition
                .on_request(append(2, b"pending prepare"), None)
                .await;
            partition.on_request(append(3, REPLACEMENT), None).await;
            let (sender, receiver) = oneshot_channel();
            partition.on_request(delete(kind, 2), Some(sender)).await;
            assert_eq!(partition.consensus().request_queue_len(), 2);
            if purge {
                partition.purge(&config, 1).await.unwrap();
            }
            commit_pending(&mut partition, &config).await;
            let reply = receiver
                .now_or_never()
                .expect("the delete must finish")
                .unwrap();
            assert_eq!(
                reply.header().status,
                if purge {
                    IggyError::ConsumerOffsetNotFound(CONSUMER_ID as usize).as_code()
                } else {
                    0
                }
            );
            assert_eq!(
                partition.consensus().sequencer().current_sequence(),
                if purge { 4 } else { 5 }
            );
            assert_eq!(partition.get_consumer_offset(polling_consumer(kind)), None);
        }
    }
}

#[compio::test]
async fn given_many_obsolete_offsets_when_purged_should_finish_rejections_and_resume_queued_writes()
{
    let (mut partition, config, _directory) = partition_with_old_record().await;
    partition
        .on_request(append(2, b"old pending prepare"), None)
        .await;
    let mut receivers = Vec::new();
    for request_id in 1_u64..=6 {
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(
                store(42 + u128::from(request_id), 1, ConsumerKind::Consumer, 0),
                Some(sender),
            )
            .await;
        receivers.push(receiver);
    }
    partition.on_request(append(3, REPLACEMENT), None).await;
    partition.purge(&config, 1).await.unwrap();
    commit_pending(&mut partition, &config).await;
    assert!(
        partition.consensus().request_queue_len() > 0,
        "the rejection budget must leave work for a later owner turn"
    );
    assert!(partition.queued_requests_ready());
    for _ in 0..8 {
        partition.resume_queued_requests().await;
        commit_pending(&mut partition, &config).await;
        if partition.consensus().request_queue_len() == 0 {
            break;
        }
    }
    assert_eq!(partition.consensus().request_queue_len(), 0);
    assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
    for receiver in receivers {
        let reply = receiver
            .now_or_never()
            .expect("every obsolete store gets a reply")
            .unwrap();
        assert_eq!(reply.header().status, IggyError::InvalidOffset(0).as_code());
    }
    let (_, next) = poll(
        partition,
        &config,
        polling_consumer(ConsumerKind::Consumer),
        &PollingArgs::new(PollingStrategy::next(), 10, false),
    );
    assert_eq!(next, vec![REPLACEMENT.to_vec()]);
}

#[compio::test]
async fn given_queued_delete_when_replacement_checkpoint_commits_should_preserve_new_progress() {
    let kind = ConsumerKind::Consumer;
    let (mut partition, config, _directory) = partition_with_old_record().await;
    let (sender, receiver) = oneshot_channel();
    partition
        .on_request(store(42, 1, kind, 0), Some(sender))
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(receiver);
    partition
        .on_request(append(2, b"old pending prepare"), None)
        .await;
    partition.on_request(append(3, REPLACEMENT), None).await;
    let mut obsolete_stores = Vec::new();
    for request_id in 2_u64..=5 {
        let (sender, receiver) = oneshot_channel();
        partition
            .on_request(store(42 + u128::from(request_id), 1, kind, 0), Some(sender))
            .await;
        obsolete_stores.push(receiver);
    }
    let (sender, deleted) = oneshot_channel();
    partition.on_request(delete(kind, 6), Some(sender)).await;
    partition.purge(&config, 1).await.unwrap();
    commit_pending(&mut partition, &config).await;
    assert_eq!(
        partition.consensus().request_queue_len(),
        1,
        "the rejection budget leaves the old delete for a later turn"
    );

    // A new owner turn can admit a valid store while obsolete queued work
    // waits for the next bounded promotion turn.
    let (sender, stored) = oneshot_channel();
    partition
        .on_request(store(43, 1, kind, 0), Some(sender))
        .await;
    commit_pending(&mut partition, &config).await;
    assert_success(stored);
    let reply = deleted
        .now_or_never()
        .expect("the old delete must finish")
        .unwrap();
    assert_eq!(
        reply.header().status,
        IggyError::ConsumerOffsetNotFound(CONSUMER_ID as usize).as_code()
    );
    assert_eq!(
        partition.get_consumer_offset(polling_consumer(kind)),
        Some(0),
        "the obsolete delete must not remove the replacement checkpoint"
    );
    assert_eq!(partition.consensus().sequencer().current_sequence(), 5);
    for receiver in obsolete_stores {
        let reply = receiver
            .now_or_never()
            .expect("the obsolete store must finish")
            .unwrap();
        assert_eq!(reply.header().status, IggyError::InvalidOffset(0).as_code());
    }
}

#[allow(clippy::future_not_send)]
async fn partition_with_old_record() -> (TestPartition, PartitionsConfig, tempfile::TempDir) {
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
        3,
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
    partition.on_request(append(1, b"old record"), None).await;
    commit_pending(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
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

fn poll(
    partition: TestPartition,
    config: &PartitionsConfig,
    consumer: PollingConsumer,
    args: &PollingArgs,
) -> (TestPartition, Vec<Vec<u8>>) {
    let owner = IggyPartitions::new(ShardId::new(0), config.clone());
    owner.insert(namespace(), partition);
    let read = owner
        .build_poll_snapshot(&namespace(), consumer, args)
        .unwrap()
        .execute_resident();
    let completion = owner.complete_poll(&namespace(), read).unwrap();
    assert!(
        completion.replication.is_none(),
        "automatic commits in this fixture must queue"
    );
    let payloads = completion
        .fragments
        .iter()
        .flat_map(|fragment| {
            let batch =
                decode_batch_slice_with(fragment.as_slice(), BatchIntegrity::Verify).unwrap();
            batch
                .iter_with_offsets()
                .map(|record| record.message.payload.to_vec())
                .collect::<Vec<_>>()
        })
        .collect();
    (owner.remove(&namespace()).unwrap(), payloads)
}

fn namespace() -> IggyNamespace {
    IggyNamespace::new(1, 1, 0)
}

const fn polling_consumer(kind: ConsumerKind) -> PollingConsumer {
    match kind {
        ConsumerKind::Consumer => PollingConsumer::Consumer(CONSUMER_ID as usize, 0),
        ConsumerKind::ConsumerGroup => PollingConsumer::ConsumerGroup(CONSUMER_ID as usize, 0),
    }
}

fn append(request: u64, payload: &[u8]) -> Message<RoutedRequestHeader> {
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
            ..Default::default()
        })
        .unwrap()
}

fn store(
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
    offset_request(Operation::StoreConsumerOffset, client, request, &body)
}

fn delete(kind: ConsumerKind, request: u64) -> Message<RoutedRequestHeader> {
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
    offset_request(Operation::DeleteConsumerOffset, 42, request, &body)
}

fn offset_request(
    operation: Operation,
    client: u128,
    request: u64,
    body: &[u8],
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
            ..Default::default()
        };
    })
}

fn assert_success(receiver: consensus::Receiver<Message<ReplyHeader>>) {
    let reply = receiver
        .now_or_never()
        .expect("the store must finish")
        .unwrap();
    assert_eq!(reply.header().status, 0);
}
