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

use super::consensus_tests::send_request_for_client;
use super::*;
use server_common::VolumeSample;

/// An estimate already at its limit, so its flag is set.
fn disk_at_limit() -> &'static DiskUsage {
    let usage: &'static DiskUsage = Box::leak(Box::new(DiskUsage::new()));
    usage.rebase(|| {
        Some((
            (),
            VolumeSample {
                used_bytes: 100,
                limit_bytes: Some(100),
            },
        ))
    });
    assert!(usage.is_blocked());
    usage
}

/// The status of the reply `client` got for `request`, if it got one.
fn reply_status(replies: &SentFrames, client: u128, request: u64) -> Option<u32> {
    replies.borrow().iter().find_map(|(_, frame)| {
        let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
            &frame.as_slice()[..size_of::<ReplyHeader>()],
        )
        .ok()?;
        (header.client == client && header.request == request).then_some(header.status)
    })
}

#[compio::test]
async fn given_a_full_disk_when_sending_messages_should_refuse_before_replication() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_disk_usage(disk_at_limit());
    let namespace = partition.namespace();

    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;

    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(
        reply_status(&replies, 1, 1),
        Some(IggyError::DiskUsageLimitReached.as_code())
    );
}

#[compio::test]
async fn given_open_disk_when_sending_messages_should_replicate() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_disk_usage(Box::leak(Box::new(DiskUsage::new())));
    let namespace = partition.namespace();

    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;

    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(reply_status(&replies, 1, 1), None);
}

#[compio::test]
async fn given_a_full_disk_on_a_backup_when_sending_should_point_at_the_primary() {
    let (mut partition, replies) = recording_partition_at(1, 3);
    partition.set_disk_usage(disk_at_limit());
    let namespace = partition.namespace();

    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;

    assert_eq!(
        reply_status(&replies, 1, 1),
        Some(IggyError::NotPrimary.as_code()),
        "a backup must send the client to the primary, whose disk decides"
    );
}

#[compio::test]
async fn given_a_full_disk_when_storing_offsets_should_refuse_only_new_keys() {
    let (mut partition, replies) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;
    partition.set_disk_usage(disk_at_limit());

    partition
        .on_request(
            store_offset_request(42, 2, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(
        partition.consensus().pipeline_len(),
        1,
        "a store to an existing key must stay admitted while the disk is full"
    );
    commit_recorded_loopback(&mut partition).await;
    partition
        .on_request(
            store_offset_request(42, 3, ConsumerKind::Consumer, 8, 0, AckLevel::Quorum),
            None,
        )
        .await;

    assert_ne!(
        reply_status(&replies, 42, 2),
        Some(IggyError::DiskUsageLimitReached.as_code())
    );
    assert_eq!(
        reply_status(&replies, 42, 3),
        Some(IggyError::DiskUsageLimitReached.as_code())
    );
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[compio::test]
async fn given_a_full_disk_when_deleting_an_offset_should_admit_it() {
    let (mut partition, replies) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;
    partition.set_disk_usage(disk_at_limit());

    partition
        .on_request(delete_offset_request(42, 2, 7), None)
        .await;

    assert_eq!(
        partition.consensus().pipeline_len(),
        1,
        "a delete frees space, so a full disk must not refuse it"
    );
    assert_ne!(
        reply_status(&replies, 42, 2),
        Some(IggyError::DiskUsageLimitReached.as_code())
    );
}

#[compio::test]
async fn given_a_full_disk_when_a_queued_send_is_promoted_should_refuse_it() {
    let (mut partition, _) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    let head = partition.consensus.sequencer().current_sequence();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                send_request_for_client(namespace, 1),
                Some(sender),
            ))
            .is_ok()
    );
    partition.set_disk_usage(disk_at_limit());

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a send queued before the disk filled must not be prepared"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::DiskUsageLimitReached.as_code()
    );
}

#[compio::test]
async fn given_a_full_disk_when_a_queued_store_for_a_new_key_is_promoted_should_refuse_it() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    let head = partition.consensus.sequencer().current_sequence();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
                Some(sender),
            ))
            .is_ok()
    );
    partition.set_disk_usage(disk_at_limit());

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::DiskUsageLimitReached.as_code()
    );
}

#[compio::test]
async fn given_a_full_disk_when_a_poll_auto_commits_a_new_key_should_refuse_it() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    partition.set_disk_usage(disk_at_limit());

    let admitted = partition.admit_poll_auto_commit(ConsumerKind::Consumer, 9, 0);

    assert!(matches!(admitted, Err(IggyError::DiskUsageLimitReached)));
    assert_eq!(partition.consensus.pipeline_len(), 0);
}

/// A send of one message stamped by `producer`, as an idempotent producer
/// sends it.
fn producer_send(
    namespace: IggyNamespace,
    client_id: u128,
    producer: BatchProducer,
) -> Message<RoutedRequestHeader> {
    let mut batch = IggyMessages::with_capacity(1);
    batch.push(IggyMessage {
        header: IggyMessageHeader {
            payload_length: 8,
            ..Default::default()
        },
        payload: Bytes::from_static(b"abcdefgh"),
        user_headers: None,
    });
    let mut owned =
        SendMessagesOwned::from_messages(namespace, &batch).expect("build send_messages batch");
    owned.header.producer = Some(producer);
    owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);
    let mut send = send_request_for_client(namespace, client_id);
    let body = &mut send.as_mut_slice()[size_of::<RoutedRequestHeader>()..];
    owned.header.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    body[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
    send
}

/// Records that the batch of `producer` with one message committed at
/// `base_offset`, as the apply of its send does.
fn record_committed_batch(
    partition: &mut IggyPartition<RecordingBus>,
    producer: BatchProducer,
    base_offset: u64,
) {
    partition.producers.record(&StampedBatch {
        producer,
        message_count: 1,
        base_offset,
        timestamp: 1,
    });
}

/// A committed batch takes no new space when its producer sends it again, so
/// the full disk lets it through to the reply that proves the first copy
/// committed. The next batch of that producer is new data and is refused.
#[compio::test]
async fn given_a_full_disk_when_a_producer_resends_a_committed_batch_should_answer_it_committed() {
    const BASE_OFFSET: u64 = 7;
    let (mut partition, replies) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    let committed = BatchProducer::new(1, 0, 0).unwrap();
    record_committed_batch(&mut partition, committed, BASE_OFFSET);
    partition.set_disk_usage(disk_at_limit());

    partition
        .on_request(producer_send(namespace, 1, committed), None)
        .await;
    partition
        .on_request(
            producer_send(namespace, 2, BatchProducer::new(1, 0, 1).unwrap()),
            None,
        )
        .await;

    assert_eq!(partition.consensus.pipeline_len(), 0);
    let duplicate = replies
        .borrow()
        .iter()
        .find_map(|(_, frame)| {
            let header = *bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..size_of::<ReplyHeader>()],
            )
            .ok()?;
            (header.client == 1).then_some(header)
        })
        .expect("the resend gets a reply");
    assert_eq!(duplicate.status, 0, "the resend must not be refused");
    assert_eq!(duplicate.reserved[0], ReplyHeader::COMMITTED_RESULT);
    assert_eq!(
        reply_status(&replies, 2, 1),
        Some(IggyError::DiskUsageLimitReached.as_code()),
        "the next batch adds data, so the full disk refuses it"
    );
}

#[compio::test]
async fn given_a_full_disk_when_a_queued_resend_of_a_committed_batch_is_promoted_should_answer_it_committed()
 {
    let (mut partition, _) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    let committed = BatchProducer::new(1, 0, 0).unwrap();
    record_committed_batch(&mut partition, committed, 7);
    let head = partition.consensus.sequencer().current_sequence();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                producer_send(namespace, 1, committed),
                Some(sender),
            ))
            .is_ok()
    );
    partition.set_disk_usage(disk_at_limit());

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    let reply = receiver.await.unwrap();
    assert_eq!(reply.header().status, 0, "the resend must not be refused");
    assert_eq!(reply.header().reserved[0], ReplyHeader::COMMITTED_RESULT);
}
