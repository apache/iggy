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

use super::*;
use crate::FreeSpaceReserve;
use iggy_binary_protocol::{
    PrepareHeader, RoutedRequestHeader, requests::partitions::TransitionPartitionHistoryRequest,
};
use iggy_common::IggyError;
use server_common::{DiskUsage, Message};
use std::rc::Rc;
use std::sync::Arc;

const RESERVE_BYTES: u64 = 1 << 30;

const USED_BYTES: u64 = 1 << 40;

/// Free space that leaves room for the sends of a test above the reserve.
const ROOMY_BYTES: u64 = 2 * RESERVE_BYTES;

/// These tests charge nothing, so one estimate serves them all.
static USAGE: DiskUsage = DiskUsage::new();

fn reserve_with(free_bytes: u64) -> Arc<FreeSpaceReserve> {
    USAGE.rebase(|| Some(((), USED_BYTES)));
    let reserve = Arc::new(FreeSpaceReserve::new(RESERVE_BYTES, &USAGE));
    reserve.observe(free_bytes, USED_BYTES);
    reserve
}

fn short_reserve() -> Arc<FreeSpaceReserve> {
    reserve_with(RESERVE_BYTES - 1)
}

fn send_request<B: MessageBus>(partition: &IggyPartition<B>) -> Message<RoutedRequestHeader> {
    let namespace = partition.namespace();
    let mut send = checksumless_send_request(namespace, 1).transmute_header(
        |header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.partition_incarnation = partition.created_revision();
        },
    );
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, 0));
    send
}

#[compio::test]
async fn given_a_short_data_disk_when_the_primary_gets_messages_should_refuse_them_until_space_returns()
 {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let reserve = short_reserve();
    partition.set_free_space_reserve(Some(Arc::clone(&reserve)));

    let (sender, refusal) = consensus::oneshot_channel();
    partition
        .on_request(send_request(&partition), Some(sender))
        .await;
    // Checked before the await: a wrongly admitted send never gets a reply.
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(reserve.refused_requests(), 1);

    reserve.observe(ROOMY_BYTES, USED_BYTES);
    partition.on_request(send_request(&partition), None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    assert_eq!(reserve.refused_requests(), 1);
    assert_eq!(
        reserve.pending_bytes(),
        0,
        "without a WAL the admitted send leaves nothing pending"
    );
}

/// The refusal says that nothing was admitted. A retry of a send that is
/// pending or committed would then never learn the outcome of its original.
#[compio::test]
async fn given_a_short_data_disk_when_a_send_is_retried_should_answer_the_outcome_of_the_original()
{
    for expected_status in [0, IggyError::TransientNotCommitted.as_code()] {
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.runtime_options.durability = iggy_common::Durability::Replicated;
        let request = send_request(&partition);
        let header = *request.header();
        let cached_reply = if expected_status == 0 {
            let prepare = PrepareHeader {
                client: header.client,
                session: header.session,
                request: header.request,
                request_checksum: header.request_checksum,
                operation: header.operation,
                op: 1,
                group: header.group,
                ..Default::default()
            };
            let reply = build_reply_message(
                &prepare,
                &send_messages_reply_body(
                    header.group,
                    Some(CommittedBatchStats {
                        producer: None,
                        timestamp: 0,
                        base_offset: 0,
                        message_count: 1,
                        size_bytes: 0,
                    }),
                ),
            );
            Some(
                partition
                    .dedup
                    .commit_partition_reply(header.user_id, header.session, reply)
                    .unwrap()
                    .into_wire_bytes(),
            )
        } else {
            let prepare =
                request
                    .clone()
                    .transmute_header(|request, prepare: &mut PrepareHeader| {
                        prepare.command = Command::Prepare;
                        prepare.operation = request.operation;
                        prepare.client = request.client;
                        prepare.user_id = request.user_id;
                        prepare.request = request.request;
                        prepare.session = request.session;
                        prepare.request_checksum = request.request_checksum;
                        prepare.retry_capacity =
                            u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap();
                        prepare.op = 1;
                        prepare.size = request.size;
                    });
            partition
                .consensus()
                .pipeline_message(PlaneKind::Partitions, &prepare);
            None
        };
        let reserve = short_reserve();
        partition.set_free_space_reserve(Some(Arc::clone(&reserve)));

        partition.on_request(request, None).await;

        let replies = replies.borrow();
        assert_eq!(replies.len(), 1);
        let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_eq!(reply.status, expected_status);
        if let Some(cached) = cached_reply {
            assert!(replies[0].1.shares_allocation(&cached));
        }
        assert_eq!(reserve.refused_requests(), 0);
        assert_eq!(reserve.pending_bytes(), 0);
    }
}

/// A producer that resends a batch under a new request id is owed the
/// committed result as well, and answering it writes nothing.
#[compio::test]
async fn given_a_short_data_disk_when_a_producer_resends_a_committed_batch_should_answer_its_result()
 {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let producer = BatchProducer::new(7, 2, 0).unwrap();
    partition.producers.record(&StampedBatch {
        producer,
        message_count: 1,
        base_offset: 0,
        timestamp: 1,
    });
    let mut resend = send_request(&partition);
    let body = &mut resend.as_mut_slice()[size_of::<RoutedRequestHeader>()..];
    let mut batch = BatchHeader::decode(&body[..COMMAND_HEADER_SIZE]).unwrap();
    batch.producer = Some(producer);
    batch.batch_checksum = batch.checksum_for_blob(&body[COMMAND_HEADER_SIZE..]);
    batch.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    let reserve = short_reserve();
    partition.set_free_space_reserve(Some(Arc::clone(&reserve)));

    let (sender, result) = consensus::oneshot_channel();
    partition.on_request(resend, Some(sender)).await;

    let result = result.await.unwrap();
    assert_eq!(result.header().status, 0);
    assert_eq!(result.header().reserved[0], ReplyHeader::COMMITTED_RESULT);
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(reserve.refused_requests(), 0);
    assert_eq!(reserve.pending_bytes(), 0);
}

/// A partition accepts a send well before its write is charged, so the
/// backlog of one WAL has to count against the sends of every other.
#[compio::test]
async fn given_a_send_waiting_in_one_wal_when_another_partition_gets_messages_should_count_the_backlog()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut backlogged, _) = recording_partition_at(0, 3);
    backlogged.runtime_options.durability = iggy_common::Durability::Replicated;
    backlogged.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backlogged.open_persistence().await.unwrap();
    let (mut other, _) = recording_partition_at(0, 3);
    other.runtime_options.durability = iggy_common::Durability::Replicated;
    let frame_bytes = send_request(&other).as_slice().len() as u64;
    // Room for the one send of each partition that the test admits last.
    let reserve = reserve_with(RESERVE_BYTES + frame_bytes);
    backlogged.set_free_space_reserve(Some(Arc::clone(&reserve)));
    other.set_free_space_reserve(Some(Arc::clone(&reserve)));

    // Queued without the writer, as behind a slow disk.
    let persistence = Rc::clone(backlogged.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"backlog");
    persistence.append(prepare.into_frozen(), false).unwrap();
    let backlog_bytes = reserve.pending_bytes();
    assert!(backlog_bytes > 0, "the queued prepare is pending");

    let (sender, refusal) = consensus::oneshot_channel();
    other.on_request(send_request(&other), Some(sender)).await;
    assert_eq!(other.consensus.sequencer().current_sequence(), 0);
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(reserve.refused_requests(), 1);
    assert_eq!(
        reserve.pending_bytes(),
        backlog_bytes,
        "a refused send takes nothing"
    );

    // Written: the disk usage estimate holds the bytes from here on.
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert_eq!(persistence.written_op(), 1);
    assert_eq!(reserve.pending_bytes(), 0);
    other.on_request(send_request(&other), None).await;
    assert_eq!(other.consensus.sequencer().current_sequence(), 1);
}

/// A send can wait in the request queue while the disk falls short. It is
/// checked again before it becomes a prepare.
#[compio::test]
async fn given_a_queued_send_when_the_data_disk_falls_short_should_refuse_it_before_it_is_prepared()
{
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let reserve = reserve_with(ROOMY_BYTES);
    partition.set_free_space_reserve(Some(Arc::clone(&reserve)));
    let (sender, refusal) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                send_request(&partition),
                Some(sender)
            ))
            .is_ok()
    );

    reserve.observe(RESERVE_BYTES - 1, USED_BYTES);
    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    assert_eq!(reserve.refused_requests(), 1);

    reserve.observe(ROOMY_BYTES, USED_BYTES);
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                send_request(&partition),
                None
            ))
            .is_ok()
    );
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    assert_eq!(reserve.pending_bytes(), 0);
}

/// Deleting the history of a partition is how its space comes back, so the
/// reserve must not refuse it.
#[compio::test]
async fn given_a_short_data_disk_when_the_primary_gets_a_history_deletion_should_admit_it() {
    let (mut partition, _) = recording_partition_at(0, 3);
    let reserve = short_reserve();
    partition.set_free_space_reserve(Some(Arc::clone(&reserve)));
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: 10,
    };

    partition
        .on_request(
            partition_control_request(
                &partition,
                Operation::TransitionPartitionHistory,
                &transition.to_bytes(),
                transition.metadata_op,
            ),
            None,
        )
        .await;

    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    assert_eq!(reserve.refused_requests(), 0);
}

/// Only the node that is short refuses. A backup that is short still names
/// the primary to a client and still journals what the primary prepares, so
/// the group commits as before.
#[compio::test]
async fn given_a_short_data_disk_on_a_backup_only_when_the_primary_gets_messages_should_replicate_them()
 {
    let (mut primary, _) = recording_partition_at(0, 3);
    primary.runtime_options.durability = iggy_common::Durability::Replicated;
    primary.set_free_space_reserve(Some(reserve_with(ROOMY_BYTES)));
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.runtime_options.durability = iggy_common::Durability::Replicated;
    let reserve = short_reserve();
    backup.set_free_space_reserve(Some(Arc::clone(&reserve)));

    let (sender, refusal) = consensus::oneshot_channel();
    backup.on_request(send_request(&backup), Some(sender)).await;
    assert_eq!(
        refusal.await.unwrap().header().status,
        IggyError::NotPrimary.as_code()
    );

    primary.on_request(send_request(&primary), None).await;
    assert_eq!(primary.consensus.sequencer().current_sequence(), 1);
    let bytes = primary.log.journal().inner.repair_entry(1).unwrap();
    let prepare =
        Message::<PrepareHeader>::try_from(Owned::copy_from_slice(bytes.as_slice())).unwrap();
    backup.on_replicate(prepare).await;

    assert!(backup.log.journal().inner.holds_op(1));
    assert_eq!(reserve.refused_requests(), 0);
}
