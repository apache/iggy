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

#[test]
fn given_topic_durability_when_installed_should_tell_consensus_whether_restarts_keep_acks() {
    let mut partition = test_partition();
    assert!(
        !partition.consensus().recovered_log_complete(),
        "the default `replicated` policy acknowledges before the write"
    );
    for (durability, consumer_offset_durability, complete) in [
        (Persisted, Persisted, true),
        (Replicated, Persisted, false),
        (Persisted, Replicated, false),
        (Replicated, Replicated, false),
    ] {
        partition.set_runtime_options(TopicRuntimeOptions {
            durability,
            consumer_offset_durability,
            ..TopicRuntimeOptions::default()
        });
        assert_eq!(
            partition.consensus().recovered_log_complete(),
            complete,
            "{durability:?} messages, {consumer_offset_durability:?} offsets"
        );
    }
}

#[compio::test]
async fn retransmission_catches_up_sequencer_after_wal_admission_backpressure() {
    let mut partition = partition_at_view(1, 1);
    let prepare = Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(
        |_, header: &mut PrepareHeader| {
            header.command = Command::Prepare;
            header.operation = Operation::StoreConsumerOffset;
            header.view = 1;
            header.replica = 1;
            header.group = partition.consensus().group();
            header.cluster = TEST_CLUSTER;
            header.op = 1;
            header.timestamp = 1;
            header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
            header.checksum = header.identity_checksum();
        },
    );
    let checksum = prepare.header().checksum;
    partition
        .log
        .journal()
        .inner
        .append(prepare.clone().into_frozen())
        .await
        .unwrap();
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
    partition.on_replicate(prepare).await;
    assert_eq!(partition.consensus().sequencer().current_sequence(), 1);
    assert_eq!(partition.consensus().last_prepare_checksum(), checksum);
}

#[compio::test]
async fn recovered_empty_wal_reopens_elections_but_new_missing_history_stays_fenced() {
    let intact = tempfile::tempdir().unwrap();
    let mut partition = Box::new(partition_at_view(0, 0));
    partition.set_partition_dir(intact.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let group = partition.consensus().group();
    let journal =
        journal::PartitionPrepareJournal::open(&intact.path().join("prepares-0"), group, 0)
            .await
            .unwrap();
    assert!(!journal.recovered_frontier());
    drop(journal);
    partition.consensus.set_recovery_election_allowed(false);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    assert!(partition.consensus.recovery_election_allowed());
    assert!(!partition.requires_state_transfer());
    drop(partition);

    let missing = tempfile::tempdir().unwrap();
    let wal = missing.path().join("prepares-0");
    PartitionPersistence::fence_missing_history(&wal, 0)
        .await
        .unwrap();
    assert!(
        !wal.exists(),
        "the durable fence must precede replacement WAL creation"
    );
    let replacement = journal::PartitionPrepareJournal::open(&wal, group, 0)
        .await
        .unwrap();
    drop(replacement);
    for _ in 0..2 {
        let mut replacement = Box::new(partition_at_view(0, 0));
        replacement.set_partition_dir(missing.path().to_string_lossy().into_owned());
        replacement.runtime_options.durability = iggy_common::Durability::Persisted;
        replacement.consensus.set_recovery_election_allowed(false);
        replacement.open_persistence().await.unwrap();
        assert!(!replacement.consensus.recovery_election_allowed());
        assert!(replacement.requires_state_transfer());
        assert!(
            crate::state_transfer::materialization_is_missing(missing.path().to_str().unwrap(), 0)
                .await
                .unwrap()
        );
    }
}

#[compio::test]
async fn recovered_committed_tail_refuses_admission_until_receipts_and_capacity_are_restored() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.set_dedup_clients_max(3);
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let mut journal = journal::PartitionPrepareJournal::open(
        &directory.path().join("prepares-0"),
        partition.consensus().group(),
        0,
    )
    .await
    .unwrap();
    journal
        .append(checksummed_segment_prepare(1, 0, 0, b"abcdefgh").into_frozen())
        .await
        .unwrap();
    drop(journal);
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(partition.consensus.commit_max(), 1);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    let mut retry = checksumless_send_request(partition.namespace(), 1);
    retry.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(retry.clone(), None).await;
    {
        let replies = replies.borrow();
        assert_eq!(replies.len(), 1);
        let header = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert_eq!(header.status, IggyError::TransientNotAccepted.as_code());
    }
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(
        partition.dedup.capacity(),
        3,
        "refused admission cannot seal the new local capacity"
    );
    partition.commit_journal(&repair_config()).await;
    assert!(
        partition.fatal.is_none(),
        "recovery commit failed: {:?}",
        partition.fatal
    );
    assert_eq!(
        partition.consensus.commit_min(),
        1,
        "pending checkpoint: {}, resident headers: {:?}",
        partition.persistence_checkpoint_pending(),
        partition
            .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
            .iter()
            .map(|header| header.op)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        partition.dedup.capacity(),
        consensus::PARTITION_DEDUP_CLIENTS_MAX
    );
    partition.on_request(retry, None).await;
    let replies = replies.borrow();
    assert_eq!(replies.len(), 2);
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies[1].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, 0);
    assert_eq!(header.op, 1, "recovery retry must replay the committed op");
    assert_eq!(
        partition.persistence.as_ref().unwrap().head(),
        1,
        "retry must not append another batch"
    );
}

#[compio::test]
async fn committed_pipeline_entries_allow_another_clients_write_before_receipt_fold() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let mut first = checksumless_send_request(partition.namespace(), 1);
    first.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(partition.namespace(), 0));
    partition.on_request(first.clone(), None).await;
    partition.consensus.advance_commit_max(1);
    assert_eq!(partition.consensus.commit_min(), 0);
    assert_eq!(
        partition.consensus.pipeline_len(),
        1,
        "initial admission failed: fatal={:?}, replies={:?}",
        partition.fatal,
        replies.borrow()
    );

    let retry = first.deep_copy();
    let second = first.transmute_header(|header, next: &mut RoutedRequestHeader| {
        *next = header;
        next.client = 2;
    });
    partition.on_request(second, None).await;
    assert!(replies.borrow().is_empty(), "both writes remain in flight");
    assert_eq!(partition.consensus.sequencer().current_sequence(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 2);

    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(partition.dedup.count(), 2);
    assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
    assert_eq!(replies.borrow().len(), 2);
    partition.on_request(retry, None).await;
    let replies = replies.borrow();
    assert_eq!(replies.len(), 3);
    for (_, bytes) in replies.iter() {
        let header = bytemuck::checked::from_bytes::<ReplyHeader>(
            &bytes.as_slice()[..size_of::<ReplyHeader>()],
        );
        assert!(header.has_committed_result());
    }
    assert_eq!(
        replies[2].1.as_slice(),
        replies[0].1.as_slice(),
        "a live retry keeps the original proof"
    );
}

fn send_request_for_client(
    namespace: IggyNamespace,
    client_id: u128,
) -> Message<RoutedRequestHeader> {
    let mut send = checksumless_send_request(namespace, 1).transmute_header(
        |header, next: &mut RoutedRequestHeader| {
            *next = header;
            next.client = client_id;
        },
    );
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(namespace, 0));
    send
}

#[compio::test]
async fn given_in_flight_send_when_storing_committed_offset_should_admit_independent_requests() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    let namespace = partition.namespace();
    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(send_request_for_client(namespace, 2), None)
        .await;
    assert_eq!(partition.consensus.pipeline_len(), 3);
    assert_eq!(partition.consensus.request_queue_len(), 0);
}

#[compio::test]
async fn given_in_flight_send_when_storing_its_offset_should_preserve_the_dependency() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let namespace = partition.namespace();
    partition
        .on_request(send_request_for_client(namespace, 1), None)
        .await;
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert!(
        replies.borrow().is_empty(),
        "the store must await its earlier send, not be rejected"
    );
}

#[compio::test]
async fn given_unstaged_auto_commit_when_another_key_completes_should_admit_only_independent_keys()
{
    let (mut partition, _) = recording_partition_at(0, 3);
    install_test_owner(&mut partition, 7, 42).await;
    install_test_owner(&mut partition, 8, 42).await;
    partition.stats.increment_messages_count(1);
    let namespace = partition.namespace();
    let request = send_request_for_client(namespace, 1).transmute_header(
        |mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        },
    );
    partition.on_request(request, None).await;
    let first = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(7, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(first.replication.is_some());
    let independent = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(8, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(independent.replication.is_some());
    let dependent = partition
        .complete_poll(poll_read_result(
            &partition,
            PollingConsumer::ConsumerGroup(7, 1),
            true,
            Some(0),
        ))
        .unwrap();
    assert!(dependent.replication.is_none());
    assert_eq!(partition.consensus.pipeline_len(), 3);
    assert_eq!(partition.consensus.request_queue_len(), 1);
}

/// A sender drops its context on `HistoryUnavailable`. A caller context from
/// a node ahead of the dispatching one can name an incarnation this replica
/// has not materialized yet, so only applied metadata can prove it gone.
#[compio::test]
async fn given_send_ahead_of_dispatch_metadata_when_admitting_should_report_not_accepted() {
    const INCARNATION: u64 = 7;
    const NEWER_INCARNATION: u64 = 9;
    const DISPATCH_WATERMARK: u64 = 10;
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_created_revision(INCARNATION);
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    let namespace = partition.namespace();
    let send_requiring = |minimum_metadata_op: u64| {
        let mut send = checksumless_send_request(namespace, 1).transmute_header(
            |header, next: &mut RoutedRequestHeader| {
                *next = header;
                next.partition_incarnation = NEWER_INCARNATION;
                next.minimum_metadata_op = minimum_metadata_op;
                next.metadata_watermark = DISPATCH_WATERMARK;
            },
        );
        send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
            .copy_from_slice(&build_segment_record(namespace, 0));
        send
    };

    for (minimum_metadata_op, expected) in [
        (DISPATCH_WATERMARK + 1, IggyError::TransientNotAccepted),
        (DISPATCH_WATERMARK, IggyError::HistoryUnavailable),
    ] {
        let (sender, refusal) = consensus::oneshot_channel();
        partition
            .on_request(send_requiring(minimum_metadata_op), Some(sender))
            .await;
        // Checked before the await: a wrongly admitted send never gets a reply.
        assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
        assert_eq!(
            IggyError::from_code(refusal.await.unwrap().header().status),
            expected,
            "a send requiring metadata op {minimum_metadata_op} dispatched at {DISPATCH_WATERMARK}"
        );
    }
}

/// The primary commits op 2 on this backup's ack and applies it later, so
/// the offer pins op 1. The backup entered its transfer before it heard
/// that op 2 committed, and installing the offer must not erase op 2.
#[compio::test]
async fn given_an_acked_op_above_the_known_commit_when_installing_a_lower_offer_should_refuse() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    let sent = backup.consensus().message_bus().sent_to_replicas.clone();
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, directory.path());
    backup.open_persistence().await.unwrap();

    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.commit = 1;
            header.checksum = header.identity_checksum();
        });
    let third = checksummed_segment_prepare(3, second.header().checksum, 2, b"third")
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.commit = 2;
            header.checksum = header.identity_checksum();
        });
    backup.on_replicate(first.clone()).await;
    backup.on_replicate(second).await;
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    let acked: Vec<_> = sent
        .borrow()
        .iter()
        .filter_map(|(_, frame)| {
            bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                .ok()
                .filter(|header| header.command == Command::PrepareOk)
                .map(|header| header.op)
        })
        .collect();
    assert_eq!(acked, vec![1, 2], "the primary counts this ack toward op 2");
    assert_eq!(backup.consensus().commit_max(), 1);

    backup.consensus().begin_state_transfer_await();
    backup.on_replicate(third).await;
    assert!(!backup.log.journal().inner.holds_op(3));
    assert_eq!(
        backup.consensus().commit_max(),
        1,
        "a transferring backup drops the prepare that names commit 2"
    );

    let body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = backup
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(first.header().checksum),
        checkpoint_prepare: first.as_slice().to_vec(),
        next_offset: 1,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        owners: Vec::new(),
        history_transition: None,
        owner_retirement: None,
        retirement_paid_through: 0,
        machines: None,
    };
    let installed = backup
        .install_state_transfer(&repair_config(), 1, vec![staged], &offsets.encode())
        .await;
    let wal_head = backup.persistence.as_ref().unwrap().head();
    assert!(
        matches!(
            installed,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                    commit_op: 1,
                    ..
                }
            )
        ),
        "the install erased op 2, which this backup acknowledged: {installed:?}, \
             holds_op(2) = {}, WAL head = {wal_head}",
        backup.log.journal().inner.holds_op(2),
    );
    assert!(backup.log.journal().inner.holds_op(2));
    assert_eq!(wal_head, 2);
}

/// The CREATE claim carries the recorded frontier, writes inside an open
/// backoff window, and counts a failure only outside that window.
#[compio::test]
async fn given_a_create_claim_when_the_store_fails_should_write_through_backoff() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(40, 40);
    partition.set_superblock(store.clone(), Some(&recovered));
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(partition.reserve_offsets_through(50).await);
    assert_eq!(store.attempts.get(), 1);
    assert_eq!(
        last_recorded_frontier(&store),
        40,
        "the claim carries the recorded frontier forward"
    );
    assert_eq!(
        last_recorded_reservation(&store),
        67,
        "the claim runs one past the offset plus the lease"
    );

    store.fail_writes.set(true);
    let window_end = partition.consensus().clock_realtime_micros() + 1_000_000;
    partition.superblock_retry_after_micros.set(window_end);
    assert!(!partition.reserve_offsets_through(100).await);
    assert_eq!(
        store.attempts.get(),
        2,
        "the CREATE claim writes inside an open backoff window"
    );
    assert_eq!(
        partition.superblock_write_failures(),
        0,
        "a failure inside the window is not counted"
    );
    assert_eq!(
        partition.superblock_retry_after_micros.get(),
        window_end,
        "a failure inside the window does not move it"
    );

    partition.superblock_retry_after_micros.set(0);
    assert!(!partition.reserve_offsets_through(100).await);
    assert_eq!(store.attempts.get(), 3);
    assert_eq!(
        partition.superblock_write_failures(),
        1,
        "a failure outside the window is counted once"
    );
    assert_ne!(
        partition.superblock_retry_after_micros.get(),
        0,
        "the counted failure arms the next window"
    );
}

/// Under a dispatcher the forced flush only requests owner work. The inline
/// path in `commit_messages_inner` stays with undispatched owners.
#[compio::test]
async fn given_a_dispatcher_when_flushing_should_leave_the_flush_to_the_owner() {
    let (mut partition, _replies) = recording_partition_at(0, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(*prepare.header()));
    });
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.advance_commit_max(1);
    partition.commit_journal(&config).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );

    assert!(!partition.flush_committed_messages(&config).await.unwrap());
    assert_eq!(partition.maintenance.flush_through, Some(1));
    assert!(
        partition.materialization.is_none(),
        "the dispatched flush must not begin materialization inline"
    );
}

#[compio::test]
async fn given_rebuilt_replicated_prepare_above_a_wal_hole_when_acknowledging_should_wait() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    backup.runtime_options.durability = iggy_common::Durability::Replicated;
    backup.open_persistence().await.unwrap();
    let sent = backup.consensus().message_bus().sent_to_replicas.clone();
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let prepare = checksummed_segment_prepare(2, first.header().checksum, 1, b"adopted");
    let header = *prepare.header();
    backup.consensus().sequencer().set_sequence(header.op);
    backup
        .log
        .journal()
        .inner
        .append(prepare.into_frozen())
        .await
        .unwrap();

    assert!(!backup.register_rebuilt_ack(&header));
    backup.acknowledge_prepare(2).await;
    assert!(
        sent.borrow().is_empty(),
        "the WAL has not accepted the missing predecessor"
    );
    assert!(backup.pending_persisted_acks.borrow().contains_key(&2));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 0);

    backup
        .log
        .journal()
        .inner
        .append(first.into_frozen())
        .await
        .unwrap();
    backup.persist_repaired_prefix();
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    let sent = sent.borrow();
    assert_eq!(sent.len(), 1);
    let ack = bytemuck::checked::from_bytes::<PrepareOkHeader>(sent[0].1.as_slice());
    assert_eq!(ack.command, Command::PrepareOk);
    assert_eq!(ack.op, 2);
    assert!(backup.pending_persisted_acks.borrow().is_empty());
}

#[compio::test]
async fn given_unflushed_wal_when_reporting_replication_progress_should_honor_topic_durability() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut backup, _) = recording_partition_at(1, 3);
        backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
        backup.runtime_options.durability = durability;
        backup.open_persistence().await.unwrap();
        let prepare = checksummed_segment_prepare(1, 0, 0, b"written");
        let header = *prepare.header();
        backup.consensus().sequencer().set_sequence(header.op);
        backup
            .log
            .journal()
            .inner
            .append(prepare.clone().into_frozen())
            .await
            .unwrap();
        let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
        persistence.append(prepare.into_frozen(), false).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_written(&header));
        assert!(!persistence.is_durable(&header));

        let expected = Some(consensus::BackupPrefix {
            held_op: header.op,
            received_op: u64::from(!durability.is_persisted()),
        });
        assert_eq!(
            backup.replication_progress(),
            expected,
            "{durability:?}: the held prefix finds gaps, and the received prefix follows \
                 the prepare acknowledgment contract"
        );
        backup.maintenance.flush_through = Some(header.op);
        assert_eq!(
            backup.replication_progress(),
            expected,
            "{durability:?}: ordinary materialization does not replace the held history"
        );
    }
}

#[compio::test]
async fn given_unacked_repair_above_a_wal_hole_when_installing_should_allow_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut backup, directory.path());
    backup.runtime_options.durability = iggy_common::Durability::Persisted;
    backup.open_persistence().await.unwrap();
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    backup.on_replicate(first.clone()).await;
    backup
        .persistence
        .as_ref()
        .unwrap()
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    backup.drive_persistence().await;
    // Op 3 is committed but unacked here: the WAL hole at op 2 holds back
    // its ack, so it must not block the install.
    backup.consensus().advance_commit_max(3);
    let third = checksummed_segment_prepare(3, 0, 2, b"unacked");
    let header = *third.header();
    backup
        .log
        .journal()
        .inner
        .append(third.into_frozen())
        .await
        .unwrap();
    assert!(!backup.register_rebuilt_ack(&header));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 1);

    let body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = backup
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        prepare_checksum: Some(first.header().checksum),
        checkpoint_prepare: first.as_slice().to_vec(),
        next_offset: 1,
        ..Default::default()
    };
    backup.consensus().begin_state_transfer_await();
    let installed = backup
        .install_state_transfer(&repair_config(), 1, vec![staged], &offsets.encode())
        .await
        .unwrap();
    assert_eq!(installed.applied_commit_op, 1);
    assert!(!backup.log.journal().inner.holds_op(3));
    assert_eq!(backup.persistence.as_ref().unwrap().head(), 1);
}

#[compio::test]
async fn given_committed_prepare_when_retransmitted_should_ack_only_matching_history() {
    let mut prepares = Vec::new();
    for client_id in [1, 2] {
        let (mut primary, _) = recording_partition_at(0, 3);
        primary
            .on_request(
                retire_session_request(
                    primary.namespace(),
                    SessionIdentity {
                        client_id,
                        session: 1,
                        metadata_watermark: 1,
                    },
                ),
                None,
            )
            .await;
        let retained = primary.log.journal().inner.repair_entry(1).unwrap();
        prepares.push(
            Message::<PrepareHeader>::try_from(Owned::copy_from_slice(retained.as_slice()))
                .unwrap(),
        );
    }
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(1, 3);
        let sent = partition.consensus().message_bus().sent_to_replicas.clone();
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.open_persistence().await.unwrap();
        let prepare = &prepares[0];
        partition.on_replicate(prepare.clone()).await;
        partition
            .persistence
            .as_ref()
            .unwrap()
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        partition.drive_persistence().await;
        partition.consensus.advance_commit_max(prepare.header().op);
        partition.commit_journal(&repair_config()).await;
        assert!(partition.fatal.is_none(), "{:?}", partition.fatal);
        assert_eq!(partition.consensus.commit_min(), prepare.header().op);
        let offset = partition.offset.load(Ordering::Acquire);
        sent.borrow_mut().clear();

        partition.on_replicate(prepare.clone()).await;
        let acknowledgements: Vec<_> = sent
            .borrow()
            .iter()
            .filter_map(|(replica, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| (*replica, header.op, header.prepare_checksum))
            })
            .collect();
        assert_eq!(
            acknowledgements,
            vec![(0, prepare.header().op, prepare.header().checksum)],
            "a committed retransmit must acknowledge the retained prepare under {durability:?}"
        );
        assert_eq!(partition.offset.load(Ordering::Acquire), offset);
        assert_eq!(partition.consensus.commit_min(), prepare.header().op);
        assert_eq!(partition.consensus.applied_log_contradictions(), 0);
        sent.borrow_mut().clear();

        partition.on_replicate(prepares[1].clone()).await;
        assert!(
            sent.borrow().is_empty(),
            "a conflicting committed prepare must never be acknowledged"
        );
        assert_eq!(partition.offset.load(Ordering::Acquire), offset);
        assert_eq!(
            partition.consensus.applied_log_contradictions(),
            1,
            "a different prepare at an applied op is a contradiction, not an old retransmit"
        );
    }
}

#[compio::test]
async fn live_retransmits_fill_an_announced_gap_only_after_the_matching_prefix() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(1, 3);
        let sent = partition.consensus().message_bus().sent_to_replicas.clone();
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.open_persistence().await.unwrap();
        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
        let third = checksummed_segment_prepare(3, second.header().checksum, 2, b"third");
        let start = Message::<StartViewHeader>::new(size_of::<StartViewHeader>()).transmute_header(
            |_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = TEST_CLUSTER;
                header.group = partition.namespace().inner();
                header.op = third.header().op;
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            },
        );
        partition
            .consensus()
            .handle_start_view(PlaneKind::Partitions, start.header(), &[]);
        assert!(!partition.consensus().view_log_is_pending());
        partition.repair = Some(armed_fetch_session(0, 3, 0));
        partition.apply_repaired_prepare(first.clone()).await;
        assert!(
            !partition.log.journal().inner.holds_op(1),
            "unattributed repair still needs a canonical header above commit"
        );
        partition.repair = None;

        partition.on_replicate(third.clone()).await;
        let fork = first
            .clone()
            .transmute_header(|original, header: &mut PrepareHeader| {
                *header = original;
                header.parent = u128::MAX;
                header.checksum = header.identity_checksum();
            });
        partition.on_replicate(fork).await;
        assert!(!partition.log.journal().inner.holds_op(1));
        assert!(!partition.log.journal().inner.holds_op(3));
        assert!(sent.borrow().is_empty());

        partition.on_replicate(first).await;
        partition.on_replicate(third.clone()).await;
        assert!(
            !partition.log.journal().inner.holds_op(3),
            "op 2 is still missing"
        );
        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        partition.on_replicate(second.clone()).await;
        assert_eq!(partition.consensus().sequencer().current_sequence(), 3);
        partition.on_replicate(third.clone()).await;
        if let Some(persistence) = &partition.persistence {
            persistence
                .drain_with_timeout(compio::runtime::time::sleep)
                .await
                .unwrap();
        }
        partition.drive_persistence().await;
        assert_eq!(
            partition.consensus().last_prepare_checksum(),
            third.header().checksum
        );
        assert!(!partition.consensus().view_log_is_pending());
        let acked: Vec<_> = sent
            .borrow()
            .iter()
            .filter_map(|(_, frame)| {
                bytemuck::checked::try_from_bytes::<PrepareOkHeader>(frame.as_slice())
                    .ok()
                    .filter(|header| header.command == Command::PrepareOk)
                    .map(|header| header.op)
            })
            .collect();
        assert_eq!(
            acked,
            vec![1, 2, 3],
            "every held body must eventually be acknowledged under {durability:?}"
        );
    }
}

#[compio::test]
async fn given_a_backup_that_cannot_journal_a_prepare_when_it_arrives_should_neither_forward_nor_ack()
 {
    const MIDDLE: u8 = 1;
    const TAIL: u8 = 2;
    /// Led by replica 0 in a group of three, like view 0, so only the view
    /// number distinguishes a prepare from another view.
    const LATER_VIEW: u32 = 3;
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let above_gap = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    let later_view = first
        .clone()
        .transmute_header(|original, header: &mut PrepareHeader| {
            *header = original;
            header.view = LATER_VIEW;
            header.checksum = header.identity_checksum();
        });
    let cases = [
        (MIDDLE, true, &first),
        (MIDDLE, false, &above_gap),
        (MIDDLE, true, &later_view),
        (TAIL, true, &first),
        (TAIL, false, &above_gap),
    ];
    for (replica, awaiting_target, prepare) in cases {
        let (mut backup, _) = recording_partition_at(replica, 3);
        let sent = backup.consensus().message_bus().sent_to_replicas.clone();
        if awaiting_target {
            backup.consensus().begin_state_transfer_await();
        }
        let header = *prepare.header();
        let prepare = prepare.clone();
        backup.on_replicate(prepare).await;

        let case = format!(
            "replica {replica}, awaiting target {awaiting_target}, op {}, view {}",
            header.op, header.view
        );
        assert!(!backup.log.journal().inner.holds_op(header.op), "{case}");
        assert!(
            sent.borrow().is_empty(),
            "a backup never forwards or acknowledges refused data: {case}"
        );
    }
}

/// A backup that awaits a transfer target relays nothing outside `Normal`
/// status, where its view is not settled, and nothing at or below its
/// commit point, where the prepare is a late copy of a committed op.
#[compio::test]
async fn given_a_backup_outside_normal_status_or_at_its_commit_point_when_a_prepare_arrives_should_not_relay_it()
 {
    const MIDDLE: u8 = 1;
    const OP: u64 = 1;
    type Setup = fn(&IggyPartition<RecordingBus>);
    let prepare = checksummed_segment_prepare(OP, 0, 0, b"first");
    let cases: [(&str, Setup); 2] = [
        ("outside normal status", |backup| {
            backup.consensus().begin_view_probe();
        }),
        ("at its commit point", |backup| {
            backup.consensus().restore_commit_state(OP, OP);
        }),
    ];
    let mut relayed = Vec::new();
    for (case, setup) in cases {
        let (mut backup, _) = recording_partition_at(MIDDLE, 3);
        let sent = backup.consensus().message_bus().sent_to_replicas.clone();
        setup(&backup);
        backup.consensus().begin_state_transfer_await();
        backup.on_replicate(prepare.clone()).await;
        if !sent.borrow().is_empty() {
            relayed.push(case);
        }
    }
    assert!(relayed.is_empty(), "relayed a prepare {relayed:?}");
}

#[compio::test]
async fn persisted_ack_survives_recovery_transfer_and_a_rewound_head() {
    let (_directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition.consensus().begin_view_probe();
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);

    partition.consensus().init();
    partition.consensus().begin_state_transfer_await();
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);

    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Idle);
    partition
        .consensus()
        .sequencer()
        .set_sequence(header.op - 1);
    partition.drive_persistence().await;
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(acknowledgments.is_empty());

    partition.consensus().sequencer().set_sequence(header.op);
    partition.drive_persistence().await;
    partition.drive_persistence().await;
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert_eq!(acknowledgments.len(), 1);
    let ack = bytemuck::checked::from_bytes::<PrepareOkHeader>(acknowledgments[0].as_slice());
    assert_eq!(ack.op, header.op);
    assert_eq!(ack.prepare_checksum, header.checksum);
}

#[compio::test]
async fn persisted_ack_remains_fenced_after_a_local_commit_failure() {
    let (_directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition.fatal = Some(FatalCommit {
        namespace_raw: partition.namespace().inner(),
        op: header.op,
        operation: Some(Operation::StoreConsumerOffset),
    });
    partition.persistence.as_ref().unwrap().request_checkpoint();
    partition
        .consensus()
        .restore_commit_state(header.op, header.op);
    partition.checkpoint_persistence(&repair_config()).await;
    partition.drive_persistence().await;
    partition.acknowledge_prepare(header.op).await;
    let mut acknowledgments = Vec::new();
    partition
        .consensus()
        .drain_loopback_into(&mut acknowledgments);
    assert!(
        acknowledgments.is_empty(),
        "a fenced partition must never acknowledge"
    );
    assert_eq!(partition.pending_persisted_acks.borrow().len(), 1);
    assert_eq!(
        partition.fatal().unwrap().operation,
        Some(Operation::StoreConsumerOffset)
    );
}

#[compio::test]
async fn receipt_publication_failure_preserves_the_paired_wal_frontier() {
    let (directory, mut partition, header) = partition_with_pending_durable_ack().await;
    partition
        .consensus()
        .restore_commit_state(header.op, header.op);
    assert!(
        partition
            .commit_messages_inner(&repair_config(), true, header.op)
            .await
            .unwrap(),
        "the committed message must materialize before injecting the receipt publication fault"
    );
    let persistence = partition.persistence.as_ref().unwrap().clone();
    let frontier = directory.path().join("prepares-0/frontier");
    let before = std::fs::read(&frontier).unwrap();
    let temporary = directory
        .path()
        .join(format!("prepares-0/receipts-{}.checkpoint.tmp", header.op));
    std::fs::create_dir(temporary).unwrap();
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&repair_config()).await;
    assert!(
        partition.fatal().is_some(),
        "failed receipt publication must fence the partition"
    );
    assert_eq!(
        persistence.checkpoint_op(),
        0,
        "the WAL prefix must remain recoverable"
    );
    assert_eq!(persistence.head(), header.op);
    assert_eq!(std::fs::read(&frontier).unwrap(), before);
    assert!(
        !directory
            .path()
            .join(format!("prepares-0/receipts-{}.checkpoint", header.op))
            .exists()
    );
}

#[compio::test]
async fn checkpoint_paths_include_offset_parent_directories() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("offsets/consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        directory
            .path()
            .join("offsets/groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let (_, directories) = partition.persistence_checkpoint_files(&repair_config());
    assert!(directories.contains(&directory.path().join("offsets")));
    assert!(directories.contains(&directory.path().join("offsets/consumers")));
    assert!(directories.contains(&directory.path().join("offsets/groups")));
    assert!(directories.contains(&directory.path().to_path_buf()));

    let (_, clean) = partition.persistence_checkpoint_files(&repair_config());
    assert_eq!(clean, [directory.path().to_path_buf()]);

    let kind = ConsumerKind::ExternalGroup;
    let external = directory.path().join("offsets/external_groups");
    partition.consumer_offset_dirs[kind.index()] = Some(external.to_string_lossy().into_owned());
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.mark_offset_dirty(kind.index(), 7, true);
    persistence.mark_offset_dirty(kind.index(), 7, false);
    let (files, deleted) = partition.persistence_checkpoint_files(&repair_config());
    assert!(files.is_empty(), "the last key was deleted");
    assert_eq!(
        deleted,
        [
            external,
            directory.path().join("offsets"),
            directory.path().to_path_buf()
        ]
    );

    persistence.reset(0, None);
    let (_, reset) = partition.persistence_checkpoint_files(&repair_config());
    assert_eq!(reset.len(), ConsumerKind::COUNT + 2);
}
