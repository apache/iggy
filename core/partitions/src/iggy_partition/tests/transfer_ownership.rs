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

use std::{
    rc::Rc,
    sync::{Arc, atomic::Ordering},
};

use consensus::{LocalPipeline, PipelineEntry};
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareHeader, PrepareOkHeader, ReplyHeader, RoutedRequestHeader,
    StartViewHeader, batch::BATCH_MESSAGE_HEADER_SIZE,
};
use iggy_common::{ConsumerKind, IggyError};
use server_common::{Message, send_messages::COMMAND_HEADER_SIZE};

use crate::{
    PollingConsumer, iggy_partition::PendingPartitionTransition,
    state_transfer::PartitionTransferUnavailable,
};

#[compio::test]
async fn state_transfer_rejects_corrupted_checkpoint_payload_with_zero_body_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let mut prepare = checksummed_segment_prepare(1, 0, 0, b"payload");
    let checksum = prepare.header().checksum;
    prepare.as_mut_slice()
        [size_of::<PrepareHeader>() + COMMAND_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE] ^= 1;
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
        next_offset: 1,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    assert!(matches!(
        partition
            .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
            .await,
        Err(crate::state_transfer::PartitionInstallError::Offsets(
            crate::state_transfer::ConsumerOffsetsWireError::InvalidPrepareChecksum
        ))
    ));
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(!directory.path().join("prepares-0").exists());
}

#[compio::test]
async fn replicated_transfer_keeps_the_evicted_checkpoint_checksum_with_an_inflight_tail() {
    let origin_directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
    let committed = checksummed_segment_prepare(1, 0, 0, b"committed");
    let inflight = checksummed_segment_prepare(2, committed.header().checksum, 1, b"inflight");
    for prepare in [&committed, &inflight] {
        origin
            .log
            .journal()
            .inner
            .append(prepare.clone().into_frozen())
            .await
            .unwrap();
    }
    origin.consensus().sequencer().set_sequence(2);
    origin
        .consensus()
        .set_last_prepare_checksum(inflight.header().checksum);
    origin.consensus().advance_commit_max(1);
    origin.consensus().advance_commit_min(1);
    origin
        .dedup_mut()
        .commit_capacity(usize::try_from(committed.header().retry_capacity).unwrap())
        .unwrap();
    // Retention can remove all polled segments while the repair ring still
    // serves the checkpoint prepare and the primary keeps accepting writes.
    origin.offset_space.committed_seeded = true;
    origin.offset.store(0, Ordering::Relaxed);
    origin.log.journal().inner.evict_prefix(1);
    assert!(origin.persistence.is_none());
    assert!(origin.log.journal().inner.header_by_op(1).is_none());
    assert!(origin.log.journal().inner.repair_entry(1).is_some());

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.dedup_capacity, origin.dedup().capacity());
    assert_eq!(offsets.prepare_checksum, Some(committed.header().checksum));
    assert_eq!(offsets.checkpoint_prepare, committed.as_slice());
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();
    assert_eq!(receiver.consensus().commit_min(), 1);
    assert_eq!(
        receiver.consensus().last_prepare_checksum(),
        committed.header().checksum
    );
}

/// An install wipes the log above its commit op. A barrier left at an
/// install it dropped would hold the installed owner until commits pass that
/// op, and the auto-commits it refuses are all an idle group would commit.
#[compio::test]
async fn given_barrier_above_the_offer_when_state_transfer_publishes_should_serve_the_installed_owner()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    let installed = partition.consumer_group_owners[&GROUP_ID];
    let successor = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
    let prepare =
        partition.pipeline_local_request(owner_install_request(&partition, &successor), None, None);
    partition.apply_replicated_operation(prepare).await.unwrap();
    assert_eq!(
        partition.acknowledged_op.get(),
        installed_op,
        "the successor may only be discarded if it was never acknowledged"
    );
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.owner_install_barrier, installed_op + 1);

    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: partition.dedup().capacity(),
        required_metadata_frontier: installed.installation.metadata_op,
        owners: vec![installed],
        ..Default::default()
    };
    partition
        .install_state_transfer(
            &repair_config(),
            installed_op,
            Vec::new(),
            &offsets.encode(),
        )
        .await
        .unwrap();
    partition.stats.increment_messages_count(1);
    let read = poll_read_result(
        &partition,
        PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0),
        true,
        Some(0),
    );

    let refusal = complete_poll_and_commit(&mut partition, read).await;
    assert!(refusal.is_none(), "{refusal:?}");
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.group_offset_state(GROUP_ID), (Some(0), Some(0)));
}

#[compio::test]
async fn uncertified_log_view_requires_transfer_before_voting() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(2, 2);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let mut journal = journal::PartitionPrepareJournal::open(
        &directory.path().join("prepares-0"),
        partition.consensus().group(),
        0,
    )
    .await
    .unwrap();
    journal.reset(7, Some(1234)).await.unwrap();
    journal.certify_log_view(1, 7, 1234).await.unwrap();
    drop(journal);
    publish_empty_retry_checkpoint(&partition, 7).await;
    partition.open_persistence().await.unwrap();
    assert!(partition.requires_state_transfer());
    assert!(partition.consensus().is_transferring());
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

#[compio::test]
async fn given_snapshot_transfer_when_writes_evict_its_tail_should_release_only_confirmed_progress_and_expire_idle_peers()
 {
    const PEER: u8 = 1;
    const NONCE: u128 = 1;
    const BYTES: usize = 64 * 1024;
    let mut partition = test_partition_with_replicas(3);
    let budget = Arc::new(crate::RepairMemoryBudget::new(BYTES));
    partition.proof_walk_memory = Some(budget.reservation());
    let journal = &partition.log.journal().inner;
    journal.set_ring_caps(1, u64::MAX);
    assert!(partition.retain_transfer_tail(PEER, NONCE, 0));
    assert!(partition.transfer_tail_available(PEER, NONCE, 1));
    for op in 1..=8 {
        let prepare = checksummed_segment_prepare(op, 0, op - 1, b"tail");
        journal.append(prepare.into_frozen()).await.unwrap();
        journal.evict_prefix(1);
    }
    assert_eq!(journal.repair_retained_from(), Some(2));
    partition.advance_transfer_tail(PEER, consensus::ReplicationProgress::Busy, 9);
    assert_eq!(journal.repair_retained_from(), Some(2));
    partition.advance_transfer_tail(
        PEER,
        consensus::ReplicationProgress::Ready { received_op: 4 },
        10,
    );
    assert_eq!(journal.repair_retained_from(), Some(5));
    partition.expire_transfer_tails(19, 10);
    assert!(partition.touch_transfer_tail(PEER, NONCE, 19));
    partition.renew_transfer_tail_for_repair(PEER, 28);
    partition.expire_transfer_tails(29, 10);
    assert_eq!(
        journal.repair_retained_from(),
        Some(5),
        "a pull keeps the suffix without proving its prefix"
    );
    partition.expire_transfer_tails(38, 10);
    assert_eq!(journal.repair_retained_from(), Some(8));
    assert!(!partition.touch_transfer_tail(PEER, NONCE, 29));
    assert!(
        !partition.retain_transfer_tail(PEER, NONCE, 29),
        "an expired nonce must not rebuild a different manifest"
    );
    assert!(partition.retain_transfer_tail(PEER, NONCE + 1, 29));
    assert!(
        !partition.transfer_tail_available(PEER, NONCE + 1, 1),
        "an old snapshot cannot promise already-missing operations"
    );
    assert!(partition.transfer_tail_available(PEER, NONCE + 1, 8));
    assert!(budget.reservation().try_add(BYTES));
}

#[compio::test]
async fn given_transfer_flush_when_tick_promotes_a_queued_send_should_ack_it_on_the_primary() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _replies, mut config, parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    config.messages_required_to_save = 8;
    let prepare = checksummed_segment_prepare(2, parent, 1, b"resident");
    let header = *prepare.header();
    persistence
        .append(prepare.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(header));
    });
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(partition.log.journal().inner.resident_count(), 1);
    partition.consensus.sequencer().set_sequence(2);
    partition
        .consensus
        .set_last_prepare_checksum(header.checksum);
    for _ in 0..2 {
        let order = partition.next_local_request_order().unwrap();
        let request = consensus::RequestEntry::with_sender(
            checksumless_send_request(partition.namespace(), 1),
            None,
        );
        assert!(partition.queue_local_request(request, order).is_ok());
    }

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    assert!(partition.drive_transfer_plan());
    assert!(partition.maintenance.transfer_flush);

    partition.resume_queued_requests().await;
    settle_partition_io(&mut partition, &config).await;
    partition.resume_queued_requests().await;

    assert!(
        partition.consensus.with_pipeline(
            |pipeline| pipeline.entry_by_op(3).is_some() && pipeline.entry_by_op(4).is_some()
        ),
        "both queued sends must be promoted"
    );
    let mut loopback = Vec::new();
    partition.consensus().drain_loopback_into(&mut loopback);
    let acked: Vec<u64> = loopback
        .into_iter()
        .map(|ack| ack.try_into_typed::<PrepareOkHeader>().unwrap().header().op)
        .collect();
    assert!(
        acked.contains(&4),
        "control: the primary acks a send promoted after the flush: {acked:?}"
    );
    assert!(
        acked.contains(&3),
        "a send promoted during the transfer flush must retain its self-ack: {acked:?}"
    );
    assert!(persistence.head() >= 4);
}

#[compio::test]
async fn given_backup_replica_when_granted_a_primary_transfer_should_refuse_and_keep_committing() {
    const BACKUP: u8 = 2;
    for target in [1, BACKUP] {
        let root = tempfile::tempdir().unwrap();
        let mut config = repair_config();
        config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
        let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
        std::fs::create_dir_all(&directory).unwrap();
        let (mut backup, _) = recording_partition_at(BACKUP, 3);
        backup.set_partition_dir(directory.to_string_lossy().into_owned());
        set_offset_dirs_under(&mut backup, &directory);
        backup.open_persistence().await.unwrap();
        backup.log.retire_front().unwrap();
        backup.install_empty_segment(&config, 0).await.unwrap();
        backup.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let persistence = Rc::clone(backup.persistence.as_ref().unwrap());
        let writer = hold_wal_writer(&persistence);
        let first = checksummed_segment_prepare(1, 0, 0, b"before-grant");
        let parent = first.header().checksum;
        backup.on_replicate(first).await;
        assert!(
            !backup.defer_primary_transfer(crate::PreparedPrimaryTransfer {
                target,
                ..primary_transfer_grant(&backup)
            }),
            "backup {BACKUP} accepted a grant for target {target}"
        );
        assert!(backup.transition.is_none());
        assert!(persistence.has_capacity(size_of::<PrepareHeader>()));

        // Replica 1 acks op 2, so the primary commits it. An unfenced
        // backup journals the forwarded prepare and commits it as well.
        backup
            .on_replicate(checksummed_segment_prepare(2, parent, 1, b"after-grant"))
            .await;
        writer.await;
        commit_on_heartbeat(&mut backup, &config, 2).await;
        settle_partition_io(&mut backup, &config).await;
        assert_eq!(backup.consensus.sequencer().current_sequence(), 2);
        assert_eq!(persistence.written_op(), 2);
        assert_eq!(backup.consensus.commit_min(), 2);
    }
}

#[compio::test]
async fn given_planned_primary_transfer_when_a_view_frame_arrives_should_preserve_real_transition()
{
    let (mut partition, _) = recording_partition_at(0, 3);
    assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
    let message = Message::<StartViewHeader>::new(size_of::<StartViewHeader>()).transmute_header(
        |_, header: &mut StartViewHeader| {
            header.command = Command::StartView;
            header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
            header.view = 1;
            header.replica = 1;
        },
    );
    partition.defer_view_transition(server_common::MessageBag::StartView(message));
    partition.cancel_primary_transfer();
    assert!(matches!(
        partition.transition,
        Some(PendingPartitionTransition::View { .. })
    ));
    assert!(matches!(partition.resume_io(&repair_config()).await,
        crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
        if message.header().view == 1));
}

#[compio::test]
async fn given_queued_requests_when_primary_transfer_drains_should_refuse_them_and_hand_off() {
    const GROUP_ID: u64 = 8;
    const OWNER_ID: u128 = 42;
    const ADMITTED_CLIENT: u128 = 100;
    const QUEUED_CLIENT: u128 = 101;
    const STEPS_MAX: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
    install_test_owner(&mut partition, GROUP_ID, OWNER_ID).await;
    set_offset_dirs_under(&mut partition, directory.path());
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.stats.increment_messages_count(1);
    for (client_id, consumer_id) in [(ADMITTED_CLIENT, 7), (QUEUED_CLIENT, 9)] {
        let store = store_offset_request(
            client_id,
            1,
            ConsumerKind::Consumer,
            consumer_id,
            0,
            AckLevel::Quorum,
        )
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            *next = header;
        });
        partition.on_request(store, None).await;
    }
    let installation = next_test_installation(&partition, GROUP_ID, OWNER_ID + 1);
    let (sender, receiver) = consensus::oneshot_channel();
    partition
        .on_request(
            owner_install_request(&partition, &installation),
            Some(sender),
        )
        .await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(partition.consensus.request_queue_len(), 2);
    assert_eq!(partition.pending_owner_install.get(), Some(installation));

    assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
    partition
        .consensus
        .advance_commit_max(partition.consensus.sequencer().current_sequence());
    let config = repair_config();
    let mut transfer = None;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::PrimaryTransfer(request) => {
                transfer = Some(request);
                break;
            }
            crate::PartitionIoStep::Pending => break,
            _ => panic!("the drain must only commit and hand off"),
        }
    }
    let Some(request) = transfer else {
        panic!(
            "a queued request must not hold the handoff: {} queued, {} prepared, commit {}/{}",
            partition.consensus.request_queue_len(),
            partition.consensus.pipeline_len(),
            partition.consensus.commit_min(),
            partition.consensus.commit_max(),
        );
    };
    assert!(
        !partition
            .consensus
            .request_primary_transfer(request.expected_view, request.target)
            .is_empty()
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code()
    );
    let statuses = |client_id| {
        replies
            .borrow()
            .iter()
            .filter(|(client, _)| *client == client_id)
            .map(|(_, frame)| {
                bytemuck::checked::from_bytes::<ReplyHeader>(
                    &frame.as_slice()[..size_of::<ReplyHeader>()],
                )
                .status
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(statuses(ADMITTED_CLIENT), [0]);
    assert_eq!(
        statuses(QUEUED_CLIENT),
        [IggyError::TransientNotAccepted.as_code()]
    );
    assert!(
        partition.pending_owner_install.get().is_none(),
        "a refused install must release its latch"
    );
    assert!(partition.fatal().is_none());
}

#[compio::test]
async fn given_unapplied_commit_when_primary_transfer_drains_should_apply_before_handoff() {
    const STEPS_MAX: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _replies, config, parent) =
        partition_with_committed_send(directory.path(), iggy_common::Durability::Replicated, 3)
            .await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let prepare = checksummed_segment_prepare(2, parent, 1, b"committed-before-transfer");
    let header = *prepare.header();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .append(prepare.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition
        .consensus
        .with_pipeline_mut(|pipeline| pipeline.push(PipelineEntry::new(header)));
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(header.op);
    partition
        .consensus
        .set_last_prepare_checksum(header.checksum);
    assert_eq!(partition.consensus.commit_min(), 1);
    assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
    assert!(
        matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ),
        "an accepted prepare must finish before the primary hands off"
    );
    partition.consensus.advance_commit_max(header.op);
    let mut finished = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::PrimaryTransfer(request) => {
                assert_eq!(partition.consensus.commit_min(), header.op);
                assert!(
                    !partition
                        .consensus
                        .request_primary_transfer(request.expected_view, request.target)
                        .is_empty()
                );
                finished = true;
                break;
            }
            _ => panic!("committed prefix must drain under the admission fence"),
        }
    }
    assert!(finished);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
    assert!(partition.fatal().is_none());
}

#[compio::test]
async fn given_accepted_no_ack_write_when_primary_transfer_drains_should_finish_its_reply() {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    const STEPS_MAX: usize = 32;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    set_offset_dirs_under(&mut partition, directory.path());
    partition.stats.increment_messages_count(1);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let store = store_offset_request(
        CLIENT,
        1,
        ConsumerKind::Consumer,
        CONSUMER,
        0,
        AckLevel::NoAck,
    );
    partition
        .apply_consumer_offset_no_ack(
            Box::new(*store.header()),
            ConsumerKind::Consumer,
            CONSUMER,
            Some(0),
            None,
        )
        .await;
    let config = repair_config();
    let captured = loop {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Ready(plan) => {
                break partition.capture_io(plan, &config).unwrap().unwrap();
            }
            _ => panic!("accepted store must reach its file write"),
        }
    };
    assert_eq!(
        captured.identity.continuation,
        crate::PartitionIoContinuation::NoAck
    );
    assert!(partition.defer_primary_transfer(primary_transfer_grant(&partition)));
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    assert!(replies.borrow().is_empty());
    let result = captured.job.execute().await;
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    partition.accept_io(captured.identity, result).unwrap();
    if let Some(gate) = captured.gate {
        gate.release();
    }
    let mut finished = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Ready(plan) => {
                execute_partition_io_plan(&mut partition, &config, plan).await;
            }
            crate::PartitionIoStep::PrimaryTransfer(_) => {
                finished = true;
                break;
            }
            _ => panic!("accepted store must settle before transfer"),
        }
    }
    assert!(finished);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(
            usize::try_from(CONSUMER).unwrap(),
            0,
        )),
        Some(0)
    );
    assert_eq!(replies.borrow().len(), 1);
    assert_eq!(
        bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
        )
        .status,
        0
    );
    assert!(
        !bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
        )
        .has_committed_result()
    );
    assert!(partition.no_ack.is_empty());
    assert_eq!(partition.consensus.view(), 0);
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_an_index_sync_failure_when_installing_state_transfer_should_refuse_backup_publication()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    let partition_dir = directory.path().to_string_lossy().into_owned();
    partition.set_partition_dir(partition_dir.clone());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    for kind in ["consumers", "groups", "external_groups"] {
        std::fs::create_dir_all(directory.path().join("offsets").join(kind)).unwrap();
    }
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(format!("{partition_dir}/offsets/consumers"));
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(format!("{partition_dir}/offsets/groups"));
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] =
        Some(format!("{partition_dir}/offsets/external_groups"));
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
    persistence
        .append(prepare.clone().into_frozen(), true)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.restore_commit_state(1, 1);
    persistence.checkpoint(1);
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert_eq!(persistence.checkpoint_op(), 1);

    let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
        .await
        .unwrap();
    assert_eq!(
        writer
            .save_indexes_buffered_at(vec![1; 32], 0)
            .await
            .unwrap(),
        32
    );
    let active = partition.log.index_writers().len() - 1;
    partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: partition.dedup().capacity(),
        required_metadata_frontier: 0,
        prepare_checksum: Some(prepare.header().checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
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

    let result = partition
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await;

    assert!(matches!(
        result,
        Err(crate::state_transfer::PartitionInstallError::SwapIo { .. })
    ));
    assert!(partition.fatal().is_some());
    assert!(!directory.path().join(".install-backup").exists());
}

#[compio::test]
async fn checkpoint_planning_and_capture_failures_fence_during_transfer_flush() {
    for fail_capture in [false, true] {
        let (mut partition, _) = recording_partition();
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        partition.maintenance.transfer_flush = true;
        partition.maintenance.checkpoint = Some(PendingCheckpoint::Receipts(1, vec![1, 2, 3]));
        if !fail_capture {
            partition.io_bytes_max.set(0);
        }
        let config = repair_config();
        let step = partition.resume_io(&config).await;
        if fail_capture {
            let crate::PartitionIoStep::Ready(plan) = step else {
                panic!("receipt checkpoint must reach capture");
            };
            let Err(error) = partition.capture_io(plan, &config) else {
                panic!("capture must fail without a partition directory");
            };
            assert!(
                matches!(&partition.maintenance.checkpoint, Some(PendingCheckpoint::Receipts(1, bytes)) if bytes == &[1, 2, 3])
            );
            partition.reject_io_capture(plan.continuation, error);
        } else {
            assert!(matches!(step, crate::PartitionIoStep::Pending));
        }
        assert!(
            partition.fatal().is_some(),
            "checkpoint failure must fence before transfer-flush recovery; fail_capture: {fail_capture}"
        );
    }
}

#[compio::test]
async fn given_multi_segment_transfer_when_installing_should_keep_writers_on_the_tail_only() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let partition_dir = directory.path().to_string_lossy().into_owned();
    partition.set_partition_dir(partition_dir.clone());
    set_offset_dirs_under(&mut partition, directory.path());
    let mut prepares = Vec::new();
    let mut parent = 0;
    for offset in 0..3u64 {
        let prepare = checksummed_segment_prepare(offset + 1, parent, offset, b"transferred");
        parent = prepare.header().checksum;
        prepares.push(prepare);
    }
    let mut staged = Vec::new();
    for (offset, prepare) in (0u64..).zip(&prepares) {
        let body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let artifact = consensus::StateArtifact::for_bytes(
            consensus::state_manifest::artifact_kind::SEGMENT_LOG,
            offset,
            &body,
        );
        staged.push(
            partition
                .spill_transfer_segment(&artifact, body, None)
                .await
                .unwrap(),
        );
    }
    let last = prepares.last().unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(last.header().checksum),
        checkpoint_prepare: last.as_slice().to_vec(),
        next_offset: 3,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    partition
        .install_state_transfer(&repair_config(), 3, staged, &offsets.encode())
        .await
        .unwrap();

    let tail = partition.log.segments().len() - 1;
    assert_eq!(
        tail, 2,
        "each staged segment must install as its own segment"
    );
    for index in 0..tail {
        let storage = &partition.log.storages()[index];
        assert!(
            storage.messages_size.is_none() && storage.index_size.is_none(),
            "sealed segment {index} must not keep write cursors"
        );
        assert!(
            partition.log.messages_writers()[index].is_none()
                && partition.log.index_writers()[index].is_none(),
            "sealed segment {index} must not keep a writer open"
        );
    }
    assert!(partition.log.storages()[tail].index_size.is_some());
    assert!(partition.log.index_writers()[tail].is_some());
}

/// Steady writes keep an applied op resident on every round, and the
/// heartbeats a receiver takes during the pull move its `commit_max` past
/// whatever op an offer names. The plan pinned by the first flush carries
/// the build, a primary still applying later commits serves it, and the
/// receiver installs the pinned op and leaves the later ones to repair.
#[compio::test]
async fn given_expired_transfer_pin_when_hashing_finishes_should_request_a_fresh_plan() {
    const RING_CAPACITY: usize = 1;
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.log.journal().inner.set_repair_retention(true);
    partition
        .log
        .journal()
        .inner
        .set_ring_caps(RING_CAPACITY, u64::MAX);

    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    settle_partition_io(&mut partition, &config).await;
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;
    let planned_len = partition.log.active_segment().size.as_bytes_u64();

    for op in 2..=3 {
        journal_send_batch(&mut partition, op).await;
        partition.consensus().advance_commit_max(op);
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(
        partition.log.journal().inner.repair_retained_from(),
        Some(3)
    );

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    assert!(
        !partition.segment_checksum_cache.borrow().is_empty(),
        "an expired pin must keep completed checksum work"
    );
    settle_partition_io(&mut partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();
    assert_eq!(offer.commit_op, 3);
    assert!(offer.segments[0].entry.len > planned_len);
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.next_offset, 3);
}

#[compio::test]
async fn transfer_plan_pins_before_commits_arriving_during_its_flush() {
    let mut config = repair_config();
    config.messages_required_to_save = 8;
    let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Progress
    ));
    assert!(partition.maintenance.transfer_flush);

    journal_send_batch(&mut partition, 2).await;
    partition.consensus().advance_commit_max(2);
    let headers = partition.collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &config);
    partition
        .handle_committed_entries(headers, &config, false)
        .await;
    assert!(
        partition.pending_commit.is_none(),
        "the transfer must pin before selecting another commit"
    );
    settle_partition_io(&mut partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();
    assert_eq!(offer.commit_op, 1);
    assert_eq!(
        partition.consensus().commit_min(),
        2,
        "commits resume once the plan is pinned"
    );
    assert!(!partition.maintenance.transfer_flush);
}

/// A receiver that already holds sealed segments of an offer links them to
/// the staging names instead of pulling them. The install then removes the
/// old names, sweeps the other staging files and renames the links, so the
/// installed sealed segments are the files that the receiver wrote. The
/// install also stamps every installed segment with its checksum, which
/// lets the next transfer reuse them too.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_receiver_holding_sealed_segments_when_installing_an_offer_should_reuse_them_without_a_copy()
 {
    let records = segment_records(ORIGIN_OPS);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let held = &records[..usize::try_from(RECEIVER_OPS).unwrap()];
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(held)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let sealed = [0, BATCHES_PER_SEGMENT];
    let written = sealed.map(|start_offset| log_inode(directory.path(), start_offset));

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(
        adopted,
        [0, 1],
        "the two sealed segments that the receiver holds"
    );
    assert_eq!(
        sealed.map(|start_offset| log_inode(directory.path(), start_offset)),
        written,
        "the installed sealed segments are the files that the receiver wrote"
    );
    assert_eq!(
        segment_file_bytes(directory.path()),
        offered_segment_bytes(&offer)
    );
    assert!(
        std::fs::read_dir(directory.path())
            .unwrap()
            .all(|entry| !entry
                .unwrap()
                .path()
                .to_string_lossy()
                .ends_with(".staging")),
        "the install renamed or swept every staging name"
    );
    assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
    let memos = receiver.segment_checksum_cache.borrow();
    for entry in offer.segments.iter().map(|source| source.entry) {
        assert_eq!(
            memos
                .get(&entry.frontier)
                .and_then(|memo| memo.checksum_of(entry.len)),
            Some(entry.checksum),
            "segment {} keeps its installed checksum",
            entry.frontier
        );
    }
}

#[compio::test]
async fn given_busy_wal_when_primary_transfer_drains_should_fence_admission_and_release_pause() {
    for expire in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.set_created_revision(7);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        let writer = hold_wal_writer(&persistence);
        let request = primary_transfer_grant(&partition);
        for (created_revision, expected_view, expires_at) in [
            (
                request.created_revision + 1,
                request.expected_view,
                request.expires_at,
            ),
            (
                request.created_revision,
                request.expected_view + 1,
                request.expires_at,
            ),
            (
                request.created_revision,
                request.expected_view,
                std::time::Instant::now(),
            ),
        ] {
            assert!(
                !partition.defer_primary_transfer(crate::PreparedPrimaryTransfer {
                    created_revision,
                    expected_view,
                    target: request.target,
                    expires_at,
                })
            );
            assert!(partition.transition.is_none());
            assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
        }
        assert!(partition.defer_primary_transfer(request));
        let config = repair_config();
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ));
        assert!(!persistence.has_capacity(size_of::<PrepareHeader>()));
        partition
            .on_request(
                store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::NoAck),
                None,
            )
            .await;
        let status = bytemuck::checked::from_bytes::<ReplyHeader>(
            &replies.borrow()[0].1.as_slice()[..size_of::<ReplyHeader>()],
        )
        .status;
        assert_eq!(status, IggyError::TransientNotAccepted.as_code());
        assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
        assert!(!partition.defer_primary_transfer(primary_transfer_grant(&partition)));

        if expire {
            let Some(PendingPartitionTransition::PrimaryTransfer { request, .. }) =
                &mut partition.transition
            else {
                panic!("the held writer must keep its grant");
            };
            request.expires_at = std::time::Instant::now();
            assert!(matches!(
                partition.resume_io(&config).await,
                crate::PartitionIoStep::Pending
            ));
            assert!(partition.transition.is_none());
            assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
        }
        writer.await;
        let step = partition.resume_io(&config).await;
        if expire {
            assert!(matches!(step, crate::PartitionIoStep::Pending));
        } else {
            let crate::PartitionIoStep::PrimaryTransfer(request) = step else {
                panic!("settled WAL must release the transfer continuation");
            };
            assert_eq!(request.created_revision, 7);
            assert_eq!(request.expected_view, 0);
            assert_eq!(request.target, 1);
        }
        assert!(persistence.has_capacity(size_of::<PrepareHeader>()));
        assert_eq!(partition.consensus.view(), 0);
        assert!(partition.transition.is_none());
        assert!(partition.fatal().is_none());
    }
}

#[compio::test]
async fn promotion_waits_for_the_matching_local_prepare_to_be_durable() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
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
    let header = *prepare.header();
    let persistence = partition.persistence.as_ref().unwrap();
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(!partition.register_rebuilt_ack(&header));
    assert!(partition.pending_persisted_acks.borrow().contains_key(&1));
    partition.start_persistence();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(partition.register_rebuilt_ack(&header));
}
