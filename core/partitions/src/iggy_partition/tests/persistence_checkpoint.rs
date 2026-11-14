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

use std::rc::Rc;

use bytes::Bytes;
use consensus::PipelineEntry;
use iggy_binary_protocol::{
    AckLevel, Operation, PrepareHeader, RoutedRequestHeader,
    requests::partitions::TransitionPartitionHistoryRequest,
};
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset};

use crate::{IggyIndex, IggyIndexReader, IggyPartition, iggy_partition::CommitPhase};

/// A message-less replica that falls behind catches up only by transfer. A donor that
/// restarted after a WAL checkpoint still serves the prepare at that op, which a
/// receiver keeping prepare history needs to install the offer.

#[compio::test]
async fn given_message_less_donor_restarted_after_checkpoint_when_persistent_replica_transfers_should_hold_its_state()
 {
    let (_origin_directory, origin, transition) = message_less_origin().await;
    let commit_op = origin.consensus().commit_min();
    let donor_directory = tempfile::tempdir().unwrap();
    let (config, mut donor) = Box::pin(restarted_replica_of(
        &origin,
        donor_directory.path(),
        1,
        commit_op,
    ))
    .await;
    // A view change makes the restarted replica the primary.
    donor.consensus.set_view(1);
    let offer = donor.state_transfer_offer(&config).await.unwrap();
    let receiver_directory = tempfile::tempdir().unwrap();
    let (receiver_config, mut receiver) =
        replica_with_prepare_history(receiver_directory.path(), 2).await;
    receiver
        .install_state_transfer(
            &receiver_config,
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
    assert!(
        receiver
            .log
            .journal()
            .inner
            .repair_entry(commit_op)
            .is_some()
    );
}

#[compio::test]
async fn given_installed_owner_when_checkpoint_and_restart_should_commit_group_offset() {
    const GROUP_ID: u32 = 7;
    const CLIENT_ID: u128 = 42;
    let directory = tempfile::tempdir().unwrap();
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let checkpoint = install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
    let owner = partition.consumer_group_owner(u64::from(GROUP_ID)).unwrap();
    partition
        .persist_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    drop(partition);

    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(checkpoint)
        .await
        .unwrap();
    assert_eq!(
        recovered.consumer_group_owner(u64::from(GROUP_ID)),
        Some(owner),
        "checkpoint recovery must retain the installed owner before replaying group offsets",
    );
    recovered
        .consensus
        .restore_commit_state(checkpoint, checkpoint);
    recovered.consensus.sequencer().set_sequence(checkpoint);
    let mut send = checksumless_send_request(recovered.namespace(), 1);
    send.as_mut_slice()[size_of::<RoutedRequestHeader>()..]
        .copy_from_slice(&build_segment_record(recovered.namespace(), 0));
    let send = send.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = recovered.required_metadata_frontier;
        *next = header;
    });
    recovered.on_request(send, None).await;
    recovered.consensus.advance_commit_max(checkpoint + 1);
    recovered.commit_journal(&repair_config()).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    let request = store_offset_request(
        CLIENT_ID,
        1,
        ConsumerKind::ConsumerGroup,
        GROUP_ID,
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = recovered.required_metadata_frontier;
        header.owner_generation = owner.generation;
        *next = header;
    });
    recovered.on_request(request, None).await;
    let offset_op = recovered.consensus.sequencer().current_sequence();
    assert_eq!(offset_op, checkpoint + 2);
    recovered.consensus.advance_commit_max(offset_op);
    recovered.commit_journal(&repair_config()).await;
    assert!(recovered.fatal().is_none(), "{:?}", recovered.fatal());
    assert_eq!(recovered.consensus.commit_min(), offset_op);
    assert_eq!(
        recovered
            .durable_consumer_offsets
            .get(ConsumerKind::ConsumerGroup, GROUP_ID)
            .map(|state| state.committed_offset),
        Some(0),
    );
}

#[compio::test]
async fn given_deleted_history_when_checkpoint_and_restart_should_preserve_the_fence() {
    for dispatched in [false, true] {
        const METADATA_OP: u64 = 10;
        let directory = tempfile::tempdir().unwrap();
        let mut config = repair_config();
        config.path_layout.streams_root = directory.path().to_string_lossy().into_owned();
        let path = config.get_partition_path(1, 1, 0);
        std::fs::create_dir_all(&path).unwrap();
        std::fs::create_dir(std::path::Path::new(&path).join("prepares-0")).unwrap();
        let mut partition = test_partition();
        partition.set_partition_dir(path.clone());
        if dispatched {
            partition.set_io_notifier(
                Rc::new(|_, _| {}),
                crate::largest_legal_job_charge().unwrap(),
            );
        }
        let transition = TransitionPartitionHistoryRequest {
            incarnation: partition.created_revision(),
            metadata_op: METADATA_OP,
        };
        let request = partition_control_request(
            &partition,
            Operation::TransitionPartitionHistory,
            &transition.to_bytes(),
            METADATA_OP,
        );
        partition.on_request(request, None).await;
        let checkpoint = partition.consensus.sequencer().current_sequence();
        assert_eq!(checkpoint, 1);
        partition.consensus.advance_commit_max(checkpoint);
        partition.commit_journal(&config).await;
        if dispatched {
            settle_partition_io(&mut partition, &config).await;
        }
        assert_eq!(partition.consensus.commit_min(), checkpoint);
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        assert!(partition.history_deleted());
        partition
            .persist_retry_checkpoint(checkpoint)
            .await
            .unwrap();
        drop(partition);

        let mut recovered = test_partition();
        recovered.set_partition_dir(path);
        recovered
            .restore_retry_checkpoint(checkpoint)
            .await
            .unwrap();
        recovered
            .consensus
            .restore_commit_state(checkpoint, checkpoint);
        assert!(recovered.history_deleted());
        assert_eq!(
            recovered.installed_history_transition(&transition),
            Some(checkpoint)
        );
    }
}

#[compio::test]
async fn given_fence_awaiting_directory_sync_when_checkpoint_is_due_should_recover_the_fence() {
    const FIRST_GROUP_ID: u64 = 1;
    const SECOND_GROUP_ID: u64 = 2;
    const CLIENT_ID: u128 = 42;
    // Only the second resident control op makes the flush due, so the owner
    // install leaves the segment names dirty for the fence to sync.
    const CONTROL_OPS_PER_FLUSH: u32 = 2;
    for operation in [
        Operation::InstallConsumerGroupOwner,
        Operation::TransitionPartitionHistory,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut config = repair_config();
        config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
        let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
        std::fs::create_dir_all(&directory).unwrap();
        let (mut partition, _) = recording_partition_at(0, 1);
        partition.set_partition_dir(directory.to_string_lossy().into_owned());
        set_offset_dirs_under(&mut partition, &directory);
        partition.runtime_options.messages_required_to_save = Some(CONTROL_OPS_PER_FLUSH);
        partition.open_persistence().await.unwrap();
        partition.log.retire_front().unwrap();
        partition.install_empty_segment(&config, 0).await.unwrap();
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

        let first = next_test_installation(&partition, FIRST_GROUP_ID, CLIENT_ID);
        partition
            .on_request(owner_install_request(&partition, &first), None)
            .await;
        persistence.drain().await.unwrap();
        partition.consensus.advance_commit_max(1);
        partition.commit_journal(&config).await;
        assert_eq!(partition.consensus.commit_min(), 1);

        let second = next_test_installation(&partition, SECOND_GROUP_ID, CLIENT_ID);
        let body = if operation == Operation::InstallConsumerGroupOwner {
            second.to_bytes()
        } else {
            TransitionPartitionHistoryRequest {
                incarnation: partition.created_revision(),
                metadata_op: second.metadata_op,
            }
            .to_bytes()
        };
        let fence_applied = |partition: &IggyPartition<RecordingBus>| {
            if operation == Operation::InstallConsumerGroupOwner {
                partition.consumer_group_owner(SECOND_GROUP_ID) == Some(second.owner)
            } else {
                partition.history_deleted()
            }
        };
        partition
            .on_request(
                partition_control_request(&partition, operation, &body, second.metadata_op),
                None,
            )
            .await;
        persistence.drain().await.unwrap();
        partition.consensus.advance_commit_max(2);
        partition.commit_journal(&config).await;
        assert!(
            matches!(
                partition
                    .pending_commit
                    .as_ref()
                    .map(|pending| pending.phase),
                Some(CommitPhase::SegmentDirectory)
            ),
            "{operation:?} must wait for the directory sync"
        );
        assert_eq!(partition.consensus.commit_min(), 1);
        assert!(
            fence_applied(&partition),
            "{operation:?} applies before its commit publishes"
        );

        // Shard ticks: one while the directory sync is pending, one after it.
        persistence.request_checkpoint();
        if partition.needs_persistence_checkpoint() {
            partition.checkpoint_persistence(&config).await;
        }
        settle_partition_io(&mut partition, &config).await;
        assert_eq!(partition.consensus.commit_min(), 2);
        if partition.needs_persistence_checkpoint() {
            partition.checkpoint_persistence(&config).await;
        }
        settle_partition_io(&mut partition, &config).await;
        persistence.drain().await.unwrap();
        settle_partition_io(&mut partition, &config).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        drop(partition);
        drop(persistence);

        let (mut recovered, _) = recording_partition_at(0, 1);
        recovered.set_partition_dir(directory.to_string_lossy().into_owned());
        set_offset_dirs_under(&mut recovered, &directory);
        let recovery = recovered.open_persistence().await;
        assert!(
            recovery.is_ok(),
            "{operation:?}: recovery refused the published checkpoint: {recovery:?}"
        );
        assert_eq!(
            recovered
                .persistence
                .as_ref()
                .map(|persistence| persistence.checkpoint_op()),
            Some(2),
            "{operation:?}: the deferred checkpoint must run once the fence publishes"
        );
        assert_eq!(
            recovered.consumer_group_owner(FIRST_GROUP_ID),
            Some(first.owner)
        );
        assert!(fence_applied(&recovered), "{operation:?}");
    }
}

#[compio::test]
async fn receipt_checkpoint_excludes_consumer_offsets_and_restores_retry_protection() {
    const CHECKPOINT_OP: u64 = 1;
    const METADATA_FRONTIER: u64 = 17;
    const CLIENT: u128 = 42;
    const SESSION: u64 = 4;
    const CONSUMER_ID: u32 = 7;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    std::fs::create_dir(directory.path().join("prepares-0")).unwrap();
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, CONSUMER_ID, 10, 10);
    partition.consumer_offsets.pin().insert(
        CONSUMER_ID as usize,
        ConsumerOffset::new(ConsumerKind::Consumer, CONSUMER_ID, 10, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, CONSUMER_ID, 11, 11);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(CONSUMER_ID as usize),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, CONSUMER_ID, 11, String::new()),
    );
    partition.required_metadata_frontier = METADATA_FRONTIER;
    partition
        .dedup_mut()
        .commit_capacity(consensus::PARTITION_DEDUP_CLIENTS_MAX)
        .unwrap();
    let reply = consensus::build_reply_message(
        &PrepareHeader {
            client: CLIENT,
            request: 1,
            session: SESSION,
            op: CHECKPOINT_OP,
            operation: Operation::StoreConsumerOffset,
            ..Default::default()
        },
        &Bytes::new(),
    );
    partition
        .dedup_mut()
        .commit_partition_reply(1, SESSION, reply)
        .unwrap();
    partition
        .consensus()
        .restore_commit_state(CHECKPOINT_OP, CHECKPOINT_OP);
    let path = partition
        .persist_retry_checkpoint(CHECKPOINT_OP)
        .await
        .unwrap();
    let bytes = std::fs::read(path).unwrap();
    let content = consensus::le_cursor::split_verified_trailer(&bytes).unwrap();
    let mut cursor = consensus::le_cursor::LeCursor::new(content);
    cursor.take(size_of::<[u8; 4]>()).unwrap();
    assert_eq!(cursor.u64().unwrap(), partition.consensus().group());
    assert_eq!(cursor.u64().unwrap(), partition.created_revision());
    assert_eq!(cursor.u64().unwrap(), CHECKPOINT_OP);
    let wire = crate::state_transfer::ConsumerOffsetsWire::decode(cursor.remaining()).unwrap();
    assert!(wire.consumers.is_empty() && wire.groups.is_empty());
    assert_eq!(wire.checkpoint_prepare, b"");
    assert_eq!(wire.required_metadata_frontier, METADATA_FRONTIER);
    let mut recovered = test_partition();
    recovered.set_partition_dir(directory.path().to_string_lossy().into_owned());
    recovered
        .restore_retry_checkpoint(CHECKPOINT_OP)
        .await
        .unwrap();
    assert_eq!(
        recovered.dedup().watermarks_sorted(),
        partition.dedup().watermarks_sorted()
    );
    assert_eq!(recovered.required_metadata_frontier, METADATA_FRONTIER);
}

#[compio::test]
async fn missing_materialization_does_not_restore_wal_checkpoint_as_applied() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
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
    drop(journal);
    crate::state_transfer::mark_materialization_missing(directory.path().to_str().unwrap(), 0)
        .await
        .unwrap();
    publish_empty_retry_checkpoint(&partition, 7).await;
    partition.open_persistence().await.unwrap();
    assert!(partition.requires_state_transfer());
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
    assert!(partition.consensus().is_transferring());
    assert_eq!(partition.persistence.as_ref().unwrap().checkpoint_op(), 7);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), 0);
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_persisted_preallocation_when_rotating_should_reserve_without_extending() {
    const SEGMENT_BYTES: u64 = 1024 * 1024;
    const BLOCK_BYTES: u64 = 512;

    for preallocate in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let probe = tempfile::tempfile_in(directory.path()).unwrap();
        let preallocation_supported = match nix::fcntl::fallocate(
            &probe,
            nix::fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
            0,
            i64::try_from(SEGMENT_BYTES).unwrap(),
        ) {
            Ok(()) => true,
            Err(nix::errno::Errno::EOPNOTSUPP | nix::errno::Errno::ENOSYS) => false,
            Err(error) => panic!("preallocation probe failed: {error}"),
        };
        let mut partition = partition_at_view(0, 0);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.runtime_options.preallocate_segments = Some(preallocate);
        partition.runtime_options.segment_size = Some(IggyByteSize::from(SEGMENT_BYTES));
        partition.open_persistence().await.unwrap();
        partition.log.retire_front().unwrap();
        partition
            .install_empty_segment(&repair_config(), 0)
            .await
            .unwrap();
        let empty = std::fs::metadata(directory.path().join("00000000000000000000.log")).unwrap();
        assert_eq!(empty.len(), 0);
        if preallocation_supported {
            assert_eq!(
                empty.blocks() * BLOCK_BYTES >= SEGMENT_BYTES,
                preallocate,
                "empty active segment allocation must follow preallocate_segments={preallocate}"
            );
        }
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        let namespace = partition.namespace();
        let bodies = [
            build_segment_record_with_payload(
                namespace,
                0,
                Bytes::from(vec![1; usize::try_from(SEGMENT_BYTES).unwrap()]),
            ),
            build_segment_record(namespace, 1),
        ];
        let mut parent = 0;
        for (offset, body) in bodies.iter().enumerate() {
            let total = size_of::<PrepareHeader>() + body.len();
            let mut prepare = Message::<PrepareHeader>::new(total);
            prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(body);
            let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
                header.command = Command::Prepare;
                header.operation = Operation::SendMessages;
                header.cluster = TEST_CLUSTER;
                header.group = namespace.inner();
                header.op = u64::try_from(offset).unwrap() + 1;
                header.parent = parent;
                header.size = u32::try_from(total).unwrap();
                header.checksum_body = u128::from(iggy_common::calculate_checksum(body));
                header.checksum = header.identity_checksum();
            });
            parent = prepare.header().checksum;
            persistence.append(prepare.into_frozen(), true).unwrap();
        }
        partition.start_persistence();
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();

        let rotated = directory.path().join("00000000000000000001.log");
        let metadata = std::fs::metadata(&rotated).unwrap();
        assert_eq!(std::fs::read(&rotated).unwrap(), bodies[1]);
        assert_eq!(metadata.len(), bodies[1].len() as u64);
        if preallocation_supported {
            assert_eq!(
                metadata.blocks() * BLOCK_BYTES >= SEGMENT_BYTES,
                preallocate,
                "rotated segment allocation must follow preallocate_segments={preallocate}"
            );
        }
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
    }
}

/// A dispatched checkpoint covers `commit_min`, which may lead the writer.
/// It flushes every body through that op first, so it waits for the writer.
#[compio::test]
async fn a_dispatched_checkpoint_waits_for_the_writer_before_covering_a_body() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, replies, config, parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let second = checksummed_segment_prepare(2, parent, 1, b"second");
    let header = *second.header();
    persistence
        .append(second.clone().into_frozen(), false)
        .unwrap();
    // Hold the writer so that the second body stays unwritten.
    assert!(persistence.start());
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(header));
    });
    partition
        .append_repaired_send_messages(second)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(header.op);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus.commit_min(), header.op);
    assert_eq!(replies.borrow().len(), 2);

    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert!(
        !partition.persistence_checkpoint_pending(),
        "a checkpoint must not cover an unwritten body"
    );
    assert_eq!(partition.log.journal().inner.resident_count(), 1);

    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut partition, &config).await;
    persistence.drain().await.unwrap();
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert_eq!(persistence.checkpoint_op(), header.op);
}

#[compio::test]
async fn given_retention_waiting_for_checkpoint_when_it_finishes_should_resume_without_another_request()
 {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _, config, _) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Persisted, 3).await;
    partition.log.segments_mut()[0].sealed = true;
    partition.install_empty_segment(&config, 1).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    assert_eq!(persistence.segment_checkpoint().unwrap().next_offset, 0);
    persistence.sync();
    assert!(persistence.start());

    partition.remove_sealed_segments_up_to(0).await;
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.maintenance.retention, Some(0));
    assert_eq!(partition.log.segments().len(), 2);
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;
    assert!(persistence.checkpoint_pending());

    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.segments().len(), 1);
    assert_eq!(partition.log.segments()[0].start_offset, 1);
    assert_eq!(partition.maintenance.retention, None);
}

/// A due checkpoint starts at `commit_min`, so until it is pending a
/// dispatched Replicated commit waits for its body write.
#[compio::test]
async fn a_due_checkpoint_holds_dispatched_replicated_commits_at_the_written_head() {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, replies, config, parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let second = checksummed_segment_prepare(2, parent, 1, b"second");
    let header = *second.header();
    persistence
        .append(second.clone().into_frozen(), false)
        .unwrap();
    // Hold the writer so that the second body stays unwritten.
    assert!(persistence.start());
    partition.consensus.with_pipeline_mut(|pipeline| {
        pipeline.push(PipelineEntry::new(header));
    });
    partition
        .append_repaired_send_messages(second)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(header.op);

    persistence.request_checkpoint();
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.consensus.commit_min(),
        header.op - 1,
        "a due checkpoint must hold commits at the written head"
    );
    assert_eq!(replies.borrow().len(), 1);

    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert!(partition.persistence_checkpoint_pending());
    partition.commit_journal(&config).await;
    assert_eq!(
        partition.consensus.commit_min(),
        header.op,
        "a pending checkpoint must let commits pass the writer"
    );
    assert_eq!(replies.borrow().len(), 2);

    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(persistence.checkpoint_op(), header.op - 1);
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
}

#[compio::test]
async fn a_pending_checkpoint_holds_persisted_commits_and_passes_replicated_ones() {
    for dispatched in [false, true] {
        for durability in [
            iggy_common::Durability::Replicated,
            iggy_common::Durability::Persisted,
        ] {
            let root = tempfile::tempdir().unwrap();
            let (mut partition, replies, config, parent) =
                partition_with_committed_send(root.path(), durability, 3).await;
            let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
            // The body is written, and durable under Persisted, before the
            // checkpoint starts. Only the checkpoint can hold the reply.
            let second = checksummed_segment_prepare(2, parent, 1, b"second");
            let header = *second.header();
            persistence
                .append(second.clone().into_frozen(), durability.is_persisted())
                .unwrap();
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(header));
            });
            partition
                .append_repaired_send_messages(second)
                .await
                .unwrap();
            persistence.checkpoint(1);
            assert!(partition.persistence_checkpoint_pending());

            if dispatched {
                partition.set_io_notifier(
                    Rc::new(|_, _| {}),
                    crate::largest_legal_job_charge().unwrap(),
                );
            }
            partition.consensus.advance_commit_max(header.op);
            partition.commit_journal(&config).await;
            if dispatched {
                settle_partition_io(&mut partition, &config).await;
            }
            assert!(partition.fatal().is_none());
            assert!(partition.persistence_checkpoint_pending());
            let held = durability.is_persisted();
            assert_eq!(
                partition.consensus.commit_min(),
                if held { 1 } else { header.op },
                "{durability:?}"
            );
            assert_eq!(replies.borrow().len(), if held { 1 } else { 2 });

            partition.start_persistence();
            persistence.drain().await.unwrap();
            assert_eq!(persistence.checkpoint_op(), 1);
            partition.commit_journal(&config).await;
            if dispatched {
                settle_partition_io(&mut partition, &config).await;
            }
            assert!(partition.fatal().is_none());
            assert_eq!(partition.consensus.commit_min(), header.op);
            assert_eq!(replies.borrow().len(), 2);
        }
    }
}

/// A half that failed never advanced its cursor, so the retry rewrites
/// the same positions: one copy of the batch, one index entry.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_failed_persist_when_retried_with_a_healthy_writer_should_overwrite_the_same_positions()
 {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;

    assert!(
        fixture.persist(FIRST_PAYLOAD, 0).await.is_err(),
        "index save over {DEV_FULL} must fail"
    );

    // The committed prefix stays resident, so the retry re-persists the
    // identical bytes; only the broken index writer is swapped out.
    let index_writer = IggyIndexWriter::new(
        index_path.to_str().expect("utf-8 path"),
        fixture.index_cursor.clone(),
        true,
        false,
    )
    .await
    .expect("open replacement index writer");
    let active = fixture.partition.log.index_writers().len() - 1;
    fixture.partition.log.index_writers_mut()[active] = Some(Rc::new(index_writer));

    fixture
        .persist(FIRST_PAYLOAD, 0)
        .await
        .expect("retry persist");

    assert_eq!(
        file_len(&log_path),
        FIRST_PAYLOAD.len() as u64,
        "the retry must overwrite the batch, not append a second copy"
    );
    assert_eq!(
        file_len(&index_path),
        IGGY_INDEX_SIZE as u64,
        "the retry must write exactly one index entry"
    );
    assert_eq!(
        fixture.cursors(),
        (FIRST_PAYLOAD.len() as u64, IGGY_INDEX_SIZE as u64),
        "both cursors must advance once the retry succeeded"
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_index_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
 {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let mut fixture = PersistFixture::new(log_path.to_str().expect("utf-8 path"), DEV_FULL).await;

    let result = fixture.persist(FIRST_PAYLOAD, 0).await;

    assert!(
        matches!(result, Err(IggyError::CannotSaveIndexToSegment)),
        "index save over {DEV_FULL} must fail, got {result:?}"
    );
    assert_eq!(
        file_len(&log_path),
        FIRST_PAYLOAD.len() as u64,
        "the segment bytes landed; only the cursor is withheld"
    );
    assert_eq!(
        fixture.cursors(),
        (0, 0),
        "neither cursor may advance when the index half failed"
    );
    assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_log_save_failure_when_persisting_should_leave_both_cursors_and_segment_size_untouched()
 {
    let dir = tempfile::tempdir().expect("temp dir");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(DEV_FULL, index_path.to_str().expect("utf-8 path")).await;

    let result = fixture.persist(FIRST_PAYLOAD, 0).await;

    assert!(
        matches!(result, Err(IggyError::CannotWriteToFile)),
        "segment save over {DEV_FULL} must fail, got {result:?}"
    );
    assert_eq!(
        file_len(&index_path),
        IGGY_INDEX_SIZE as u64,
        "the index entry landed; only the cursor is withheld"
    );
    assert_eq!(
        fixture.cursors(),
        (0, 0),
        "neither cursor may advance when the segment half failed"
    );
    assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_both_saves_failing_when_persisting_should_leave_both_cursors_untouched() {
    let mut fixture = PersistFixture::new(DEV_FULL, DEV_FULL).await;

    let result = fixture.persist(FIRST_PAYLOAD, 0).await;

    assert!(
        matches!(result, Err(IggyError::CannotWriteToFile)),
        "a failed segment save must win over the index error, got {result:?}"
    );
    assert_eq!(fixture.cursors(), (0, 0), "no cursor may advance");
    assert_eq!(fixture.segment_size(), 0, "segment size must not advance");
}

/// The two saves run concurrently, so each must read its own cursor
/// without observing the other's advance: the second chunk lands exactly
/// where the first one ended, and its index entry says so.
#[compio::test]
async fn given_two_successful_persists_when_reading_back_should_place_second_chunk_at_first_chunk_end()
 {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;

    fixture
        .persist(FIRST_PAYLOAD, 0)
        .await
        .expect("first persist");
    fixture
        .persist(SECOND_PAYLOAD, 7)
        .await
        .expect("second persist");

    let log_bytes = (FIRST_PAYLOAD.len() + SECOND_PAYLOAD.len()) as u64;
    let index_bytes = 2 * IGGY_INDEX_SIZE as u64;
    assert_eq!(
        fixture.cursors(),
        (log_bytes, index_bytes),
        "both cursors must cover both persists"
    );
    assert_eq!(file_len(&log_path), log_bytes, "segment file length");
    assert_eq!(file_len(&index_path), index_bytes, "index file length");
    assert_eq!(fixture.segment_size(), log_bytes, "segment size");

    let reader = IggyIndexReader::new(index_path.to_str().expect("utf-8 path"))
        .await
        .expect("open index reader");
    let last = reader
        .load_last()
        .await
        .expect("read last index entry")
        .expect("index entry present");
    assert_eq!(
        last,
        IggyIndex::new(7, 8, FIRST_PAYLOAD.len() as u64),
        "the second entry must address the first chunk's end"
    );
}

#[compio::test]
async fn given_advanced_view_when_persist_gate_runs_should_write_vsr_state_once() {
    for (view, log_view) in [(3, 2), (2, 0)] {
        let case = format!("view {view}, log_view {log_view}");
        let mut partition = partition_at_view(view, log_view);
        let store = Rc::new(RecordingSuperblock::default());
        partition.set_superblock(store.clone(), None);

        assert!(
            partition.persist_superblock_if_needed().await,
            "{case}: the first gate run must persist the view"
        );

        let state = consensus::VsrState::try_from(store.writes.borrow()[0].as_slice())
            .expect("recorded payload decodes as a VsrState");
        assert_eq!(state.cluster, TEST_CLUSTER, "{case}");
        assert_eq!(state.view, view, "{case}");
        assert_eq!(state.log_view, log_view, "{case}");
        assert_eq!(
            (state.checkpoint_op, state.checkpoint_checksum),
            (0, 0),
            "{case}"
        );
        assert!(
            !partition.consensus().needs_superblock_persist(),
            "{case}: a landed write must make the view durable"
        );

        assert!(
            partition.persist_superblock_if_needed().await,
            "{case}: a durable view must pass the gate"
        );
        assert_eq!(
            store.attempts.get(),
            1,
            "{case}: an unchanged view must not rewrite"
        );
    }
}
