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

#[compio::test]
async fn persisted_singleton_with_failed_segment_directory_sync_withholds_reply() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(
        directory
            .path()
            .join("missing")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consensus().advance_commit_max(1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        command: Command::Prepare,
        operation: Operation::SendMessages,
        op: 1,
        group: partition.consensus().group(),
        client: 1,
        request: 1,
        ..PrepareHeader::default()
    };
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(partition.fatal().is_some());
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(replies.borrow().is_empty());
}

#[compio::test]
async fn persisted_singleton_skips_directory_sync_after_names_are_published() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 1);
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    for op in 1..=2 {
        partition.consensus().advance_commit_max(op);
        let prepare = checksummed_segment_prepare(op, 0, op - 1, b"payload");
        let header = *prepare.header();
        partition
            .log
            .journal()
            .inner
            .append(prepare.into_frozen())
            .await
            .unwrap();
        partition
            .handle_committed_entries(vec![header], &repair_config(), true)
            .await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus().commit_min(), op);
        assert!(!partition.segment_names_dirty.get());
        if op == 1 {
            std::fs::remove_dir(directory.path()).unwrap();
        }
    }
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    assert!(partition.segment_names_dirty.get());
}

#[compio::test]
async fn given_committed_deletes_when_drained_together_should_sync_directory_once_before_replies() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let mut drained = Vec::new();
    for id in 1..=2 {
        partition
            .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
                ConsumerKind::Consumer,
                id,
                0,
            ))
            .await
            .unwrap();
        partition.stage_consumer_offset_delete(u64::from(id), ConsumerKind::Consumer, id);
        let header = PrepareHeader {
            retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
            session: 1,
            op: u64::from(id),
            operation: Operation::DeleteConsumerOffset,
            client: 42,
            request: u64::from(id),
            ..Default::default()
        };
        drained.push(header);
    }
    partition.consensus.restore_commit_state(0, 2);
    partition
        .handle_committed_entries(drained, &repair_config(), true)
        .await;
    assert_eq!(partition.offset_dir_sync_count.get(), 1);
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(sent.borrow().len(), 2);
    assert!(!dir.path().join("1").exists());
    assert!(!dir.path().join("2").exists());
}

#[compio::test]
async fn deferred_views_keep_newer_progress_without_replacing_history_transitions() {
    let (mut partition, _) = recording_partition_at(0, 5);
    let start_change = |view, replica| {
        server_common::MessageBag::StartViewChange(
            Message::<iggy_binary_protocol::StartViewChangeHeader>::new(size_of::<
                iggy_binary_protocol::StartViewChangeHeader,
            >())
            .transmute_header(
                |_, header: &mut iggy_binary_protocol::StartViewChangeHeader| {
                    header.command = Command::StartViewChange;
                    header.size =
                        u32::try_from(size_of::<iggy_binary_protocol::StartViewChangeHeader>())
                            .unwrap();
                    header.view = view;
                    header.replica = replica;
                },
            ),
        )
    };
    let start_view = |view| {
        server_common::MessageBag::StartView(
            Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
                iggy_binary_protocol::StartViewHeader,
            >())
            .transmute_header(
                |_, header: &mut iggy_binary_protocol::StartViewHeader| {
                    header.command = Command::StartView;
                    header.size =
                        u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
                    header.view = view;
                },
            ),
        )
    };
    partition.defer_view_transition(start_change(1, 2));
    partition.defer_view_transition(start_view(1));
    partition.defer_view_transition(start_change(1, 1));
    partition.defer_view_transition(start_change(1, 2));
    partition.defer_view_transition(start_change(1, 5));
    partition.defer_view_transition(start_view(0));
    for replica in [1, 2] {
        assert!(matches!(partition.resume_io(&repair_config()).await,
                crate::PartitionIoStep::Transition(server_common::MessageBag::StartViewChange(message))
                if message.header().view == 1 && message.header().replica == replica));
    }
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 1));
    assert!(partition.deferred_views.is_empty());
    partition.defer_view_transition(start_view(1));
    partition.defer_view_transition(start_change(2, 1));
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 1));
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartViewChange(message))
            if message.header().view == 2));
    partition.transition = Some(PendingPartitionTransition::Truncate {
        ops: 3..=u64::MAX,
        actions: Vec::new(),
        peer: None,
        drain: None,
    });
    partition.defer_view_transition(start_view(3));
    assert!(matches!(
        partition.transition.as_ref(),
        Some(PendingPartitionTransition::Truncate { ops, .. }) if *ops == (3..=u64::MAX)
    ));
    partition.defer_view_transition(start_view(4));
    partition.transition = None;
    assert!(matches!(partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::StartView(message))
            if message.header().view == 4));
    assert!(partition.deferred_views.is_empty());
}

#[compio::test]
async fn missing_materialization_allows_deferred_recovery_control() {
    let (mut partition, _) = recording_partition();
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let message = Message::<iggy_binary_protocol::RequestStartViewHeader>::new(size_of::<
        iggy_binary_protocol::RequestStartViewHeader,
    >());
    partition.defer_view_transition(server_common::MessageBag::RequestStartView(message));
    assert!(
        matches!(
            partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Transition(server_common::MessageBag::RequestStartView(_))
        ),
        "recovery control must not be blocked by the state it repairs"
    );
}

#[compio::test]
async fn transfer_install_waits_for_captured_writer_before_replacing_history() {
    let directory = tempfile::tempdir().unwrap();
    let config = repair_config();
    let mut partition = partition_at_view(1, 1);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.configure_consumer_offset_storage(
        ["consumers", "groups", "external_groups"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned()),
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    assert!(!partition.persist_superblock_if_needed().await);
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("the current view must reach its superblock write");
    };
    let captured = partition.capture_io(plan, &config).unwrap().unwrap();
    let old_history = partition.poll_history;
    for stage in [
        consensus::StateTransferStage::AwaitingTarget,
        consensus::StateTransferStage::Fetching,
        consensus::StateTransferStage::Installing,
    ] {
        partition.consensus().set_state_transfer_stage(stage);
    }
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        ..Default::default()
    }
    .encode();
    partition
        .queue_state_transfer_install(1, Vec::new(), &offsets, 1)
        .unwrap();
    partition.begin_shutdown_io();
    assert!(matches!(
        partition.queue_state_transfer_install(2, Vec::new(), &offsets, 1),
        Err(crate::state_transfer::PartitionInstallError::ShuttingDown)
    ));
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    let result = captured.job.execute().await;
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    assert_eq!(partition.poll_history, old_history);
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    partition.accept_io(captured.identity, result).unwrap();
    captured.gate.unwrap().release();

    let outcome = finish_install_io(&mut partition).await.unwrap();
    assert_eq!(outcome.applied_commit_op, 1);
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_ne!(partition.poll_history, old_history);
    assert!(partition.fatal().is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join("00000000000000000000.log"))
            .unwrap()
            .len(),
        0
    );
}

#[compio::test]
async fn installed_offsets_survive_a_materialization_marker_unlink_failure() {
    const MARKER: &str = "materialization.missing";
    for wal in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let mut partition = partition_at_view(1, 1);
        partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
        if wal {
            partition.runtime_options.durability = iggy_common::Durability::Persisted;
            partition.open_persistence().await.unwrap();
        }
        let consumers = directory.path().join("consumers");
        let dirs = ["consumers", "groups", "external_groups"]
            .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
        for dir in &dirs {
            std::fs::create_dir(dir).unwrap();
        }
        partition.configure_consumer_offset_storage(
            dirs,
            ConsumerOffsets::with_capacity(1),
            ConsumerGroupOffsets::with_capacity(1),
        );
        partition.materialization_missing = true;
        let marker = directory.path().join(MARKER);
        std::fs::create_dir(&marker).unwrap();
        let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
        let wire = crate::state_transfer::ConsumerOffsetsWire {
            producers: Vec::new(),
            dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
            prepare_checksum: Some(prepare.header().checksum),
            checkpoint_prepare: prepare.as_slice().to_vec(),
            next_offset: 9,
            consumers: vec![(7, 4)],
            ..Default::default()
        };
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        partition
            .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
            .unwrap();
        assert!(finish_install_io(&mut partition).await.is_err());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert!(
            partition.fatal().is_none(),
            "marker cleanup must not fence a published install"
        );
        assert!(partition.materialization_missing);
        assert!(marker.is_dir());
        assert!(
            consumers.join("7").is_file(),
            "published offsets must survive cleanup failure"
        );
        assert_eq!(
            partition.durable_consumer_offset_count(ConsumerKind::Consumer),
            1
        );
    }
}

/// A fresh backup's sequencer is still 0 between the install's WAL reset and
/// the publication of the installed consensus.
#[compio::test]
async fn given_a_fresh_backup_when_the_tick_runs_during_an_install_should_certify_the_installed_head()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let dirs = ["consumers", "groups", "external_groups"]
        .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
    for dir in &dirs {
        std::fs::create_dir(dir).unwrap();
    }
    partition.configure_consumer_offset_storage(
        dirs,
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(prepare.header().checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
        next_offset: 9,
        ..Default::default()
    };
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();

    let outcome = finish_install_io(&mut partition).await;
    assert_eq!(outcome.unwrap().applied_commit_op, 1);
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.persistence.as_ref().unwrap().certified_log_view(),
        Some(1)
    );
}

#[compio::test]
async fn install_refuses_an_unreadable_offset_directory() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    let consumers = directory.path().join("consumers");
    std::fs::write(&consumers, b"not a directory").unwrap();
    partition.consumer_offset_dirs = ["consumers", "groups", "external_groups"]
        .map(|name| Some(directory.path().join(name).to_str().unwrap().to_owned()));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(
            1,
            Vec::new(),
            &crate::state_transfer::ConsumerOffsetsWire {
                producers: Vec::new(),
                dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
                ..Default::default()
            }
            .encode(),
            1,
        )
        .unwrap();
    let outcome = finish_install_io(&mut partition).await;
    assert!(
        outcome.is_err(),
        "an unreadable offset directory must fail installation: {outcome:?}"
    );
}

#[compio::test]
async fn given_old_offsets_when_install_replaces_them_should_keep_cleanup_keys_and_batch_renames() {
    const OLD_CONSUMER: u32 = 99;
    const INCOMING_OFFSETS: u32 = 33;
    const STEPS_MAX: usize = 128;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.open_persistence().await.unwrap();
    let dirs = ["consumers", "groups", "external_groups"]
        .map(|name| directory.path().join(name).to_str().unwrap().to_owned());
    for dir in &dirs {
        std::fs::create_dir(dir).unwrap();
    }
    let consumers = dirs[0].clone();
    partition.configure_consumer_offset_storage(
        dirs,
        ConsumerOffsets::with_capacity(1),
        ConsumerGroupOffsets::with_capacity(1),
    );
    let old = PendingConsumerOffsetCommit::upsert(ConsumerKind::Consumer, OLD_CONSUMER, 4);
    partition.persist_consumer_offset_commit(old).await.unwrap();
    partition.apply_consumer_offset_commit(old);
    let prepare = checksummed_segment_prepare(1, 0, 8, b"installed");
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(prepare.header().checksum),
        checkpoint_prepare: prepare.as_slice().to_vec(),
        next_offset: 9,
        consumers: (0..INCOMING_OFFSETS).map(|id| (id, 4)).collect(),
        ..Default::default()
    };
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();
    let config = repair_config();
    let mut cleaned = false;
    let mut batches = Vec::new();
    let mut installed = false;
    for _ in 0..STEPS_MAX {
        let _ = partition.persist_superblock_if_needed().await;
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                if let crate::PartitionIoJob::OffsetCleanup {
                    directory, known, ..
                } = &captured.job
                    && *directory == consumers
                {
                    assert!(
                        known.contains(&OLD_CONSUMER),
                        "a directory scan failure must still leave a known cleanup path"
                    );
                    cleaned = true;
                }
                if let crate::PartitionIoJob::Transfer(crate::io::TransferFileJob::CommitOffsets(
                    paths,
                )) = &captured.job
                {
                    batches.push(paths.len());
                }
                let result = captured.job.execute().await;
                partition.accept_io(captured.identity, result).unwrap();
                if let Some(gate) = captured.gate {
                    gate.release();
                }
                captured.quiescence.settle(captured.identity);
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => {
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
            }
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                outcome.unwrap();
                installed = true;
                break;
            }
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(installed && cleaned);
    assert_eq!(batches, [16, 16, 1]);
    assert!(
        !std::path::Path::new(&consumers)
            .join(OLD_CONSUMER.to_string())
            .exists()
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        INCOMING_OFFSETS as usize
    );
    for id in 0..INCOMING_OFFSETS {
        assert_eq!(
            crate::offset_storage::read_offset_max(&format!("{consumers}/{id}"), 0)
                .await
                .unwrap()
                .offset,
            4
        );
    }
}

/// The durable table does not list a file the partition failed to load or
/// unlink, so the install cleanup has to budget for its stranded id and
/// then clear it.
#[compio::test]
async fn given_durable_and_stranded_offsets_when_install_cleans_old_offsets_should_reserve_and_release_both()
 {
    const DURABLE: u32 = 7;
    const STRANDED: u32 = 42;
    const STEPS_MAX: usize = 128;
    let (_directory, mut partition) = transfer_receiver();
    for kind in ConsumerKind::ALL {
        partition
            .durable_consumer_offsets
            .record_explicit(kind, DURABLE, 0, 0);
        partition
            .consumer_offset_capacity_for(kind)
            .record_stranded(STRANDED);
    }
    let file_phase_charge = crate::io::file_phase_charge::<PingPongSuperblock>(
        std::iter::once(partition.partition_dir.as_deref())
            .chain(ConsumerKind::ALL.map(|kind| partition.consumer_offsets_dir(kind)))
            .flatten(),
    )
    .unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // An offer without offsets retains no ids, so only the cleanup
    // reservation can cover the known set.
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: 1,
        ..Default::default()
    };
    partition
        .queue_state_transfer_install(1, Vec::new(), &wire.encode(), 1)
        .unwrap();
    let config = repair_config();
    let mut cleanups = 0;
    let mut installed = false;
    for _ in 0..STEPS_MAX {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan) => {
                let captured = partition.capture_io(plan, &config).unwrap().unwrap();
                if let crate::PartitionIoJob::OffsetCleanup { known, .. } = &captured.job {
                    assert_eq!(
                        *known,
                        HashSet::from([DURABLE, STRANDED]),
                        "the cleanup must know the durable and the stranded file"
                    );
                    assert!(
                        plan.allocation_charge
                            >= file_phase_charge + known.capacity() * size_of::<u32>(),
                        "the plan must reserve the known set on top of the file phase"
                    );
                    cleanups += 1;
                }
                let result = captured.job.execute().await;
                partition.accept_io(captured.identity, result).unwrap();
                if let Some(gate) = captured.gate {
                    gate.release();
                }
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::InstallFinished { outcome, .. } => {
                outcome.unwrap();
                installed = true;
                break;
            }
            _ => panic!("unexpected install continuation"),
        }
    }
    assert!(installed);
    assert_eq!(cleanups, ConsumerKind::COUNT);
    for kind in ConsumerKind::ALL {
        assert_eq!(
            partition.stranded_consumer_offset_count(kind),
            0,
            "the install must release the stranded {kind:?} id"
        );
    }
}
