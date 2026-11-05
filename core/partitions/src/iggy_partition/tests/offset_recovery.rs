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

/// A kind directory whose presence boot cannot check may hold offsets, so
/// the load fails instead of starting that kind empty.
#[compio::test]
async fn given_failed_offset_directory_check_when_recovering_should_fail_the_load() {
    let dir = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: dir.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    for (kind, path) in [
        (
            ConsumerKind::Consumer,
            config.get_consumer_offsets_path(1, 1, 0),
        ),
        (
            ConsumerKind::ConsumerGroup,
            config.get_consumer_group_offsets_path(1, 1, 0),
        ),
        (
            ConsumerKind::ExternalGroup,
            config.get_external_group_offsets_path(1, 1, 0),
        ),
    ] {
        let (mut partition, _) = recording_partition();

        let Err(error) = crate::partition_storage::configure_consumer_offsets_with_storage(
            &FailingPathStorage(path.into()),
            &mut partition,
            &config,
            IggyNamespace::new(1, 1, 0),
            0,
        )
        .await
        else {
            panic!("{kind}: a failed directory check must fail the load");
        };

        assert!(
            matches!(
                &error,
                crate::PartitionRecoveryError::ConsumerOffsetsLoad { consumer_kind, .. }
                    if *consumer_kind == kind.as_str()
            ),
            "{kind}: {error:?}"
        );
    }
}

/// The first boot after an upgrade creates the external group offsets
/// directory. When that fails, the error has to say why, or the operator
/// sees a path and nothing else.
#[compio::test]
async fn given_unwritable_offsets_dir_when_recovering_should_report_why_the_external_dir_failed() {
    let directory = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: directory.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    // A file where the external group directory belongs fails its creation.
    // One at `offsets/` would fail the consumer directory check first.
    std::fs::create_dir_all(config.get_offsets_path(1, 1, 0)).unwrap();
    std::fs::write(
        config.get_external_group_offsets_path(1, 1, 0),
        b"not a directory",
    )
    .unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(config.get_partition_path(1, 1, 0));

    let error = crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        0,
    )
    .await
    .expect_err("the external group directory cannot be created");

    let crate::PartitionRecoveryError::CreateOffsetsDirectory { source, .. } = &error else {
        panic!("expected CreateOffsetsDirectory, got {error:?}");
    };
    assert!(
        error.to_string().ends_with(&source.to_string()),
        "the message must carry the I/O error: {error}"
    );
}

/// A partition made before external groups existed has no directory for them, and every WAL
/// checkpoint syncs each offset directory. Boot must create it before the first checkpoint,
/// or that checkpoint fails and fences the partition.
#[compio::test]
async fn given_layout_without_external_groups_dir_when_checkpointing_after_boot_should_succeed() {
    let directory = tempfile::tempdir().unwrap();
    let config = PartitionsConfig {
        path_layout: crate::PartitionPathLayout {
            streams_root: directory.path().to_string_lossy().into_owned(),
        },
        ..repair_config()
    };
    for dir in [
        config.get_consumer_offsets_path(1, 1, 0),
        config.get_consumer_group_offsets_path(1, 1, 0),
    ] {
        std::fs::create_dir_all(dir).unwrap();
    }
    let external_dir = config.get_external_group_offsets_path(1, 1, 0);
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(config.get_partition_path(1, 1, 0));
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
        0,
    )
    .await
    .unwrap();
    assert!(std::path::Path::new(&external_dir).is_dir());
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .append(
            checksummed_segment_prepare(1, 0, 0, b"durable").into_frozen(),
            true,
        )
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.consensus.restore_commit_state(1, 1);

    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();

    assert!(partition.fatal().is_none());
    assert!(persistence.failure().is_none());
    assert_eq!(persistence.checkpoint_op(), 1);
}

#[compio::test]
async fn given_committed_store_when_offset_persist_fails_should_set_fatal_commit() {
    let dir = tempfile::tempdir().expect("temporary offset parent");
    let parent = dir.path().join("not-a-directory");
    std::fs::File::create(&parent).expect("create invalid directory fixture");
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(parent.to_string_lossy().into_owned());
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let header = partition
        .log
        .journal()
        .inner
        .header_by_op(1)
        .expect("prepared offset op");
    partition.consensus.restore_commit_state(0, 1);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    let fatal = partition
        .fatal
        .as_ref()
        .expect("committed persistence failure fences partition");
    assert_eq!(fatal.operation, Some(Operation::StoreConsumerOffset));
    assert_eq!(fatal.op, 1);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
}

#[test]
fn given_reserved_owner_trailer_when_sizing_an_offset_prepare_should_match_its_projected_frame() {
    let partition = test_partition();
    let automatic = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, 7, 0)
        .expect("auto-commit request");
    let explicit = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    for (label, request) in [("auto-commit", automatic), ("client", explicit)] {
        let sized = IggyPartition::<IggyMessageBus>::prepared_request_size(&request);
        let prepare = request.project(partition.consensus());
        assert_eq!(
            sized,
            prepare.as_slice().len(),
            "{label} store must be sized as the prepare it projects to"
        );
    }
}

#[compio::test]
async fn given_queued_offset_when_wal_capacity_is_exhausted_should_deny_before_minting() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    partition.stats.increment_messages_count(1);
    let request = store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum);
    let retry = request.deep_copy();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(request, None))
            .is_ok()
    );
    partition
        .persistence
        .as_ref()
        .unwrap()
        .exhaust_capacity_for_test();
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 0);
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    assert!(partition.fatal().is_none());
    assert_eq!(replies.borrow().len(), 1);
    let reply = replies.borrow()[0].clone();
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &reply.1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, IggyError::TransientNotAccepted.as_code());

    partition
        .persistence
        .as_ref()
        .unwrap()
        .release_capacity_for_test();
    partition.on_request(retry, None).await;
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
    assert!(partition.fatal().is_none());
}

#[compio::test]
async fn given_queued_group_store_when_its_owner_is_replaced_should_refuse_it_at_promotion() {
    const GROUP_ID: u32 = 7;
    const CLIENT_ID: u128 = (1 << 64) + 42;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
    let replaced = partition.consumer_group_owner(u64::from(GROUP_ID)).unwrap();
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID + 1).await;
    let store = store_offset_request(
        replaced.client_id,
        1,
        ConsumerKind::ConsumerGroup,
        GROUP_ID,
        0,
        AckLevel::Quorum,
    )
    .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        header.owner_generation = replaced.generation;
        *next = header;
    });
    let head = partition.consensus.sequencer().current_sequence();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, Some(sender)))
            .is_ok()
    );

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "a store of a replaced owner must not be prepared"
    );
    assert_eq!(
        receiver.await.unwrap().header().status,
        IggyError::ConsumerGroupPartitionNotOwned(replaced.client_id, 0).as_code()
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_queued_auto_commit_when_already_covered_should_reply_with_committed_proof() {
    const CONSUMER_ID: u32 = 7;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    let store = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, CONSUMER_ID, 0)
        .unwrap();
    let (sender, receiver) = consensus::oneshot_channel();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, Some(sender)))
            .is_ok()
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, CONSUMER_ID, 0, 0);
    let head = partition.consensus.sequencer().current_sequence();

    partition.drain_request_queue_into_prepares(1).await;

    let reply = receiver.await.unwrap();
    assert_eq!(reply.header().status, 0);
    assert!(reply.header().has_committed_result());
    assert_eq!(partition.consensus.sequencer().current_sequence(), head);
    assert!(partition.consensus.pop_queued_request().is_none());
}

#[compio::test]
async fn given_queued_auto_commit_when_its_owner_is_replaced_should_refuse_it_at_promotion() {
    const GROUP_ID: u32 = 7;
    const CLIENT_ID: u128 = 42;
    let mut partition = test_partition();
    partition.stats.increment_messages_count(1);
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID).await;
    let store = partition
        .build_poll_auto_commit_request(ConsumerKind::ConsumerGroup, GROUP_ID, 0)
        .unwrap();
    let generation = store.header().owner_generation;
    install_test_owner(&mut partition, u64::from(GROUP_ID), CLIENT_ID + 1).await;
    assert!(
        partition
            .consumer_group_owner(u64::from(GROUP_ID))
            .unwrap()
            .generation
            > generation
    );
    let head = partition.consensus.sequencer().current_sequence();
    assert!(
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(store, None),)
            .is_ok()
    );

    partition.drain_request_queue_into_prepares(1).await;

    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        head,
        "an internal auto-commit must retain the owner generation it captured"
    );
    assert!(partition.consensus.pop_queued_request().is_none());
    assert_eq!(
        partition.group_offset_state(u64::from(GROUP_ID)),
        (None, None)
    );
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
}

#[compio::test]
async fn given_queued_capacity_denial_when_promoting_should_use_the_slot_for_next_existing_key() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.stats.increment_messages_count(1);
    for (request_id, consumer_id) in [(1, 8), (2, 7)] {
        assert!(
            partition
                .consensus
                .push_queued_request(consensus::RequestEntry::with_sender(
                    store_offset_request(
                        42,
                        request_id,
                        ConsumerKind::Consumer,
                        consumer_id,
                        0,
                        AckLevel::Quorum
                    ),
                    None,
                ))
                .is_ok()
        );
    }
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition
            .pending_consumer_offset_commits
            .get(&1)
            .expect("existing update projected")
            .consumer_id,
        7
    );
    assert!(partition.consensus.pop_queued_request().is_none());
}

#[compio::test]
async fn given_same_view_truncation_when_resynchronizing_should_release_discarded_reservations() {
    let mut partition = test_partition();
    journal_prepare(&partition, 1, Operation::StoreConsumerOffset).await;
    journal_prepare(&partition, 2, Operation::StoreConsumerOffset).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 0, false);
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 8, 0, false);
    partition.truncate_uncommitted_from(2).await.unwrap();
    partition.resynchronize_consumer_offset_reservations();
    assert!(partition.pending_consumer_offset_commits.contains_key(&1));
    assert!(!partition.pending_consumer_offset_commits.contains_key(&2));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
}

/// A primary-elect defers dropping the suffix its new view discards. An
/// acknowledgement that arrives first scans the new view and latches an
/// install from that suffix. A latch left on the dropped install refuses
/// every later install until another view change. Uncertain accounting
/// holds rescans for the shard tick, and the rebuild does not wait for it.
#[compio::test]
async fn given_ack_scan_before_deferred_truncation_when_resynchronizing_should_release_discarded_install()
 {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    let config = repair_config();
    for uncertain in [false, true] {
        let mut partition = test_partition();
        let installed_op = install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
        let discarded = next_test_installation(&partition, GROUP_ID, CLIENT_ID + 1);
        partition
            .on_request(owner_install_request(&partition, &discarded), None)
            .await;
        let discarded_op = partition.consensus.sequencer().current_sequence();
        assert_eq!(discarded_op, installed_op + 1);

        partition.consensus.set_view(1);
        let stale_ack = Message::<PrepareOkHeader>::new(size_of::<PrepareOkHeader>())
            .transmute_header(|_, header: &mut PrepareOkHeader| {
                header.command = Command::PrepareOk;
                header.cluster = TEST_CLUSTER;
                header.group = partition.consensus.group();
                header.op = installed_op;
                header.operation = Operation::InstallConsumerGroupOwner;
                header.size = u32::try_from(size_of::<PrepareOkHeader>()).unwrap();
            });
        partition.on_ack(stale_ack, &config).await;
        assert_eq!(partition.pending_owner_install.get(), Some(discarded));

        partition.defer_view_truncation(discarded_op..=u64::MAX, Vec::new(), None);
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::ViewApplied { .. }
        ));
        // The view starts on the retained head with an empty pipeline.
        partition.consensus.sequencer().set_sequence(installed_op);
        partition.consensus.with_pipeline_mut(LocalPipeline::clear);
        if uncertain {
            for capacity in &partition.consumer_offset_capacities {
                capacity.mark_uncertain();
            }
        }
        partition.commit_journal(&config).await;
        assert!(
            !partition.consumer_group_owner_admitted(&discarded),
            "uncertain {uncertain}: the latch still names the dropped install: {:?}",
            partition.pending_owner_install.get()
        );

        let successor = InstallConsumerGroupOwnerRequest {
            owner: ConsumerGroupOwner {
                client_id: CLIENT_ID + 2,
                session: 1,
                generation: discarded.owner.generation + 1,
            },
            metadata_op: discarded.metadata_op + 1,
            ..discarded
        };
        partition
            .on_request(owner_install_request(&partition, &successor), None)
            .await;
        assert_eq!(
            partition.pending_owner_install.get(),
            Some(successor),
            "uncertain {uncertain}"
        );
        commit_log(&mut partition).await;
        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        assert_eq!(
            partition.consumer_group_owner(GROUP_ID),
            Some(successor.owner),
            "uncertain {uncertain}"
        );
    }
}

#[compio::test]
async fn given_promotion_denial_budget_when_no_more_commits_arrive_should_resume_queued_work() {
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.stats.increment_messages_count(1);
    for (request, id) in [8, 9, 10, 11, 12, 7].into_iter().enumerate() {
        partition
            .consensus
            .push_queued_request(consensus::RequestEntry::with_sender(
                store_offset_request(
                    42,
                    request as u64 + 1,
                    ConsumerKind::Consumer,
                    id,
                    0,
                    AckLevel::Quorum,
                ),
                None,
            ))
            .unwrap();
    }
    partition.drain_request_queue_into_prepares(1).await;
    assert_eq!(sent.borrow().len(), PROMOTION_DENIALS_MAX);
    assert_eq!(partition.consensus.request_queue_len(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert!(partition.queued_requests_ready());
    partition.resume_queued_requests().await;
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert!(!partition.queued_requests_ready());
}

#[compio::test]
async fn given_covered_store_with_dirty_directory_when_sync_fails_should_fence_its_own_operation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let pending = PendingConsumerOffsetCommit::upsert_auto_commit(ConsumerKind::Consumer, 7, 10);
    partition
        .persist_consumer_offset_commit(pending)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(pending);
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 5, true);
    partition.consensus.restore_commit_state(0, 1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 1,
        operation: Operation::StoreConsumerOffset,
        client: 42,
        request: 1,
        ..Default::default()
    };
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert_eq!(partition.fatal.as_ref().unwrap().op, 1);
    assert_eq!(
        partition.fatal.as_ref().unwrap().operation,
        Some(Operation::StoreConsumerOffset)
    );
    assert_eq!(partition.consensus.commit_min(), 0);
    assert!(sent.borrow().is_empty());
    assert!(partition.consumer_offset_dirs_dirty[0].get());
}

#[compio::test]
async fn given_repeated_admitted_deletes_when_committed_should_be_idempotent() {
    let (mut partition, _) = recording_partition();
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 0, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    for op in 1..=2 {
        partition.stage_consumer_offset_delete(op, ConsumerKind::Consumer, 7);
    }
    for op in 1..=2 {
        partition
            .apply_staged_consumer_offset_commit(op)
            .await
            .expect("admitted delete converges");
    }
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
}

#[compio::test]
async fn given_queued_auto_commit_when_view_changes_should_release_its_provisional_slot() {
    let namespace = IggyNamespace::new(1, 1, 0);
    let consensus = VsrConsensus::new(
        TEST_CLUSTER,
        0,
        3,
        namespace.inner(),
        RecordingBus::default(),
        LocalPipeline::with_capacities(1, 2),
    );
    consensus.init();
    let mut partition: IggyPartition<RecordingBus> = IggyPartition::with_in_memory_storage(
        Arc::new(PartitionStats::default()),
        consensus,
        IggyByteSize::from(1024 * 1024),
    );
    partition.set_runtime_options(TopicRuntimeOptions {
        durability: iggy_common::Durability::Persisted,
        consumer_offset_durability: iggy_common::Durability::Persisted,
        ..Default::default()
    });
    partition.stats.increment_messages_count(1);
    partition.set_consumer_offsets_max(2);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let result = poll_read_result(&partition, PollingConsumer::Consumer(8, 0), true, Some(0));
    let completion = partition
        .complete_poll(result)
        .expect("queue automatic commit");
    assert!(completion.replication.is_none());
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(8, 0)),
        Some(0)
    );
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        2
    );
    assert_eq!(partition.consensus.request_queue_len(), 1);
    partition.consensus.set_view(3);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[compio::test]
async fn given_auto_commit_guard_when_pump_admits_should_transfer_to_journal_without_reply() {
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.stats.increment_messages_count(1);
    partition.set_consumer_offsets_max(1);
    let result = poll_read_result(&partition, PollingConsumer::Consumer(7, 0), true, Some(0));
    let completion = partition
        .complete_poll(result)
        .expect("accept automatic commit");
    assert!(partition.pending_consumer_offset_commits.is_empty());
    partition
        .replicate_poll_completion(completion.replication.expect("assigned prepare"))
        .await;
    assert_eq!(partition.pending_consumer_offset_commits.len(), 1);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(sent.borrow().is_empty());
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert!(sent.borrow().is_empty());
}

/// Auto-commit ops carry no segment bytes, so no message threshold ever
/// flushes them; a consume-only partition must still evict them at the
/// same message-count bound instead of holding one op per poll forever.
#[compio::test]
async fn given_only_consumer_offset_ops_when_committed_should_keep_the_journal_bounded() {
    const THRESHOLD: u32 = 8;
    let mut partition = test_partition();
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let last_op = 3 * u64::from(THRESHOLD);
    for op in 1..=last_op {
        journal_store_offset(&mut partition, op, 7, op).await;
        partition.consensus().advance_commit_max(op);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus().commit_min(), op);
        assert!(
            partition.log.journal().inner.resident_count() < THRESHOLD as usize,
            "op {op}: the resident journal must evict at the threshold"
        );
    }
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(last_op),
        "every committed offset must survive the evictions"
    );
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        0,
        "offset ops never reach the segment"
    );
}

/// A below-threshold batch committed earlier sits in front of the offset
/// ops. The control-op bound flushes it as a small chunk, the way the
/// shutdown flush would, and evicts the whole prefix behind it.
#[compio::test]
async fn given_small_message_tail_when_offset_ops_reach_the_bound_should_flush_it_and_evict() {
    const THRESHOLD: u32 = 8;
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        0,
        "one message stays resident below the threshold"
    );

    let last_op = 1 + u64::from(THRESHOLD);
    for op in 2..=last_op {
        journal_store_offset(&mut partition, op, 7, op).await;
    }
    partition.consensus().advance_commit_max(last_op);
    partition.commit_journal(&config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus().commit_min(), last_op);
    let one_record = build_segment_record(IggyNamespace::new(1, 1, 0), 0).len() as u64;
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        one_record,
        "the resident tail lands in the segment"
    );
    assert!(
        partition.log.journal().inner.is_empty(),
        "the committed prefix, offset ops included, is evicted"
    );
    assert_eq!(partition.log.journal().info.messages_count, 0);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(last_op)
    );

    let offset_path = partition
        .persisted_offset_path(ConsumerKind::Consumer, 7)
        .unwrap();
    drop(partition);
    let segment = std::fs::read(directory.path().join("00000000000000000000.log")).unwrap();
    assert_eq!(segment.len() as u64, one_record);
    let batch = decode_batch_slice(&segment).unwrap();
    assert_eq!(batch.header.base_offset, 0);
    assert_eq!(batch.message_count(), 1);
    assert_eq!(batch.iter().next().unwrap().payload, b"abcdefgh");
    let index = IggyIndexReader::new(
        directory
            .path()
            .join("00000000000000000000.index")
            .to_str()
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        index.load_last().await.unwrap(),
        Some(IggyIndex::new(0, batch.header.base_timestamp, 0))
    );
    assert_eq!(
        crate::offset_storage::decode_offset_record(&std::fs::read(offset_path).unwrap()),
        crate::offset_storage::OffsetRecord::Value {
            offset: last_op,
            checksummed: true,
        }
    );
}

#[compio::test]
async fn given_new_external_group_offset_when_committed_should_request_reconciliation() {
    let (mut partition, _) = recording_partition();
    let dirty = Rc::clone(&partition.consumer_group_offsets_dirty);
    partition.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 7, 1, false);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert_eq!(
        dirty.take(),
        BTreeSet::from([partition.namespace()]),
        "a new external group key must be reconciled against deleted groups"
    );
}

#[compio::test]
async fn given_group_offset_updates_when_key_already_exists_should_keep_reconciliation_idle() {
    let (mut partition, _) = recording_partition();
    let dirty = Rc::clone(&partition.consumer_group_offsets_dirty);
    let marked = BTreeSet::from([partition.namespace()]);
    partition.stage_consumer_offset_upsert(1, ConsumerKind::ConsumerGroup, 7, 1, true);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    assert_eq!(dirty.take(), marked);
    assert_eq!(partition.dead_group_offset_keys(|_| true), []);
    assert!(dirty.borrow().is_empty());
    partition.stage_consumer_offset_upsert(2, ConsumerKind::ConsumerGroup, 7, 2, true);
    partition
        .apply_staged_consumer_offset_commit(2)
        .await
        .unwrap();
    assert!(dirty.borrow().is_empty());
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(8),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 8, 3, String::new()),
    );
    partition.stage_consumer_offset_upsert(3, ConsumerKind::ConsumerGroup, 8, 3, true);
    partition
        .apply_staged_consumer_offset_commit(3)
        .await
        .unwrap();
    assert_eq!(
        dirty.take(),
        marked,
        "first durable commit must discover an eager map key"
    );
}

#[compio::test]
async fn given_primary_view_change_when_map_pressure_arrives_should_reclaim_only_unprotected_key() {
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_consumer_offsets_max(4);
    partition.stats.increment_messages_count(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 8, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let held = partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .reserve_provisional(9, &partition.durable_consumer_offsets)
        .unwrap();
    for id in 7..=10 {
        partition.consumer_offsets.pin().insert(
            id as usize,
            ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
        );
    }
    partition.consensus.set_view(3);
    partition.resynchronize_consumer_offset_reservations();
    assert_eq!(partition.consumer_offsets.len(), 4);
    partition.reclaim_phantom_offsets(ConsumerKind::Consumer, 4);
    assert_eq!(partition.consumer_offsets.len(), 3);
    assert!(!partition.consumer_offsets.pin().contains_key(&10));
    for id in 7..=9 {
        assert!(partition.consumer_offsets.pin().contains_key(&id));
    }
    drop(held);
}

#[test]
fn given_read_result_when_owner_accepts_should_advance_before_replication() {
    let (mut partition, _) = recording_partition();
    // Let Next compare its starting offset with the committed frontier (0)
    // instead of taking the shortcut for a partition that has never had data.
    partition.offset_space.committed_seeded = true;
    let consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;
    let consumer = PollingConsumer::Consumer(consumer_id, partition_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    assert_eq!(partition.get_consumer_offset(consumer), None);

    // Acceptance advances local progress and assigns replication work, but
    // the automatic commit has not yet been staged in the journal.
    let completion = partition.complete_poll(read_result).expect("accept poll");
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    assert!(completion.replication.is_some());
    assert!(partition.pending_consumer_offset_commits.is_empty());

    // Next already starts after offset 0, before replication runs.
    let validate_checksum = false;
    let next_result = partition
        .build_poll_plan(
            consumer,
            &PollingArgs::new(iggy_common::PollingStrategy::next(), 1, auto_commit),
            validate_checksum,
        )
        .execute_resident();
    assert!(next_result.fragments.is_empty());
}

#[test]
fn given_capacity_refusal_when_poll_completes_should_preserve_existing_progress() {
    let (mut partition, _) = recording_partition();
    let durable_consumer_id = 8;
    let polling_consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;

    // Another consumer owns the only durable slot. The polling consumer has
    // local progress at 4, but cannot admit an automatic commit through 9.
    partition.set_consumer_offsets_max(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, durable_consumer_id, 0, 0);
    partition.apply_local_poll_offset(ConsumerKind::Consumer, polling_consumer_id, 4);
    let consumer = PollingConsumer::Consumer(polling_consumer_id as usize, partition_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TooManyConsumerOffsets)
    ));
    assert_eq!(partition.get_consumer_offset(consumer), Some(4));
    assert_eq!(partition.consensus.pipeline_len(), 0);
}

/// A backup answering a read cannot originate the offset prepare, so
/// admitting the commit would advance local progress alone. The primary
/// would still hold the old offset and hand the same messages out again.
/// A normal backup names the primary as the place to retry, and a backup
/// in transfer cannot name it.
#[test]
fn given_backup_when_auto_commit_poll_completes_should_reject_without_progress() {
    for (transferring, expected) in [
        (false, IggyError::NotPrimary),
        (true, IggyError::TransientNotAccepted),
    ] {
        let (mut partition, _) = recording_partition_at(1, 3);
        if transferring {
            partition.consensus.begin_state_transfer_await();
        }
        let consumer = PollingConsumer::Consumer(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));

        assert_eq!(partition.complete_poll(read_result).unwrap_err(), expected);
        assert_eq!(partition.get_consumer_offset(consumer), None);
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

/// A backup whose durable table already covers the offset has nothing to
/// replicate, so there is no divergence to prevent and the refusal must
/// not reach it. Otherwise a caught-up follower would fail reads over a
/// commit the group already agreed.
#[test]
fn given_backup_when_auto_commit_offset_is_already_durable_should_be_accepted() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let consumer = PollingConsumer::Consumer(7, 0);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 9, 9);
    let read_result = poll_read_result(&partition, consumer, true, Some(9));

    let completion = partition
        .complete_poll(read_result)
        .expect("an already-durable offset admits no commit");
    assert!(completion.replication.is_none());
    assert_eq!(partition.consensus.pipeline_len(), 0);
}

/// An empty read never reaches automatic-commit admission and mutates no
/// progress, so the refusal above must not spread to it: a backup has to
/// keep answering the tail of a partition it is caught up on.
#[test]
fn given_backup_when_empty_auto_commit_poll_completes_should_be_accepted() {
    let (mut partition, _) = recording_partition_at(1, 3);
    let consumer = PollingConsumer::Consumer(7, 0);
    let read_result = poll_read_result(&partition, consumer, true, None);

    let completion = partition
        .complete_poll(read_result)
        .expect("an empty read admits no commit");
    assert!(completion.replication.is_none());
    assert_eq!(partition.get_consumer_offset(consumer), None);
}

#[test]
fn given_primary_when_role_changes_before_auto_commit_completion_should_reject_without_progress() {
    for transferring in [false, true] {
        let (mut partition, _) = recording_partition_at(0, 3);
        let consumer = PollingConsumer::ConsumerGroup(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));
        if transferring {
            partition.consensus.begin_state_transfer_await();
        } else {
            partition.consensus.begin_view_probe();
        }
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(partition.group_offset_state(7), (None, None));
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

/// `on_replicate` refuses prepares during shutdown, retirement and a
/// transition. An automatic commit admitted then would hold a pipeline
/// slot that no journal backs, and every later op would wait behind it.
#[test]
fn given_primary_refusing_prepares_when_auto_commit_poll_completes_should_reject_without_progress()
{
    let refusals: [fn(&mut IggyPartition<RecordingBus>); 3] = [
        IggyPartition::begin_shutdown_io,
        |partition| partition.io_active.retire(),
        |partition| {
            let message = Message::<iggy_binary_protocol::RequestStartViewHeader>::new(size_of::<
                iggy_binary_protocol::RequestStartViewHeader,
            >(
            ));
            partition.defer_view_transition(server_common::MessageBag::RequestStartView(message));
        },
    ];
    for refuse in refusals {
        let (mut partition, _) = recording_partition();
        let consumer = PollingConsumer::Consumer(7, 0);
        let read_result = poll_read_result(&partition, consumer, true, Some(9));
        refuse(&mut partition);
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        assert_eq!(partition.get_consumer_offset(consumer), None);
        assert_eq!(partition.consensus.pipeline_len(), 0);
    }
}

/// Shutdown promotes no queued request, because `on_replicate` would drop
/// its prepare and `DrainAccepted` would wait for it until the pump drain
/// times out, skipping the final flush.
#[compio::test]
async fn given_queued_request_when_shutdown_drains_should_deny_it_and_complete() {
    const STEPS_MAX: usize = 32;
    let (mut partition, replies) = recording_partition();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let order = partition.next_local_request_order().unwrap();
    let request = consensus::RequestEntry::with_sender(
        checksumless_send_request(partition.namespace(), 1),
        None,
    );
    assert!(partition.queue_local_request(request, order).is_ok());

    partition.begin_shutdown_io();
    let config = repair_config();
    for _ in 0..STEPS_MAX {
        if partition.shutdown_io_complete() {
            break;
        }
        partition.resume_io(&config).await;
    }

    assert!(partition.shutdown_io_complete());
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
    let replies = replies.borrow();
    assert_eq!(replies.len(), 1);
    let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
        &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(reply.status, IggyError::TransientNotAccepted.as_code());
}

#[compio::test]
async fn given_shutdown_waiting_for_io_when_interrupted_should_notify_and_keep_resources() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    assert!(!persistence.certify_log_view(2, 0, 0));
    assert!(persistence.start());
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), None);
    let notifications = Rc::new(Cell::new(0));
    let observed = Rc::clone(&notifications);
    partition.set_io_notifier(
        Rc::new(move |_, _| observed.set(observed.get() + 1)),
        crate::largest_legal_job_charge().unwrap(),
    );
    assert!(!partition.persist_superblock_if_needed().await);
    let config = repair_config();
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("view persistence must be ready");
    };
    let captured = partition.capture_io(plan, &config).unwrap().unwrap();

    partition.begin_shutdown_io();
    assert!(matches!(
        partition.resume_io(&config).await,
        crate::PartitionIoStep::Pending
    ));
    assert!(!partition.shutdown_io_complete());
    notifications.set(0);

    captured.quiescence.interrupt();
    partition.interrupt_io(captured.identity);
    assert!(notifications.get() > 0, "the shutdown owner must be ready");
    partition.resume_io(&config).await;

    assert!(
        !partition.shutdown_io_complete(),
        "accepted WAL work must drain"
    );
    assert!(
        partition.needs_io_retry(),
        "the retained file identity must not suppress WAL deadline checks"
    );
    Rc::clone(&persistence).run().await;
    partition.resume_io(&config).await;
    assert!(persistence.is_quiescent());
    assert!(partition.shutdown_io_complete());
    assert!(!partition.needs_io_retry());
    assert_eq!(partition.io_active.get(), Some(captured.identity));
    assert_eq!(captured.quiescence.get(), Some(captured.identity));
    assert!(partition.superblock_lock.try_acquire().is_none());
}

#[compio::test]
async fn shutdown_flushes_committed_messages_with_an_unacknowledged_pipeline_tail() {
    for (replica_count, store_offset) in [(3, false), (1, true), (3, true)] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, replica_count);
        let mut config = repair_config();
        config.messages_required_to_save = u32::MAX;
        partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
        partition.log.retire_front().unwrap();
        partition.install_empty_segment(&config, 0).await.unwrap();
        let first = checksummed_segment_prepare(1, 0, 0, b"committed");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"pending");
        let mut expected = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let tail = second.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        for prepare in [first, second] {
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(*prepare.header()));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus.sequencer().set_sequence(2);
        partition.consensus.advance_commit_max(1);
        partition.commit_journal(&config).await;
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(partition.consensus.pipeline_len(), 1);
        assert!(partition.persistence.is_none());
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        replies.borrow_mut().clear();
        if store_offset {
            partition
                .on_request(
                    store_offset_request(42, 1, ConsumerKind::Consumer, 7, 1, AckLevel::NoAck),
                    None,
                )
                .await;
            assert_eq!(partition.no_ack.len(), usize::from(replica_count == 1));
            assert_eq!(
                partition.consensus.request_queue_len(),
                usize::from(replica_count > 1)
            );
            assert!(replies.borrow().is_empty());
        }
        partition.begin_shutdown_io();
        settle_partition_io(&mut partition, &config).await;
        assert!(
            partition.shutdown_io_complete(),
            "shutdown stalled: replica_count={replica_count}, store_offset={store_offset}"
        );
        if store_offset {
            let replies = replies.borrow();
            assert_eq!(replies.len(), 1);
            let reply = bytemuck::checked::from_bytes::<ReplyHeader>(
                &replies[0].1.as_slice()[..size_of::<ReplyHeader>()],
            );
            assert_eq!(reply.status, IggyError::TransientNotAccepted.as_code());
            assert!(partition.consumer_offsets.pin().is_empty());
            assert_eq!(
                partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
                0
            );
        }
        let path = directory.path().join("00000000000000000000.log");
        assert_eq!(std::fs::read(&path).unwrap(), expected);

        partition.consensus.advance_commit_max(2);
        assert!(
            !partition.shutdown_io_complete(),
            "a late commit must invalidate completion"
        );
        settle_partition_io(&mut partition, &config).await;
        expected.extend(tail);
        assert!(partition.shutdown_io_complete());
        assert_eq!(std::fs::read(&path).unwrap(), expected);
    }
}

#[compio::test]
async fn shutdown_keeps_wal_enqueue_paused_after_a_late_commit_flush() {
    const STEPS_MAX: usize = 64;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    let tail = checksummed_segment_prepare(3, second.header().checksum, 2, b"uncommitted");
    for prepare in [&first, &second] {
        persistence
            .append(prepare.clone().into_frozen(), true)
            .unwrap();
    }
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    for prepare in [first, second, tail] {
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.consensus.sequencer().set_sequence(3);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();

    for commit_max in 1..=3 {
        partition.consensus.advance_commit_max(commit_max);
        for _ in 0..STEPS_MAX {
            settle_partition_io(&mut partition, &config).await;
            if partition.shutdown_io_complete() {
                break;
            }
            persistence
                .drain_with_timeout(compio::runtime::time::sleep)
                .await
                .unwrap();
        }
        assert!(
            partition.shutdown_io_complete(),
            "shutdown must finish the flush of commit {commit_max}"
        );
        assert!(persistence.is_quiescent());
        assert_eq!(persistence.head(), 2);
        partition.acknowledge_prepare(3).await;
        assert_eq!(
            persistence.head(),
            2,
            "shutdown must keep resident repair history out of the WAL after flushing commit {commit_max}"
        );
        assert!(persistence.is_quiescent());
    }
}

#[compio::test]
async fn shutdown_ignores_commit_heartbeats_beyond_the_local_journal() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let mut config = repair_config();
    config.messages_required_to_save = u32::MAX;
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    let expected = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(1);
    partition.consensus.advance_commit_max(1);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.shutdown_io_complete());
    assert_eq!(
        std::fs::read(directory.path().join("00000000000000000000.log")).unwrap(),
        expected
    );

    for commit_max in 2..=4 {
        partition.consensus.advance_commit_max(commit_max);
        assert!(
            partition.shutdown_io_complete(),
            "remote commit {commit_max} must not invalidate the local flush"
        );
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ));
        assert!(partition.shutdown_io_complete());
    }
}

#[compio::test]
async fn shutdown_with_missing_materialization_drains_wal_without_collapsing_reservation() {
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    let store = Rc::new(RecordingSuperblock::default());
    let recovered = recorded_state(1, 100);
    partition.set_superblock(Rc::clone(&store), Some(&recovered));
    partition.set_partition_dir(directory.path().to_str().unwrap().to_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.certify_log_view(1, 0, 0);
    assert!(persistence.start());
    partition.materialization_missing = true;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.begin_shutdown_io();
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(!partition.shutdown_io_complete());
    Rc::clone(&persistence).run().await;
    settle_partition_io(&mut partition, &repair_config()).await;
    assert!(partition.shutdown_io_complete());
    assert!(persistence.is_quiescent());
    assert_eq!(store.attempts.get(), 0);
    assert_eq!(
        partition.durable_offset_reserved.get(),
        recovered.offset_reserved
    );
}

#[compio::test]
async fn shutdown_fences_a_failed_flush_already_requested_by_transfer() {
    let mut partition = test_partition();
    let config = repair_config();
    let prepare = checksummed_segment_prepare(1, 0, 0, b"committed");
    partition
        .append_repaired_send_messages(prepare)
        .await
        .unwrap();
    partition.consensus.restore_commit_state(1, 1);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.maintenance.transfer_flush = true;
    partition.request_flush(1);
    partition.begin_shutdown_io();
    partition.reject_io_capture(
        crate::PartitionIoContinuation::Materialization,
        IggyError::CannotWriteToFile,
    );
    assert!(partition.fatal().is_some());
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.shutdown_io_complete());
}
