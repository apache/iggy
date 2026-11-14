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
use crate::remote_segments::IndexFetchFence;

/// A replica applies the delete fence from its partition log, and its
/// metadata can apply the delete later. Until then reads planned before or
/// after the fence pass every metadata check. Completion refuses them with
/// or without automatic commits, also when the offset is already durable.
#[compio::test]
async fn given_deleted_history_when_poll_completes_should_refuse_without_progress() {
    const GROUP_ID: u64 = 7;
    const CLIENT_ID: u128 = 42;
    const CONSUMER_ID: u32 = 8;
    const DURABLE_CONSUMER_ID: u32 = 9;
    const METADATA_OP: u64 = 10;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    install_test_owner(&mut partition, GROUP_ID, CLIENT_ID).await;
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, DURABLE_CONSUMER_ID, 0, 0);
    let group = PollingConsumer::ConsumerGroup(usize::try_from(GROUP_ID).unwrap(), 0);
    let consumer = |id: u32| PollingConsumer::Consumer(usize::try_from(id).unwrap(), 0);
    let reads = |partition: &IggyPartition<RecordingBus>| {
        [
            (group, false),
            (group, true),
            (consumer(CONSUMER_ID), false),
            (consumer(CONSUMER_ID), true),
            (consumer(DURABLE_CONSUMER_ID), true),
        ]
        .map(|(polling, auto_commit)| poll_read_result(partition, polling, auto_commit, Some(0)))
    };
    let planned_before_fence = reads(&partition);
    let transition = TransitionPartitionHistoryRequest {
        incarnation: partition.created_revision(),
        metadata_op: METADATA_OP,
    };
    let fence = partition_control_request(
        &partition,
        Operation::TransitionPartitionHistory,
        &transition.to_bytes(),
        METADATA_OP,
    );
    partition.on_request(fence, None).await;
    let fence_op = partition.consensus.sequencer().current_sequence();
    partition.consensus.advance_commit_max(fence_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.installed_history_transition(&transition),
        Some(fence_op)
    );

    for read in planned_before_fence.into_iter().chain(reads(&partition)) {
        let (polling, auto_commit) = (read.context.consumer, read.context.auto_commit);
        let completion = partition.complete_poll(read);
        assert!(
            matches!(completion, Err(IggyError::HistoryUnavailable)),
            "{polling:?} with auto_commit {auto_commit} read the deleted history: {:?}",
            completion.map(|accepted| accepted.fragments.len())
        );
    }
    assert_eq!(partition.group_offset_state(GROUP_ID), (None, None));
    for id in [CONSUMER_ID, DURABLE_CONSUMER_ID] {
        assert_eq!(partition.get_consumer_offset(consumer(id)), None);
    }
    assert_eq!(partition.consensus.sequencer().current_sequence(), fence_op);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
}

#[compio::test]
async fn given_stale_poll_when_reservations_need_resync_should_reject_before_reconciliation() {
    let mut partition = test_partition();
    partition.set_consumer_offsets_max(1);
    let discarded_consumer_id = 8;
    let discarded_operation = 1;
    journal_prepare(
        &partition,
        discarded_operation,
        Operation::StoreConsumerOffset,
    )
    .await;
    partition
        .consensus
        .sequencer()
        .set_sequence(discarded_operation);
    partition.stage_consumer_offset_upsert(
        discarded_operation,
        ConsumerKind::Consumer,
        discarded_consumer_id,
        0,
        false,
    );
    let consumer = PollingConsumer::Consumer(7, 0);
    let auto_commit = true;
    let stale_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // Truncation removes the pending operation but leaves its capacity
    // reservation for reconciliation. The old read cannot belong to the
    // replacement history or trigger that maintenance.
    partition.invalidate_poll_history();
    partition
        .truncate_uncommitted_from(discarded_operation)
        .await
        .unwrap();
    assert!(partition.pending_consumer_offset_commits.is_empty());
    assert!(partition.offset_reservations_need_resync.get());
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    assert!(matches!(
        partition.complete_poll(stale_result),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1,
        "rejecting a stale read must not reconcile the discarded reservation"
    );
    assert!(partition.offset_reservations_need_resync.get());
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert_eq!(partition.consensus.pipeline_len(), 0);

    // A valid completion still reconciles before admission, freeing the
    // only slot so the owner can admit an automatic commit for this consumer.
    let fresh_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let completion = partition
        .complete_poll(fresh_result)
        .expect("accept fresh poll");
    assert!(completion.replication.is_some());
    assert!(!partition.offset_reservations_need_resync.get());
    assert_eq!(partition.get_consumer_offset(consumer), Some(0));
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[compio::test]
async fn given_full_wal_when_poll_completes_should_reject_without_progress() {
    // A primary with persisted offsets needs write-ahead log capacity before
    // accepting an automatic commit. Use real persistence, then exhaust it.
    let directory = tempfile::tempdir().unwrap();
    let primary_replica = 0;
    let replica_count = 3;
    let (mut partition, _) = recording_partition_at(primary_replica, replica_count);
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let operation_before_poll = partition.consensus.sequencer().current_sequence();
    persistence.exhaust_capacity_for_test();

    // Refusal must leave progress, queues, operation assignment, and
    // occupied capacity unchanged.
    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(partition.consensus.request_queue_len(), 0);
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        operation_before_poll
    );
    assert_eq!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .occupied(&partition.durable_consumer_offsets),
        0
    );

    // The same offset becomes acceptable when capacity returns. These are
    // local progress updates; the assigned replication has not run yet.
    persistence.release_capacity_for_test();
    let retry_result = poll_read_result(&partition, consumer, auto_commit, Some(0));
    let retry_completion = partition.complete_poll(retry_result).expect("accept retry");
    assert!(retry_completion.replication.is_some());
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, Some(0));
    assert_eq!(committed, Some(0));
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        operation_before_poll + 1
    );
}

#[test]
fn given_pending_poll_when_materialization_is_missing_should_reject_without_progress() {
    let group_id = 7;
    let member_id = 1;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    for auto_commit in [false, true] {
        let (mut partition, _) = recording_partition();
        let read_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

        // The partition can lose its materialized data after planning even
        // when its history identity has not changed.
        partition.materialization_missing = true;
        assert!(matches!(
            partition.complete_poll(read_result),
            Err(IggyError::TransientNotAccepted)
        ));
        let (last_polled, committed) = partition.group_offset_state(group_id as u64);
        assert_eq!(
            last_polled, None,
            "a rejected read must not record group progress"
        );
        assert_eq!(committed, None, "a rejected read must not commit an offset");
        assert_eq!(partition.consensus.pipeline_len(), 0);
        assert_eq!(partition.consensus.request_queue_len(), 0);
    }
}

#[test]
fn given_empty_read_when_partition_is_replaced_should_reject_old_result() {
    let (partition, _) = recording_partition();
    let consumer_id = 7;
    let partition_id = 0;
    let auto_commit = true;
    let consumer = PollingConsumer::Consumer(consumer_id, partition_id);
    let old_empty_result = poll_read_result(&partition, consumer, auto_commit, None);

    // The same namespace does not give a replacement ownership of
    // the old instance's results, even when they contain no messages.
    let (mut replacement, _) = recording_partition();
    assert!(matches!(
        replacement.complete_poll(old_empty_result),
        Err(IggyError::TransientNotAccepted)
    ));
}

/// Disk reads of one group can finish out of order, and each carries its
/// own automatic commit. Both are admitted, so ordering has to be settled
/// where progress is recorded rather than by the order they complete in.
#[compio::test]
async fn given_group_reads_completing_in_reverse_order_should_keep_progress_monotone() {
    let (mut partition, replies) = recording_partition_at(0, 3);
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    replies.borrow_mut().clear();
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let earlier_result = poll_read_result(&partition, consumer, auto_commit, Some(4));
    let later_result = poll_read_result(&partition, consumer, auto_commit, Some(9));

    let later = partition
        .complete_poll(later_result)
        .expect("accept later poll")
        .replication
        .expect("sequence first auto-commit");
    assert!(
        partition
            .complete_poll(earlier_result)
            .expect("accept earlier poll")
            .replication
            .is_none()
    );
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled,
        Some(9),
        "the slower read must not rewind group progress"
    );
    assert_eq!(
        committed,
        Some(9),
        "the slower read must not rewind its offset"
    );
    assert_eq!(partition.consensus.pipeline_len(), 1);
    assert_eq!(
        partition
            .consensus
            .with_pipeline(consensus::LocalPipeline::request_queue_len),
        1
    );
    let offset_op = later.prepare.header().op;
    partition.on_replicate(later.prepare).await;
    drop(later.reservation);
    partition.consensus.advance_commit_max(offset_op);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consensus.commit_min(),
        offset_op,
        "the queued offset is already covered"
    );
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(
        partition
            .consensus
            .with_pipeline(consensus::LocalPipeline::request_queue_len),
        0
    );
    assert!(partition.durable_consumer_offsets.covers(
        ConsumerKind::ConsumerGroup,
        u32::try_from(group_id).unwrap(),
        9
    ));
    assert!(
        replies.borrow().is_empty(),
        "automatic no-ops must not emit unsolicited replies"
    );
}

#[test]
fn given_pending_auto_commit_when_history_changes_should_release_only_its_queue_entry() {
    let (mut partition, _) = recording_partition();
    partition.set_consumer_offsets_max(1);
    let automatic_consumer_id = 7;
    let explicit_consumer_id = 8;
    let explicit_client_id = 42;

    // Only the automatic request holds the provisional slot. Queue an
    // explicit store beside it to prove that retiring the history does not
    // discard unrelated client requests.
    let automatic_reservation = partition
        .consumer_offset_capacity_for(ConsumerKind::Consumer)
        .reserve_provisional(automatic_consumer_id, &partition.durable_consumer_offsets)
        .unwrap();
    let automatic_request = partition
        .build_poll_auto_commit_request(ConsumerKind::Consumer, automatic_consumer_id, 0)
        .unwrap();
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::with_auto_commit(
            automatic_request,
            AutoCommitRequestContext {
                reservation: automatic_reservation,
            },
        ))
        .unwrap();
    let explicit_request = store_offset_request(
        explicit_client_id,
        1,
        ConsumerKind::Consumer,
        explicit_consumer_id,
        0,
        AckLevel::Quorum,
    );
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::new(explicit_request))
        .unwrap();
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    partition.invalidate_poll_history();
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    let retained_request = partition
        .consensus
        .pop_queued_request()
        .expect("explicit request survives");
    assert_eq!(retained_request.message.header().client, explicit_client_id);
    assert!(partition.consensus.pop_queued_request().is_none());
}

/// Promotion does not check a queued offset mutation against the poll
/// history, so an install must leave none queued to run against the
/// installed offsets.
#[compio::test]
async fn given_queued_offset_mutations_when_install_publishes_should_drop_them_and_keep_installed_offsets()
 {
    const STORED: u32 = 7;
    const DELETED: u32 = 8;
    const INSTALLED_OP: u64 = 1;
    const INSTALLED_OFFSET: u64 = 4;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    partition.open_persistence().await.unwrap();
    queue_offset_mutations_then_arm_transfer(&mut partition, STORED, DELETED).await;

    let checkpoint = checksummed_segment_prepare(INSTALLED_OP, 0, INSTALLED_OFFSET, b"installed");
    let installed_state = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        prepare_checksum: Some(checkpoint.header().checksum),
        checkpoint_prepare: checkpoint.as_slice().to_vec(),
        next_offset: INSTALLED_OFFSET + 1,
        consumers: vec![(STORED, INSTALLED_OFFSET), (DELETED, INSTALLED_OFFSET)],
        ..Default::default()
    };
    partition
        .install_state_transfer(
            &repair_config(),
            INSTALLED_OP,
            Vec::new(),
            &installed_state.encode(),
        )
        .await
        .unwrap();

    assert_eq!(partition.consensus().request_queue_len(), 0);
    for consumer in [STORED, DELETED] {
        assert_eq!(
            partition.get_consumer_offset(PollingConsumer::Consumer(consumer as usize, 0)),
            Some(INSTALLED_OFFSET),
            "consumer {consumer} must keep its installed offset"
        );
    }
}

/// Converge wipes the journal that held the pipeline's prepares, and a
/// queued mutation must not run against the converged offsets.
#[compio::test]
async fn given_queued_offset_mutations_when_failed_install_converges_should_clear_the_pipeline() {
    const STORED: u32 = 7;
    const DELETED: u32 = 8;
    const OFFERED_OP: u64 = 12;
    const OFFERED_NEXT_OFFSET: u64 = 2;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) =
        recording_partition_with_pipeline(0, 3, LocalPipeline::with_capacities(1, 2));
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    queue_offset_mutations_then_arm_transfer(&mut partition, STORED, DELETED).await;

    // Staged files that do not exist fail the swap after preflight, and
    // without a WAL the partition converges instead of fencing.
    let missing_staged_segment = crate::state_transfer::StagedSegmentMeta {
        start_offset: 0,
        end_offset: 0,
        index_size: 0,
        size: 8,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        log_staging: directory.path().join("00000000000000000000.log.staging"),
        index_staging: directory.path().join("00000000000000000000.index.staging"),
        checksum: 0,
        linked: false,
    };
    let offered_state = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        next_offset: OFFERED_NEXT_OFFSET,
        ..Default::default()
    };
    let install_error = partition
        .install_state_transfer(
            &repair_config(),
            OFFERED_OP,
            vec![missing_staged_segment],
            &offered_state.encode(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            install_error,
            crate::state_transfer::PartitionInstallError::SwapIo { .. }
        ),
        "unexpected failure: {install_error:?}"
    );

    assert_eq!(
        partition.consensus().request_queue_len(),
        0,
        "request queue"
    );
    assert_eq!(partition.consensus().pipeline_len(), 0, "prepare queue");
}

/// Admits an explicit store and a delete while a filler store holds the
/// only prepare slot, so both wait in the request queue, then arms a
/// state transfer behind them.
async fn queue_offset_mutations_then_arm_transfer(
    partition: &mut IggyPartition<RecordingBus>,
    stored: u32,
    deleted: u32,
) {
    // A key of its own, so the mutations queue for the full pipeline alone.
    let filler = stored.max(deleted) + 1;
    // One message makes offset 0 storable, and a delete on a replicated
    // partition needs a durable entry to remove.
    partition.stats.increment_messages_count(1);
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, deleted, 0, 0);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, filler, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(
            store_offset_request(43, 1, ConsumerKind::Consumer, stored, 0, AckLevel::Quorum),
            None,
        )
        .await;
    partition
        .on_request(delete_offset_request(44, 1, deleted), None)
        .await;
    assert_eq!(partition.consensus().pipeline_len(), 1);
    assert_eq!(partition.consensus().request_queue_len(), 2);

    for stage in [
        consensus::StateTransferStage::AwaitingTarget,
        consensus::StateTransferStage::Fetching,
        consensus::StateTransferStage::Installing,
    ] {
        partition.consensus().set_state_transfer_stage(stage);
    }
}

/// An install unlinks the segment files a suspended poll may still hold
/// open, so the poll has to reopen by path instead of reading the old inode.
#[compio::test]
async fn given_in_flight_sealed_poll_when_state_is_installed_should_wipe_its_read_state() {
    let (directory, mut partition) = transfer_receiver();
    let namespace = partition.namespace();
    let partition_dir = directory.path().to_string_lossy().into_owned();
    let log_path = format!("{partition_dir}/{:0>20}.log", 0u64);
    let index_path = format!("{partition_dir}/{:0>20}.index", 0u64);
    partition.log.segments_mut()[0].sealed = true;
    partition.log.storages_mut()[0] = SegmentStorage::new(&log_path, &index_path, 0, 0, false)
        .await
        .unwrap();
    let record = build_segment_record(namespace, 0);
    let record_len = record.len() as u64;
    std::fs::write(&log_path, record).unwrap();
    partition.log.touch_sealed_read_state(0);
    let handle = Rc::clone(&partition.log.sealed_read_state()[0]);
    let poll = || DiskReadPlan {
        partition_dir: PartitionDirResolution::Resolved(partition_dir.clone()),
        remote: None,
        object_prefix: None,
        index_fetches: IndexFetchFence::detached(),
        cut: false,
        deadline: None,
        bytes_per_message: None,
        widest_batch_bytes: 0,
        validate_checksum: true,
        segments: vec![DiskSegment {
            start_offset: 0,
            persisted: record_len,
            read_state: Rc::clone(&handle),
            sealed: true,
            source: SegmentSource::Local,
        }],
        start_position: 0,
        start_index_offset: None,
        namespace_raw: namespace.inner(),
    };
    let lookup = MessageLookup::Offset {
        offset: 0,
        count: 1,
        ceiling: u64::MAX,
    };
    assert!(matches!(
        poll().read_disk(lookup).await,
        DiskReadOutcome::Matched { .. }
    ));
    assert!(handle.fd.borrow().is_some());
    assert!(handle.index.borrow().is_some());

    let installed_state = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::PARTITION_DEDUP_CLIENTS_MAX,
        next_offset: 10,
        ..Default::default()
    };
    partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &installed_state.encode())
        .await
        .unwrap();

    assert!(
        handle.fd.borrow().is_none(),
        "install must drop the fd the poll cached"
    );
    assert!(
        handle.index.borrow().is_none(),
        "install must drop the index the poll cached"
    );
    assert!(
        !handle.tracked.get(),
        "a resumed walk must not cache into the old handle"
    );
    assert!(
        !matches!(
            poll().read_disk(lookup).await,
            DiskReadOutcome::Matched { .. }
        ),
        "a resumed walk must not serve the unlinked segment"
    );
}

#[compio::test]
async fn given_read_when_failed_install_converges_should_reject_the_previous_history() {
    // Valid offset table paths let installation reach the segment swap.
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        directory
            .path()
            .join("groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        directory
            .path()
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.log.journal().inner.set_repair_retention(true);
    let first = checksummed_segment_prepare(1, 0, 0, b"old first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"old second");
    for prepare in [first, second] {
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.evict_committed_prefix(1);
    assert!(partition.log.journal().inner.repair_entry(1).is_some());
    assert!(partition.log.journal().inner.holds_op(2));
    assert_eq!(partition.log.journal().info.messages_count, 1);
    let group_id = 7;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let old_result = poll_read_result(&partition, consumer, auto_commit, Some(4));
    for kind in ConsumerKind::ALL {
        let path = partition.persisted_offset_path(kind, 42).unwrap();
        persist_offset(&path, 0, true).await.unwrap();
        partition
            .consumer_offset_capacity_for(kind)
            .record_stranded(42);
    }

    // Describe staged files without creating them. The swap fails after
    // preflight, forcing recovery to replace the served log with an empty one.
    let missing_staged_segment = crate::state_transfer::StagedSegmentMeta {
        start_offset: 0,
        end_offset: 0,
        index_size: 0,
        size: 8,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        log_staging: directory.path().join("00000000000000000000.log.staging"),
        index_staging: directory.path().join("00000000000000000000.index.staging"),
        checksum: 0,
        linked: false,
    };
    let offered_state = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 2,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let offered_commit_operation = 12;
    let install_error = partition
        .install_state_transfer(
            &repair_config(),
            offered_commit_operation,
            vec![missing_staged_segment],
            &offered_state.encode(),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            install_error,
            crate::state_transfer::PartitionInstallError::SwapIo { .. }
        ),
        "unexpected failure: {install_error:?}"
    );

    for kind in ConsumerKind::ALL {
        assert!(
            !std::path::Path::new(&partition.persisted_offset_path(kind, 42).unwrap()).exists()
        );
        assert_eq!(
            partition
                .consumer_offset_capacity_for(kind)
                .stranded_count(),
            0,
            "convergence must release removed offset files for {kind:?}"
        );
    }

    // Failure did not preserve the old history: recovery left an empty log,
    // and its owner must reject the result captured before installation.
    assert!(
        partition
            .log
            .journal()
            .inner
            .repair_retained_from()
            .is_none()
    );
    assert!(partition.log.journal().inner.repair_entry(1).is_none());
    assert!(!partition.log.journal().inner.holds_op(2));
    assert_eq!(partition.log.journal().info.messages_count, 0);
    assert_eq!(partition.log.journal().info.size.as_bytes_u64(), 0);
    assert_eq!(partition.log.segments().len(), 1);
    assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
    assert!(matches!(
        partition.complete_poll(old_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
}

#[compio::test]
async fn given_read_when_install_preflight_fails_should_keep_the_existing_history() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let group_id = 7;
    install_test_owner(&mut partition, group_id as u64, 42).await;
    let member_id = 1;
    let auto_commit = false;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let pending_result = poll_read_result(&partition, consumer, auto_commit, Some(4));

    // Empty offset bytes fail decoding before installation mutates served
    // state. The result's original history identity must remain acceptable.
    let offered_commit_operation = 12;
    assert!(
        partition
            .install_state_transfer(&repair_config(), offered_commit_operation, Vec::new(), &[],)
            .await
            .is_err()
    );
    partition
        .complete_poll(pending_result)
        .expect("unmodified history remains valid");
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled,
        Some(4),
        "the read still belongs to the served history"
    );
    assert_eq!(committed, None, "automatic commits are disabled");
}

#[compio::test]
async fn given_full_prepare_queue_when_poll_completes_should_queue_its_context_and_advance() {
    let primary_replica = 0;
    let replica_count = 3;
    let prepare_capacity = 1;
    let request_capacity = 1;
    let pipeline = LocalPipeline::with_capacities(prepare_capacity, request_capacity);
    let (mut partition, _) =
        recording_partition_with_pipeline(primary_replica, replica_count, pipeline);
    let client_id = 42;
    let explicit_consumer_id = 7;
    let group_id = 8;
    let installed_op = install_test_owner(&mut partition, group_id as u64, client_id).await;

    // One visible message makes offset 0 valid. Its explicit store occupies
    // the only prepare slot while the request queue remains available.
    partition.stats.increment_messages_count(1);
    let mut explicit = store_offset_request(
        client_id,
        1,
        ConsumerKind::Consumer,
        explicit_consumer_id,
        0,
        AckLevel::Quorum,
    );
    explicit = explicit.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
        header.metadata_watermark = partition.required_metadata_frontier;
        *next = header;
    });
    partition.on_request(explicit, None).await;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // Acceptance can advance local progress after queueing the automatic
    // commit, even though no operation or replication work is assigned yet.
    let completion = partition
        .complete_poll(read_result)
        .expect("request queue has room");
    assert!(completion.replication.is_none());
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, Some(0));
    assert_eq!(committed, Some(0));
    assert_eq!(
        partition.consensus.sequencer().current_sequence(),
        installed_op + 1
    );

    // The queued request owns the capacity reservation. Taking it out of
    // the queue must keep that slot occupied.
    let queued_request = partition
        .consensus
        .pop_queued_request()
        .expect("automatic commit queued");
    let queued_context = queued_request
        .auto_commit()
        .expect("request carries its context");
    assert_eq!(
        queued_context.reservation.kind(),
        ConsumerKind::ConsumerGroup
    );
    assert_eq!(queued_context.reservation.consumer_id() as usize, group_id);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    drop(queued_request);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
}

#[compio::test]
async fn given_full_prepare_and_request_queues_when_poll_completes_should_preserve_progress() {
    let primary_replica = 0;
    let replica_count = 3;
    let prepare_capacity = 1;
    let request_capacity = 1;
    let pipeline = LocalPipeline::with_capacities(prepare_capacity, request_capacity);
    let (mut partition, _) =
        recording_partition_with_pipeline(primary_replica, replica_count, pipeline);
    let client_id = 42;
    let prepared_consumer_id = 7;
    let queued_consumer_id = 9;

    // Fill both admission paths: the first explicit store owns the prepare
    // slot, and the second waits in the only request slot.
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(
                client_id,
                1,
                ConsumerKind::Consumer,
                prepared_consumer_id,
                0,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    partition
        .consensus
        .push_queued_request(consensus::RequestEntry::new(store_offset_request(
            client_id,
            2,
            ConsumerKind::Consumer,
            queued_consumer_id,
            0,
            AckLevel::Quorum,
        )))
        .unwrap();
    let group_id = 8;
    let member_id = 1;
    let auto_commit = true;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let read_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // The group cannot queue its automatic commit. Rejection must leave its
    // progress empty, release its reservation, and retain the existing work.
    assert!(matches!(
        partition.complete_poll(read_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(last_polled, None);
    assert_eq!(committed, None);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    assert_eq!(partition.consensus.request_queue_len(), 1);
    assert_eq!(partition.consensus.sequencer().current_sequence(), 1);
}

#[test]
fn given_full_live_map_when_polling_existing_key_should_reclaim_only_for_new_keys() {
    let (mut partition, _) = recording_partition();
    partition.set_consumer_offsets_max(2);
    partition.offset_space.committed_seeded = true;
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    for id in 7..=8 {
        partition.consumer_offsets.pin().insert(
            id as usize,
            ConsumerOffset::new(ConsumerKind::Consumer, id, 0, String::new()),
        );
    }
    let args = PollingArgs::new(iggy_common::PollingStrategy::first(), 1, true);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(7, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 2);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(9, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 1);
    assert!(partition.consumer_offsets.pin().contains_key(&7));
}

#[compio::test]
async fn given_missing_retained_header_when_journal_progresses_should_retry_without_view_change() {
    let mut partition = test_partition();
    journal_prepare(&partition, 2, Operation::SendMessages).await;
    partition.consensus.sequencer().set_sequence(2);
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(!partition.offset_reservations_need_resync.get());
    let failed_scan = partition.offset_reservations_scan_state;
    for op in 3..=20 {
        journal_prepare(&partition, op, Operation::SendMessages).await;
        partition.consensus.sequencer().set_sequence(op);
        partition.offset_reservations_need_resync.set(true);
        partition.resynchronize_consumer_offset_reservations();
        assert_eq!(partition.offset_reservations_scan_state, failed_scan);
    }
    partition.retry_consumer_offset_reservations();
    assert_ne!(partition.offset_reservations_scan_state, failed_scan);
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::Consumer, 7, 0, 0);
    assert!(partition.auto_commit_admission_ready(ConsumerKind::Consumer, 7));
    assert!(!partition.auto_commit_admission_ready(ConsumerKind::Consumer, 8));
    partition.log.journal().inner.clear_all();
    for op in 1..=3 {
        journal_prepare(&partition, op, Operation::SendMessages).await;
    }
    partition.consensus.sequencer().set_sequence(3);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    partition.retry_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
}

#[compio::test]
async fn given_evicted_committed_prefix_when_promoted_should_preserve_staging_without_latching() {
    let mut partition = test_partition();
    partition.consensus.restore_commit_state(0, 5000);
    partition.consensus.sequencer().set_sequence(5001);
    partition.stage_consumer_offset_upsert(4999, ConsumerKind::Consumer, 7, 0, false);
    journal_prepare(&partition, 5001, Operation::SendMessages).await;
    partition.consensus.set_view(1);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&4999)
    );
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
}

#[test]
fn given_truncated_journal_when_sequencer_is_ahead_should_not_latch_capacity() {
    let (mut partition, _) = recording_partition();
    partition.consensus.sequencer().set_sequence(100);
    partition.offset_reservations_need_resync.set(true);
    partition.resynchronize_consumer_offset_reservations();
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_uncertain()
    );
    assert!(
        partition
            .reserve_consumer_offset(ConsumerKind::Consumer, 7)
            .is_ok()
    );
}

#[test]
fn given_full_backup_phantom_map_when_new_consumer_polls_should_reclaim_without_promotion() {
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_consumer_offsets_max(2);
    partition.offset_space.committed_seeded = true;
    for id in 1..=2 {
        partition.apply_local_poll_offset(ConsumerKind::Consumer, id, 0);
    }
    let args = PollingArgs::new(iggy_common::PollingStrategy::first(), 1, true);
    let _ = partition.build_poll_plan(PollingConsumer::Consumer(3, 0), &args, false);
    assert_eq!(partition.consumer_offsets.len(), 1);
    let retained_id = *partition.consumer_offsets.pin().keys().next().unwrap();
    assert!(retained_id == 1 || retained_id == 2);
    assert!(
        partition
            .check_local_poll_key(ConsumerKind::Consumer, 3)
            .is_ok()
    );
    partition.apply_local_poll_offset(ConsumerKind::Consumer, 3, 0);
    assert!(partition.consumer_offsets.pin().contains_key(&retained_id));
    assert_eq!(partition.consumer_offsets.len(), 2);
    assert!(!partition.consensus.is_primary());
}

#[compio::test]
async fn given_full_offset_table_when_storing_new_id_should_reject_before_replication() {
    let (mut partition, sent_to_clients) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );

    partition
        .on_request(
            store_offset_request(42, 2, ConsumerKind::Consumer, 8, 0, AckLevel::Quorum),
            None,
        )
        .await;
    let sent = sent_to_clients.borrow();
    let denied = sent
        .iter()
        .find_map(|(_, frame)| {
            let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
            )
            .ok()?;
            (header.request == 2).then_some(*header)
        })
        .expect("capacity denial reply");
    assert_eq!(denied.status, IggyError::TooManyConsumerOffsets.as_code());
    assert_eq!(denied.op, 0);
    assert!(partition.consumer_offsets.pin().get(&8).is_none());
    assert_eq!(partition.consensus().pipeline_len(), 0);
}

#[compio::test]
async fn given_full_offset_table_when_updating_existing_id_should_succeed() {
    let (mut partition, _) = recording_partition();
    let directory = tempfile::tempdir().unwrap();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(directory.path().to_string_lossy().into_owned());
    partition.set_consumer_offsets_max(1);
    partition.stats.increment_messages_count(1);
    for request_id in 1..=2 {
        partition
            .on_request(
                store_offset_request(
                    42,
                    request_id,
                    ConsumerKind::Consumer,
                    7,
                    0,
                    AckLevel::Quorum,
                ),
                None,
            )
            .await;
        commit_recorded_loopback(&mut partition).await;
    }
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert_eq!(partition.consumer_offsets.pin().len(), 1);
}

#[compio::test]
async fn given_solo_persisted_offset_when_wal_is_pending_should_withhold_reply() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(
        directory
            .path()
            .join("consumers")
            .to_string_lossy()
            .into_owned(),
    );
    partition.stats.increment_messages_count(1);
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.set_group_commit_delay(std::time::Duration::from_millis(1));
    // Hold the worker until the self-ack path has had a chance to commit.
    persistence.sync();
    assert!(persistence.start());

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;

    assert_eq!(persistence.head(), 1);
    assert_eq!(persistence.durable_op(), 0);
    assert!(
        replies.borrow().is_empty(),
        "reply preceded the WAL barrier"
    );
    assert_eq!(partition.consensus().commit_min(), 0);

    Rc::clone(&persistence).run().await;
    commit_recorded_loopback(&mut partition).await;

    assert_eq!(persistence.durable_op(), 1);
    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(replies.borrow().len(), 1);
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
}
