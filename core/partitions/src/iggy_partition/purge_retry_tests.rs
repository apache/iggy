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

//! Retry full purge after cleanup fails, preserving messages committed meanwhile.
//!
//! The fixture commits replicated operations directly: a replica withholding its
//! own `PrepareOk` can still learn commits acknowledged by the other replicas.
//! These tests exercise the resulting local state, not client acknowledgment.

use std::cell::Cell;
use std::io;
use std::mem::size_of;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use consensus::{Consensus, PipelineEntry, Sequencer, oneshot_channel};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::requests::consumer_offsets::DeleteConsumerOffsetRequest;
use iggy_binary_protocol::{
    AckLevel, Command, Operation, PrepareHeader, WireEncode, WireIdentifier,
};
use iggy_common::{
    ConsumerGroupId, ConsumerKind, ConsumerOffset, Durability, IggyError, PollingStrategy,
};
use journal::durable_storage::{DiskStorage, DurableStorage, OpenMode, RegularFiles, StorageEntry};
use message_bus::IggyMessageBus;
use server_common::Message;
use server_common::send_messages::decode_batch_slice;

use super::tests::{
    checksummed_segment_prepare, disk_poll_partition, disk_poll_partition_with_replicas,
    journal_store_offset, repair_config, store_offset_request, test_partition,
    test_partition_with_replicas,
};
use super::{IggyPartition, PurgeError};
use crate::offset_storage::{PURGE_GENERATION_FILE, PURGE_RESET_FILE, persist_offset};
use crate::{Partition, PollingArgs, PollingConsumer};

const OLD_LAST_OPERATION: u64 = 3;
const FRESH_LAST_OPERATION: u64 = 8;
const PURGE_GENERATION: u64 = 1;
const CONSUMER_ID: usize = 7;
const GROUP_ID: usize = 9;
const OLD_PAYLOAD: &[u8] = b"before purge";
const FRESH_PAYLOAD: &[u8] = b"fresh after purge";

#[compio::test]
async fn given_replicated_consumer_sync_failure_when_retrying_purge_should_preserve_fresh_messages()
{
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Replicated,
        PurgeFault::OffsetDirectorySync(ConsumerKind::Consumer),
    ))
    .await;
}

#[compio::test]
async fn given_replicated_group_sync_failure_when_retrying_purge_should_preserve_fresh_messages() {
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Replicated,
        PurgeFault::OffsetDirectorySync(ConsumerKind::ConsumerGroup),
    ))
    .await;
}

#[compio::test]
async fn given_persisted_consumer_sync_failure_when_retrying_purge_should_preserve_fresh_messages()
{
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Persisted,
        PurgeFault::OffsetDirectorySync(ConsumerKind::Consumer),
    ))
    .await;
}

#[compio::test]
async fn given_persisted_group_sync_failure_when_retrying_purge_should_preserve_fresh_messages() {
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Persisted,
        PurgeFault::OffsetDirectorySync(ConsumerKind::ConsumerGroup),
    ))
    .await;
}

#[compio::test]
async fn given_replicated_marker_failure_when_retrying_purge_should_preserve_fresh_messages() {
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Replicated,
        PurgeFault::CompletionMarkerRename,
    ))
    .await;
}

#[compio::test]
async fn given_persisted_marker_failure_when_retrying_purge_should_preserve_fresh_messages() {
    Box::pin(assert_retry_preserves_fresh_messages(
        Durability::Persisted,
        PurgeFault::CompletionMarkerRename,
    ))
    .await;
}

#[compio::test]
async fn given_fresh_history_after_purge_when_new_generation_arrives_should_reset_again() {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    partition.purge(&config, PURGE_GENERATION).await.unwrap();

    append_committed_history(
        &mut partition,
        OLD_LAST_OPERATION + 1..=FRESH_LAST_OPERATION,
        FRESH_PAYLOAD,
    )
    .await;
    assert_eq!(partition.stats.messages_count_inconsistent(), 5);

    partition
        .purge(&config, PURGE_GENERATION + 1)
        .await
        .unwrap();

    assert_eq!(partition.applied_purge_generation(), PURGE_GENERATION + 1);
    assert_eq!(partition.purge_floor_op(), FRESH_LAST_OPERATION);
    assert_eq!(partition.offset_frontier(), 0);
    assert_eq!(partition.mint_frontier(), 0);
    assert_eq!(partition.stats.messages_count_inconsistent(), 0);
    assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
    assert_next_messages(&mut partition, &[], FRESH_PAYLOAD).await;
}

#[compio::test]
async fn given_pending_purge_cleanup_when_offset_commits_arrive_should_apply_them_after_retry() {
    for policy in [Durability::Replicated, Durability::Persisted] {
        Box::pin(assert_offset_commits_wait_for_cleanup(
            policy,
            CommitPath::Journal,
        ))
        .await;
    }
}

#[compio::test]
async fn given_pending_purge_cleanup_when_primary_has_offset_commits_should_keep_them_in_pipeline()
{
    for policy in [Durability::Replicated, Durability::Persisted] {
        Box::pin(assert_offset_commits_wait_for_cleanup(
            policy,
            CommitPath::PrimaryPipeline,
        ))
        .await;
    }
}

#[compio::test]
async fn given_pending_purge_cleanup_when_singleton_stores_without_ack_should_refuse_until_retry() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    assert_eq!(partition.consensus().replica_count(), 1);
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    persist_old_bookmarks(&partition).await;
    let storage = FaultingPurgeStorage::new(
        directory.path(),
        PurgeFault::OffsetDirectorySync(ConsumerKind::Consumer),
    );
    assert!(matches!(
        partition
            .purge_with_storage(&config, PURGE_GENERATION, &storage)
            .await,
        Err(PurgeError::OffsetCleanupNotRecorded(_))
    ));
    append_committed_history(
        &mut partition,
        OLD_LAST_OPERATION + 1..=FRESH_LAST_OPERATION,
        FRESH_PAYLOAD,
    )
    .await;

    let consumer_id = u32::try_from(CONSUMER_ID).unwrap();
    let consumer = PollingConsumer::Consumer(CONSUMER_ID, 0);
    let (denial_sender, denial_receiver) = oneshot_channel();
    partition
        .on_request(
            store_offset_request(
                42,
                1,
                ConsumerKind::Consumer,
                consumer_id,
                2,
                AckLevel::NoAck,
            ),
            Some(denial_sender),
        )
        .await;
    assert_eq!(
        denial_receiver.await.unwrap().header().status,
        IggyError::TransientNotAccepted.as_code(),
        "the singleton fast path must not write a bookmark that cleanup would erase"
    );
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert!(!directory.path().join("consumer_offsets/7").exists());
    assert!(partition.fatal().is_none());

    partition
        .purge_with_storage(&config, PURGE_GENERATION, &storage)
        .await
        .unwrap();
    let (success_sender, success_receiver) = oneshot_channel();
    partition
        .on_request(
            store_offset_request(
                42,
                2,
                ConsumerKind::Consumer,
                consumer_id,
                2,
                AckLevel::NoAck,
            ),
            Some(success_sender),
        )
        .await;
    assert_eq!(success_receiver.await.unwrap().header().status, 0);
    assert_eq!(partition.get_consumer_offset(consumer), Some(2));
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert!(directory.path().join("consumer_offsets/7").exists());
}

#[compio::test]
async fn given_singleton_purge_marker_when_consensus_restarts_should_accept_new_operation_one() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    partition.purge(&config, PURGE_GENERATION).await.unwrap();
    assert_eq!(partition.purge_floor_op(), OLD_LAST_OPERATION);
    drop(partition);

    // Singleton boot starts a new consensus sequence. Reuse only its durable
    // purge markers here; this fixture does not exercise the full boot loader.
    let mut restarted = Box::new(test_partition());
    restarted.set_partition_dir(directory.path().to_string_lossy().into_owned());
    restarted.log.retire_front().unwrap();
    restarted.install_empty_segment(&config, 0).await.unwrap();
    restarted.hydrate_applied_purge_generation().await.unwrap();
    assert_eq!(restarted.applied_purge_generation(), PURGE_GENERATION);
    assert_eq!(restarted.consensus().sequencer().current_sequence(), 0);
    assert_eq!(
        restarted.purge_floor_op(),
        0,
        "an earlier process's purge floor must not fence the new consensus sequence"
    );

    append_committed_history(&mut restarted, 1..=1, FRESH_PAYLOAD).await;

    assert_eq!(restarted.consensus().commit_min(), 1);
    assert_eq!(restarted.stats.messages_count_inconsistent(), 1);
    assert_next_messages(&mut restarted, &[0], FRESH_PAYLOAD).await;
}

#[compio::test]
async fn given_replicated_purge_when_empty_history_recovery_retries_should_poll_new_operation_one()
{
    for fault in [
        PurgeFault::ResetMarkerRename,
        PurgeFault::PartitionDirectorySync,
    ] {
        let config = repair_config();
        let (directory, mut partition) =
            Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
        partition.runtime_options.durability = Durability::Replicated;
        partition.runtime_options.consumer_offset_durability = Durability::Replicated;
        append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
        partition.purge(&config, PURGE_GENERATION).await.unwrap();
        drop(partition);

        let mut recovering = Box::pin(recover_replicated_partition(directory.path())).await;
        assert!(recovering.persistence.is_none());
        assert_eq!(recovering.consensus().sequencer().current_sequence(), 0);
        assert_eq!(recovering.purge_floor_op(), OLD_LAST_OPERATION);
        assert!(recovering.purge_recovery_pending());
        assert!(!recovering.queued_requests_ready());

        let storage = FaultingPurgeStorage::new(directory.path(), fault);
        assert!(
            recovering
                .recover_purge_boundary_with_storage(&storage, 0)
                .await
                .is_err()
        );
        assert!(storage.failed.get());
        assert!(recovering.purge_recovery_pending());
        assert_eq!(recovering.purge_floor_op(), OLD_LAST_OPERATION);
        drop(recovering);

        // Lose all memory again, including the failed recovery attempt. The
        // directory sync error can leave the replacement record visible to boot.
        let mut restarted = Box::pin(recover_replicated_partition(directory.path())).await;
        assert!(restarted.purge_recovery_pending());
        assert_eq!(restarted.applied_purge_generation(), PURGE_GENERATION);
        restarted.recover_purge_boundary(0).await.unwrap();
        assert!(!restarted.purge_recovery_pending());
        assert_eq!(restarted.purge_floor_op(), 0);
        assert_eq!(restarted.purge_reset.unwrap().floor, 0);

        append_committed_history(&mut restarted, 1..=1, FRESH_PAYLOAD).await;
        assert_eq!(restarted.consensus().commit_min(), 1);
        assert_eq!(restarted.stats.messages_count_inconsistent(), 1);
        assert_next_messages(&mut restarted, &[0], FRESH_PAYLOAD).await;

        // A repeated recovery notification must not clear the newly committed tail.
        restarted.recover_purge_boundary(0).await.unwrap();
        assert_next_messages(&mut restarted, &[0], FRESH_PAYLOAD).await;
    }
}

#[compio::test]
async fn given_replicated_purge_when_selected_history_is_nonempty_should_wait_for_snapshot() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    partition.runtime_options.durability = Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = Durability::Replicated;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    partition.purge(&config, PURGE_GENERATION).await.unwrap();
    drop(partition);

    let mut restarted = Box::pin(recover_replicated_partition(directory.path())).await;
    let poll = restarted
        .build_poll_plan(
            PollingConsumer::Consumer(CONSUMER_ID, 0),
            &PollingArgs::new(PollingStrategy::next(), 1, false),
            true,
        )
        .execute()
        .await;
    assert!(matches!(
        restarted.complete_poll(poll),
        Err(IggyError::TransientNotAccepted)
    ));

    // The peer could hold either the old sequence or a new sequence reusing its
    // numbers. Journal replay alone cannot tell which messages the purge removed.
    restarted
        .recover_purge_boundary(OLD_LAST_OPERATION)
        .await
        .unwrap();
    assert!(restarted.purge_recovery_pending());
    assert!(restarted.consensus().is_transferring());
    assert_eq!(restarted.purge_floor_op(), OLD_LAST_OPERATION);
    restarted
        .apply_repaired_prepare(checksummed_segment_prepare(1, 0, 0, OLD_PAYLOAD))
        .await;
    assert!(restarted.log.journal().inner.header_by_op(1).is_none());
    assert_eq!(restarted.consensus().commit_min(), 0);
}

// Rebuild only the partition and its empty segment after purge. Server boot and
// consensus selection are exercised separately; no volatile journal is retained.
async fn recover_replicated_partition(directory: &Path) -> Box<IggyPartition<IggyMessageBus>> {
    let mut partition = Box::new(test_partition_with_replicas(3));
    partition.runtime_options.durability = Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = Durability::Replicated;
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    partition.hydrate_applied_purge_generation().await.unwrap();
    partition
}

#[compio::test]
async fn given_reset_marker_publication_failure_when_purging_should_require_fencing() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    let storage = FaultingPurgeStorage::new(directory.path(), PurgeFault::ResetMarkerRename);

    let result = partition
        .purge_with_storage(&config, PURGE_GENERATION, &storage)
        .await;

    assert!(
        storage.failed.get(),
        "purge must reach reset marker publication"
    );
    assert!(matches!(result, Err(PurgeError::Unserviceable(_))));
    assert_eq!(partition.applied_purge_generation(), 0);
    assert!(partition.purge_deferred);
    assert!(!directory.path().join(PURGE_RESET_FILE).exists());
    let mut reloaded = Box::new(test_partition());
    reloaded.set_partition_dir(directory.path().to_string_lossy().into_owned());
    reloaded.hydrate_applied_purge_generation().await.unwrap();
    assert_eq!(reloaded.applied_purge_generation(), 0);
}

// Keep refusal, cleanup, and the deferred commits together so their ordering stays visible.
#[allow(clippy::too_many_lines)]
async fn assert_offset_commits_wait_for_cleanup(policy: Durability, commit_path: CommitPath) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.runtime_options.consumer_offset_durability = policy;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    persist_old_bookmarks(&partition).await;
    let storage = FaultingPurgeStorage::new(
        directory.path(),
        PurgeFault::OffsetDirectorySync(ConsumerKind::Consumer),
    );
    assert!(matches!(
        partition
            .purge_with_storage(&config, PURGE_GENERATION, &storage)
            .await,
        Err(PurgeError::OffsetCleanupNotRecorded(_))
    ));
    append_committed_history(
        &mut partition,
        OLD_LAST_OPERATION + 1..=FRESH_LAST_OPERATION,
        FRESH_PAYLOAD,
    )
    .await;

    // Fresh progress must not land in a directory that cleanup still has to sweep.
    let consumer = PollingConsumer::Consumer(CONSUMER_ID, 0);
    let pending_poll = partition
        .build_poll_plan(
            consumer,
            &PollingArgs::new(PollingStrategy::next(), 5, true),
            true,
        )
        .execute()
        .await;
    assert!(matches!(
        partition.complete_poll(pending_poll),
        Err(IggyError::TransientNotAccepted)
    ));
    assert_eq!(partition.get_consumer_offset(consumer), None);

    let store_operation = FRESH_LAST_OPERATION + 1;
    let delete_operation = store_operation + 1;
    let consumer_id = u32::try_from(CONSUMER_ID).unwrap();
    journal_store_offset(&mut partition, store_operation, consumer_id, 2).await;
    journal_consumer_offset_delete(&mut partition, delete_operation, consumer_id).await;
    if matches!(commit_path, CommitPath::PrimaryPipeline) {
        assert!(partition.consensus().is_primary());
        assert!(
            partition.persistence.is_none(),
            "exercise the pipeline path without a WAL"
        );
        for operation in [store_operation, delete_operation] {
            let header = partition
                .log
                .journal()
                .inner
                .header_by_op(operation)
                .unwrap();
            partition.consensus().with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(header));
            });
        }
    }
    partition.consensus().advance_commit_max(store_operation);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), FRESH_LAST_OPERATION);
    if matches!(commit_path, CommitPath::PrimaryPipeline) {
        assert_eq!(
            partition.consensus().pipeline_len(),
            2,
            "cleanup must retain the primary's reply slots"
        );
    }
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&store_operation)
    );
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&delete_operation)
    );
    assert!(partition.fatal().is_none());

    partition
        .purge_with_storage(&config, PURGE_GENERATION, &storage)
        .await
        .expect("finish cleanup before applying the waiting offset operations");
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&store_operation)
    );
    assert!(
        partition
            .pending_consumer_offset_commits
            .contains_key(&delete_operation)
    );
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), store_operation);
    assert_eq!(partition.get_consumer_offset(consumer), Some(2));

    partition.consensus().advance_commit_max(delete_operation);
    partition.commit_journal(&config).await;
    assert_eq!(partition.consensus().commit_min(), delete_operation);
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert_next_messages(&mut partition, &[0, 1, 2, 3, 4], FRESH_PAYLOAD).await;

    let fresh_poll = partition
        .build_poll_plan(
            consumer,
            &PollingArgs::new(PollingStrategy::next(), 5, true),
            true,
        )
        .execute()
        .await;
    partition
        .complete_poll(fresh_poll)
        .expect("automatic progress resumes after cleanup");
    assert_eq!(partition.get_consumer_offset(consumer), Some(4));
}

async fn assert_retry_preserves_fresh_messages(policy: Durability, fault: PurgeFault) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.runtime_options.consumer_offset_durability = policy;
    append_committed_history(&mut partition, 1..=OLD_LAST_OPERATION, OLD_PAYLOAD).await;
    persist_old_bookmarks(&partition).await;
    assert_eq!(partition.stats.messages_count_inconsistent(), 3);

    let storage = FaultingPurgeStorage::new(directory.path(), fault);
    let result = partition
        .purge_with_storage(&config, PURGE_GENERATION, &storage)
        .await;
    assert!(
        storage.failed.get(),
        "purge must reach the selected I/O fault"
    );
    match fault {
        PurgeFault::OffsetDirectorySync(_) => {
            assert!(matches!(
                result,
                Err(PurgeError::OffsetCleanupNotRecorded(_))
            ));
            assert_eq!(
                storage.entries_at_failed_sync.get(),
                Some(0),
                "the injected sync failure must follow bookmark deletion"
            );
        }
        PurgeFault::CompletionMarkerRename => {
            assert!(matches!(result, Err(PurgeError::GenerationNotRecorded(_))));
        }
        PurgeFault::ResetMarkerRename | PurgeFault::PartitionDirectorySync => {
            unreachable!("reset marker failure requires fencing")
        }
    }
    assert_eq!(partition.applied_purge_generation(), 0);
    assert!(partition.purge_deferred);
    assert_eq!(partition.purge_floor_op(), OLD_LAST_OPERATION);
    assert_eq!(partition.stats.messages_count_inconsistent(), 0);

    // Other replicas can commit fresh sends while this replica defers PrepareOk.
    // Apply those commits locally before the same purge generation is retried.
    append_committed_history(
        &mut partition,
        OLD_LAST_OPERATION + 1..=FRESH_LAST_OPERATION,
        FRESH_PAYLOAD,
    )
    .await;
    assert_eq!(partition.consensus().commit_min(), FRESH_LAST_OPERATION);
    assert_eq!(partition.offset_frontier(), 5);
    assert_eq!(partition.mint_frontier(), 5);
    assert_eq!(partition.stats.messages_count_inconsistent(), 5);
    let fresh_bytes = partition.stats.size_bytes_inconsistent();
    let log_path = directory.path().join("00000000000000000000.log");
    let fresh_log = std::fs::read(&log_path).unwrap();
    assert!(!fresh_log.is_empty());

    partition
        .purge_with_storage(&config, PURGE_GENERATION, &storage)
        .await
        .expect("retry cleanup without resetting the fresh history");

    assert_eq!(partition.applied_purge_generation(), PURGE_GENERATION);
    assert!(!partition.purge_deferred);
    if matches!(fault, PurgeFault::OffsetDirectorySync(_)) {
        assert_eq!(
            storage.directory_sync_attempts.get(),
            2,
            "retry must sync the directory even though its files are already gone"
        );
    }
    for directory_name in ["consumer_offsets", "consumer_group_offsets"] {
        assert!(
            DiskStorage
                .entries(&directory.path().join(directory_name))
                .await
                .unwrap()
                .is_empty()
        );
    }
    assert_fresh_history_unchanged(&partition, fresh_bytes, &log_path, &fresh_log);
    assert_next_messages(&mut partition, &[0, 1, 2, 3, 4], FRESH_PAYLOAD).await;

    // Redundant delivery after completion must preserve the same history too.
    partition.purge(&config, PURGE_GENERATION).await.unwrap();
    assert_fresh_history_unchanged(&partition, fresh_bytes, &log_path, &fresh_log);
    assert_next_messages(&mut partition, &[0, 1, 2, 3, 4], FRESH_PAYLOAD).await;
}

fn assert_fresh_history_unchanged(
    partition: &IggyPartition<IggyMessageBus>,
    fresh_bytes: u64,
    log_path: &Path,
    fresh_log: &[u8],
) {
    assert_eq!(
        partition.purge_floor_op(),
        OLD_LAST_OPERATION,
        "retry must not fence the fresh committed operations"
    );
    assert_eq!(partition.consensus().commit_min(), FRESH_LAST_OPERATION);
    assert_eq!(partition.offset_frontier(), 5);
    assert_eq!(partition.mint_frontier(), 5);
    assert_eq!(partition.stats.messages_count_inconsistent(), 5);
    assert_eq!(partition.stats.size_bytes_inconsistent(), fresh_bytes);
    assert_eq!(std::fs::read(log_path).unwrap(), fresh_log);
}

/// Poll both consumer kinds before checking results, with automatic commits disabled.
async fn assert_next_messages(
    partition: &mut IggyPartition<IggyMessageBus>,
    expected_offsets: &[u64],
    expected_payload: &[u8],
) {
    let mut completed_polls = Vec::new();
    for consumer in [
        PollingConsumer::Consumer(CONSUMER_ID, 0),
        PollingConsumer::ConsumerGroup(GROUP_ID, 1),
    ] {
        let result = partition
            .build_poll_plan(
                consumer,
                &PollingArgs::new(PollingStrategy::next(), 10, false),
                true,
            )
            .execute()
            .await;
        let completion = partition.complete_poll(result).expect("complete Next poll");
        completed_polls.push((consumer, completion));
    }
    for (consumer, completion) in completed_polls {
        let mut offsets = Vec::new();
        for fragment in &completion.fragments {
            let batch = decode_batch_slice(fragment.as_slice()).unwrap();
            assert_eq!(batch.message_count(), 1);
            assert_eq!(batch.iter().next().unwrap().payload, expected_payload);
            offsets.push(batch.header.base_offset);
        }
        assert_eq!(offsets, expected_offsets, "{consumer:?}");
        assert_eq!(partition.get_consumer_offset(consumer), None);
    }
}

/// Apply replicated sends and their learned commit frontier through production paths.
async fn append_committed_history(
    partition: &mut IggyPartition<IggyMessageBus>,
    operations: RangeInclusive<u64>,
    payload: &[u8],
) {
    let last_operation = *operations.end();
    for operation in operations {
        let prepare = checksummed_segment_prepare(operation, 0, 0, payload);
        partition.apply_replicated_operation(prepare).await.unwrap();
        partition.consensus().sequencer().set_sequence(operation);
    }
    partition.consensus().advance_commit_max(last_operation);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none());
}

async fn journal_consumer_offset_delete(
    partition: &mut IggyPartition<IggyMessageBus>,
    operation: u64,
    consumer_id: u32,
) {
    let body = DeleteConsumerOffsetRequest {
        consumer: WireConsumer::consumer(WireIdentifier::Numeric(consumer_id)),
        stream_id: WireIdentifier::Numeric(1),
        topic_id: WireIdentifier::Numeric(1),
        partition_id: Some(0),
        ack: AckLevel::Quorum,
    }
    .to_bytes();
    let message_size = size_of::<PrepareHeader>() + body.len();
    let mut prepare = Message::<PrepareHeader>::new(message_size);
    prepare.as_mut_slice()[size_of::<PrepareHeader>()..].copy_from_slice(&body);
    let prepare = prepare.transmute_header(|_, header: &mut PrepareHeader| {
        header.command = Command::Prepare;
        header.operation = Operation::DeleteConsumerOffset;
        header.op = operation;
        header.group = partition.namespace().inner();
        header.size = u32::try_from(message_size).unwrap();
    });
    partition.apply_replicated_operation(prepare).await.unwrap();
    partition.consensus().sequencer().set_sequence(operation);
}

/// Bookmark 2 remains a valid offset in the five messages written after the failure.
async fn persist_old_bookmarks(partition: &IggyPartition<IggyMessageBus>) {
    for (kind, consumer_id) in [
        (ConsumerKind::Consumer, CONSUMER_ID),
        (ConsumerKind::ConsumerGroup, GROUP_ID),
    ] {
        let numeric_id = u32::try_from(consumer_id).unwrap();
        let path = partition.persisted_offset_path(kind, numeric_id).unwrap();
        persist_offset(&path, 2, true).await.unwrap();
        let bookmark = ConsumerOffset::new(kind, numeric_id, 2, path);
        match kind {
            ConsumerKind::Consumer => {
                partition
                    .consumer_offsets
                    .pin()
                    .insert(consumer_id, bookmark);
            }
            ConsumerKind::ConsumerGroup => {
                partition
                    .consumer_group_offsets
                    .pin()
                    .insert(ConsumerGroupId(consumer_id), bookmark);
            }
        }
        partition.seed_recovered_consumer_offset(kind, numeric_id, 2, 2);
    }
}

#[derive(Clone, Copy)]
enum CommitPath {
    Journal,
    PrimaryPipeline,
}

#[derive(Clone, Copy)]
enum PurgeFault {
    OffsetDirectorySync(ConsumerKind),
    CompletionMarkerRename,
    ResetMarkerRename,
    PartitionDirectorySync,
}

struct FaultingPurgeStorage {
    fault: PurgeFault,
    target: PathBuf,
    failed: Cell<bool>,
    entries_at_failed_sync: Cell<Option<usize>>,
    directory_sync_attempts: Cell<usize>,
}

impl FaultingPurgeStorage {
    fn new(directory: &Path, fault: PurgeFault) -> Self {
        let target = directory.join(match fault {
            PurgeFault::OffsetDirectorySync(ConsumerKind::Consumer) => "consumer_offsets",
            PurgeFault::OffsetDirectorySync(ConsumerKind::ConsumerGroup) => {
                "consumer_group_offsets"
            }
            PurgeFault::CompletionMarkerRename => PURGE_GENERATION_FILE,
            PurgeFault::ResetMarkerRename => PURGE_RESET_FILE,
            PurgeFault::PartitionDirectorySync => "",
        });
        Self {
            fault,
            target,
            failed: Cell::new(false),
            entries_at_failed_sync: Cell::new(None),
            directory_sync_attempts: Cell::new(0),
        }
    }
}

impl DurableStorage for FaultingPurgeStorage {
    type File = <DiskStorage as DurableStorage>::File;

    fn writer_identity(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        DiskStorage.writer_identity(path)
    }

    async fn open(&self, path: &Path, mode: OpenMode) -> io::Result<Self::File> {
        DiskStorage.open(path, mode).await
    }

    async fn create_directories(&self, path: &Path) -> io::Result<()> {
        DiskStorage.create_directories(path).await
    }

    async fn sync_directory(&self, path: &Path) -> io::Result<()> {
        if matches!(self.fault, PurgeFault::PartitionDirectorySync)
            && path == self.target
            && !self.failed.replace(true)
        {
            return Err(io::Error::other(
                "injected partition directory sync failure",
            ));
        }
        if matches!(self.fault, PurgeFault::OffsetDirectorySync(_)) && path == self.target {
            self.directory_sync_attempts
                .set(self.directory_sync_attempts.get() + 1);
            if !self.failed.replace(true) {
                self.entries_at_failed_sync
                    .set(Some(DiskStorage.entries(path).await?.len()));
                return Err(io::Error::other("injected offset directory sync failure"));
            }
        }
        DiskStorage.sync_directory(path).await
    }

    async fn rename(&self, source: &Path, target: &Path) -> io::Result<()> {
        if matches!(
            self.fault,
            PurgeFault::CompletionMarkerRename | PurgeFault::ResetMarkerRename
        ) && target == self.target
            && !self.failed.replace(true)
        {
            return Err(io::Error::other("injected purge marker rename failure"));
        }
        DiskStorage.rename(source, target).await
    }

    async fn remove_file(&self, path: &Path) -> io::Result<()> {
        DiskStorage.remove_file(path).await
    }

    async fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        DiskStorage.hard_link(source, target).await
    }

    async fn exists(&self, path: &Path) -> io::Result<bool> {
        DiskStorage.exists(path).await
    }

    async fn exists_following_links(&self, path: &Path) -> io::Result<bool> {
        DiskStorage.exists_following_links(path).await
    }

    async fn entries(&self, path: &Path) -> io::Result<Vec<StorageEntry>> {
        DiskStorage.entries(path).await
    }

    async fn regular_files(&self, path: &Path) -> io::Result<RegularFiles> {
        DiskStorage.regular_files(path).await
    }

    async fn remove_tree(&self, path: &Path) -> io::Result<()> {
        DiskStorage.remove_tree(path).await
    }
}
