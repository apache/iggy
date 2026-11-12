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
async fn committed_offset_mutations_obey_checkpoint_dependencies() {
    const CLIENT: u128 = 42;
    const CONSUMER: u32 = 7;
    let kind = ConsumerKind::Consumer;
    for delete in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (mut partition, replies, config, parent) =
            partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3)
                .await;
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        let request = if delete {
            let initial = PendingConsumerOffsetCommit::upsert(kind, CONSUMER, 0);
            partition
                .persist_consumer_offset_commit(initial)
                .await
                .unwrap();
            partition.apply_consumer_offset_commit(initial);
            delete_offset_request(CLIENT, 1, CONSUMER)
        } else {
            store_offset_request(CLIENT, 1, kind, CONSUMER, 0, AckLevel::Quorum)
        };
        let store = request.transmute_header(|request, header: &mut PrepareHeader| {
            *header = PrepareHeader {
                command: Command::Prepare,
                operation: request.operation,
                cluster: TEST_CLUSTER,
                group: request.group,
                size: request.size,
                client: CLIENT,
                user_id: request.user_id,
                session: request.session,
                request: request.request,
                retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
                op: 2,
                parent,
                ..PrepareHeader::default()
            };
            header.checksum = header.identity_checksum();
        });
        let header = *store.header();
        persistence
            .append(store.clone().into_frozen(), false)
            .unwrap();
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(header));
        });
        partition
            .log
            .journal()
            .inner
            .append(store.into_frozen())
            .await
            .unwrap();
        if delete {
            partition.stage_consumer_offset_delete(header.op, kind, CONSUMER);
        } else {
            partition.stage_consumer_offset_upsert(header.op, kind, CONSUMER, 0, false);
        }
        persistence.checkpoint(1);
        partition.consensus.advance_commit_max(header.op);
        let path = partition.persisted_offset_path(kind, CONSUMER).unwrap();

        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(
            partition.consensus.commit_min(),
            if delete { 1 } else { header.op },
            "only a delete must wait for the checkpoint"
        );
        assert!(std::path::Path::new(&path).exists());
        assert_eq!(replies.borrow().len(), if delete { 1 } else { 2 });

        partition.start_persistence();
        persistence.drain().await.unwrap();
        assert!(!partition.persistence_checkpoint_pending());
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), header.op);
        assert_eq!(std::path::Path::new(&path).is_file(), !delete);
        assert_eq!(replies.borrow().len(), 2);
    }
}

#[compio::test]
async fn no_ack_offset_writes_continue_during_a_pending_checkpoint() {
    const CONSUMER: u32 = 7;
    let kind = ConsumerKind::Consumer;
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _replies, _config, _parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 1).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence.checkpoint(1);
    let path = partition.persisted_offset_path(kind, CONSUMER).unwrap();
    let (sender, receiver) = consensus::oneshot_channel();
    let mut receiver = std::pin::pin!(receiver);

    partition
        .on_request(
            store_offset_request(42, 1, kind, CONSUMER, 0, AckLevel::NoAck),
            Some(sender),
        )
        .await;
    assert!(partition.fatal().is_none());
    assert!(std::path::Path::new(&path).exists());
    let reply = receiver.as_mut().await.unwrap();
    assert_eq!(reply.header().status, 0);

    partition.start_persistence();
    persistence.drain().await.unwrap();
    partition.resume_queued_requests().await;
    assert!(std::path::Path::new(&path).is_file());
}

#[compio::test]
async fn consumer_offset_open_failure_does_not_poison_the_wal_and_can_be_retried() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let path = directory.path().join("consumer-offset");
    std::fs::create_dir(&path).unwrap();
    assert!(matches!(
        partition
            .write_consumer_offset(path.to_str().unwrap(), 7, false)
            .await,
        Err(IggyError::CannotOpenConsumerOffsetsFile(_))
    ));
    partition.drive_persistence().await;
    assert!(partition.persistence.as_ref().unwrap().failure().is_none());
    assert!(partition.fatal.is_none());

    std::fs::remove_dir(&path).unwrap();
    partition
        .write_consumer_offset(path.to_str().unwrap(), 7, false)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        crate::offset_storage::encode_offset_record(7)
    );
    assert!(
        partition
            .persistence
            .as_ref()
            .unwrap()
            .take_offset_file(path.to_str().unwrap())
            .is_some()
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn consumer_offset_write_failure_keeps_its_error_kind_and_original_writer() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    assert!(matches!(
        partition.write_consumer_offset(DEV_FULL, 7, false).await,
        Err(IggyError::CannotWriteToFile)
    ));
    let persistence = partition.persistence.as_ref().unwrap();
    assert_eq!(
        persistence.failure().unwrap().kind(),
        std::io::ErrorKind::StorageFull
    );
    assert!(persistence.take_offset_file(DEV_FULL).is_some());
    partition.drive_persistence().await;
    assert!(
        partition.fatal.is_some(),
        "a failed write must remain fenced"
    );
    assert_eq!(
        partition.fatal.as_ref().unwrap().operation,
        Some(Operation::StoreConsumerOffset)
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn workerless_offset_write_failure_invalidates_its_retained_descriptor() {
    let (partition, _) = recording_partition_at(0, 3);
    assert!(partition.persistence.is_none());
    assert!(matches!(
        partition.write_consumer_offset(DEV_FULL, 7, false).await,
        Err(IggyError::CannotWriteToFile)
    ));
    assert!(partition.workerless_offset_files.take(DEV_FULL).is_none());
    assert!(
        partition.fatal.is_none(),
        "workerless errors retain their caller's policy"
    );
}

#[compio::test]
async fn workerless_offsets_reuse_a_writer_and_delete_reopens_the_recreated_path() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    let offset_dir = directory.path().join("offsets");
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(offset_dir.to_string_lossy().into_owned());
    // Persisted offsets bypass the workerless descriptor cache.
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let kind = ConsumerKind::Consumer;
    let consumer = 7;
    let path = partition.persisted_offset_path(kind, consumer).unwrap();
    let store = PendingConsumerOffsetCommit::upsert(kind, consumer, 3);
    partition
        .persist_consumer_offset_commit(store)
        .await
        .unwrap();
    let (file, permit) = partition.workerless_offset_files.checkout(&path).unwrap();
    assert!(file.is_some(), "first write retained its descriptor");
    partition
        .workerless_offset_files
        .put(&path, file.unwrap(), permit);
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(kind, consumer, 5))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        crate::offset_storage::encode_offset_record(5)
    );
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::delete(kind, consumer))
        .await
        .unwrap();
    assert!(partition.workerless_offset_files.take(&path).is_none());
    assert!(!std::path::Path::new(&path).exists());
    partition
        .persist_consumer_offset_commit(store)
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(&path).unwrap(),
        crate::offset_storage::encode_offset_record(3)
    );
    partition.invalidate_poll_history();
    assert!(partition.workerless_offset_files.take(&path).is_none());
}

/// The record is an EXCLUSIVE frontier, so `u64::MAX` can never be covered:
/// covering it would need `u64::MAX + 1`. Saturating and reporting success
/// there confirms an offset to a client that the next boot re-mints, which is
/// the exact defect this path exists to prevent, at the one offset where it
/// would be silent.
#[compio::test]
async fn given_an_exhausted_offset_space_when_fencing_should_refuse_rather_than_confirm() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    assert!(
        partition.reserve_offsets_through(u64::MAX - 1).await,
        "the last representable offset is still reservable"
    );
    assert_eq!(
        last_recorded_reservation(&store),
        u64::MAX,
        "a claim clamped to the top of the space still sits strictly above \
         the offset it covers"
    );

    let attempts = store.attempts.get();
    assert!(
        !partition.reserve_offsets_through(u64::MAX).await,
        "the terminal offset must be refused, not confirmed"
    );
    assert_eq!(
        store.attempts.get(),
        attempts,
        "and refused without attempting a write it could not make correct"
    );
}

/// The boot after that refusal: the counter resumes AT the terminal offset
/// and the fence keeps refusing it, so nothing a client holds is reissued.
#[compio::test]
async fn given_a_saturated_reservation_when_restored_should_keep_refusing_the_terminal_offset() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(0, u64::MAX);
    partition.set_superblock(store.clone(), Some(&recovered));
    partition.set_offset_reservation_lease(test_lease(16));
    partition.restore_offset_frontier(Some(&recovered));

    assert_eq!(
        partition.mint_frontier(),
        u64::MAX,
        "the append point resumes above every offset the reservation covered"
    );
    assert!(
        !partition.reserve_offsets_through(u64::MAX).await,
        "the one offset the record never covered must stay unmintable"
    );
}

#[test]
fn given_group_read_without_auto_commit_when_history_changes_should_not_record_last_polled() {
    let (mut partition, _) = recording_partition();
    let group_id = 7;
    let member_id = 1;
    let auto_commit = false;
    let consumer = PollingConsumer::ConsumerGroup(group_id, member_id);
    let old_result = poll_read_result(&partition, consumer, auto_commit, Some(0));

    // Group reads still record last_polled with automatic commits disabled.
    // Retiring the history must prevent even that local progress update.
    partition.invalidate_poll_history();
    assert!(matches!(
        partition.complete_poll(old_result),
        Err(IggyError::TransientNotAccepted)
    ));
    let (last_polled, committed) = partition.group_offset_state(group_id as u64);
    assert_eq!(
        last_polled, None,
        "the old read must not record group progress"
    );
    assert_eq!(committed, None, "automatic commits are disabled");
}

#[compio::test]
async fn given_a_crafted_offset_name_when_admitting_should_not_reserve_offsets() {
    let store = Rc::new(RecordingSuperblock::default());
    let mut partition = solo_recording_partition();
    partition.set_superblock(store.clone(), None);
    partition.set_offset_reservation_lease(test_lease(16));

    // The consumer prefix is three bytes. Its name overlaps the batch
    // length and count fields, making this non-send body a valid header.
    let mut name = vec![0u8; 255];
    name[29..37].copy_from_slice(&256u64.to_le_bytes());
    name[45..49].copy_from_slice(&[0x7F; 4]);
    let body = StoreConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: ConsumerKind::Consumer.as_code(),
            id: WireIdentifier::String(WireName::new(String::from_utf8(name).unwrap()).unwrap()),
        },
        stream_id: WireIdentifier::Numeric(1),
        topic_id: WireIdentifier::Numeric(1),
        partition_id: Some(0),
        offset: 0,
        ack: AckLevel::NoAck,
    }
    .to_bytes();
    assert!(BatchHeader::decode(&body).is_ok());
    let header_size = std::mem::size_of::<RoutedRequestHeader>();
    let total = header_size + body.len();
    let mut message = Message::<RoutedRequestHeader>::new(total);
    message.as_mut_slice()[header_size..].copy_from_slice(&body);
    let message = message.transmute_header(|_, header: &mut RoutedRequestHeader| {
        header.command = Command::Request;
        header.operation = Operation::StoreConsumerOffset;
        header.client = 1;
        header.session = 1;
        header.request = 1;
        header.group = IggyNamespace::new(1, 1, 0).inner();
        header.size = u32::try_from(total).unwrap();
    });

    assert_eq!(partition.request_mint_ceiling(&message), None);
    assert_eq!(
        partition.admit_reserved_send(&message).await,
        crate::PartitionIoVerdict::Ready
    );
    assert!(store.writes.borrow().is_empty());
    assert_eq!(partition.durable_offset_reserved.get(), 0);
}

#[compio::test]
async fn given_a_deleted_group_when_reconciling_should_list_its_external_offset() {
    let (partition, _) = recording_partition();
    partition.seed_recovered_consumer_offset(ConsumerKind::ExternalGroup, 7, 11, 11);
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 9, 11, 11);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(9),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 9, 11, String::new()),
    );

    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![
            (ConsumerKind::ExternalGroup, 7),
            (ConsumerKind::ConsumerGroup, 9)
        ]
    );
    assert_eq!(partition.dead_group_offset_keys(|_| true), []);
}

/// Kafka never lets a committed offset block retention, and neither does its gateway.
#[compio::test]
async fn given_an_external_group_offset_when_finding_the_retention_barrier_should_ignore_it() {
    let (mut partition, _) = recording_partition();
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::ExternalGroup, 5, 0, AckLevel::NoAck),
            None,
        )
        .await;
    assert_eq!(partition.external_group_offset(5), Some(0));
    assert_eq!(partition.min_committed_offset(), None);

    install_test_owner(&mut partition, 6, 42).await;
    let request = store_offset_request(42, 2, ConsumerKind::ConsumerGroup, 6, 0, AckLevel::NoAck)
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            header.owner_generation = partition.consumer_group_owner(6).unwrap().generation;
            *next = header;
        });
    partition.on_request(request, None).await;
    assert_eq!(
        partition.min_committed_offset(),
        Some((0, ConsumerKind::ConsumerGroup, 6))
    );
}

#[compio::test]
async fn given_offsets_past_the_partition_end_when_recovered_should_clamp_only_iggy_kinds() {
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let group_file = format!("{}/8", config.get_consumer_group_offsets_path(1, 1, 0));
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    for path in [&group_file, &external_file] {
        persist_offset(path, 1_000, true).await.unwrap();
    }

    let partition = recover_offsets_at(&config, Some(10)).await.unwrap();

    assert_eq!(partition.external_group_offset(9), Some(1_000));
    assert_eq!(persisted_offset(&external_file), Some(1_000));
    assert_eq!(group_offset(&partition, 8), Some(10));
    assert_eq!(persisted_offset(&group_file), Some(10));
}

/// A stored offset says that message was consumed, so none fits a partition that holds
/// nothing. A kept 0 makes `Next` start at 1 and skip the first message of the new history.
#[compio::test]
async fn given_stored_offsets_when_recovered_into_partition_that_holds_nothing_should_remove_them()
{
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let consumer_file = format!("{}/7", config.get_consumer_offsets_path(1, 1, 0));
    let group_file = format!("{}/8", config.get_consumer_group_offsets_path(1, 1, 0));
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    persist_offset(&consumer_file, 7, true).await.unwrap();
    persist_offset(&group_file, 0, true).await.unwrap();
    persist_offset(&external_file, 7, true).await.unwrap();

    let partition = recover_offsets_at(&config, None).await.unwrap();

    assert_eq!(consumer_offset(&partition, 7), None);
    assert_eq!(group_offset(&partition, 8), None);
    for kind in [ConsumerKind::Consumer, ConsumerKind::ConsumerGroup] {
        assert_eq!(partition.durable_consumer_offset_count(kind), 0, "{kind}");
    }
    assert_eq!(persisted_offset(&consumer_file), None);
    assert_eq!(persisted_offset(&group_file), None);
    assert_eq!(partition.external_group_offset(9), Some(7));
    assert_eq!(persisted_offset(&external_file), Some(7));
}

/// An auto-commit writes no file while its offset is at or below the value the file is known
/// to hold. A clamp that leaves the old value in the file makes every later boot read it again.
#[compio::test]
async fn given_clamped_offset_when_auto_committed_and_recovered_again_should_keep_the_committed_value()
 {
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let consumer_file = format!("{}/7", config.get_consumer_offsets_path(1, 1, 0));
    persist_offset(&consumer_file, 1_000, true).await.unwrap();
    let partition = recover_offsets_at(&config, Some(10)).await.unwrap();
    assert_eq!(consumer_offset(&partition, 7), Some(10));

    // The partition takes messages 11 through 20, and a poll commits 15.
    partition.offset.store(20, Ordering::Release);
    partition.dirty_offset.store(20, Ordering::Relaxed);
    let commit = PendingConsumerOffsetCommit::upsert_auto_commit(ConsumerKind::Consumer, 7, 15);
    partition
        .persist_consumer_offset_commit(commit)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(commit);
    assert_eq!(consumer_offset(&partition, 7), Some(15));
    drop(partition);

    let restarted = recover_offsets_at(&config, Some(2_000)).await.unwrap();

    assert_eq!(consumer_offset(&restarted, 7), Some(15));
    assert_eq!(persisted_offset(&consumer_file), Some(15));
}

/// A partition that serves after a clamp its file did not take reads the old value on the
/// next boot, so the recovery has to fail.
#[compio::test]
async fn given_offset_file_that_cannot_be_rewritten_when_recovered_past_the_end_should_fail() {
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let consumer_file = format!("{}/7", config.get_consumer_offsets_path(1, 1, 0));
    persist_offset(&consumer_file, 1_000, true).await.unwrap();
    // A directory where the replacement belongs fails the rewrite.
    std::fs::create_dir(format!(
        "{consumer_file}{}",
        crate::offset_storage::OFFSET_REPLACEMENT_SUFFIX
    ))
    .unwrap();

    let Err(error) = recover_offsets_at(&config, Some(10)).await else {
        panic!("a clamp that cannot rewrite its file must fail the recovery");
    };

    assert!(
        matches!(
            &error,
            crate::PartitionRecoveryError::ConsumerOffsetClamp { path, .. }
                if *path == consumer_file
        ),
        "{error:?}"
    );
    assert_eq!(persisted_offset(&consumer_file), Some(1_000));
}

/// The clamp promises that each changed file and its directory are durable on return, so it
/// fails for a changed file whose kind has no directory to sync.
#[compio::test]
async fn given_a_clamped_offset_of_a_kind_without_a_directory_when_clamping_should_fail() {
    let dir = tempfile::tempdir().unwrap();
    let consumer_file = dir.path().join("7").to_string_lossy().into_owned();
    persist_offset(&consumer_file, 1_000, true).await.unwrap();
    let (partition, _) = recording_partition();
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, 1_000, consumer_file.clone()),
    );

    let Err(error) =
        crate::partition_storage::clamp_consumer_offsets(&partition, IggyNamespace::new(1, 1, 0))
            .await
    else {
        panic!("a clamp that cannot sync the directory of a changed file must fail");
    };

    assert!(
        matches!(
            &error,
            crate::PartitionRecoveryError::ConsumerOffsetClamp { path, .. }
                if *path == consumer_file
        ),
        "{error:?}"
    );
}

/// Recover the offsets under `config` into a new partition whose committed messages end at
/// `last_offset`, or which holds nothing.
async fn recover_offsets_at(
    config: &PartitionsConfig,
    last_offset: Option<u64>,
) -> Result<IggyPartition<RecordingBus>, crate::PartitionRecoveryError> {
    let (mut partition, _) = recording_partition();
    if let Some(last_offset) = last_offset {
        partition.set_offset_space_used(true);
        partition.offset.store(last_offset, Ordering::Release);
        partition.dirty_offset.store(last_offset, Ordering::Relaxed);
    }
    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        config,
        IggyNamespace::new(1, 1, 0),
    )
    .await?;
    Ok(partition)
}

fn consumer_offset(partition: &IggyPartition<RecordingBus>, consumer_id: usize) -> Option<u64> {
    partition
        .consumer_offsets
        .pin()
        .get(&consumer_id)
        .map(|offset| offset.offset.load(Ordering::Relaxed))
}

fn group_offset(partition: &IggyPartition<RecordingBus>, group_id: usize) -> Option<u64> {
    partition
        .consumer_group_offsets
        .pin()
        .get(&ConsumerGroupId(group_id))
        .map(|offset| offset.offset.load(Ordering::Relaxed))
}

/// The offset a consumer offset file holds, `None` when the file is absent.
fn persisted_offset(path: &str) -> Option<u64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => panic!("cannot read {path}: {error}"),
    };
    match decode_offset_record(&bytes) {
        OffsetRecord::Value { offset, .. } => Some(offset),
        record => panic!("{path} holds {record:?}"),
    }
}

/// External group offsets can be stored and deleted on a partition that never took a
/// message. Its commit floor then moves with no bytes behind it, and the offer carries
/// exactly that: no segment, frontier 0 and no offset.
#[compio::test]
async fn given_external_offset_stored_then_deleted_on_empty_partition_when_offer_requested_should_serve_it()
 {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    origin.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 5, 1 << 40, false);
    origin.apply_staged_consumer_offset_commit(1).await.unwrap();
    origin.stage_consumer_offset_delete(2, ConsumerKind::ExternalGroup, 5);
    origin.apply_staged_consumer_offset_commit(2).await.unwrap();
    origin.consensus().sequencer().set_sequence(2);
    origin.consensus().advance_commit_max(2);
    for op in 1..=2 {
        origin.consensus().advance_commit_min(op);
    }
    assert_eq!(origin.external_group_offset(5), None);

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();

    assert_eq!(offer.commit_op, 2);
    assert!(offer.segments.is_empty());
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.next_offset, 0);
    assert_eq!(offsets.external_groups, []);
}

/// The sender snapshot of a real offer carries an external group offset, and the receiver
/// installs it unclamped although it sits far past the origin's only message.
#[compio::test]
async fn given_external_offset_on_origin_when_receiver_installs_its_offer_should_hold_it_unclamped()
{
    let origin_directory = tempfile::tempdir().unwrap();
    let receiver_directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
    // One message whose segment retention removed: the counter backs the empty chain.
    origin.set_offset_space_used(true);
    origin.offset.store(0, Ordering::Release);
    origin.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 5, 1 << 40, false);
    origin.apply_staged_consumer_offset_commit(1).await.unwrap();
    origin.consensus().sequencer().set_sequence(1);
    origin.consensus().advance_commit_max(1);
    origin.consensus().advance_commit_min(1);
    origin
        .dedup_mut()
        .commit_capacity(consensus::CLIENTS_TABLE_MAX)
        .unwrap();
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(receiver_directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, receiver_directory.path());

    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_eq!(receiver.external_group_offset(5), Some(1 << 40));
    assert!(
        receiver_directory
            .path()
            .join("offsets/external_groups/5")
            .exists()
    );
}

/// A restarted replica that only holds an external group offset reads 0 as
/// its next offset, so the message rewind check cannot protect it. An offer
/// that holds nothing, such as one from a primary that lost its directory,
/// must not unlink that offset.
#[compio::test]
async fn given_receiver_with_only_external_offsets_when_offered_nothing_should_refuse() {
    let directory = tempfile::tempdir().unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, directory.path());
    receiver.stage_consumer_offset_upsert(1, ConsumerKind::ExternalGroup, 7, 1 << 40, false);
    receiver
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    let offset_file = directory.path().join("offsets/external_groups/7");
    assert!(offset_file.exists());

    let nothing = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: receiver.dedup().capacity(),
        ..Default::default()
    };
    let refused = receiver
        .install_state_transfer(&repair_config(), 5, Vec::new(), &nothing.encode())
        .await;

    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                    local_offsets: 1
                }
            )
        ),
        "expected the empty offer to be refused, got {refused:?}"
    );
    assert_eq!(receiver.external_group_offset(7), Some(1 << 40));
    assert!(offset_file.exists());
}

/// A replica holding an offset whose delete no peer retains catches up by
/// transfer. The donor's empty offer names the delete that emptied its table,
/// and the receiver keeps that op to serve it in turn.
#[compio::test]
async fn given_counter_zero_replica_that_missed_an_evicted_offset_delete_when_transferring_should_install_and_drop_the_deleted_offset()
 {
    let donor_directory = tempfile::tempdir().unwrap();
    let (mut donor, _) = recording_partition_at(0, 3);
    donor.set_partition_dir(donor_directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut donor, donor_directory.path());
    donor
        .on_request(
            store_offset_request(
                MESSAGE_LESS_CLIENT_ID,
                1,
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                MESSAGE_LESS_EXTERNAL_OFFSET,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    donor.consensus().advance_commit_max(1);
    donor.commit_journal(&repair_config()).await;
    let (receiver_directory, mut receiver) = transfer_receiver();
    receiver.on_replicate(retained_prepare(&donor, 1)).await;
    receiver.consensus().advance_commit_max(1);
    receiver.commit_journal(&repair_config()).await;
    let offset_file = receiver_directory.path().join(format!(
        "offsets/external_groups/{MESSAGE_LESS_EXTERNAL_GROUP_ID}"
    ));
    assert!(offset_file.exists());
    donor
        .on_request(
            delete_offset_request_with_ack(
                MESSAGE_LESS_CLIENT_ID,
                2,
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    donor.consensus().advance_commit_max(2);
    donor.commit_journal(&repair_config()).await;
    assert_eq!(
        donor.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );

    // No peer retains the delete any more, so repair cannot reach it.
    receiver.consensus().advance_commit_max(2);
    receiver.repair = Some(armed_session(2, 2));
    assert_eq!(
        receiver.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused { floor: 2, to_op: 2 }
    );
    let offer = donor.state_transfer_offer(&repair_config()).await.unwrap();
    let installed = receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_eq!(installed.applied_commit_op, 2);
    assert_eq!(receiver.consensus().commit_min(), 2);
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );
    assert!(!offset_file.exists());
    assert_eq!(emptied_marker_op(receiver_directory.path()), Some(2));
    assert_eq!(receiver.durable_consumer_offsets.emptied_at(), 2);
}
