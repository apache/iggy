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

fn unique_temp_offset_dir() -> String {
    let mut dir = std::env::temp_dir();
    dir.push(format!(
        "iggy-offset-test-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_nanos(),
    ));
    dir.to_string_lossy().into_owned()
}

/// A server auto-commit persists monotonically. Disk-tier polls replicate
/// their offsets in IO-completion order, so the last committed op can carry
/// a lower offset than an earlier one; the file must keep the max or a
/// restart reloads the rewound value and re-delivers. An explicit client
/// store still overwrites, so a deliberate offset reset holds.
#[compio::test]
async fn auto_commit_offset_persists_monotonically_explicit_store_rewinds() {
    let mut partition = test_partition();
    let dir = unique_temp_offset_dir();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(dir.clone());
    let consumer_id: u32 = 5;
    let path = format!("{dir}/{consumer_id}");
    let read_disk = |p: &str| -> u64 {
        let bytes = std::fs::read(p).expect("offset file exists");
        match crate::offset_storage::decode_offset_record(&bytes) {
            crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
            other => panic!("offset file must hold a readable value, got {other:?}"),
        }
    };

    // Reordered auto-commits: the later op (109) trails the earlier (114).
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            114,
        ))
        .await
        .expect("auto-commit persist 114");
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("auto-commit persist 109");
    assert_eq!(
        read_disk(&path),
        114,
        "auto-commit must not rewind the file on IO-completion reorder"
    );

    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "committed high-water covers the persisted offset"
    );
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 115),
        "an advancing offset is not covered and must submit"
    );

    // An explicit client store may deliberately rewind.
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("explicit store persist 109");
    assert_eq!(read_disk(&path), 109, "explicit store may rewind the file");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "explicit rewind lowers the high-water so a later auto-commit may re-advance"
    );

    // The accepted edge: an auto-commit racing the explicit rewind
    // re-advances the file past it.
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            114,
        ))
        .await
        .expect("auto-commit persist 114 after rewind");
    assert_eq!(
        read_disk(&path),
        114,
        "auto-commit re-advances past a rewind"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Promotion from the request queue must keep an explicit store's
/// overwrite, or a client could never move its cursor back.
#[compio::test]
async fn given_queued_explicit_store_below_committed_offset_when_promoted_should_rewind_it() {
    const CLIENT: u128 = 42;
    const FILLER_CLIENT: u128 = 43;
    const CONSUMER: u32 = 7;
    const FILLER_CONSUMER: u32 = 8;
    for (kind, consumer) in [
        (
            ConsumerKind::Consumer,
            PollingConsumer::Consumer(CONSUMER as usize, 0),
        ),
        (
            ConsumerKind::ConsumerGroup,
            PollingConsumer::ConsumerGroup(CONSUMER as usize, 0),
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) =
            recording_partition_with_pipeline(0, 1, LocalPipeline::with_capacities(1, 1));
        set_offset_dirs_under(&mut partition, directory.path());
        partition.stats.increment_messages_count(2);
        partition.stats.set_current_offset(1);
        if kind == ConsumerKind::ConsumerGroup {
            for (group_id, client_id) in [(CONSUMER, CLIENT), (FILLER_CONSUMER, FILLER_CLIENT)] {
                let installation =
                    next_test_installation(&partition, u64::from(group_id), client_id);
                partition
                    .on_request(owner_install_request(&partition, &installation), None)
                    .await;
                commit_recorded_loopback(&mut partition).await;
                assert!(
                    partition
                        .installed_consumer_group_owner(&installation)
                        .is_some()
                );
            }
        }
        let owned = |partition: &IggyPartition<_>, request: Message<RoutedRequestHeader>| {
            request.transmute_header(|mut header, next: &mut RoutedRequestHeader| {
                let consumer_id = if header.client == CLIENT {
                    CONSUMER
                } else {
                    FILLER_CONSUMER
                };
                header.metadata_watermark = partition.required_metadata_frontier;
                header.owner_generation = partition
                    .consumer_group_owner(u64::from(consumer_id))
                    .map_or(0, |owner| owner.generation);
                *next = header;
            })
        };
        let applied = partition.consensus().commit_min();
        let request = owned(
            &partition,
            store_offset_request(CLIENT, 1, kind, CONSUMER, 1, AckLevel::Quorum),
        );
        partition.on_request(request, None).await;
        commit_recorded_loopback(&mut partition).await;
        assert_eq!(partition.get_consumer_offset(consumer), Some(1));

        // The filler holds the only prepare slot, so the rewind has to queue.
        let filler = owned(
            &partition,
            store_offset_request(FILLER_CLIENT, 1, kind, FILLER_CONSUMER, 0, AckLevel::Quorum),
        );
        partition.on_request(filler, None).await;
        let rewind = owned(
            &partition,
            store_offset_request(CLIENT, 2, kind, CONSUMER, 0, AckLevel::Quorum),
        );
        partition.on_request(rewind, None).await;
        assert_eq!(partition.consensus().request_queue_len(), 1);
        commit_recorded_loopback(&mut partition).await;
        commit_recorded_loopback(&mut partition).await;

        assert_eq!(partition.consensus().commit_min(), applied + 3, "{kind:?}");
        assert_eq!(
            partition.get_consumer_offset(consumer),
            Some(0),
            "a promoted {kind:?} store must overwrite, not fold to the max"
        );
    }
}
#[compio::test]
async fn wal_backed_cold_offsets_skip_covered_values_and_write_advances() {
    let (directory, partition, _) = partition_with_pending_durable_ack().await;
    let path = directory.path().join("offset");
    let path = path.to_str().unwrap();
    persist_offset(path, 114, false).await.unwrap();
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 109, true)
            .await
            .unwrap(),
        (114, false)
    );
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 114, true)
            .await
            .unwrap(),
        (114, false)
    );
    assert!(
        partition
            .persistence
            .as_ref()
            .unwrap()
            .take_offset_file(path)
            .is_none()
    );
    assert_eq!(
        partition
            .write_cold_consumer_offset(path, 115, true)
            .await
            .unwrap(),
        (115, true)
    );
    assert_eq!(
        crate::offset_storage::read_offset_max(path, 0)
            .await
            .unwrap()
            .offset,
        115
    );
}

/// The persisted-offset tracker is cold after a restart; the first
/// auto-commit folds against the file once (so a pre-existing higher value
/// wins, exactly like the old per-commit read-modify-write) and warms the
/// tracker with the on-disk value, not the op's. A delete drops both the
/// file and the tracker entry so a later auto-commit starts a fresh fold.
#[compio::test]
async fn auto_commit_cold_key_folds_against_file_once() {
    let mut partition = test_partition();
    let dir = unique_temp_offset_dir();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] = Some(dir.clone());
    let consumer_id: u32 = 5;
    let path = format!("{dir}/{consumer_id}");
    let read_disk = |p: &str| -> u64 {
        let bytes = std::fs::read(p).expect("offset file exists");
        match crate::offset_storage::decode_offset_record(&bytes) {
            crate::offset_storage::OffsetRecord::Value { offset, .. } => offset,
            other => panic!("offset file must hold a readable value, got {other:?}"),
        }
    };

    // Simulate the previous process run: the file already holds 114.
    persist_offset(&path, 114, false)
        .await
        .expect("seed offset file");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 1),
        "a cold key is never covered; the first submit must go through"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            109,
        ))
        .await
        .expect("auto-commit persist 109 on cold key");
    assert_eq!(
        read_disk(&path),
        114,
        "cold-key fold must not rewind the pre-existing on-disk value"
    );
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 114),
        "tracker warms with the on-disk value, not the trailing op offset"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::delete(
            ConsumerKind::Consumer,
            consumer_id,
        ))
        .await
        .expect("delete persisted offset");
    assert!(!std::path::Path::new(&path).exists(), "file unlinked");
    assert!(
        !partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, consumer_id, 1),
        "delete drops the tracker entry with the file"
    );

    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
            ConsumerKind::Consumer,
            consumer_id,
            7,
        ))
        .await
        .expect("auto-commit persist 7 after delete");
    assert_eq!(read_disk(&path), 7, "post-delete auto-commit starts fresh");

    let _ = std::fs::remove_dir_all(&dir);
}

#[compio::test]
async fn given_dead_group_when_reclaimed_should_keep_capacity_until_replicated_delete() {
    let (mut partition, _) = recording_partition();
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![(ConsumerKind::ConsumerGroup, 7)]
    );
    assert_eq!(partition.consumer_group_offset_ids(), vec![7]);
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    partition.stage_consumer_offset_delete(1, ConsumerKind::ConsumerGroup, 7);
    partition
        .apply_staged_consumer_offset_commit(1)
        .await
        .expect("delete commits");
    assert_eq!(
        partition.occupied_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
    assert_eq!(partition.consumer_group_offset_ids(), [] as [u64; 0]);
}

#[compio::test]
async fn given_known_stranded_group_file_when_reconciling_should_not_submit_delete_loop() {
    let dir = tempfile::tempdir().unwrap();
    let group_dir = dir.path().join("groups");
    std::fs::create_dir_all(group_dir.join("7")).unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(group_dir.to_string_lossy().into_owned());
    // In the live map, so the reclaim walk actually meets the key and the
    // stranded filter is what keeps it out of the delete log.
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .record_stranded(7);
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .is_stranded(7)
    );
    assert_eq!(
        partition.dead_group_offset_keys(|_| true),
        Vec::<(ConsumerKind, u32)>::new()
    );
    assert!(
        partition.dead_group_offset_keys(|_| false).is_empty(),
        "automatic cleanup must not resubmit a known failed unlink"
    );
    partition
        .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
        .clear_stranded(7);
    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![(ConsumerKind::ConsumerGroup, 7)],
        "the same dead key is reclaimed once it is no longer stranded"
    );
    assert_eq!(partition.offsets_wire_snapshot_for_test().unwrap(), []);
}

#[test]
fn given_local_only_key_when_replicated_should_require_durable_membership_to_delete() {
    let (partition, _) = recording_partition_at(0, 3);
    partition.seed_stranded_consumer_offset(ConsumerKind::ConsumerGroup, 7);
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 7)
            .is_err(),
        "a stranded file is not a replicated key"
    );
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(8),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 8, 0, String::new()),
    );
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 8)
            .is_err(),
        "a local poll cursor must not produce a missing-key delete on an older peer"
    );
    partition
        .durable_consumer_offsets
        .record_explicit(ConsumerKind::ConsumerGroup, 9, 0, 0);
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 9)
            .is_ok()
    );
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 10)
            .is_err(),
        "an unknown key still answers not found"
    );

    let (single, _) = recording_partition();
    single.seed_stranded_consumer_offset(ConsumerKind::ConsumerGroup, 7);
    assert!(
        single
            .ensure_consumer_offset_exists(ConsumerKind::ConsumerGroup, 7)
            .is_ok()
    );
}

#[compio::test]
async fn given_committed_delete_when_unlink_fails_should_preserve_state_and_report_failure() {
    let dir = tempfile::tempdir().unwrap();
    let group_dir = dir.path().join("groups");
    std::fs::create_dir_all(group_dir.join("7")).unwrap();
    let (mut partition, _) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(group_dir.to_string_lossy().into_owned());
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    partition.stage_consumer_offset_delete(1, ConsumerKind::ConsumerGroup, 7);

    // A failed file mutation cannot become a successful logical delete.
    assert!(
        partition
            .apply_staged_consumer_offset_commit(1)
            .await
            .is_err()
    );
    assert_eq!(partition.consumer_group_offset_ids(), vec![7]);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
    assert!(partition.pending_consumer_offset_commits.contains_key(&1));
    assert!(
        partition
            .consumer_offset_capacity_for(ConsumerKind::ConsumerGroup)
            .is_stranded(7)
    );
    assert!(partition.fatal.is_none());
}

#[compio::test]
async fn given_quorum_store_when_unrelated_dirty_directory_is_gone_should_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] = Some(
        dir.path()
            .join("missing-groups")
            .to_string_lossy()
            .into_owned(),
    );
    partition.consumer_offset_dirs_dirty[1].set(true);
    partition.stats.increment_messages_count(1);

    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::Quorum),
            None,
        )
        .await;
    commit_recorded_loopback(&mut partition).await;

    assert!(partition.consumer_offsets.pin().contains_key(&7));
    assert!(
        partition
            .durable_consumer_offsets
            .covers(ConsumerKind::Consumer, 7, 0)
    );
    let reply = sent.borrow();
    let header = reply
        .first()
        .and_then(|(_, message)| {
            bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &message.as_slice()[..size_of::<ReplyHeader>()],
            )
            .ok()
        })
        .expect("success reply");
    assert_eq!(header.status, 0);
}

#[compio::test]
async fn given_no_ack_explicit_store_when_submitted_should_apply_without_replication() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    partition.open_persistence().await.unwrap();
    partition.stats.increment_messages_count(1);
    partition
        .on_request(
            store_offset_request(42, 1, ConsumerKind::Consumer, 7, 0, AckLevel::NoAck),
            None,
        )
        .await;
    assert_eq!(
        partition.get_consumer_offset(PollingConsumer::Consumer(7, 0)),
        Some(0)
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        1
    );
    assert_eq!(partition.consensus().pipeline_len(), 0);
    assert_eq!(partition.persistence.as_ref().unwrap().head(), 0);
    let sent = sent.borrow();
    let header = bytemuck::checked::from_bytes::<ReplyHeader>(
        &sent[0].1.as_slice()[..size_of::<ReplyHeader>()],
    );
    assert_eq!(header.status, 0);
    assert_eq!(header.op, 0);
}

#[compio::test]
async fn given_stale_dirt_on_the_other_kind_when_its_sync_fails_should_not_fence_the_walk() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition_at(0, 3);
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(dir.path().join("groups").to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    // Dirt a NoAck request left on the groups directory, whose sync now
    // fails. This walk writes consumer offsets only.
    partition.consumer_offset_dirs_dirty[1].set(true);
    partition.consumer_offset_dir_sync_fault.set(Some(1));
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            1,
            0,
        ))
        .await
        .unwrap();
    partition.stage_consumer_offset_delete(1, ConsumerKind::Consumer, 1);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 1,
        operation: Operation::DeleteConsumerOffset,
        client: 42,
        request: 1,
        ..Default::default()
    };
    partition.consensus.restore_commit_state(0, 1);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(
        partition.fatal.is_none(),
        "a failure on a kind this walk did not write must not fence it"
    );
    assert_eq!(partition.consensus.commit_min(), 1);
    assert_eq!(sent.borrow().len(), 1);
    assert!(!partition.consumer_offset_dirs_dirty[0].get());
    assert!(partition.consumer_offset_dirs_dirty[1].get());

    // The same failure on the kind the walk wrote fences it.
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition
        .persist_consumer_offset_commit(PendingConsumerOffsetCommit::upsert(
            ConsumerKind::Consumer,
            2,
            0,
        ))
        .await
        .unwrap();
    partition.stage_consumer_offset_delete(2, ConsumerKind::Consumer, 2);
    let header = PrepareHeader {
        retry_capacity: u32::try_from(consensus::PARTITION_DEDUP_CLIENTS_MAX).unwrap(),
        session: 1,
        op: 2,
        operation: Operation::DeleteConsumerOffset,
        client: 42,
        request: 2,
        ..Default::default()
    };
    partition.consensus.advance_commit_max(2);
    partition
        .handle_committed_entries(vec![header], &repair_config(), true)
        .await;
    assert!(
        partition.fatal.is_some(),
        "a sync failure on a written kind fences the walk"
    );
    assert_eq!(partition.consensus.commit_min(), 1);
}

#[compio::test]
async fn given_no_ack_delete_sync_failure_when_retried_should_retry_barrier_and_succeed() {
    let dir = tempfile::tempdir().unwrap();
    let (mut partition, sent) = recording_partition();
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(dir.path().to_string_lossy().into_owned());
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    let stored = PendingConsumerOffsetCommit::upsert(ConsumerKind::Consumer, 7, 0);
    partition
        .persist_consumer_offset_commit(stored)
        .await
        .unwrap();
    partition.apply_consumer_offset_commit(stored);
    partition.consumer_offset_dir_sync_fault.set(Some(0));
    partition
        .apply_consumer_offset_no_ack(
            Box::new(*delete_offset_request(42, 1, 7).header()),
            ConsumerKind::Consumer,
            7,
            None,
            None,
        )
        .await;
    let status = |index: usize| {
        let frames = sent.borrow();
        let bytes = frames[index].1.as_slice();
        let start = std::mem::offset_of!(ReplyHeader, status);
        u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap())
    };
    assert_eq!(status(0), IggyError::CannotSyncFile.as_code());
    assert!(!dir.path().join("7").exists());
    assert!(
        partition
            .ensure_consumer_offset_exists(ConsumerKind::Consumer, 7)
            .is_ok()
    );
    partition.consumer_offset_dir_sync_fault.set(None);
    partition
        .apply_consumer_offset_no_ack(
            Box::new(*delete_offset_request(42, 2, 7).header()),
            ConsumerKind::Consumer,
            7,
            None,
            None,
        )
        .await;
    assert_eq!(status(1), 0);
    assert!(!partition.consumer_offset_dirs_dirty[0].get());
    assert!(
        !partition
            .consumer_offset_capacity_for(ConsumerKind::Consumer)
            .is_stranded(7)
    );
}

/// Both the commit walk and a no-ack store apply through
/// `apply_consumer_offset_commit`, so its stamps hold for both.
#[test]
fn given_client_stores_and_auto_commits_when_applied_should_stamp_only_client_stores() {
    let (partition, _) = recording_partition();
    let consumer = PollingConsumer::Consumer(7, 0);
    let before = partition.consensus.message_bus().monotonic_micros();
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::upsert_auto_commit(
        ConsumerKind::Consumer,
        7,
        3,
    ));
    assert!(
        !partition.consumer_offset_stored_since(consumer, before),
        "the commit of a read is no client store"
    );
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::explicit(
        ConsumerKind::Consumer,
        7,
        Some(3),
    ));
    assert_eq!(partition.get_consumer_offset(consumer), Some(3));
    assert!(
        partition.consumer_offset_stored_since(consumer, before),
        "a client store of the value it held stamps the offset"
    );
    assert!(
        !partition.consumer_offset_stored_since(PollingConsumer::ConsumerGroup(7, 0), before),
        "the stamp names its kind"
    );
    let other = PollingConsumer::Consumer(8, 0);
    assert!(!partition.consumer_offset_stored_since(other, before));
    partition.apply_consumer_offset_commit(PendingConsumerOffsetCommit::explicit(
        ConsumerKind::Consumer,
        7,
        None,
    ));
    assert_eq!(partition.get_consumer_offset(consumer), None);
    assert!(
        partition.consumer_offset_stored_since(other, before),
        "an offset without a stamp of its own counts the last delete"
    );
}

/// The primary stages a client store when it admits it and stamps it when
/// it commits. A parked read that finishes in between would commit at a
/// higher op than the store, so its monotone apply would pass over the
/// rewind.
#[test]
fn given_a_staged_client_store_when_checking_since_admission_should_report_it_before_it_applies() {
    let (mut partition, _) = recording_partition();
    let consumer = PollingConsumer::Consumer(7, 0);
    let other = PollingConsumer::Consumer(8, 0);
    let before = partition.consensus.message_bus().monotonic_micros();
    partition.stage_consumer_offset_upsert(1, ConsumerKind::Consumer, 7, 3, true);
    assert!(
        !partition.consumer_offset_stored_since(consumer, before),
        "a staged commit of a read is no client store"
    );
    partition.stage_consumer_offset_upsert(2, ConsumerKind::Consumer, 7, 1, false);
    assert_eq!(
        partition.get_consumer_offset(consumer),
        None,
        "the staged store is not applied yet"
    );
    assert!(
        partition.consumer_offset_stored_since(consumer, before),
        "a staged client store fences a read that finishes before it applies"
    );
    assert!(
        !partition.consumer_offset_stored_since(other, before),
        "the staged store names its consumer"
    );
    partition.stage_consumer_offset_delete(3, ConsumerKind::Consumer, 8);
    assert!(
        partition.consumer_offset_stored_since(other, before),
        "a staged delete fences as a staged store does"
    );
}

#[test]
fn given_follower_when_group_is_deleted_should_wait_for_ordered_reclamation() {
    let (partition, _) = recording_partition_at(1, 3);
    partition.consumer_group_offsets.pin().insert(
        ConsumerGroupId(7),
        ConsumerOffset::new(ConsumerKind::ConsumerGroup, 7, 11, String::new()),
    );
    partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, 7, 11, 11);
    assert_eq!(partition.dead_group_offset_keys(|_| false), []);
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        1
    );
}
