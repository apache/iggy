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
async fn given_empty_transfer_frontier_when_incoming_offsets_are_clamped_out_should_remove_old_files()
 {
    let dir = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    let consumers = dir.path().join("offsets/consumers");
    let groups = dir.path().join("offsets/groups");
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(consumers.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(groups.to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        dir.path()
            .join("offsets/external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    for path in [consumers.join("7"), groups.join("8")] {
        persist_offset(path.to_str().unwrap(), 5, true)
            .await
            .unwrap();
    }
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        emptied_at: 0,
        consumers: vec![(7, 5)],
        groups: vec![(8, 5)],
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    partition
        .install_state_transfer(&repair_config(), 1, Vec::new(), &wire.encode())
        .await
        .unwrap();
    assert!(!consumers.join("7").exists());
    assert!(!groups.join("8").exists());
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::Consumer),
        0
    );
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ConsumerGroup),
        0
    );
}

/// A Kafka commit can name an offset past the transferred frontier, or any offset on an empty
/// partition. The install must not clamp or drop it.
#[compio::test]
async fn given_empty_transfer_frontier_when_external_offsets_arrive_should_keep_them_as_sent() {
    let dir = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(dir.path().to_string_lossy().into_owned());
    let offsets = dir.path().join("offsets");
    partition.consumer_offset_dirs[ConsumerKind::Consumer.index()] =
        Some(offsets.join("consumers").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ConsumerGroup.index()] =
        Some(offsets.join("groups").to_string_lossy().into_owned());
    partition.consumer_offset_dirs[ConsumerKind::ExternalGroup.index()] = Some(
        offsets
            .join("external_groups")
            .to_string_lossy()
            .into_owned(),
    );
    let wire = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: partition.dedup().capacity(),
        external_groups: vec![(8, 0), (9, 1 << 40)],
        ..Default::default()
    };

    partition
        .install_state_transfer(&repair_config(), 1, Vec::new(), &wire.encode())
        .await
        .unwrap();

    assert_eq!(partition.external_group_offset(8), Some(0));
    assert_eq!(partition.external_group_offset(9), Some(1 << 40));
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ExternalGroup),
        2
    );
    assert!(offsets.join("external_groups/9").exists());
}

#[compio::test]
async fn given_empty_partition_when_storing_an_external_group_offset_should_skip_the_range_check() {
    let (mut partition, sent_to_clients) = recording_partition();
    for (request, offset) in [(1, 0), (2, 1_000)] {
        partition
            .on_request(
                store_offset_request(
                    42,
                    request,
                    ConsumerKind::ExternalGroup,
                    5,
                    offset,
                    AckLevel::NoAck,
                ),
                None,
            )
            .await;
        assert_eq!(partition.external_group_offset(5), Some(offset));
    }

    install_test_owner(&mut partition, 5, 42).await;
    let request = store_offset_request(42, 3, ConsumerKind::ConsumerGroup, 5, 0, AckLevel::NoAck)
        .transmute_header(|mut header, next: &mut RoutedRequestHeader| {
            header.metadata_watermark = partition.required_metadata_frontier;
            header.owner_generation = partition.consumer_group_owner(5).unwrap().generation;
            *next = header;
        });
    partition.on_request(request, None).await;

    let sent = sent_to_clients.borrow();
    let denied = sent
        .iter()
        .find_map(|(_, frame)| {
            let header = bytemuck::checked::try_from_bytes::<ReplyHeader>(
                &frame.as_slice()[..std::mem::size_of::<ReplyHeader>()],
            )
            .ok()?;
            (header.request == 3).then_some(*header)
        })
        .expect("range denial reply");
    assert_eq!(denied.status, IggyError::InvalidOffset(0).as_code());
    assert_eq!(
        partition.durable_consumer_offset_count(ConsumerKind::ExternalGroup),
        1
    );
    assert!(partition.consumer_group_offsets.pin().is_empty());
}

/// The reconciler deletes a bounded number of dead keys per pass. Ordered
/// by group id, a backlog of dead consumer group keys cannot hold every
/// external group key back by whole passes.
#[compio::test]
async fn given_dead_keys_of_both_kinds_when_listed_should_order_them_by_group_id() {
    let (partition, _) = recording_partition();
    for group_id in [1, 2, 3] {
        partition.seed_recovered_consumer_offset(ConsumerKind::ConsumerGroup, group_id, 11, 11);
        partition.consumer_group_offsets.pin().insert(
            ConsumerGroupId(group_id as usize),
            ConsumerOffset::new(ConsumerKind::ConsumerGroup, group_id, 11, String::new()),
        );
    }
    for group_id in [1, 2] {
        partition.seed_recovered_consumer_offset(ConsumerKind::ExternalGroup, group_id, 11, 11);
    }

    assert_eq!(
        partition.dead_group_offset_keys(|_| false),
        vec![
            (ConsumerKind::ConsumerGroup, 1),
            (ConsumerKind::ExternalGroup, 1),
            (ConsumerKind::ConsumerGroup, 2),
            (ConsumerKind::ExternalGroup, 2),
            (ConsumerKind::ConsumerGroup, 3),
        ]
    );
}

/// A replica whose only offset is one external group's, committed through
/// `commit_min`.
async fn external_offset_receiver(
    root: &std::path::Path,
    commit_min: u64,
) -> IggyPartition<RecordingBus> {
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(root.to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, root);
    receiver.stage_consumer_offset_upsert(
        1,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        false,
    );
    receiver
        .apply_staged_consumer_offset_commit(1)
        .await
        .unwrap();
    receiver.consensus().sequencer().set_sequence(commit_min);
    receiver.consensus().advance_commit_max(commit_min);
    for op in 1..=commit_min {
        receiver.consensus().advance_commit_min(op);
    }
    receiver
}

/// A receiver that already applied the op its donor's table was emptied at
/// cannot have missed that delete, so the empty offer does not explain the
/// offset it holds.
#[compio::test]
async fn given_receiver_at_the_emptied_op_when_offered_nothing_should_refuse() {
    const EMPTIED_AT: u64 = 2;
    const OFFER_COMMIT_OP: u64 = 5;
    let directory = tempfile::tempdir().unwrap();
    let mut receiver = external_offset_receiver(directory.path(), EMPTIED_AT).await;
    let emptied = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: EMPTIED_AT,
        ..Default::default()
    };

    let refused = receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &emptied.encode(),
        )
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
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
}

/// An emptied op past the offer's own op is not one its donor applied, so it
/// proves nothing about the table at the offer.
#[compio::test]
async fn given_emptied_op_past_the_offer_when_offered_nothing_should_refuse() {
    const OFFER_COMMIT_OP: u64 = 5;
    let directory = tempfile::tempdir().unwrap();
    let mut receiver = external_offset_receiver(directory.path(), 1).await;
    let ahead = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: OFFER_COMMIT_OP + 1,
        ..Default::default()
    };

    let refused = receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &ahead.encode(),
        )
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
    assert_eq!(emptied_marker_op(directory.path()), None);
}

/// An installed table that holds offsets keeps no marker, neither the
/// receiver's nor one the offer names, so none can outlive the installed files.
#[compio::test]
async fn given_emptied_receiver_when_installing_offsets_should_remove_its_marker() {
    const OFFER_COMMIT_OP: u64 = 5;
    const OFFER_EMPTIED_AT: u64 = 4;
    let directory = tempfile::tempdir().unwrap();
    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, directory.path());
    let marker = directory.path().join("offsets/emptied");
    persist_offset(&marker.to_string_lossy(), 1, true)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: receiver.dedup().capacity(),
        emptied_at: OFFER_EMPTIED_AT,
        external_groups: vec![(MESSAGE_LESS_EXTERNAL_GROUP_ID, MESSAGE_LESS_EXTERNAL_OFFSET)],
        ..Default::default()
    };

    receiver
        .install_state_transfer(
            &repair_config(),
            OFFER_COMMIT_OP,
            Vec::new(),
            &offsets.encode(),
        )
        .await
        .unwrap();

    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(
        receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
}

async fn commit_external_store_then_delete(primary: &mut IggyPartition<RecordingBus>) {
    let store = store_offset_request(
        MESSAGE_LESS_CLIENT_ID,
        1,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        AckLevel::Quorum,
    );
    let delete = delete_offset_request_with_ack(
        MESSAGE_LESS_CLIENT_ID,
        2,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        AckLevel::Quorum,
    );
    for (op, request) in [(1, store), (2, delete)] {
        primary.on_request(request, None).await;
        primary.consensus().advance_commit_max(op);
        primary.commit_journal(&repair_config()).await;
    }
    assert!(primary.fatal().is_none(), "{:?}", primary.fatal());
    assert_eq!(primary.consensus().commit_min(), 2);
}

const LIVE_EXTERNAL_GROUP_ID: u32 = MESSAGE_LESS_EXTERNAL_GROUP_ID + 1;

/// A primary with offset directories whose only external group offset was
/// stored at op 1 and deleted at op 2.
async fn emptied_external_origin() -> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut origin, directory.path());
    commit_external_store_then_delete(&mut origin).await;
    (directory, origin)
}

/// A store into an emptied table retires the marker before its file lands,
/// so a crash that loses the file cannot leave a marker claiming the table
/// is still empty.
#[compio::test]
async fn given_emptied_marker_when_an_offset_is_stored_again_should_remove_the_marker() {
    let (directory, mut partition) = emptied_external_origin().await;
    assert_eq!(emptied_marker_op(directory.path()), Some(2));

    partition
        .on_request(
            store_offset_request(
                MESSAGE_LESS_CLIENT_ID,
                3,
                ConsumerKind::ExternalGroup,
                MESSAGE_LESS_EXTERNAL_GROUP_ID,
                MESSAGE_LESS_EXTERNAL_OFFSET,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    partition.consensus().advance_commit_max(3);
    partition.commit_journal(&repair_config()).await;

    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(emptied_marker_op(directory.path()), None);
    let offer = partition
        .state_transfer_offer(&repair_config())
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.emptied_at, 0);
}

/// The node-local store skips the commit walk, and it too retires the marker
/// before its file lands.
#[compio::test]
async fn given_no_ack_store_in_flight_when_a_send_is_queued_should_keep_the_emptied_marker_cleared()
{
    let config = repair_config();
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    set_offset_dirs_under(&mut partition, directory.path());
    commit_external_store_then_delete(&mut partition).await;
    assert_eq!(emptied_marker_op(directory.path()), Some(2));
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let store = store_offset_request(
        MESSAGE_LESS_CLIENT_ID,
        3,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        MESSAGE_LESS_EXTERNAL_OFFSET,
        AckLevel::NoAck,
    );

    partition
        .apply_consumer_offset_no_ack(
            Box::new(*store.header()),
            ConsumerKind::ExternalGroup,
            MESSAGE_LESS_EXTERNAL_GROUP_ID,
            Some(MESSAGE_LESS_EXTERNAL_OFFSET),
            None,
        )
        .await;
    assert!(matches!(partition.drive_no_ack(), NoAckStep::Progress));
    let store_job = partition.capture_no_ack_phase().unwrap();

    let send = history_send_request(&partition, 1, partition.required_metadata_frontier);
    partition.on_request(send, None).await;
    partition.consensus().advance_commit_max(3);
    assert_eq!(partition.select_persistable_commits(&config), []);
    partition.accept_no_ack_phase(store_job.execute().await);

    assert_eq!(
        partition.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);

    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
    assert_eq!(partition.consensus().commit_min(), 3);
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(offsets.emptied_at, 0);
}

/// A stranded file may hold an offset the table never loaded, so a delete of
/// the last loaded offset does not mark the table emptied.
#[compio::test]
async fn given_stranded_offset_when_a_delete_empties_the_table_should_not_mark_it_emptied() {
    const STRANDED_GROUP_ID: u32 = MESSAGE_LESS_EXTERNAL_GROUP_ID + 1;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    assert!(
        partition.seed_stranded_consumer_offset(ConsumerKind::ExternalGroup, STRANDED_GROUP_ID)
    );

    commit_external_store_then_delete(&mut partition).await;

    assert_eq!(
        partition.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
}

/// A primary with offset directories that stored the offsets of external
/// groups `MESSAGE_LESS_EXTERNAL_GROUP_ID` at op 1 and
/// `LIVE_EXTERNAL_GROUP_ID` at op 2, then deleted the first at op 3.
async fn external_origin_that_deleted_one_of_two_groups()
-> (tempfile::TempDir, IggyPartition<RecordingBus>) {
    let directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut origin, directory.path());
    let store = |op, group_id| {
        store_offset_request(
            MESSAGE_LESS_CLIENT_ID,
            op,
            ConsumerKind::ExternalGroup,
            group_id,
            MESSAGE_LESS_EXTERNAL_OFFSET,
            AckLevel::Quorum,
        )
    };
    let delete = delete_offset_request_with_ack(
        MESSAGE_LESS_CLIENT_ID,
        3,
        ConsumerKind::ExternalGroup,
        MESSAGE_LESS_EXTERNAL_GROUP_ID,
        AckLevel::Quorum,
    );
    for (op, request) in [
        (1, store(1, MESSAGE_LESS_EXTERNAL_GROUP_ID)),
        (2, store(2, LIVE_EXTERNAL_GROUP_ID)),
        (3, delete),
    ] {
        origin.on_request(request, None).await;
        origin.consensus().advance_commit_max(op);
        origin.commit_journal(&repair_config()).await;
    }
    assert_eq!(origin.consensus().commit_min(), 3);
    (directory, origin)
}

/// A delete that leaves another offset does not empty the table. Only the
/// delete of the last offset marks it, with that delete's op.
#[compio::test]
async fn given_two_offsets_when_both_are_deleted_should_mark_only_the_last_delete() {
    const LAST_DELETE_OP: u64 = 4;
    let (directory, mut origin) = external_origin_that_deleted_one_of_two_groups().await;
    assert_eq!(emptied_marker_op(directory.path()), None);
    assert_eq!(origin.durable_consumer_offsets.emptied_at(), 0);

    origin
        .on_request(
            delete_offset_request_with_ack(
                MESSAGE_LESS_CLIENT_ID,
                LAST_DELETE_OP,
                ConsumerKind::ExternalGroup,
                LIVE_EXTERNAL_GROUP_ID,
                AckLevel::Quorum,
            ),
            None,
        )
        .await;
    origin.consensus().advance_commit_max(LAST_DELETE_OP);
    origin.commit_journal(&repair_config()).await;

    assert_eq!(origin.consensus().commit_min(), LAST_DELETE_OP);
    assert_eq!(emptied_marker_op(directory.path()), Some(LAST_DELETE_OP));
    assert_eq!(origin.durable_consumer_offsets.emptied_at(), LAST_DELETE_OP);
}

/// A donor that restarts after emptying its table still proves it: boot reads
/// the marker back, and the offer at the checkpointed op names the delete.
#[compio::test]
async fn given_table_emptied_by_a_delete_when_the_donor_restarts_should_offer_the_emptied_op() {
    const EMPTIED_AT: u64 = 2;
    let (_origin_directory, origin) = emptied_external_origin().await;
    let donor_directory = tempfile::tempdir().unwrap();
    let (config, mut donor) = Box::pin(restarted_replica_of(
        &origin,
        donor_directory.path(),
        1,
        EMPTIED_AT,
    ))
    .await;
    // A view change makes the restarted replica the primary.
    donor.consensus.set_view(1);

    let offer = donor.state_transfer_offer(&config).await.unwrap();

    let offsets = crate::state_transfer::ConsumerOffsetsWire::decode(&offer.offsets.1).unwrap();
    assert_eq!(
        (offer.commit_op, offsets.emptied_at),
        (EMPTIED_AT, EMPTIED_AT)
    );
    assert_eq!(offsets.external_groups, []);
}

/// A donor whose boot lost its offset files holds an empty table, so its
/// delete of a key that table lacks removes nothing. Its empty offer must
/// not name that delete, or a receiver behind it erases an offset that the
/// origin still holds.
#[compio::test]
async fn given_donor_that_lost_its_offset_files_when_it_deletes_an_absent_offset_should_not_erase_the_receiver_offsets()
 {
    const CHECKPOINT_OP: u64 = 2;
    const DELETE_OP: u64 = 3;
    let (_origin_directory, origin) = external_origin_that_deleted_one_of_two_groups().await;

    for tear_files in [true, false] {
        let donor_directory = tempfile::tempdir().unwrap();
        let (config, donor) = Box::pin(restarted_replica_of(
            &origin,
            donor_directory.path(),
            1,
            CHECKPOINT_OP,
        ))
        .await;
        drop(donor);
        let offsets_dir = std::path::PathBuf::from(config.get_offsets_path(1, 1, 0));
        if tear_files {
            for group_id in [MESSAGE_LESS_EXTERNAL_GROUP_ID, LIVE_EXTERNAL_GROUP_ID] {
                let file = offsets_dir.join(format!("external_groups/{group_id}"));
                std::fs::write(file, []).unwrap();
            }
        } else {
            std::fs::remove_dir_all(&offsets_dir).unwrap();
        }
        let (config, mut donor) = replica_with_prepare_history(donor_directory.path(), 1).await;
        assert_eq!(donor.external_group_offset(LIVE_EXTERNAL_GROUP_ID), None);
        commit_replicated_through(&mut donor, &origin, &config, DELETE_OP).await;
        // A view change makes the restarted replica the primary.
        donor.consensus.set_view(1);
        let offer = donor.state_transfer_offer(&config).await.unwrap();
        let (receiver_directory, mut receiver) = transfer_receiver();
        for op in 1..=CHECKPOINT_OP {
            receiver.on_replicate(retained_prepare(&origin, op)).await;
        }
        receiver.consensus().advance_commit_max(CHECKPOINT_OP);
        receiver.commit_journal(&repair_config()).await;
        assert_eq!(receiver.consensus().commit_min(), CHECKPOINT_OP);

        let refused = receiver
            .install_state_transfer(
                &repair_config(),
                offer.commit_op,
                Vec::new(),
                &offer.offsets.1,
            )
            .await;

        assert!(
            matches!(
                refused,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferErasesDurableOffsets {
                        local_offsets: 2
                    }
                )
            ),
            "files torn: {tear_files}, expected the empty offer to be refused, got {refused:?}"
        );
        assert_eq!(
            receiver.external_group_offset(LIVE_EXTERNAL_GROUP_ID),
            Some(MESSAGE_LESS_EXTERNAL_OFFSET)
        );
        assert!(
            receiver_directory
                .path()
                .join(format!("offsets/external_groups/{LIVE_EXTERNAL_GROUP_ID}"))
                .exists()
        );
    }
}

/// Boot lost the file of one of the donor's two offsets, so its delete of
/// that key removes nothing and the table still holds the other. That
/// delete must not mark the table emptied.
#[compio::test]
async fn given_donor_that_lost_one_of_two_offset_files_when_it_deletes_that_offset_should_not_mark_the_table_emptied()
 {
    const CHECKPOINT_OP: u64 = 2;
    const DELETE_OP: u64 = 3;
    let (_origin_directory, origin) = external_origin_that_deleted_one_of_two_groups().await;
    let donor_directory = tempfile::tempdir().unwrap();
    let (config, donor) = Box::pin(restarted_replica_of(
        &origin,
        donor_directory.path(),
        1,
        CHECKPOINT_OP,
    ))
    .await;
    drop(donor);
    let lost_file = format!(
        "{}/{MESSAGE_LESS_EXTERNAL_GROUP_ID}",
        config.get_external_group_offsets_path(1, 1, 0)
    );
    std::fs::write(lost_file, []).unwrap();
    let (config, mut donor) = replica_with_prepare_history(donor_directory.path(), 1).await;
    assert_eq!(
        donor.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
        None
    );

    commit_replicated_through(&mut donor, &origin, &config, DELETE_OP).await;

    assert_eq!(
        donor.external_group_offset(LIVE_EXTERNAL_GROUP_ID),
        Some(MESSAGE_LESS_EXTERNAL_OFFSET)
    );
    let partition_dir = config.get_partition_path(1, 1, 0);
    assert_eq!(
        emptied_marker_op(std::path::Path::new(&partition_dir)),
        None
    );
    assert_eq!(donor.durable_consumer_offsets.emptied_at(), 0);
}

/// The simulator's restart image keeps the emptied-at op, as boot reads the
/// marker back from disk.
#[compio::test]
async fn given_emptied_table_when_retained_across_a_restart_should_keep_the_emptied_op() {
    let (_directory, mut origin) = emptied_external_origin().await;
    let retained = origin.take_retained_state();
    let (mut restarted, _) = recording_partition_at(0, 3);

    restarted.adopt_retained_log(retained);

    assert_eq!(restarted.durable_consumer_offsets.emptied_at(), 2);
}

/// A crash that beats the directory sync can bring back an offset file the
/// emptying delete unlinked. Boot then drops the marker instead of trusting it.
#[compio::test]
async fn given_emptied_marker_beside_an_offset_file_when_recovering_should_remove_the_marker() {
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    persist_offset(&external_file, 1_000, true).await.unwrap();
    let offsets_dir = config.get_offsets_path(1, 1, 0);
    persist_offset(&format!("{offsets_dir}/emptied"), 2, true)
        .await
        .unwrap();
    let (mut partition, _) = recording_partition();

    crate::partition_storage::configure_consumer_offsets_with_storage(
        &DiskStorage,
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
    )
    .await
    .unwrap();

    assert_eq!(partition.external_group_offset(9), Some(1_000));
    let partition_dir = std::path::Path::new(&offsets_dir).parent().unwrap();
    assert_eq!(emptied_marker_op(partition_dir), None);
}

/// An offset file that boot cannot read strands its key, and it may hold an
/// offset the table never loaded. Boot then drops the marker.
#[compio::test]
async fn given_emptied_marker_beside_a_stranded_offset_file_when_recovering_should_remove_the_marker()
 {
    let dir = tempfile::tempdir().unwrap();
    let config = offsets_config(&dir);
    let external_file = format!("{}/9", config.get_external_group_offsets_path(1, 1, 0));
    persist_offset(&external_file, 1_000, true).await.unwrap();
    let offsets_dir = config.get_offsets_path(1, 1, 0);
    persist_offset(&format!("{offsets_dir}/emptied"), 2, true)
        .await
        .unwrap();
    let (mut partition, _) = recording_partition();

    crate::partition_storage::configure_consumer_offsets_with_storage(
        &FailingPathStorage(external_file.into()),
        &mut partition,
        &config,
        IggyNamespace::new(1, 1, 0),
    )
    .await
    .unwrap();

    assert_eq!(
        partition.stranded_consumer_offset_count(ConsumerKind::ExternalGroup),
        1
    );
    let partition_dir = std::path::Path::new(&offsets_dir).parent().unwrap();
    assert_eq!(emptied_marker_op(partition_dir), None);
    assert_eq!(partition.durable_consumer_offsets.emptied_at(), 0);
}
