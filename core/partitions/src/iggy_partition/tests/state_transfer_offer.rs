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

/// An offer whose frontier sits below this replica's own offset counter is
/// refused: installing it would rewind the counter, and the next replicated
/// prepare is re-stamped from it, so this replica would persist different
/// bytes (and a different `batch_checksum`) than the rest of the group.
#[compio::test]
async fn given_offer_below_local_counter_when_installed_should_refuse_rewind() {
    let partition_dir = transfer_fence_dir("rewind-refused").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    partition.set_offset_space_used(true);
    partition.offset.store(99, Ordering::Release);
    // Committed and resident: the threshold-gated flush leaves exactly this
    // shape, and the fence has to count it as data.
    {
        let info = &mut partition.log.journal_mut().info;
        info.messages_count = 100;
        info.current_offset = 99;
    }

    let behind = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 50,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &behind.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 50,
                    local_next_offset: 100,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// A chain installed EMPTY at frontier N holds no sized segment and no
/// journal entry, so a fence reading only held bytes reads 0 and skips
/// itself, letting a stale offer rewind the counter under offsets this
/// replica already claimed. The committed frontier is what carries N.
#[compio::test]
async fn given_an_empty_chain_installed_at_a_frontier_when_a_stale_offer_arrives_should_refuse() {
    let partition_dir = transfer_fence_dir("empty-install-rewind").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    // What an install of an all-GC'd origin leaves: the counter at the group
    // frontier, nothing on disk, nothing resident.
    partition.set_offset_space_used(true);
    partition.offset.store(4_095, Ordering::Release);
    partition.dirty_offset.store(4_095, Ordering::Relaxed);
    assert_eq!(
        partition.held_offset_frontier(),
        4_096,
        "the committed arm has to carry a frontier no byte on disk names"
    );

    let stale = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        owner_retirement: None,
        owners: Vec::new(),
        history_transition: None,
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 1_000,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        retirement_paid_through: 0,
        machines: None,
    };
    let refused = partition
        .install_state_transfer(&repair_config(), 12, Vec::new(), &stale.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferRewindsDurableData {
                    offer_next_offset: 1_000,
                    local_next_offset: 4_096,
                }
            )
        ),
        "expected a rewind refusal, got {refused:?}"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// Primary-by-index at view 0 with nothing committed refuses to serve: an
/// empty group is trivially "caught up", so this gate is the only thing
/// separating a real primary from a phantom whose directory vanished, whose
/// zero-segment offer at frontier 0 would make a data-holding receiver
/// unlink its chain.
#[compio::test]
async fn given_nothing_committed_when_offer_requested_should_refuse() {
    let partition_dir = transfer_fence_dir("nothing-committed").await;
    let mut partition = test_partition();
    partition.set_partition_dir(partition_dir.clone());
    assert_eq!(partition.consensus().commit_max(), 0);

    let refused = partition.state_transfer_offer(&repair_config()).await;
    assert!(
        matches!(
            refused,
            Err(crate::state_transfer::PartitionTransferUnavailable::NothingCommitted)
        ),
        "expected a NothingCommitted refusal, got {refused:?}"
    );
    assert!(
        refused.is_err_and(|reason| reason.transient()),
        "the refusal must be transient: the requester rotates rather than \
             charging its failure count"
    );

    let _ = std::fs::remove_dir_all(&partition_dir);
}

/// A partition that never took a message still commits owner, history and
/// retry state, so a replica behind the repair window can only recover by
/// transfer. The caught-up primary serves its empty chain at frontier 0,
/// and the receiver lands on that state without claiming an offset.
#[compio::test]
async fn given_message_less_primary_when_offer_requested_should_serve_its_committed_state() {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    assert!(offer.segments.is_empty());
    let (_receiver_directory, mut receiver) = transfer_receiver();
    let incarnation = receiver.created_revision();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
    assert_eq!(receiver.created_revision(), incarnation);
}

/// A replica that never took a message and lags past every op its peers
/// retain cannot repair, so it catches up from the primary's offer.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_fully_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // An empty serving journal reports eviction from its commit point.
    receiver.repair = Some(armed_session(3, 3));

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        conclusion,
        RepairConclusion::FloorRefused { floor: 3, to_op: 3 }
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
}

/// A complete window without messages says nothing about the ops below
/// its floor when the replica never took a message. Op 1 stored an
/// external offset, so jumping the floor would drop it unseen.
#[compio::test]
async fn given_lagging_message_less_replica_when_window_partly_evicted_should_catch_up_by_transfer()
{
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver.consensus().advance_commit_max(3);
    // The serving peer evicted op 1 and still retains ops 2 and 3.
    receiver.repair = Some(armed_session(3, 1));
    for op in [2, 3] {
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, op))
            .await;
    }

    let conclusion = receiver.complete_repair(&repair_config()).await;
    assert_eq!(
        (conclusion, receiver.consensus().commit_min()),
        (RepairConclusion::FloorRefused { floor: 1, to_op: 3 }, 0)
    );
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_holds_message_less_state(&receiver, &origin, &transition);
}

/// A backup restarts with origin op 1 replayed from its WAL but not
/// committed, and its peers retain only op 3. A floor at op 2 would skip
/// that op 1 and the op 2 nobody serves, so a replica with no committed
/// message walks op 1, refuses the floor and catches up by transfer.
#[compio::test]
async fn given_restarted_message_less_backup_when_floor_skips_unserved_op_should_refuse_and_transfer()
 {
    let (_origin_directory, mut origin, transition) = message_less_origin().await;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let open_backup = async || {
            let (mut backup, _) = recording_partition_at(1, 3);
            backup.set_partition_dir(directory.path().to_string_lossy().into_owned());
            set_offset_dirs_under(&mut backup, directory.path());
            backup.runtime_options.durability = durability;
            backup.runtime_options.consumer_offset_durability = durability;
            backup.open_persistence().await.unwrap();
            backup
        };
        let mut backup = open_backup().await;
        backup.on_replicate(retained_prepare(&origin, 1)).await;
        // Replicated durability writes op 1 without a barrier, and recovery
        // drops what was never synced, so sync it before the crash.
        let persistence = backup.persistence.as_ref().unwrap();
        persistence.sync();
        backup.start_persistence();
        persistence
            .drain_with_timeout(compio::runtime::time::sleep)
            .await
            .unwrap();
        assert!(persistence.is_durable_through(1), "{durability:?}");
        drop(backup);

        let mut receiver = open_backup().await;
        assert_eq!(
            receiver.consensus().sequencer().current_sequence(),
            1,
            "{durability:?}"
        );
        assert!(receiver.log.journal().inner.holds_op(1), "{durability:?}");
        assert_eq!(receiver.consensus().commit_min(), 0, "{durability:?}");
        receiver.consensus().advance_commit_max(3);
        receiver.repair = Some(armed_session(3, 2));
        receiver
            .apply_repaired_prepare(retained_prepare(&origin, 3))
            .await;

        let conclusion = receiver.complete_repair(&repair_config()).await;
        assert_eq!(
            conclusion,
            RepairConclusion::FloorRefused { floor: 2, to_op: 3 },
            "{durability:?}"
        );
        assert_eq!(receiver.consensus().commit_min(), 1, "{durability:?}");
        assert_eq!(
            receiver.external_group_offset(MESSAGE_LESS_EXTERNAL_GROUP_ID),
            Some(MESSAGE_LESS_EXTERNAL_OFFSET),
            "{durability:?}"
        );
        let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
        receiver
            .install_state_transfer(
                &repair_config(),
                offer.commit_op,
                Vec::new(),
                &offer.offsets.1,
            )
            .await
            .unwrap();
        assert_holds_message_less_state(&receiver, &origin, &transition);
    }
}

fn batch_stats(base_offset: u64, message_count: u32) -> CommittedBatchStats {
    CommittedBatchStats {
        producer: None,
        timestamp: 0,
        base_offset,
        message_count,
        size_bytes: 128,
    }
}

#[test]
fn given_send_messages_when_offsets_resolved_should_confirm_base_offset() {
    let namespace = IggyNamespace::new(3, 7, 5);
    let stats = batch_stats(42, 3);

    let body = send_messages_reply_body(namespace.inner(), Some(stats));
    let (response, consumed) = SendMessagesResponse::decode(&body).unwrap();

    assert_eq!(consumed, body.len());
    assert_eq!(
        response.confirmations,
        vec![SendMessagesConfirmationResponse {
            stream_id: 3,
            topic_id: 7,
            partition_id: 5,
            base_offset: 42,
        }]
    );
}

#[test]
fn given_send_messages_when_offsets_unavailable_should_reply_zero_confirmations() {
    let namespace = IggyNamespace::new(1, 1, 0);

    let body = send_messages_reply_body(namespace.inner(), None);

    assert_eq!(&body[..], &[0, 0, 0, 0]);
    let (response, _) = SendMessagesResponse::decode(&body).unwrap();
    assert_eq!(response.confirmations, []);
}

#[test]
fn given_batch_stats_when_end_offset_derived_should_span_the_message_run() {
    assert_eq!(batch_stats(9, 1).end_offset(), 9);
    assert_eq!(batch_stats(9, 4).end_offset(), 12);
}

#[test]
fn given_result_framed_operation_when_committed_should_reply_empty_result_section() {
    assert_eq!(
        &committed_reply_body(Operation::StoreConsumerOffset)[..],
        &[0, 0, 0, 0]
    );
}

#[test]
fn given_unframed_operation_when_committed_should_reply_empty_body() {
    assert!(committed_reply_body(Operation::DeleteSegments).is_empty());
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_an_unsyncable_messages_writer_when_installing_should_refuse_the_barrier() {
    let directory = tempfile::tempdir().unwrap();
    let wal = directory.path().join("wal");
    let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
    let messages_writer =
        MessagesWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), false, false, None)
            .await
            .unwrap();
    let index_path = directory.path().join("segment.index");
    let index_writer = IggyIndexWriter::new(
        index_path.to_str().unwrap(),
        Rc::new(AtomicU64::new(0)),
        false,
        false,
    )
    .await
    .unwrap();
    let mut partition = test_partition();
    partition.log.add_persisted_segment(
        Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
        SegmentStorage::default(),
        Some(Rc::new(messages_writer)),
        Some(Rc::new(index_writer)),
    );
    partition.persistence = Some(Rc::clone(&persistence));

    partition.barrier_install_files_locked();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.failure().is_some());
}
