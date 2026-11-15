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

use crate::RepairConclusion;

#[compio::test]
async fn given_a_fenced_window_when_the_anchor_proves_it_should_release_the_walk() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    let second = journal_sealed(&partition, 2, first.checksum, 2).await;
    partition.consensus.restore_commit_state(0, 2);
    partition.fence_view_window(2, None);
    assert!(
        walked_ops(&partition).is_empty(),
        "the walk applied an entry the view has not vouched for"
    );

    partition.offer_view_anchor(sealed_prepare(3, second.checksum, 3).header());
    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Proven)
    );
    assert!(!partition.view_fenced());
    assert_eq!(walked_ops(&partition), vec![1, 2]);
}

#[compio::test]
async fn given_a_hole_in_the_fenced_window_when_proving_should_truncate_nothing() {
    let mut partition = test_partition();
    journal_sealed(&partition, 1, 0, 1).await;
    let third = journal_sealed(&partition, 3, 7, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.fence_view_window(3, Some(*sealed_prepare(4, third.checksum, 4).header()));

    assert_eq!(
        partition.prove_view_window().await,
        Some(ViewWindowVerdict::Hole { op: 2 })
    );
    assert!(
        partition.log.journal().inner.holds_op(1) && partition.log.journal().inner.holds_op(3),
        "a hole is no evidence against the entries around it"
    );
    assert_eq!(partition.view_fence_hole(), Some(2));
    assert!(partition.view_fenced());
}

#[compio::test]
async fn given_a_recovered_commit_point_when_fenced_should_walk_the_committed_prefix() {
    let mut partition = test_partition();
    let first = journal_sealed(&partition, 1, 0, 1).await;
    let second = journal_sealed(&partition, 2, first.checksum, 2).await;
    journal_sealed(&partition, 3, second.checksum, 3).await;
    partition.consensus.restore_commit_state(0, 3);
    partition.recovered_commit = 2;
    partition.fence_view_window(3, None);

    assert!(partition.view_fenced());
    assert_eq!(
        walked_ops(&partition),
        vec![1, 2],
        "the recovered WAL already named ops 1 and 2 committed"
    );
}

#[compio::test]
async fn given_paused_repair_when_valid_frames_arrive_should_refresh_activity_without_writing() {
    const IDLE_TICKS: u32 = 7;
    let mut partition = test_partition();
    partition.repair = Some(armed_session(3, 0));
    partition.repair.as_mut().unwrap().idle_ticks = IDLE_TICKS;
    partition.repair_attempts = 2;
    partition.transition = Some(PendingPartitionTransition::View { drain: None });

    partition
        .apply_repaired_prepare(repaired_send_prepare(4, 0, 4))
        .await;
    assert_eq!(partition.repair.as_ref().unwrap().idle_ticks, IDLE_TICKS);
    assert_eq!(partition.repair_attempts, 2);
    partition
        .apply_repaired_prepare(repaired_send_prepare(1, 0, 1))
        .await;
    assert_eq!(partition.repair.as_ref().unwrap().idle_ticks, 0);
    assert_eq!(partition.repair_attempts, 0);
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert_eq!(partition.consensus().sequencer().current_sequence(), 0);
}

#[compio::test]
async fn given_local_ops_below_repair_floor_when_completing_should_apply_them_and_cancel_transfer()
{
    let mut partition = test_partition();
    for op in 1..=3 {
        journal_send_batch(&mut partition, op).await;
    }
    partition.consensus().advance_commit_max(3);
    partition.repair = Some(armed_session(3, 2));
    partition.transfer_rearm = Some(crate::state_transfer::PendingTransferRearm {
        peer: 0,
        after_ticks: 1,
    });
    assert_eq!(partition.recovered_durable_offset, None);

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(conclusion, RepairConclusion::Done);
    assert_eq!(partition.consensus().commit_min(), 3);
    assert!(partition.repair.is_none());
    assert!(partition.transfer_rearm.is_none());
}

#[compio::test]
async fn given_no_repaired_batch_when_window_never_arrived_should_refuse_commit_floor() {
    let mut partition = test_partition();
    partition.consensus().advance_commit_max(8);
    partition.repair = Some(armed_session(8, 5));

    let conclusion = partition.complete_repair(&repair_config()).await;

    assert_eq!(
        conclusion,
        RepairConclusion::InProgress,
        "an incomplete window is not a definitive refusal"
    );
    assert_eq!(partition.consensus().commit_min(), 0);
    assert!(
        partition.repair.is_some(),
        "session must stay armed for retry"
    );
}

#[compio::test]
async fn given_unproven_history_when_repair_floor_connects_should_require_state_transfer() {
    const FLOOR: u64 = 2;
    const HEAD: u64 = 3;
    let (mut backup, _) = recording_partition_at(1, 3);
    backup.consensus().advance_commit_max(HEAD);
    backup.repair = Some(armed_session(HEAD, FLOOR));
    backup
        .apply_repaired_prepare(checksummed_segment_prepare(HEAD, 0, 0, b"connected"))
        .await;
    backup.consensus().fence_view_log(FLOOR, FLOOR);

    assert_eq!(
        backup.complete_repair(&repair_config()).await,
        RepairConclusion::FloorRefused {
            floor: FLOOR,
            to_op: HEAD
        }
    );
    assert_eq!(backup.consensus().commit_min(), 0);
    assert!(backup.consensus().view_log_unproven());
    assert!(backup.repair.is_none());
}

#[compio::test]
async fn receipt_recovery_reclaims_only_unreferenced_artifacts_after_validation() {
    const CHECKPOINT_OP: u64 = 7;
    const OBSOLETE_OP: u64 = 3;
    const ORPHAN_OP: u64 = 99;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = partition_at_view(1, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let prepares = directory.path().join("prepares-0");
    std::fs::create_dir(&prepares).unwrap();
    for through in [OBSOLETE_OP, CHECKPOINT_OP, ORPHAN_OP] {
        publish_empty_retry_checkpoint(&partition, through).await;
    }
    let current = prepares.join("receipts-7.checkpoint");
    let valid = std::fs::read(&current).unwrap();
    let retained = [
        "receipts-unknown.checkpoint",
        "prepare-99.log",
        "receipts-4.checkpoint-dir",
    ];
    for name in retained {
        std::fs::write(prepares.join(name), b"unrelated").unwrap();
    }
    std::fs::write(prepares.join("receipts-8.checkpoint.tmp"), b"unfinished").unwrap();
    std::fs::create_dir(prepares.join("receipts-9.checkpoint")).unwrap();
    std::fs::write(&current, b"corrupt").unwrap();
    assert!(
        partition
            .restore_retry_checkpoint(CHECKPOINT_OP)
            .await
            .is_err()
    );
    assert!(prepares.join("receipts-3.checkpoint").exists());
    assert!(prepares.join("receipts-99.checkpoint").exists());

    std::fs::write(&current, &valid).unwrap();
    partition
        .restore_retry_checkpoint(CHECKPOINT_OP)
        .await
        .unwrap();
    assert_eq!(std::fs::read(&current).unwrap(), valid);
    for name in [
        "receipts-3.checkpoint",
        "receipts-99.checkpoint",
        "receipts-8.checkpoint.tmp",
    ] {
        assert!(
            !prepares.join(name).exists(),
            "obsolete artifact retained: {name}"
        );
    }
    for name in retained {
        assert!(
            prepares.join(name).exists(),
            "unrelated entry removed: {name}"
        );
    }
    assert!(prepares.join("receipts-9.checkpoint").is_dir());

    // A receipt published before a failed WAL publication remains orphaned at boot.
    publish_empty_retry_checkpoint(&partition, ORPHAN_OP).await;
    partition
        .restore_retry_checkpoint(CHECKPOINT_OP)
        .await
        .unwrap();
    assert!(!prepares.join("receipts-99.checkpoint").exists());
    assert!(current.exists());
    partition.restore_retry_checkpoint(0).await.unwrap();
    assert!(!current.exists());
}

#[compio::test]
async fn checkpoint_only_recovery_restores_the_prepare_chain_anchor() {
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
    journal.certify_log_view(1, 7, 1234).await.unwrap();
    drop(journal);
    publish_empty_retry_checkpoint(&partition, 7).await;
    partition.open_persistence().await.unwrap();
    assert_eq!(partition.consensus().sequencer().current_sequence(), 7);
    assert_eq!(partition.consensus().commit_min(), 7);
    assert_eq!(partition.consensus().last_prepare_checksum(), 1234);
}

/// A restored counter above the chain must move the chain, not just the
/// counter. Left as one segment named at 0, the next mint lands INSIDE it --
/// a shape production's boot never produces, so the harness would model
/// something the server cannot reach and could not expose the chain refusal
/// the boot after would hit.
#[test]
fn given_a_restored_counter_above_an_empty_chain_when_reanchoring_should_plant_at_the_mint() {
    let mut partition = solo_recording_partition();
    let recovered = recorded_state(0, 65_537);
    partition.set_superblock(Rc::new(RecordingSuperblock::default()), Some(&recovered));
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.mint_frontier(), 65_537);
    assert_eq!(
        partition.log.segments().len(),
        1,
        "the premise: one empty segment named at 0, as the rebuild leaves it"
    );

    partition.reanchor_in_memory_to_mint_frontier(IggyByteSize::from(1024 * 1024));

    let starts: Vec<u64> = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .collect();
    assert_eq!(
        starts,
        vec![65_537],
        "the empty segment claiming 0.. is retired and one planted at the \
         append point, exactly as boot's emptied-chain arm does"
    );
}

/// A SIZED tail is the only copy of its messages, so it is sealed and the
/// plant goes past it, leaving the gap the chain guard admits by anchor.
#[test]
fn given_a_restored_counter_above_a_sized_tail_when_reanchoring_should_seal_and_plant_past_it() {
    let mut partition = solo_recording_partition();
    {
        let tail = partition.log.active_segment_mut();
        tail.size = IggyByteSize::from(4_096);
        tail.end_offset = 9;
    }
    partition.note_committed_seeded();
    partition.offset.store(9, Ordering::Release);
    partition.dirty_offset.store(9, Ordering::Relaxed);
    let recovered = recorded_state(10, 65_547);
    partition.restore_offset_frontier(Some(&recovered));
    assert_eq!(partition.mint_frontier(), 65_547);

    partition.reanchor_in_memory_to_mint_frontier(IggyByteSize::from(1024 * 1024));

    let segments = partition.log.segments();
    assert_eq!(segments.len(), 2, "the sized tail is kept, not retired");
    assert!(segments[0].sealed, "and sealed before the plant lands");
    assert_eq!(segments[0].end_offset, 9);
    assert_eq!(
        segments[1].start_offset, 65_547,
        "the plant names the append point, so the next mint starts a segment \
         rather than landing inside one"
    );
}

/// A tail already named AT the append point takes the appends as it stands.
/// Planting beside it would leave two segments claiming the same start
/// offset, which no chain guard admits.
#[test]
fn given_a_chain_already_anchored_at_the_mint_when_reanchoring_should_leave_it_alone() {
    let segment_size = IggyByteSize::from(1024 * 1024);
    let mut partition = solo_recording_partition();
    {
        let tail = partition.log.active_segment_mut();
        tail.size = IggyByteSize::from(4_096);
        tail.end_offset = 9;
    }
    // The shape a clean boot leaves: the flushed tail, then an empty segment
    // already named for the append point.
    partition.log.add_persisted_segment(
        crate::Segment::new(10, segment_size),
        server_common::SegmentStorage::default(),
        None,
        None,
    );
    partition.note_committed_seeded();
    partition.offset.store(9, Ordering::Release);
    partition.dirty_offset.store(9, Ordering::Relaxed);
    assert_eq!(partition.mint_frontier(), 10);

    partition.reanchor_in_memory_to_mint_frontier(segment_size);

    let starts: Vec<u64> = partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .collect();
    assert_eq!(
        starts,
        vec![0, 10],
        "the empty tail is named at the append point, so it is neither retired \
         nor planted beside"
    );
    assert!(
        !partition.log.segments()[0].sealed,
        "and nothing was sealed, since no plant needed a gap"
    );
}

#[compio::test]
async fn given_durable_adopted_partition_headers_when_reopening_should_keep_body_certification_separate()
 {
    const BACKUP: u8 = 1;
    const ADOPTED: u32 = 2;
    const HELD: u64 = 1;
    const HEAD: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
    let partition_dir = config.get_partition_path(1, 1, 0);
    let store = Rc::new(PingPongSuperblock::open(&partition_dir).await.unwrap());
    partition.set_superblock(store.clone(), None);
    let held = checksummed_segment_prepare(HELD, 0, 0, b"retained");
    partition.on_replicate(held.clone()).await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert_eq!(persistence.head(), HELD);
    assert_eq!(persistence.certified_log_view(), Some(0));
    assert!(
        partition
            .complete_superblock_intent(SuperblockIntent::Advance {
                frontier: partition.offset_frontier(),
                reserved: 0
            })
            .await
    );
    let mut parent = *held.header();
    let mut headers = Vec::new();
    for op in HELD + 1..=HEAD {
        let mut header = PrepareHeader {
            op,
            request: op,
            timestamp: op,
            view: ADOPTED,
            parent: parent.checksum,
            ..parent
        };
        header.checksum = header.identity_checksum();
        parent = header;
        headers.push(header);
    }
    headers.reverse();
    let start_view = Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
        iggy_binary_protocol::StartViewHeader,
    >())
    .transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
        header.command = Command::StartView;
        header.cluster = TEST_CLUSTER;
        header.group = partition.namespace().inner();
        header.view = ADOPTED;
        header.replica = 2;
        header.op = HEAD;
        header.commit = HEAD - 1;
        header.size = u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
    });
    partition.consensus().handle_start_view(
        consensus::PlaneKind::Partitions,
        start_view.header(),
        bytemuck::cast_slice(&headers),
    );
    assert!(partition.consensus().view_log_unproven());
    assert!(
        partition.persist_superblock_if_needed().await,
        "DVC identity can persist while the adopted bodies are missing"
    );
    assert_eq!(persistence.certified_log_view(), Some(0));
    assert!(!partition.register_rebuilt_ack(&headers[0]));
    let segment_size = partition.log.active_segment().max_size;
    drop(persistence);
    drop(partition);
    drop(store);

    let (store, latest) = PingPongSuperblock::open_with_latest(&partition_dir)
        .await
        .unwrap();
    let journal::superblock::SuperblockContents::Present(bytes) = latest else {
        panic!("adopted record must be durable");
    };
    let state = consensus::VsrState::try_from(bytes.as_slice()).unwrap();
    assert_eq!(state.log_view, 0);
    state
        .validate_group(IggyNamespace::new(1, 1, 0).inner(), 0)
        .unwrap();
    let (mut reopened, _) = recording_partition_at(BACKUP, 3);
    reopened.consensus.set_view(state.view);
    reopened.consensus.set_log_view(state.log_view);
    reopened
        .consensus
        .mark_superblock_durable(state.view, state.log_view);
    reopened.set_superblock(Rc::new(store), Some(&state));
    reopened.set_partition_dir(partition_dir);
    reopened.log.active_segment_mut().max_size = segment_size;
    // Open the files already referenced by the WAL; creating an empty segment
    // would erase the retained body this restart is meant to preserve.
    reopened.open_persistence().await.unwrap();
    assert!(!reopened.requires_state_transfer());
    let persistence = reopened.persistence.as_ref().unwrap();
    assert_eq!(persistence.head(), HELD);
    assert_eq!(persistence.checksum(HELD), Some(held.header().checksum));
    assert_eq!(persistence.certified_log_view(), Some(0));
    assert_eq!(
        reopened.consensus().adopted_view_head(),
        Some((ADOPTED, HEAD))
    );
    assert_eq!(reopened.consensus().log_view_claim(), ADOPTED);
    assert_eq!(reopened.consensus().sequencer().current_sequence(), HEAD);
    assert!(reopened.consensus().view_log_unproven());
    assert!(!reopened.consensus().view_log_entry_proven(HEAD));
    assert!(!reopened.register_rebuilt_ack(&headers[0]));
}

/// An install below the head of an adopted `StartView` leaves the WAL without
/// that view's log. A restart before repair must restore the adopted head, or
/// the replica votes for the view with a head at the install op.
#[compio::test]
async fn given_an_install_below_the_adopted_head_when_reopening_should_resume_the_adopted_view() {
    const BACKUP: u8 = 1;
    const ADOPTED: u32 = 2;
    const HELD: u64 = 1;
    const HEAD: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
    let partition_dir = config.get_partition_path(1, 1, 0);
    let store = Rc::new(PingPongSuperblock::open(&partition_dir).await.unwrap());
    partition.set_superblock(store.clone(), None);
    let held = checksummed_segment_prepare(HELD, 0, 0, b"retained");
    partition.on_replicate(held.clone()).await;
    let persistence = partition.persistence.as_ref().unwrap().clone();
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    let mut parent = *held.header();
    let mut headers = Vec::new();
    for op in HELD + 1..=HEAD {
        let mut header = PrepareHeader {
            op,
            request: op,
            timestamp: op,
            view: ADOPTED,
            parent: parent.checksum,
            ..parent
        };
        header.checksum = header.identity_checksum();
        parent = header;
        headers.push(header);
    }
    headers.reverse();
    let start_view = Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
        iggy_binary_protocol::StartViewHeader,
    >())
    .transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
        header.command = Command::StartView;
        header.cluster = TEST_CLUSTER;
        header.group = partition.namespace().inner();
        header.view = ADOPTED;
        header.replica = 2;
        header.op = HEAD;
        header.commit = HEAD - 1;
        header.size = u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
    });
    partition.consensus().handle_start_view(
        consensus::PlaneKind::Partitions,
        start_view.header(),
        bytemuck::cast_slice(&headers),
    );
    assert!(partition.persist_superblock_if_needed().await);

    let body = held.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = partition
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        prepare_checksum: Some(held.header().checksum),
        checkpoint_prepare: held.as_slice().to_vec(),
        next_offset: 1,
        ..Default::default()
    };
    partition.consensus().begin_state_transfer_await();
    partition
        .install_state_transfer(&repair_config(), HELD, vec![staged], &offsets.encode())
        .await
        .unwrap();
    assert_eq!(partition.consensus().sequencer().current_sequence(), HEAD);
    assert!(partition.consensus().view_log_unproven());
    let certified_after_install = persistence.certified_log_view();
    let segment_size = partition.log.active_segment().max_size;
    drop(persistence);
    drop(partition);
    drop(store);

    let (store, latest) = PingPongSuperblock::open_with_latest(&partition_dir)
        .await
        .unwrap();
    let journal::superblock::SuperblockContents::Present(bytes) = latest else {
        panic!("adopted record must be durable");
    };
    let state = consensus::VsrState::try_from(bytes.as_slice()).unwrap();
    let (mut reopened, _) = recording_partition_at(BACKUP, 3);
    reopened.consensus.set_view(state.view);
    reopened.consensus.set_log_view(state.log_view);
    reopened
        .consensus
        .mark_superblock_durable(state.view, state.log_view);
    reopened.set_superblock(Rc::new(store), Some(&state));
    reopened.set_partition_dir(partition_dir);
    reopened.log.active_segment_mut().max_size = segment_size;
    reopened.open_persistence().await.unwrap();
    assert!(
        !reopened.requires_state_transfer(),
        "the certificate after the install is below the persisted log view"
    );
    assert_eq!(
        reopened.consensus().adopted_view_head(),
        Some((ADOPTED, HEAD))
    );
    assert_eq!(
        (
            reopened.consensus().sequencer().current_sequence(),
            reopened.consensus().view_log_unproven(),
        ),
        (HEAD, true),
        "the restart dropped the adopted head: certified view after the install \
         {certified_after_install:?}, state log_view {}",
        state.log_view
    );
}

/// An install above the head of an adopted `StartView` holds every op that view
/// announced. The replica leaves the install, and a restart after it, with its
/// head at the checkpoint and nothing of the adopted log left to prove.
#[compio::test]
async fn given_an_install_above_the_adopted_head_when_reopening_should_keep_the_head_at_the_checkpoint()
 {
    const BACKUP: u8 = 1;
    const ADOPTED: u32 = 2;
    const HEAD: u64 = 1;
    const INSTALLED: u64 = HEAD + 1;
    let directory = tempfile::tempdir().unwrap();
    let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
    let partition_dir = config.get_partition_path(1, 1, 0);
    let store = Rc::new(PingPongSuperblock::open(&partition_dir).await.unwrap());
    partition.set_superblock(store.clone(), None);
    let checkpoint = checksummed_segment_prepare(INSTALLED, 0, 0, b"retained");
    let mut announced = PrepareHeader {
        op: HEAD,
        request: HEAD,
        timestamp: HEAD,
        view: ADOPTED,
        ..*checkpoint.header()
    };
    announced.checksum = announced.identity_checksum();
    let start_view = Message::<iggy_binary_protocol::StartViewHeader>::new(size_of::<
        iggy_binary_protocol::StartViewHeader,
    >())
    .transmute_header(|_, header: &mut iggy_binary_protocol::StartViewHeader| {
        header.command = Command::StartView;
        header.cluster = TEST_CLUSTER;
        header.group = partition.namespace().inner();
        header.view = ADOPTED;
        header.replica = 2;
        header.op = HEAD;
        header.commit = HEAD - 1;
        header.size = u32::try_from(size_of::<iggy_binary_protocol::StartViewHeader>()).unwrap();
    });
    partition.consensus().handle_start_view(
        consensus::PlaneKind::Partitions,
        start_view.header(),
        bytemuck::bytes_of(&announced),
    );
    assert!(partition.consensus().view_log_unproven());
    assert!(partition.persist_superblock_if_needed().await);

    let body = checkpoint.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let staged = partition
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        prepare_checksum: Some(checkpoint.header().checksum),
        checkpoint_prepare: checkpoint.as_slice().to_vec(),
        next_offset: 1,
        ..Default::default()
    };
    partition.consensus().begin_state_transfer_await();
    partition
        .install_state_transfer(&repair_config(), INSTALLED, vec![staged], &offsets.encode())
        .await
        .unwrap();
    assert_eq!(
        (
            partition.consensus().sequencer().current_sequence(),
            partition.consensus().view_log_unproven(),
            partition.consensus().view_log_is_pending(),
        ),
        (INSTALLED, false, false),
        "the install at op {INSTALLED} left the view-{ADOPTED} log parked and unproven, with its \
         head at op {HEAD} below the commit point"
    );
    let segment_size = partition.log.active_segment().max_size;
    drop(partition);
    drop(store);

    let (store, latest) = PingPongSuperblock::open_with_latest(&partition_dir)
        .await
        .unwrap();
    let journal::superblock::SuperblockContents::Present(bytes) = latest else {
        panic!("adopted record must be durable");
    };
    let state = consensus::VsrState::try_from(bytes.as_slice()).unwrap();
    let (mut reopened, _) = recording_partition_at(BACKUP, 3);
    reopened.consensus.set_view(state.view);
    reopened.consensus.set_log_view(state.log_view);
    reopened
        .consensus
        .mark_superblock_durable(state.view, state.log_view);
    reopened.set_superblock(Rc::new(store), Some(&state));
    reopened.set_partition_dir(partition_dir);
    reopened.log.active_segment_mut().max_size = segment_size;
    reopened.open_persistence().await.unwrap();
    assert!(
        !reopened.requires_state_transfer(),
        "the install certified the WAL below the log view {} that it persisted",
        state.log_view
    );
    assert_eq!(
        (
            reopened.consensus().sequencer().current_sequence(),
            reopened.consensus().view_log_unproven(),
            reopened.consensus().view_log_is_pending(),
        ),
        (INSTALLED, false, false),
        "the restart resumed the view-{ADOPTED} adoption below the installed checkpoint"
    );
}

/// The WAL drops a divergent run before the journal does. The canonical
/// tail cannot extend that WAL branch and exists only in the journal.
/// A crash before refill leaves the committed prefix uncertified,
/// so the replica claims no commit point and waits for a state transfer.
#[compio::test]
async fn given_a_crash_after_a_divergent_run_truncation_when_reopening_should_keep_the_committed_prefix_and_await_a_transfer()
 {
    const BACKUP: u8 = 1;
    const APPLIED: u64 = 2;
    const FIRST: u64 = APPLIED + 1;
    const LAST: u64 = FIRST + 1;
    const HEAD: u64 = LAST + 2;
    const TRUNCATION_STEPS_MAX: usize = 8;
    const COMMIT_ROUNDS_MAX: usize = 8;
    let parents = segment_chain_parents(HEAD, b"canonical");
    let canonical: Vec<_> = (1..=HEAD)
        .map(|op| {
            let parent = parents[usize::try_from(op - 1).unwrap()];
            checksummed_segment_prepare(op, parent, op - 1, b"canonical")
        })
        .collect();
    let mut parent = canonical[usize::try_from(APPLIED - 1).unwrap()]
        .header()
        .checksum;
    let stale: Vec<_> = (FIRST..=LAST)
        .map(|op| {
            let prepare = checksummed_segment_prepare(op, parent, op - 1, b"stale");
            parent = prepare.header().checksum;
            prepare
        })
        .collect();

    for deferred in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (config, mut partition) = replica_with_prepare_history(directory.path(), BACKUP).await;
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        for prepare in canonical
            .iter()
            .take(usize::try_from(APPLIED).unwrap())
            .chain(&stale)
        {
            partition.on_replicate(prepare.clone()).await;
        }
        partition.consensus().advance_commit_max(APPLIED);
        for _ in 0..COMMIT_ROUNDS_MAX {
            partition.commit_journal(&config).await;
            persistence
                .drain_with_timeout(compio::runtime::time::sleep)
                .await
                .unwrap();
            partition.drive_persistence().await;
            if partition.consensus().commit_min() == APPLIED {
                break;
            }
        }
        assert_eq!(partition.consensus().commit_min(), APPLIED);
        assert_eq!(persistence.head(), LAST, "the WAL holds the stale run");
        for prepare in canonical.iter().skip(usize::try_from(LAST).unwrap()) {
            partition
                .log
                .journal()
                .inner
                .append(prepare.clone().into_frozen())
                .await
                .unwrap();
        }
        let segment_size = partition.log.active_segment().max_size;
        // Fails the resident removal after the WAL truncation is durable.
        partition.log.active_segment_mut().size = IggyByteSize::from(u64::MAX);

        if deferred {
            partition.defer_view_truncation(FIRST..=LAST, Vec::new(), None);
            for _ in 0..TRUNCATION_STEPS_MAX {
                if !partition.view_truncation_pending() {
                    break;
                }
                partition.resume_io(&config).await;
                persistence
                    .drain_with_timeout(compio::runtime::time::sleep)
                    .await
                    .unwrap();
            }
            assert!(!partition.view_truncation_pending());
        } else {
            assert!(
                partition
                    .remove_divergent_prepares(FIRST..=LAST)
                    .await
                    .is_err()
            );
        }
        assert_eq!(persistence.head(), APPLIED, "deferred {deferred}");
        assert!(
            (FIRST..=HEAD).all(|op| partition.log.journal().inner.holds_op(op)),
            "deferred {deferred}: the crash comes before the journal removed anything"
        );
        drop(persistence);
        drop(partition);

        let (mut reopened, _) = recording_partition_at(BACKUP, 3);
        reopened.set_partition_dir(config.get_partition_path(1, 1, 0));
        reopened.log.active_segment_mut().max_size = segment_size;
        // Reuse the files the WAL references; installing an empty segment
        // here would truncate the committed message bodies before recovery.
        reopened.open_persistence().await.unwrap();
        let wal = reopened.persistence.as_ref().unwrap();
        assert_eq!(
            wal.head(),
            APPLIED,
            "deferred {deferred}: no op above the committed prefix"
        );
        for prepare in canonical.iter().take(usize::try_from(APPLIED).unwrap()) {
            let header = prepare.header();
            assert_eq!(
                wal.checksum(header.op),
                Some(header.checksum),
                "deferred {deferred}: committed op {} must stay in the WAL",
                header.op
            );
        }
        assert!(reopened.requires_state_transfer(), "deferred {deferred}");
        assert_eq!(
            (
                reopened.consensus().status(),
                reopened.consensus().state_transfer_stage(),
            ),
            (
                consensus::Status::Recovering,
                consensus::StateTransferStage::AwaitingTarget,
            ),
            "deferred {deferred}"
        );
        assert_eq!(
            reopened.consensus().commit_min(),
            0,
            "deferred {deferred}: the old certificate cannot vouch for the prefix"
        );
    }
}
