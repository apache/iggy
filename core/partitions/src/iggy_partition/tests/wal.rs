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
async fn given_wal_lane_published_persisted_runs_when_idle_should_leave_the_tick_retry_set() {
    const RUNS: u64 = 4;
    const IDLE_TURNS_MAX: usize = 64;
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _replies, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Persisted, 1).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let notified = Rc::new(std::cell::Cell::new(0_usize));
    let counter = Rc::clone(&notified);
    partition.set_io_notifier(
        Rc::new(move |_, _| counter.set(counter.get() + 1)),
        crate::largest_legal_job_charge().unwrap(),
    );
    for run in 0..RUNS {
        let first = 2 * run + 2;
        let mut prepares = Vec::new();
        for op in [first, first + 1] {
            let prepare = checksummed_segment_prepare(op, parent, op - 1, b"run");
            parent = prepare.header().checksum;
            persistence
                .append(prepare.clone().into_frozen(), true)
                .unwrap();
            prepares.push(prepare);
        }
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        for prepare in prepares {
            let header = *prepare.header();
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(header));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus().advance_commit_max(first + 1);
        let headers = partition.select_persistable_commits(&config);
        assert_eq!(headers.len(), 2);
        // Select inline, as `on_ack` does, so the lane only publishes the run.
        partition
            .handle_committed_entries(headers, &config, true)
            .await;
        assert!(
            partition.pending_commit.is_some(),
            "a Persisted run under a dispatcher must wait for a lane job"
        );
        settle_partition_io(&mut partition, &config).await;
        assert_eq!(partition.consensus().commit_min(), first + 1);
    }
    let mut idle_turns = 0;
    while idle_turns < IDLE_TURNS_MAX {
        notified.set(0);
        if !partition.needs_io_retry() {
            break;
        }
        idle_turns += 1;
        assert!(matches!(
            partition.resume_io(&config).await,
            crate::PartitionIoStep::Pending
        ));
    }
    assert!(partition.fatal().is_none());
    assert_eq!(
        idle_turns, 0,
        "an idle partition must not stay in the tick retry set"
    );
}

/// Installing wipes the journal, so an offer below a committed op this
/// replica holds would drop an op the quorum may have counted it for.
#[compio::test]
async fn given_a_held_committed_op_above_the_offer_when_installing_should_refuse() {
    let (_directory, mut receiver) = transfer_receiver();
    let held = checksummed_segment_prepare(2, 0, 1, b"held");
    receiver
        .log
        .journal()
        .inner
        .append(held.into_frozen())
        .await
        .unwrap();
    receiver.consensus().advance_commit_max(3);
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: None,
        checkpoint_prepare: Vec::new(),
        next_offset: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        ..Default::default()
    };
    let refused = receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await;
    assert!(
        matches!(
            refused,
            Err(
                crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                    commit_op: 1,
                    held_max: 3,
                }
            )
        ),
        "expected a held-op refusal, got {refused:?}"
    );
}

#[compio::test]
async fn given_a_no_wal_ack_above_commit_max_when_installing_should_preserve_the_held_op() {
    const HELD_OP: u64 = 2;
    for rebuilt in [false, true] {
        let (_directory, mut receiver) = transfer_receiver();
        receiver.runtime_options.durability = iggy_common::Durability::Replicated;
        let held = checksummed_segment_prepare(HELD_OP, 0, HELD_OP - 1, b"acknowledged");
        let header = *held.header();
        receiver
            .log
            .journal()
            .inner
            .append(held.into_frozen())
            .await
            .unwrap();
        receiver.consensus.sequencer().set_sequence(HELD_OP);
        if rebuilt {
            assert!(receiver.register_rebuilt_ack(&header));
        } else {
            assert!(receiver.send_prepare_ok(&header).await);
        }
        assert_eq!(receiver.consensus.commit_max(), 0);
        let offsets = crate::state_transfer::ConsumerOffsetsWire {
            producers: Vec::new(),
            dedup_capacity: consensus::CLIENTS_TABLE_MAX,
            ..Default::default()
        };
        let result = receiver
            .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
            .await;
        assert!(
            matches!(
                result,
                Err(
                    crate::state_transfer::PartitionInstallError::OfferErasesHeldOps {
                        commit_op: 1,
                        held_max: HELD_OP
                    }
                )
            ),
            "acknowledged operation was not protected: {result:?}"
        );
        assert_eq!(
            receiver
                .log
                .journal()
                .inner
                .header_by_op(HELD_OP)
                .unwrap()
                .checksum,
            header.checksum
        );
    }
}

#[compio::test]
async fn given_an_unacknowledged_no_wal_tail_when_installing_should_allow_repair_after_transfer() {
    let (_directory, mut receiver) = transfer_receiver();
    let tail = checksummed_segment_prepare(2, 0, 1, b"unacknowledged");
    receiver
        .log
        .journal()
        .inner
        .append(tail.into_frozen())
        .await
        .unwrap();
    receiver.consensus.sequencer().set_sequence(2);
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        ..Default::default()
    };
    receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await
        .unwrap();
    assert!(receiver.log.journal().inner.is_empty());
    assert_eq!(receiver.consensus.commit_min(), 1);
}

/// An install below the head keeps the head, and an offer at or below the
/// known commit keeps the commit point, so only the journal wipe can tell a
/// suffix snapshot taken before the install that the bodies it offers are
/// gone.
#[compio::test]
async fn given_a_suffix_snapshot_when_an_install_keeps_the_head_should_invalidate_it() {
    const HEAD: u64 = 3;
    let (_directory, mut receiver) = transfer_receiver();
    receiver.consensus().sequencer().set_sequence(HEAD);
    receiver.consensus().advance_commit_max(HEAD);
    receiver
        .consensus()
        .set_local_dvc_suffix(consensus::DvcSuffix::empty());
    assert!(!receiver.consensus().local_dvc_suffix_stale());
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        ..Default::default()
    };

    let installed = receiver
        .install_state_transfer(&repair_config(), 1, Vec::new(), &offsets.encode())
        .await;

    assert!(
        installed.is_ok(),
        "the offer erases no held op, got {installed:?}"
    );
    assert_eq!(receiver.consensus().sequencer().current_sequence(), HEAD);
    assert!(
        receiver.consensus().local_dvc_suffix_stale(),
        "a suffix snapshot taken before the install still matches the log, so the \
             next DoViewChange offers bodies the install wiped"
    );
}

/// A replica that needs state transfer while producers keep writing. The
/// serving primary builds its offer at its `commit_min`. The pull takes
/// seconds for a large active segment, and meanwhile the primary's commit
/// heartbeats raise the receiver's `commit_max` past the offer. The
/// receiver's journal stopped at the gap, so its sequencer is below the
/// offer and the install erases nothing it journaled. The committed tail
/// above the offer is what journal repair fetches after the install.
///
/// Pins `install_state_transfer`, not the repair floor check. Refusing
/// such an offer is a livelock: under sustained load every offer
/// is below `commit_max` by the time it lands, every round is refused with
/// "transfer frontier X is below the local commit frontier Y", and the
/// replica keeps serving reads from its stale prefix.
#[compio::test]
async fn given_commit_max_advanced_during_the_pull_when_installing_should_land_and_leave_the_tail_to_repair()
 {
    let origin_directory = tempfile::tempdir().unwrap();
    let receiver_directory = tempfile::tempdir().unwrap();
    let (mut origin, _) = recording_partition_at(0, 3);
    origin.set_partition_dir(origin_directory.path().to_string_lossy().into_owned());
    let committed = checksummed_segment_prepare(1, 0, 0, b"committed");
    origin
        .log
        .journal()
        .inner
        .append(committed.clone().into_frozen())
        .await
        .unwrap();
    origin.consensus().sequencer().set_sequence(1);
    origin
        .consensus()
        .set_last_prepare_checksum(committed.header().checksum);
    origin.consensus().advance_commit_max(1);
    origin.consensus().advance_commit_min(1);
    origin
        .dedup_mut()
        .commit_capacity(usize::try_from(committed.header().retry_capacity).unwrap())
        .unwrap();
    origin.offset_space.committed_seeded = true;
    origin.offset.store(0, Ordering::Relaxed);
    origin.log.journal().inner.evict_prefix(1);
    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    assert_eq!(offer.commit_op, 1);

    let (mut receiver, _) = recording_partition_at(1, 3);
    receiver.set_partition_dir(receiver_directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut receiver, receiver_directory.path());
    // Learned from the primary's commit heartbeats while the pull ran. The
    // journal never got past the gap, so nothing above the offer is
    // journaled here.
    receiver.consensus().advance_commit_max(4);
    assert_eq!(receiver.consensus().sequencer().current_sequence(), 0);

    let installed = receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await;

    assert!(
        installed.is_ok(),
        "an offer that erases no journaled op must install, got {installed:?}"
    );
    assert_eq!(receiver.consensus().commit_min(), 1);
    assert_eq!(receiver.consensus().sequencer().current_sequence(), 1);
    assert_eq!(
        receiver.consensus().commit_max(),
        4,
        "the committed tail above the offer stays known for journal repair to fetch"
    );
}

/// Two batches: the first is what a lagging replica holds, the second what
/// the primary appended after it.
fn held_and_pulled_batches(held: &[u8]) -> (Vec<u8>, usize) {
    let first = checksummed_segment_prepare(1, 0, 0, held);
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"pulled");
    let mut body = first.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    let prefix_len = body.len();
    body.extend_from_slice(&second.as_slice()[size_of::<PrepareHeader>()..]);
    (body, prefix_len)
}

#[compio::test]
async fn given_a_held_prefix_when_spilling_the_artifact_should_stage_what_a_whole_pull_stages() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &body[..prefix_len]).unwrap();
    let (mut bytes, prefix) = crate::state_transfer::read_segment_prefix(
        held.to_str().unwrap(),
        artifact,
        prefix_len as u64,
        &Cell::new(false),
    )
    .await
    .unwrap();
    bytes.extend_from_slice(&body[prefix_len..]);

    let seeded = partition
        .spill_transfer_segment(&artifact, bytes, Some(prefix))
        .await
        .unwrap();
    let seeded_index = std::fs::read(&seeded.index_staging).unwrap();
    let pulled = partition
        .spill_transfer_segment(&artifact, body, None)
        .await
        .unwrap();
    assert_eq!(
        seeded, pulled,
        "the seeded spill must stage the same segment"
    );
    assert_eq!(
        seeded_index,
        std::fs::read(&pulled.index_staging).unwrap(),
        "the walk resumed after the prefix must rebuild the same index"
    );
}

#[compio::test]
async fn given_a_held_prefix_with_other_bytes_when_spilling_should_fail_the_checksum() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let (other, other_len) = held_and_pulled_batches(b"HELD");
    assert_eq!(prefix_len, other_len);
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &other[..prefix_len]).unwrap();
    let (mut bytes, prefix) = crate::state_transfer::read_segment_prefix(
        held.to_str().unwrap(),
        artifact,
        prefix_len as u64,
        &Cell::new(false),
    )
    .await
    .unwrap();
    bytes.extend_from_slice(&body[prefix_len..]);

    let spilled = partition
        .spill_transfer_segment(&artifact, bytes, Some(prefix))
        .await;
    assert!(
        matches!(
            spilled,
            Err(crate::state_transfer::SpillError::ManifestChecksum { frontier: 0 })
        ),
        "a valid prefix with other bytes must fail the manifest checksum, got {spilled:?}"
    );
}

#[compio::test]
async fn given_a_dropped_session_when_its_prefix_read_runs_should_stop_without_filling_a_buffer() {
    const REACTOR_TURNS_MAX: usize = 1024;
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    let (body, prefix_len) = held_and_pulled_batches(b"held");
    let artifact = consensus::StateArtifact::for_bytes(
        consensus::state_manifest::artifact_kind::SEGMENT_LOG,
        0,
        &body,
    );
    let held = directory.path().join("held.log");
    std::fs::write(&held, &body[..prefix_len]).unwrap();
    partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
        nonce: 1,
        peer: 0,
        commit_op: 0,
        artifacts: vec![crate::state_transfer::TransferArtifact::Pending(
            consensus::ArtifactProgress {
                entry: artifact,
                buf: Vec::new(),
            },
        )],
        target_accepted: true,
        idle_ticks: 0,
        spill: None,
        scan: None,
        seed: Some(crate::state_transfer::PrefixSeed::Planned {
            artifact: 0,
            log_path: held.to_str().unwrap().to_owned(),
            len: prefix_len as u64,
        }),
        pulled_len: 0,
    });
    assert!(
        partition.drive_prefix_seed(),
        "the pull must wait for the read"
    );
    let Some(crate::state_transfer::PrefixSeed::Reading { read, .. }) = partition
        .transfer
        .as_ref()
        .and_then(|session| session.seed.as_ref())
    else {
        panic!("the read must start");
    };
    let landed = Rc::clone(&read.result);

    partition.transfer = None;
    for _ in 0..REACTOR_TURNS_MAX {
        if landed.borrow().is_some() {
            break;
        }
        server_common::yield_to_reactor().await;
    }

    let read = landed.borrow();
    assert!(
        matches!(
            read.as_ref(),
            Some(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted
        ),
        "a read whose session dropped must stop before it fills a buffer, got {read:?}"
    );
}

/// A backup that fetches one segment artifact, whose prefix read landed.
fn fetching_with_a_landed_prefix_read() -> IggyPartition<RecordingBus> {
    let (mut partition, _) = recording_partition_at(1, 3);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::AwaitingTarget);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Fetching);
    partition.transfer = Some(crate::state_transfer::PartitionTransferSession {
        nonce: 1,
        peer: 0,
        commit_op: 0,
        artifacts: vec![crate::state_transfer::TransferArtifact::Pending(
            consensus::ArtifactProgress {
                entry: consensus::StateArtifact::for_bytes(
                    consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                    0,
                    b"segment",
                ),
                buf: Vec::new(),
            },
        )],
        target_accepted: true,
        idle_ticks: 0,
        spill: None,
        scan: None,
        seed: Some(crate::state_transfer::PrefixSeed::Reading {
            artifact: 0,
            read: crate::state_transfer::LocalWork {
                result: Rc::new(RefCell::new(Some(Err(std::io::Error::other(
                    "the read failed",
                ))))),
                ..crate::state_transfer::LocalWork::default()
            },
        }),
        pulled_len: 0,
    });
    partition
}

#[compio::test]
async fn given_a_landed_prefix_read_when_resuming_io_should_hand_the_pull_its_turn() {
    let mut partition = fetching_with_a_landed_prefix_read();

    let step = partition.resume_io(&repair_config()).await;

    assert!(
        matches!(step, crate::PartitionIoStep::TransferReady),
        "a landed read must resume the pull before its stall sweep"
    );
}

#[compio::test]
async fn transfer_establishes_wal_body_ownership_without_losing_the_active_index_writer() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        for materialized in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (mut partition, _) = recording_partition_at(1, 3);
            let partition_dir = directory.path().to_string_lossy().into_owned();
            partition.set_partition_dir(partition_dir.clone());
            partition.runtime_options.durability = durability;
            partition.runtime_options.consumer_offset_durability =
                iggy_common::Durability::Persisted;
            set_offset_dirs_under(&mut partition, directory.path());
            crate::state_transfer::mark_materialization_missing(&partition_dir, 0)
                .await
                .unwrap();
            partition.open_persistence().await.unwrap();
            assert!(
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .segment_checkpoint()
                    .is_none()
            );
            assert!(!partition.consensus().recovery_election_allowed());
            assert!(!partition.consensus().recovered_log_complete());
            let prepare = checksummed_segment_prepare(1, 0, 0, b"transferred");
            let mut staged = Vec::new();
            if materialized {
                let body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
                let artifact = consensus::StateArtifact::for_bytes(
                    consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                    0,
                    &body,
                );
                staged.push(
                    partition
                        .spill_transfer_segment(&artifact, body, None)
                        .await
                        .unwrap(),
                );
            }
            let offsets = crate::state_transfer::ConsumerOffsetsWire {
                producers: Vec::new(),
                owner_retirement: None,
                owners: Vec::new(),
                history_transition: None,
                dedup_capacity: consensus::CLIENTS_TABLE_MAX,
                required_metadata_frontier: 0,
                prepare_checksum: Some(prepare.header().checksum),
                checkpoint_prepare: prepare.as_slice().to_vec(),
                next_offset: 1,
                emptied_at: 0,
                consumers: Vec::new(),
                groups: Vec::new(),
                external_groups: Vec::new(),
                dedup: Vec::new(),
                retirement_paid_through: 0,
                machines: None,
            };
            partition
                .install_state_transfer(&repair_config(), 1, staged, &offsets.encode())
                .await
                .unwrap();
            assert!(
                partition
                    .persistence
                    .as_ref()
                    .unwrap()
                    .segment_checkpoint()
                    .is_some()
            );
            assert!(
                partition
                    .log
                    .storages()
                    .iter()
                    .all(|storage| storage.messages_size.is_none())
            );
            assert!(partition.log.messages_writers().iter().all(Option::is_none));
            assert!(partition.log.index_writers().last().unwrap().is_some());
            assert_eq!(partition.consensus().commit_min(), 1);
            assert!(!partition.requires_state_transfer());
            assert!(
                partition.consensus().recovery_election_allowed(),
                "{durability:?}: the installed copy may vote and lead again"
            );
            assert_eq!(
                partition.consensus().recovered_log_complete(),
                durability.is_persisted()
            );
        }
    }
}

#[compio::test]
async fn given_commit_min_above_the_wal_head_when_checkpointing_should_not_report_one_due() {
    const WAL_HEAD: u64 = 1;
    for commit_min in [WAL_HEAD, WAL_HEAD + 1] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.open_persistence().await.unwrap();
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        let prepare = checksummed_segment_prepare(WAL_HEAD, 0, 0, b"durable");
        persistence.append(prepare.into_frozen(), true).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition
            .consensus
            .restore_commit_state(commit_min, commit_min);
        persistence.request_checkpoint();

        let due = partition.needs_persistence_checkpoint();
        partition.checkpoint_persistence(&repair_config()).await;

        assert!(partition.fatal().is_none(), "{:?}", partition.fatal());
        let started = partition.maintenance.checkpoint.is_some();
        assert_eq!(
            due, started,
            "commit_min {commit_min}: the tick starts a checkpoint whenever one is due"
        );
        assert_eq!(
            started,
            commit_min <= WAL_HEAD,
            "commit_min {commit_min}: receipts at commit_min need the WAL to hold it"
        );
    }
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn checkpoint_index_sync_failure_fences_before_reclaiming_wal_history() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, _) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let prepare = checksummed_segment_prepare(1, 0, 0, b"durable");
    persistence.append(prepare.into_frozen(), true).unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    assert!(persistence.is_durable_through(1));
    partition.consensus.restore_commit_state(1, 1);
    let writer = IggyIndexWriter::new("/dev/null", Rc::new(AtomicU64::new(0)), true, false)
        .await
        .unwrap();
    assert_eq!(
        writer
            .save_indexes_buffered_at(vec![1; 32], 0)
            .await
            .unwrap(),
        32
    );
    let active = partition.log.index_writers().len() - 1;
    partition.log.index_writers_mut()[active] = Some(Rc::new(writer));
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&repair_config()).await;
    assert!(partition.fatal().is_some());
    assert!(!persistence.checkpoint_pending());
    assert!(persistence.is_durable_through(1));
}

#[compio::test]
async fn pending_wal_prefix_keeps_pipeline_replies_and_does_not_partially_flush() {
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = durability;
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
        let segment_size =
            IggyByteSize::from((first.as_slice().len() - size_of::<PrepareHeader>()) as u64);
        partition.runtime_options.segment_size = Some(segment_size);
        partition.log.active_segment_mut().max_size = segment_size;
        partition.open_persistence().await.unwrap();
        partition.log.retire_front().unwrap();
        partition
            .install_empty_segment(&repair_config(), 0)
            .await
            .unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
        persistence
            .append(first.clone().into_frozen(), durability.is_persisted())
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        persistence
            .append(second.clone().into_frozen(), durability.is_persisted())
            .unwrap();
        assert!(persistence.start());
        let writer = Rc::clone(&persistence).run();
        for prepare in [first, second] {
            partition.consensus.with_pipeline_mut(|pipeline| {
                pipeline.push(PipelineEntry::new(*prepare.header()));
            });
            partition
                .append_repaired_send_messages(prepare)
                .await
                .unwrap();
        }
        partition.consensus.restore_commit_state(0, 2);
        let config = repair_config();
        {
            let mut flush = Box::pin(partition.commit_messages_inner(&config, true, 2));
            assert!(matches!(
                futures::poll!(&mut flush),
                std::task::Poll::Ready(Ok(false))
            ));
        }
        assert_eq!(partition.log.journal().inner.resident_count(), 2);
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
        assert_eq!(partition.stats.messages_count_inconsistent(), 0);
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 2);
        assert_eq!(replies.borrow().len(), 1);
        writer.await;
        partition.commit_journal(&config).await;
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 2);
        assert_eq!(partition.consensus.pipeline_len(), 0);
        assert_eq!(partition.stats.messages_count_inconsistent(), 2);
        assert_eq!(replies.borrow().len(), 2);
        let consensus::client_table::RequestStatus::Duplicate(cached) = partition
            .dedup
            .check_partition_request(1, 0, 1, 2, Operation::SendMessages)
        else {
            panic!("the committed reply must remain replayable");
        };
        assert!(
            replies.borrow()[1]
                .1
                .shares_allocation(&cached.into_wire_bytes())
        );
    }
}

#[compio::test]
async fn given_a_replicated_singleton_when_a_flush_write_completes_should_reply_without_a_tick() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 1);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    partition.open_persistence().await.unwrap();
    let config = repair_config();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let completion = Rc::new(Cell::new(None));
    let captured = Rc::clone(&completion);
    persistence.set_notifier(Rc::new(move |value| captured.set(Some(value))));

    // Hold the writer until the self-ack has reached the physical-write gate.
    persistence.sync();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    let mut request = checksumless_send_request(partition.namespace(), 2);
    let body = &mut request.as_mut_slice()[size_of::<RoutedRequestHeader>()..];
    let mut batch = BatchHeader::decode(body).unwrap();
    batch.batch_checksum = batch.checksum_for_blob(&body[COMMAND_HEADER_SIZE..]);
    batch.encode_into(&mut body[..COMMAND_HEADER_SIZE]);
    partition.on_request(request.clone(), None).await;
    assert_eq!(
        partition.consensus.pipeline_len(),
        1,
        "the send must be admitted: {:?}",
        replies
            .borrow()
            .iter()
            .map(|(_, reply)| bytemuck::checked::from_bytes::<ReplyHeader>(
                &reply.as_slice()[..size_of::<ReplyHeader>()]
            )
            .status)
            .collect::<Vec<_>>()
    );
    commit_recorded_loopback(&mut partition).await;
    let header = partition.consensus.pipeline_head_header().unwrap();
    assert_eq!(partition.consensus.commit_max(), header.op);
    assert_eq!(partition.consensus.commit_min(), 0);
    assert!(!persistence.is_written(&header));
    assert!(partition.pending_persisted_acks.borrow().is_empty());
    assert!(replies.borrow().is_empty());

    writer.await;
    assert!(persistence.failure().is_none());
    assert!(persistence.is_written(&header));
    assert_eq!(persistence.durable_op(), 0);
    assert!(replies.borrow().is_empty());
    let completed = completion
        .get()
        .expect("the buffered write must notify its owner");
    for stale in [
        PersistenceCompletion {
            instance: completed.instance + 1,
            ..completed
        },
        PersistenceCompletion {
            epoch: completed.epoch + 1,
            ..completed
        },
    ] {
        partition.on_persistence_completed(stale, &config).await;
        assert!(replies.borrow().is_empty());
        assert_eq!(partition.consensus.commit_min(), 0);
    }

    partition.materialization_missing = true;
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition.materialization_missing = false;

    partition.consensus().begin_state_transfer_await();
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition
        .consensus()
        .set_state_transfer_stage(consensus::StateTransferStage::Idle);

    partition.consensus().begin_view_probe();
    partition.on_persistence_completed(completed, &config).await;
    assert!(replies.borrow().is_empty());
    assert_eq!(partition.consensus.commit_min(), 0);
    partition.consensus().init();
    assert_eq!(partition.consensus.commit_max(), header.op);
    assert_eq!(partition.consensus.pipeline_len(), 1);

    partition.on_persistence_completed(completed, &config).await;

    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.consensus.commit_min(),
        header.op,
        "a completed flush must release the self-acked reply without a consensus tick"
    );
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(replies.borrow().len(), 1);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    let consensus::client_table::RequestStatus::Duplicate(cached) = partition
        .dedup
        .check_partition_request(1, 0, 1, 1, Operation::SendMessages)
    else {
        panic!("the completed send must retain its original receipt");
    };
    assert!(
        replies.borrow()[0]
            .1
            .shares_allocation(&cached.into_wire_bytes())
    );

    partition.on_persistence_completed(completed, &config).await;
    assert_eq!(replies.borrow().len(), 1);
    partition.on_request(request, None).await;
    assert_eq!(replies.borrow().len(), 2);
    assert!(
        replies.borrow()[0]
            .1
            .shares_allocation(&replies.borrow()[1].1)
    );
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    assert_eq!(persistence.head(), header.op);
}

#[compio::test]
async fn mixed_durability_replies_before_body_writes_below_the_flush_threshold() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
    partition.open_persistence().await.unwrap();
    let mut config = repair_config();
    config.messages_required_to_save = 2;
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    persistence
        .append(first.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    for (index, prepare) in [first, second].into_iter().enumerate() {
        let header = *prepare.header();
        if index > 0 {
            persistence
                .append(prepare.clone().into_frozen(), false)
                .unwrap();
        }
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(header));
        });
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
        partition.consensus.advance_commit_max(header.op);
        if index == 0 {
            let write_lock = partition.write_lock.clone();
            let _guard = write_lock.lock().await;
            let mut commit = Box::pin(partition.commit_journal(&config));
            assert!(
                futures::poll!(&mut commit).is_ready(),
                "a below-threshold reply must not acquire a later materialization requirement while waiting for the append lock"
            );
        } else {
            partition.commit_journal(&config).await;
        }
        assert!(partition.fatal().is_none());
        assert_eq!(partition.consensus.commit_min(), 1);
        assert_eq!(
            replies.borrow().len(),
            1,
            "only the below-threshold send can reply"
        );
        assert_eq!(partition.log.active_segment().size.as_bytes_u64(), 0);
        assert_eq!(partition.log.journal().inner.resident_count(), index + 1);
        assert!(!persistence.is_written(&header));
    }
    assert_eq!(partition.consensus.pipeline_head_header().unwrap().op, 2);
    {
        let mut flush = Box::pin(partition.commit_messages_inner(&config, true, 2));
        assert!(matches!(
            futures::poll!(&mut flush),
            std::task::Poll::Ready(Ok(false))
        ));
    }
    writer.await;
    assert_eq!(persistence.durable_op(), 0);
    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(partition.consensus.pipeline_len(), 0);
    assert_eq!(replies.borrow().len(), 2);
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
}

/// Under a dispatcher a Replicated reply needs neither its body write nor
/// the flush, and the flush that follows takes only written bodies.
#[compio::test]
async fn dispatched_replicated_send_replies_before_its_write_and_flushes_written_bodies() {
    let directory = tempfile::tempdir().unwrap();
    let (mut partition, replies) = recording_partition_at(0, 3);
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.open_persistence().await.unwrap();
    let mut config = repair_config();
    config.messages_required_to_save = 1;
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let second = checksummed_segment_prepare(2, first.header().checksum, 1, b"second");
    persistence
        .append(first.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    persistence
        .append(second.clone().into_frozen(), false)
        .unwrap();
    assert!(persistence.start());
    let writer = Rc::clone(&persistence).run();
    for prepare in [first, second] {
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(*prepare.header()));
        });
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
    }
    partition.consensus.advance_commit_max(2);

    partition.commit_journal(&config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        replies.borrow().len(),
        2,
        "a Replicated reply must not wait for its body write"
    );
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);

    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        1,
        "the flush must leave the unwritten body resident"
    );
    assert_eq!(
        partition.offset.load(Ordering::Acquire),
        1,
        "a flush behind the commits must not hide them from polls"
    );
    assert_eq!(partition.stats.current_offset(), 1);

    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    assert!(!partition.history_writes_pending());
    assert_eq!(partition.offset.load(Ordering::Acquire), 1);
    assert_eq!(partition.stats.messages_count_inconsistent(), 2);
}

/// Under sustained produce load every lane turn sees a busy WAL writer.
/// Retention of a sealed segment that a published checkpoint covers must
/// not wait for that writer.
#[compio::test]
async fn given_busy_wal_writer_when_retaining_a_checkpointed_segment_should_not_wait() {
    const RECORDS_PER_SEGMENT: u64 = 2;

    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let record_len = checksummed_segment_prepare(1, 0, 0, b"payload")
        .as_slice()
        .len()
        - size_of::<PrepareHeader>();
    let segment_size = IggyByteSize::from(RECORDS_PER_SEGMENT * record_len as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    let layout = |partition: &IggyPartition<RecordingBus>| {
        partition
            .log
            .segments()
            .iter()
            .map(|segment| (segment.start_offset, segment.end_offset, segment.sealed))
            .collect::<Vec<_>>()
    };

    // Sustained produce load: a new append restarts the writer before
    // each lane turn, so no turn sees an idle writer.
    let mut parent = 0;
    let mut writer = hold_wal_writer(&persistence);
    for offset in 0..=RECORDS_PER_SEGMENT {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, b"payload");
        parent = prepare.header().checksum;
        partition.on_replicate(prepare).await;
        writer.await;
        writer = hold_wal_writer(&persistence);
        commit_on_heartbeat(&mut partition, &config, op).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(layout(&partition), vec![(0, 1, true), (2, 2, false)]);

    // A due checkpoint, as the shard tick starts it. The byte trigger needs
    // half of a WAL budget of at least 128 MiB, so the test requests it.
    persistence.request_checkpoint();
    assert!(partition.needs_persistence_checkpoint());
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;

    // The load stops: the writer completes the checkpoint and goes idle.
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert!(
        persistence
            .segment_checkpoint()
            .is_some_and(|checkpoint| checkpoint.next_offset > RECORDS_PER_SEGMENT - 1),
        "the checkpoint covers the sealed segment"
    );
    assert_eq!(layout(&partition), vec![(0, 1, true), (2, 2, false)]);
    assert!(directory.join("00000000000000000000.log").is_file());
    assert_eq!(
        partition.stats.messages_count_inconsistent(),
        RECORDS_PER_SEGMENT + 1
    );
    let writer = hold_wal_writer(&persistence);
    assert!(!persistence.is_quiescent());
    partition
        .remove_sealed_segments_up_to(RECORDS_PER_SEGMENT - 1)
        .await;
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("a busy writer must not block retention of a checkpointed sealed segment");
    };
    assert_eq!(plan.continuation, crate::PartitionIoContinuation::Retention);
    execute_partition_io_plan(&mut partition, &config, plan).await;
    assert_eq!(layout(&partition), vec![(2, 2, false)]);
    writer.await;
}

/// Past the byte cap the evicted ring releases the bodies that the WAL
/// wrote to segments and keeps their headers. Each body reads back from
/// its segment as the replicated prepare, after the WAL checkpoint too:
/// ops 1 and 2 in one read from the first segment, op 3 from the second.
/// Retention of a segment forgets the ops in it.
#[compio::test]
async fn given_bodies_past_the_ring_byte_cap_when_read_back_should_return_the_replicated_prepares()
{
    const RECORDS_PER_SEGMENT: u64 = 2;
    const OPS: u64 = 4;
    const PAYLOAD: [u8; 4096] = [7; 4096];

    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let (mut partition, _) = recording_partition_at(1, 3);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Replicated;
    partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Replicated;
    let prepare_len = checksummed_segment_prepare(1, 0, 0, &PAYLOAD)
        .as_slice()
        .len();
    let segment_size =
        IggyByteSize::from(RECORDS_PER_SEGMENT * (prepare_len - size_of::<PrepareHeader>()) as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition.install_empty_segment(&config, 0).await.unwrap();
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    // Room for one body, but not for two.
    partition
        .log
        .journal()
        .inner
        .set_ring_caps(crate::EVICTED_RING_CAPACITY, (prepare_len * 3 / 2) as u64);
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

    let mut prepares = Vec::new();
    let mut parent = 0;
    let mut writer = hold_wal_writer(&persistence);
    for offset in 0..OPS {
        let op = offset + 1;
        let prepare = checksummed_segment_prepare(op, parent, offset, &PAYLOAD);
        parent = prepare.header().checksum;
        prepares.push(prepare.clone());
        partition.on_replicate(prepare).await;
        writer.await;
        writer = hold_wal_writer(&persistence);
        commit_on_heartbeat(&mut partition, &config, op).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(
        partition.log.journal().inner.repair_retained_from(),
        Some(1),
        "repair serves every committed op"
    );

    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    settle_partition_io(&mut partition, &config).await;
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    let released: Vec<_> = prepares[..prepares.len() - 1]
        .iter()
        .map(|prepare| {
            partition
                .log
                .journal()
                .inner
                .evicted_header(prepare.header().op)
                .expect("the byte cap released this body")
        })
        .collect();
    let mut bodies = crate::ReleasedRead::new(&partition.log, released.clone())
        .read()
        .await;
    for (evicted, prepare) in released.iter().zip(&prepares) {
        let body = bodies
            .take(evicted)
            .expect("the read brings every body back");
        assert_eq!(body.as_slice(), prepare.as_slice());
    }

    partition
        .remove_sealed_segments_up_to(RECORDS_PER_SEGMENT - 1)
        .await;
    let crate::PartitionIoStep::Ready(plan) = partition.resume_io(&config).await else {
        panic!("retention of the checkpointed segment is due");
    };
    execute_partition_io_plan(&mut partition, &config, plan).await;
    assert_eq!(
        partition.log.journal().inner.repair_retained_from(),
        Some(RECORDS_PER_SEGMENT + 1),
        "repair answers RangeEvicted for the ops of the removed segment"
    );
}

#[compio::test]
async fn given_small_written_prefix_when_flush_is_due_should_wait_without_rebuilding_the_tail() {
    const THROUGH: u64 = 9;
    const WRITTEN: u64 = 3;
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for op in 2..=THROUGH {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"queued");
        parent = prepare.header().checksum;
        persistence
            .append(prepare.clone().into_frozen(), false)
            .unwrap();
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
        if op == WRITTEN {
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
        }
    }
    let writer = hold_wal_writer(&persistence);
    partition.consensus.advance_commit_max(THROUGH);
    for op in 2..=THROUGH {
        partition.consensus.advance_commit_min(op);
    }
    partition.consensus.sequencer().set_sequence(THROUGH);
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.defer_flush(THROUGH);
    partition.resume_io(&config).await;
    assert!(
        partition.materialization.is_none(),
        "two written entries must not rebuild six retained entries"
    );
    assert_eq!(
        partition.log.journal().inner.resident_count(),
        usize::try_from(THROUGH - 1).unwrap()
    );
    writer.await;
    settle_partition_io(&mut partition, &config).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
}

#[compio::test]
async fn given_a_discarded_suffix_with_an_offset_gap_when_truncated_should_reuse_the_retained_frontier()
 {
    let mut partition = partition_at_view(0, 0);
    let retained = checksummed_segment_prepare(1, 0, 0, b"retained");
    let parent = retained.header().checksum;
    partition
        .append_repaired_send_messages(retained)
        .await
        .unwrap();
    let discarded = checksummed_segment_prepare(3, parent, 2, b"discarded");
    partition
        .append_repaired_send_messages(discarded)
        .await
        .unwrap();
    partition.consensus.sequencer().set_sequence(3);

    partition.truncate_uncommitted_from(2).await.unwrap();

    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 0);
    let replacement = checksummed_segment_prepare(2, parent, 1, b"replacement");
    partition
        .append_received_send_messages_to_journal(replacement)
        .await
        .unwrap();
    assert_eq!(partition.dirty_offset.load(Ordering::Relaxed), 1);
    assert!(partition.log.journal().inner.holds_op(1));
    assert!(partition.log.journal().inner.holds_op(2));
    assert!(!partition.log.journal().inner.holds_op(3));
}

#[compio::test]
async fn live_wal_rollback_preserves_replacement_indexes_and_pollable_bodies() {
    let root = tempfile::tempdir().unwrap();
    let mut config = repair_config();
    config.path_layout.streams_root = root.path().to_string_lossy().into_owned();
    let directory = std::path::PathBuf::from(config.get_partition_path(1, 1, 0));
    std::fs::create_dir_all(&directory).unwrap();
    let mut partition = partition_at_view(0, 0);
    partition.set_partition_dir(directory.to_string_lossy().into_owned());
    partition.runtime_options.durability = iggy_common::Durability::Persisted;
    let first = checksummed_segment_prepare(1, 0, 0, b"first");
    let parent = first.header().checksum;
    let segment_size =
        IggyByteSize::from((first.as_slice().len() - size_of::<PrepareHeader>()) as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.open_persistence().await.unwrap();
    partition.log.retire_front().unwrap();
    partition
        .install_empty_segment(&repair_config(), 0)
        .await
        .unwrap();
    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    persistence
        .append(first.clone().into_frozen(), true)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition
        .append_repaired_send_messages(first)
        .await
        .unwrap();
    partition.consensus.restore_commit_state(0, 1);
    partition.commit_journal(&repair_config()).await;
    let old = checksummed_segment_prepare(2, parent, 1, b"discarded");
    persistence.append(old.clone().into_frozen(), true).unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition.append_repaired_send_messages(old).await.unwrap();
    partition.truncate_uncommitted_from(2).await.unwrap();
    let replacement = checksummed_segment_prepare(2, parent, 1, b"replacement");
    let expected = replacement.as_slice()[size_of::<PrepareHeader>()..].to_vec();
    persistence
        .append(replacement.clone().into_frozen(), true)
        .unwrap();
    assert!(persistence.start());
    Rc::clone(&persistence).run().await;
    partition
        .append_repaired_send_messages(replacement)
        .await
        .unwrap();
    partition.consensus.advance_commit_max(2);
    partition.commit_journal(&repair_config()).await;
    assert!(partition.fatal().is_none());
    assert_eq!(partition.consensus.commit_min(), 2);
    assert_eq!(
        std::fs::read(directory.join("00000000000000000001.log")).unwrap(),
        expected
    );
    assert_eq!(
        std::fs::metadata(directory.join("00000000000000000001.index"))
            .unwrap()
            .len(),
        IGGY_INDEX_SIZE as u64
    );
    assert_eq!(partition.log.journal().inner.resident_count(), 0);
    let args = PollingArgs::new(iggy_common::PollingStrategy::offset(1), 1, false);
    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, true)
        .execute()
        .await;
    let completion = partition.complete_poll(result).unwrap();
    let polled: Vec<_> = completion
        .fragments
        .iter()
        .flat_map(|fragment| fragment.as_slice().iter().copied())
        .collect();
    let batch = decode_batch_slice(&polled).unwrap();
    assert_eq!(batch.header.base_offset, 1);
    assert_eq!(batch.message_count(), 1);
    assert_eq!(batch.iter().next().unwrap().payload, b"replacement");
    persistence.request_checkpoint();
    partition.checkpoint_persistence(&config).await;
    persistence
        .drain_with_timeout(compio::runtime::time::sleep)
        .await
        .unwrap();
    assert!(persistence.failure().is_none());
    assert_eq!(persistence.checkpoint_op(), 2);
}
