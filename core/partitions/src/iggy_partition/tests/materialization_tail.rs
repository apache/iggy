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
async fn given_large_saved_appends_when_accepted_should_keep_the_checksum_of_the_written_bytes() {
    const PAYLOAD_BYTES: usize = 12 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::with_replicas(log_path.to_str().unwrap(), index_path.to_str().unwrap(), 3)
            .await;
    fixture
        .partition
        .set_partition_dir(directory.path().to_str().unwrap().to_owned());
    let payload = vec![0x5a; PAYLOAD_BYTES];
    for offset in 0..2 {
        let job = fixture.capture(&payload, offset).unwrap();
        assert!(
            fixture.partition.segment_checksum_cache.borrow().is_empty(),
            "the in-flight append owns its hasher"
        );
        let result = job.execute().await;
        assert!(
            fixture.partition.segment_checksum_cache.borrow().is_empty(),
            "file execution cannot publish a stamp before acceptance"
        );
        fixture.partition.accept_materialization(result).unwrap();
        let written = std::fs::read(&log_path).unwrap();
        let memos = fixture.partition.segment_checksum_cache.borrow();
        assert_eq!(
            memos
                .get(&0)
                .and_then(|memo| memo.checksum_of(written.len() as u64)),
            Some(consensus::state_artifact_checksum(&written)),
            "append {offset} must hash exactly the bytes accepted on disk"
        );
    }
}

#[compio::test]
async fn reference_wal_backlog_keeps_disk_poll_seek_bounded() {
    const RECORDS: u64 = 8;
    const PAYLOAD_BYTES: usize = 512 * 1024;
    const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        for (span, expected_indexes) in [
            (iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE, 4),
            (
                3 * iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE,
                2,
            ),
            (
                iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE / 4,
                8,
            ),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let namespace = IggyNamespace::new(1, 1, 0);
            let mut config = repair_config();
            config.size_of_messages_required_to_save = IggyByteSize::from(span);
            config.path_layout.streams_root = directory
                .path()
                .join("streams")
                .to_string_lossy()
                .into_owned();
            let partition_path = config.get_partition_path(1, 1, 0);
            std::fs::create_dir_all(&partition_path).unwrap();
            let (mut partition, _) = recording_partition_at(0, 1);
            set_offset_dirs_under(&mut partition, std::path::Path::new(&partition_path));
            partition.set_partition_dir(partition_path);
            partition.runtime_options.durability = durability;
            partition.runtime_options.segment_size = Some(IggyByteSize::from(SEGMENT_BYTES));
            partition.log.active_segment_mut().max_size = IggyByteSize::from(SEGMENT_BYTES);
            partition.open_persistence().await.unwrap();
            partition.log.retire_front().unwrap();
            partition.install_empty_segment(&config, 0).await.unwrap();
            let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
            assert!(persistence.segment_checkpoint().is_some());
            let mut parent = 0;
            let mut expected = Vec::new();
            let mut last_position = 0;
            for ordinal in 0..RECORDS {
                let payload = vec![u8::try_from(ordinal).unwrap(); PAYLOAD_BYTES];
                let prepare = checksummed_segment_prepare(ordinal + 1, parent, ordinal, &payload);
                parent = prepare.header().checksum;
                last_position = expected.len() as u64;
                expected.extend_from_slice(&prepare.as_slice()[size_of::<PrepareHeader>()..]);
                persistence
                    .append(prepare.clone().into_frozen(), durability.is_persisted())
                    .unwrap();
                partition
                    .append_repaired_send_messages(prepare)
                    .await
                    .unwrap();
            }
            assert!(persistence.start());
            Rc::clone(&persistence).run().await;
            partition.consensus.restore_commit_state(RECORDS, RECORDS);
            partition.consensus.sequencer().set_sequence(RECORDS);
            assert!(
                partition
                    .commit_messages_inner(&config, true, RECORDS)
                    .await
                    .unwrap()
            );
            assert_eq!(
                std::fs::read(config.get_messages_path(1, 1, 0, 0)).unwrap(),
                expected
            );
            assert_eq!(
                file_len(std::path::Path::new(&config.get_index_path(1, 1, 0, 0)))
                    / crate::iggy_index::IGGY_INDEX_SIZE as u64,
                expected_indexes
            );
            let args =
                PollingArgs::new(iggy_common::PollingStrategy::offset(RECORDS - 1), 1, false);
            let plan = partition.build_poll_plan(PollingConsumer::Consumer(1, 0), &args, true);
            let PollTier::Disk { disk, .. } = &plan.tier else {
                panic!("the flushed backlog must be polled from disk");
            };
            assert!(
                last_position.saturating_sub(disk.start_position) <= span,
                "the last poll must not rescan the whole coalesced WAL backlog: {} bytes skipped",
                last_position.saturating_sub(disk.start_position)
            );
            let result = plan.execute().await;
            assert_eq!(result.last_matching_offset, Some(RECORDS - 1));
            let completion = partition.complete_poll(result).unwrap();
            let polled: Vec<_> = completion
                .fragments
                .iter()
                .flat_map(|fragment| fragment.as_slice().iter().copied())
                .collect();
            assert_eq!(polled, expected[usize::try_from(last_position).unwrap()..]);
            persistence.request_checkpoint();
            partition.checkpoint_persistence(&config).await;
            persistence.drain().await.unwrap();
            assert_eq!(persistence.checkpoint_op(), RECORDS);
            let checkpoint = persistence.segment_checkpoint();
            drop(partition);
            drop(persistence);
            let recovered = crate::segment_recovery::load_persisted_segments_with_checkpoint(
                &config,
                namespace,
                IggyByteSize::from(SEGMENT_BYTES),
                durability.is_persisted(),
                &PartitionStats::default(),
                checkpoint,
            )
            .await
            .unwrap()
            .segments;
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].segment.end_offset, RECORDS - 1);
            assert_eq!(
                recovered[0].segment.size.as_bytes_u64(),
                expected.len() as u64
            );
            assert_eq!(
                std::fs::read(config.get_messages_path(1, 1, 0, 0)).unwrap(),
                expected
            );
        }
    }
}

#[compio::test]
async fn given_bounded_save_when_pieces_settle_should_publish_one_index_and_advance_together() {
    const RECORDS: u64 = 3;
    const PAYLOAD_BYTES: usize = 512;
    for split in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let log_path = directory.path().join("segment.log");
        let index_path = directory.path().join("segment.index");
        let mut fixture =
            PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
        let mut expected = Vec::new();
        let mut parent = 0;
        let mut single_charge = 0;
        for ordinal in 0..RECORDS {
            let payload = vec![u8::try_from(ordinal).unwrap(); PAYLOAD_BYTES];
            let prepare = checksummed_segment_prepare(ordinal + 1, parent, ordinal, &payload);
            parent = prepare.header().checksum;
            let frozen = prepare.into_frozen();
            expected.extend_from_slice(&frozen.as_slice()[size_of::<PrepareHeader>()..]);
            single_charge = crate::io::materialization_allocation_charge(
                std::slice::from_ref(&frozen),
                1,
                crate::iggy_index::IGGY_INDEX_SIZE,
            )
            .unwrap();
            fixture
                .partition
                .log
                .journal()
                .inner
                .append(frozen)
                .await
                .unwrap();
        }
        let bytes_max = if split {
            single_charge
        } else {
            single_charge * usize::try_from(RECORDS).unwrap()
        };
        fixture.partition.io_bytes_max.set(bytes_max);
        fixture.partition.consensus.advance_commit_max(RECORDS);
        assert!(
            fixture
                .partition
                .begin_materialization(
                    RECORDS,
                    iggy_common::DEFAULT_SIZE_OF_MESSAGES_REQUIRED_TO_SAVE
                )
                .unwrap()
        );
        let mut jobs = 0;
        loop {
            match fixture.partition.drive_materialization().unwrap() {
                MaterializationStep::Progress => {}
                MaterializationStep::Pending => panic!("no WAL or task can block this fixture"),
                MaterializationStep::Complete => break,
                MaterializationStep::Ready => {
                    let job = fixture
                        .partition
                        .capture_materialization(&repair_config())
                        .unwrap()
                        .unwrap();
                    let crate::PartitionIoJob::Materialize(save) = &job else {
                        panic!("segment has room for every record");
                    };
                    assert!(save.allocation_charge() <= bytes_max);
                    let completes = save.completes;
                    assert_eq!(save.indexes.is_empty(), !completes);
                    assert_eq!(
                        fixture.cursors(),
                        (0, 0),
                        "a partial save cannot expose either cursor"
                    );
                    let result = job.execute().await;
                    assert_eq!(
                        fixture.cursors(),
                        (0, 0),
                        "execution cannot publish progress"
                    );
                    fixture
                        .partition
                        .accept_materialization_phase(result)
                        .unwrap();
                    jobs += 1;
                    if !completes {
                        assert_eq!(fixture.cursors(), (0, 0));
                        assert_eq!(fixture.segment_size(), 0);
                        assert_eq!(file_len(&index_path), 0);
                        assert_eq!(
                            fixture
                                .partition
                                .log
                                .journal()
                                .inner
                                .with_entries(VecDeque::len),
                            usize::try_from(RECORDS).unwrap()
                        );
                    }
                }
            }
        }
        assert_eq!(jobs, if split { RECORDS } else { 1 });
        assert_eq!(
            fixture.cursors(),
            (
                expected.len() as u64,
                crate::iggy_index::IGGY_INDEX_SIZE as u64
            )
        );
        assert_eq!(std::fs::read(&log_path).unwrap(), expected);
        assert_eq!(
            file_len(&index_path),
            crate::iggy_index::IGGY_INDEX_SIZE as u64
        );
        assert_eq!(
            fixture.partition.log.active_segment().end_offset,
            RECORDS - 1
        );
        assert!(fixture.partition.log.journal().inner.is_empty());
    }
}

#[compio::test]
async fn given_a_fenced_window_when_flushing_should_write_nothing_above_commit_min() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.log.journal().inner.set_repair_retention(true);
    partition.repair = Some(armed_session(3, 0));
    for op in 1..=3 {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(3);
    partition.fence_view_window(3, None);

    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush");
    assert!(
        std::fs::read(&log_path).unwrap().is_empty(),
        "a forced flush wrote unproven entries to the segment"
    );
    assert!((1..=3).all(|op| partition.log.journal().inner.holds_op(op)));
}

#[compio::test]
async fn owned_materialization_keeps_physical_cursors_private_until_acceptance() {
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
    let job = fixture.capture(FIRST_PAYLOAD, 0).unwrap();
    let result = job.execute().await;
    assert_eq!(std::fs::read(&log_path).unwrap(), FIRST_PAYLOAD);
    assert_eq!(file_len(&index_path), IGGY_INDEX_SIZE as u64);
    assert_eq!(
        fixture.cursors(),
        (0, 0),
        "file completion is not owner acceptance"
    );
    assert_eq!(fixture.segment_size(), 0);
    fixture.partition.accept_materialization(result).unwrap();
    assert_eq!(
        fixture.cursors(),
        (FIRST_PAYLOAD.len() as u64, IGGY_INDEX_SIZE as u64)
    );
    assert_eq!(fixture.segment_size(), FIRST_PAYLOAD.len() as u64);
}

#[compio::test]
async fn completed_materialization_cannot_advance_a_recreated_writer_at_the_same_path() {
    let directory = tempfile::tempdir().unwrap();
    let log_path = directory.path().join("segment.log");
    let index_path = directory.path().join("segment.index");
    let mut fixture =
        PersistFixture::new(log_path.to_str().unwrap(), index_path.to_str().unwrap()).await;
    let result = fixture.capture(FIRST_PAYLOAD, 0).unwrap().execute().await;
    std::fs::remove_file(&index_path).unwrap();
    let replacement = IggyIndexWriter::new(
        index_path.to_str().unwrap(),
        Rc::new(AtomicU64::new(0)),
        true,
        false,
    )
    .await
    .unwrap();
    let active = fixture.partition.log.index_writers().len() - 1;
    fixture.partition.log.index_writers_mut()[active] = Some(Rc::new(replacement));
    assert!(fixture.partition.accept_materialization(result).is_err());
    assert_eq!(fixture.cursors(), (0, 0));
    assert_eq!(fixture.segment_size(), 0);
}

#[compio::test]
async fn given_flushed_repair_ahead_of_commit_min_when_replayed_should_persist_only_new_batches() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.log.journal().inner.set_repair_retention(true);
    partition.repair = Some(armed_session(4, 0));
    let prepares: Vec<_> = (1..=4)
        .map(|op| repaired_send_prepare(op, u128::from(op - 1), u128::from(op)).into_frozen())
        .collect();
    let replay = |op: usize| {
        let bytes = prepares[op - 1].as_slice();
        let mut message = Message::<PrepareHeader>::new(bytes.len());
        message.as_mut_slice().copy_from_slice(bytes);
        message
    };
    for op in 1..=3 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition.consensus().advance_commit_max(3);
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush before the commit walk catches up");
    assert_eq!(partition.consensus().commit_min(), 0);
    assert_eq!(partition.recovered_durable_offset, None);
    let original = std::fs::read(&log_path).expect("read initial segment");
    let original_index = std::fs::read(&index_path).expect("read initial index");

    for op in 2..=3 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush replayed batches");
    assert_eq!(std::fs::read(&log_path).unwrap(), original);
    assert_eq!(std::fs::read(&index_path).unwrap(), original_index);

    let next = replay(4);
    let mut expected = original;
    expected.extend_from_slice(&next.as_slice()[size_of::<PrepareHeader>()..]);
    partition.apply_repaired_prepare(next).await;
    partition.consensus().advance_commit_max(4);
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush the new batch");
    assert_eq!(std::fs::read(&log_path).unwrap(), expected);
    // The commit walk needs resident headers after the earlier flush.
    for op in 1..=4 {
        partition.apply_repaired_prepare(replay(op)).await;
    }
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), 4);
    assert!(partition.fatal.is_none());
    assert_eq!(std::fs::read(&log_path).unwrap(), expected);
}

/// Base offsets of the batches in a segment log, in file order.
fn segment_base_offsets(mut log: &[u8]) -> Vec<u64> {
    let mut offsets = Vec::new();
    while !log.is_empty() {
        let batch = decode_batch_slice(log).expect("segment holds whole batches");
        offsets.push(batch.header.base_offset);
        log = &log[batch.header.total_size()..];
    }
    offsets
}

/// A repaired op can arrive before the ops below it. Writing it first puts
/// its offsets past a hole, and the flush skips every batch that ends at or
/// below the segment end, so the lower ops would read as persisted and be
/// dropped.
#[compio::test]
async fn given_repaired_op_above_a_hole_when_flushing_should_hold_it_until_the_hole_fills() {
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let index_path = dir.path().join("segment.index");
    let mut fixture = PersistFixture::new(
        log_path.to_str().expect("utf-8 path"),
        index_path.to_str().expect("utf-8 path"),
    )
    .await;
    let partition = &mut fixture.partition;
    partition.repair = Some(armed_session(6, 0));
    for op in [1, 2, 3, 6] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(6);
    for _ in 0..2 {
        partition
            .flush_committed_messages(&repair_config())
            .await
            .expect("flush below the hole");
    }
    assert_eq!(
        segment_base_offsets(&std::fs::read(&log_path).unwrap()),
        [1, 2, 3]
    );
    assert!(partition.log.journal().inner.holds_op(6));

    for op in [4, 5] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition
        .flush_committed_messages(&repair_config())
        .await
        .expect("flush the filled window");
    assert_eq!(
        segment_base_offsets(&std::fs::read(&log_path).unwrap()),
        [1, 2, 3, 4, 5, 6]
    );
    assert!(partition.log.journal().inner.is_empty());
}

/// Repair that journals `[1, 2, 3, 6, 4, 5]` must still reach the segment
/// in op order through the I/O owner.
#[compio::test]
async fn given_out_of_order_repair_when_the_io_owner_flushes_should_write_in_op_order() {
    const FLUSH_ROUNDS_MAX: usize = 8;
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    partition.repair = Some(armed_session(6, 0));
    for op in [1, 2, 3, 6, 4, 5] {
        partition
            .apply_repaired_prepare(repaired_send_prepare(
                op,
                u128::from(op - 1),
                u128::from(op),
            ))
            .await;
    }
    partition.consensus().advance_commit_max(6);

    let mut flushed = false;
    for _ in 0..FLUSH_ROUNDS_MAX {
        if partition.flush_committed_messages(&config).await.unwrap() {
            flushed = true;
            break;
        }
        settle_partition_io(&mut partition, &config).await;
    }
    assert!(flushed, "the flush must finish");
    assert!(partition.fatal().is_none());
    assert_eq!(
        segment_base_offsets(
            &std::fs::read(directory.path().join("00000000000000000000.log")).unwrap()
        ),
        [1, 2, 3, 4, 5, 6]
    );
}

/// An install leaves no evicted op behind and moves the commit point past
/// the history it brings, so only the commit point tells the flush where
/// the next op may start.
#[compio::test]
async fn given_installed_history_when_flushing_the_next_ops_should_write_them() {
    const INSTALLED_OPS: u64 = 3;
    let directory = tempfile::tempdir().unwrap();
    let mut partition = test_partition();
    partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
    set_offset_dirs_under(&mut partition, directory.path());
    let mut prepares = Vec::new();
    let mut parent = 0;
    for offset in 0..INSTALLED_OPS {
        let prepare = checksummed_segment_prepare(offset + 1, parent, offset, b"transferred");
        parent = prepare.header().checksum;
        prepares.push(prepare);
    }
    let mut staged = Vec::new();
    for (offset, prepare) in (0u64..).zip(&prepares) {
        let body = prepare.as_slice()[size_of::<PrepareHeader>()..].to_vec();
        let artifact = consensus::StateArtifact::for_bytes(
            consensus::state_manifest::artifact_kind::SEGMENT_LOG,
            offset,
            &body,
        );
        staged.push(
            partition
                .spill_transfer_segment(&artifact, body, None)
                .await
                .unwrap(),
        );
    }
    let last = prepares.last().unwrap();
    let offsets = crate::state_transfer::ConsumerOffsetsWire {
        producers: Vec::new(),
        dedup_capacity: consensus::CLIENTS_TABLE_MAX,
        required_metadata_frontier: 0,
        prepare_checksum: Some(last.header().checksum),
        checkpoint_prepare: last.as_slice().to_vec(),
        next_offset: INSTALLED_OPS,
        emptied_at: 0,
        consumers: Vec::new(),
        groups: Vec::new(),
        external_groups: Vec::new(),
        dedup: Vec::new(),
        owners: Vec::new(),
        history_transition: None,
        owner_retirement: None,
        retirement_paid_through: 0,
        machines: None,
    };
    partition
        .install_state_transfer(&repair_config(), INSTALLED_OPS, staged, &offsets.encode())
        .await
        .unwrap();
    assert_eq!(partition.consensus().commit_min(), INSTALLED_OPS);

    for op in [INSTALLED_OPS + 1, INSTALLED_OPS + 2] {
        journal_send_batch(&mut partition, op).await;
    }
    partition.consensus().advance_commit_max(INSTALLED_OPS + 2);
    assert!(
        partition
            .flush_committed_messages(&repair_config())
            .await
            .unwrap()
    );
    assert!(partition.log.journal().inner.is_empty());
    assert_eq!(
        partition.log.active_segment().end_offset,
        INSTALLED_OPS + 1,
        "offsets 3 and 4 follow the installed 0..=2"
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_persist_failure_on_a_committed_op_should_fence_the_partition_not_panic() {
    // The op is cluster-committed and the local write cannot be made, so
    // the replica is divergent. This used to `panic!`, which the pump
    // task swallows: the shard stopped serving every partition it owned
    // while the process reported healthy. The partition must fence itself
    // instead, so the shard's tick can see the fault and stop the server.
    let dir = tempfile::tempdir().expect("temp dir");
    let log_path = dir.path().join("segment.log");
    let log_cursor = Rc::new(AtomicU64::new(0));
    let index_cursor = Rc::new(AtomicU64::new(0));
    let messages_writer = MessagesWriter::new(
        log_path.to_str().expect("utf-8 path"),
        log_cursor,
        true,
        false,
        None,
    )
    .await
    .expect("open segment log writer");
    // The index half writes to a device that is always full, so the
    // persist fails the way a full disk fails.
    let index_writer = IggyIndexWriter::new(DEV_FULL, index_cursor, true, false)
        .await
        .expect("open segment index writer");

    let mut partition = test_partition();
    partition.log.add_persisted_segment(
        Segment::new(0, IggyByteSize::from(1024 * 1024_u64)),
        SegmentStorage::default(),
        Some(Rc::new(messages_writer)),
        Some(Rc::new(index_writer)),
    );

    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);

    // Would abort the test process before the fence existed.
    partition.commit_journal(&repair_config()).await;

    let fault = partition
        .fatal()
        .expect("a failed commit of a cluster-committed op must fence the partition");
    assert_eq!(fault.op, 1);
    assert_eq!(fault.operation, Some(Operation::SendMessages));

    // The fence holds: a fenced partition must not advance again, or the
    // pump's tail drain walks it into the `advance_commit_min` assert.
    let commit_min = partition.consensus().commit_min();
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.consensus().commit_min(),
        commit_min,
        "a fenced partition must not advance on a later commit"
    );
}

#[compio::test]
async fn given_a_shutdown_flush_failure_when_fencing_should_keep_an_earlier_commit_fault() {
    let mut partition = test_partition();

    // A flush failure on a healthy partition fences it, so the pump's
    // post-flush scan turns the exit non-zero instead of reporting a
    // clean shutdown over unpersisted cluster-committed data.
    partition.fence_flush_failure();
    let fault = partition
        .fatal()
        .expect("a failed shutdown flush must fence the partition");
    assert_eq!(fault.op, partition.consensus().commit_min());
    assert_eq!(fault.operation, Some(Operation::SendMessages));

    // A partition the commit path already fenced keeps that fault: it
    // names the op that first diverged.
    let commit_fault = FatalCommit {
        namespace_raw: fault.namespace_raw,
        op: 42,
        operation: Some(Operation::StoreConsumerOffset),
    };
    partition.fatal = Some(commit_fault);
    partition.fence_flush_failure();
    let kept = partition.fatal().expect("the fence must hold");
    assert_eq!(kept.op, 42);
    assert_eq!(kept.operation, Some(Operation::StoreConsumerOffset));
}

/// Eviction is what makes a second copy reachable at all:
/// `apply_repaired_prepare` refuses an op the journal still holds, so a
/// re-delivered repair frame is only re-journaled once the flush that
/// persisted it has evicted it. `commit_min` is what lets the frame past
/// the other guard: the flush persists the whole committed prefix while
/// the walk advances `commit_min` by at most [`COMMIT_WALK_OPS_MAX`], so
/// every op above the walk's reach is persisted, evicted, and still
/// re-deliverable. The flush is the last gate, and a durable line frozen at
/// boot cannot see its own writes -- a rejoining backup then diverged from
/// the group by exactly the ops it repaired.
///
/// Journaled through the repair path, the one that keeps a batch's original
/// offsets; `apply_replicated_operation` re-stamps from the local counter
/// and so cannot collide with itself.
#[compio::test]
async fn given_offsets_already_persisted_when_flushed_again_should_not_append_a_second_copy() {
    const CHECKSUM: u128 = 0x5a;
    // One op past the walk budget, so the last one stays above `commit_min`
    // after the flush that persists it.
    const OPS: u64 = COMMIT_WALK_OPS_MAX as u64 + 1;
    let mut partition = test_partition();
    let record_len = build_segment_record(IggyNamespace::new(1, 1, 0), 1).len() as u64;
    let chained = |op: u64| if op == 1 { 0 } else { CHECKSUM };
    partition.repair = Some(armed_session(OPS, 0));

    for op in 1..=OPS {
        partition
            .apply_repaired_prepare(repaired_send_prepare(op, chained(op), CHECKSUM))
            .await;
    }
    partition.consensus().advance_commit_max(OPS);
    partition.commit_journal(&repair_config()).await;
    assert_eq!(partition.consensus().commit_min(), OPS - 1);
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * (OPS - 1)
    );
    assert_eq!(
        partition
            .collect_committable_from_journal(COMMIT_WALK_OPS_MAX, &repair_config())
            .iter()
            .map(|entry| entry.op)
            .collect::<Vec<_>>(),
        vec![OPS]
    );
    partition
        .flush_committed_messages(&repair_config())
        .await
        .unwrap();
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * OPS,
        "the first flush persists the whole committed prefix"
    );
    assert!(
        partition.consensus().commit_min() < OPS,
        "the premise: the walk leaves the last op above `commit_min`, which \
             is what lets the repair ingest re-deliver it"
    );

    partition.repair = Some(armed_session(OPS, 0));
    partition
        .apply_repaired_prepare(repaired_send_prepare(OPS, chained(OPS), CHECKSUM))
        .await;
    partition.commit_journal(&repair_config()).await;
    assert_eq!(
        partition.log.active_segment().size.as_bytes_u64(),
        record_len * OPS,
        "offsets the segment already holds must not be appended twice"
    );
}

#[cfg(test)]
mod review_4092_tests {
    use super::*;
    use super::{checksummed_segment_prepare, recording_partition_at};

    /// ENOSPC on 28 is the raw errno; `io::ErrorKind::StorageFull` is unstable.
    const ENOSPC: i32 = 28;

    /// `tick_partitions` turns a partition's `fatal()` into a server shutdown
    /// (`shard/src/lib.rs:7378-7382`). A refused offset write leaves prior bytes
    /// intact and nothing undefined, so it should fence the partition at worst,
    /// the way `mark_materialization_missing` and `partitions.tombstone` already
    /// do for an unserviceable namespace.
    #[compio::test]
    #[ignore = "PR #4092 review: a refused consumer-offset write raises `FatalCommit`, which the shard pump converts into a whole-node shutdown"]
    async fn given_a_full_disk_when_driving_persistence_then_only_the_partition_should_fence() {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.consumer_offset_durability = iggy_common::Durability::Persisted;
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

        persistence.fail_operation(
            std::io::Error::from_raw_os_error(ENOSPC),
            Operation::StoreConsumerOffset,
        );
        partition.drive_persistence().await;

        assert!(
            partition.fatal().is_none(),
            "a refused consumer-offset write raised FatalCommit, which the shard pump converts into a whole-node shutdown; every other partition on the core is taken down with it, including topics with no persisted policy"
        );
    }

    /// Contested between reviewers: distsys argued the pre-checks leave only a
    /// genuinely divergent prepare here, where fail-closed is defensible; storage
    /// argued the same errno classification fix covers both call sites. Recorded
    /// so the decision is explicit rather than implied by a missing test.
    #[compio::test]
    #[ignore = "PR #4092 review: CONTESTED between reviewers -- whether a divergent prepare at an already-accepted op should latch the whole partition"]
    async fn given_a_divergent_prepare_when_submitting_then_the_partition_should_not_latch() {
        let directory = tempfile::tempdir().unwrap();
        let (mut partition, _replies) = recording_partition_at(0, 3);
        partition.set_partition_dir(directory.path().to_string_lossy().into_owned());
        partition.runtime_options.durability = iggy_common::Durability::Persisted;
        partition.open_persistence().await.unwrap();
        let persistence = Rc::clone(partition.persistence.as_ref().unwrap());

        let first = checksummed_segment_prepare(1, 0, 0, b"first");
        assert!(partition.submit_prepare_persistence(first.into_frozen(), Operation::SendMessages));

        // Same op, different bytes: `append` answers InvalidData, which is not
        // `WouldBlock`, so `:1192-1193` latches the whole partition.
        let divergent = checksummed_segment_prepare(1, 0, 0, b"divergent");
        assert!(
            !partition.submit_prepare_persistence(divergent.into_frozen(), Operation::SendMessages)
        );
        assert!(
            persistence.failure().is_none(),
            "a single refused prepare latched the partition permanently; `failure` has no clearing path, so every later durability query answers false"
        );
    }
}
