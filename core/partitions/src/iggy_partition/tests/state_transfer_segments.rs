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
async fn referenced_prepare_reads_reject_batch_and_payload_corruption_without_body_checksum() {
    for corrupt_at in [
        0,
        COMMAND_HEADER_SIZE,
        COMMAND_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let wal = directory.path().join("prepares-0");
        let prepare = checksummed_segment_prepare(1, 0, 0, b"payload");
        assert_eq!(prepare.header().checksum_body, 0);
        let group = prepare.header().group;
        let mut journal = journal::PartitionPrepareJournal::open(&wal, group, 0)
            .await
            .unwrap();
        journal
            .enable_segment_storage(
                journal::partition_journal::SegmentPosition::default(),
                1024 * 1024,
                None,
            )
            .await
            .unwrap();
        journal.append(prepare.clone().into_frozen()).await.unwrap();
        assert_eq!(
            journal.prepares().await.unwrap()[0].as_slice(),
            prepare.as_slice()
        );
        let path = directory.path().join("00000000000000000000.log");
        let mut body = std::fs::read(&path).unwrap();
        body[corrupt_at] ^= 1;
        std::fs::write(&path, body).unwrap();
        assert_eq!(
            journal.prepares().await.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        drop(journal);
        assert!(
            journal::PartitionPrepareJournal::open(&wal, group, 0)
                .await
                .is_err()
        );
    }
}

/// A tail whose bodies the ring released to segments does not keep a
/// plan pinned. The receiver would repair it from disk, which moves far
/// less per round than repair from memory, so the source re-pins once the
/// tail of the plan leaves memory.
#[compio::test]
async fn given_a_transfer_pin_whose_tail_left_memory_when_hashing_finishes_should_request_a_fresh_plan()
 {
    let root = tempfile::tempdir().unwrap();
    let (mut partition, _, config, mut parent) =
        partition_with_committed_send(root.path(), iggy_common::Durability::Replicated, 3).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let journal = &partition.log.journal().inner;
    journal.set_repair_retention(true);
    journal.set_ring_caps(crate::EVICTED_RING_CAPACITY, 1);
    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;

    let persistence = Rc::clone(partition.persistence.as_ref().unwrap());
    for op in 2..=3 {
        let prepare = checksummed_segment_prepare(op, parent, op - 1, b"tail");
        let header = *prepare.header();
        parent = header.checksum;
        persistence
            .append(prepare.clone().into_frozen(), false)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        partition.consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(header));
        });
        partition
            .append_repaired_send_messages(prepare)
            .await
            .unwrap();
        partition.consensus.advance_commit_max(op);
        partition.commit_journal(&config).await;
        settle_partition_io(&mut partition, &config).await;
    }
    assert_eq!(partition.consensus().commit_min(), 3);
    assert!(
        partition.log.journal().inner.evicted_header(2).is_some(),
        "the byte cap released the body of op 2 to its segment"
    );

    let offer = partition.state_transfer_offer(&config).await;
    assert!(
        matches!(
            offer,
            Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
        ),
        "{:?}",
        offer.map(|offer| offer.commit_op)
    );
    assert!(matches!(
        *partition.transfer_plan.borrow(),
        Some(TransferPlanSlot::Wanted)
    ));
}

#[compio::test]
async fn given_ops_committing_during_a_transfer_when_the_receiver_installs_should_land_on_the_pinned_op()
 {
    const THRESHOLD: u32 = 8;
    let mut config = repair_config();
    config.messages_required_to_save = THRESHOLD;
    let (_directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    journal_send_batch(&mut partition, 1).await;
    partition.consensus().advance_commit_max(1);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 1);
    assert!(partition.log.journal().inner.holds_op(1));

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(crate::state_transfer::PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(&mut partition, &config).await;
    let planned_len = partition.log.active_segment().size.as_bytes_u64();
    assert!(planned_len > 0, "the transfer flush writes the applied op");

    journal_send_batch(&mut partition, 2).await;
    partition.consensus().advance_commit_max(2);
    settle_partition_io(&mut partition, &config).await;
    assert_eq!(partition.consensus().commit_min(), 2);
    assert!(partition.log.journal().inner.holds_op(2));
    journal_send_batch(&mut partition, 3).await;
    partition.consensus().advance_commit_max(3);
    assert_eq!(
        partition.consensus().commit_min(),
        2,
        "op 3 is not applied yet"
    );

    let offer = partition.state_transfer_offer(&config).await.unwrap();
    assert_eq!(offer.commit_op, 1);
    assert_eq!(offer.segments.len(), 1);
    assert_eq!(offer.segments[0].entry.len, planned_len);
    assert!(
        partition.transfer_plan.borrow().is_none(),
        "a finished build releases its plan"
    );

    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver
        .consensus()
        .advance_commit_max(partition.consensus().commit_min());
    let segment = &offer.segments[0];
    let mut bytes = std::fs::read(&segment.log_path).unwrap();
    bytes.truncate(usize::try_from(segment.entry.len).unwrap());
    let staged = receiver
        .spill_transfer_segment(&segment.entry, bytes, None)
        .await
        .unwrap();
    receiver
        .install_state_transfer(&config, offer.commit_op, vec![staged], &offer.offsets.1)
        .await
        .unwrap();
    assert_eq!(receiver.consensus().commit_min(), 1);
    assert_eq!(receiver.consensus().commit_max(), 2);
}

/// Removes every segment log of `partition`, then builds an offer. The
/// offer can only take each stamp from the checksum memo, and each stamp
/// must equal the manifest checksum of the bytes that the file held.
async fn assert_offer_reads_no_segment_file<B: MessageBus>(partition: &mut IggyPartition<B>) {
    let config = repair_config();
    let expected = segment_artifacts_on_disk(partition);
    let directory = partition.partition_dir().unwrap();
    for artifact in &expected {
        std::fs::remove_file(format!("{directory}/{:020}.log", artifact.frontier)).unwrap();
    }

    assert!(matches!(
        partition.state_transfer_offer(&config).await,
        Err(PartitionTransferUnavailable::FlushPending)
    ));
    settle_partition_io(partition, &config).await;
    let offer = partition.state_transfer_offer(&config).await.unwrap();

    let offered: Vec<_> = offer.segments.iter().map(|segment| segment.entry).collect();
    assert_eq!(offered, expected);
}

/// A save that the I/O byte cap splits into pieces feeds the memo the
/// bytes of every piece, in file order.
#[compio::test]
async fn given_saves_split_into_pieces_when_building_an_offer_should_not_read_the_segments() {
    const OPS: u64 = 4;
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let job_charge_max = crate::largest_legal_job_charge().unwrap();
    partition.set_io_notifier(Rc::new(|_, _| {}), job_charge_max);
    for op in 1..=OPS {
        journal_send_batch(&mut partition, op).await;
    }
    let piece_charge = partition
        .log
        .journal()
        .inner
        .with_entries(|entries| {
            crate::io::materialization_allocation_charge(
                std::slice::from_ref(&entries[0]),
                1,
                crate::iggy_index::IGGY_INDEX_SIZE,
            )
        })
        .unwrap();
    partition.io_bytes_max.set(piece_charge);
    partition.consensus().advance_commit_max(OPS);
    settle_partition_io(&mut partition, &config).await;
    partition.io_bytes_max.set(job_charge_max);
    assert!(partition.fatal().is_none());
    assert_eq!(
        std::fs::metadata(directory.path().join(format!("{:020}.index", 0)))
            .unwrap()
            .len(),
        crate::iggy_index::IGGY_INDEX_SIZE as u64,
        "one save must write every batch, so the cap split it into pieces"
    );

    Box::pin(assert_offer_reads_no_segment_file(&mut partition)).await;
}

/// A save whose bodies the partition WAL already wrote feeds the memo the
/// same bytes that the segment file holds. The group has peers, so its
/// journal keeps the committed prepare that an offer carries.
#[compio::test]
async fn given_segment_bodies_written_by_the_wal_when_building_an_offer_should_not_read_them() {
    const REPLICAS: u8 = 3;
    for durability in [
        iggy_common::Durability::Replicated,
        iggy_common::Durability::Persisted,
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut partition, _replies, _config, _parent) =
            partition_with_committed_send(root.path(), durability, REPLICAS).await;
        assert!(
            partition
                .log
                .messages_writers()
                .last()
                .is_some_and(Option::is_none),
            "{durability:?}: the segment must have no writer, so the WAL wrote its bodies"
        );
        partition.set_io_notifier(
            Rc::new(|_, _| {}),
            crate::largest_legal_job_charge().unwrap(),
        );

        Box::pin(assert_offer_reads_no_segment_file(&mut partition)).await;
    }
}

/// The manifest entry of each segment of `partition`, read from its file.
fn segment_artifacts_on_disk<B: MessageBus>(
    partition: &IggyPartition<B>,
) -> Vec<consensus::StateArtifact> {
    let directory = partition.partition_dir().unwrap();
    partition
        .log
        .segments()
        .iter()
        .map(|segment| {
            let bytes =
                std::fs::read(format!("{directory}/{:020}.log", segment.start_offset)).unwrap();
            assert_eq!(bytes.len() as u64, segment.size.as_bytes_u64());
            consensus::StateArtifact::for_bytes(
                consensus::state_manifest::artifact_kind::SEGMENT_LOG,
                segment.start_offset,
                &bytes,
            )
        })
        .collect()
}

/// The append feeds the checksum memo of each segment it writes, so the
/// offer takes every stamp from memory and reads no segment file. The test
/// removes the files before the build to prove that. The stamps must still
/// equal the manifest checksum of the bytes that the files held.
#[compio::test]
async fn given_segments_written_by_this_process_when_building_an_offer_should_not_read_them() {
    const OPS: u64 = 5;
    let (directory, mut partition) =
        Box::pin(partition_with_sealed_segments(&segment_records(OPS))).await;
    let expected = segment_artifacts_on_disk(&partition);
    assert_eq!(expected.len(), 3, "two sealed segments and the active one");
    for artifact in &expected {
        std::fs::remove_file(
            directory
                .path()
                .join(format!("{:020}.log", artifact.frontier)),
        )
        .unwrap();
    }

    let offer = Box::pin(serve_offer(&mut partition)).await;

    assert_eq!(offer.commit_op, OPS);
    let offered: Vec<_> = offer.segments.iter().map(|segment| segment.entry).collect();
    assert_eq!(offered, expected);
}

#[compio::test]
async fn given_cached_sealed_segment_corruption_when_installing_should_fetch_verified_bytes() {
    let records = segment_records(ORIGIN_OPS);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
        &records[..usize::try_from(RECEIVER_OPS).unwrap()],
    ))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let entry = offer.segments[0].entry;
    let path = directory.path().join(format!("{:020}.log", entry.frontier));
    let mut corrupted = std::fs::read(&path).unwrap();
    *corrupted.last_mut().unwrap() ^= 1;
    std::fs::write(&path, &corrupted).unwrap();
    assert_eq!(corrupted.len() as u64, entry.len);
    assert_eq!(
        receiver.segment_checksum_cache.borrow()[&entry.frontier].checksum_of(entry.len),
        Some(entry.checksum),
        "the append checksum still describes the original bytes"
    );

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(adopted, [1], "the corrupt sealed segment must be fetched");
    assert_eq!(
        segment_file_bytes(directory.path()),
        offered_segment_bytes(&offer)
    );
    assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
}

#[compio::test]
async fn given_corrupt_sealed_index_when_reusing_should_rebuild_from_verified_log() {
    let records = segment_records(ORIGIN_OPS);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
        &records[..usize::try_from(RECEIVER_OPS).unwrap()],
    ))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];
    let expected = origin
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();
    let expected_index = std::fs::read(&expected.index_staging).unwrap();
    let index_path = directory
        .path()
        .join(format!("{:020}.index", source.entry.frontier));
    let mut corrupted = std::fs::read(&index_path).unwrap();
    corrupted.fill(u8::MAX);
    std::fs::write(&index_path, &corrupted).unwrap();

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(adopted, [0, 1], "valid logs remain reusable");
    assert_eq!(std::fs::read(index_path).unwrap(), expected_index);
    assert_eq!(
        segment_file_bytes(directory.path()),
        offered_segment_bytes(&offer)
    );
}

#[compio::test]
async fn given_pending_reuse_scan_when_tearing_down_should_wait_for_its_task() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let manifest = Box::pin(serve_offer(&mut receiver)).await.manifest();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let scan = receiver.begin_reuse_scan(&manifest);
    let mut drain = Box::pin(receiver.capture_teardown().drain());

    assert!(
        futures::poll!(&mut drain).is_pending(),
        "teardown must wait even before the detached scan is first polled"
    );
    assert!(scan.stop());
    assert!(
        futures::poll!(&mut drain).is_pending(),
        "cancellation does not prove that the file task has settled"
    );
    drop(release);
    drain.await.unwrap();
    assert!(
        scan.result.borrow().is_some(),
        "the scan finished before teardown"
    );
}

#[compio::test]
async fn given_a_pending_spill_when_tearing_down_should_wait_for_its_task() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let source = &offer.segments[0];
    let bytes = std::fs::read(&source.log_path).unwrap();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let spill = receiver.begin_transfer_segment_spill(&source.entry, bytes, None);
    let mut drain = Box::pin(receiver.capture_teardown().drain());

    assert!(futures::poll!(&mut drain).is_pending());
    assert!(spill.stop());
    assert!(
        futures::poll!(&mut drain).is_pending(),
        "cancellation must not release the staging writer before it settles"
    );
    drop(release);
    drain.await.unwrap();

    assert!(matches!(
        spill.result.borrow_mut().take().unwrap(),
        Err(crate::state_transfer::SpillError::Cancelled)
    ));
    assert!(
        !directory
            .path()
            .join("00000000000000000000.log.staging")
            .exists(),
        "the cancelled task must not write after teardown"
    );
}

#[compio::test]
async fn given_an_abandoned_spill_when_a_reuse_scan_starts_should_wait_for_the_writer() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let source = &offer.segments[0];
    let bytes = std::fs::read(&source.log_path).unwrap();
    let (release, previous) = futures::channel::oneshot::channel::<()>();
    receiver.staging_work_ended = Some(previous);
    let spill = receiver.begin_transfer_segment_spill(&source.entry, bytes.clone(), None);
    assert!(spill.stop());
    let scan = receiver.begin_reuse_scan(&offer.manifest());
    server_common::yield_to_reactor().await;
    assert!(
        !scan.landed(),
        "a replacement scan must not overtake the abandoned staging writer"
    );

    drop(release);
    let _ = receiver.staging_work_ended.take().unwrap().await;

    assert!(matches!(
        spill.result.borrow_mut().take().unwrap(),
        Err(crate::state_transfer::SpillError::Cancelled)
    ));
    let reused = receiver.accept_reuse_scan(scan.result.borrow_mut().take().unwrap());
    let staged = &reused.iter().find(|(index, _)| *index == 0).unwrap().1;
    assert_eq!(std::fs::read(&staged.log_staging).unwrap(), bytes);
}

#[compio::test]
async fn given_singleton_materialization_when_writing_should_skip_transfer_checksums() {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition(&config)).await;
    let record = build_segment_record(partition.namespace(), 0);
    Box::pin(commit_records(
        &mut partition,
        1,
        std::slice::from_ref(&record),
    ))
    .await;

    assert_eq!(partition.consensus().commit_min(), 1);
    assert_eq!(segment_file_bytes(directory.path())[0].len(), record.len());
    assert!(
        partition.segment_checksum_cache.borrow().is_empty(),
        "a singleton has no state-transfer peer to serve"
    );
}

/// A sealed segment takes no more appends, so its memo keeps only the
/// stamp, whether the appends or an offer read hashed it. The active
/// segment keeps its hasher for the bytes that it gains.
#[compio::test]
async fn given_sealed_segments_when_their_memos_cover_them_should_keep_no_hasher() {
    let (_directory, mut partition) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let active = partition.log.active_segment().start_offset;
    let hashing = |partition: &IggyPartition<IggyMessageBus>| -> Vec<u64> {
        partition
            .segment_checksum_cache
            .borrow()
            .iter()
            .filter(|(_, memo)| {
                matches!(
                    memo,
                    crate::state_transfer::SegmentChecksumMemo::Hashing { .. }
                )
            })
            .map(|(start_offset, _)| *start_offset)
            .collect()
    };
    assert_eq!(hashing(&partition), [active], "after the appends");

    partition.segment_checksum_cache.borrow_mut().clear();
    Box::pin(serve_offer(&mut partition)).await;

    assert_eq!(hashing(&partition), [active], "after an offer read");
    assert_eq!(
        partition.segment_checksum_cache.borrow().len(),
        partition.log.segments().len()
    );
}

/// A receiver links every sealed segment of an offer that it holds, however
/// many it holds, so the transfer pulls none of them.
#[cfg(unix)]
#[compio::test]
async fn given_many_held_sealed_segments_when_installing_an_offer_should_link_every_one() {
    const SEALED: u64 = 12;
    let records = segment_records(SEALED * BATCHES_PER_SEGMENT + 1);
    let (_origin_directory, mut origin) = Box::pin(partition_with_sealed_segments(&records)).await;
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&records)).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert_eq!(
        adopted,
        (0..u32::try_from(SEALED).unwrap()).collect::<Vec<_>>(),
        "every held sealed segment"
    );
}

#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_partial_reuse_scan_when_an_offer_advances_should_keep_verified_files() {
    for spill_other in [false, true] {
        let records = segment_records(ORIGIN_OPS + 1);
        let (_origin_directory, mut origin) =
            Box::pin(partition_with_sealed_segments(&records)).await;
        let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(
            &records[..usize::try_from(RECEIVER_OPS).unwrap()],
        ))
        .await;
        let previous = Box::pin(serve_offer(&mut receiver)).await.manifest();
        let scan = receiver.begin_reuse_scan(&previous);
        let _ = receiver.staging_work_ended.take().unwrap().await;
        let mut scanned = scan.result.borrow_mut().take().unwrap();
        assert!(scanned.reused.len() > 1);
        // Model a deadline between artifacts using files verified by the real scan.
        for (_, unused) in scanned.reused.split_off(1) {
            std::fs::remove_file(unused.log_staging).unwrap();
            std::fs::remove_file(unused.index_staging).unwrap();
        }
        let first = scanned.reused[0].1.clone();
        receiver.accept_reuse_scan(scanned);
        let index = std::fs::File::open(&first.index_staging).unwrap();
        let verified_inode = index.metadata().unwrap().ino();
        let offer = Box::pin(serve_offer(&mut origin)).await;
        let manifest = offer.manifest();
        assert_ne!(manifest, previous, "the source kept accepting writes");
        let stopped = receiver.begin_reuse_scan(&manifest);
        assert!(stopped.stop());
        let _ = receiver.staging_work_ended.take().unwrap().await;
        receiver.accept_reuse_scan(stopped.result.borrow_mut().take().unwrap());
        if spill_other {
            let source = offer.segments.last().unwrap();
            let mut bytes = std::fs::read(&source.log_path).unwrap();
            bytes.truncate(usize::try_from(source.entry.len).unwrap());
            receiver
                .spill_transfer_segment(&source.entry, bytes, None)
                .await
                .unwrap();
        }

        let reused = reuse_scan(&mut receiver, &manifest).await;

        assert!(reused.iter().any(|(position, _)| *position == 0));
        assert_eq!(
            std::fs::metadata(&first.index_staging).unwrap().ino(),
            verified_inode,
            "spill_other={spill_other}: verified staging must not be rebuilt on every retry"
        );
        drop(index);
        Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;
        assert_eq!(
            segment_file_bytes(directory.path()),
            offered_segment_bytes(&offer)
        );
    }
}

#[compio::test]
async fn given_verified_staging_when_a_spill_replaces_it_should_revalidate_for_an_older_offer() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let own = Box::pin(serve_offer(&mut receiver)).await;
    reuse_scan(&mut receiver, &own.manifest()).await;
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];
    assert_ne!(source.entry.checksum, own.segments[0].entry.checksum);
    receiver
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();

    let reused = reuse_scan(&mut receiver, &own.manifest()).await;

    let first = &reused.iter().find(|(index, _)| *index == 0).unwrap().1;
    assert_eq!(
        std::fs::read(&first.log_staging).unwrap(),
        std::fs::read(&own.segments[0].log_path).unwrap(),
        "an older offer must recover its verified bytes after a staging replacement"
    );
}

/// A scan that is asked to stop before its first entry stages nothing and
/// leaves no memo, so the next scan of the same segment set runs whole.
#[cfg(unix)]
#[compio::test]
async fn given_a_stopped_reuse_scan_when_it_lands_should_reuse_nothing_and_leave_no_memo() {
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let manifest = Box::pin(serve_offer(&mut receiver)).await.manifest();
    let scan = receiver.begin_reuse_scan(&manifest);
    assert!(scan.stop());

    let _ = receiver.staging_work_ended.take().unwrap().await;

    let scanned = scan.result.borrow_mut().take().unwrap();
    assert_eq!(scanned.reused, Vec::new());
    assert!(scanned.memoized.is_empty(), "a stopped scan saw no entry");
    assert_eq!(receiver.accept_reuse_scan(scanned), Vec::new());
    assert!(receiver.reuse_scan_memo.borrow().is_none());
    assert_eq!(
        reuse_scan(&mut receiver, &manifest).await.len(),
        2,
        "the next scan links both held sealed segments"
    );
}

/// A link that fails leaves the staging names as they were, so the staged
/// copy of an earlier attempt still serves the transfer.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_failing_link_when_a_staged_copy_exists_should_adopt_the_copy() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut receiver)).await;
    let held = &offer.segments[0];
    receiver
        .spill_transfer_segment(&held.entry, std::fs::read(&held.log_path).unwrap(), None)
        .await
        .unwrap();
    let staged = directory
        .path()
        .join(format!("{:020}.log.staging", held.entry.frontier));
    let copy = std::fs::metadata(&staged).unwrap().ino();
    // A directory at the scratch name fails the link.
    std::fs::create_dir(
        directory
            .path()
            .join(format!("{:020}.log.link.staging", held.entry.frontier)),
    )
    .unwrap();

    let adopted = reuse_scan(&mut receiver, &offer.manifest()).await;

    assert!(
        adopted
            .iter()
            .any(|(_, meta)| meta.start_offset == held.entry.frontier && !meta.linked)
    );
    assert_eq!(
        std::fs::metadata(&staged).unwrap().ino(),
        copy,
        "the staging name still names the staged copy"
    );
}

/// A spill gives a staging name that links a live segment a new file.
/// Writing through the link would change the live segment.
#[cfg(target_os = "linux")]
#[compio::test]
async fn given_a_linked_staging_name_when_spilling_other_bytes_should_keep_the_live_segment() {
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let own = Box::pin(serve_offer(&mut receiver)).await;
    reuse_scan(&mut receiver, &own.manifest()).await;
    let paths = ["log", "index"].map(|kind| directory.path().join(format!("{:020}.{kind}", 0)));
    let held = paths.clone().map(|path| std::fs::read(path).unwrap());
    let staged_log = directory.path().join(format!("{:020}.log.staging", 0));
    assert_eq!(
        std::fs::metadata(&staged_log).unwrap().ino(),
        log_inode(directory.path(), 0),
        "the scan linked the first segment"
    );
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let source = &offer.segments[0];

    receiver
        .spill_transfer_segment(
            &source.entry,
            std::fs::read(&source.log_path).unwrap(),
            None,
        )
        .await
        .unwrap();

    assert!(
        paths.map(|path| std::fs::read(path).unwrap()) == held,
        "the live segment keeps its bytes"
    );
    assert_ne!(
        std::fs::metadata(&staged_log).unwrap().ino(),
        log_inode(directory.path(), 0)
    );
}

/// A sealed segment with the base offset and the length of an offered one
/// but other bytes fails the checksum match. The receiver then pulls the
/// offered segment, and the install replaces the file.
#[compio::test]
async fn given_receiver_segment_with_other_bytes_when_installing_an_offer_should_copy_it() {
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(&segment_records(ORIGIN_OPS))).await;
    let (directory, mut receiver) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    let offer = Box::pin(serve_offer(&mut origin)).await;
    let held = segment_file_bytes(directory.path());
    let offered = offered_segment_bytes(&offer);
    assert_eq!(held[0].len(), offered[0].len());
    assert_ne!(held[0], offered[0]);

    let adopted = Box::pin(install_offer_with_reuse(&mut receiver, &offer)).await;

    assert!(
        adopted.is_empty(),
        "no held segment matches an offered checksum"
    );
    assert_eq!(segment_file_bytes(directory.path()), offered);
    assert_eq!(receiver.consensus().commit_min(), ORIGIN_OPS);
}

/// The segment that was active at an install keeps a hasher, so the
/// appends that seal it extend its checksum. The next transfer then links
/// it like any other sealed segment instead of pulling it again.
#[compio::test]
async fn given_installed_active_segment_when_appends_seal_it_should_reuse_it_in_the_next_transfer()
{
    let records = segment_records(ORIGIN_OPS + BATCHES_PER_SEGMENT);
    let (origin_held, appended) = records.split_at(usize::try_from(ORIGIN_OPS).unwrap());
    let (_origin_directory, mut origin) =
        Box::pin(partition_with_sealed_segments(origin_held)).await;
    let receiver_held = &records[..usize::try_from(RECEIVER_OPS).unwrap()];
    let (_directory, mut receiver) = Box::pin(partition_with_sealed_segments(receiver_held)).await;
    let first = Box::pin(serve_offer(&mut origin)).await;
    Box::pin(install_offer_with_reuse(&mut receiver, &first)).await;
    for partition in [&mut origin, &mut receiver] {
        Box::pin(commit_records(partition, ORIGIN_OPS + 1, appended)).await;
    }
    let second = Box::pin(serve_offer(&mut origin)).await;
    let active_at_install = first.segments.last().unwrap().entry.frontier;

    let adopted: Vec<u64> = reuse_scan(&mut receiver, &second.manifest())
        .await
        .into_iter()
        .map(|(_, meta)| meta.start_offset)
        .collect();

    assert_eq!(
        adopted,
        [
            0,
            BATCHES_PER_SEGMENT,
            2 * BATCHES_PER_SEGMENT,
            active_at_install
        ],
        "every sealed segment, the one active at the install included"
    );
}

/// The offer carries the committed producer table, so a replica that
/// catches up by transfer answers a resent batch as its donor does.
#[compio::test]
async fn given_a_producer_table_when_a_replica_transfers_should_hold_it() {
    let (_origin_directory, mut origin, _) = message_less_origin().await;
    origin.producers.record(&StampedBatch {
        producer: BatchProducer::new(7, 2, 0).unwrap(),
        message_count: 3,
        base_offset: 0,
        timestamp: 1,
    });

    let offer = origin.state_transfer_offer(&repair_config()).await.unwrap();
    let (_receiver_directory, mut receiver) = transfer_receiver();
    receiver
        .install_state_transfer(
            &repair_config(),
            offer.commit_op,
            Vec::new(),
            &offer.offsets.1,
        )
        .await
        .unwrap();

    assert_eq!(receiver.producer_entries(), origin.producer_entries());
    assert_eq!(
        receiver
            .producers
            .admit(BatchProducer::new(7, 2, 0).unwrap(), 3, &[]),
        ProducerAdmission::Duplicate { base_offset: 0 }
    );
}
