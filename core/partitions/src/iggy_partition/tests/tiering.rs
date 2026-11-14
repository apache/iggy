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

use super::eviction::{KEEP_NOTHING, chain_records, evict};
use super::floor_eviction::apply;
use super::segment_chain::PREFIX;
use super::*;
use crate::segment_chain::{
    ChainRecord, HASH_STEP_LEN, HashSource, ObjectPrefix, chain_path, create_chain,
    hash_segment_file,
};
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{SegmentEviction, SegmentLocation};

/// A partition with [`RECEIVER_OPS`] one-message ops and no chain: two sealed
/// segments and an active one that holds a message.
async fn untiered_partition() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    Box::pin(untiered_partition_of(Bytes::from_static(b"abcdefgh"))).await
}

/// [`untiered_partition`] with `payload` in every message.
async fn untiered_partition_of(
    payload: Bytes,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let record = build_segment_record_with_payload(partition.namespace(), 0, payload);
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * record.len() as u64);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    partition.set_io_notifier(
        Rc::new(|_, _| {}),
        crate::largest_legal_job_charge().unwrap(),
    );
    let records: Vec<_> = (0..RECEIVER_OPS).map(|_| record.clone()).collect();
    Box::pin(commit_records(&mut partition, 1, &records)).await;
    (directory, partition)
}

/// A payload that makes every sealed segment of [`untiered_partition_of`]
/// longer than one hash step and shorter than two.
fn two_step_payload() -> Bytes {
    Bytes::from(vec![7; HASH_STEP_LEN * 3 / 4])
}

/// Runs the lane until it plans a tiering job, `None` when it goes idle
/// without one.
async fn next_tiering_plan(
    partition: &mut IggyPartition<IggyMessageBus>,
) -> Option<crate::PartitionIoPlan> {
    let config = repair_config();
    loop {
        match partition.resume_io(&config).await {
            crate::PartitionIoStep::Ready(plan)
                if plan.continuation == crate::PartitionIoContinuation::Tiering =>
            {
                return Some(plan);
            }
            crate::PartitionIoStep::Ready(plan) => {
                Box::pin(execute_partition_io_plan(partition, &config, plan)).await;
            }
            crate::PartitionIoStep::Progress => {}
            crate::PartitionIoStep::Pending => return None,
            _ => panic!("unexpected partition I/O continuation"),
        }
    }
}

/// Runs the next tiering job, which the test expects to exist.
async fn run_tiering_job(partition: &mut IggyPartition<IggyMessageBus>) -> crate::PartitionIoPlan {
    let plan = Box::pin(next_tiering_plan(partition)).await.unwrap();
    Box::pin(execute_partition_io_plan(partition, &repair_config(), plan)).await;
    plan
}

async fn enable(partition: &mut IggyPartition<IggyMessageBus>, prefix: ObjectPrefix) {
    partition.enable_tiering(prefix);
    Box::pin(settle_partition_io(partition, &repair_config())).await;
}

/// The sealed records of the chain on disk, each checked against the hash of
/// its `.log`, as `(start offset, source)`.
async fn recorded(directory: &Path) -> Vec<(u64, HashSource)> {
    let mut sealed = Vec::new();
    for record in chain_records(directory).await {
        let ChainRecord::Sealed(segment) = record else {
            continue;
        };
        let log = directory.join(format!("{:020}.log", segment.start_offset));
        let on_disk = hash_segment_file(&log.to_string_lossy(), segment.size)
            .await
            .unwrap();
        assert_eq!(
            (segment.hash, segment.crc32c),
            (on_disk.blake3, on_disk.crc32c),
            "segment {}",
            segment.start_offset
        );
        sealed.push((segment.start_offset, segment.source));
    }
    sealed
}

#[compio::test]
async fn given_a_partition_with_sealed_segments_when_tiering_is_enabled_should_record_each_from_disk()
 {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    assert!(!partition.is_tiered());
    assert_eq!(
        Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await,
        SegmentEviction::default(),
        "a partition without a chain evicts nothing"
    );

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert!(partition.is_tiered());
    assert_eq!(partition.object_prefix(), Some(PREFIX));
    assert_eq!(partition.chain_trim_point(), None, "the front is at zero");
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(0, HashSource::Disk), (2, HashSource::Disk)]
    );
    for start_offset in [0, 2] {
        assert!(partition.sealed_segment_record(start_offset).is_some());
    }

    // The active segment held a message before the chain existed, so it
    // seals without a commit-path hash and is hashed from disk too. The
    // segment after it started under the chain.
    Box::pin(commit_records(
        &mut partition,
        RECEIVER_OPS + 1,
        &segment_records(4),
    ))
    .await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [
            (0, HashSource::Disk),
            (2, HashSource::Disk),
            (4, HashSource::Disk),
            (6, HashSource::CommitPath),
        ]
    );
}

#[compio::test]
async fn given_a_front_that_retention_moved_when_tiering_is_enabled_should_record_it_as_the_trim_point()
 {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let first_end = partition.log.segments()[0].end_offset;
    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    let front = partition.log.segments()[0].start_offset;
    assert_eq!(front, first_end + 1);

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert_eq!(partition.chain_trim_point(), Some(front - 1));
    assert_eq!(
        chain_records(directory.path()).await[0],
        ChainRecord::TrimPoint(front - 1)
    );
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(front, HashSource::Disk)]
    );
}

#[compio::test]
async fn given_a_complete_chain_when_tiering_is_enabled_again_should_append_nothing() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    Box::pin(enable(&mut partition, PREFIX)).await;
    let chain = std::fs::read(chain_path(&directory.path().to_string_lossy())).unwrap();

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert_eq!(
        std::fs::read(chain_path(&directory.path().to_string_lossy())).unwrap(),
        chain
    );
}

#[compio::test]
async fn given_a_chain_of_another_incarnation_when_tiering_is_enabled_should_stay_untiered() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let foreign = ObjectPrefix {
        created_revision: PREFIX.created_revision + 1,
        ..PREFIX
    };
    create_chain(&directory.path().to_string_lossy(), &foreign)
        .await
        .unwrap();

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert!(!partition.is_tiered());
    assert_eq!(chain_records(directory.path()).await, Vec::new());
    // The request is dropped, so the lane does not ask again on every pass.
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
}

async fn expire_all(partition: &mut IggyPartition<IggyMessageBus>) -> crate::SegmentRemoval {
    partition
        .clean_expired_segments(
            IggyTimestamp::from(u64::MAX),
            iggy_common::IggyExpiry::from(1_u64),
            Some(0),
        )
        .await
}

/// Retention of a tiered partition is decided once, for its local and remote
/// segments together, so the local pass leaves even expired segments alone.
#[compio::test]
async fn given_expired_segments_when_the_partition_is_tiered_should_leave_them_to_total_retention()
{
    let (_directory, mut partition) = Box::pin(untiered_partition()).await;
    let segments = partition.log.segments().len();
    Box::pin(enable(&mut partition, PREFIX)).await;

    let removal = Box::pin(expire_all(&mut partition)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;

    assert_eq!(removal.segments, 0);
    assert_eq!(partition.log.segments().len(), segments);

    // The same pass on a partition without a chain removes them.
    let (_directory, mut untiered) = Box::pin(untiered_partition()).await;
    Box::pin(expire_all(&mut untiered)).await;
    Box::pin(settle_partition_io(&mut untiered, &repair_config())).await;
    assert!(untiered.log.segments().len() < segments);
}

/// A tiered primary whose own retention removed its first segment before the
/// topic was tiered, with the trim point that left. One sealed segment is
/// left to upload, and it starts right after the trim point.
async fn tiered_after_local_retention() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>, u64) {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let first_end = partition.log.segments()[0].end_offset;
    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    Box::pin(enable(&mut partition, PREFIX)).await;
    let _ = partition.register_builtin_state_machines();
    let trim_point = partition.chain_trim_point().unwrap();
    assert_eq!(trim_point, first_end);
    assert_eq!(partition.log.segments()[0].start_offset, trim_point + 1);
    (directory, partition, trim_point)
}

#[compio::test]
async fn given_a_trim_point_above_the_floor_when_asking_what_to_commit_should_offer_it_until_the_watermark_covers_it()
 {
    let (_directory, partition, trim_point) = Box::pin(tiered_after_local_retention()).await;
    assert!(trim_point > 0);

    assert_eq!(partition.trim_point_to_commit(0), Some(trim_point));
    assert_eq!(
        partition.trim_point_to_commit(trim_point - 1),
        Some(trim_point)
    );
    assert_eq!(partition.trim_point_to_commit(trim_point), None);
    assert_eq!(partition.trim_point_to_commit(trim_point + 1), None);
}

#[compio::test]
async fn given_no_trim_point_when_asking_what_to_commit_should_offer_nothing() {
    let (_directory, mut partition) = Box::pin(untiered_partition()).await;
    assert_eq!(
        partition.trim_point_to_commit(0),
        None,
        "a partition without a chain"
    );

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert_eq!(partition.chain_trim_point(), None);
    assert_eq!(partition.trim_point_to_commit(0), None);
    // The front is at zero, so nothing holds back the upload either.
    assert!(partition.begin_segment_upload().is_some());
}

#[compio::test]
async fn given_a_backup_with_a_trim_point_when_asking_what_to_commit_should_offer_nothing() {
    let (_directory, mut partition, trim_point) = Box::pin(tiered_after_local_retention()).await;
    assert_eq!(partition.trim_point_to_commit(0), Some(trim_point));

    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);

    assert_eq!(partition.trim_point_to_commit(0), None);
}

/// Another replica can still hold the offsets this primary removed before
/// the topic was tiered, and the advance over the first upload covers them.
#[compio::test]
async fn given_an_uncommitted_trim_point_when_the_primary_would_upload_should_wait_for_the_committed_watermark()
 {
    let (_directory, mut partition, trim_point) = Box::pin(tiered_after_local_retention()).await;
    let waiting = partition.next_segment_upload().unwrap().record;
    assert_eq!(waiting.start_offset, trim_point + 1);

    assert!(partition.begin_segment_upload().is_none());
    partition.note_committed_truncation(trim_point - 1);
    assert!(partition.begin_segment_upload().is_none());

    partition.note_committed_truncation(trim_point);
    // A pass that carries an older watermark takes nothing back.
    partition.note_committed_truncation(0);
    let (upload, _scope, attempt) = partition.begin_segment_upload().unwrap();
    assert_eq!(upload.record, waiting);

    let advance = partition
        .finish_segment_upload(attempt, &upload.record, &Ok(Ok(())))
        .unwrap();
    assert_eq!(
        advance,
        AdvanceTieredFloor {
            start: 0,
            up_to: waiting.end_offset,
            hash: waiting.hash,
        }
    );
    // `apply` fails on an advance the floor machine refuses.
    Box::pin(apply(&mut partition, 100, advance)).await;
    assert_eq!(partition.tiered_floor(), Some(waiting.end_offset));
    assert_eq!(
        partition.trim_point_to_commit(0),
        None,
        "the floor passed the trim point"
    );
}

#[compio::test]
async fn given_a_gap_the_committed_watermark_does_not_cover_when_a_segment_is_stored_should_build_no_advance()
 {
    let (_directory, mut partition, trim_point) = Box::pin(tiered_after_local_retention()).await;
    let stored = partition.next_segment_upload().unwrap().record;

    assert_eq!(partition.note_segment_uploaded(&stored), None);
    assert_eq!(
        partition.log.segments()[0].location,
        SegmentLocation::Uploaded
    );
    assert_eq!(partition.pending_floor_advance(), None);

    partition.note_committed_truncation(trim_point);
    assert_eq!(
        partition.pending_floor_advance(),
        Some(AdvanceTieredFloor {
            start: 0,
            up_to: stored.end_offset,
            hash: stored.hash,
        })
    );
}

/// The floor can pass the trim point without a truncation: an advance of an
/// earlier primary that held the offsets this replica removed covers them.
#[compio::test]
async fn given_a_floor_past_the_trim_point_when_the_primary_would_upload_should_not_wait_for_a_truncation()
 {
    let (_directory, mut partition, trim_point) = Box::pin(tiered_after_local_retention()).await;
    let stored = partition.next_segment_upload().unwrap().record;
    let earlier_primary = AdvanceTieredFloor {
        start: 0,
        up_to: stored.end_offset,
        hash: stored.hash,
    };
    Box::pin(commit_records(
        &mut partition,
        RECEIVER_OPS + 1,
        &segment_records(4),
    ))
    .await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    Box::pin(apply(&mut partition, 100, earlier_primary)).await;
    assert!(partition.tiered_floor() > Some(trim_point));

    assert_eq!(partition.trim_point_to_commit(0), None);
    let (upload, _scope, attempt) = partition.begin_segment_upload().unwrap();
    let advance = partition
        .finish_segment_upload(attempt, &upload.record, &Ok(Ok(())))
        .unwrap();
    assert_eq!(advance.start, stored.end_offset + 1);
}

#[compio::test]
async fn given_a_sealed_segment_when_its_hash_is_planned_should_reserve_the_read_buffer() {
    let (_directory, mut partition) = Box::pin(untiered_partition()).await;
    partition.enable_tiering(PREFIX);
    let open = Box::pin(run_tiering_job(&mut partition)).await;
    assert!(partition.is_tiered());

    let hash = Box::pin(next_tiering_plan(&mut partition)).await.unwrap();

    let segment_len = usize::try_from(partition.log.segments()[0].size.as_bytes_u64()).unwrap();
    assert_eq!(hash.allocation_charge, open.allocation_charge + segment_len);
}

/// A hash from disk runs one step per job, and a commit that became ready
/// behind a step runs before the next one.
#[compio::test]
async fn given_a_segment_longer_than_one_hash_step_when_a_commit_waits_should_persist_it_between_steps()
 {
    let (directory, mut partition) = Box::pin(untiered_partition_of(two_step_payload())).await;
    partition.enable_tiering(PREFIX);
    Box::pin(run_tiering_job(&mut partition)).await;

    let step = Box::pin(run_tiering_job(&mut partition)).await;
    let base = step.allocation_charge - HASH_STEP_LEN;
    assert_eq!(
        partition.sealed_segment_record(0),
        None,
        "one step leaves the segment's tail unread"
    );

    let record = build_segment_record(partition.namespace(), 0);
    let op = RECEIVER_OPS + 1;
    journal_send_record(&mut partition, op, &record).await;
    partition.consensus().advance_commit_max(op);
    assert!(
        matches!(
            partition.resume_io(&repair_config()).await,
            crate::PartitionIoStep::Progress
        ),
        "the commit is selected before the next hash step"
    );
    let tail = Box::pin(next_tiering_plan(&mut partition)).await.unwrap();
    assert_eq!(partition.consensus().commit_min(), op);
    let segment_len = usize::try_from(partition.log.segments()[0].size.as_bytes_u64()).unwrap();
    assert_eq!(tail.allocation_charge, base + segment_len - HASH_STEP_LEN);

    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(partition.pending_tiering(), None);
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(0, HashSource::Disk), (2, HashSource::Disk)]
    );
}

#[compio::test]
async fn given_a_segment_retention_removed_between_hash_steps_when_hashing_resumes_should_hash_the_next_segment_whole()
 {
    let (directory, mut partition) = Box::pin(untiered_partition_of(two_step_payload())).await;
    partition.enable_tiering(PREFIX);
    Box::pin(run_tiering_job(&mut partition)).await;
    Box::pin(run_tiering_job(&mut partition)).await;
    assert_eq!(partition.sealed_segment_record(0), None);

    let first_end = partition.log.segments()[0].end_offset;
    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;

    assert_eq!(partition.chain_trim_point(), Some(first_end));
    assert_eq!(partition.pending_tiering(), None);
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(first_end + 1, HashSource::Disk)]
    );
}

#[compio::test]
async fn given_a_hash_that_fails_when_its_delay_has_passed_should_retry_and_record_the_segment() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    let log = directory.path().join(format!("{:020}.log", 0));
    let moved = directory.path().join("moved");
    std::fs::rename(&log, &moved).unwrap();

    Box::pin(enable(&mut partition, PREFIX)).await;

    assert!(partition.is_tiered());
    assert_eq!(partition.pending_tiering(), Some(PREFIX));
    assert_eq!(chain_records(directory.path()).await, Vec::new());

    std::fs::rename(&moved, &log).unwrap();
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(
        chain_records(directory.path()).await,
        Vec::new(),
        "the retry waits for its delay"
    );

    partition
        .maintenance
        .tiering
        .as_mut()
        .unwrap()
        .retry_after_micros = 0;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(partition.pending_tiering(), None);
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(0, HashSource::Disk), (2, HashSource::Disk)]
    );
}

#[compio::test]
async fn given_sealed_segments_without_records_when_shutdown_begins_should_plan_no_hash() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    partition.enable_tiering(PREFIX);
    Box::pin(run_tiering_job(&mut partition)).await;
    assert!(partition.is_tiered());

    partition.begin_shutdown_io();

    assert!(Box::pin(next_tiering_plan(&mut partition)).await.is_none());
    assert_eq!(partition.pending_tiering(), None);
    assert_eq!(chain_records(directory.path()).await, Vec::new());
}

/// A request scans on from the segment it reached, and a later request
/// starts at the front again.
#[compio::test]
async fn given_a_scan_past_a_segment_when_the_next_hash_is_picked_should_not_revisit_it() {
    let (directory, mut partition) = Box::pin(untiered_partition()).await;
    partition.enable_tiering(PREFIX);
    Box::pin(run_tiering_job(&mut partition)).await;
    Box::pin(next_tiering_plan(&mut partition)).await.unwrap();
    let scan_from = |partition: &IggyPartition<IggyMessageBus>| {
        partition.maintenance.tiering.as_ref().unwrap().scan_from
    };
    assert_eq!(scan_from(&partition), 0);
    partition.maintenance.tiering.as_mut().unwrap().scan_from = 1;

    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(2, HashSource::Disk)]
    );

    Box::pin(enable(&mut partition, PREFIX)).await;
    assert_eq!(
        Box::pin(recorded(directory.path())).await,
        [(2, HashSource::Disk), (0, HashSource::Disk)]
    );
}
