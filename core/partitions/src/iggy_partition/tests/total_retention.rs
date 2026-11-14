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

use super::eviction::{KEEP_NOTHING, chain_records, confirm, evict};
use super::segment_chain::tiered_partition;
use super::*;
use crate::iggy_partition::retention_end;
use crate::segment_chain::ChainRecord;
use crate::{Segment, SegmentLocation, SegmentRemoval};
use iggy_common::{IggyDuration, IggyExpiry, IggyTimestamp};
use std::time::Duration;

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops, as
/// `(start offset, end offset)`. The active segment starts at offset 4.
const SEALED: [(u64, u64); 2] = [(0, 1), (2, 3)];

const SEALED_STARTS: [u64; 2] = [SEALED[0].0, SEALED[1].0];

const REMOTE: SegmentLocation = SegmentLocation::Remote;
const LOCAL: SegmentLocation = SegmentLocation::Local;

fn one_second() -> IggyExpiry {
    IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)))
}

/// Late enough that every message committed by a fixture is past
/// [`one_second`].
fn an_hour_from_now() -> IggyTimestamp {
    IggyTimestamp::from(
        IggyTimestamp::now().as_micros() + IggyDuration::from(Duration::from_hours(1)).as_micros(),
    )
}

/// A tiered primary whose two sealed segments are both evicted.
async fn partition_with_remote_segments() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED_STARTS);
    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(locations(&partition), [REMOTE, REMOTE, LOCAL]);
    (directory, partition)
}

fn locations(partition: &IggyPartition<IggyMessageBus>) -> Vec<SegmentLocation> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.location)
        .collect()
}

fn starts(partition: &IggyPartition<IggyMessageBus>) -> Vec<u64> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| segment.start_offset)
        .collect()
}

fn sealed_size(partition: &IggyPartition<IggyMessageBus>, slot: usize) -> u64 {
    partition.log.segments()[slot].size.as_bytes_u64()
}

fn commit_consumer_offset(partition: &IggyPartition<IggyMessageBus>, offset: u64) {
    partition.consumer_offsets.pin().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, offset, String::new()),
    );
}

fn file(directory: &Path, start_offset: u64, extension: &str) -> PathBuf {
    directory.join(format!("{start_offset:020}.{extension}"))
}

fn segment(start_offset: u64, end_offset: u64, size: u64, sealed: bool) -> Segment {
    let mut segment = Segment::new(start_offset, IggyByteSize::from(0u64));
    segment.end_offset = end_offset;
    segment.max_timestamp = 1;
    segment.size = IggyByteSize::from(size);
    segment.sealed = sealed;
    segment
}

/// Three expired sealed segments of 100 bytes and an active one.
fn expired_run() -> Vec<Segment> {
    vec![
        segment(0, 9, 100, true),
        segment(10, 19, 100, true),
        segment(20, 29, 100, true),
        segment(30, 39, 100, false),
    ]
}

#[test]
fn given_expired_and_oversized_runs_when_deciding_should_take_the_longer_one() {
    let now = IggyTimestamp::now();
    let segments = expired_run();

    assert_eq!(
        retention_end(&segments, now, one_second(), None, None),
        Some(29)
    );
    assert_eq!(
        retention_end(&segments, now, IggyExpiry::NeverExpire, Some(200), None),
        Some(9)
    );
    assert_eq!(
        retention_end(&segments, now, one_second(), Some(200), None),
        Some(29)
    );
    assert_eq!(
        retention_end(&segments, now, IggyExpiry::NeverExpire, Some(300), None),
        None
    );
    assert_eq!(
        retention_end(&segments, now, IggyExpiry::NeverExpire, None, None),
        None
    );
}

#[test]
fn given_a_consumer_barrier_when_deciding_should_stop_at_the_last_segment_below_it() {
    let now = IggyTimestamp::now();
    let segments = expired_run();
    let decide = |barrier| retention_end(&segments, now, one_second(), None, Some(barrier));

    assert_eq!(decide(u64::MAX), Some(29));
    assert_eq!(decide(29), Some(29));
    // A consumer inside a segment still reads it.
    assert_eq!(decide(28), Some(19));
    assert_eq!(decide(10), Some(9));
    assert_eq!(decide(9), Some(9));
    assert_eq!(decide(8), None);
}

#[test]
fn given_an_expired_active_segment_over_budget_when_deciding_should_not_name_it() {
    let now = IggyTimestamp::now();
    let mut segments = expired_run();
    segments[3].sealed = true;

    assert_eq!(
        retention_end(&segments, now, one_second(), Some(0), None),
        Some(29)
    );
    assert_eq!(
        retention_end(&segments[3..], now, one_second(), Some(0), None),
        None
    );
}

#[compio::test]
async fn given_expired_remote_segments_when_the_primary_decides_should_name_the_last_of_them() {
    let (_directory, partition) = Box::pin(partition_with_remote_segments()).await;

    assert_eq!(
        partition.total_retention_end(an_hour_from_now(), one_second(), None),
        Some(SEALED[1].1)
    );
    assert_eq!(
        partition.total_retention_end(IggyTimestamp::now(), IggyExpiry::NeverExpire, None),
        None
    );
}

#[compio::test]
async fn given_a_remote_and_a_local_segment_when_the_primary_decides_by_size_should_count_both() {
    let (_directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    confirm(&mut partition, &SEALED_STARTS);
    Box::pin(evict(&mut partition, SEALED[0].1, KEEP_NOTHING)).await;
    assert_eq!(locations(&partition), [REMOTE, LOCAL, LOCAL]);
    let now = IggyTimestamp::now();
    let (remote, local) = (sealed_size(&partition, 0), sealed_size(&partition, 1));
    let decide = |max_bytes| partition.total_retention_end(now, IggyExpiry::NeverExpire, max_bytes);

    assert_eq!(decide(Some(remote + local)), None);
    // Over budget only when the remote segment's bytes count.
    assert_eq!(decide(Some(local)), Some(SEALED[0].1));
    assert_eq!(decide(Some(0)), Some(SEALED[1].1));
    assert_eq!(decide(None), None);
}

#[compio::test]
async fn given_a_committed_consumer_offset_when_the_primary_decides_should_stop_below_it() {
    let (_directory, partition) = Box::pin(partition_with_remote_segments()).await;
    let decide = || partition.total_retention_end(an_hour_from_now(), one_second(), Some(0));

    commit_consumer_offset(&partition, SEALED[1].1 - 1);
    assert_eq!(decide(), Some(SEALED[0].1));

    commit_consumer_offset(&partition, SEALED[0].1 - 1);
    assert_eq!(decide(), None);
}

#[compio::test]
async fn given_a_backup_when_deciding_should_decide_nothing() {
    let (_directory, mut partition) = Box::pin(partition_with_remote_segments()).await;
    let decide = |partition: &IggyPartition<IggyMessageBus>| {
        partition.total_retention_end(an_hour_from_now(), one_second(), Some(0))
    };
    assert_eq!(decide(&partition), Some(SEALED[1].1));

    // The next view's primary is another replica.
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    assert_eq!(decide(&partition), None);
}

#[compio::test]
async fn given_a_primary_in_a_state_transfer_when_deciding_should_decide_nothing() {
    let (_directory, partition) = Box::pin(partition_with_remote_segments()).await;

    partition.consensus.begin_state_transfer_await();
    assert!(partition.consensus.is_primary());

    assert_eq!(
        partition.total_retention_end(an_hour_from_now(), one_second(), Some(0)),
        None
    );
}

#[compio::test]
async fn given_a_partition_without_a_chain_when_deciding_should_decide_nothing() {
    let (_directory, partition) = Box::pin(partition_with_sealed_segments(&segment_records(
        RECEIVER_OPS,
    )))
    .await;
    assert!(partition.consensus.is_primary());

    assert_eq!(
        partition.total_retention_end(an_hour_from_now(), one_second(), Some(0)),
        None
    );
}

#[compio::test]
async fn given_every_segment_expired_and_over_budget_when_the_primary_decides_should_not_name_the_active_one()
 {
    let (_directory, partition) = Box::pin(partition_with_remote_segments()).await;
    let active = partition.log.active_segment();
    assert!(active.size.as_bytes_u64() > 0);
    assert!(active.end_offset > SEALED[1].1);

    assert_eq!(
        partition.total_retention_end(an_hour_from_now(), one_second(), Some(0)),
        Some(SEALED[1].1)
    );
}

#[compio::test]
async fn given_a_remote_segment_when_a_truncation_is_enforced_should_record_the_trim_point_and_remove_it()
 {
    let (directory, mut partition) = Box::pin(partition_with_remote_segments()).await;
    let config = repair_config();
    let (first_start, first_end) = SEALED[0];
    let removed_bytes = sealed_size(&partition, 0);
    let (size, segments, messages) = (
        partition.stats.size_bytes_inconsistent(),
        partition.stats.segments_count_inconsistent(),
        partition.stats.messages_count_inconsistent(),
    );
    assert!(file(directory.path(), first_start, "index").exists());
    // The fixture plants its segments without anchors.
    std::fs::write(file(directory.path(), first_start, "anchor"), b"anchor").unwrap();

    Box::pin(partition.remove_sealed_segments_up_to(first_end)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(starts(&partition), [SEALED[1].0, SEALED[1].1 + 1]);
    assert_eq!(locations(&partition), [REMOTE, LOCAL]);
    for extension in ["log", "index", "anchor"] {
        assert!(
            !file(directory.path(), first_start, extension).exists(),
            "the removed segment's .{extension} is gone"
        );
    }
    assert!(file(directory.path(), SEALED[1].0, "index").exists());
    assert_eq!(partition.chain_trim_point(), Some(first_end));
    assert!(partition.sealed_segment_record(first_start).is_none());
    assert_eq!(
        chain_records(directory.path()).await.last(),
        Some(&ChainRecord::TrimPoint(first_end))
    );
    assert_eq!(
        partition.stats.size_bytes_inconsistent(),
        size - removed_bytes,
        "the remote segment's bytes leave the partition's size once"
    );
    assert_eq!(partition.stats.segments_count_inconsistent(), segments - 1);
    assert_eq!(
        partition.stats.messages_count_inconsistent(),
        messages - (first_end - first_start + 1)
    );
}

#[compio::test]
async fn given_expired_remote_segments_when_no_truncation_is_committed_should_keep_them() {
    let (directory, mut partition) = Box::pin(partition_with_remote_segments()).await;
    let config = repair_config();
    let now = an_hour_from_now();
    let before = (
        starts(&partition),
        locations(&partition),
        partition.stats.size_bytes_inconsistent(),
        partition.stats.segments_count_inconsistent(),
        chain_records(directory.path()).await,
    );

    // The per-replica expiry, the eviction pass and the primary's own
    // decision each run over segments that are expired and over budget.
    assert_eq!(
        Box::pin(partition.clean_expired_segments(now, one_second(), Some(0))).await,
        SegmentRemoval::default()
    );
    Box::pin(evict(&mut partition, u64::MAX, KEEP_NOTHING)).await;
    assert_eq!(
        partition.total_retention_end(now, one_second(), Some(0)),
        Some(SEALED[1].1)
    );
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(
        (
            starts(&partition),
            locations(&partition),
            partition.stats.size_bytes_inconsistent(),
            partition.stats.segments_count_inconsistent(),
            chain_records(directory.path()).await,
        ),
        before
    );
    for start_offset in SEALED_STARTS {
        assert!(file(directory.path(), start_offset, "index").exists());
    }
    assert_eq!(partition.chain_trim_point(), None);
}
