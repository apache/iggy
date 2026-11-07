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

use iggy_common::{IggyByteSize, IggyDuration, IggyExpiry, IggyTimestamp};
use message_bus::IggyMessageBus;
use server_common::SegmentStorage;
use std::time::Duration;

use crate::{
    IggyPartition, Segment, SegmentRemoval,
    iggy_partition::{
        SEGMENT_REMOVAL_BUDGET_PER_PASS, leading_expired_end, leading_oversized_end,
        nth_oldest_sealed_end,
    },
};

fn segment(end_offset: u64, max_timestamp: u64, size: u64, sealed: bool) -> Segment {
    let mut segment = Segment::new(0, IggyByteSize::from(0u64));
    segment.end_offset = end_offset;
    segment.max_timestamp = max_timestamp;
    segment.size = IggyByteSize::from(size);
    segment.sealed = sealed;
    segment
}

fn one_second() -> IggyExpiry {
    IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)))
}

/// Shape of one sealed segment in the [`partition_with_sealed_run`] fixture.
const MESSAGES_PER_SEGMENT: u64 = 10;
const BYTES_PER_SEGMENT: u64 = 100;

/// A topic's configured segment size, and the largest batch that can be
/// appended to it.
const SEGMENT_SIZE: u64 = 1_000;
const MAX_BATCH_SIZE: u64 = 100;
/// The size a sealed segment actually reaches. The batch that crosses
/// `SEGMENT_SIZE` is appended whole, so a sealed segment closes somewhere
/// in `[SEGMENT_SIZE, SEGMENT_SIZE + MAX_BATCH_SIZE)`. Retention asserted
/// at exactly `SEGMENT_SIZE` would miss that entirely.
const SEALED_SIZE: u64 = SEGMENT_SIZE + 40;
/// What the cleaner enforces for a cap of one segment: the per-partition
/// share, floored at the largest a sealed segment can be.
const FLOORED_BUDGET: u64 = SEGMENT_SIZE + MAX_BATCH_SIZE;

fn sealed_run_segment(start_offset: u64) -> Segment {
    let mut segment = segment(
        start_offset + MESSAGES_PER_SEGMENT - 1,
        1,
        BYTES_PER_SEGMENT,
        true,
    );
    segment.start_offset = start_offset;
    segment
}

/// Partition whose log is `sealed_count` sealed segments followed by the
/// active one, with stats seeded to match. Every storage is the in-memory
/// default (no reader, so no path), which is what lets retirement run its
/// full body here without touching the filesystem.
fn partition_with_sealed_run(sealed_count: u64) -> IggyPartition<IggyMessageBus> {
    let mut partition = super::test_partition();
    // The fixture ships one unsealed segment: rewrite it as the head of the
    // run and append a fresh active segment last, where the removal walk
    // always stops.
    partition.log.segments_mut()[0] = sealed_run_segment(0);
    for index in 1..sealed_count {
        partition.log.add_persisted_segment(
            sealed_run_segment(index * MESSAGES_PER_SEGMENT),
            SegmentStorage::default(),
            None,
            None,
        );
    }
    partition.log.add_persisted_segment(
        Segment::new(
            sealed_count * MESSAGES_PER_SEGMENT,
            IggyByteSize::from(0u64),
        ),
        SegmentStorage::default(),
        None,
        None,
    );
    let stats = &partition.stats;
    stats.increment_segments_count(u32::try_from(sealed_count).expect("run fits a u32"));
    stats.increment_messages_count(sealed_count * MESSAGES_PER_SEGMENT);
    stats.increment_size_bytes(sealed_count * BYTES_PER_SEGMENT);
    partition
}

#[test]
fn leading_expired_end_skips_active_and_returns_last_expired() {
    let segments = vec![
        segment(9, 1, 100, true),
        segment(19, 2, 100, true),
        segment(29, 3, 100, true),
        segment(39, 0, 100, false), // active: never considered
    ];
    assert_eq!(
        leading_expired_end(&segments, IggyTimestamp::now(), one_second()),
        Some(29)
    );
}

#[test]
fn leading_expired_end_stops_at_first_unexpired() {
    let now = IggyTimestamp::now();
    let expiry = IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_hours(1)));
    let segments = vec![
        segment(9, 1, 100, true),                // expired
        segment(19, now.as_micros(), 100, true), // recent: not expired, stops run
        segment(29, 1, 100, true),
        segment(39, 0, 100, false),
    ];
    assert_eq!(leading_expired_end(&segments, now, expiry), Some(9));
}

#[test]
fn leading_expired_end_none_for_never_expire() {
    let segments = vec![segment(9, 1, 100, true), segment(19, 0, 100, false)];
    assert_eq!(
        leading_expired_end(&segments, IggyTimestamp::now(), IggyExpiry::NeverExpire),
        None
    );
}

#[test]
fn leading_expired_end_none_for_lone_active_segment() {
    let segments = vec![segment(9, 1, 100, false)];
    assert_eq!(
        leading_expired_end(&segments, IggyTimestamp::now(), one_second()),
        None
    );
}

#[test]
fn leading_oversized_end_trims_oldest_until_under_budget() {
    // 3 x 100 = 300 SEALED resident, the active segment's bytes excluded.
    // Budget 250: drop seg0 (200 <= 250, stop). up_to = seg0.end_offset.
    let segments = vec![
        segment(9, 1, 100, true),
        segment(19, 2, 100, true),
        segment(29, 3, 100, true),
        segment(39, 0, 100, false),
    ];
    assert_eq!(leading_oversized_end(&segments, 250), Some(9));
}

#[test]
fn leading_oversized_end_none_when_under_budget() {
    let segments = vec![segment(9, 1, 100, true), segment(19, 0, 100, false)];
    assert_eq!(leading_oversized_end(&segments, 10_000), None);
}

#[test]
fn leading_oversized_end_never_drops_active_segment() {
    let segments = vec![segment(9, 1, 1_000, false)];
    assert_eq!(leading_oversized_end(&segments, 10), None);
}

#[test]
fn leading_oversized_end_retains_an_overshot_sealed_segment_at_the_floored_budget() {
    // The shape admission accepts at the floor: max_topic_size == one
    // segment. The sealed segment overshot, and the active one holds far
    // more than the budget. Counting the active segment, or dividing the
    // cap without a floor, deletes the only history this partition has.
    let segments = vec![
        segment(9, 1, SEALED_SIZE, true),
        segment(19, 0, SEGMENT_SIZE * 5, false),
    ];
    assert_eq!(leading_oversized_end(&segments, FLOORED_BUDGET), None);
    // The same segments against the UNFLOORED share, which is what a cap of
    // one segment divides into. It is under what a sealed segment reaches,
    // so the history goes -- which is why the budget carries a floor.
    assert_eq!(leading_oversized_end(&segments, SEGMENT_SIZE), Some(9));
}

#[test]
fn leading_oversized_end_still_drops_the_oldest_once_two_sealed_segments_exceed_the_budget() {
    // Same budget, one sealed segment more: the cap is real, not disabled.
    let segments = vec![
        segment(9, 1, SEALED_SIZE, true),
        segment(19, 2, SEALED_SIZE, true),
        segment(29, 0, SEGMENT_SIZE * 5, false),
    ];
    assert_eq!(
        leading_oversized_end(&segments, FLOORED_BUDGET),
        Some(9),
        "the oldest sealed segment goes, the newest one stays"
    );
}

#[test]
fn nth_oldest_sealed_end_resolves_count_to_offset() {
    let segments = vec![
        segment(9, 1, 100, true),
        segment(19, 2, 100, true),
        segment(29, 3, 100, true),
        segment(39, 0, 100, false), // active: excluded
    ];
    assert_eq!(nth_oldest_sealed_end(&segments, 1), Some(9));
    assert_eq!(nth_oldest_sealed_end(&segments, 2), Some(19));
    // More than available sealed: clamps to the last sealed segment.
    assert_eq!(nth_oldest_sealed_end(&segments, 10), Some(29));
    assert_eq!(nth_oldest_sealed_end(&segments, 0), None);
}

#[test]
fn nth_oldest_sealed_end_stops_at_first_unsealed() {
    let segments = vec![
        segment(9, 1, 100, true),
        segment(19, 2, 100, false), // unsealed mid-run stops the count
        segment(29, 3, 100, true),
        segment(39, 0, 100, false),
    ];
    assert_eq!(nth_oldest_sealed_end(&segments, 5), Some(9));
}

#[test]
fn nth_oldest_sealed_end_none_for_lone_active_segment() {
    let segments = vec![segment(9, 1, 100, false)];
    assert_eq!(nth_oldest_sealed_end(&segments, 1), None);
}

#[compio::test]
async fn removal_spends_the_per_pass_budget_and_resumes_on_the_next_pass() {
    let budget = u64::try_from(SEGMENT_REMOVAL_BUDGET_PER_PASS).expect("budget fits a u64");
    let sealed_count = budget + 3;
    let mut partition = partition_with_sealed_run(sealed_count);
    // The whole run qualifies: every sealed segment ends at or below
    // `up_to`, and a partition nobody has committed against has no barrier.
    let up_to = sealed_count * MESSAGES_PER_SEGMENT - 1;
    let segments_len = |partition: &IggyPartition<IggyMessageBus>| {
        u64::try_from(partition.log.segments().len()).expect("log fits a u64")
    };

    let removal = partition.remove_sealed_segments_up_to(up_to).await;
    assert_eq!(removal.segments, budget, "one pass stops at the budget");
    assert_eq!(removal.messages, budget * MESSAGES_PER_SEGMENT);
    assert!(
        removal.budget_spent,
        "a pass that stopped on the budget must ask to be re-staged"
    );
    assert_eq!(segments_len(&partition), sealed_count + 1 - budget);
    assert_eq!(
        partition.log.segments()[0].start_offset,
        budget * MESSAGES_PER_SEGMENT,
        "the surviving run starts where the budget stopped"
    );

    let remainder = sealed_count - budget;
    let removal = partition.remove_sealed_segments_up_to(up_to).await;
    assert_eq!(
        removal.segments, remainder,
        "a later pass finishes the run it was handed"
    );
    assert_eq!(removal.messages, remainder * MESSAGES_PER_SEGMENT);
    assert!(
        !removal.budget_spent,
        "a pass that drained the run must not re-stage"
    );
    assert_eq!(
        segments_len(&partition),
        1,
        "only the active segment survives"
    );

    let stats = &partition.stats;
    assert_eq!(stats.segments_count_inconsistent(), 1);
    assert_eq!(stats.messages_count_inconsistent(), 0);
    assert_eq!(stats.size_bytes_inconsistent(), 0);
    assert_eq!(
        partition.remove_sealed_segments_up_to(up_to).await,
        SegmentRemoval::default(),
        "a converged partition removes nothing"
    );

    // A run that ends exactly on the budget is finished by the pass that
    // spends it; re-staging would hand the pump a no-op frame.
    let mut exact = partition_with_sealed_run(budget);
    let removal = exact
        .remove_sealed_segments_up_to(budget * MESSAGES_PER_SEGMENT - 1)
        .await;
    assert_eq!(removal.segments, budget, "the whole run goes in one pass");
    assert!(
        !removal.budget_spent,
        "a run that ends on the budget has nothing left to re-stage"
    );
    assert_eq!(segments_len(&exact), 1, "only the active segment survives");
}
