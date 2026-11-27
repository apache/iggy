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

use super::eviction::{reboot, tiered_partition_on_disk};
use super::floor_eviction::{advance_over, apply};
use super::placed_batches::{
    commit_long_term_request, leader_with_three_long_term_segments, merged_of,
};
use super::remote_poll::layout;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{
    ExecutorManifestPublisher, MANIFEST_REPEAT_AFTER, MANIFEST_RETRY_AFTER,
    MANIFEST_RETRY_AFTER_MAX, MANIFEST_STORE_INTERVAL, ManifestPublishOutcome, ManifestPublisher,
    upload_segment,
};
use iggy_object_store::{
    InlineTierExecutor, MemoryStore, ObjectStore, ObjectStoreError, PartitionManifest, SegmentKey,
    SegmentObject, TierJobCancelled,
};
use std::time::Duration;

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops, as
/// `(first offset, last offset)`.
pub(super) const SEALED: [(u64, u64); 2] = [(0, 1), (2, 3)];

const STORED: ManifestPublishOutcome = Ok(Ok(()));

type Publisher = ExecutorManifestPublisher<InlineTierExecutor<MemoryStore>>;

fn publisher() -> Publisher {
    ExecutorManifestPublisher::new(InlineTierExecutor::new(MemoryStore::new()), layout())
}

/// A tiered partition whose store holds the first `stored` sealed segments,
/// as committed floor advances say.
pub(super) async fn partition_with_stored(
    stored: usize,
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) =
        Box::pin(tiered_partition(&segment_records(RECEIVER_OPS), 0)).await;
    for (op, segment) in (100..).zip(&SEALED[..stored]) {
        let advance = Box::pin(advance_over(directory.path(), *segment)).await;
        Box::pin(apply(&mut partition, op, advance)).await;
    }
    (directory, partition)
}

fn after(moment: IggyTimestamp, wait: std::time::Duration) -> IggyTimestamp {
    IggyTimestamp::from(moment.as_micros() + u64::try_from(wait.as_micros()).unwrap())
}

/// One pass that the store answers with `outcome`. Returns the manifest the
/// pass built, `None` when it started no publish.
fn pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    now: IggyTimestamp,
    outcome: &ManifestPublishOutcome,
) -> Option<PartitionManifest> {
    let (id, manifest, _scope) = partition.begin_manifest_publish(now)?;
    partition.finish_manifest_publish(id, outcome, now);
    Some(manifest)
}

fn listed(manifest: &PartitionManifest) -> Vec<(u64, u64)> {
    manifest
        .segments
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset))
        .collect()
}

/// Make the replica the primary of a later view, as after primacy moved away
/// and came back.
pub(super) fn regain_primacy(partition: &mut IggyPartition<IggyMessageBus>) {
    loop {
        let view = partition.consensus.view() + 1;
        partition.consensus.set_view(view);
        if partition.consensus.is_primary() {
            break;
        }
    }
}

#[compio::test]
async fn given_committed_advances_when_building_the_manifest_should_list_their_segments_as_the_chain_records_them()
 {
    let (_directory, partition) = Box::pin(partition_with_stored(2)).await;
    let now = IggyTimestamp::now();

    let manifest = partition.manifest(now).unwrap();

    assert_eq!(manifest.prefix, PREFIX);
    assert_eq!(manifest.published_at, now.as_micros());
    assert_eq!(manifest.floor, Some(SEALED[1].1));
    assert_eq!(manifest.trim_point, None);
    assert_eq!(listed(&manifest), SEALED);
    for segment in &manifest.segments {
        let record = partition
            .sealed_segment_record(segment.start_offset)
            .unwrap();
        assert_eq!(segment.size, record.size);
        assert_eq!(segment.hash, record.hash);
        assert_eq!(segment.crc32c, record.crc32c);
        assert_eq!(segment.max_timestamp, record.max_timestamp);
    }
}

#[compio::test]
async fn given_a_sealed_segment_no_advance_covers_when_building_the_manifest_should_leave_it_out() {
    let (_directory, partition) = Box::pin(partition_with_stored(1)).await;

    let manifest = partition.manifest(IggyTimestamp::now()).unwrap();

    assert_eq!(listed(&manifest), [SEALED[0]]);
    assert_eq!(manifest.floor, Some(SEALED[0].1));
}

#[compio::test]
async fn given_no_stored_segment_when_building_the_manifest_should_list_nothing() {
    let (_directory, partition) = Box::pin(partition_with_stored(0)).await;

    let manifest = partition.manifest(IggyTimestamp::now()).unwrap();

    assert_eq!(listed(&manifest), []);
    assert_eq!(manifest.floor, None);
}

/// The store holds the second segment with other bytes than this replica's,
/// so the replica cannot describe it.
#[compio::test]
async fn given_an_advance_this_replica_contradicts_when_building_the_manifest_should_build_none() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let other_bytes = AdvanceTieredFloor {
        start: SEALED[1].0,
        up_to: SEALED[1].1,
        hash: [7; 32],
    };
    Box::pin(apply(&mut partition, 200, other_bytes)).await;
    assert_eq!(partition.tiered_floor(), Some(SEALED[1].1));

    assert_eq!(partition.manifest(IggyTimestamp::now()), None);
    assert!(
        partition
            .begin_manifest_publish(IggyTimestamp::now())
            .is_none()
    );
}

#[compio::test]
async fn given_an_unchanged_manifest_when_passes_run_should_publish_once_until_it_changes() {
    let (directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();

    let first = pass(&mut partition, now, &STORED).unwrap();
    assert_eq!(listed(&first), [SEALED[0]]);
    assert_eq!(pass(&mut partition, now, &STORED), None);

    let advance = Box::pin(advance_over(directory.path(), SEALED[1])).await;
    Box::pin(apply(&mut partition, 200, advance)).await;

    let second = pass(&mut partition, now, &STORED).unwrap();
    assert_eq!(listed(&second), SEALED);
    assert_eq!(pass(&mut partition, now, &STORED), None);
}

#[compio::test]
async fn given_a_primary_with_nothing_stored_when_its_first_pass_runs_should_publish_an_empty_manifest()
 {
    let (_directory, mut partition) = Box::pin(partition_with_stored(0)).await;

    let manifest = pass(&mut partition, IggyTimestamp::now(), &STORED).unwrap();

    assert_eq!(listed(&manifest), []);
}

#[compio::test]
async fn given_a_first_manifest_when_the_repeat_wait_ends_should_publish_the_same_one_once_more() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let first = IggyTimestamp::now();
    pass(&mut partition, first, &STORED).unwrap();
    let just_inside = IggyTimestamp::from(after(first, MANIFEST_REPEAT_AFTER).as_micros() - 1);
    assert_eq!(pass(&mut partition, just_inside, &STORED), None);

    let due = after(first, MANIFEST_REPEAT_AFTER);
    let repeated = pass(&mut partition, due, &STORED).unwrap();

    assert_eq!(listed(&repeated), [SEALED[0]]);
    assert_eq!(pass(&mut partition, due, &STORED), None);
    assert_eq!(
        pass(&mut partition, after(due, MANIFEST_REPEAT_AFTER), &STORED),
        None
    );
}

#[compio::test]
async fn given_a_publish_the_store_refused_when_the_next_pass_runs_should_publish_again() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();
    let refused = Ok(Err(ObjectStoreError::Unavailable("503".to_owned())));

    assert!(pass(&mut partition, now, &refused).is_some());
    assert!(pass(&mut partition, now, &Err(TierJobCancelled)).is_some());
    assert!(pass(&mut partition, now, &STORED).is_some());
    assert_eq!(pass(&mut partition, now, &STORED), None);
}

#[compio::test]
async fn given_a_publish_in_flight_when_another_pass_runs_should_start_none() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();
    let (id, _manifest, _scope) = partition.begin_manifest_publish(now).unwrap();

    assert!(partition.begin_manifest_publish(now).is_none());

    // The result of a job the partition does not wait for changes nothing.
    partition.finish_manifest_publish(id + 1, &STORED, now);
    assert!(partition.begin_manifest_publish(now).is_none());
    partition.finish_manifest_publish(id, &STORED, now);
    assert!(partition.begin_manifest_publish(now).is_none());
}

#[compio::test]
async fn given_a_backup_when_a_pass_runs_should_publish_nothing_and_cancel_its_job() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();
    let (id, _manifest, scope) = partition.begin_manifest_publish(now).unwrap();
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    assert!(partition.begin_manifest_publish(now).is_none());

    assert!(scope.is_cancelled());
    // The result arrives after primacy moved, and counts for nothing.
    partition.finish_manifest_publish(id, &STORED, now);
    regain_primacy(&mut partition);
    assert!(partition.begin_manifest_publish(now).is_some());
}

/// Whatever the store holds was written by another primary, or by this one
/// before a put of another could land.
#[compio::test]
async fn given_a_primary_of_a_later_view_when_its_first_pass_runs_should_publish_an_unchanged_manifest()
 {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();
    pass(&mut partition, now, &STORED).unwrap();
    assert_eq!(pass(&mut partition, now, &STORED), None);

    regain_primacy(&mut partition);

    assert!(pass(&mut partition, now, &STORED).is_some());
}

#[compio::test]
async fn given_a_trim_point_the_manifest_does_not_hold_when_asking_should_make_object_deletes_wait()
{
    let (_directory, mut partition) = Box::pin(partition_with_stored(2)).await;
    let now = IggyTimestamp::now();
    assert!(!partition.object_deletes_await_manifest());
    pass(&mut partition, now, &STORED).unwrap();

    Box::pin(partition.remove_sealed_segments_up_to(SEALED[0].1)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    assert_eq!(partition.chain_trim_point(), Some(SEALED[0].1));

    assert!(partition.object_deletes_await_manifest());
    let manifest = pass(&mut partition, now, &STORED).unwrap();
    assert_eq!(manifest.trim_point, Some(SEALED[0].1));
    assert_eq!(listed(&manifest), [SEALED[1]]);
    assert!(!partition.object_deletes_await_manifest());

    // A later view stored nothing yet, so its deletes wait again.
    regain_primacy(&mut partition);
    assert!(partition.object_deletes_await_manifest());
}

/// A put that outlives its view reports back after the replica leads again.
/// Taken as the new view's publish, it would let deletes go ahead under a
/// manifest this view never stored.
#[compio::test]
async fn given_a_publish_of_an_earlier_view_when_its_result_arrives_during_the_next_views_publish_should_change_nothing()
 {
    let (_directory, mut partition) = Box::pin(partition_with_stored(2)).await;
    let now = IggyTimestamp::now();
    Box::pin(partition.remove_sealed_segments_up_to(SEALED[0].1)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    let (earlier, _manifest, _scope) = partition.begin_manifest_publish(now).unwrap();
    regain_primacy(&mut partition);
    let (id, _manifest, scope) = partition.begin_manifest_publish(now).unwrap();

    partition.finish_manifest_publish(earlier, &STORED, now);

    assert!(partition.object_deletes_await_manifest());
    assert!(!scope.is_cancelled());
    assert!(partition.begin_manifest_publish(now).is_none());
    partition.finish_manifest_publish(id, &STORED, now);
    assert!(!partition.object_deletes_await_manifest());
}

#[compio::test]
async fn given_a_backup_with_a_trim_point_when_asking_should_not_hold_its_delete_pass_back() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(2)).await;
    Box::pin(partition.remove_sealed_segments_up_to(SEALED[0].1)).await;
    Box::pin(settle_partition_io(&mut partition, &repair_config())).await;
    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());

    assert!(!partition.object_deletes_await_manifest());
}

#[compio::test]
async fn given_a_manifest_when_publishing_through_an_executor_should_store_it_under_the_manifest_key()
 {
    let (_directory, mut partition) = Box::pin(partition_with_stored(2)).await;
    let publisher = publisher();
    let now = IggyTimestamp::now();
    let (id, manifest, scope) = partition.begin_manifest_publish(now).unwrap();

    let outcome = publisher.publish(manifest.clone(), scope).await;
    partition.finish_manifest_publish(id, &outcome, now);

    assert!(matches!(outcome, Ok(Ok(()))), "{outcome:?}");
    let store = publisher.executor().store();
    let key = layout().manifest_key(&PREFIX);
    assert_eq!(store.keys(), std::slice::from_ref(&key));
    let size = store.head(&key).await.unwrap().size;
    let object = store.get_range(&key, 0..size).await.unwrap();
    assert_eq!(PartitionManifest::decode(&object.bytes), Ok(manifest));
}

/// Store the oldest sealed segment above the floor through `publisher`'s
/// store, and commit its advance as `op`.
async fn upload_and_advance(
    partition: &mut IggyPartition<IggyMessageBus>,
    publisher: &Publisher,
    op: u64,
) {
    let upload = partition.next_segment_upload().unwrap();
    upload_segment(publisher.executor().store(), &layout(), &upload)
        .await
        .unwrap();
    let advance = partition.note_segment_uploaded(&upload.record).unwrap();
    Box::pin(apply(partition, op, advance)).await;
}

/// One pass that `publisher` stores.
async fn published_pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    publisher: &Publisher,
    now: IggyTimestamp,
) {
    let (id, manifest, scope) = partition.begin_manifest_publish(now).unwrap();
    let outcome = publisher.publish(manifest, scope).await;
    partition.finish_manifest_publish(id, &outcome, now);
    assert!(matches!(outcome, Ok(Ok(()))), "{outcome:?}");
}

async fn manifest_in(store: &MemoryStore) -> PartitionManifest {
    let key = layout().manifest_key(&PREFIX);
    let size = store.head(&key).await.unwrap().size;
    let object = store.get_range(&key, 0..size).await.unwrap();
    PartitionManifest::decode(&object.bytes).unwrap()
}

/// The primary stores a segment and commits its advance, then crashes before
/// the pass that would publish it. The manifest it left behind names only
/// objects the store holds, and the restarted primary builds the next one
/// from its chain file, not from the one it finds in the store.
#[compio::test]
async fn given_a_crash_between_a_segment_upload_and_its_manifest_when_the_primary_restarts_should_catch_the_manifest_up()
 {
    let root = tempfile::tempdir().unwrap();
    let (config, mut partition) = Box::pin(tiered_partition_on_disk(root.path())).await;
    let publisher = publisher();
    let store = publisher.executor().store();
    let now = IggyTimestamp::now();
    Box::pin(upload_and_advance(&mut partition, &publisher, 100)).await;
    Box::pin(published_pass(&mut partition, &publisher, now)).await;

    Box::pin(upload_and_advance(&mut partition, &publisher, 101)).await;
    drop(partition);

    let behind = manifest_in(store).await;
    assert_eq!(listed(&behind), [SEALED[0]]);
    assert_eq!(behind.floor, Some(SEALED[0].1));
    for segment in &behind.segments {
        for object in [SegmentObject::Log, SegmentObject::Index] {
            let key = layout().segment_key(&SegmentKey {
                prefix: PREFIX,
                start_offset: segment.start_offset,
                object,
            });
            assert!(store.head(&key).await.is_ok(), "{key} is not stored");
        }
    }

    let mut rebooted = Box::pin(reboot(&config)).await;
    Box::pin(published_pass(&mut rebooted, &publisher, now)).await;

    let caught_up = manifest_in(store).await;
    assert_eq!(listed(&caught_up), SEALED);
    assert_eq!(caught_up.floor, Some(SEALED[1].1));
    assert_eq!(caught_up.trim_point, None);
}

/// A replica of a group of three that leads view 0 of a partition that
/// places its batches.
async fn placing_leader() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (directory, mut partition) =
        Box::pin(disk_poll_partition_with_replicas(&repair_config(), 3)).await;
    partition.set_places_batches(true);
    assert!(partition.consensus.is_primary());
    (directory, partition)
}

const REFUSED: ManifestPublishOutcome = Ok(Err(ObjectStoreError::Unavailable(String::new())));

/// How long after `moment` the partition owes its next pass.
fn owed_after(partition: &IggyPartition<IggyMessageBus>, moment: IggyTimestamp) -> Duration {
    let due_at = partition.manifest_pass_due_at().unwrap();
    Duration::from_micros(due_at.as_micros() - moment.as_micros())
}

#[compio::test]
async fn given_a_failed_publish_of_a_placing_primary_when_asking_should_owe_a_pass_only_after_its_backoff()
 {
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let failed_at = IggyTimestamp::now();
    assert_eq!(partition.manifest_pass_due_at(), None, "no pass ran yet");

    assert!(pass(&mut partition, failed_at, &REFUSED).is_some());

    assert_eq!(owed_after(&partition, failed_at), MANIFEST_RETRY_AFTER);
    let retried = pass(
        &mut partition,
        after(failed_at, MANIFEST_RETRY_AFTER),
        &STORED,
    );
    assert!(retried.is_some(), "the owed pass publishes");
}

/// The executor of this view dropped the job: the scope the partition holds
/// was not cancelled, so the put may never have run.
#[compio::test]
async fn given_a_job_cancelled_within_its_view_when_asking_should_owe_its_retry() {
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let cancelled_at = IggyTimestamp::now();

    assert!(pass(&mut partition, cancelled_at, &Err(TierJobCancelled)).is_some());

    assert_eq!(owed_after(&partition, cancelled_at), MANIFEST_RETRY_AFTER);
}

#[compio::test]
async fn given_publishes_that_keep_failing_when_asking_should_double_the_wait_up_to_its_most() {
    const FAILURES: u32 = 9;
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let mut failed_at = IggyTimestamp::now();
    let mut waits = Vec::new();

    for _ in 0..FAILURES {
        assert!(pass(&mut partition, failed_at, &REFUSED).is_some());
        let wait = owed_after(&partition, failed_at);
        waits.push(wait);
        failed_at = after(failed_at, wait);
    }

    let doubled: Vec<Duration> = (0..FAILURES)
        .map(|failure| (MANIFEST_RETRY_AFTER * 2_u32.pow(failure)).min(MANIFEST_RETRY_AFTER_MAX))
        .collect();
    assert_eq!(waits, doubled);
    assert_eq!(waits.last(), Some(&MANIFEST_RETRY_AFTER_MAX), "capped");
}

#[compio::test]
async fn given_failed_publishes_when_one_stores_should_clear_the_backoff() {
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let mut failed_at = IggyTimestamp::now();
    for _ in 0..3 {
        assert!(pass(&mut partition, failed_at, &REFUSED).is_some());
        failed_at = partition.manifest_pass_due_at().unwrap();
    }

    let stored_at = failed_at;
    assert!(pass(&mut partition, stored_at, &STORED).is_some());

    assert_eq!(
        owed_after(&partition, stored_at),
        MANIFEST_REPEAT_AFTER,
        "only the repeat is owed"
    );
    let repeat_at = after(stored_at, MANIFEST_REPEAT_AFTER);
    assert!(pass(&mut partition, repeat_at, &REFUSED).is_some());
    assert_eq!(
        owed_after(&partition, repeat_at),
        MANIFEST_RETRY_AFTER,
        "the next failure waits the shortest time"
    );
}

#[compio::test]
async fn given_a_first_manifest_of_a_placing_primary_when_asking_should_owe_the_repeat_after_its_wait()
 {
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let first = IggyTimestamp::now();
    assert!(pass(&mut partition, first, &STORED).is_some());

    assert_eq!(owed_after(&partition, first), MANIFEST_REPEAT_AFTER);

    let repeat_at = after(first, MANIFEST_REPEAT_AFTER);
    assert!(pass(&mut partition, repeat_at, &STORED).is_some());
    assert_eq!(partition.manifest_pass_due_at(), None, "repeated once");
}

#[compio::test]
async fn given_a_publish_in_flight_or_a_backup_when_asking_should_owe_no_pass() {
    let (_directory, mut partition) = Box::pin(placing_leader()).await;
    let now = IggyTimestamp::now();
    assert!(pass(&mut partition, now, &REFUSED).is_some());
    let due_at = partition.manifest_pass_due_at().unwrap();

    let (id, _manifest, _scope) = partition.begin_manifest_publish(due_at).unwrap();
    assert_eq!(partition.manifest_pass_due_at(), None, "a publish runs");
    partition.finish_manifest_publish(id, &REFUSED, due_at);
    assert!(partition.manifest_pass_due_at().is_some());

    let view = partition.consensus.view() + 1;
    partition.consensus.set_view(view);
    assert!(!partition.consensus.is_primary());
    assert_eq!(partition.manifest_pass_due_at(), None, "a backup");

    regain_primacy(&mut partition);
    assert_eq!(
        partition.manifest_pass_due_at(),
        None,
        "a new view owes its first pass instead"
    );
    assert!(partition.manifest_awaits_first_pass());
}

/// A leader of three whose long-term log holds three segments, which stored
/// the manifest of them at `stored_at` and then committed a replace of the
/// first two and a trim of what replaced them. Returns the segment left.
async fn changed_twice_after_a_store(
    stored_at: IggyTimestamp,
) -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    iggy_object_store::ManifestSegment,
) {
    let (directory, mut partition, [first, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    assert!(pass(&mut partition, stored_at, &STORED).is_some());
    let merged = merged_of(&first, &second);
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, replace, false)).await;
    assert!(
        pass(
            &mut partition,
            after(stored_at, Duration::from_millis(100)),
            &STORED
        )
        .is_none(),
        "the replace waits"
    );
    let trim = partition
        .trim_segments_request(merged.end_offset, 11)
        .unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    assert!(
        pass(
            &mut partition,
            after(stored_at, Duration::from_millis(900)),
            &STORED
        )
        .is_none(),
        "the trim waits"
    );
    (directory, partition, third)
}

/// Changes of the long-term log within the store interval share one store,
/// which lists the latest of them, owed once the interval passed with no
/// further change to ask for it.
#[compio::test]
async fn given_changes_of_the_long_term_log_within_the_store_interval_when_passes_run_should_store_one_manifest_of_the_latest_state()
 {
    let stored_at = IggyTimestamp::now();
    let (_directory, mut partition, third) = Box::pin(changed_twice_after_a_store(stored_at)).await;

    assert_eq!(owed_after(&partition, stored_at), MANIFEST_STORE_INTERVAL);
    let later = after(stored_at, MANIFEST_STORE_INTERVAL);
    let manifest = pass(&mut partition, later, &STORED).expect("the interval passed");

    assert_eq!(manifest.segments, [third]);
    assert_eq!(manifest.trim_point, Some(third.start_offset - 1));
    assert_eq!(manifest.floor, Some(third.end_offset));
    assert!(
        pass(
            &mut partition,
            after(later, MANIFEST_STORE_INTERVAL),
            &STORED
        )
        .is_none(),
        "no change, no store"
    );
    assert_eq!(
        owed_after(&partition, stored_at),
        MANIFEST_REPEAT_AFTER,
        "only the repeat is owed"
    );
}

/// A store that the interval held back and that then failed is retried
/// after its backoff, and the retry lists the latest state.
#[compio::test]
async fn given_a_held_store_that_fails_when_asking_should_retry_it_after_its_backoff() {
    let stored_at = IggyTimestamp::now();
    let (_directory, mut partition, third) = Box::pin(changed_twice_after_a_store(stored_at)).await;
    let failed_at = after(stored_at, MANIFEST_STORE_INTERVAL);
    assert!(pass(&mut partition, failed_at, &REFUSED).is_some());

    assert_eq!(owed_after(&partition, failed_at), MANIFEST_RETRY_AFTER);
    assert!(
        pass(
            &mut partition,
            after(failed_at, MANIFEST_RETRY_AFTER / 2),
            &STORED
        )
        .is_none(),
        "the backoff runs"
    );
    let retried = pass(
        &mut partition,
        after(failed_at, MANIFEST_RETRY_AFTER),
        &STORED,
    )
    .expect("the retry is due");
    assert_eq!(retried.segments, [third]);
}

/// The segment cleaner runs the passes of a tiered partition.
#[compio::test]
async fn given_a_tiered_partition_when_its_publish_fails_should_owe_no_pass() {
    let (_directory, mut partition) = Box::pin(partition_with_stored(1)).await;
    let now = IggyTimestamp::now();

    assert!(pass(&mut partition, now, &REFUSED).is_some());
    assert_eq!(partition.manifest_pass_due_at(), None);
    assert!(pass(&mut partition, now, &STORED).is_some());
    assert_eq!(partition.manifest_pass_due_at(), None);
}
