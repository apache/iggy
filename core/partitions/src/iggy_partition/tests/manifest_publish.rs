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

use super::floor_eviction::{advance_over, apply};
use super::remote_poll::layout;
use super::segment_chain::{PREFIX, tiered_partition};
use super::*;
use crate::tiered_floor::AdvanceTieredFloor;
use crate::{
    ExecutorManifestPublisher, MANIFEST_REPEAT_AFTER, ManifestPublishOutcome, ManifestPublisher,
};
use iggy_object_store::{
    InlineTierExecutor, MemoryStore, ObjectStore, ObjectStoreError, PartitionManifest,
    TierJobCancelled,
};

/// Sealed segments of a partition with [`RECEIVER_OPS`] one-message ops, as
/// `(first offset, last offset)`.
const SEALED: [(u64, u64); 2] = [(0, 1), (2, 3)];

const STORED: ManifestPublishOutcome = Ok(Ok(()));

type Publisher = ExecutorManifestPublisher<InlineTierExecutor<MemoryStore>>;

fn publisher() -> Publisher {
    ExecutorManifestPublisher::new(InlineTierExecutor::new(MemoryStore::new()), layout())
}

/// A tiered partition whose store holds the first `stored` sealed segments,
/// as committed floor advances say.
async fn partition_with_stored(
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
fn regain_primacy(partition: &mut IggyPartition<IggyMessageBus>) {
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
