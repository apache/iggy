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

use super::object_delete::{Deleter, Deletes, Faulty, after, deleter, put, store};
use super::placed_batches::{
    Registering, begin_registered_rewrite, commit_long_term_request,
    leader_with_three_long_term_segments, long_term_key, lose_primacy, merged_of,
    placing_leader_of_three_with, plan_up_to, regain_primacy, store_with, whole_and_placed, writer,
};
use super::remote_poll::layout;
use super::*;
use crate::long_term::{LONG_TERM_REGISTRATION_TTL, ObjectRegistration, concatenated};
use crate::{
    LONG_TERM_DELETE_DELAY, LONG_TERM_MERGE_MIN_BYTES, LONG_TERM_SWEEP_INTERVAL, LongTermNames,
    LongTermObject, LongTermWriter, MergePass, MergeSettled, MergeStart, ObjectDeleteDone,
    ObjectDeleteWork, ObjectDeleter, SegmentMerge, SweptLongTerm,
};
use iggy_object_store::{ManifestSegment, SEGMENT_HASH_LEN, SegmentObject};

fn delay() -> u64 {
    u64::try_from(LONG_TERM_DELETE_DELAY.as_micros()).unwrap()
}

fn store_interval() -> u64 {
    u64::try_from(crate::MANIFEST_STORE_INTERVAL.as_micros()).unwrap()
}

/// Store both objects of the long-term segment with these offsets.
async fn put_segment(store: &Faulty, start_offset: u64, end_offset: u64) {
    for object in SegmentObject::ALL {
        put(store, &long_term_key(start_offset, end_offset, object)).await;
    }
}

/// The long-term segments the store holds an object of, each as often as it
/// holds one.
fn stored_segments(store: &Faulty) -> Vec<(u64, u64)> {
    store
        .inner
        .keys()
        .iter()
        .filter_map(|key| layout().parse_long_term_key(key))
        .map(|key| (key.start_offset, key.end_offset))
        .collect()
}

/// A leader of three whose long-term log holds the offsets `0`, `1` and
/// `2..=3` as three segments, and a store that holds both objects of each.
async fn leader_with_stored_segments() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    [ManifestSegment; 3],
    Deleter,
) {
    let (directory, partition, segments) = Box::pin(leader_with_three_long_term_segments()).await;
    let deleter = deleter();
    for segment in &segments {
        put_segment(store(&deleter), segment.start_offset, segment.end_offset).await;
    }
    (directory, partition, segments, deleter)
}

/// One pass: start the next job, run it and hand its result back. Returns
/// what the job did, `None` when the pass started none.
async fn pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
    now: IggyTimestamp,
) -> Option<ObjectDeleteWork> {
    let (job, scope) = partition.begin_object_delete(now, 0)?;
    let (id, work) = (job.id, job.work.clone());
    let outcome = deleter.run(job, scope).await;
    partition.finish_object_delete(id, outcome, now);
    Some(work)
}

fn sweep_of(segments: &[(u64, u64)]) -> ObjectDeleteWork {
    ObjectDeleteWork::SweepLongTerm {
        log: LongTermNames {
            segments: segments.to_vec(),
            ..LongTermNames::default()
        },
    }
}

fn delete_of(segments: &[(u64, u64)]) -> ObjectDeleteWork {
    ObjectDeleteWork::DeleteLongTerm {
        segments: segments.to_vec(),
    }
}

/// Store the manifest of the long-term log as it is, at `now`.
fn store_manifest(partition: &mut IggyPartition<IggyMessageBus>, now: IggyTimestamp) {
    let (id, _, _scope) = partition
        .begin_manifest_publish(now)
        .expect("the manifest changed");
    partition.finish_manifest_publish(id, &Ok(Ok(())), now);
}

/// Commit a replace of `first` and `second` by the segment that holds both,
/// whose objects the store holds by then.
async fn replace_first_two(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
    first: &ManifestSegment,
    second: &ManifestSegment,
) -> ManifestSegment {
    let merged = merged_of(first, second);
    put_segment(store(deleter), merged.start_offset, merged.end_offset).await;
    let replace = partition.replace_segments_request(&merged, 10).unwrap();
    Box::pin(commit_long_term_request(partition, replace, false)).await;
    merged
}

/// The primary sees the replace commit, so it knows what the log stopped
/// listing without asking the store. The objects go an hour after the
/// manifest that stopped listing them, every `.index` before any `.log`.
#[compio::test]
async fn given_a_replace_on_the_primary_when_the_delay_after_its_manifest_ends_should_delete_what_it_replaced()
 {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, now)).await,
        Some(sweep_of(&[(0, 0), (1, 1), (2, 3)])),
        "the first job of a primary lists what the store holds"
    );
    store_manifest(&mut partition, now);
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    let stored_at = after(now, store_interval());
    store_manifest(&mut partition, stored_at);

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, after(now, delay() - 1))).await,
        None,
        "a reader of the manifest before may still read them"
    );
    let due = after(stored_at, delay());
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(sweep_of(&[(0, 1), (2, 3)])),
        "the listing is due again by then"
    );
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (1, 1)]))
    );

    assert_eq!(
        stored_segments(store(&deleter)),
        [(0, 1), (0, 1), (2, 3), (2, 3)]
    );
    assert_eq!(
        store(&deleter).delete_calls.take(),
        [2, 2],
        "both indexes, then both logs"
    );
    assert_eq!(Box::pin(pass(&mut partition, &deleter, due)).await, None);
}

/// The commit of a replace does not tell a reader anything: it goes by the
/// manifest in the store. So the hour counts from the manifest that leaves
/// the segments out, however long ago the replace committed.
#[compio::test]
async fn given_a_replace_whose_manifest_is_not_stored_when_time_passes_should_delete_nothing_until_an_hour_after_the_manifest()
 {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    Box::pin(pass(&mut partition, &deleter, now)).await;
    store_manifest(&mut partition, now);
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;

    let much_later = after(now, 3 * delay());
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, much_later)).await,
        Some(sweep_of(&[(0, 1), (2, 3)]))
    );
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, much_later)).await,
        None,
        "the manifest in the store still lists what the replace removed"
    );
    assert_eq!(stored_segments(store(&deleter)).len(), 8);

    store_manifest(&mut partition, much_later);
    assert_eq!(
        Box::pin(pass(
            &mut partition,
            &deleter,
            after(much_later, delay() - 1)
        ))
        .await,
        None
    );
    let due = after(much_later, delay());
    Box::pin(pass(&mut partition, &deleter, due)).await;
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (1, 1)]))
    );
}

#[compio::test]
async fn given_a_trim_on_the_primary_when_the_delay_after_its_manifest_ends_should_delete_what_it_removed()
 {
    let (_directory, mut partition, [_, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    Box::pin(pass(&mut partition, &deleter, now)).await;
    let trim = partition
        .trim_segments_request(second.end_offset, 10)
        .unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    store_manifest(&mut partition, now);

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, after(now, delay() - 1))).await,
        None
    );
    let due = after(now, delay());
    Box::pin(pass(&mut partition, &deleter, due)).await;
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (1, 1)]))
    );
    assert_eq!(stored_segments(store(&deleter)), [(2, 3), (2, 3)]);
}

/// What a primary queued is gone with its view. The next primary finds the
/// same objects by listing them, and waits an hour from its own manifest:
/// it does not know how long the one before waited. An object that the log
/// can still come to list is not dead, and stays.
#[compio::test]
async fn given_replaced_and_trimmed_objects_when_a_new_view_lists_them_should_delete_the_dead_ones_and_keep_the_unannounced()
 {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let merged = Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    let trim = partition
        .trim_segments_request(merged.end_offset, 11)
        .unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    // A segment that starts below the trim point, and one from where the
    // last segment starts to above the floor, as a primary stores that lost
    // to the one whose segment the log lists.
    put_segment(store(&deleter), 0, 3).await;
    put_segment(store(&deleter), 2, 9).await;
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    let now = IggyTimestamp::now();

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, now)).await,
        Some(sweep_of(&[(2, 3)]))
    );
    assert_eq!(partition.long_term_unannounced_objects(), 1);
    let much_later = after(now, 3 * delay());
    Box::pin(pass(&mut partition, &deleter, much_later)).await;
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, much_later)).await,
        None,
        "this view stored no manifest yet"
    );

    store_manifest(&mut partition, much_later);
    let due = after(much_later, delay());
    Box::pin(pass(&mut partition, &deleter, due)).await;
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (0, 1), (0, 3), (1, 1)]))
    );
    assert_eq!(
        stored_segments(store(&deleter)),
        [(2, 3), (2, 3), (2, 9), (2, 9)]
    );
    assert_eq!(partition.long_term_unannounced_objects(), 1);
}

/// A listing can end after the manifest of the state it was sorted by was
/// stored, and then no other manifest follows. The objects it found wait
/// from the end of the listing.
#[compio::test]
async fn given_a_manifest_stored_before_a_listing_when_the_listing_finds_dead_objects_should_delete_them_an_hour_later()
 {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    let now = IggyTimestamp::now();
    store_manifest(&mut partition, now);
    assert!(partition.begin_manifest_publish(now).is_none());

    let listed_at = after(now, 1_000);
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, listed_at)).await,
        Some(sweep_of(&[(0, 1), (2, 3)]))
    );

    assert_eq!(
        Box::pin(pass(
            &mut partition,
            &deleter,
            after(listed_at, delay() - 1)
        ))
        .await,
        None
    );
    let due = after(listed_at, delay());
    Box::pin(pass(&mut partition, &deleter, due)).await;
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (1, 1)]))
    );
}

/// A listing finds a segment by either object, and the `.log` is the one a
/// reader needs. A segment whose `.index` the store kept keeps its `.log`
/// and stays queued.
#[compio::test]
async fn given_an_index_the_store_keeps_when_deleting_dead_objects_should_keep_its_log_and_try_again()
 {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    Box::pin(pass(&mut partition, &deleter, now)).await;
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    store_manifest(&mut partition, now);
    let refused = long_term_key(1, 1, SegmentObject::Index);
    store(&deleter)
        .deletes
        .replace(Deletes::RefuseKey(refused.clone()));
    let due = after(now, delay());
    Box::pin(pass(&mut partition, &deleter, due)).await;

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(0, 0), (1, 1)]))
    );

    let keys = store(&deleter).inner.keys();
    assert!(keys.contains(&refused));
    assert!(keys.contains(&long_term_key(1, 1, SegmentObject::Log)));
    assert!(!keys.contains(&long_term_key(0, 0, SegmentObject::Log)));
    assert_eq!(
        store(&deleter).delete_calls.take(),
        [2, 1],
        "both indexes, then the log of the one whose index went"
    );
    store(&deleter).deletes.replace(Deletes::Work);
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, due)).await,
        Some(delete_of(&[(1, 1)]))
    );
    assert_eq!(
        stored_segments(store(&deleter)),
        [(0, 1), (0, 1), (2, 3), (2, 3)]
    );
}

#[compio::test]
async fn given_a_listing_that_fails_when_the_next_pass_runs_should_list_again() {
    let (_directory, mut partition, _, deleter) = Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    store(&deleter).lists_fail.set(true);
    assert!(
        Box::pin(pass(&mut partition, &deleter, now))
            .await
            .is_some()
    );

    store(&deleter).lists_fail.set(false);

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, now)).await,
        Some(sweep_of(&[(0, 0), (1, 1), (2, 3)]))
    );
    assert_eq!(Box::pin(pass(&mut partition, &deleter, now)).await, None);
}

/// A listing costs a request for every thousand objects of the partition,
/// so the primary lists once when it starts and then once an hour.
#[compio::test]
async fn given_a_listing_in_this_view_when_passes_run_should_list_again_after_the_interval() {
    let (_directory, mut partition, _, deleter) = Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    let interval = u64::try_from(LONG_TERM_SWEEP_INTERVAL.as_micros()).unwrap();
    assert!(
        Box::pin(pass(&mut partition, &deleter, now))
            .await
            .is_some()
    );

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, after(now, interval - 1))).await,
        None
    );
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, after(now, interval))).await,
        Some(sweep_of(&[(0, 0), (1, 1), (2, 3)]))
    );
}

#[compio::test]
async fn given_a_job_in_flight_when_another_pass_runs_should_start_none() {
    let (_directory, mut partition, _, _deleter) = Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();

    let (job, _scope) = partition.begin_object_delete(now, 0).unwrap();

    assert!(partition.begin_object_delete(now, 0).is_none());
    partition.finish_object_delete(
        job.id,
        Ok(ObjectDeleteDone::LongTermSwept(
            Ok(SweptLongTerm::default()),
        )),
        now,
    );
    assert!(partition.begin_object_delete(now, 0).is_none());
}

/// The result of a listing belongs to the view that started it. A replica
/// that lost primacy meanwhile queues nothing of it, and lists again when
/// it leads.
#[compio::test]
async fn given_a_listing_in_flight_when_primacy_moves_should_drop_its_result() {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    let now = IggyTimestamp::now();
    let (job, scope) = partition.begin_object_delete(now, 0).unwrap();
    let outcome = deleter.run(job.clone(), scope).await;
    lose_primacy(&mut partition);

    partition.finish_object_delete(job.id, outcome, now);

    assert!(partition.begin_object_delete(now, 0).is_none());
    regain_primacy(&mut partition);
    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, now)).await,
        Some(sweep_of(&[(0, 1), (2, 3)]))
    );
}

#[compio::test]
async fn given_a_backup_when_a_pass_runs_should_start_no_job() {
    let (_directory, mut partition, _, deleter) = Box::pin(leader_with_stored_segments()).await;
    lose_primacy(&mut partition);

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, IggyTimestamp::now())).await,
        None
    );
    assert_eq!(partition.long_term_unannounced_objects(), 0);
}

/// One pass, as [`pass`], that returns what its job did and how many
/// objects the partition counts as deleted by it.
async fn counted_pass(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
    now: IggyTimestamp,
) -> (ObjectDeleteWork, usize) {
    let (job, scope) = partition.begin_object_delete(now, 0).unwrap();
    let (id, work) = (job.id, job.work.clone());
    let outcome = deleter.run(job, scope).await;
    (work, partition.finish_object_delete(id, outcome, now))
}

/// What waits for its delete and what a delete removed are what an operator
/// reads the store's garbage from. A listing removes nothing, and an object
/// the store kept is not counted as gone.
#[compio::test]
async fn given_dead_objects_when_passes_run_should_count_the_queued_and_the_deleted() {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    let now = IggyTimestamp::now();
    assert_eq!(partition.queued_dead_long_term_objects(), 0);
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    assert_eq!(partition.queued_dead_long_term_objects(), 2);
    store_manifest(&mut partition, now);
    let due = after(now, delay());

    let (listing, counted) = Box::pin(counted_pass(&mut partition, &deleter, due)).await;
    assert_eq!(listing, sweep_of(&[(0, 1), (2, 3)]));
    assert_eq!(counted, 0);
    store(&deleter)
        .deletes
        .replace(Deletes::RefuseKey(long_term_key(
            0,
            0,
            SegmentObject::Index,
        )));
    let (delete, counted) = Box::pin(counted_pass(&mut partition, &deleter, due)).await;
    assert_eq!(delete, delete_of(&[(0, 0), (1, 1)]));
    assert_eq!(counted, 1);
    assert_eq!(partition.queued_dead_long_term_objects(), 1);

    store(&deleter).deletes.replace(Deletes::Work);
    let (delete, counted) = Box::pin(counted_pass(&mut partition, &deleter, due)).await;
    assert_eq!(delete, delete_of(&[(0, 0)]));
    assert_eq!(counted, 1);
    assert_eq!(partition.queued_dead_long_term_objects(), 0);
}

/// What a replica queued while it led is not what it deletes once it does
/// not, so it reports none of it.
#[compio::test]
async fn given_dead_objects_queued_by_a_primary_when_it_is_demoted_should_report_none() {
    let (_directory, mut partition, [first, second, _], deleter) =
        Box::pin(leader_with_stored_segments()).await;
    Box::pin(replace_first_two(&mut partition, &deleter, &first, &second)).await;
    assert_eq!(partition.queued_dead_long_term_objects(), 2);

    lose_primacy(&mut partition);

    assert_eq!(partition.queued_dead_long_term_objects(), 0);
}

const MIB: u64 = 1024 * 1024;

fn ttl() -> u64 {
    u64::try_from(LONG_TERM_REGISTRATION_TTL.as_micros()).unwrap()
}

/// A long-term segment of the one offset `offset`, of `size` bytes.
fn sized_segment(offset: u64, size: u64) -> ManifestSegment {
    ManifestSegment {
        start_offset: offset,
        end_offset: offset,
        start_timestamp: 0,
        end_timestamp: 0,
        max_timestamp: 0,
        size,
        crc32c: 0,
        hash: [0; SEGMENT_HASH_LEN],
    }
}

/// Commit `request` to the long-term log as the primary does.
async fn commit(
    partition: &mut IggyPartition<IggyMessageBus>,
    request: Message<RoutedRequestHeader>,
) {
    Box::pin(commit_long_term_request(partition, request, false)).await;
}

/// Publish a segment of one offset of `size` bytes after the floor, and
/// store both of its objects.
async fn publish_sized(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
    size: u64,
) {
    let offset = partition.long_term_floor().map_or(0, |floor| floor + 1);
    let request = partition
        .publish_segment_request(&sized_segment(offset, size), 7)
        .unwrap();
    Box::pin(commit(partition, request)).await;
    put_segment(store(deleter), offset, offset).await;
}

/// A leader of three whose long-term log holds a segment of one offset for
/// each of `sizes`, and a store that holds both objects of each.
async fn leader_with_sized_segments(
    sizes: &[u64],
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>, Deleter) {
    let (_whole, placed, object) = whole_and_placed(1);
    let store = Box::pin(store_with(object)).await;
    let (directory, mut partition) = Box::pin(placing_leader_of_three_with(&placed, &store)).await;
    let deleter = deleter();
    for size in sizes {
        Box::pin(publish_sized(&mut partition, &deleter, *size)).await;
    }
    (directory, partition, deleter)
}

/// Commit the registration of the best merge at `now` and start the merge,
/// as the merge passes of a primary do.
async fn start_merge(
    partition: &mut IggyPartition<IggyMessageBus>,
    now: IggyTimestamp,
) -> (u64, SegmentMerge) {
    let Some(MergeStart::Register(registration)) =
        partition.begin_segment_merge(MergePass::Scan, now)
    else {
        panic!("a merge registers what it stores first");
    };
    let request = partition.register_object_request(&registration, 8).unwrap();
    Box::pin(commit(partition, request)).await;
    let Some(MergeStart::Merge(id, merge, _scope)) =
        partition.begin_segment_merge(MergePass::Scan, now)
    else {
        panic!("the registration committed, so the merge starts");
    };
    (id, merge)
}

/// What a merge of `merge` stores, after it stored both of its objects.
async fn store_merged(deleter: &Deleter, merge: &SegmentMerge) -> ManifestSegment {
    let merged = concatenated(&merge.sources, [9; SEGMENT_HASH_LEN]).unwrap();
    put_segment(store(deleter), merged.start_offset, merged.end_offset).await;
    merged
}

/// The first and the last offset the sources of `merge` hold.
fn span_of(merge: &SegmentMerge) -> (u64, u64) {
    (
        merge.sources[0].start_offset,
        merge.sources[merge.sources.len() - 1].end_offset,
    )
}

/// Expire the registrations that are due at `now`, as the primary's
/// retention pass does. Whether it found any.
async fn expire_due(partition: &mut IggyPartition<IggyMessageBus>, now: IggyTimestamp) -> bool {
    let Some(before) = partition.long_term_registrations_expiry(now) else {
        return false;
    };
    let request = partition.expire_registrations_request(before, 12).unwrap();
    Box::pin(commit(partition, request)).await;
    true
}

/// Run passes at `now` until one starts nothing.
async fn passes(
    partition: &mut IggyPartition<IggyMessageBus>,
    deleter: &Deleter,
    now: IggyTimestamp,
) {
    for _ in 0..8 {
        if Box::pin(pass(partition, deleter, now)).await.is_none() {
            return;
        }
    }
    panic!("passes at one moment end");
}

/// A primary registers a merge and stores its object, and loses primacy
/// before the replace commits. The next primary merges another run, so the
/// segments still start and end where the object does and never make it
/// dead. Its registration does, once it is older than the time limit, and
/// the object goes the delete delay after that.
#[compio::test]
async fn given_a_merge_whose_primary_changed_when_the_next_primary_merges_another_run_should_delete_its_object_once_the_registration_expires()
 {
    let healthy = LONG_TERM_MERGE_MIN_BYTES;
    let (_directory, mut partition, deleter) = Box::pin(leader_with_sized_segments(&[
        MIB, MIB, MIB, healthy, MIB, MIB,
    ]))
    .await;
    let registered_at = IggyTimestamp::now();
    let (id, merge) = Box::pin(start_merge(&mut partition, registered_at)).await;
    assert_eq!(span_of(&merge), (0, 2), "the run that gains most");
    let abandoned = Box::pin(store_merged(&deleter, &merge)).await;

    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    assert_eq!(partition.finish_segment_merge(id, Ok(Ok(abandoned))), None);
    for _ in 0..2 {
        Box::pin(publish_sized(&mut partition, &deleter, MIB)).await;
    }
    let next = after(registered_at, 1_000_000);
    let (id, merge) = Box::pin(start_merge(&mut partition, next)).await;
    assert_eq!(span_of(&merge), (4, 7), "the other run gains more now");
    let merged = Box::pin(store_merged(&deleter, &merge)).await;
    assert_eq!(
        partition.finish_segment_merge(id, Ok(Ok(merged))),
        Some(merged)
    );
    let replace = partition.replace_segments_request(&merged, 9).unwrap();
    Box::pin(commit(&mut partition, replace)).await;
    assert_eq!(
        partition.settle_segment_merge(),
        MergeSettled::Committed { replaced: 4 }
    );
    store_manifest(&mut partition, next);
    assert_eq!(
        partition.long_term_object(0, 2),
        LongTermObject::Registered,
        "segments still start and end where it does"
    );
    Box::pin(passes(&mut partition, &deleter, next)).await;
    assert_eq!(partition.long_term_unannounced_objects(), 0);

    assert!(
        !Box::pin(expire_due(&mut partition, after(registered_at, ttl()))).await,
        "not before the time limit"
    );
    let expired_at = after(registered_at, ttl() + 1);
    assert!(Box::pin(expire_due(&mut partition, expired_at)).await);
    assert_eq!(partition.long_term_object(0, 2), LongTermObject::Dead);
    Box::pin(passes(&mut partition, &deleter, expired_at)).await;
    Box::pin(passes(
        &mut partition,
        &deleter,
        after(expired_at, delay() - 1),
    ))
    .await;
    assert!(stored_segments(store(&deleter)).contains(&(0, 2)));

    Box::pin(passes(&mut partition, &deleter, after(expired_at, delay()))).await;

    let mut stored = stored_segments(store(&deleter));
    stored.dedup();
    assert_eq!(
        stored,
        [(0, 0), (1, 1), (2, 2), (3, 3), (4, 7)],
        "the store holds what the log lists, and nothing else"
    );
}

/// The registration of the merge this primary runs, or whose replace has
/// yet to commit, is never expired, however long it takes. The expiry still
/// takes what an earlier primary left.
#[compio::test]
async fn given_a_merged_object_the_primary_has_not_listed_when_registrations_expire_should_never_delete_it()
 {
    let healthy = LONG_TERM_MERGE_MIN_BYTES;
    let (_directory, mut partition, deleter) = Box::pin(leader_with_sized_segments(&[
        MIB, MIB, healthy, MIB, MIB, MIB,
    ]))
    .await;
    let registered_at = IggyTimestamp::now();
    // What a primary before this one registered and stored.
    let earlier = ObjectRegistration {
        start_offset: 0,
        end_offset: 1,
        registered_at: registered_at.as_micros() - 1,
    };
    let request = partition.register_object_request(&earlier, 8).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    put_segment(store(&deleter), 0, 1).await;
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    let (id, merge) = Box::pin(start_merge(&mut partition, registered_at)).await;
    assert_eq!(span_of(&merge), (3, 5));
    let merged = Box::pin(store_merged(&deleter, &merge)).await;
    assert_eq!(
        partition.finish_segment_merge(id, Ok(Ok(merged))),
        Some(merged)
    );
    store_manifest(&mut partition, registered_at);

    for limits in 1..=3 {
        let now = after(registered_at, limits * (ttl() + delay()));
        Box::pin(expire_due(&mut partition, now)).await;
        Box::pin(passes(&mut partition, &deleter, now)).await;
        assert_eq!(
            partition.settle_segment_merge(),
            MergeSettled::Waiting(merged)
        );
        assert_eq!(partition.long_term_object(3, 5), LongTermObject::Registered);
        assert!(stored_segments(store(&deleter)).contains(&(3, 5)));
    }
    assert_eq!(partition.long_term_object(0, 1), LongTermObject::Dead);
    assert!(!stored_segments(store(&deleter)).contains(&(0, 1)));
    assert_eq!(partition.long_term_unannounced_objects(), 0);
}

/// The segment a rewrite stored is the next one the log lists, and its
/// registration waits for that publish, however late it comes.
#[compio::test]
async fn given_a_rewritten_segment_the_primary_has_not_published_when_registrations_expire_should_never_delete_it()
 {
    let (_whole, placed, object) = whole_and_placed(3);
    let rewrites = Box::pin(store_with(object)).await;
    let (_directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&placed, &rewrites)).await;
    let deleter = deleter();
    let registered_at = IggyTimestamp::now();
    let (id, rewrite, scope) = Box::pin(begin_registered_rewrite(
        &mut partition,
        Registering::Committed,
    ))
    .await
    .unwrap();
    let outcome = writer(&rewrites).write(rewrite, scope).await;
    let stored = partition.finish_segment_rewrite(id, outcome).unwrap();
    assert_eq!((stored.start_offset, stored.end_offset), (0, 2));
    put_segment(store(&deleter), 0, 2).await;

    let much_later = after(registered_at, 3 * (ttl() + delay()));
    assert!(!Box::pin(expire_due(&mut partition, much_later)).await);
    let request = partition
        .expire_registrations_request(much_later.as_micros(), 12)
        .unwrap();
    Box::pin(commit(&mut partition, request)).await;
    Box::pin(passes(&mut partition, &deleter, much_later)).await;
    Box::pin(passes(&mut partition, &deleter, after(much_later, delay()))).await;

    assert_eq!(partition.long_term_object(0, 2), LongTermObject::Registered);
    assert_eq!(partition.unpublished_segment(), Some(stored));
    assert_eq!(stored_segments(store(&deleter)), [(0, 2), (0, 2)]);
}

/// A primary stored the next segment and lost primacy before its publish
/// committed. The next primary published a shorter one, then one that ends
/// where the first did, so the segments start and end where the first
/// object does and never make it dead. It was registered before it was
/// stored, and goes once the registration expires.
#[compio::test]
async fn given_the_segment_of_a_deposed_primary_when_the_next_one_lists_its_offsets_in_two_segments_should_delete_it_once_its_registration_expires()
 {
    let (_whole, placed, object) = whole_and_placed(4);
    let rewrites = Box::pin(store_with(object)).await;
    let (_directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&placed, &rewrites)).await;
    let deleter = deleter();
    let registered_at = IggyTimestamp::now();
    let (id, rewrite, scope) = Box::pin(begin_registered_rewrite(
        &mut partition,
        Registering::Committed,
    ))
    .await
    .unwrap();
    let outcome = writer(&rewrites).write(rewrite, scope).await;
    let abandoned = partition.finish_segment_rewrite(id, outcome).unwrap();
    assert_eq!((abandoned.start_offset, abandoned.end_offset), (0, 3));
    put_segment(store(&deleter), 0, 3).await;

    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    Box::pin(plan_up_to(&mut partition, 1, Registering::Committed)).await;
    for _ in 0..2 {
        let (id, rewrite, scope) = Box::pin(begin_registered_rewrite(
            &mut partition,
            Registering::Committed,
        ))
        .await
        .unwrap();
        let outcome = writer(&rewrites).write(rewrite, scope).await;
        let segment = partition.finish_segment_rewrite(id, outcome).unwrap();
        put_segment(store(&deleter), segment.start_offset, segment.end_offset).await;
        let publish = partition.publish_segment_request(&segment, 7).unwrap();
        Box::pin(commit(&mut partition, publish)).await;
    }
    let listed: Vec<(u64, u64)> = partition
        .long_term_segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset))
        .collect();
    assert_eq!(listed, [(0, 1), (2, 3)]);
    assert_eq!(partition.long_term_object(0, 3), LongTermObject::Registered);
    let now = after(registered_at, 60_000_000);
    store_manifest(&mut partition, now);
    Box::pin(passes(&mut partition, &deleter, now)).await;

    let expired_at = after(now, ttl());
    assert!(Box::pin(expire_due(&mut partition, expired_at)).await);
    Box::pin(passes(&mut partition, &deleter, expired_at)).await;
    Box::pin(passes(&mut partition, &deleter, after(expired_at, delay()))).await;

    assert_eq!(partition.long_term_object(0, 3), LongTermObject::Dead);
    let mut stored = stored_segments(store(&deleter));
    stored.dedup();
    assert_eq!(stored, [(0, 1), (2, 3)]);
}

/// The registration is replicated state: it outlives the view that made it.
/// The next primary that picks the same run registers it again and takes
/// the stored object as its own.
#[compio::test]
async fn given_a_registered_merge_when_primacy_moves_should_keep_the_registration_and_let_the_next_primary_list_the_object()
 {
    let (_directory, mut partition, deleter) =
        Box::pin(leader_with_sized_segments(&[MIB, MIB, MIB])).await;
    let registered_at = IggyTimestamp::now();
    let (_, merge) = Box::pin(start_merge(&mut partition, registered_at)).await;
    let stored = Box::pin(store_merged(&deleter, &merge)).await;

    lose_primacy(&mut partition);
    assert_eq!(
        partition.long_term_log().unwrap().registrations(),
        [ObjectRegistration {
            start_offset: 0,
            end_offset: 2,
            registered_at: registered_at.as_micros(),
        }]
    );
    assert_eq!(
        partition.long_term_registrations_expiry(after(registered_at, 2 * ttl())),
        None
    );
    regain_primacy(&mut partition);
    let next = after(registered_at, 1_000_000);
    let (id, merge) = Box::pin(start_merge(&mut partition, next)).await;
    assert_eq!(
        partition.long_term_log().unwrap().registration(0, 2),
        Some(&ObjectRegistration {
            start_offset: 0,
            end_offset: 2,
            registered_at: next.as_micros(),
        }),
        "the later registration stands"
    );
    let merged = Box::pin(store_merged(&deleter, &merge)).await;
    assert_eq!(merged, stored, "the same offsets hold the same bytes");
    assert_eq!(
        partition.finish_segment_merge(id, Ok(Ok(merged))),
        Some(merged)
    );
    let replace = partition.replace_segments_request(&merged, 9).unwrap();
    Box::pin(commit(&mut partition, replace)).await;

    assert_eq!(partition.long_term_object(0, 2), LongTermObject::Listed);
    assert_eq!(partition.long_term_log().unwrap().registrations(), []);
}

/// A publish that ends past the end of a registered next segment leaves its
/// object dead, and the primary queues it on the commit without a listing.
#[compio::test]
async fn given_a_registered_next_segment_when_a_longer_one_is_published_should_queue_its_object() {
    let (_directory, mut partition, deleter) = Box::pin(leader_with_sized_segments(&[MIB])).await;
    let shorter = ObjectRegistration {
        start_offset: 1,
        end_offset: 1,
        registered_at: IggyTimestamp::now().as_micros(),
    };
    let request = partition.register_object_request(&shorter, 8).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    put_segment(store(&deleter), 1, 1).await;
    assert_eq!(partition.queued_dead_long_term_objects(), 0);

    let longer = ManifestSegment {
        end_offset: 2,
        ..sized_segment(1, MIB)
    };
    let request = partition.publish_segment_request(&longer, 7).unwrap();
    Box::pin(commit(&mut partition, request)).await;

    assert_eq!(partition.long_term_object(1, 1), LongTermObject::Dead);
    assert_eq!(partition.queued_dead_long_term_objects(), 1);
}

/// A listing sorts what it finds by the names the log had when it started.
/// An object registered and stored while it ran is not one that nothing
/// accounts for.
#[compio::test]
async fn given_an_object_registered_and_stored_while_a_listing_runs_when_it_ends_should_not_count_it_as_unannounced()
 {
    let (_directory, mut partition, deleter) =
        Box::pin(leader_with_sized_segments(&[MIB, MIB])).await;
    let now = IggyTimestamp::now();
    let (job, scope) = partition.begin_object_delete(now, 0).unwrap();
    let registration = ObjectRegistration {
        start_offset: 0,
        end_offset: 1,
        registered_at: now.as_micros(),
    };
    let request = partition.register_object_request(&registration, 8).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    put_segment(store(&deleter), 0, 1).await;

    let outcome = deleter.run(job.clone(), scope).await;
    partition.finish_object_delete(job.id, outcome, now);

    assert_eq!(partition.long_term_unannounced_objects(), 0);
    assert_eq!(partition.long_term_object(0, 1), LongTermObject::Registered);
}

/// A log that takes no more registrations still takes the next segment: a
/// rewrite that waited for a registration would stop the partition's floor.
#[compio::test]
async fn given_a_log_that_takes_no_more_registrations_when_a_rewrite_pass_runs_should_store_the_segment_without_one()
 {
    let (_whole, placed, object) = whole_and_placed(3);
    let rewrites = Box::pin(store_with(object)).await;
    let (_directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&placed, &rewrites)).await;
    let most = crate::long_term::LONG_TERM_REGISTRATIONS_MAX as u64;
    for (op, end_offset) in (100..).zip(3..3 + most) {
        let registration = ObjectRegistration {
            start_offset: 0,
            end_offset,
            registered_at: 1,
        };
        Box::pin(super::placed_batches::register_object(
            &mut partition,
            &registration,
            Registering::AsOp(op),
        ))
        .await;
    }
    assert!(partition.long_term_log().unwrap().registrations_full());

    let (id, rewrite, scope) = Box::pin(begin_registered_rewrite(
        &mut partition,
        Registering::AsOp(100 + most),
    ))
    .await
    .expect("the rewrite starts");
    let outcome = writer(&rewrites).write(rewrite, scope).await;
    let stored = partition.finish_segment_rewrite(id, outcome).unwrap();

    assert_eq!((stored.start_offset, stored.end_offset), (0, 2));
    assert_eq!(
        partition.long_term_object(0, 2),
        LongTermObject::Unannounced
    );
}

/// An earlier primary registered the same run. The registration of this
/// view, at a later time, commits before the merge stores anything, so the
/// time limit of the object counts from the primary that stores it last.
#[compio::test]
async fn given_a_registration_of_the_same_run_from_an_earlier_view_when_a_merge_pass_runs_should_register_it_again_before_merging()
 {
    let (_directory, mut partition, _deleter) =
        Box::pin(leader_with_sized_segments(&[MIB, MIB, MIB])).await;
    let earlier = ObjectRegistration {
        start_offset: 0,
        end_offset: 2,
        registered_at: 1,
    };
    let request = partition.register_object_request(&earlier, 8).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    let now = IggyTimestamp::now();

    for _ in 0..2 {
        let Some(MergeStart::Register(registration)) =
            partition.begin_segment_merge(MergePass::Scan, now)
        else {
            panic!("the merge waits for its own registration");
        };
        assert_eq!(registration.key(), (0, 2));
        assert_eq!(registration.registered_at, now.as_micros());
    }
    let (_, merge) = Box::pin(start_merge(&mut partition, now)).await;
    assert_eq!(span_of(&merge), (0, 2));
}

/// The same for the next segment: a plan that finds the offsets an earlier
/// primary registered registers them again before the rewrite stores them.
#[compio::test]
async fn given_a_registration_of_the_next_segment_from_an_earlier_view_when_a_rewrite_pass_runs_should_register_it_again_before_storing()
 {
    let (_whole, placed, object) = whole_and_placed(3);
    let rewrites = Box::pin(store_with(object)).await;
    let (_directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&placed, &rewrites)).await;
    let earlier = ObjectRegistration {
        start_offset: 0,
        end_offset: 2,
        registered_at: 1,
    };
    let request = partition.register_object_request(&earlier, 8).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    lose_primacy(&mut partition);
    regain_primacy(&mut partition);
    let Some(crate::RewriteStart::Plan(id, plan)) = partition.begin_segment_rewrite() else {
        panic!("a pass without a plan starts one");
    };
    let outcome = plan.run().await;
    let now = IggyTimestamp::now();
    let registration = partition.finish_segment_plan(id, outcome, now).unwrap();
    assert_eq!(registration.key(), (0, 2));

    for _ in 0..2 {
        let Some(crate::RewriteStart::Register(again)) = partition.begin_segment_rewrite() else {
            panic!("the rewrite waits for its own registration");
        };
        assert_eq!(again, registration);
    }
    let request = partition.register_object_request(&registration, 9).unwrap();
    Box::pin(commit(&mut partition, request)).await;
    assert!(matches!(
        partition.begin_segment_rewrite(),
        Some(crate::RewriteStart::Rewrite(..))
    ));
}

/// A rewrite stores only the offsets its registration names. One whose
/// batches end elsewhere stores nothing, and the next pass plans again.
#[compio::test]
async fn given_a_rewrite_whose_batches_end_elsewhere_than_its_registration_when_it_runs_should_store_nothing_and_plan_again()
 {
    let (_whole, placed, object) = whole_and_placed(3);
    let rewrites = Box::pin(store_with(object)).await;
    let (_directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&placed, &rewrites)).await;
    let (id, mut rewrite, scope) = Box::pin(begin_registered_rewrite(
        &mut partition,
        Registering::Committed,
    ))
    .await
    .unwrap();
    assert_eq!(rewrite.up_to, 2);
    let keys = rewrites.keys();
    // Past the last committed offset, which no batch ends at.
    rewrite.up_to = 5;

    let outcome = writer(&rewrites).write(rewrite, scope).await;

    assert!(matches!(
        outcome,
        Ok(Err(crate::RewriteError::Unregistered { end_offset: 2 }))
    ));
    assert_eq!(partition.finish_segment_rewrite(id, outcome), None);
    assert_eq!(rewrites.keys(), keys);
    assert!(matches!(
        partition.begin_segment_rewrite(),
        Some(crate::RewriteStart::Plan(..))
    ));
}
