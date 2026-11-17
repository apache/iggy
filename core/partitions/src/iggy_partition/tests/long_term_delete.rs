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
    commit_long_term_request, leader_with_three_long_term_segments, long_term_key, lose_primacy,
    merged_of, regain_primacy,
};
use super::remote_poll::layout;
use super::*;
use crate::{
    LONG_TERM_DELETE_DELAY, LONG_TERM_SWEEP_INTERVAL, ObjectDeleteDone, ObjectDeleteWork,
    ObjectDeleter, SweptLongTerm,
};
use iggy_object_store::{ManifestSegment, SegmentObject};

fn delay() -> u64 {
    u64::try_from(LONG_TERM_DELETE_DELAY.as_micros()).unwrap()
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
        segments: segments.to_vec(),
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
    store_manifest(&mut partition, now);

    assert_eq!(
        Box::pin(pass(&mut partition, &deleter, after(now, delay() - 1))).await,
        None,
        "a reader of the manifest before may still read them"
    );
    let due = after(now, delay());
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
