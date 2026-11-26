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

use super::placed_batches::{
    FRESH_OBJECT, commit_long_term_request, commit_published_segment,
    leader_with_three_long_term_segments, lose_primacy, placing_leader_of_three_with,
    placing_partition_of_small_segments, store_segment_up_to, store_with, whole_and_placed,
    whole_and_placed_in,
};
use super::remote_poll::{layout, polled_all, polled_all_reading};
use super::*;
use crate::long_term::{TRIM_SEGMENTS, TrimSegments};
use crate::placed_read::LONG_TERM_SEGMENTS_PER_POLL;
use crate::state_machine::{Applied, MachineCommand};
use crate::{PartitionReadSettings, ReadCache, ReadCacheSettings, RemoteObjects, RemoteSegments};
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::{IggyDuration, IggyExpiry, PollingStrategy};
use iggy_object_store::{
    ManifestSegment, MemoryStore, ObjectRange, ObjectStore, ObjectStoreError, PutBody, PutOptions,
    SEGMENT_HASH_LEN,
};
use std::cell::RefCell;
use std::ops::Range;
use std::time::Duration;

const SECOND_OBJECT: u128 = 0x52;

const ONE_SECOND_MICROS: u64 = 1_000_000;

fn one_second() -> IggyExpiry {
    IggyExpiry::ExpireDuration(IggyDuration::from(Duration::from_secs(1)))
}

fn at(micros: u64) -> IggyTimestamp {
    IggyTimestamp::from(micros)
}

/// Long-term segments of ten offsets each, one per `(newest timestamp,
/// size)`, in offset order from offset 0.
fn segments_of(shapes: &[(u64, u64)]) -> Vec<ManifestSegment> {
    (0..)
        .step_by(10)
        .zip(shapes)
        .map(|(start_offset, (max_timestamp, size))| ManifestSegment {
            start_offset,
            end_offset: start_offset + 9,
            start_timestamp: *max_timestamp,
            end_timestamp: *max_timestamp,
            max_timestamp: *max_timestamp,
            size: *size,
            crc32c: 0,
            hash: [0; SEGMENT_HASH_LEN],
        })
        .collect()
}

/// A leader of three that places its batches, whose long-term log holds
/// [`segments_of`] `shapes`.
async fn leader_with_log_of(
    shapes: &[(u64, u64)],
) -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let store = Rc::new(MemoryStore::new());
    let (directory, mut partition) = Box::pin(placing_leader_of_three_with(&[], &store)).await;
    for (op, segment) in (100..).zip(segments_of(shapes)) {
        commit_published_segment(&mut partition, &segment, op);
    }
    (directory, partition)
}

/// Four segments whose newest messages are a second apart from the first
/// second on, of 100 bytes each.
const A_SECOND_APART: [(u64, u64); 4] = [
    (ONE_SECOND_MICROS, 100),
    (2 * ONE_SECOND_MICROS, 100),
    (3 * ONE_SECOND_MICROS, 100),
    (4 * ONE_SECOND_MICROS, 100),
];

fn commit_consumer_offset(partition: &IggyPartition<IggyMessageBus>, offset: u64) {
    partition.consumer_offsets.borrow_mut().insert(
        7,
        ConsumerOffset::new(ConsumerKind::Consumer, 7, offset, String::new()),
    );
}

fn apply_trim(partition: &mut IggyPartition<IggyMessageBus>, up_to_offset: u64, op: u64) {
    let payload = TrimSegments { up_to_offset }.encode();
    let applied = partition
        .state_machines
        .apply(
            op,
            MachineCommand {
                kind: TRIM_SEGMENTS,
                payload: &payload,
            },
        )
        .unwrap();
    assert_eq!(applied, Applied::Changed);
}

/// A segment expires when its newest message does, as a segment on disk
/// does, and retention removes the expired ones from the front on.
#[compio::test]
async fn given_long_term_segments_whose_messages_expired_when_deciding_should_name_the_last_expired_one()
 {
    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    let decide = |now| partition.long_term_retention_end(at(now), one_second(), None);

    assert_eq!(decide(2 * ONE_SECOND_MICROS - 1), None);
    assert_eq!(decide(2 * ONE_SECOND_MICROS), Some(9));
    assert_eq!(decide(3 * ONE_SECOND_MICROS), Some(19));
    assert_eq!(decide(4 * ONE_SECOND_MICROS - 1), Some(19));
}

/// The budget is in bytes of messages, which is what the segments' sizes
/// are. The last segment is never removed, so its bytes do not count
/// against the ones before it.
#[compio::test]
async fn given_long_term_segments_over_the_byte_budget_when_deciding_should_name_the_last_one_that_exceeds_it()
 {
    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    let decide = |max_bytes| {
        partition.long_term_retention_end(at(0), IggyExpiry::NeverExpire, Some(max_bytes))
    };

    assert_eq!(decide(300), None);
    assert_eq!(decide(299), Some(9));
    assert_eq!(decide(150), Some(19));
    assert_eq!(decide(0), Some(29));
}

#[compio::test]
async fn given_a_topic_that_never_expires_and_has_no_size_limit_when_deciding_should_name_nothing()
{
    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    let much_later = at(u64::MAX / 2);

    for never in [IggyExpiry::NeverExpire, IggyExpiry::ServerDefault] {
        assert_eq!(
            partition.long_term_retention_end(much_later, never, None),
            None
        );
    }
}

/// A consumer that has not read a segment keeps it, as it keeps a segment
/// on disk.
#[compio::test]
async fn given_a_consumer_behind_the_expired_segments_when_deciding_should_stop_at_its_offset() {
    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    let decide =
        || partition.long_term_retention_end(at(10 * ONE_SECOND_MICROS), one_second(), Some(0));
    assert_eq!(decide(), Some(29));

    commit_consumer_offset(&partition, 19);
    assert_eq!(decide(), Some(19));
    commit_consumer_offset(&partition, 18);
    assert_eq!(decide(), Some(9));
    commit_consumer_offset(&partition, 8);
    assert_eq!(decide(), None);
}

/// The floor and the trim point are read from the segments the log holds,
/// so retention leaves the newest one however old and large it is.
#[compio::test]
async fn given_only_expired_segments_when_deciding_should_never_name_the_last_one() {
    let much_later = at(10 * ONE_SECOND_MICROS);
    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    assert_eq!(
        partition.long_term_retention_end(much_later, one_second(), Some(0)),
        Some(29)
    );

    let (_directory, partition) = Box::pin(leader_with_log_of(&A_SECOND_APART[..1])).await;
    assert_eq!(partition.long_term_segments().len(), 1);
    assert_eq!(
        partition.long_term_retention_end(much_later, one_second(), Some(0)),
        None
    );
}

/// One decision per partition: a backup's log can trail the primary's.
#[compio::test]
async fn given_a_backup_when_deciding_should_name_nothing() {
    let (_directory, mut partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;
    let much_later = at(10 * ONE_SECOND_MICROS);
    assert!(
        partition
            .long_term_retention_end(much_later, one_second(), Some(0))
            .is_some()
    );

    lose_primacy(&mut partition);

    assert_eq!(
        partition.long_term_retention_end(much_later, one_second(), Some(0)),
        None
    );
}

#[compio::test]
async fn given_a_partition_of_whole_batches_when_deciding_should_name_nothing() {
    let (_directory, mut partition) = Box::pin(leader_with_log_of(&A_SECOND_APART)).await;

    partition.set_places_batches(false);

    assert_eq!(
        partition.long_term_retention_end(at(10 * ONE_SECOND_MICROS), one_second(), Some(0)),
        None
    );
}

fn local_segments(partition: &IggyPartition<IggyMessageBus>) -> Vec<(u64, u64)> {
    partition
        .log
        .segments()
        .iter()
        .map(|segment| (segment.start_offset, segment.end_offset))
        .collect()
}

/// A backup whose segments of placed batches hold the offsets `0..=1`,
/// `2..=3` and `4`, the last one open, and whose long-term log holds the
/// offsets `0`, `1..=2` and `3..=4`.
async fn backup_with_local_segments() -> (tempfile::TempDir, IggyPartition<IggyMessageBus>) {
    let (_whole, placed, _object) = whole_and_placed(RECEIVER_OPS);
    let (directory, mut partition) = Box::pin(placing_partition_of_small_segments()).await;
    partition.register_builtin_state_machines().unwrap();
    Box::pin(commit_records(&mut partition, 1, &placed)).await;
    assert_eq!(local_segments(&partition), [(0, 1), (2, 3), (4, 4)]);
    let mut long_term = segments_of(&A_SECOND_APART[..3]);
    for (segment, (start_offset, end_offset)) in long_term.iter_mut().zip([(0, 0), (1, 2), (3, 4)])
    {
        (segment.start_offset, segment.end_offset) = (start_offset, end_offset);
    }
    for (op, segment) in (100..).zip(&long_term) {
        commit_published_segment(&mut partition, segment, op);
    }
    lose_primacy(&mut partition);
    (directory, partition)
}

/// The placed batches below the trim point name messages that are gone, so
/// every replica removes the segments that hold nothing else. A segment
/// that reaches past the trim point stays whole, and so does everything up
/// to the long-term floor.
#[compio::test]
async fn given_a_committed_trim_when_a_replica_follows_it_should_remove_its_segments_at_or_below_the_trim_point_only()
 {
    let config = repair_config();
    let (directory, mut partition) = Box::pin(backup_with_local_segments()).await;
    let log = |start_offset: u64| directory.path().join(format!("{start_offset:020}.log"));

    Box::pin(partition.remove_trimmed_segments()).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(
        local_segments(&partition),
        [(0, 1), (2, 3), (4, 4)],
        "nothing is trimmed yet"
    );

    apply_trim(&mut partition, 0, 200);
    assert_eq!(partition.long_term_trim_point(), Some(0));
    Box::pin(partition.remove_trimmed_segments()).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(
        local_segments(&partition),
        [(0, 1), (2, 3), (4, 4)],
        "the first segment holds an offset above the trim point"
    );

    apply_trim(&mut partition, 2, 201);
    assert_eq!(partition.long_term_trim_point(), Some(2));
    assert_eq!(partition.long_term_floor(), Some(4));
    Box::pin(partition.remove_trimmed_segments()).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(local_segments(&partition), [(2, 3), (4, 4)]);
    assert!(!log(0).exists());
    assert!(log(2).exists());
}

/// A segment of placed batches is a few hundred bytes for any amount of
/// messages, and as old as its batches whatever the long-term log still
/// holds. Its own size and age remove nothing.
#[compio::test]
async fn given_a_placing_partition_when_the_cleaner_expires_segments_should_follow_the_trim_point_and_not_their_own_age_and_size()
 {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(backup_with_local_segments()).await;
    let much_later = at(IggyTimestamp::now().as_micros() + 3600 * ONE_SECOND_MICROS);

    Box::pin(partition.clean_expired_segments(much_later, one_second(), Some(0))).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(local_segments(&partition), [(0, 1), (2, 3), (4, 4)]);

    apply_trim(&mut partition, 2, 200);
    Box::pin(partition.clean_expired_segments(much_later, one_second(), Some(0))).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;

    assert_eq!(local_segments(&partition), [(2, 3), (4, 4)]);
}

/// What every poll of [`polled_all`] returns from a partition of whole
/// batches that committed `records` and whose retention removed its first
/// segment, which holds the offsets `0..=1`.
async fn polls_of_whole_batches_after_retention(
    records: &[Vec<u8>],
) -> Vec<(Option<u64>, Vec<u8>)> {
    let config = repair_config();
    let (_directory, mut partition) = Box::pin(disk_poll_partition_with_replicas(&config, 3)).await;
    let batch_len = records[0].len() as u64;
    let segment_size = IggyByteSize::from(BATCHES_PER_SEGMENT * batch_len);
    partition.runtime_options.segment_size = Some(segment_size);
    partition.log.active_segment_mut().max_size = segment_size;
    Box::pin(commit_records(&mut partition, 1, records)).await;
    Box::pin(partition.remove_sealed_segments_up_to(1)).await;
    Box::pin(settle_partition_io(&mut partition, &config)).await;
    assert_eq!(partition.log.segments()[0].start_offset, 2);
    Box::pin(polled_all(&mut partition)).await
}

/// A leader that committed `first` batches whose messages are in
/// [`FRESH_OBJECT`] and then `second` batches whose messages are in
/// [`SECOND_OBJECT`], with the store that holds both objects and the whole
/// batches of all of them.
async fn leader_with_two_fresh_objects(
    first: u64,
    second: u64,
) -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<MemoryStore>,
    Vec<Vec<u8>>,
) {
    let (first_whole, first_placed, first_object) = whole_and_placed(first);
    let (second_whole, second_placed, second_object) = whole_and_placed_in(SECOND_OBJECT, second);
    let store = Box::pin(store_with(first_object)).await;
    store
        .put(
            &layout().fresh_key(SECOND_OBJECT),
            PutBody::from_bytes(second_object),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let (directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&first_placed, &store)).await;
    // A message of the second object is stamped after every one of the
    // first, so a timestamp tells the two apart.
    compio::time::sleep(Duration::from_millis(2)).await;
    Box::pin(commit_records(&mut partition, first + 1, &second_placed)).await;
    let whole = first_whole.into_iter().chain(second_whole).collect();
    (directory, partition, store, whole)
}

/// Rewrite the batches above the long-term floor up to each of `ends` into
/// a segment of its own, and commit it.
async fn rewrite_up_to(
    partition: &mut IggyPartition<IggyMessageBus>,
    store: &Rc<MemoryStore>,
    ends: &[u64],
) {
    for (request, up_to) in (1..).zip(ends) {
        let segment = Box::pin(store_segment_up_to(partition, store, *up_to)).await;
        let command = partition
            .publish_segment_request(&segment, request)
            .unwrap();
        Box::pin(commit_long_term_request(partition, command, false)).await;
    }
}

/// Delete the fresh object the rewritten batches were read from, and drop
/// what this replica remembers of it.
async fn lose_first_fresh_object(
    partition: &mut IggyPartition<IggyMessageBus>,
    store: &Rc<MemoryStore>,
) {
    let deleted = store
        .delete(&[layout().fresh_key(FRESH_OBJECT)])
        .await
        .unwrap();
    assert!(deleted.failed.is_empty());
    super::remote_poll::attach(partition, Rc::clone(store) as Rc<dyn crate::RemoteObjects>);
}

/// A poll that asks for an offset retention removed gets the messages from
/// the first one the topic still holds, whichever kind of batches the
/// partition keeps.
#[compio::test]
async fn given_a_trimmed_long_term_log_when_polling_below_its_start_should_return_what_a_partition_of_whole_batches_returns_after_retention()
 {
    let (_directory, mut partition, store, whole) =
        Box::pin(leader_with_two_fresh_objects(4, 1)).await;
    let expected = Box::pin(polls_of_whole_batches_after_retention(&whole)).await;
    assert_eq!(expected[0].0, Some(4));
    Box::pin(rewrite_up_to(&mut partition, &store, &[1, 3])).await;
    let trim = partition.trim_segments_request(1, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;
    assert_eq!(partition.long_term_trim_point(), Some(1));
    Box::pin(lose_first_fresh_object(&mut partition, &store)).await;

    let polled = Box::pin(polled_all(&mut partition)).await;

    assert_eq!(polled, expected);
}

/// The batch before the first one a timestamp matches is at the long-term
/// floor, and the fresh object it names is gone. The poll starts above the
/// floor and never asks for it.
#[compio::test]
async fn given_a_timestamp_just_above_the_long_term_floor_when_polling_should_not_read_a_placed_batch_at_the_floor()
 {
    let (_directory, mut partition, store, _whole) =
        Box::pin(leader_with_two_fresh_objects(3, 2)).await;
    Box::pin(rewrite_up_to(&mut partition, &store, &[2])).await;
    let newest_rewritten = partition.long_term_segments()[0].max_timestamp;
    Box::pin(lose_first_fresh_object(&mut partition, &store)).await;
    let strategy = PollingStrategy::timestamp(at(newest_rewritten + 1));
    let args = PollingArgs::new(strategy, 10, false);

    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute()
        .await;

    assert_eq!(result.read_error, None);
    assert_eq!(result.message_count, 2);
    assert_eq!(result.last_matching_offset, Some(4));
}

/// A poll carries a few long-term segments. One that reads them all and
/// wants more ends there: the batches that follow are in the next long-term
/// segment, and the placed batches above the floor do not follow the ones
/// it has.
#[compio::test]
async fn given_more_long_term_segments_than_a_poll_carries_when_polling_from_the_start_should_end_with_the_last_one_it_carries()
 {
    let carried = LONG_TERM_SEGMENTS_PER_POLL as u64;
    let rewritten = carried + 2;
    let (_directory, mut partition, store, _whole) =
        Box::pin(leader_with_two_fresh_objects(rewritten, 1)).await;
    let ends: Vec<u64> = (0..rewritten).collect();
    Box::pin(rewrite_up_to(&mut partition, &store, &ends)).await;
    assert_eq!(partition.long_term_floor(), Some(rewritten - 1));
    Box::pin(lose_first_fresh_object(&mut partition, &store)).await;
    let args = PollingArgs::new(PollingStrategy::offset(0), 100, false);

    let result = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute()
        .await;

    assert_eq!(result.read_error, None);
    assert_eq!(u64::from(result.message_count), carried);
    assert_eq!(result.last_matching_offset, Some(carried - 1));
}

/// Retention to deletion in one line: the primary decides over its
/// long-term segments, the trim it sends commits, the manifest then starts
/// at the trim point, and the objects of the trimmed segments wait for
/// their delete.
#[compio::test]
async fn given_expired_long_term_segments_when_the_trim_retention_decides_commits_should_publish_the_trim_point_and_queue_their_objects_for_delete()
 {
    let (_directory, mut partition, [_, second, third]) =
        Box::pin(leader_with_three_long_term_segments()).await;
    let now = IggyTimestamp::now();
    let much_later = at(now.as_micros() + 3600 * ONE_SECOND_MICROS);
    assert_eq!(partition.long_term_trim_point(), None);
    assert_eq!(partition.queued_dead_long_term_objects(), 0);

    let decision = partition
        .long_term_retention_end(much_later, one_second(), None)
        .expect("two segments expired, and the last one stays");
    assert_eq!(decision, second.end_offset);
    let trim = partition.trim_segments_request(decision, 10).unwrap();
    Box::pin(commit_long_term_request(&mut partition, trim, false)).await;

    assert_eq!(partition.long_term_trim_point(), Some(second.end_offset));
    assert_eq!(partition.long_term_segments(), [third]);
    assert!(partition.take_long_term_changed());
    let (id, manifest, _scope) = partition
        .begin_manifest_publish(much_later)
        .expect("the manifest changed");
    partition.finish_manifest_publish(id, &Ok(Ok(())), much_later);
    assert_eq!(manifest.trim_point, Some(second.end_offset));
    assert_eq!(manifest.segments, [third]);
    assert_eq!(partition.queued_dead_long_term_objects(), 2);
    assert_eq!(
        partition.long_term_retention_end(much_later, one_second(), None),
        None,
        "nothing is left to decide once the trim applied"
    );
}

/// A store that keeps the key of every GET that reaches it.
struct CountedGets {
    store: Rc<MemoryStore>,
    keys: RefCell<Vec<String>>,
}

impl RemoteObjects for CountedGets {
    fn get_range<'a>(
        &'a self,
        key: &'a str,
        range: Range<u64>,
    ) -> LocalBoxFuture<'a, Result<ObjectRange, ObjectStoreError>> {
        self.keys.borrow_mut().push(key.to_owned());
        ObjectStore::get_range(&*self.store, key, range).boxed_local()
    }
}

/// A partition whose first long-term segments the store holds, and whose
/// fresh object below the floor is gone, reading through `cache_bytes` of
/// cache, with the keys of the GETs it sends.
async fn rewritten_partition_reading_through(
    cache_bytes: usize,
) -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<CountedGets>,
) {
    let (directory, mut partition, store, _whole) =
        Box::pin(leader_with_two_fresh_objects(4, 1)).await;
    Box::pin(rewrite_up_to(&mut partition, &store, &[1, 3])).await;
    Box::pin(lose_first_fresh_object(&mut partition, &store)).await;
    let counted = Rc::new(CountedGets {
        store,
        keys: RefCell::default(),
    });
    let cache = ReadCache::new(ReadCacheSettings {
        cache_bytes,
        block_bytes: 256 * 1024,
        read_ahead: 0,
        read_ahead_fetches: 4,
        read_ahead_fetch_bytes: 32 * 1024 * 1024,
    });
    partition.set_remote_segments(Rc::new(
        RemoteSegments::new(Rc::clone(&counted) as _, layout())
            .with_cache(Rc::clone(&counted) as _, &cache),
    ));
    (directory, partition, counted)
}

#[compio::test]
async fn given_long_term_segments_polled_twice_when_the_shard_keeps_what_it_reads_should_ask_the_store_once_for_each_object()
 {
    let (_directory, mut partition, counted) =
        Box::pin(rewritten_partition_reading_through(64 * 1024 * 1024)).await;

    let first = Box::pin(polled_all(&mut partition)).await;
    let asked = counted.keys.borrow().clone();
    let second = Box::pin(polled_all(&mut partition)).await;

    assert_eq!(first, second);
    assert_eq!(*counted.keys.borrow(), asked);
    let indexes = asked
        .iter()
        .filter(|key| {
            std::path::Path::new(key)
                .extension()
                .is_some_and(|extension| extension == "index")
        })
        .count();
    assert_eq!(indexes, 2, "{asked:?}");
    let mut unique = asked.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), asked.len(), "{asked:?}");
}

/// What the polls of a geo partition keep of its long-term segments is the
/// partition's, and leaves with it. A limit below eight blocks keeps none.
#[compio::test]
async fn given_long_term_segments_polled_through_a_cache_when_limited_or_gone_should_keep_none_of_them()
 {
    let (_directory, mut partition, counted) =
        Box::pin(rewritten_partition_reading_through(64 * 1024 * 1024)).await;
    let cached = |partition: &IggyPartition<IggyMessageBus>| {
        partition
            .remote_segments()
            .and_then(|remote| remote.take_read_metrics())
            .map_or(0, |metrics| metrics.cached_bytes)
    };
    let under = PartitionReadSettings {
        limit: Some(1),
        read_ahead: None,
        left: None,
    };

    let first = Box::pin(polled_all(&mut partition)).await;
    assert!(cached(&partition) > 0);
    let asked = counted.keys.borrow().len();
    let limited = Box::pin(polled_all_reading(&mut partition, &under)).await;
    assert_eq!(cached(&partition), 0);
    let keys = counted.keys.borrow().clone();
    let (cached_pass, limited_pass) = keys.split_at(asked);
    assert!(
        cached_pass.iter().all(|key| limited_pass.contains(key)),
        "each object asked again: {keys:?}"
    );
    Box::pin(polled_all_reading(
        &mut partition,
        &PartitionReadSettings::SHARD,
    ))
    .await;
    assert!(cached(&partition) > 0);
    partition.forget_reads();

    assert_eq!(first, limited);
    assert_eq!(cached(&partition), 0);
}

/// With no bytes for the cache, a poll of long-term segments asks the store
/// as it did before the cache: again for each poll.
#[compio::test]
async fn given_long_term_segments_polled_twice_when_the_cache_keeps_nothing_fetched_should_ask_the_store_again()
 {
    let (_directory, mut partition, counted) =
        Box::pin(rewritten_partition_reading_through(0)).await;

    let first = Box::pin(polled_all(&mut partition)).await;
    let asked = counted.keys.borrow().len();
    let second = Box::pin(polled_all(&mut partition)).await;

    assert_eq!(first, second);
    assert!(asked > 0);
    assert_eq!(counted.keys.borrow().len(), 2 * asked);
}

/// A leader that committed batches in [`FRESH_OBJECT`] and then in
/// [`SECOND_OBJECT`], which its shard uploaded and holds, with the store that
/// holds both objects and the reads of the partition.
async fn leader_holding_second_object() -> (
    tempfile::TempDir,
    IggyPartition<IggyMessageBus>,
    Rc<MemoryStore>,
    Rc<RemoteSegments>,
) {
    let (_first_whole, first_placed, first_object) = whole_and_placed(2);
    let (_second_whole, second_placed, second_object) = whole_and_placed_in(SECOND_OBJECT, 2);
    let store = Box::pin(store_with(first_object)).await;
    store
        .put(
            &layout().fresh_key(SECOND_OBJECT),
            PutBody::from_bytes(second_object.clone()),
            &PutOptions::default(),
        )
        .await
        .unwrap();
    let (directory, mut partition) =
        Box::pin(placing_leader_of_three_with(&first_placed, &store)).await;
    let cache = ReadCache::new(ReadCacheSettings {
        cache_bytes: 64 * 1024 * 1024,
        block_bytes: 256 * 1024,
        read_ahead: 0,
        read_ahead_fetches: 4,
        read_ahead_fetch_bytes: 32 * 1024 * 1024,
    });
    let remote = Rc::new(
        RemoteSegments::new(Rc::clone(&store) as _, layout())
            .with_cache(Rc::clone(&store) as _, &cache),
    );
    partition.set_remote_segments(Rc::clone(&remote));
    remote.cache_fresh_object(SECOND_OBJECT, second_object, [partition.namespace()]);
    Box::pin(commit_records(&mut partition, 3, &second_placed)).await;
    remote.settle_fresh_object(SECOND_OBJECT);
    assert!(remote.holds_fresh_object(SECOND_OBJECT));
    (directory, partition, store, remote)
}

/// The polls of a partition read what the shard uploaded until the rewrite
/// of those batches commits, and its long-term segment after that.
#[compio::test]
async fn given_an_object_this_shard_uploaded_when_the_rewrite_of_its_batches_commits_should_release_it()
 {
    let (_directory, mut partition, store, remote) = Box::pin(leader_holding_second_object()).await;

    Box::pin(rewrite_up_to(&mut partition, &store, &[1, 3])).await;

    assert_eq!(partition.long_term_floor(), Some(3));
    assert!(!remote.holds_fresh_object(SECOND_OBJECT));
}

/// A transfer can restore a long-term floor that passed an object this
/// shard holds, and the shard lets it go as when the rewrite commits here.
#[compio::test]
async fn given_an_object_this_shard_holds_when_a_restored_floor_passes_it_should_release_it() {
    let (_rewritten_directory, mut rewritten, store, _) =
        Box::pin(leader_holding_second_object()).await;
    Box::pin(rewrite_up_to(&mut rewritten, &store, &[1, 3])).await;
    let op = rewritten.state_machines().applied_op();
    let snapshot = rewritten.machines_snapshot_at(op).unwrap();
    let (_directory, mut behind, _store, remote) = Box::pin(leader_holding_second_object()).await;

    behind.restore_machines(snapshot.as_ref(), op).unwrap();

    assert_eq!(behind.long_term_floor(), Some(3));
    assert!(!remote.holds_fresh_object(SECOND_OBJECT));
}

/// A long-term segment that the store returned damaged once is read again
/// on the next poll, once the store holds it whole.
#[compio::test]
async fn given_a_damaged_long_term_segment_in_the_cache_when_the_store_holds_it_whole_again_should_read_it_again()
 {
    let (_directory, mut partition, counted) =
        Box::pin(rewritten_partition_reading_through(64 * 1024 * 1024)).await;
    let store = Rc::clone(&counted.store);
    let mut logs: Vec<String> = store
        .keys()
        .into_iter()
        .filter(|key| key.contains('-') && key.ends_with(".log"))
        .collect();
    logs.sort();
    let key = logs.first().unwrap().clone();
    let whole = ObjectStore::get_range(&*store, &key, 0..u64::MAX)
        .await
        .unwrap()
        .bytes;
    let mut damaged = whole.to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 0xff;
    let put = |bytes: Bytes| {
        let store = Rc::clone(&store);
        let key = key.clone();
        async move {
            store
                .put(&key, PutBody::from_bytes(bytes), &PutOptions::default())
                .await
                .unwrap();
        }
    };
    put(Bytes::from(damaged)).await;
    let args = PollingArgs::new(PollingStrategy::offset(0), 10, false);
    let failed = partition
        .build_poll_plan(PollingConsumer::Consumer(1, 0), &args, false)
        .execute()
        .await;
    assert!(failed.read_error.is_some());

    put(whole).await;

    Box::pin(polled_all(&mut partition)).await;
}

/// A rewrite hands the segment it stored to the cache, so a consumer just
/// behind it reads it from memory. A cache that keeps nothing fetched takes
/// nothing.
#[compio::test]
async fn given_a_rewrite_that_stored_a_segment_when_polling_it_should_read_it_from_memory_with_a_cache()
 {
    for cache_bytes in [64 * 1024 * 1024, 0] {
        let (_directory, mut partition, store, _whole) =
            Box::pin(leader_with_two_fresh_objects(4, 1)).await;
        let counted = Rc::new(CountedGets {
            store: Rc::clone(&store),
            keys: RefCell::default(),
        });
        let cache = ReadCache::new(ReadCacheSettings {
            cache_bytes,
            block_bytes: 256 * 1024,
            read_ahead: 0,
            read_ahead_fetches: 4,
            read_ahead_fetch_bytes: 32 * 1024 * 1024,
        });
        partition.set_remote_segments(Rc::new(
            RemoteSegments::new(Rc::clone(&counted) as _, layout())
                .with_cache(Rc::clone(&counted) as _, &cache),
        ));
        Box::pin(rewrite_up_to(&mut partition, &store, &[3])).await;
        counted.keys.borrow_mut().clear();

        Box::pin(polled_all(&mut partition)).await;

        let long_term = counted
            .keys
            .borrow()
            .iter()
            .filter(|key| key.contains('-'))
            .count();
        if cache_bytes > 0 {
            assert_eq!(long_term, 0, "{:?}", counted.keys.borrow());
            assert!(cache.take_metrics().carried_bytes > 0);
        } else {
            assert!(long_term > 0);
            assert_eq!(cache.take_metrics().carried_bytes, 0);
        }
    }
}
