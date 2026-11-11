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

//! The deletion of the objects of segments a tiered partition no longer has.
//!
//! Only the partition primary deletes, and only the objects of a segment that
//! ended at or below both the truncation watermark metadata committed and the
//! primary's own trim point. A segment waits [`RETIRED_OBJECT_GRACE`] between
//! the moment the primary learned it is gone and the delete.
//!
//! The segments that wait are kept in memory. A primary that starts without
//! them lists its incarnation's objects once and takes up the ones that
//! [`sweepable`] proves gone.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ListRequest, MAX_KEYS_PER_REQUEST, ObjectPrefix, ObjectStore, ObjectStoreError,
    SegmentKey, SegmentObject, TierExecutor, TierJobCancelled, TierJobClass, TierJobRequest,
    TierScopeToken,
};

/// How long the objects of a removed segment stay in the store after the
/// primary learned the segment is gone.
pub const RETIRED_OBJECT_GRACE: Duration = Duration::from_mins(10);

/// What one job does in the store for a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDeleteWork {
    /// List the first offset of every segment object of the incarnation.
    Sweep,
    /// Delete the `.log` and the `.index` of the segments that start at
    /// these offsets.
    Delete { start_offsets: Vec<u64> },
}

/// A job the partition's owner captured, to run under the scope it came with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDeleteJob {
    /// The partition's own number for the job, which its result carries back.
    pub id: u64,
    pub prefix: ObjectPrefix,
    pub work: ObjectDeleteWork,
}

/// What a job that ran to its end found or left.
#[derive(Debug)]
pub enum ObjectDeleteDone {
    /// The first offsets a sweep listed, or why the listing stopped.
    Swept(Result<BTreeSet<u64>, ObjectStoreError>),
    /// The first offsets of the segments that still have an object.
    Deleted { failed: Vec<u64> },
}

/// What a job ended as: its own result, or a cancellation before it finished.
pub type ObjectDeleteOutcome = Result<ObjectDeleteDone, TierJobCancelled>;

/// Runs the object deletes of a shard, behind a pointer, so the shard does
/// not carry the executor's type.
pub trait ObjectDeleter {
    /// Run `job` under `scope`. The job stops when the scope is cancelled, as
    /// when its partition loses primacy.
    fn run(
        &self,
        job: ObjectDeleteJob,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ObjectDeleteOutcome>;
}

/// Deletes through a tier executor. Deletes go in its delete class and the
/// listing of a sweep in its housekeeping class, so neither takes a slot from
/// an upload or a read.
pub struct ExecutorDeleter<E> {
    executor: E,
    layout: KeyLayout,
}

impl<E> ExecutorDeleter<E> {
    #[must_use]
    pub const fn new(executor: E, layout: KeyLayout) -> Self {
        Self { executor, layout }
    }

    /// The executor the jobs go through.
    #[must_use]
    pub const fn executor(&self) -> &E {
        &self.executor
    }
}

impl<E: TierExecutor> ObjectDeleter for ExecutorDeleter<E> {
    fn run(
        &self,
        job: ObjectDeleteJob,
        scope: TierScopeToken,
    ) -> LocalBoxFuture<'static, ObjectDeleteOutcome> {
        let layout = self.layout.clone();
        let prefix = job.prefix;
        match job.work {
            ObjectDeleteWork::Sweep => self
                .executor
                .submit(
                    TierJobRequest::new(TierJobClass::Housekeeping, scope),
                    move |store| async move {
                        ObjectDeleteDone::Swept(
                            list_segment_starts(&*store, &layout, &prefix).await,
                        )
                    },
                )
                .boxed_local(),
            ObjectDeleteWork::Delete { start_offsets } => self
                .executor
                .submit(
                    TierJobRequest::new(TierJobClass::Delete, scope),
                    move |store| async move {
                        ObjectDeleteDone::Deleted {
                            failed: delete_segments(&*store, &layout, &prefix, &start_offsets)
                                .await,
                        }
                    },
                )
                .boxed_local(),
        }
    }
}

/// Delete the objects under `keys`, at most [`MAX_KEYS_PER_REQUEST`] in one
/// call to the store, and return the keys that were not deleted.
///
/// A key without an object counts as deleted, so sending the same keys again
/// is harmless. Once the store refuses a whole call, the keys after it are
/// not sent and count as failed.
pub async fn delete_objects<S: ObjectStore>(store: &S, keys: &[String]) -> Vec<String> {
    let mut failed = Vec::new();
    let mut calls = keys.chunks(MAX_KEYS_PER_REQUEST);
    for call in calls.by_ref() {
        let Ok(outcome) = store.delete(call).await else {
            failed.extend_from_slice(call);
            break;
        };
        failed.extend(outcome.failed.into_iter().map(|failure| failure.key));
    }
    failed.extend(calls.flatten().cloned());
    failed
}

/// Delete both objects of every segment of `prefix` that starts at one of
/// `start_offsets`, and return the first offsets of the segments that still
/// have an object.
pub async fn delete_segments<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    start_offsets: &[u64],
) -> Vec<u64> {
    let keys: Vec<String> = start_offsets
        .iter()
        .flat_map(|start_offset| {
            SegmentObject::ALL.map(|object| {
                layout.segment_key(&SegmentKey {
                    prefix: *prefix,
                    start_offset: *start_offset,
                    object,
                })
            })
        })
        .collect();
    let failed: BTreeSet<u64> = delete_objects(store, &keys)
        .await
        .iter()
        .filter_map(|key| layout.parse_segment_key(key))
        .map(|key| key.start_offset)
        .collect();
    failed.into_iter().collect()
}

/// The first offset of every segment object the store lists for `prefix`.
///
/// A key under the listing prefix that the layout did not build for this
/// incarnation is left out, so nothing ever deletes it.
///
/// # Errors
/// The store's error for the page that failed. Nothing is returned for the
/// pages before it: a partial listing cannot tell where a segment ends.
pub async fn list_segment_starts<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
) -> Result<BTreeSet<u64>, ObjectStoreError> {
    let listing_prefix = layout.incarnation_prefix(prefix);
    let mut start_offsets = BTreeSet::new();
    let mut continuation: Option<String> = None;
    loop {
        let request = ListRequest::new(&listing_prefix);
        let request = continuation
            .as_deref()
            .map_or(request, |continuation| request.after(continuation));
        let page = store.list(&request).await?;
        start_offsets.extend(
            page.objects
                .iter()
                .filter_map(|object| layout.parse_segment_key(&object.key))
                .filter(|key| key.prefix == *prefix)
                .map(|key| key.start_offset),
        );
        continuation = page.next;
        if continuation.is_none() {
            return Ok(start_offsets);
        }
    }
}

/// The listed segments that are proven to end at or below `bound`, each as
/// `(first offset, the last offset it can hold)`.
///
/// A key names only where its segment starts. Segments of one incarnation
/// never overlap, so a segment ends before the next start that is known: the
/// next listed object, or the next segment of the partition's own list. A
/// listed segment is taken only when that offset is at or below `bound`.
/// It is kept when nothing starts after it, and when the partition still
/// lists a segment at its first offset.
///
/// `listed` and `segment_starts` both ascend.
#[must_use]
pub fn sweepable(listed: &BTreeSet<u64>, segment_starts: &[u64], bound: u64) -> Vec<(u64, u64)> {
    listed
        .iter()
        .filter(|start_offset| segment_starts.binary_search(start_offset).is_err())
        .filter_map(|start_offset| {
            let next_listed = listed.range(start_offset + 1..).next().copied();
            let next_segment = segment_starts
                .get(segment_starts.partition_point(|start| start <= start_offset))
                .copied();
            let next_start = next_listed.into_iter().chain(next_segment).min()?;
            let last_possible = next_start - 1;
            (last_possible <= bound).then_some((*start_offset, last_possible))
        })
        .collect()
}

/// A segment whose objects wait for their delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetiredSegment {
    /// The last offset the segment can hold.
    pub end_offset: u64,
    /// When the primary learned the segment is gone, in microseconds.
    pub retired_at: u64,
}

/// The segments whose objects wait for their delete, by first offset.
#[derive(Debug, Default)]
pub struct RetiredSegments {
    by_start: BTreeMap<u64, RetiredSegment>,
}

impl RetiredSegments {
    /// Queue the segment from `start_offset` to `end_offset`. A segment that
    /// is queued already waits from the later of the two moments: an upload
    /// that lands after its segment was removed stores objects after the
    /// first one.
    pub fn retire(&mut self, start_offset: u64, end_offset: u64, retired_at: IggyTimestamp) {
        let retired_at = retired_at.as_micros();
        let retired = self.by_start.entry(start_offset).or_insert(RetiredSegment {
            end_offset,
            retired_at,
        });
        retired.end_offset = retired.end_offset.max(end_offset);
        retired.retired_at = retired.retired_at.max(retired_at);
    }

    /// Queue a segment a sweep found, unless it waits already.
    pub fn retire_swept(&mut self, start_offset: u64, end_offset: u64, retired_at: IggyTimestamp) {
        self.by_start.entry(start_offset).or_insert(RetiredSegment {
            end_offset,
            retired_at: retired_at.as_micros(),
        });
    }

    /// The queued segments whose grace period ended by `now`, that end at or
    /// below `bound`, and that the partition does not list at `segment_starts`.
    pub fn due(
        &self,
        now: IggyTimestamp,
        bound: u64,
        segment_starts: &[u64],
    ) -> Vec<(u64, RetiredSegment)> {
        let grace = u64::try_from(RETIRED_OBJECT_GRACE.as_micros()).unwrap_or(u64::MAX);
        self.by_start
            .iter()
            .filter(|(start_offset, retired)| {
                now.as_micros() >= retired.retired_at.saturating_add(grace)
                    && retired.end_offset <= bound
                    && segment_starts.binary_search(start_offset).is_err()
            })
            .map(|(start_offset, retired)| (*start_offset, *retired))
            .collect()
    }

    /// Forget the segments of `sent` that a delete removed. A segment in
    /// `failed` stays, and so does one that was queued again while the delete
    /// ran, since its objects may have been stored after it.
    pub fn settle(&mut self, sent: &[(u64, RetiredSegment)], failed: &[u64]) {
        for (start_offset, retired) in sent {
            if !failed.contains(start_offset) && self.by_start.get(start_offset) == Some(retired) {
                self.by_start.remove(start_offset);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.by_start.len()
    }

    #[cfg(test)]
    pub fn start_offsets(&self) -> Vec<u64> {
        self.by_start.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(micros: u64) -> IggyTimestamp {
        IggyTimestamp::from(micros)
    }

    fn grace() -> u64 {
        u64::try_from(RETIRED_OBJECT_GRACE.as_micros()).unwrap()
    }

    fn listed(start_offsets: &[u64]) -> BTreeSet<u64> {
        start_offsets.iter().copied().collect()
    }

    #[test]
    fn given_listed_segments_when_sweeping_should_take_only_those_a_later_start_proves_below_the_bound()
     {
        // Objects at 0, 10 and 20, and the partition lists segments at 30
        // and 40.
        let objects = listed(&[0, 10, 20]);
        let segments = [30, 40];

        assert_eq!(sweepable(&objects, &segments, 8), []);
        assert_eq!(sweepable(&objects, &segments, 9), [(0, 9)]);
        assert_eq!(sweepable(&objects, &segments, 28), [(0, 9), (10, 19)]);
        // The partition's own next segment bounds the last object.
        assert_eq!(
            sweepable(&objects, &segments, 29),
            [(0, 9), (10, 19), (20, 29)]
        );
    }

    #[test]
    fn given_a_listed_segment_with_nothing_after_it_when_sweeping_should_keep_it() {
        assert_eq!(sweepable(&listed(&[50]), &[30, 40], u64::MAX), []);
        assert_eq!(sweepable(&listed(&[50]), &[], u64::MAX), []);
        // The object before it is bounded by it.
        assert_eq!(
            sweepable(&listed(&[45, 50]), &[30, 40], u64::MAX),
            [(45, 49)]
        );
    }

    #[test]
    fn given_a_listed_segment_the_partition_still_lists_when_sweeping_should_keep_it() {
        let objects = listed(&[0, 10, 20]);

        assert_eq!(sweepable(&objects, &[10, 20, 30], u64::MAX), [(0, 9)]);
        assert_eq!(sweepable(&objects, &[0, 10, 20, 30], u64::MAX), []);
    }

    #[test]
    fn given_a_start_between_two_objects_when_sweeping_should_end_the_earlier_one_there() {
        // The partition's segment at 5 is the nearest start after 0.
        assert_eq!(sweepable(&listed(&[0, 10]), &[5, 20], 4), [(0, 4)]);
        assert_eq!(sweepable(&listed(&[0, 10]), &[5, 20], 3), []);
    }

    #[test]
    fn given_a_retired_segment_when_asking_what_is_due_should_wait_for_the_grace_period_and_the_bound()
     {
        let mut retired = RetiredSegments::default();
        retired.retire(0, 9, at(1_000));

        assert_eq!(retired.due(at(1_000 + grace() - 1), 9, &[10]), []);
        assert_eq!(retired.due(at(1_000 + grace()), 8, &[10]), []);
        assert_eq!(retired.due(at(1_000 + grace()), 9, &[0, 10]), []);
        assert_eq!(
            retired.due(at(1_000 + grace()), 9, &[10]),
            [(
                0,
                RetiredSegment {
                    end_offset: 9,
                    retired_at: 1_000
                }
            )]
        );
    }

    #[test]
    fn given_a_segment_retired_twice_when_asking_what_is_due_should_wait_from_the_later_moment() {
        let mut retired = RetiredSegments::default();
        retired.retire(0, 9, at(1_000));
        retired.retire(0, 9, at(5_000));
        retired.retire(0, 9, at(2_000));

        assert_eq!(retired.due(at(5_000 + grace() - 1), 9, &[]), []);
        assert_eq!(retired.due(at(5_000 + grace()), 9, &[]).len(), 1);
    }

    #[test]
    fn given_a_swept_segment_that_waits_already_when_queued_should_keep_its_first_moment() {
        let mut retired = RetiredSegments::default();
        retired.retire(0, 9, at(1_000));
        retired.retire_swept(0, 9, at(5_000));

        assert_eq!(retired.due(at(1_000 + grace()), 9, &[]).len(), 1);
    }

    #[test]
    fn given_a_delete_result_when_settling_should_keep_failed_and_requeued_segments() {
        let mut retired = RetiredSegments::default();
        for start_offset in [0, 10, 20] {
            retired.retire(start_offset, start_offset + 9, at(1_000));
        }
        let sent = retired.due(at(1_000 + grace()), u64::MAX, &[]);
        assert_eq!(sent.len(), 3);
        // The segment at 20 was queued again while the delete ran.
        retired.retire(20, 29, at(9_000));

        retired.settle(&sent, &[10]);

        assert_eq!(retired.start_offsets(), [10, 20]);
    }
}
