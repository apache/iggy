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
//! [`sweepable`] allows and whose `.log` object records an end at or below
//! the same bound. The `.log` object is therefore the last of a segment to
//! go: [`delete_segments`] deletes it only after the segment's `.index`.
//!
//! A partition that a committed delete removed has no primary left, so
//! [`DeletedIncarnations`] holds its incarnation for the node that saw the
//! delete commit, and [`delete_incarnation`] removes everything the
//! incarnation stored.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use futures::FutureExt;
use futures::future::LocalBoxFuture;
use iggy_common::IggyTimestamp;
use iggy_object_store::{
    KeyLayout, ListRequest, MAX_KEYS_PER_REQUEST, ObjectPrefix, ObjectStore, ObjectStoreError,
    SegmentKey, SegmentMetadata, SegmentObject, TierExecutor, TierJobCancelled, TierJobClass,
    TierJobRequest, TierScope, TierScopeToken,
};
use tracing::warn;

/// How long the objects of a removed segment stay in the store after the
/// primary learned the segment is gone.
pub const RETIRED_OBJECT_GRACE: Duration = Duration::from_mins(10);

/// How long a deleted incarnation waits after a delete of its objects that
/// did not finish, so a store that refuses is not asked on every pass.
pub const INCARNATION_DELETE_RETRY: Duration = Duration::from_mins(1);

/// The most deleted incarnations that wait at once. The queue is memory
/// only, so a delete of more partitions than this leaves the objects of the
/// rest in the store.
pub const MAX_QUEUED_INCARNATIONS: usize = 65_536;

/// The most segments whose objects of one kind fit one delete call to the
/// store.
const SEGMENTS_PER_DELETE_CALL: usize = MAX_KEYS_PER_REQUEST;

/// What one job does in the store for a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDeleteWork {
    /// List the first offset of every segment object of the incarnation, and
    /// read the offsets that the `.log` object records for each segment that
    /// [`sweepable`] takes for the partition's `segment_starts` and `bound`.
    Sweep {
        /// The first offsets of the partition's own segments, ascending.
        segment_starts: Vec<u64>,
        /// The last offset a segment may hold for its objects to be deleted.
        bound: u64,
    },
    /// Delete the `.index` and then the `.log` of the segments that start
    /// at these offsets.
    Delete { start_offsets: Vec<u64> },
    /// Delete every segment object and the manifest of the incarnation,
    /// which a committed delete removed.
    DeleteIncarnation,
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
    /// What a sweep listed and read, or why it stopped.
    Swept(Result<SweptObjects, ObjectStoreError>),
    /// The first offsets of the segments that still have an object.
    Deleted { failed: Vec<u64> },
    /// How many objects of a deleted incarnation are still to delete, or why
    /// its listing stopped.
    IncarnationDeleted(Result<usize, ObjectStoreError>),
}

/// What a job ended as: its own result, or a cancellation before it finished.
pub type ObjectDeleteOutcome = Result<ObjectDeleteDone, TierJobCancelled>;

/// A job to run, with the token of the scope its owner keeps.
pub type ObjectDeleteStart = (ObjectDeleteJob, TierScopeToken);

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

/// Deletes through a tier executor. Deletes go in its delete class and a
/// sweep in its housekeeping class, so neither takes a slot from an upload or
/// a read.
///
/// The delete of an incarnation is one job of the delete class, its listing
/// included.
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
            ObjectDeleteWork::Sweep {
                segment_starts,
                bound,
            } => self
                .executor
                .submit(
                    TierJobRequest::new(TierJobClass::Housekeeping, scope),
                    move |store| async move {
                        ObjectDeleteDone::Swept(
                            sweep_segments(&*store, &layout, &prefix, &segment_starts, bound).await,
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
            ObjectDeleteWork::DeleteIncarnation => self
                .executor
                .submit(
                    TierJobRequest::new(TierJobClass::Delete, scope),
                    move |store| async move {
                        ObjectDeleteDone::IncarnationDeleted(
                            delete_incarnation(&*store, &layout, &prefix).await,
                        )
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
///
/// Every `.index` is deleted before any `.log`, and a segment keeps its
/// `.log` while its `.index` is not deleted. A sweep finds a segment by its
/// `.log` object alone, so a segment that lost only that one would keep its
/// `.index` for as long as the incarnation lives.
pub async fn delete_segments<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    start_offsets: &[u64],
) -> Vec<u64> {
    let mut failed =
        delete_segment_objects(store, layout, prefix, start_offsets, SegmentObject::Index).await;
    let without_index: Vec<u64> = start_offsets
        .iter()
        .copied()
        .filter(|start_offset| !failed.contains(start_offset))
        .collect();
    failed.extend(
        delete_segment_objects(store, layout, prefix, &without_index, SegmentObject::Log).await,
    );
    failed.into_iter().collect()
}

/// Delete the `object` of every segment of `prefix` that starts at one of
/// `start_offsets`, and return the first offsets of the segments that still
/// have it.
async fn delete_segment_objects<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    start_offsets: &[u64],
    object: SegmentObject,
) -> BTreeSet<u64> {
    let keys: Vec<String> = start_offsets
        .iter()
        .map(|start_offset| {
            layout.segment_key(&SegmentKey {
                prefix: *prefix,
                start_offset: *start_offset,
                object,
            })
        })
        .collect();
    let failed: HashSet<String> = delete_objects(store, &keys).await.into_iter().collect();
    start_offsets
        .iter()
        .zip(&keys)
        .filter(|(_, key)| failed.contains(*key))
        .map(|(start_offset, _)| *start_offset)
        .collect()
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

/// List the segment objects of `prefix`, and read where each segment ends
/// that [`sweepable`] takes for `segment_starts` and `bound`.
///
/// The end is the one in the user metadata of the segment's `.log` object,
/// read with one HEAD for a segment, one segment after another. A segment
/// gets no end, so nothing deletes it, when the `.log` object is missing,
/// when its metadata does not decode, and when the metadata names another
/// first offset than the key.
///
/// # Errors
/// The store's error for the listing page or the HEAD that failed, unless
/// the HEAD found no object. Nothing is returned for what came before it, so
/// the caller sweeps again and no segment is passed over for good.
pub async fn sweep_segments<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
    segment_starts: &[u64],
    bound: u64,
) -> Result<SweptObjects, ObjectStoreError> {
    let listed = list_segment_starts(store, layout, prefix).await?;
    let mut end_offsets = BTreeMap::new();
    for (start_offset, _) in sweepable(&listed, segment_starts, bound) {
        let key = layout.segment_key(&SegmentKey {
            prefix: *prefix,
            start_offset,
            object: SegmentObject::Log,
        });
        let held = match store.head(&key).await {
            Ok(held) => held,
            Err(ObjectStoreError::NotFound { .. }) => continue,
            Err(error) => return Err(error),
        };
        match SegmentMetadata::try_from(&held.metadata) {
            Ok(metadata) if metadata.start_offset == start_offset => {
                end_offsets.insert(start_offset, metadata.end_offset);
            }
            Ok(metadata) => warn!(
                key,
                start_offset = metadata.start_offset,
                "a segment object records another first offset than its key and is kept"
            ),
            Err(error) => warn!(
                key,
                %error,
                "the metadata of a segment object does not decode and the object is kept"
            ),
        }
    }
    Ok(SweptObjects {
        listed,
        end_offsets,
    })
}

/// Delete every segment object the store lists for `prefix`, then the
/// incarnation's manifest, and return how many objects are still to delete.
///
/// Only for an incarnation a committed delete removed: nothing here asks
/// whether a segment is still in use. The keys come from
/// [`list_segment_starts`], so a key under the listing prefix that the layout
/// did not build for this incarnation stays. Deletes stop at the first call
/// that leaves an object behind, and the manifest goes only after every
/// segment did. Running it again deletes what is left.
///
/// # Errors
/// The store's error for the listing page that failed. Nothing is deleted
/// then.
pub async fn delete_incarnation<S: ObjectStore>(
    store: &S,
    layout: &KeyLayout,
    prefix: &ObjectPrefix,
) -> Result<usize, ObjectStoreError> {
    let start_offsets: Vec<u64> = list_segment_starts(store, layout, prefix)
        .await?
        .into_iter()
        .collect();
    let mut calls = start_offsets.chunks(SEGMENTS_PER_DELETE_CALL);
    let mut segments_left = 0;
    for call in calls.by_ref() {
        segments_left = delete_segments(store, layout, prefix, call).await.len();
        if segments_left > 0 {
            break;
        }
    }
    segments_left += calls.map(<[u64]>::len).sum::<usize>();
    if segments_left > 0 {
        // The manifest is the one object left to count besides them.
        return Ok(segments_left + 1);
    }
    Ok(delete_objects(store, &[layout.manifest_key(prefix)])
        .await
        .len())
}

/// The listed segments that the starts after them place at or below `bound`,
/// each as `(first offset, the last offset it can hold)`.
///
/// A key names only where its segment starts. While every primary of an
/// incarnation seals its segments at the same offsets, a segment ends before
/// the next start that is known: the next listed object, or the next segment
/// of the partition's own list. A listed segment is taken only when that
/// offset is at or below `bound`. It is kept when nothing starts after it,
/// and when the partition still lists a segment at its first offset.
///
/// Primaries that sealed at different offsets leave segments that overlap,
/// so this alone does not prove an end: [`SweptObjects::ended_by`] asks the
/// object as well.
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

/// What a sweep found in the store for one incarnation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweptObjects {
    /// The first offset of every segment object the store listed.
    pub listed: BTreeSet<u64>,
    /// The last offset the `.log` object records, by first offset, for the
    /// segments whose `.log` object was read and agrees with its key.
    pub end_offsets: BTreeMap<u64, u64>,
}

impl SweptObjects {
    /// The segments that are proven to end at or below `bound`, each as
    /// `(first offset, the last offset its .log object records)`.
    ///
    /// A segment is taken only when [`sweepable`] takes it for
    /// `segment_starts` and `bound` as they are now, and its `.log` object
    /// records an end at or below `bound`. One whose `.log` object was not
    /// read is kept.
    #[must_use]
    pub fn ended_by(&self, segment_starts: &[u64], bound: u64) -> Vec<(u64, u64)> {
        sweepable(&self.listed, segment_starts, bound)
            .into_iter()
            .filter_map(|(start_offset, _)| {
                let end_offset = *self.end_offsets.get(&start_offset)?;
                (end_offset <= bound).then_some((start_offset, end_offset))
            })
            .collect()
    }
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
    /// below `bound`, and whose first offset `is_segment_start` does not take
    /// for a segment the partition lists.
    pub fn due(
        &self,
        now: IggyTimestamp,
        bound: u64,
        is_segment_start: impl Fn(u64) -> bool,
    ) -> Vec<(u64, RetiredSegment)> {
        let grace = u64::try_from(RETIRED_OBJECT_GRACE.as_micros()).unwrap_or(u64::MAX);
        self.by_start
            .iter()
            .filter(|(start_offset, retired)| {
                now.as_micros() >= retired.retired_at.saturating_add(grace)
                    && retired.end_offset <= bound
                    && !is_segment_start(**start_offset)
            })
            .map(|(start_offset, retired)| (*start_offset, *retired))
            .collect()
    }

    /// Forget the segments of `sent` that a delete removed. A segment in
    /// `failed` stays, and so does one that was queued again while the delete
    /// ran, since its objects may have been stored after it.
    pub fn settle(&mut self, sent: &[(u64, RetiredSegment)], failed: &[u64]) {
        let failed: HashSet<u64> = failed.iter().copied().collect();
        for (start_offset, retired) in sent {
            if !failed.contains(start_offset) && self.by_start.get(start_offset) == Some(retired) {
                self.by_start.remove(start_offset);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.by_start.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_start.is_empty()
    }

    #[cfg(test)]
    pub fn start_offsets(&self) -> Vec<u64> {
        self.by_start.keys().copied().collect()
    }
}

/// The incarnations a committed delete removed, whose objects wait for their
/// delete on the node that saw the delete commit.
///
/// Kept in memory only. What is queued here and lost, on a restart or when
/// the node stops being the one that deletes, stays in the store.
#[derive(Debug, Default)]
pub struct DeletedIncarnations {
    /// When each incarnation's delete may next run, in microseconds.
    due_at: BTreeMap<ObjectPrefix, u64>,
    /// The same entries by that moment, so a pass finds the next one without
    /// walking the queue.
    by_due: BTreeSet<(u64, ObjectPrefix)>,
    /// The job in flight. Dropping its scope cancels it.
    running: Option<(u64, ObjectPrefix, TierScope)>,
    jobs: u64,
}

impl DeletedIncarnations {
    /// Queue the incarnation, to wait [`RETIRED_OBJECT_GRACE`] from `now`,
    /// the moment its committed delete was observed. An incarnation that
    /// waits already keeps its moment. `false` when the queue is full and
    /// the incarnation was left out.
    pub fn queue(&mut self, prefix: ObjectPrefix, now: IggyTimestamp) -> bool {
        if self.due_at.contains_key(&prefix) {
            return true;
        }
        if self.due_at.len() >= MAX_QUEUED_INCARNATIONS {
            return false;
        }
        let due_at = now.as_micros().saturating_add(micros(RETIRED_OBJECT_GRACE));
        self.due_at.insert(prefix, due_at);
        self.by_due.insert((due_at, prefix));
        true
    }

    /// Start the delete of the next incarnation that waited long enough by
    /// `now`. `None` while a job is in flight and when nothing is due.
    ///
    /// `is_live` says whether committed metadata holds a partition of exactly
    /// that incarnation. Such an incarnation is dropped from the queue and
    /// never deleted: its queueing was wrong, whatever caused it.
    pub fn begin(
        &mut self,
        now: IggyTimestamp,
        mut is_live: impl FnMut(&ObjectPrefix) -> bool,
    ) -> Option<ObjectDeleteStart> {
        if self.running.is_some() {
            return None;
        }
        loop {
            let (due_at, prefix) = *self.by_due.first()?;
            if due_at > now.as_micros() {
                return None;
            }
            if is_live(&prefix) {
                warn!(
                    ?prefix,
                    "a live partition incarnation was queued for the delete of its objects and is dropped from the queue"
                );
                self.forget(&prefix);
                continue;
            }
            let scope = TierScope::new();
            let token = scope.token();
            self.jobs += 1;
            let id = self.jobs;
            self.running = Some((id, prefix, scope));
            return Some((
                ObjectDeleteJob {
                    id,
                    prefix,
                    work: ObjectDeleteWork::DeleteIncarnation,
                },
                token,
            ));
        }
    }

    /// Settle the job `id`. An incarnation with nothing left to delete
    /// leaves the queue. Any other end keeps it, to be tried again
    /// [`INCARNATION_DELETE_RETRY`] after `now`. A result of a job that is
    /// not the one in flight settles nothing.
    pub fn finish(&mut self, id: u64, outcome: ObjectDeleteOutcome, now: IggyTimestamp) {
        if self
            .running
            .as_ref()
            .is_none_or(|(running, ..)| *running != id)
        {
            return;
        }
        let Some((_, prefix, _scope)) = self.running.take() else {
            return;
        };
        match outcome {
            Ok(ObjectDeleteDone::IncarnationDeleted(Ok(0))) => {
                self.forget(&prefix);
                return;
            }
            Ok(ObjectDeleteDone::IncarnationDeleted(Ok(left))) => warn!(
                ?prefix,
                left, "some objects of a deleted incarnation were not deleted and are tried again"
            ),
            Ok(ObjectDeleteDone::IncarnationDeleted(Err(error))) => warn!(
                ?prefix,
                %error,
                "listing the objects of a deleted incarnation failed and is tried again"
            ),
            // A cancelled job, or a result of another kind of work, proves
            // nothing about what the store still holds.
            Ok(ObjectDeleteDone::Swept(_) | ObjectDeleteDone::Deleted { .. }) | Err(_) => {}
        }
        self.forget(&prefix);
        let due_at = now
            .as_micros()
            .saturating_add(micros(INCARNATION_DELETE_RETRY));
        self.due_at.insert(prefix, due_at);
        self.by_due.insert((due_at, prefix));
    }

    /// Forget every incarnation and cancel the job in flight.
    pub fn clear(&mut self) {
        *self = Self {
            jobs: self.jobs,
            ..Self::default()
        };
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.due_at.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.due_at.is_empty()
    }

    /// Whether the incarnation waits for its delete.
    #[must_use]
    pub fn contains(&self, prefix: &ObjectPrefix) -> bool {
        self.due_at.contains_key(prefix)
    }

    fn forget(&mut self, prefix: &ObjectPrefix) {
        if let Some(due_at) = self.due_at.remove(prefix) {
            self.by_due.remove(&(due_at, *prefix));
        }
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
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

    fn swept(start_offsets: &[u64], end_offsets: &[(u64, u64)]) -> SweptObjects {
        SweptObjects {
            listed: listed(start_offsets),
            end_offsets: end_offsets.iter().copied().collect(),
        }
    }

    #[test]
    fn given_recorded_ends_when_asking_what_ended_should_take_the_recorded_end_at_or_below_the_bound()
     {
        // The next start places the segment at 0 at or below 9, and its
        // object records less.
        let objects = swept(&[0, 10, 20], &[(0, 7), (10, 19)]);

        assert_eq!(objects.ended_by(&[30], 6), []);
        assert_eq!(objects.ended_by(&[30], 9), [(0, 7)]);
        assert_eq!(objects.ended_by(&[30], 19), [(0, 7), (10, 19)]);
    }

    #[test]
    fn given_an_object_that_records_more_than_the_next_start_allows_when_asking_what_ended_should_keep_it_above_the_bound()
     {
        // The object at 0 holds offsets through 14, over the start at 10.
        let objects = swept(&[0, 10], &[(0, 14)]);

        assert_eq!(sweepable(&objects.listed, &[20], 9), [(0, 9)]);
        assert_eq!(objects.ended_by(&[20], 9), []);
        assert_eq!(objects.ended_by(&[20], 13), []);
        assert_eq!(objects.ended_by(&[20], 14), [(0, 14)]);
    }

    #[test]
    fn given_a_recorded_end_the_starts_do_not_allow_when_asking_what_ended_should_keep_the_segment()
    {
        let objects = swept(&[0, 10], &[(0, 9), (10, 19)]);

        // Nothing starts after the object at 10.
        assert_eq!(objects.ended_by(&[], u64::MAX), [(0, 9)]);
        // The partition still lists a segment at 0.
        assert_eq!(objects.ended_by(&[0, 30], u64::MAX), [(10, 19)]);
        // No end was read for the object at 10.
        assert_eq!(
            swept(&[0, 10], &[(0, 9)]).ended_by(&[30], u64::MAX),
            [(0, 9)]
        );
    }

    #[test]
    fn given_a_retired_segment_when_asking_what_is_due_should_wait_for_the_grace_period_and_the_bound()
     {
        let mut retired = RetiredSegments::default();
        retired.retire(0, 9, at(1_000));

        assert_eq!(
            retired.due(at(1_000 + grace() - 1), 9, |start| start == 10),
            []
        );
        assert_eq!(retired.due(at(1_000 + grace()), 8, |start| start == 10), []);
        assert_eq!(
            retired.due(at(1_000 + grace()), 9, |start| [0, 10].contains(&start)),
            []
        );
        assert_eq!(
            retired.due(at(1_000 + grace()), 9, |start| start == 10),
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

        assert_eq!(retired.due(at(5_000 + grace() - 1), 9, |_| false), []);
        assert_eq!(retired.due(at(5_000 + grace()), 9, |_| false).len(), 1);
    }

    #[test]
    fn given_a_swept_segment_that_waits_already_when_queued_should_keep_its_first_moment() {
        let mut retired = RetiredSegments::default();
        retired.retire(0, 9, at(1_000));
        retired.retire_swept(0, 9, at(5_000));

        assert_eq!(retired.due(at(1_000 + grace()), 9, |_| false).len(), 1);
    }

    #[test]
    fn given_a_delete_result_when_settling_should_keep_failed_and_requeued_segments() {
        let mut retired = RetiredSegments::default();
        for start_offset in [0, 10, 20] {
            retired.retire(start_offset, start_offset + 9, at(1_000));
        }
        let sent = retired.due(at(1_000 + grace()), u64::MAX, |_| false);
        assert_eq!(sent.len(), 3);
        // The segment at 20 was queued again while the delete ran.
        retired.retire(20, 29, at(9_000));

        retired.settle(&sent, &[10]);

        assert_eq!(retired.start_offsets(), [10, 20]);
    }
}
