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

//! The deletion of the objects of long-term segments that the long-term log
//! no longer lists.
//!
//! A key of a long-term segment names both its offsets, so the log alone
//! says whether an object it does not list can ever be listed:
//! [`LongTermObject::classify`]. An object that cannot is dead. At or below
//! the floor, segments are only joined by a replace or removed from the
//! front by a trim, so the offsets where segments start and end there only
//! get fewer, and an object that is dead once stays dead. A primary that
//! acts on a state the group has moved past therefore deletes nothing a
//! later state lists.
//!
//! Only the partition primary deletes. It learns of dead objects from the
//! commands it applies, and from a listing of the incarnation once in its
//! view and every [`LONG_TERM_SWEEP_INTERVAL`] after, which finds what an
//! earlier primary left and what a rewrite stored without ever committing.
//!
//! A reader of the bucket goes by the manifest. An object is deleted
//! [`LONG_TERM_DELETE_DELAY`] after this primary stored a manifest of a
//! state in which the object was dead already, so a reader that fetched the
//! manifest before it has that long to finish with the object.
//!
//! What waits is kept in memory for the view. The next primary lists the
//! objects again and waits again.

use std::collections::BTreeMap;
use std::time::Duration;

use iggy_common::IggyTimestamp;
use iggy_object_store::{ManifestSegment, TierScope};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use tracing::{debug, warn};

use crate::long_term::{REPLACE_SEGMENTS, REPLACE_SOURCES_MAX, ReplaceSegments};
use crate::long_term::{TRIM_SEGMENTS, TrimSegments};
use crate::manifest_publish::ManifestProgress;
use crate::state_machine::MachineCommand;
use crate::{
    IggyPartition, ObjectDeleteDone, ObjectDeleteJob, ObjectDeleteOutcome, ObjectDeleteStart,
    ObjectDeleteWork,
};

/// How long a dead object stays in the store after this primary stored a
/// manifest that does not list it.
pub const LONG_TERM_DELETE_DELAY: Duration = Duration::from_hours(1);

/// How long after a listing of the incarnation's objects the primary lists
/// them again.
pub const LONG_TERM_SWEEP_INTERVAL: Duration = Duration::from_hours(1);

/// The most dead objects one listing reports, and the most that wait for
/// their delete at once. The next listing reports the ones left out.
pub const LONG_TERM_SWEEP_DEAD_MAX: usize = 65_536;

/// The first and the last offset of a long-term segment, which is all that
/// [`LongTermObject::classify`] reads of one.
pub trait SegmentSpan {
    fn start_offset(&self) -> u64;
    fn end_offset(&self) -> u64;
}

impl SegmentSpan for ManifestSegment {
    fn start_offset(&self) -> u64 {
        self.start_offset
    }

    fn end_offset(&self) -> u64 {
        self.end_offset
    }
}

impl SegmentSpan for (u64, u64) {
    fn start_offset(&self) -> u64 {
        self.0
    }

    fn end_offset(&self) -> u64 {
        self.1
    }
}

/// What the long-term log says of an object keyed by two offsets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LongTermObject {
    /// The log lists a segment with these offsets.
    Listed,
    /// Not listed, and a later command can still list it: the output of a
    /// rewrite whose command has not committed, or one that lost to another
    /// and still starts and ends where segments do. Never deleted.
    Unannounced,
    /// No later state of the log lists it.
    Dead,
}

impl LongTermObject {
    /// Sort the object from `start_offset` to `end_offset` by `segments`,
    /// the segments of the long-term log in offset order.
    ///
    /// The object is dead when either of its offsets is at or below the
    /// floor and no segment starts, or ends, there. A later segment needs
    /// both: a publish starts above the floor, and a replace starts where a
    /// segment starts and ends where one ends.
    #[must_use]
    pub fn classify<S: SegmentSpan>(segments: &[S], start_offset: u64, end_offset: u64) -> Self {
        let Some(floor) = segments.last().map(SegmentSpan::end_offset) else {
            return Self::Unannounced;
        };
        let starts = segments.binary_search_by_key(&start_offset, SegmentSpan::start_offset);
        if starts.is_ok_and(|index| segments[index].end_offset() == end_offset) {
            return Self::Listed;
        }
        let ends = segments.binary_search_by_key(&end_offset, SegmentSpan::end_offset);
        let start_can_be_listed = start_offset > floor || starts.is_ok();
        let end_can_be_listed = end_offset > floor || ends.is_ok();
        if start_can_be_listed && end_can_be_listed {
            Self::Unannounced
        } else {
            Self::Dead
        }
    }
}

/// What a listing of an incarnation's long-term objects found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SweptLongTerm {
    /// The first and the last offset of the dead objects, ascending, at most
    /// [`LONG_TERM_SWEEP_DEAD_MAX`] of them.
    pub dead: Vec<(u64, u64)>,
    /// How many objects are neither listed nor dead.
    pub unannounced: usize,
}

impl SweptLongTerm {
    /// Sort the objects `listed`, each by its first and last offset and
    /// ascending, by the `segments` of the long-term log.
    #[must_use]
    pub fn classified<S: SegmentSpan>(
        listed: impl IntoIterator<Item = (u64, u64)>,
        segments: &[S],
    ) -> Self {
        let mut swept = Self::default();
        for (start_offset, end_offset) in listed {
            match LongTermObject::classify(segments, start_offset, end_offset) {
                LongTermObject::Unannounced => swept.unannounced += 1,
                LongTermObject::Dead if swept.dead.len() < LONG_TERM_SWEEP_DEAD_MAX => {
                    swept.dead.push((start_offset, end_offset));
                }
                LongTermObject::Listed | LongTermObject::Dead => {}
            }
        }
        swept
    }
}

/// The dead objects that wait for their delete, by first and last offset.
#[derive(Debug, Default)]
pub struct DeadObjects {
    /// The objects no manifest of this view leaves out yet, each with the
    /// state of the log in which it was found dead.
    unpublished: BTreeMap<(u64, u64), ManifestProgress>,
    /// The objects a stored manifest leaves out, each with the moment that
    /// was known, in microseconds.
    waiting: BTreeMap<(u64, u64), u64>,
}

impl DeadObjects {
    /// Queue `object`, found dead in the state `died_at`, to wait for a
    /// manifest of that state or a later one. An object that is queued
    /// already keeps its place.
    ///
    /// A full queue takes no more. What it leaves out is found by the next
    /// listing, so a store that refuses every delete does not grow the
    /// queue for as long as the view lasts.
    pub fn queue(&mut self, object: (u64, u64), died_at: ManifestProgress) {
        if self.queued() < LONG_TERM_SWEEP_DEAD_MAX && !self.waiting.contains_key(&object) {
            self.unpublished.entry(object).or_insert(died_at);
        }
    }

    /// A manifest of the state `stored` reached the store by `now`. The
    /// objects found dead in that state or an earlier one wait from `now`.
    pub fn manifest_stored(&mut self, stored: ManifestProgress, now: IggyTimestamp) {
        let waiting = &mut self.waiting;
        self.unpublished.retain(|object, died_at| {
            let left_out = *died_at <= stored;
            if left_out {
                waiting.insert(*object, now.as_micros());
            }
            !left_out
        });
    }

    /// The objects that waited [`LONG_TERM_DELETE_DELAY`] by `now`.
    #[must_use]
    pub fn due(&self, now: IggyTimestamp) -> Vec<(u64, u64)> {
        let delay = micros(LONG_TERM_DELETE_DELAY);
        self.waiting
            .iter()
            .filter(|(_, since)| now.as_micros() >= since.saturating_add(delay))
            .map(|(object, _)| *object)
            .collect()
    }

    /// Forget the objects of `sent` that a delete removed. One in `failed`
    /// stays and is sent again.
    pub fn settle(&mut self, sent: &[(u64, u64)], failed: &[(u64, u64)]) {
        for object in sent {
            if !failed.contains(object) {
                self.waiting.remove(object);
            }
        }
    }

    /// How many objects are queued, with a manifest that leaves them out
    /// or without one.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.unpublished.len() + self.waiting.len()
    }
}

/// What a replica deletes of its long-term objects while it is the primary
/// of a partition that places its batches. Nothing here is persisted.
#[derive(Default)]
pub struct LongTermDeletes {
    /// The view this state belongs to, `None` until the replica acts as the
    /// primary.
    view: Option<u32>,
    /// When this view last listed the incarnation's objects, in
    /// microseconds.
    swept_at: Option<u64>,
    /// How many objects that listing found neither listed nor dead.
    unannounced: usize,
    dead: DeadObjects,
    /// The job in flight: its number, what it does, and the scope whose end
    /// stops it.
    running: Option<(u64, RunningLongTermDelete, TierScope)>,
    /// Number of the last job this partition started. Kept across views, so
    /// the result of a job of an earlier view is never taken for another's.
    jobs: u64,
}

impl LongTermDeletes {
    /// A manifest of the state `stored` was stored in `view` by `now`.
    pub(crate) fn manifest_stored(
        &mut self,
        view: u32,
        stored: ManifestProgress,
        now: IggyTimestamp,
    ) {
        if self.view == Some(view) {
            self.dead.manifest_stored(stored, now);
        }
    }

    fn sweep_due(&self, now: IggyTimestamp) -> bool {
        self.swept_at.is_none_or(|swept_at| {
            now.as_micros() >= swept_at.saturating_add(micros(LONG_TERM_SWEEP_INTERVAL))
        })
    }

    /// Forget what waits and cancel the job in flight.
    fn clear(&mut self) {
        *self = Self {
            jobs: self.jobs,
            ..Self::default()
        };
    }
}

/// What the long-term delete job in flight was started with.
enum RunningLongTermDelete {
    /// A listing, sorted by the log as it was in this state.
    Sweep { taken_at: ManifestProgress },
    /// A delete of these objects.
    Delete(Vec<(u64, u64)>),
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// How many long-term objects the last listing of this view found that
    /// the log does not list and that are not dead. Zero on a replica that
    /// is not the primary, and before the view's first listing.
    #[must_use]
    pub fn long_term_unannounced_objects(&self) -> usize {
        if self.long_term_deletes.view == Some(self.consensus().view())
            && self.deletes_long_term_objects()
        {
            self.long_term_deletes.unannounced
        } else {
            0
        }
    }

    /// Start the next object-store job of this partition's long-term
    /// deletes, when this replica is the primary and none is in flight. The
    /// caller runs it under the returned scope and hands the result to
    /// [`Self::finish_long_term_delete`].
    ///
    /// A listing goes first when one is due. Every other job deletes the
    /// objects whose delay ended by `now`.
    ///
    /// A replica that is not the primary drops what it queued and cancels
    /// the job it started, so a pass over a demoted replica is what stops
    /// its job.
    pub(crate) fn begin_long_term_delete(
        &mut self,
        now: IggyTimestamp,
    ) -> Option<ObjectDeleteStart> {
        if !self.deletes_long_term_objects() {
            self.long_term_deletes.clear();
            return None;
        }
        let taken_at = self.long_term_progress();
        let deletes = self.long_term_deletes_of_this_view();
        if deletes.running.is_some() {
            return None;
        }
        let (work, running) = if deletes.sweep_due(now) {
            let segments = self
                .long_term_segments()
                .iter()
                .map(|segment| (segment.start_offset, segment.end_offset))
                .collect();
            (
                ObjectDeleteWork::SweepLongTerm { segments },
                RunningLongTermDelete::Sweep { taken_at },
            )
        } else {
            let due = deletes.dead.due(now);
            if due.is_empty() {
                return None;
            }
            (
                ObjectDeleteWork::DeleteLongTerm {
                    segments: due.clone(),
                },
                RunningLongTermDelete::Delete(due),
            )
        };
        let scope = TierScope::new();
        let token = scope.token();
        let prefix = crate::object_prefix(self.namespace(), self.created_revision());
        let deletes = &mut self.long_term_deletes;
        deletes.jobs += 1;
        let id = deletes.jobs;
        deletes.running = Some((id, running, scope));
        Some((ObjectDeleteJob { id, prefix, work }, token))
    }

    /// Take the result of the job numbered `id`. A result of a job this
    /// partition does not wait for, or one that arrives after primacy moved,
    /// changes nothing.
    ///
    /// A listing queues the dead objects it found. One that failed is
    /// repeated by a later pass. A delete forgets the objects it removed,
    /// and the ones it did not stay queued.
    pub(crate) fn finish_long_term_delete(
        &mut self,
        id: u64,
        outcome: ObjectDeleteOutcome,
        now: IggyTimestamp,
    ) {
        let awaited = self
            .long_term_deletes
            .running
            .as_ref()
            .is_some_and(|(running, _, _)| *running == id);
        if !awaited {
            return;
        }
        if !self.deletes_long_term_objects()
            || self.long_term_deletes.view != Some(self.consensus().view())
        {
            self.long_term_deletes.clear();
            return;
        }
        let namespace_raw = self.namespace().inner();
        let stored = self.stored_manifest_progress();
        let deletes = &mut self.long_term_deletes;
        let Some((_, running, _scope)) = deletes.running.take() else {
            return;
        };
        match (running, outcome) {
            (
                RunningLongTermDelete::Sweep { taken_at },
                Ok(ObjectDeleteDone::LongTermSwept(Ok(swept))),
            ) => {
                for object in &swept.dead {
                    deletes.dead.queue(*object, taken_at);
                }
                // The manifest in the store may hold the state the listing
                // was sorted by, and then no later one is on its way.
                if let Some(stored) = stored {
                    deletes.dead.manifest_stored(stored, now);
                }
                deletes.swept_at = Some(now.as_micros());
                deletes.unannounced = swept.unannounced;
                debug!(
                    namespace_raw,
                    dead = swept.dead.len(),
                    unannounced = swept.unannounced,
                    queued = deletes.dead.queued(),
                    "listed the long-term objects of the partition"
                );
            }
            (
                RunningLongTermDelete::Sweep { .. },
                Ok(ObjectDeleteDone::LongTermSwept(Err(error))),
            ) => {
                warn!(
                    namespace_raw,
                    %error,
                    "listing the long-term objects of the partition failed and is tried again"
                );
            }
            (
                RunningLongTermDelete::Delete(sent),
                Ok(ObjectDeleteDone::LongTermDeleted { failed }),
            ) => {
                deletes.dead.settle(&sent, &failed);
                if !failed.is_empty() {
                    warn!(
                        namespace_raw,
                        failed = failed.len(),
                        queued = deletes.dead.queued(),
                        "some long-term objects were not deleted and are tried again"
                    );
                }
            }
            // A cancelled job, or a result of another kind of work, settles
            // nothing: the queue is as the job found it, and a listing that
            // did not finish runs again.
            (RunningLongTermDelete::Sweep { .. } | RunningLongTermDelete::Delete(_), _) => {}
        }
    }

    /// The long-term segments that `command` removes from the log if it
    /// applies, read before it does: the ones a replace holds between its
    /// offsets, and the ones a trim finds at or below its own. Empty on a
    /// replica that deletes nothing, and for any other command.
    pub(crate) fn long_term_segments_at_stake(
        &self,
        command: MachineCommand<'_>,
    ) -> Vec<(u64, u64)> {
        if !self.deletes_long_term_objects() {
            return Vec::new();
        }
        let (first_offset, last_offset, at_most) = match command.kind {
            REPLACE_SEGMENTS => match ReplaceSegments::try_from(command.payload) {
                Ok(ReplaceSegments(merged)) => {
                    (merged.start_offset, merged.end_offset, REPLACE_SOURCES_MAX)
                }
                Err(_) => return Vec::new(),
            },
            TRIM_SEGMENTS => match TrimSegments::try_from(command.payload) {
                Ok(trim) => (0, trim.up_to_offset, usize::MAX),
                Err(_) => return Vec::new(),
            },
            _ => return Vec::new(),
        };
        let segments = self.long_term_segments();
        let from = segments.partition_point(|segment| segment.start_offset < first_offset);
        let to = segments.partition_point(|segment| segment.end_offset <= last_offset);
        segments
            .get(from..to)
            .filter(|at_stake| at_stake.len() <= at_most)
            .map_or_default(|at_stake| {
                at_stake
                    .iter()
                    .map(|segment| (segment.start_offset, segment.end_offset))
                    .collect()
            })
    }

    /// Queue the objects of the segments of `at_stake` that the command
    /// removed, which makes them dead. They wait for a manifest of the log
    /// as it is now.
    pub(crate) fn queue_dead_long_term_objects(&mut self, at_stake: &[(u64, u64)]) {
        if at_stake.is_empty() || !self.deletes_long_term_objects() {
            return;
        }
        let segments = self.long_term_segments();
        let dead: Vec<(u64, u64)> = at_stake
            .iter()
            .copied()
            .filter(|(start_offset, end_offset)| {
                LongTermObject::classify(segments, *start_offset, *end_offset)
                    == LongTermObject::Dead
            })
            .collect();
        let died_at = self.long_term_progress();
        let deletes = self.long_term_deletes_of_this_view();
        for object in dead {
            deletes.dead.queue(object, died_at);
        }
    }

    /// How many dead objects this view has queued.
    #[cfg(test)]
    pub(crate) fn queued_dead_long_term_objects(&self) -> usize {
        self.long_term_deletes.dead.queued()
    }

    /// Whether this replica is the one that deletes the partition's
    /// long-term objects: the settled primary of a partition that places
    /// its batches.
    fn deletes_long_term_objects(&self) -> bool {
        self.places_batches() && self.state_machines_current()
    }

    /// The delete state of the current view. What an earlier view left is
    /// dropped, and its job cancelled: a primary only acts on what it
    /// learned while it has been the primary without a break.
    fn long_term_deletes_of_this_view(&mut self) -> &mut LongTermDeletes {
        let view = self.consensus().view();
        if self.long_term_deletes.view != Some(view) {
            self.long_term_deletes.clear();
            self.long_term_deletes.view = Some(view);
        }
        &mut self.long_term_deletes
    }
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::long_term::{LongTermLog, PUBLISH_SEGMENT, PublishSegment};
    use crate::state_machine::{Applied, PartitionStateMachine};
    use iggy_object_store::SEGMENT_HASH_LEN;

    use LongTermObject::{Dead, Listed, Unannounced};

    fn at(micros: u64) -> IggyTimestamp {
        IggyTimestamp::from(micros)
    }

    fn delay() -> u64 {
        micros(LONG_TERM_DELETE_DELAY)
    }

    const fn progress(floor: u64, trim_point: Option<u64>, segments: usize) -> ManifestProgress {
        ManifestProgress::new(Some(floor), trim_point, segments)
    }

    #[test]
    fn given_a_long_term_log_when_classifying_objects_should_take_as_dead_only_those_no_later_log_lists()
     {
        // A trim removed everything below 10, and the floor is 49.
        let segments = [(10, 19), (20, 29), (30, 49)];
        let cases = [
            ((10, 19), Listed, "a segment of the log"),
            ((30, 49), Listed, "the last segment of the log"),
            ((0, 9), Dead, "below the trim point"),
            ((0, 19), Dead, "starts below the trim point"),
            (
                (5, 60),
                Dead,
                "starts below the trim point and ends above the floor",
            ),
            ((10, 15), Dead, "ends inside a segment"),
            ((12, 19), Dead, "starts inside a segment"),
            ((22, 25), Dead, "inside a segment"),
            ((15, 25), Dead, "from inside one segment into the next"),
            ((40, 60), Dead, "starts inside the last segment"),
            (
                (10, 29),
                Unannounced,
                "what a replace of two segments stores",
            ),
            (
                (20, 49),
                Unannounced,
                "what a replace up to the floor stores",
            ),
            (
                (10, 49),
                Unannounced,
                "what a replace of every segment stores",
            ),
            (
                (30, 60),
                Unannounced,
                "from a start of the log to above the floor",
            ),
            ((50, 60), Unannounced, "what the next publish stores"),
            ((70, 80), Unannounced, "above the floor with a gap"),
            ((50, 50), Unannounced, "one offset above the floor"),
            ((49, 49), Dead, "the floor alone, where no segment starts"),
            ((30, 30), Dead, "one offset where a segment starts"),
        ];

        for ((start_offset, end_offset), expected, what) in cases {
            assert_eq!(
                LongTermObject::classify(&segments, start_offset, end_offset),
                expected,
                "{start_offset}..={end_offset}: {what}"
            );
        }
    }

    #[test]
    fn given_an_empty_long_term_log_when_classifying_an_object_should_take_nothing_as_dead() {
        let segments: [(u64, u64); 0] = [];

        assert_eq!(LongTermObject::classify(&segments, 0, 9), Unannounced);
        assert_eq!(LongTermObject::classify(&segments, 5, 5), Unannounced);
    }

    #[test]
    fn given_segments_of_one_offset_when_classifying_objects_should_tell_starts_from_ends() {
        let segments = [(3, 3), (4, 4), (5, 9)];

        assert_eq!(LongTermObject::classify(&segments, 3, 3), Listed);
        assert_eq!(LongTermObject::classify(&segments, 3, 4), Unannounced);
        assert_eq!(LongTermObject::classify(&segments, 4, 9), Unannounced);
        assert_eq!(LongTermObject::classify(&segments, 3, 5), Dead);
        assert_eq!(LongTermObject::classify(&segments, 2, 3), Dead);
        assert_eq!(LongTermObject::classify(&segments, 9, 9), Dead);
    }

    /// Offsets the histories stay below, so every object a test can name is
    /// classified in every state.
    const HISTORY_OFFSETS: u64 = 48;

    /// A long-term log under random commands. Xorshift, so the run needs no
    /// new dependency.
    struct History {
        random: u64,
        log: LongTermLog,
        op: u64,
    }

    impl History {
        fn new(seed: u64) -> Self {
            Self {
                random: seed,
                log: LongTermLog::default(),
                op: 0,
            }
        }

        fn below(&mut self, bound: u64) -> u64 {
            self.random ^= self.random << 13;
            self.random ^= self.random >> 7;
            self.random ^= self.random << 17;
            self.random % bound
        }

        fn spans(&self) -> Vec<(u64, u64)> {
            self.log
                .segments()
                .iter()
                .map(|segment| (segment.start_offset, segment.end_offset))
                .collect()
        }

        fn apply(&mut self, kind: crate::state_machine::CommandKind, payload: &[u8]) {
            self.op += 1;
            let applied = self.log.apply(self.op, MachineCommand { kind, payload });
            assert_eq!(applied, Applied::Changed);
        }

        fn publish(&mut self) {
            let start_offset = self.log.floor().map_or(0, |floor| floor + 1);
            let end_offset = start_offset + self.below(4);
            if end_offset >= HISTORY_OFFSETS {
                return;
            }
            let segment = ManifestSegment {
                start_offset,
                end_offset,
                start_timestamp: 0,
                end_timestamp: 0,
                max_timestamp: 0,
                size: 1 + self.below(100),
                crc32c: u32::try_from(self.below(u64::from(u32::MAX))).unwrap(),
                hash: [0; SEGMENT_HASH_LEN],
            };
            self.apply(PUBLISH_SEGMENT, &PublishSegment(segment).encode());
        }

        fn replace(&mut self) {
            let segments = self.log.segments();
            if segments.len() < 2 {
                return;
            }
            let count = u64::try_from(segments.len()).unwrap();
            let first = self.below(count - 1);
            let last = first + 1 + self.below((count - first - 1).min(4));
            let sources = &self.log.segments()
                [usize::try_from(first).unwrap()..=usize::try_from(last).unwrap()];
            let merged = sources[1..]
                .iter()
                .fold(sources[0], |merged, next| ManifestSegment {
                    end_offset: next.end_offset,
                    size: merged.size + next.size,
                    crc32c: crc32c::crc32c_combine(
                        merged.crc32c,
                        next.crc32c,
                        usize::try_from(next.size).unwrap(),
                    ),
                    ..merged
                });
            self.apply(REPLACE_SEGMENTS, &ReplaceSegments(merged).encode());
        }

        fn trim(&mut self) {
            let up_to_offset = self.below(self.log.floor().map_or(1, |floor| floor + 3));
            self.apply(TRIM_SEGMENTS, &TrimSegments { up_to_offset }.encode());
        }

        fn step(&mut self) {
            match self.below(8) {
                0..4 => self.publish(),
                4..7 => self.replace(),
                _ => self.trim(),
            }
        }
    }

    /// Every object below [`HISTORY_OFFSETS`], by first and last offset.
    fn every_object() -> impl Iterator<Item = (u64, u64)> {
        (0..HISTORY_OFFSETS).flat_map(|start_offset| {
            (start_offset..HISTORY_OFFSETS).map(move |end_offset| (start_offset, end_offset))
        })
    }

    /// The delete acts on the log as one primary saw it, which may be a
    /// state the group has moved past. So an object dead in one state must
    /// be dead in every later one, whatever commands came between, or a
    /// delete could remove what a later manifest lists.
    #[test]
    fn given_random_publish_replace_and_trim_histories_when_an_object_is_dead_should_stay_dead_in_every_later_state()
     {
        for seed in 1..=200 {
            let mut history = History::new(seed);
            let mut dead_before: Vec<(u64, u64)> = Vec::new();
            for step in 0..64 {
                let before = history.spans();
                history.step();
                let segments = history.spans();

                for (start_offset, end_offset) in &dead_before {
                    assert_eq!(
                        LongTermObject::classify(&segments, *start_offset, *end_offset),
                        Dead,
                        "seed {seed} step {step}: {start_offset}..={end_offset} was dead \
                         and is not in {segments:?}"
                    );
                }
                for (start_offset, end_offset) in &segments {
                    assert_eq!(
                        LongTermObject::classify(&segments, *start_offset, *end_offset),
                        Listed
                    );
                }
                // What a command removed is what the primary queues on its
                // commit, without a listing.
                for (start_offset, end_offset) in &before {
                    if !segments.contains(&(*start_offset, *end_offset)) {
                        assert_eq!(
                            LongTermObject::classify(&segments, *start_offset, *end_offset),
                            Dead,
                            "seed {seed} step {step}: removed {start_offset}..={end_offset}"
                        );
                    }
                }
                dead_before = every_object()
                    .filter(|(start_offset, end_offset)| {
                        LongTermObject::classify(&segments, *start_offset, *end_offset) == Dead
                    })
                    .collect();
            }
            assert!(
                history.log.segments().len() < 64 && !dead_before.is_empty(),
                "seed {seed}: the history replaced or trimmed something"
            );
        }
    }

    #[test]
    fn given_listed_objects_when_sorting_a_listing_should_report_the_dead_and_count_the_unannounced()
     {
        let segments = [(10, 19), (20, 29)];
        let listed = [
            (0, 9),
            (10, 14),
            (10, 19),
            (10, 29),
            (15, 19),
            (20, 29),
            (30, 39),
        ];

        let swept = SweptLongTerm::classified(listed, &segments);

        assert_eq!(swept.dead, [(0, 9), (10, 14), (15, 19)]);
        assert_eq!(
            swept.unannounced, 2,
            "the replace output and the next segment"
        );
    }

    #[test]
    fn given_more_dead_objects_than_a_listing_reports_when_sorting_it_should_report_the_first_ones()
    {
        let dead_max = u64::try_from(LONG_TERM_SWEEP_DEAD_MAX).unwrap();
        let segments = [(dead_max + 10, dead_max + 19)];
        let listed = (0..dead_max + 5).map(|offset| (offset, offset));

        let swept = SweptLongTerm::classified(listed, &segments);

        assert_eq!(swept.dead.len(), LONG_TERM_SWEEP_DEAD_MAX);
        assert_eq!(swept.dead.last(), Some(&(dead_max - 1, dead_max - 1)));
    }

    #[test]
    fn given_a_dead_object_when_no_manifest_of_its_state_is_stored_should_not_be_due() {
        let mut dead = DeadObjects::default();
        // Found dead after a replace took the list from three to two.
        dead.queue((0, 9), progress(29, None, 2));

        assert_eq!(dead.due(at(u64::MAX)), []);
        // The manifest of the state before the replace still lists it.
        dead.manifest_stored(progress(29, None, 3), at(1_000));
        assert_eq!(dead.due(at(u64::MAX)), []);
        assert_eq!(dead.queued(), 1);
    }

    #[test]
    fn given_a_dead_object_when_a_manifest_of_its_state_is_stored_should_be_due_after_the_delay() {
        let mut dead = DeadObjects::default();
        dead.queue((0, 9), progress(29, None, 2));

        dead.manifest_stored(progress(29, None, 2), at(1_000));

        assert_eq!(dead.due(at(1_000 + delay() - 1)), []);
        assert_eq!(dead.due(at(1_000 + delay())), [(0, 9)]);
    }

    #[test]
    fn given_dead_objects_of_two_states_when_a_manifest_of_a_later_state_is_stored_should_start_the_delay_of_both()
     {
        let mut dead = DeadObjects::default();
        dead.queue((0, 9), progress(29, None, 2));
        dead.queue((10, 19), progress(29, Some(9), 1));
        dead.queue((40, 49), progress(59, Some(9), 1));

        dead.manifest_stored(progress(39, Some(9), 2), at(1_000));

        assert_eq!(dead.due(at(1_000 + delay())), [(0, 9), (10, 19)]);
        dead.manifest_stored(progress(59, Some(9), 1), at(5_000));
        assert_eq!(dead.due(at(1_000 + delay())), [(0, 9), (10, 19)]);
        assert_eq!(dead.due(at(5_000 + delay())), [(0, 9), (10, 19), (40, 49)]);
    }

    #[test]
    fn given_a_dead_object_that_waits_when_queued_or_published_again_should_keep_its_first_moment()
    {
        let mut dead = DeadObjects::default();
        dead.queue((0, 9), progress(29, None, 2));
        dead.manifest_stored(progress(29, None, 2), at(1_000));

        dead.queue((0, 9), progress(39, None, 3));
        dead.manifest_stored(progress(39, None, 3), at(9_000));

        assert_eq!(dead.queued(), 1);
        assert_eq!(dead.due(at(1_000 + delay())), [(0, 9)]);
    }

    #[test]
    fn given_a_full_queue_when_another_object_dies_should_leave_it_to_the_next_listing() {
        let mut dead = DeadObjects::default();
        let dead_max = u64::try_from(LONG_TERM_SWEEP_DEAD_MAX).unwrap();
        for offset in 0..dead_max {
            dead.queue((offset, offset), progress(dead_max, None, 1));
        }

        dead.queue((dead_max, dead_max), progress(dead_max + 9, None, 1));

        assert_eq!(dead.queued(), LONG_TERM_SWEEP_DEAD_MAX);
        dead.manifest_stored(progress(dead_max + 9, None, 1), at(1_000));
        assert!(
            !dead
                .due(at(1_000 + delay()))
                .contains(&(dead_max, dead_max))
        );
    }

    #[test]
    fn given_a_delete_result_when_settling_should_keep_the_objects_that_were_not_deleted() {
        let mut dead = DeadObjects::default();
        for object in [(0, 9), (10, 19), (20, 29)] {
            dead.queue(object, progress(39, None, 1));
        }
        dead.manifest_stored(progress(39, None, 1), at(1_000));
        let sent = dead.due(at(1_000 + delay()));
        assert_eq!(sent.len(), 3);

        dead.settle(&sent, &[(10, 19)]);

        assert_eq!(dead.due(at(1_000 + delay())), [(10, 19)]);
        assert_eq!(dead.queued(), 1);
    }
}
