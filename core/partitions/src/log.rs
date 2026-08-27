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

use crate::iggy_index::IggyIndexCache;
use crate::iggy_index_writer::IggyIndexWriter;
use crate::messages_writer::MessagesWriter;
use crate::poll_plan::SealedSegmentHandle;
use crate::segment::Segment;
use iggy_common::IggyByteSize;
use journal::Journal;
use ringbuffer::AllocRingBuffer;
use server_common::SegmentStorage;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;

const SEGMENTS_CAPACITY: usize = 1024;
const ACCESS_MAP_CAPACITY: usize = 8;
/// Max sealed segments per partition that keep a resident read handle (fd +
/// sparse index). Without a cap every sealed segment a reader ever touched pins
/// one fd for the partition's lifetime; the server-wide budget is this cap times
/// the partition count, so keep it small. 12 covers a lagging consumer's working
/// set (the recent sealed segments it re-reads) with room for a few concurrent
/// readers before an LRU eviction forces a re-open. Each resident index is
/// itself capped at `SEALED_INDEX_RESIDENT_MAX_BYTES`, so this also bounds the
/// partition's resident index bytes, not just its descriptors.
const SEALED_READ_STATE_CAP: usize = 12;

/// Tracking metadata for the journal's current state.
///
/// Replaces the server journal's `Inner` struct — lives in the `SegmentedLog`
/// rather than inside the journal, and is used as a lookup table for
/// constructing `MessageLookup` headers.
#[derive(Default, Debug, Clone, Copy)]
pub struct JournalInfo {
    pub base_offset: u64,
    pub current_offset: u64,
    pub first_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    pub messages_count: u32,
    pub size: IggyByteSize,
}

/// Groups the journal implementation with its tracking metadata.
#[derive(Debug)]
pub struct JournalState<J> {
    pub inner: J,
    pub info: JournalInfo,
}

impl<J> Journal for JournalState<J>
where
    J: Journal,
{
    type Header = J::Header;
    type Entry = J::Entry;
    type HeaderRef<'a>
        = J::HeaderRef<'a>
    where
        Self: 'a;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        self.inner.header(idx)
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        self.inner.previous_header(header)
    }

    fn append(&self, entry: Self::Entry) -> impl Future<Output = std::io::Result<()>> {
        self.inner.append(entry)
    }

    fn entry(&self, header: &Self::Header) -> impl Future<Output = Option<Self::Entry>> {
        self.inner.entry(header)
    }

    // Forward EVERY method, including the ones the trait could default. A
    // wrapper that silently substitutes a default for its inner journal is a
    // trap: `snapshot_op` would answer 0 and `set_snapshot_op` would vanish,
    // so a state-transfer install through this wrapper would neither evict
    // superseded entries nor advance the watermark it thinks it advanced.
    fn snapshot_op(&self) -> u64 {
        self.inner.snapshot_op()
    }

    fn set_snapshot_op(&self, op: u64) {
        self.inner.set_snapshot_op(op);
    }

    fn remaining_capacity(&self) -> Option<usize> {
        self.inner.remaining_capacity()
    }

    fn drain(
        &self,
        ops: std::ops::RangeInclusive<u64>,
    ) -> impl Future<Output = std::io::Result<Vec<Self::Entry>>> {
        self.inner.drain(ops)
    }

    fn truncate_from(&self, from_op: u64) -> impl Future<Output = std::io::Result<usize>> {
        self.inner.truncate_from(from_op)
    }

    fn last_op(&self) -> Option<u64> {
        self.inner.last_op()
    }
}

impl<J: Default> Default for JournalState<J> {
    fn default() -> Self {
        Self {
            inner: J::default(),
            info: JournalInfo::default(),
        }
    }
}

#[derive(Debug)]
pub struct SegmentedLog<J>
where
    J: Debug + Journal,
{
    journal: JournalState<J>,
    // Ring buffer tracking recently accessed segment indices for cleanup optimization.
    // A background task uses this to identify and close file descriptors for unused segments.
    _access_map: AllocRingBuffer<usize>,
    _cache: (),
    segments: Vec<Segment>,
    indexes: Vec<Option<IggyIndexCache>>,
    storage: Vec<SegmentStorage>,
    messages_writers: Vec<Option<Rc<MessagesWriter>>>,
    index_writers: Vec<Option<Rc<IggyIndexWriter>>>,
    // Parallel to `segments`: a shared read-state handle (fd + sparse index)
    // per segment, filled lazily on the first sealed-segment poll and cloned
    // into the off-borrow poll plan. Maintained in lockstep with `segments`
    // (push/remove together).
    sealed_read_state: Vec<SealedSegmentHandle>,
    // LRU of sealed-segment `start_offset`s (most-recently-used at the front)
    // bounding how many `sealed_read_state` handles stay resident, capped at
    // `SEALED_READ_STATE_CAP`. Keyed by offset (stable), not slot index (which
    // shifts on retire). See `touch_sealed_read_state`.
    sealed_lru: VecDeque<u64>,
}

impl<J> Default for SegmentedLog<J>
where
    J: Debug + Default + Journal,
{
    fn default() -> Self {
        Self {
            journal: JournalState::default(),
            _access_map: AllocRingBuffer::with_capacity_power_of_2(ACCESS_MAP_CAPACITY),
            _cache: (),
            segments: Vec::with_capacity(SEGMENTS_CAPACITY),
            storage: Vec::with_capacity(SEGMENTS_CAPACITY),
            indexes: Vec::with_capacity(SEGMENTS_CAPACITY),
            messages_writers: Vec::with_capacity(SEGMENTS_CAPACITY),
            index_writers: Vec::with_capacity(SEGMENTS_CAPACITY),
            sealed_read_state: Vec::with_capacity(SEGMENTS_CAPACITY),
            sealed_lru: VecDeque::with_capacity(SEALED_READ_STATE_CAP + 1),
        }
    }
}

impl<J> SegmentedLog<J>
where
    J: Debug + Journal,
{
    pub const fn has_segments(&self) -> bool {
        !self.segments.is_empty()
    }

    pub const fn segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    /// Mutable segment views. Length mutation lives in
    /// [`Self::add_persisted_segment`] / [`Self::retire_front`] only, so the
    /// parallel vecs cannot desync from the outside.
    pub fn segments_mut(&mut self) -> &mut [Segment] {
        &mut self.segments
    }

    /// Shared read-state handles, parallel to [`Self::segments`]. Cloned into
    /// the poll plan for sealed segments (see [`SealedSegmentHandle`]).
    pub fn sealed_read_state(&self) -> &[SealedSegmentHandle] {
        &self.sealed_read_state
    }

    /// Record a sealed-segment access and enforce [`SEALED_READ_STATE_CAP`]
    /// (LRU). `slot` indexes [`Self::segments`]; an out-of-range slot or an
    /// unsealed (active) segment is a no-op, so the poll path passes its start
    /// segment unconditionally. The LRU is keyed by `start_offset` - stable
    /// across retire, unlike the slot index. The touched segment moves to the
    /// most-recently-used front and its handle is marked tracked (eligible to
    /// cache a read fd, see `SealedSegmentReadState::tracked`); once more than
    /// the cap distinct sealed segments are tracked, the least-recently-used
    /// one's handle is untracked and dropped (replaced with a fresh empty
    /// handle) so its fd + sparse index free. An in-flight poll holding a clone
    /// of the dropped handle keeps it alive until it finishes (see
    /// [`SealedSegmentHandle`]).
    pub fn touch_sealed_read_state(&mut self, slot: usize) {
        let Some(touched) = self.segments.get(slot) else {
            return;
        };
        if !touched.sealed {
            return;
        }
        let start_offset = touched.start_offset;
        self.sealed_read_state[slot].tracked.set(true);
        if let Some(pos) = self
            .sealed_lru
            .iter()
            .position(|&offset| offset == start_offset)
        {
            self.sealed_lru.remove(pos);
        }
        self.sealed_lru.push_front(start_offset);
        if self.sealed_lru.len() > SEALED_READ_STATE_CAP {
            let Some(evicted) = self.sealed_lru.pop_back() else {
                return;
            };
            if let Some(evicted_slot) = self
                .segments
                .iter()
                .position(|segment| segment.start_offset == evicted)
            {
                // Untrack before orphaning so an in-flight poll holding the old
                // handle stops caching fds into it.
                self.sealed_read_state[evicted_slot].tracked.set(false);
                self.sealed_read_state[evicted_slot] = SealedSegmentHandle::default();
            }
        }
    }

    /// Wipe every shared read-state handle in place (cached fd + sparse index
    /// cleared, handle untracked) and reset the sealed LRU. In-flight polls
    /// hold `Rc` clones of these handles, so clearing the slots (not just
    /// dropping the pump's references) is what a suspended walk observes: its
    /// next segment resolve re-opens by path and sees the current files. Purge
    /// needs this because it recreates segment files at the paths it just
    /// unlinked; a stale cached fd would keep serving the purged inodes as
    /// live data. Retention retirement deliberately skips this: retired paths
    /// are never recreated, so a cached fd reading the unlinked inode stays
    /// consistent (see [`Self::retire_front`]).
    pub fn invalidate_sealed_read_state(&mut self) {
        for handle in &self.sealed_read_state {
            handle.tracked.set(false);
            *handle.fd.borrow_mut() = None;
            *handle.index.borrow_mut() = None;
            handle.offset_cursor.set(None);
        }
        self.sealed_lru.clear();
    }

    /// Retire the oldest segment: pop the front of every parallel vec in
    /// lockstep and purge the segment's sealed-LRU entry. Returns the pieces
    /// the caller still needs (stats + file unlink), or `None` on an empty
    /// log. The read-state handle is dropped here; an in-flight poll holding a
    /// clone keeps it alive until it finishes (a cached fd reads the unlinked
    /// inode).
    pub fn retire_front(&mut self) -> Option<(Segment, SegmentStorage)> {
        if self.segments.is_empty() {
            return None;
        }
        self.debug_assert_lockstep();
        let segment = self.segments.remove(0);
        let storage = self.storage.remove(0);
        self.indexes.remove(0);
        self.messages_writers.remove(0);
        self.index_writers.remove(0);
        self.sealed_read_state.remove(0);
        self.sealed_lru
            .retain(|&offset| offset != segment.start_offset);
        Some((segment, storage))
    }

    fn debug_assert_lockstep(&self) {
        debug_assert!(
            self.segments.len() == self.storage.len()
                && self.segments.len() == self.indexes.len()
                && self.segments.len() == self.messages_writers.len()
                && self.segments.len() == self.index_writers.len()
                && self.segments.len() == self.sealed_read_state.len(),
            "segment parallel vecs out of lockstep"
        );
    }

    pub fn storages_mut(&mut self) -> &mut [SegmentStorage] {
        &mut self.storage
    }

    pub const fn storages(&self) -> &Vec<SegmentStorage> {
        &self.storage
    }

    pub const fn messages_writers(&self) -> &Vec<Option<Rc<MessagesWriter>>> {
        &self.messages_writers
    }

    pub fn messages_writers_mut(&mut self) -> &mut [Option<Rc<MessagesWriter>>] {
        &mut self.messages_writers
    }

    pub const fn index_writers(&self) -> &Vec<Option<Rc<IggyIndexWriter>>> {
        &self.index_writers
    }

    pub fn index_writers_mut(&mut self) -> &mut [Option<Rc<IggyIndexWriter>>] {
        &mut self.index_writers
    }

    pub fn active_segment(&self) -> &Segment {
        self.segments
            .last()
            .expect("active segment called on empty log")
    }

    pub fn active_segment_mut(&mut self) -> &mut Segment {
        self.segments
            .last_mut()
            .expect("active segment called on empty log")
    }

    pub fn active_storage(&self) -> &SegmentStorage {
        self.storage
            .last()
            .expect("active storage called on empty log")
    }

    pub fn active_storage_mut(&mut self) -> &mut SegmentStorage {
        self.storage
            .last_mut()
            .expect("active storage called on empty log")
    }

    pub const fn indexes(&self) -> &Vec<Option<IggyIndexCache>> {
        &self.indexes
    }

    pub fn indexes_mut(&mut self) -> &mut [Option<IggyIndexCache>] {
        &mut self.indexes
    }

    /// Index cache for segment `index`, if resident.
    pub fn segment_indexes(&self, index: usize) -> Option<&IggyIndexCache> {
        self.indexes.get(index).and_then(Option::as_ref)
    }

    pub fn active_indexes(&self) -> Option<&IggyIndexCache> {
        self.indexes
            .last()
            .expect("active indexes called on empty log")
            .as_ref()
    }

    pub fn active_indexes_mut(&mut self) -> Option<&mut IggyIndexCache> {
        self.indexes
            .last_mut()
            .expect("active indexes called on empty log")
            .as_mut()
    }

    pub fn clear_active_indexes(&mut self) {
        let indexes = self
            .indexes
            .last_mut()
            .expect("active indexes called on empty log");
        *indexes = None;
    }

    pub fn ensure_indexes(&mut self) {
        let indexes = self
            .indexes
            .last_mut()
            .expect("active indexes called on empty log");
        if indexes.is_none() {
            *indexes = Some(IggyIndexCache::empty());
        }
    }

    pub fn add_persisted_segment(
        &mut self,
        segment: Segment,
        storage: SegmentStorage,
        messages_writer: Option<Rc<MessagesWriter>>,
        index_writer: Option<Rc<IggyIndexWriter>>,
    ) {
        self.segments.push(segment);
        self.storage.push(storage);
        self.indexes.push(None);
        self.messages_writers.push(messages_writer);
        self.index_writers.push(index_writer);
        self.sealed_read_state.push(SealedSegmentHandle::default());
        self.debug_assert_lockstep();
    }

    pub fn set_segment_indexes(&mut self, segment_index: usize, indexes: IggyIndexCache) {
        if let Some(segment_indexes) = self.indexes.get_mut(segment_index) {
            *segment_indexes = Some(indexes);
        }
    }
}

impl<J> SegmentedLog<J>
where
    J: Debug + Journal,
{
    pub const fn journal_mut(&mut self) -> &mut JournalState<J> {
        &mut self.journal
    }

    pub const fn journal(&self) -> &JournalState<J> {
        &self.journal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{PartitionJournal, PartitionJournalMemStorage};

    type TestLog = SegmentedLog<PartitionJournal<PartitionJournalMemStorage>>;

    /// Push a sealed segment with a resident (index-filled) read handle and
    /// return a clone of that handle, standing in for an in-flight poll's clone.
    /// Goes through `add_persisted_segment` so every parallel vec stays in
    /// lockstep (segment start offsets equal their slot indexes in every test
    /// that pushes ascending offsets from an empty log).
    fn push_resident_sealed(log: &mut TestLog, start_offset: u64) -> SealedSegmentHandle {
        log.add_persisted_segment(
            Segment {
                start_offset,
                sealed: true,
                ..Segment::default()
            },
            SegmentStorage::default(),
            None,
            None,
        );
        let slot = log.sealed_read_state.len() - 1;
        *log.sealed_read_state[slot].index.borrow_mut() = Some(IggyIndexCache::with_capacity(1));
        Rc::clone(&log.sealed_read_state[slot])
    }

    #[test]
    fn touch_sealed_read_state_evicts_least_recently_used_past_cap() {
        let mut log = TestLog::default();
        let handles: Vec<_> = (0..=SEALED_READ_STATE_CAP as u64)
            .map(|offset| push_resident_sealed(&mut log, offset))
            .collect();
        // Ascending touch order: slot/offset 0 is the least-recently used.
        for slot in 0..=SEALED_READ_STATE_CAP {
            log.touch_sealed_read_state(slot);
        }

        // Slot 0 dropped: the pump handle was replaced with a fresh empty one.
        assert!(
            !Rc::ptr_eq(&handles[0], &log.sealed_read_state()[0]),
            "least-recently-used handle must be dropped past the cap",
        );
        assert!(
            log.sealed_read_state()[0].index.borrow().is_none(),
            "the dropped slot resets to an empty handle",
        );
        // In-flight safety: the dropped handle's clone stays alive and still
        // sees its cached index, so a poll holding it finishes without a UAF.
        assert_eq!(
            Rc::strong_count(&handles[0]),
            1,
            "the dropped handle survives for an in-flight poll's clone",
        );
        assert!(
            handles[0].index.borrow().is_some(),
            "the in-flight clone keeps reading the cached index",
        );
        assert!(
            !handles[0].tracked.get(),
            "eviction untracks the orphaned handle so in-flight polls stop caching fds into it",
        );
        // Every more-recently-used slot is retained (same Rc).
        for (handle, resident) in handles.iter().zip(log.sealed_read_state().iter()).skip(1) {
            assert!(
                Rc::ptr_eq(handle, resident),
                "recently-used handles stay resident",
            );
        }
    }

    #[test]
    fn touch_sealed_read_state_reorders_eviction_on_reaccess() {
        let mut log = TestLog::default();
        let mut handles: Vec<_> = (0..SEALED_READ_STATE_CAP as u64)
            .map(|offset| push_resident_sealed(&mut log, offset))
            .collect();
        for slot in 0..SEALED_READ_STATE_CAP {
            log.touch_sealed_read_state(slot);
        }
        // Re-access slot 0 -> now most-recently used, so slot 1 becomes the
        // least-recently used and next to be evicted.
        log.touch_sealed_read_state(0);

        handles.push(push_resident_sealed(&mut log, SEALED_READ_STATE_CAP as u64));
        log.touch_sealed_read_state(SEALED_READ_STATE_CAP);

        assert!(
            !Rc::ptr_eq(&handles[1], &log.sealed_read_state()[1]),
            "the least-recently-used segment is evicted, not the re-accessed one",
        );
        assert!(
            Rc::ptr_eq(&handles[0], &log.sealed_read_state()[0]),
            "the re-accessed segment stays resident",
        );
    }

    #[test]
    fn evicted_sealed_slot_is_empty_and_refillable() {
        let mut log = TestLog::default();
        for offset in 0..=SEALED_READ_STATE_CAP as u64 {
            push_resident_sealed(&mut log, offset);
        }
        for slot in 0..=SEALED_READ_STATE_CAP {
            log.touch_sealed_read_state(slot);
        }

        // The evicted slot holds a fresh empty handle, so the next poll re-opens
        // instead of reusing a stale descriptor.
        let evicted = &log.sealed_read_state()[0];
        assert!(evicted.fd.borrow().is_none());
        assert!(evicted.index.borrow().is_none());

        // Re-filling it (what the next sealed poll does) works.
        *log.sealed_read_state()[0].index.borrow_mut() = Some(IggyIndexCache::with_capacity(1));
        assert!(log.sealed_read_state()[0].index.borrow().is_some());
    }

    #[test]
    fn touch_sealed_read_state_ignores_active_and_out_of_range_slots() {
        let mut log = TestLog::default();
        log.add_persisted_segment(
            Segment {
                start_offset: 0,
                sealed: false,
                ..Segment::default()
            },
            SegmentStorage::default(),
            None,
            None,
        );

        // Active (unsealed) segment: no LRU entry, handle stays untracked, so
        // its fd is never retained outside the cap.
        log.touch_sealed_read_state(0);
        assert!(log.sealed_lru.is_empty());
        assert!(!log.sealed_read_state()[0].tracked.get());

        // Out-of-range slot (the purge drain window empties the vec across
        // awaits): must be a no-op, not a panic.
        log.touch_sealed_read_state(1);
        assert!(log.sealed_lru.is_empty());
    }

    #[test]
    fn retire_front_purges_lru_entry_and_keeps_vecs_lockstep() {
        let mut log = TestLog::default();
        push_resident_sealed(&mut log, 0);
        push_resident_sealed(&mut log, 5);
        log.touch_sealed_read_state(0);
        log.touch_sealed_read_state(1);

        let (segment, _storage) = log.retire_front().expect("log has segments");
        assert_eq!(segment.start_offset, 0);
        assert!(
            !log.sealed_lru.contains(&0),
            "retire must purge the segment's LRU entry",
        );
        assert!(log.sealed_lru.contains(&5), "the survivor's entry stays");
        assert_eq!(log.segments().len(), 1);
        assert_eq!(log.sealed_read_state().len(), 1);
        assert_eq!(log.storages().len(), 1);

        assert!(log.retire_front().is_some());
        assert!(
            log.retire_front().is_none(),
            "an empty log retires nothing instead of panicking",
        );
    }

    #[test]
    fn invalidate_sealed_read_state_clears_slots_observed_by_in_flight_clones() {
        let mut log = TestLog::default();
        let handle = push_resident_sealed(&mut log, 0);
        log.touch_sealed_read_state(0);
        assert!(handle.tracked.get());
        assert!(handle.index.borrow().is_some());

        log.invalidate_sealed_read_state();

        assert!(log.sealed_lru.is_empty(), "the LRU resets with the slots");
        assert!(
            Rc::ptr_eq(&handle, &log.sealed_read_state()[0]),
            "the wipe happens in place, not by handle replacement",
        );
        // The shared state is what an in-flight poll's clone reads, so the
        // clone must observe the cleared slots and re-open by path.
        assert!(handle.index.borrow().is_none());
        assert!(handle.fd.borrow().is_none());
        assert!(
            !handle.tracked.get(),
            "an in-flight open must not cache a new fd into the wiped handle",
        );
    }
}
