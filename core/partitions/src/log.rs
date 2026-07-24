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

use crate::iggy_index::{IGGY_INDEX_SIZE, IggyIndexCache};
use crate::iggy_index_writer::IggyIndexWriter;
use crate::messages_writer::MessagesWriter;
use crate::poll_plan::SealedSegmentHandle;
use crate::segment::Segment;
use iggy_common::{IggyByteSize, IggyMessagesBatch};
use journal::{Journal, Storage};
use ringbuffer::AllocRingBuffer;
use server_common::{IggyMessagesBatchSetInFlight, SegmentStorage};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::rc::Rc;

const SEGMENTS_CAPACITY: usize = 1024;
const ACCESS_MAP_CAPACITY: usize = 8;
const SIZE_16MB: usize = 16 * 1024 * 1024;

/// Max sealed segments per partition that keep a resident read handle (fd +
/// sparse index). Without a cap every sealed segment a reader ever touched pins
/// one fd for the partition's lifetime; the server-wide budget is this cap times
/// the partition count, so keep it small. 12 covers a lagging consumer's working
/// set (the recent sealed segments it re-reads) with room for a few concurrent
/// readers before an LRU eviction forces a re-open.
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

impl<J, S> Journal<S> for JournalState<J>
where
    S: Storage,
    J: Journal<S>,
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
}

impl<J: Default> Default for JournalState<J> {
    fn default() -> Self {
        Self {
            inner: J::default(),
            info: JournalInfo::default(),
        }
    }
}

// TODO: Structure this better, the segmented log does not need to be generic over S, the Journal needs.

// This struct aliases in terms of the code contained the `SegmentedLog` from `core/server/src/streaming/partitions/log.rs`.
// The only difference is the `Journal` generic, we use different trait.
#[derive(Debug)]
pub struct SegmentedLog<J, S>
where
    S: Storage,
    J: Debug + Journal<S>,
{
    journal: JournalState<J>,
    _pd: std::marker::PhantomData<S>,
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
    in_flight: IggyMessagesBatchSetInFlight,
}

impl<J, S> Default for SegmentedLog<J, S>
where
    S: Storage,
    J: Debug + Default + Journal<S>,
{
    fn default() -> Self {
        Self {
            journal: JournalState::default(),
            _pd: std::marker::PhantomData,
            _access_map: AllocRingBuffer::with_capacity_power_of_2(ACCESS_MAP_CAPACITY),
            _cache: (),
            segments: Vec::with_capacity(SEGMENTS_CAPACITY),
            storage: Vec::with_capacity(SEGMENTS_CAPACITY),
            indexes: Vec::with_capacity(SEGMENTS_CAPACITY),
            messages_writers: Vec::with_capacity(SEGMENTS_CAPACITY),
            index_writers: Vec::with_capacity(SEGMENTS_CAPACITY),
            sealed_read_state: Vec::with_capacity(SEGMENTS_CAPACITY),
            sealed_lru: VecDeque::with_capacity(SEALED_READ_STATE_CAP + 1),
            in_flight: IggyMessagesBatchSetInFlight::default(),
        }
    }
}

impl<J, S> SegmentedLog<J, S>
where
    S: Storage,
    J: Debug + Journal<S>,
{
    pub const fn has_segments(&self) -> bool {
        !self.segments.is_empty()
    }

    pub const fn segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub const fn segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    /// Shared read-state handles, parallel to [`Self::segments`]. Cloned into
    /// the poll plan for sealed segments (see [`SealedSegmentHandle`]).
    pub const fn sealed_read_state(&self) -> &Vec<SealedSegmentHandle> {
        &self.sealed_read_state
    }

    /// Mutable read-state handles. `remove(0)` in lockstep with `segments` when
    /// a segment is retired so the pump drops its handle (freeing the fd +
    /// index once any in-flight poll holding a clone completes).
    pub const fn sealed_read_state_mut(&mut self) -> &mut Vec<SealedSegmentHandle> {
        &mut self.sealed_read_state
    }

    /// Record a sealed-segment access and enforce [`SEALED_READ_STATE_CAP`]
    /// (LRU). `start_offset` keys the segment - stable across retire, unlike the
    /// slot index. It moves to the most-recently-used front; once more than the
    /// cap distinct sealed segments are tracked, the least-recently-used one's
    /// handle is dropped (replaced with a fresh empty handle) so its fd + sparse
    /// index free. An in-flight poll holding a clone of the dropped handle keeps
    /// it alive until it finishes (see [`SealedSegmentHandle`]).
    pub fn touch_sealed_read_state(&mut self, start_offset: u64) {
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
            if let Some(slot) = self
                .segments
                .iter()
                .position(|segment| segment.start_offset == evicted)
            {
                self.sealed_read_state[slot] = SealedSegmentHandle::default();
            }
        }
    }

    pub const fn storages_mut(&mut self) -> &mut Vec<SegmentStorage> {
        &mut self.storage
    }

    pub const fn storages(&self) -> &Vec<SegmentStorage> {
        &self.storage
    }

    pub const fn messages_writers(&self) -> &Vec<Option<Rc<MessagesWriter>>> {
        &self.messages_writers
    }

    pub const fn messages_writers_mut(&mut self) -> &mut Vec<Option<Rc<MessagesWriter>>> {
        &mut self.messages_writers
    }

    pub const fn index_writers(&self) -> &Vec<Option<Rc<IggyIndexWriter>>> {
        &self.index_writers
    }

    pub const fn index_writers_mut(&mut self) -> &mut Vec<Option<Rc<IggyIndexWriter>>> {
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

    pub const fn indexes_mut(&mut self) -> &mut Vec<Option<IggyIndexCache>> {
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
            let capacity = SIZE_16MB / IGGY_INDEX_SIZE;
            *indexes = Some(IggyIndexCache::with_capacity(capacity));
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
    }

    pub fn set_segment_indexes(&mut self, segment_index: usize, indexes: IggyIndexCache) {
        if let Some(segment_indexes) = self.indexes.get_mut(segment_index) {
            *segment_indexes = Some(indexes);
        }
    }

    pub const fn in_flight(&self) -> &IggyMessagesBatchSetInFlight {
        &self.in_flight
    }

    pub const fn in_flight_mut(&mut self) -> &mut IggyMessagesBatchSetInFlight {
        &mut self.in_flight
    }

    pub fn set_in_flight(&mut self, batches: Vec<IggyMessagesBatch>) {
        self.in_flight.set(batches);
    }

    pub fn clear_in_flight(&mut self) {
        self.in_flight.clear();
    }
}

impl<J, S> SegmentedLog<J, S>
where
    S: Storage,
    J: Debug + Journal<S>,
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

    type TestLog =
        SegmentedLog<PartitionJournal<PartitionJournalMemStorage>, PartitionJournalMemStorage>;

    /// Push a sealed segment with a resident (index-filled) read handle and
    /// return a clone of that handle, standing in for an in-flight poll's clone.
    fn push_resident_sealed(log: &mut TestLog, start_offset: u64) -> SealedSegmentHandle {
        log.segments.push(Segment {
            start_offset,
            sealed: true,
            ..Segment::default()
        });
        log.sealed_read_state.push(SealedSegmentHandle::default());
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
        // Ascending touch order: offset 0 is the least-recently used.
        for offset in 0..=SEALED_READ_STATE_CAP as u64 {
            log.touch_sealed_read_state(offset);
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
        for offset in 0..SEALED_READ_STATE_CAP as u64 {
            log.touch_sealed_read_state(offset);
        }
        // Re-access offset 0 -> now most-recently used, so offset 1 becomes the
        // least-recently used and next to be evicted.
        log.touch_sealed_read_state(0);

        let new_offset = SEALED_READ_STATE_CAP as u64;
        handles.push(push_resident_sealed(&mut log, new_offset));
        log.touch_sealed_read_state(new_offset);

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
        for offset in 0..=SEALED_READ_STATE_CAP as u64 {
            log.touch_sealed_read_state(offset);
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
}
