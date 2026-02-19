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

use iggy_common::{
    INDEX_SIZE, IggyByteSize, IggyIndexesMut, IggyMessagesBatch, IggyMessagesBatchSetInFlight,
    Segment, SegmentStorage,
};
use journal::{Journal, Storage};
use ringbuffer::AllocRingBuffer;
use std::fmt::Debug;

const SEGMENTS_CAPACITY: usize = 1024;
const ACCESS_MAP_CAPACITY: usize = 8;
const SIZE_16MB: usize = 16 * 1024 * 1024;

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

    fn append(&self, entry: Self::Entry) -> impl Future<Output = ()> {
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
    indexes: Vec<Option<IggyIndexesMut>>,
    storage: Vec<SegmentStorage>,
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
            in_flight: IggyMessagesBatchSetInFlight::default(),
        }
    }
}

impl<J, S> SegmentedLog<J, S>
where
    S: Storage,
    J: Debug + Journal<S>,
{
    pub fn has_segments(&self) -> bool {
        !self.segments.is_empty()
    }

    pub fn segments(&self) -> &Vec<Segment> {
        &self.segments
    }

    pub fn segments_mut(&mut self) -> &mut Vec<Segment> {
        &mut self.segments
    }

    pub fn storages_mut(&mut self) -> &mut Vec<SegmentStorage> {
        &mut self.storage
    }

    pub fn storages(&self) -> &Vec<SegmentStorage> {
        &self.storage
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

    pub fn indexes(&self) -> &Vec<Option<IggyIndexesMut>> {
        &self.indexes
    }

    pub fn indexes_mut(&mut self) -> &mut Vec<Option<IggyIndexesMut>> {
        &mut self.indexes
    }

    pub fn active_indexes(&self) -> Option<&IggyIndexesMut> {
        self.indexes
            .last()
            .expect("active indexes called on empty log")
            .as_ref()
    }

    pub fn active_indexes_mut(&mut self) -> Option<&mut IggyIndexesMut> {
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
            let capacity = SIZE_16MB / INDEX_SIZE;
            *indexes = Some(IggyIndexesMut::with_capacity(capacity, 0));
        }
    }

    pub fn add_persisted_segment(&mut self, segment: Segment, storage: SegmentStorage) {
        self.segments.push(segment);
        self.storage.push(storage);
        self.indexes.push(None);
    }

    pub fn set_segment_indexes(&mut self, segment_index: usize, indexes: IggyIndexesMut) {
        if let Some(segment_indexes) = self.indexes.get_mut(segment_index) {
            *segment_indexes = Some(indexes);
        }
    }

    pub fn in_flight(&self) -> &IggyMessagesBatchSetInFlight {
        &self.in_flight
    }

    pub fn in_flight_mut(&mut self) -> &mut IggyMessagesBatchSetInFlight {
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
    pub fn journal_mut(&mut self) -> &mut JournalState<J> {
        &mut self.journal
    }

    pub fn journal(&self) -> &JournalState<J> {
        &self.journal
    }
}
