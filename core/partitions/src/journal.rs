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

use iggy_common::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use journal::{Journal, Storage};
use std::cell::UnsafeCell;

// TODO: Fix that, we need to figure out how to store the `IggyMessagesBatchSet`.
/// No-op storage backend for the in-memory partition journal.
#[derive(Debug)]
pub struct Noop;

impl Storage for Noop {
    type Buffer = ();

    async fn write(&self, _buf: ()) -> usize {
        0
    }

    async fn read(&self, _offset: usize, buffer: ()) -> () {
        buffer
    }
}

/// Lookup key for querying messages from the journal.
#[derive(Debug, Clone, Copy)]
pub enum MessageLookup {
    Offset { offset: u64, count: u32 },
    Timestamp { timestamp: u64, count: u32 },
}

impl std::ops::Deref for MessageLookup {
    type Target = Self;

    fn deref(&self) -> &Self {
        self
    }
}

/// In-memory journal that accumulates message batches as an `IggyMessagesBatchSet`.
///
/// This is a pure storage layer — it holds batches and supports lookups via
/// `MessageLookup`. All tracking metadata (offsets, timestamps, counts) lives
/// outside the journal in the `SegmentedLog`'s `JournalInfo`.
///
/// Uses `UnsafeCell` for interior mutability, matching the single-threaded
/// per-shard execution model.
pub struct PartitionJournal {
    batch_set: UnsafeCell<IggyMessagesBatchSet>,
}

impl PartitionJournal {
    pub fn new() -> Self {
        Self {
            batch_set: UnsafeCell::new(IggyMessagesBatchSet::empty()),
        }
    }

    /// Drain all accumulated batches, returning the batch set.
    pub fn commit(&self) -> IggyMessagesBatchSet {
        let batch_set = unsafe { &mut *self.batch_set.get() };
        std::mem::take(batch_set)
    }

    pub fn is_empty(&self) -> bool {
        let batch_set = unsafe { &*self.batch_set.get() };
        batch_set.is_empty()
    }
}

impl Default for PartitionJournal {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for PartitionJournal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionJournal").finish()
    }
}

impl Journal<Noop> for PartitionJournal {
    type Header = MessageLookup;
    type Entry = IggyMessagesBatchMut;
    type HeaderRef<'a> = MessageLookup;

    fn header(&self, _idx: usize) -> Option<Self::HeaderRef<'_>> {
        unreachable!("fn header: header lookup not supported for partition journal.");
    }

    fn previous_header(&self, _header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        unreachable!("fn previous_header: header lookup not supported for partition journal.");
    }

    async fn append(&self, entry: Self::Entry) {
        let batch_set = unsafe { &mut *self.batch_set.get() };
        batch_set.add_batch(entry);
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        // Entry lookups go through SegmentedLog which uses JournalInfo
        // to construct MessageLookup headers. The actual query is done
        // via get() below, not through the Journal trait.
        let _ = header;
        unreachable!("fn entry: use SegmentedLog::get() instead for partition journal lookups.");
    }
}

impl PartitionJournal {
    /// Query messages by offset or timestamp with count.
    ///
    /// This is called by `SegmentedLog` using `MessageLookup` headers
    /// constructed from `JournalInfo`.
    pub fn get(&self, header: &MessageLookup) -> Option<IggyMessagesBatchSet> {
        let batch_set = unsafe { &*self.batch_set.get() };
        let result = match header {
            MessageLookup::Offset { offset, count } => batch_set.get_by_offset(*offset, *count),
            MessageLookup::Timestamp { timestamp, count } => {
                batch_set.get_by_timestamp(*timestamp, *count)
            }
        };
        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}
