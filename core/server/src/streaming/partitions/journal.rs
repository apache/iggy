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

use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use iggy_common::{IggyByteSize, IggyError};
use std::fmt::Debug;

#[derive(Default, Debug)]
pub struct Inner {
    /// Base offset for the next journal epoch. After commit(), set to
    /// current_offset + 1. Used in `append()`: `current_offset = base_offset +
    /// messages_count - 1`.
    pub base_offset: u64,
    pub current_offset: u64,
    pub first_timestamp: u64,
    pub end_timestamp: u64,
    pub messages_count: u32,
    pub size: IggyByteSize,
}

#[derive(Debug)]
pub struct MemoryMessageJournal {
    batches: IggyMessagesBatchSet,
    inner: Inner,
}

impl MemoryMessageJournal {
    /// Create an empty journal for a fresh partition (no existing data).
    pub fn empty() -> Self {
        Self {
            batches: IggyMessagesBatchSet::default(),
            inner: Inner::default(),
        }
    }

    /// Create an empty journal positioned at the given offset. Used after
    /// bootstrap when the partition already has data on disk up to some offset.
    pub fn at_offset(base_offset: u64) -> Self {
        Self {
            batches: IggyMessagesBatchSet::default(),
            inner: Inner {
                base_offset,
                ..Default::default()
            },
        }
    }
}

impl Journal for MemoryMessageJournal {
    type Container = IggyMessagesBatchSet;
    type Entry = IggyMessagesBatchMut;
    type Inner = Inner;
    type AppendResult = Result<(u32, u32), IggyError>;

    fn append(&mut self, entry: Self::Entry) -> Self::AppendResult {
        let batch_messages_count = entry.count();
        tracing::trace!(
            "Coalescing batch with base_offset: {}, current_offset: {}, self.messages_count: {}, batch.count: {}",
            self.inner.base_offset,
            self.inner.current_offset,
            self.inner.messages_count,
            batch_messages_count
        );

        // Defense-in-depth: on first append after empty/default state, correct
        // base_offset from the batch's actual first offset. Mirrors the existing
        // first_timestamp initialization pattern below. Catches code paths that
        // create a journal without calling init().
        if self.inner.messages_count == 0
            && let Some(first_offset) = entry.first_offset()
        {
            // Allow disagreement when either side is 0 (fresh partition or
            // reset after purge). Only flag when both are non-zero and differ.
            debug_assert!(
                self.inner.base_offset == 0
                    || first_offset == 0
                    || self.inner.base_offset == first_offset,
                "journal base_offset ({}) disagrees with batch first_offset ({})",
                self.inner.base_offset,
                first_offset
            );
            self.inner.base_offset = first_offset;
        }

        let batch_size = entry.size();
        let first_timestamp = entry.first_timestamp().unwrap();
        let last_timestamp = entry.last_timestamp().unwrap();
        self.batches.add_batch(entry);

        if self.inner.first_timestamp == 0 {
            self.inner.first_timestamp = first_timestamp;
        }
        self.inner.end_timestamp = last_timestamp;
        self.inner.messages_count += batch_messages_count;
        self.inner.current_offset = self.inner.base_offset + self.inner.messages_count as u64 - 1;
        self.inner.size = IggyByteSize::from(self.inner.size.as_bytes_u64() + batch_size as u64);

        Ok((self.inner.messages_count, self.inner.size.as_bytes_u32()))
    }

    async fn flush(&self) -> Result<(), IggyError> {
        Ok(())
    }

    fn init(&mut self, inner: Self::Inner) {
        self.inner = inner
    }

    fn get<U>(&self, filter: impl FnOnce(&Self::Container) -> U) -> U {
        filter(&self.batches)
    }

    fn commit(&mut self) -> Self::Container {
        self.inner.base_offset = self.inner.current_offset + 1;
        self.inner.first_timestamp = 0;
        self.inner.end_timestamp = 0;
        self.inner.size = IggyByteSize::default();
        self.inner.messages_count = 0;
        std::mem::take(&mut self.batches)
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn first_offset(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            Some(self.inner.base_offset)
        }
    }

    fn last_offset(&self) -> Option<u64> {
        if self.is_empty() {
            None
        } else {
            Some(self.inner.current_offset)
        }
    }

    fn first_timestamp(&self) -> Option<u64> {
        if self.is_empty() || self.inner.first_timestamp == 0 {
            None
        } else {
            Some(self.inner.first_timestamp)
        }
    }

    fn last_timestamp(&self) -> Option<u64> {
        if self.is_empty() || self.inner.end_timestamp == 0 {
            None
        } else {
            Some(self.inner.end_timestamp)
        }
    }
}

pub trait Journal {
    type Container;
    type Entry;
    type Inner;
    type AppendResult;

    fn init(&mut self, inner: Self::Inner);

    fn append(&mut self, entry: Self::Entry) -> Self::AppendResult;

    fn get<U>(&self, filter: impl FnOnce(&Self::Container) -> U) -> U;

    fn commit(&mut self) -> Self::Container;

    fn is_empty(&self) -> bool;

    fn inner(&self) -> &Self::Inner;

    /// First offset of data in the journal, or None if empty.
    fn first_offset(&self) -> Option<u64>;

    /// Last offset of data in the journal, or None if empty.
    fn last_offset(&self) -> Option<u64>;

    /// Timestamp of first message in journal, or None if empty.
    fn first_timestamp(&self) -> Option<u64>;

    /// Timestamp of last message in journal, or None if empty.
    fn last_timestamp(&self) -> Option<u64>;

    // `flush` is only useful in case of an journal that has disk backed WAL.
    // This could be merged together with `append`, but not doing this for two reasons.
    // 1. In case of the `Journal` being used as part of structure that utilizes interior mutability, async with borrow_mut is not possible.
    // 2. Having it as separate function allows for more optimal usage patterns, e.g. batching multiple appends before flushing.
    fn flush(&self) -> impl Future<Output = Result<(), IggyError>>;
}
