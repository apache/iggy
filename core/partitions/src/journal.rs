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

use bytes::BytesMut;
use iggy_common::{
    INDEX_SIZE, IggyIndexesMut, IggyMessagesBatchMut, IggyMessagesBatchSet, PooledBuffer,
    header::{Operation, PrepareHeader},
    message::Message,
};
use journal::{Journal, Storage};
use std::{cell::UnsafeCell, collections::HashMap};

// TODO: Fix that, we need to figure out how to store the `IggyMessagesBatchSet`.
/// No-op storage backend for the in-memory partition journal.
#[derive(Debug)]
pub struct Noop;

impl Storage for Noop {
    type Buffer = ();

    async fn write(&self, _buf: ()) -> usize {
        0
    }

    async fn read(&self, _offset: usize, _buffer: ()) -> () { ()}
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

// [LEGACY]
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

pub trait PartitionJournal2<S>: Journal<S>
where
    S: Storage,
{
    type Query;

    fn get(&self, query: &Self::Query) -> impl Future<Output = Option<IggyMessagesBatchSet>>;
}

pub struct PartitionJournal2Impl {
    message_offset_to_op: UnsafeCell<HashMap<u64, usize>>,
    timestamp_to_op: UnsafeCell<HashMap<u64, usize>>,
    inner: UnsafeCell<JournalInner>,
}

struct JournalInner {
    set: Vec<Message<PrepareHeader>>,
}

impl Default for PartitionJournal2Impl {
    fn default() -> Self {
        Self {
            message_offset_to_op: UnsafeCell::new(HashMap::new()),
            timestamp_to_op: UnsafeCell::new(HashMap::new()),
            inner: UnsafeCell::new(JournalInner { set: Vec::new() }),
        }
    }
}

impl PartitionJournal2Impl {
    fn decode_send_messages_batch(body: bytes::Bytes) -> Option<IggyMessagesBatchMut> {
        // TODO: This is bad, 
        let mut body = body
            .try_into_mut()
            .unwrap_or_else(|body| BytesMut::from(body.as_ref()));

        if body.len() < 4 {
            return None;
        }

        let count_bytes = body.split_to(4);
        let count = u32::from_le_bytes(count_bytes.as_ref().try_into().ok()?);
        let indexes_len = (count as usize).checked_mul(INDEX_SIZE)?;

        if body.len() < indexes_len {
            return None;
        }

        let indexes_bytes = body.split_to(indexes_len);
        let indexes = IggyIndexesMut::from_bytes(PooledBuffer::from(indexes_bytes), 0);
        let messages = PooledBuffer::from(body);

        Some(IggyMessagesBatchMut::from_indexes_and_messages(
            indexes, messages,
        ))
    }

    fn message_to_batch(message: &Message<PrepareHeader>) -> Option<IggyMessagesBatchMut> {
        if message.header().operation != Operation::SendMessages {
            return None;
        }

        Self::decode_send_messages_batch(message.body_bytes())
    }

    fn messages_to_batch_set<'a>(messages: impl Iterator<Item = &'a Message<PrepareHeader>>) -> IggyMessagesBatchSet {
        let mut batch_set = IggyMessagesBatchSet::empty();

        for message in messages {
            if let Some(batch) = Self::message_to_batch(message) {
                batch_set.add_batch(batch);
            }
        }

        batch_set
    }

    fn candidate_start_op(&self, query: &MessageLookup) -> usize {
        match query {
            MessageLookup::Offset { offset, .. } => {
                let offsets = unsafe { &*self.message_offset_to_op.get() };
                offsets.get(offset).copied().unwrap_or_default()
            }
            MessageLookup::Timestamp { timestamp, .. } => {
                let timestamps = unsafe { &*self.timestamp_to_op.get() };
                timestamps.get(timestamp).copied().unwrap_or_default()
            }
        }
    }

    fn messages_from_op(&self, start_op: usize) -> impl Iterator<Item = &Message<PrepareHeader>> {
        let inner = unsafe { &*self.inner.get() };
        inner.set.iter().skip(start_op)
    }
}

impl Journal<Noop> for PartitionJournal2Impl {
    type Header = PrepareHeader;
    type Entry = Message<Self::Header>;
    type HeaderRef<'a> = &'a Self::Header;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        // TODO: Fixes
        let inner = unsafe { &*self.inner.get() };
        inner.set.get(idx).map(|msg| msg.header())
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        // TODO: Fixes
        let prev_idx = header.op.saturating_sub(1) as usize;
        let inner = unsafe { &*self.inner.get() };
        inner.set.get(prev_idx).map(|msg| msg.header())
    }

    async fn append(&self, entry: Self::Entry) {
        let first_offset_and_timestamp = Self::message_to_batch(&entry)
            .and_then(|batch| Some((batch.first_offset()?, batch.first_timestamp()?)));

        let inner = unsafe { &mut *self.inner.get() };
        let op = inner.set.len();
        inner.set.push(entry);

        if let Some((offset, timestamp)) = first_offset_and_timestamp {
            let offsets = unsafe { &mut *self.message_offset_to_op.get() };
            offsets.insert(offset, op);

            let timestamps = unsafe { &mut *self.timestamp_to_op.get() };
            timestamps.insert(timestamp, op);
        }
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        let op = header.op as usize;
        let inner = unsafe { &*self.inner.get() };
        inner.set.get(op).cloned()
    }
}

impl PartitionJournal2<Noop> for PartitionJournal2Impl {
    type Query = MessageLookup;

    async fn get(&self, query: &Self::Query) -> Option<IggyMessagesBatchSet> {
        let query = *query;
            let start_op = self.candidate_start_op(&query);
            let messages = self.messages_from_op(start_op);
            let batch_set = Self::messages_to_batch_set(messages);

            let result = match query {
                MessageLookup::Offset { offset, count } => batch_set.get_by_offset(offset, count),
                MessageLookup::Timestamp { timestamp, count } => {
                    batch_set.get_by_timestamp(timestamp, count)
                }
            };

            if result.is_empty() {
                None
            } else {
                Some(result)
            }
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
