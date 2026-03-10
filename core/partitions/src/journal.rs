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

use bytes::Bytes;
use iggy_common::{
    IggyMessagesBatchMut, IggyMessagesBatchSet,
    header::{Operation, PrepareHeader},
    message::Message,
};
use journal::{Journal, Storage};
use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap},
};

const ZERO_LEN: usize = 0;

/// Lookup key for querying messages from the journal.
#[derive(Debug, Clone, Copy)]
pub enum MessageLookup {
    #[allow(dead_code)]
    Offset { offset: u64, count: u32 },
    #[allow(dead_code)]
    Timestamp { timestamp: u64, count: u32 },
}

impl std::ops::Deref for MessageLookup {
    type Target = Self;

    fn deref(&self) -> &Self {
        self
    }
}

#[allow(dead_code)]
pub trait QueryableJournal<S>: Journal<S>
where
    S: Storage,
{
    type Query;

    fn get(&self, query: &Self::Query) -> impl Future<Output = Option<IggyMessagesBatchSet>>;
}

#[derive(Debug, Default)]
pub struct PartitionJournalMemStorage {
    entries: UnsafeCell<Vec<Bytes>>,
    op_to_index: UnsafeCell<HashMap<u64, usize>>,
}

impl Storage for PartitionJournalMemStorage {
    type Buffer = Bytes;

    async fn write(&self, buf: Self::Buffer) -> usize {
        let op = Message::<PrepareHeader>::from_bytes(buf.clone())
            .ok()
            .map(|message| message.header().op);

        let entries = unsafe { &mut *self.entries.get() };
        let index = entries.len();
        entries.push(buf.clone());

        if let Some(op) = op {
            let op_to_index = unsafe { &mut *self.op_to_index.get() };
            op_to_index.insert(op, index);
        }

        buf.len()
    }

    async fn read(&self, offset: usize, _len: usize) -> Self::Buffer {
        let op = offset as u64;
        let Some(index) = ({
            let op_to_index = unsafe { &*self.op_to_index.get() };
            op_to_index.get(&op).copied()
        }) else {
            return Bytes::new();
        };

        let entries = unsafe { &*self.entries.get() };
        entries.get(index).cloned().unwrap_or_default()
    }
}

pub struct PartitionJournal<S>
where
    S: Storage<Buffer = Bytes>,
{
    message_offset_to_op: UnsafeCell<BTreeMap<u64, u64>>,
    timestamp_to_op: UnsafeCell<BTreeMap<u64, u64>>,
    headers: UnsafeCell<Vec<PrepareHeader>>,
    inner: UnsafeCell<JournalInner<S>>,
}

impl<S> Default for PartitionJournal<S>
where
    S: Storage<Buffer = Bytes> + Default,
{
    fn default() -> Self {
        Self {
            message_offset_to_op: UnsafeCell::new(BTreeMap::new()),
            timestamp_to_op: UnsafeCell::new(BTreeMap::new()),
            headers: UnsafeCell::new(Vec::new()),
            inner: UnsafeCell::new(JournalInner {
                storage: S::default(),
            }),
        }
    }
}

impl<S> std::fmt::Debug for PartitionJournal<S>
where
    S: Storage<Buffer = Bytes>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionJournal2Impl").finish()
    }
}

struct JournalInner<S>
where
    S: Storage<Buffer = Bytes>,
{
    storage: S,
}

impl PartitionJournalMemStorage {
    fn drain(&self) -> Vec<Bytes> {
        let entries = unsafe { &mut *self.entries.get() };
        let drained = std::mem::take(entries);
        let op_to_index = unsafe { &mut *self.op_to_index.get() };
        op_to_index.clear();
        drained
    }

    fn is_empty(&self) -> bool {
        let entries = unsafe { &*self.entries.get() };
        entries.is_empty()
    }
}

impl PartitionJournal<PartitionJournalMemStorage> {
    /// Drain all accumulated batches, matching the legacy `PartitionJournal` API.
    pub fn commit(&self) -> IggyMessagesBatchSet {
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };

        let mut messages = Vec::with_capacity(entries.len());
        for bytes in entries {
            if let Ok(message) = Message::from_bytes(bytes) {
                messages.push(message);
            }
        }

        let headers = unsafe { &mut *self.headers.get() };
        headers.clear();
        let offsets = unsafe { &mut *self.message_offset_to_op.get() };
        offsets.clear();
        let timestamps = unsafe { &mut *self.timestamp_to_op.get() };
        timestamps.clear();

        Self::messages_to_batch_set(&messages)
    }

    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.storage.is_empty()
    }
}

impl<S> PartitionJournal<S>
where
    S: Storage<Buffer = Bytes>,
{
    fn message_to_batch(message: &Message<PrepareHeader>) -> Option<IggyMessagesBatchMut> {
        if message.header().operation != Operation::SendMessages {
            return None;
        }

        crate::decode_send_messages_batch(message.body_bytes())
    }

    fn messages_to_batch_set(messages: &[Message<PrepareHeader>]) -> IggyMessagesBatchSet {
        let mut batch_set = IggyMessagesBatchSet::empty();

        for message in messages {
            if let Some(batch) = Self::message_to_batch(message) {
                batch_set.add_batch(batch);
            }
        }

        batch_set
    }

    #[allow(dead_code)]
    fn candidate_start_op(&self, query: &MessageLookup) -> Option<u64> {
        match query {
            MessageLookup::Offset { offset, .. } => {
                let offsets = unsafe { &*self.message_offset_to_op.get() };
                offsets
                    .range(..=*offset)
                    .next_back()
                    .or_else(|| offsets.range(*offset..).next())
                    .map(|(_, op)| *op)
            }
            MessageLookup::Timestamp { timestamp, .. } => {
                let timestamps = unsafe { &*self.timestamp_to_op.get() };
                timestamps
                    .range(..=*timestamp)
                    .next_back()
                    .or_else(|| timestamps.range(*timestamp..).next())
                    .map(|(_, op)| *op)
            }
        }
    }

    async fn message_by_op(&self, op: u64) -> Option<Message<PrepareHeader>> {
        let offset = usize::try_from(op).ok()?;
        let bytes = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.read(offset, ZERO_LEN).await
        };

        Some(
            Message::from_bytes(bytes)
                .expect("partition.journal.storage.read: invalid bytes for message"),
        )
    }

    #[allow(dead_code)]
    async fn load_messages_from_storage(
        &self,
        start_op: u64,
        count: u32,
    ) -> Vec<Message<PrepareHeader>> {
        if count == 0 {
            return Vec::new();
        }

        let mut messages = Vec::new();
        let mut loaded_messages = 0u32;
        let mut op = start_op;

        while loaded_messages < count {
            let Some(message) = self.message_by_op(op).await else {
                break;
            };

            if let Some(batch) = Self::message_to_batch(&message) {
                loaded_messages = loaded_messages.saturating_add(batch.count());
                messages.push(message);
            }

            op += 1;
        }

        messages
    }
}

impl<S> Journal<S> for PartitionJournal<S>
where
    S: Storage<Buffer = Bytes>,
{
    type Header = PrepareHeader;
    type Entry = Message<Self::Header>;
    #[rustfmt::skip] // Scuffed formatter.
    type HeaderRef<'a> = &'a Self::Header where S: 'a;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = unsafe { &mut *self.headers.get() };
        headers.get(idx)
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        if header.op == 0 {
            return None;
        }

        let prev_op = header.op - 1;
        let headers = unsafe { &*self.headers.get() };
        headers.iter().find(|candidate| candidate.op == prev_op)
    }

    async fn append(&self, entry: Self::Entry) {
        let first_offset_and_timestamp = Self::message_to_batch(&entry)
            .and_then(|batch| Some((batch.first_offset()?, batch.first_timestamp()?)));

        let header = *entry.header();
        let op = header.op;

        {
            let headers = unsafe { &mut *self.headers.get() };
            headers.push(header);
        };

        let bytes = entry.into_inner();
        {
            let inner = unsafe { &*self.inner.get() };
            let _ = inner.storage.write(bytes).await;
        }

        if let Some((offset, timestamp)) = first_offset_and_timestamp {
            let offsets = unsafe { &mut *self.message_offset_to_op.get() };
            offsets.insert(offset, op);

            let timestamps = unsafe { &mut *self.timestamp_to_op.get() };
            timestamps.insert(timestamp, op);
        }
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        self.message_by_op(header.op).await
    }
}

impl<S> QueryableJournal<S> for PartitionJournal<S>
where
    S: Storage<Buffer = Bytes>,
{
    type Query = MessageLookup;

    async fn get(&self, query: &Self::Query) -> Option<IggyMessagesBatchSet> {
        let query = *query;
        let start_op = self.candidate_start_op(&query)?;
        let count = match query {
            MessageLookup::Offset { count, .. } | MessageLookup::Timestamp { count, .. } => count,
        };

        let messages = self.load_messages_from_storage(start_op, count).await;

        let batch_set = Self::messages_to_batch_set(&messages);
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
