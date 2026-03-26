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
    header::{Operation, PrepareHeader},
    send_messages2::{COMMAND_HEADER_SIZE, SendMessages2Ref, decode_prepare_slice},
};
use iobuf::{Frozen, Owned};
use journal::{Journal, Storage};
use std::{
    cell::UnsafeCell,
    collections::{BTreeMap, HashMap},
};

use crate::{Fragment, PollFragments, PollQueryResult};

const ZERO_LEN: usize = 0;
type JournalBuffer = Frozen<4096>;

/// Lookup key for querying messages from the journal.
#[derive(Debug, Clone, Copy)]
pub enum MessageLookup {
    #[allow(dead_code)]
    Offset { offset: u64, count: u32 },
    #[allow(dead_code)]
    Timestamp { timestamp: u64, count: u32 },
}

impl MessageLookup {
    const fn count(self) -> u32 {
        match self {
            Self::Offset { count, .. } | Self::Timestamp { count, .. } => count,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SelectedBatchSlice {
    start: usize,
    end: usize,
    matched_messages: u32,
    last_matching_offset: u64,
}

#[allow(dead_code)]
pub trait QueryableJournal<S>: Journal<S>
where
    S: Storage,
{
    type Query;

    fn get(&self, query: &Self::Query) -> impl Future<Output = Option<PollQueryResult<4096>>>;
}

#[derive(Debug, Default)]
pub struct PartitionJournalMemStorage {
    entries: UnsafeCell<Vec<JournalBuffer>>,
    /// Maps byte offset (as if disk-backed) to index in entries Vec
    offset_to_index: UnsafeCell<HashMap<usize, usize>>,
    /// Current write position (cumulative byte offset)
    current_offset: UnsafeCell<usize>,
}

impl Storage for PartitionJournalMemStorage {
    type Buffer = JournalBuffer;

    async fn write(&self, buf: Self::Buffer) -> usize {
        let len = buf.len();
        let entries = unsafe { &mut *self.entries.get() };
        let offset_to_index = unsafe { &mut *self.offset_to_index.get() };
        let current_offset = unsafe { &mut *self.current_offset.get() };

        let index = entries.len();
        offset_to_index.insert(*current_offset, index);
        entries.push(buf);

        let write_offset = *current_offset;
        *current_offset += len;

        write_offset
    }

    async fn read(&self, offset: usize, _len: usize) -> Self::Buffer {
        let offset_to_index = unsafe { &*self.offset_to_index.get() };
        let Some(&index) = offset_to_index.get(&offset) else {
            return Owned::<4096>::zeroed(0).into();
        };

        let entries = unsafe { &*self.entries.get() };
        entries
            .get(index)
            .cloned()
            .unwrap_or_else(|| Owned::<4096>::zeroed(0).into())
    }
}

pub struct PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    /// Maps op -> storage byte offset (for all entries)
    op_to_storage_offset: UnsafeCell<BTreeMap<u64, usize>>,
    /// Maps message offset -> op (for queryable entries)
    offset_to_op: UnsafeCell<BTreeMap<u64, u64>>,
    /// Maps timestamp -> op (for queryable entries)
    timestamp_to_op: UnsafeCell<BTreeMap<u64, u64>>,
    headers: UnsafeCell<Vec<PrepareHeader>>,
    inner: UnsafeCell<JournalInner<S>>,
}

impl<S> Default for PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer> + Default,
{
    fn default() -> Self {
        Self {
            op_to_storage_offset: UnsafeCell::new(BTreeMap::new()),
            offset_to_op: UnsafeCell::new(BTreeMap::new()),
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
    S: Storage<Buffer = JournalBuffer>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionJournal2Impl").finish()
    }
}

struct JournalInner<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    storage: S,
}

impl PartitionJournalMemStorage {
    fn drain(&self) -> Vec<JournalBuffer> {
        let entries = unsafe { &mut *self.entries.get() };
        let offset_to_index = unsafe { &mut *self.offset_to_index.get() };
        let current_offset = unsafe { &mut *self.current_offset.get() };

        offset_to_index.clear();
        *current_offset = 0;

        std::mem::take(entries)
    }

    fn is_empty(&self) -> bool {
        let entries = unsafe { &*self.entries.get() };
        entries.is_empty()
    }
}

impl PartitionJournal<PartitionJournalMemStorage> {
    /// Drain all accumulated batches, matching the legacy `PartitionJournal` API.
    pub fn commit(&self) -> Vec<JournalBuffer> {
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };

        let headers = unsafe { &mut *self.headers.get() };
        headers.clear();
        let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
        op_to_storage_offset.clear();
        let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
        offset_to_op.clear();
        let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
        timestamp_to_op.clear();

        entries
    }

    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.storage.is_empty()
    }
}

impl<S> PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    #[allow(dead_code)]
    fn candidate_start_op(&self, query: &MessageLookup) -> Option<u64> {
        match query {
            MessageLookup::Offset { offset, .. } => {
                let offset_to_op = unsafe { &*self.offset_to_op.get() };
                offset_to_op
                    .range(..=*offset)
                    .next_back()
                    .or_else(|| offset_to_op.range(*offset..).next())
                    .map(|(_, op)| *op)
            }
            MessageLookup::Timestamp { timestamp, .. } => {
                let timestamp_to_op = unsafe { &*self.timestamp_to_op.get() };
                timestamp_to_op
                    .range(..=*timestamp)
                    .next_back()
                    .or_else(|| timestamp_to_op.range(*timestamp..).next())
                    .map(|(_, op)| *op)
            }
        }
    }

    async fn bytes_by_op(&self, op: u64) -> Option<JournalBuffer> {
        let storage_offset = {
            let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
            *op_to_storage_offset.get(&op)?
        };

        let bytes = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.read(storage_offset, ZERO_LEN).await
        };

        if bytes.is_empty() {
            return None;
        }

        Some(bytes)
    }

    #[allow(dead_code)]
    async fn load_polled_batches_from_storage(
        &self,
        start_op: u64,
        query: MessageLookup,
    ) -> PollQueryResult<4096> {
        let count = query.count();

        if count == 0 {
            return (PollFragments::new(), None);
        }

        // Get (op, storage_offset) pairs directly from the mapping
        // BTreeMap is already sorted by op
        let op_offsets: Vec<(u64, usize)> = {
            let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
            op_to_storage_offset
                .range(start_op..)
                .map(|(op, offset)| (*op, *offset))
                .collect()
        };

        let mut fragments = PollFragments::new();
        let mut last_matching_offset = None;
        let mut matched_messages = 0u32;

        for (_, storage_offset) in op_offsets {
            if matched_messages >= count {
                break;
            }

            let bytes = {
                let inner = unsafe { &*self.inner.get() };
                inner.storage.read(storage_offset, ZERO_LEN).await
            };

            if bytes.is_empty() {
                continue;
            }

            let header_size = std::mem::size_of::<PrepareHeader>();
            let header_bytes = &bytes[..header_size];
            let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
                .expect("partition journal storage must contain a valid prepare header");
            if header.operation != Operation::SendMessages {
                continue;
            }
            let Ok(batch) = decode_prepare_slice(bytes.as_slice()) else {
                continue;
            };

            let Some(selection) = select_batch_slice(&batch, query, matched_messages) else {
                continue;
            };
            push_selected_batch_fragments(
                &mut fragments,
                &mut last_matching_offset,
                &mut matched_messages,
                &bytes,
                &header,
                &batch,
                selection,
            );
        }

        (fragments, last_matching_offset)
    }
}

impl<S> Journal<S> for PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    type Header = PrepareHeader;
    type Entry = JournalBuffer;
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
        let header_size = std::mem::size_of::<PrepareHeader>();
        let header_bytes = &entry[..header_size];
        let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
            .expect("partition journal append expects a valid prepare header");
        let op = header.op;
        let first_offset_and_timestamp = if header.operation == Operation::SendMessages {
            decode_prepare_slice(entry.as_slice())
                .ok()
                .and_then(|batch| {
                    batch.message_count().ok().and_then(|count| {
                        (count != 0)
                            .then_some((batch.header.base_offset, batch.header.base_timestamp))
                    })
                })
        } else {
            None
        };

        {
            let headers = unsafe { &mut *self.headers.get() };
            headers.push(header);
        };

        let storage_offset = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.write(entry).await
        };

        {
            let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
            op_to_storage_offset.insert(op, storage_offset);
        }

        if let Some((offset, timestamp)) = first_offset_and_timestamp {
            let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
            offset_to_op.insert(offset, op);

            let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
            timestamp_to_op.insert(timestamp, op);
        }
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        self.bytes_by_op(header.op).await
    }
}

impl<S> QueryableJournal<S> for PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    type Query = MessageLookup;

    async fn get(&self, query: &Self::Query) -> Option<PollQueryResult<4096>> {
        let query = *query;
        let start_op = self.candidate_start_op(&query)?;
        let result = self.load_polled_batches_from_storage(start_op, query).await;

        if result.0.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

fn select_batch_slice(
    batch: &SendMessages2Ref<'_>,
    query: MessageLookup,
    already_matched: u32,
) -> Option<SelectedBatchSlice> {
    let remaining = query.count().saturating_sub(already_matched);
    let batch_message_count = batch.message_count().ok()?;
    if remaining == 0 || batch_message_count == 0 {
        return None;
    }

    if matches!(query, MessageLookup::Timestamp { timestamp, .. } if batch.header.base_timestamp < timestamp)
    {
        return None;
    }

    let mut start = None;
    let mut end = 0usize;
    let mut matched = 0u32;
    let mut last_matching_offset = None;

    for record in batch.iter_with_offsets() {
        let offset = batch.header.base_offset + u64::from(record.message.header.offset_delta);

        let selected = match query {
            MessageLookup::Offset {
                offset: query_offset,
                ..
            } => offset >= query_offset,
            MessageLookup::Timestamp { .. } => true,
        };
        if !selected {
            continue;
        }

        start.get_or_insert(record.start);
        end = record.end;
        matched += 1;
        last_matching_offset = Some(offset);

        if matched == remaining {
            break;
        }
    }

    Some(SelectedBatchSlice {
        start: start?,
        end,
        matched_messages: matched,
        last_matching_offset: last_matching_offset?,
    })
}

fn push_selected_batch_fragments(
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
    matched_messages: &mut u32,
    prepare: &Frozen<4096>,
    prepare_header: &PrepareHeader,
    batch: &SendMessages2Ref<'_>,
    selection: SelectedBatchSlice,
) {
    let prepare_header_size = std::mem::size_of::<PrepareHeader>();
    let prepare_size = prepare_header.size as usize;
    let full_body_selected = selection.start == 0 && selection.end == batch.blob().len();

    if full_body_selected {
        fragments.push(Fragment::slice(
            prepare.clone(),
            prepare_header_size,
            prepare_size,
        ));
    } else {
        let mut rewritten = batch.header;
        rewritten.batch_length =
            u64::try_from(COMMAND_HEADER_SIZE + (selection.end - selection.start))
                .expect("sliced batch length exceeds u64::MAX");
        fragments.push(Fragment::whole(rewritten.into_frozen()));
        fragments.push(Fragment::slice(
            prepare.clone(),
            prepare_header_size + COMMAND_HEADER_SIZE + selection.start,
            prepare_header_size + COMMAND_HEADER_SIZE + selection.end,
        ));
    }

    *last_matching_offset = Some(selection.last_matching_offset);
    *matched_messages += selection.matched_messages;
}
