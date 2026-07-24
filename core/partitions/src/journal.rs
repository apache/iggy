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

use iggy_binary_protocol::{Operation, PrepareHeader};
use journal::{Journal, Storage};
use server_common::{
    iobuf::{Frozen, Owned},
    send_messages2::{
        COMMAND_HEADER_SIZE, SendMessages2Ref, decode_prepare_slice, decode_prepare_slice_trusted,
    },
};
use std::io;
use std::{
    cell::{Cell, UnsafeCell},
    collections::{BTreeMap, HashMap, VecDeque},
};
use tracing::warn;

use crate::{Fragment, PollFragments, PollQueryResult};

const ZERO_LEN: usize = 0;
const PREPARE_HEADER_SIZE: usize = std::mem::size_of::<PrepareHeader>();
type JournalBuffer = Frozen<4096>;

/// Decoded `SendMessages` header fields surfaced from a journal (re-)append so a
/// caller can fold segment accounting without a second decode of the same bytes.
/// Raw header values only: the journal stays agnostic of partition-layer
/// accounting types (`JournalInfo` lives in the log layer). `None` is surfaced
/// for non-`SendMessages` ops, which carry no segment bytes.
#[derive(Clone, Copy)]
pub struct RetainedBatchMeta {
    pub base_offset: u64,
    pub base_timestamp: u64,
    pub total_size: u64,
    pub message_count: u32,
}

/// Lookup key for querying messages from the journal.
///
/// `ceiling` is the inclusive commit-frontier bound: the resident journal holds
/// replicated-but-uncommitted prepares (a pipeline ahead of the commit
/// frontier), so a poll must never return a message past `ceiling` or it leaks
/// a dirty read of view-change-rollbackable data.
#[derive(Debug, Clone, Copy)]
pub enum MessageLookup {
    Offset {
        offset: u64,
        count: u32,
        ceiling: u64,
    },
    Timestamp {
        timestamp: u64,
        count: u32,
        ceiling: u64,
    },
}

impl MessageLookup {
    pub const fn count(self) -> u32 {
        match self {
            Self::Offset { count, .. } | Self::Timestamp { count, .. } => count,
        }
    }

    /// Inclusive commit-frontier upper bound: no message with a greater offset
    /// may be served (uncommitted, rollbackable on a view change).
    pub const fn ceiling(self) -> u64 {
        match self {
            Self::Offset { ceiling, .. } | Self::Timestamp { ceiling, .. } => ceiling,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SelectedBatchSlice {
    pub start: usize,
    pub end: usize,
    pub matched_messages: u32,
    pub last_matching_offset: u64,
}

/// In-memory only partition journal storage. Non-durable.
///
/// # Warning — development storage only
///
/// This storage backs the `Journal` trait with a plain `Vec<JournalBuffer>`
/// inside an `UnsafeCell`. Writes never hit disk, nothing is `fsync`ed, and
/// every entry is lost on process exit.
///
/// That property breaks VSR invariants in two visible ways once a cluster
/// is running real workloads:
///
/// - `VsrAction::RetransmitPrepares` (see `shard::IggyShard::apply_actions`)
///   reads from this journal. After a node restart the journal is empty, so
///   the retransmit is a silent no-op and peers waiting on the missing ops
///   stall until a view change kicks in.
/// - A restarting replica that rejoins the cluster cannot replay its WAL
///   to catch up; it looks to peers like a pristine empty node claiming
///   the replica slot.
///
/// These are safe for single-process tests, the simulator, and local dev
/// workloads. They are NOT safe for any multi-process or restart-sensitive
/// deployment. Use a disk-backed `Storage` implementation before serving
/// production cluster traffic.
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

    async fn write_at(&self, _offset: usize, buf: Self::Buffer) -> io::Result<usize> {
        let len = buf.len();
        let entries = unsafe { &mut *self.entries.get() };
        let offset_to_index = unsafe { &mut *self.offset_to_index.get() };
        let current_offset = unsafe { &mut *self.current_offset.get() };

        let index = entries.len();
        offset_to_index.insert(*current_offset, index);
        entries.push(buf);
        *current_offset += len;

        Ok(len)
    }

    async fn read_at(&self, offset: usize, _buffer: Self::Buffer) -> io::Result<Self::Buffer> {
        let offset_to_index = unsafe { &*self.offset_to_index.get() };
        let Some(&index) = offset_to_index.get(&offset) else {
            return Ok(Owned::<4096>::zeroed(0).into());
        };

        let entries = unsafe { &*self.entries.get() };
        Ok(entries
            .get(index)
            .cloned()
            .unwrap_or_else(|| Owned::<4096>::zeroed(0).into()))
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
    /// Maps `(origin_timestamp, op)` -> op (for queryable entries).
    ///
    /// Keeping `op` in the key preserves duplicate timestamps while still
    /// letting us seek to the closest batch for timestamp-based polling.
    timestamp_to_op: UnsafeCell<BTreeMap<(u64, u64), u64>>,
    headers: UnsafeCell<Vec<PrepareHeader>>,
    inner: UnsafeCell<JournalInner<S>>,
    /// Ring of recently evicted committed entries, keyed by op, retained so
    /// this replica can serve journal repair for rejoin windows after the
    /// entries left the resident journal at flush. Bounded by
    /// [`EVICTED_RING_CAPACITY`]; requests older than the ring answer
    /// `RangeEvicted` honestly.
    evicted_ring: UnsafeCell<VecDeque<(u64, JournalBuffer)>>,
    /// Running byte total of the buffers held by `evicted_ring`.
    evicted_ring_bytes: Cell<u64>,
    /// Single-replica groups have nobody to repair; retaining evicted
    /// entries for them is pure memory waste.
    repair_retention: Cell<bool>,
}

/// How many evicted entries each partition retains for repair. Sized to
/// cover a few seconds of traffic around a node restart; anything older is
/// bulk-sync (phase 3) territory.
pub const EVICTED_RING_CAPACITY: usize = 4096;

/// Byte ceiling for the evicted ring: the entry cap alone lets each
/// partition pin up to 4096 full-sized batches, which is unbounded in byte
/// terms across many partitions. Whichever cap trips first evicts.
pub const EVICTED_RING_BYTES_MAX: u64 = 16 * 1024 * 1024;

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
            evicted_ring: UnsafeCell::new(VecDeque::new()),
            evicted_ring_bytes: Cell::new(0),
            repair_retention: Cell::new(true),
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
    /// Synchronous mirror of [`Storage::read_at`] for the poll path. Mem
    /// storage never hits the reactor (it copies from an in-memory `Vec`), so
    /// the read can run under a partition borrow without crossing an `.await`
    /// - the property that keeps poll-read sound.
    fn read_at_sync(&self, offset: usize) -> JournalBuffer {
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

    fn entries(&self) -> Vec<JournalBuffer> {
        let entries = unsafe { &*self.entries.get() };
        entries.clone()
    }

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

    fn current_offset(&self) -> usize {
        let current_offset = unsafe { &*self.current_offset.get() };
        *current_offset
    }
}

impl PartitionJournal<PartitionJournalMemStorage> {
    /// Entry bytes for `op`, from the resident journal or the evicted ring.
    /// `None` when the op predates the ring (bulk-sync territory) or was
    /// never journaled here.
    /// Disable repair retention (single-replica groups: nobody to repair).
    pub fn set_repair_retention(&self, enabled: bool) {
        self.repair_retention.set(enabled);
        if !enabled {
            let ring = unsafe { &mut *self.evicted_ring.get() };
            ring.clear();
            self.evicted_ring_bytes.set(0);
        }
    }

    /// Resident (un-evicted) entry count; diagnostics only.
    pub fn resident_count(&self) -> usize {
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        op_to_storage_offset.len()
    }

    pub fn repair_entry(&self, op: u64) -> Option<JournalBuffer> {
        {
            let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
            if let Some(&storage_offset) = op_to_storage_offset.get(&op) {
                let inner = unsafe { &*self.inner.get() };
                return Some(inner.storage.read_at_sync(storage_offset));
            }
        }
        let ring = unsafe { &*self.evicted_ring.get() };
        ring.iter()
            .find(|(ring_op, _)| *ring_op == op)
            .map(|(_, entry)| entry.clone())
    }

    /// Oldest op this journal can still serve for repair (ring front, else
    /// resident head), or `None` when it holds nothing at all.
    pub fn repair_retained_from(&self) -> Option<u64> {
        {
            let ring = unsafe { &*self.evicted_ring.get() };
            if let Some((op, _)) = ring.front() {
                return Some(*op);
            }
        }
        let headers = unsafe { &*self.headers.get() };
        headers.first().map(|header| header.op)
    }

    /// Synchronous resident-range poll read. Never awaits (mem storage reads
    /// are pure memory copies), so a partition borrow held across it cannot span
    /// a scheduler yield. The poll path uses this; the disk tier, which does
    /// await file IO, runs off the borrow on owned descriptors.
    pub fn get_sync(&self, query: &MessageLookup) -> Option<PollQueryResult<4096>> {
        let query = *query;
        let start_op = self.candidate_start_op(&query)?;
        let result = self.load_polled_batches_from_storage_sync(start_op, query);
        (!result.0.is_empty()).then_some(result)
    }

    fn load_polled_batches_from_storage_sync(
        &self,
        start_op: u64,
        query: MessageLookup,
    ) -> PollQueryResult<4096> {
        let count = query.count();
        if count == 0 {
            return (PollFragments::new(), None);
        }

        // Disjoint `UnsafeCell`s: this borrows `op_to_storage_offset` while the
        // loop borrows `inner.storage` (via `read_at_sync`); the loop mutates
        // neither, so iterating the range in place avoids a per-poll Vec copy.
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };

        let mut fragments = PollFragments::new();
        let mut last_matching_offset = None;
        let mut matched_messages = 0u32;

        for (_, &storage_offset) in op_to_storage_offset.range(start_op..) {
            if matched_messages >= count {
                break;
            }

            let bytes = {
                let inner = unsafe { &*self.inner.get() };
                inner.storage.read_at_sync(storage_offset)
            };

            try_push_resident_entry(
                &bytes,
                query,
                &mut fragments,
                &mut last_matching_offset,
                &mut matched_messages,
            );
        }

        (fragments, last_matching_offset)
    }

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

    /// Entries forming the contiguous committed op-run from the front of the
    /// journal up to and including `commit_max`, WITHOUT evicting them.
    ///
    /// A backup journals replicated prepares up to a full pipeline ahead of the
    /// commit frontier. Only this gapless prefix may be flushed to a segment;
    /// persisting the uncommitted tail would write per-replica-timing bytes to
    /// disk (cross-replica divergence) and drop the headers those ops need when
    /// their own commit later lands (`commit_min` wedge). Stopping at the first
    /// gap keeps a post-gap op (even one `<= commit_max`) resident until its
    /// predecessor lands, so nothing is persisted ahead of a replication hole.
    /// Entries are append-ordered, op-ascending on a backup, so the prefix is
    /// the front. Read-only: the caller evicts via `evict_prefix` only once the
    /// bytes are durable, so a persist failure leaves the prefix recoverable.
    pub fn committed_prefix(&self, commit_max: u64) -> Vec<JournalBuffer> {
        let headers = unsafe { &*self.headers.get() };
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.entries()
        };
        let mut committed = Vec::new();
        let mut expected: Option<u64> = None;
        for (header, entry) in headers.iter().zip(entries) {
            let contiguous = expected.is_none_or(|next| header.op == next);
            if header.op > commit_max || !contiguous {
                break;
            }
            expected = Some(header.op + 1);
            committed.push(entry);
        }
        committed
    }

    /// Evict the first `count` entries (the committed prefix just read via
    /// `committed_prefix`) and keep the rest resident with the op / offset /
    /// timestamp indexes rebuilt for the compacted layout. Returns each retained
    /// entry paired with its `RetainedBatchMeta`, surfaced from the re-append
    /// decode, so the caller folds its accounting without decoding the tail a
    /// second time. Re-appending replays the original bytes, valid when first
    /// appended, so it cannot fail. Call only after the evicted bytes are
    /// durable: on a persist failure the prefix must stay resident for recovery.
    pub async fn evict_prefix(
        &self,
        count: usize,
    ) -> Vec<(JournalBuffer, Option<RetainedBatchMeta>)> {
        let all_entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };
        // Ops are positional against `headers` until the clear below; capture
        // the evicted prefix's ops first so the ring stays op-addressable.
        let evicted_ops: Vec<u64> = {
            let headers = unsafe { &*self.headers.get() };
            headers.iter().take(count).map(|header| header.op).collect()
        };

        {
            let headers = unsafe { &mut *self.headers.get() };
            headers.clear();
            let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
            op_to_storage_offset.clear();
            let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
            offset_to_op.clear();
            let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
            timestamp_to_op.clear();
        }

        let mut all_entries = all_entries.into_iter();
        if self.repair_retention.get() {
            let ring = unsafe { &mut *self.evicted_ring.get() };
            let mut ring_bytes = self.evicted_ring_bytes.get();
            for op in evicted_ops {
                let Some(entry) = all_entries.next() else {
                    break;
                };
                ring_bytes += entry.len() as u64;
                ring.push_back((op, entry));
                while ring.len() > EVICTED_RING_CAPACITY
                    || (ring_bytes > EVICTED_RING_BYTES_MAX && ring.len() > 1)
                {
                    if let Some((_, dropped)) = ring.pop_front() {
                        ring_bytes -= dropped.len() as u64;
                    }
                }
            }
            self.evicted_ring_bytes.set(ring_bytes);
        } else {
            // Consume without retaining: the iterator itself must still
            // advance past the evicted prefix so the retained tail below is
            // aligned.
            for _ in &evicted_ops {
                if all_entries.next().is_none() {
                    break;
                }
            }
        }
        let retained: Vec<JournalBuffer> = all_entries.collect();
        let mut result = Vec::with_capacity(retained.len());
        for entry in retained {
            let meta = self
                .append_with_meta(entry.clone())
                .await
                .expect("re-appending a retained journal entry must not fail");
            result.push((entry, meta));
        }

        result
    }

    /// `append`, additionally returning the decoded `RetainedBatchMeta` for a
    /// `SendMessages` entry so the eviction path folds its accounting without a
    /// second decode of the same bytes.
    ///
    /// INVARIANT (length-lock): the header is pushed before `storage.write_at`,
    /// so `headers[i]` and the entry at storage index `i` stay positionally
    /// paired - `committed_prefix`'s zip relies on that. `MemStorage::write_at`
    /// is infallible, so the push never runs ahead of a failed write. A future
    /// fallible `Storage` MUST roll the header push back on a write error (or
    /// write before pushing the header) or the zip desyncs.
    async fn append_with_meta(
        &self,
        entry: JournalBuffer,
    ) -> io::Result<Option<RetainedBatchMeta>> {
        let header_bytes = &entry[..PREPARE_HEADER_SIZE];
        let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
            .expect("partition journal append expects a valid prepare header");
        let op = header.op;
        // One decode feeds both the offset/timestamp index (keyed on
        // `origin_timestamp`) and the surfaced accounting meta (`base_timestamp`,
        // size, count); the two timestamps are distinct fields, do not conflate.
        // Trusted (no batch-hash): every entry reaching append was just stamped
        // by `stamp_prepare_for_persistence` (its checksum recomputed over this
        // exact blob) or re-appended from an already-validated resident entry,
        // so re-hashing the ~1 MiB blob here only to read the header is waste.
        let (index_offset_timestamp, meta) = if header.operation == Operation::SendMessages {
            match decode_prepare_slice_trusted(entry.as_slice()) {
                Ok(batch) if batch.message_count() != 0 => {
                    let message_count = batch.message_count();
                    let meta = RetainedBatchMeta {
                        base_offset: batch.header.base_offset,
                        base_timestamp: batch.header.base_timestamp,
                        total_size: batch.header.total_size() as u64,
                        message_count,
                    };
                    (
                        Some((batch.header.base_offset, batch.header.origin_timestamp)),
                        Some(meta),
                    )
                }
                _ => (None, None),
            }
        } else {
            (None, None)
        };

        {
            let headers = unsafe { &mut *self.headers.get() };
            headers.push(header);
        };

        let storage_offset = {
            let inner = unsafe { &*self.inner.get() };
            let storage_offset = inner.storage.current_offset();
            inner.storage.write_at(storage_offset, entry).await?;
            storage_offset
        };

        {
            let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
            op_to_storage_offset.insert(op, storage_offset);
        }

        if let Some((offset, timestamp)) = index_offset_timestamp {
            let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
            offset_to_op.insert(offset, op);

            let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
            timestamp_to_op.insert((timestamp, op), op);
        }

        Ok(meta)
    }

    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.storage.is_empty()
    }

    /// Owned, op-ascending clones of every resident journal entry. Each clone
    /// is a `Frozen` refcount bump, not a deep copy. Used to snapshot the
    /// resident tail at poll-plan time so a disk-tier straddle can be spliced
    /// off the partition borrow on owned data ([`crate::iggy_partition`]).
    pub fn resident_entries(&self) -> Vec<JournalBuffer> {
        let inner = unsafe { &*self.inner.get() };
        inner.storage.entries()
    }
}

impl<S> PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer>,
{
    #[must_use]
    pub const fn with_storage(storage: S) -> Self {
        Self {
            op_to_storage_offset: UnsafeCell::new(BTreeMap::new()),
            offset_to_op: UnsafeCell::new(BTreeMap::new()),
            timestamp_to_op: UnsafeCell::new(BTreeMap::new()),
            headers: UnsafeCell::new(Vec::new()),
            inner: UnsafeCell::new(JournalInner { storage }),
            evicted_ring: UnsafeCell::new(VecDeque::new()),
            evicted_ring_bytes: Cell::new(0),
            repair_retention: Cell::new(true),
        }
    }

    pub fn header_by_op(&self, op: u64) -> Option<PrepareHeader> {
        let headers = unsafe { &*self.headers.get() };
        headers.iter().find(|header| header.op == op).copied()
    }

    /// Headers for the contiguous op run `from_op ..= commit_max`, in op order,
    /// stopping at the first missing op. A replication gap must not be skipped:
    /// the caller advances `commit_min` strictly by one, so a hole would break
    /// that contract. Headers are append-ordered, which is op-ascending on a
    /// backup, so this is a single linear scan: drop ops below `from_op`, take
    /// while contiguous, stop at the first gap or past `commit_max`.
    pub fn committed_headers_from(&self, from_op: u64, commit_max: u64) -> Vec<PrepareHeader> {
        // Walk by OP, not by append position: after a rejoin the journal
        // interleaves live tail ops (which arrive while repair is still
        // streaming) with repaired window ops, so append order is no longer
        // op-ascending and a positional sequential scan would break at the
        // first interleave boundary forever.
        let mut result = Vec::new();
        let mut op = from_op;
        while op <= commit_max {
            let Some(header) = self.header_by_op(op) else {
                break;
            };
            result.push(header);
            op += 1;
        }
        result
    }

    /// Oldest message offset still resident in the in-memory journal, if
    /// any. Polls below this must fall back to the on-disk segments.
    pub fn oldest_resident_offset(&self) -> Option<u64> {
        let offset_to_op = unsafe { &*self.offset_to_op.get() };
        offset_to_op.keys().next().copied()
    }

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
                let next_at_or_after = timestamp_to_op
                    .range((*timestamp, 0)..)
                    .next()
                    .map(|(key, op)| (*key, *op));

                if let Some(((candidate_timestamp, _), op)) = next_at_or_after
                    && candidate_timestamp == *timestamp
                {
                    return Some(op);
                }

                timestamp_to_op
                    .range(..(*timestamp, 0))
                    .next_back()
                    .map(|(_, op)| *op)
                    .or_else(|| next_at_or_after.map(|(_, op)| op))
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
            inner
                .storage
                .read_at(storage_offset, Owned::<4096>::zeroed(ZERO_LEN).into())
                .await
                .unwrap_or_else(|_| Owned::<4096>::zeroed(ZERO_LEN).into())
        };

        if bytes.is_empty() {
            return None;
        }

        Some(bytes)
    }
}

impl Journal<PartitionJournalMemStorage> for PartitionJournal<PartitionJournalMemStorage> {
    type Header = PrepareHeader;
    type Entry = JournalBuffer;
    #[rustfmt::skip]
    type HeaderRef<'a> = &'a Self::Header;

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

    async fn append(&self, entry: Self::Entry) -> io::Result<()> {
        self.append_with_meta(entry).await.map(|_| ())
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        self.bytes_by_op(header.op).await
    }
}

pub fn select_batch_slice(
    batch: &SendMessages2Ref<'_>,
    query: MessageLookup,
    already_matched: u32,
) -> Option<SelectedBatchSlice> {
    let remaining = query.count().saturating_sub(already_matched);
    let batch_message_count = batch.message_count();
    if remaining == 0 || batch_message_count == 0 {
        return None;
    }

    let mut start = None;
    let mut end = 0usize;
    let mut matched = 0u32;
    let mut last_matching_offset = None;

    let ceiling = query.ceiling();
    for record in batch.iter_with_offsets() {
        let offset = batch.header.base_offset + u64::from(record.message.header.offset_delta);

        // Offsets within a batch ascend with the record index, so once we pass
        // the commit frontier every later record is uncommitted too: stop here
        // rather than skipping, which would punch a hole into the byte slice.
        if offset > ceiling {
            break;
        }

        let selected = match query {
            MessageLookup::Offset {
                offset: query_offset,
                ..
            } => offset >= query_offset,
            MessageLookup::Timestamp { timestamp, .. } => {
                batch.header.origin_timestamp + u64::from(record.message.header.timestamp_delta)
                    >= timestamp
            }
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

/// Push the fragments for one selected batch, shared by the resident-journal
/// walk and the disk-chunk walk. `source` holds a stamped
/// `[256B SendMessages2Header][blob]` batch starting at byte `batch_base`
/// (the disk walk passes the chunk cursor; the resident walk passes
/// `size_of::<PrepareHeader>()`, the batch's offset past the prepare header).
/// A full-body selection forwards the original batch bytes by reference; a
/// partial selection emits a rewritten header (clamped length/count/checksum)
/// plus a body slice.
pub fn push_selected_batch_fragments(
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
    matched_messages: &mut u32,
    source: &Frozen<4096>,
    batch_base: usize,
    batch: &SendMessages2Ref<'_>,
    selection: SelectedBatchSlice,
) {
    let full_body_selected = selection.start == 0 && selection.end == batch.blob().len();

    if full_body_selected {
        fragments.push(Fragment::slice(
            source.clone(),
            batch_base,
            batch_base + batch.header.total_size(),
        ));
    } else {
        let mut rewritten = batch.header;
        rewritten.batch_length =
            u64::try_from(COMMAND_HEADER_SIZE + (selection.end - selection.start))
                .expect("sliced batch length exceeds u64::MAX");
        rewritten.message_count = selection.matched_messages;
        rewritten.batch_checksum = rewritten.checksum_for_blob(
            batch
                .blob()
                .get(selection.start..selection.end)
                .expect("selected batch slice must stay within blob bounds"),
        );
        fragments.push(Fragment::whole(rewritten.into_frozen()));
        fragments.push(Fragment::slice(
            source.clone(),
            batch_base + COMMAND_HEADER_SIZE + selection.start,
            batch_base + COMMAND_HEADER_SIZE + selection.end,
        ));
    }

    *last_matching_offset = Some(selection.last_matching_offset);
    *matched_messages += selection.matched_messages;
}

/// Decode one resident `Frozen` entry and push its matching fragments. Shared by
/// the live storage walk and the owned-snapshot walk so the corrupt-header skip
/// and `SendMessages` filter live in one place. Skips (never panics) on a short
/// or undecodable entry: a poll must not crash the shard on bad storage.
fn try_push_resident_entry(
    prepare: &Frozen<4096>,
    query: MessageLookup,
    fragments: &mut PollFragments<4096>,
    last_matching_offset: &mut Option<u64>,
    matched_messages: &mut u32,
) {
    let Some(header_bytes) = prepare.as_slice().get(..PREPARE_HEADER_SIZE) else {
        return;
    };
    let Ok(header) = bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes) else {
        warn!(
            target: "iggy.partitions.diag",
            "partition journal poll: skipping entry with undecodable prepare header"
        );
        return;
    };
    if header.operation != Operation::SendMessages {
        return;
    }
    let Ok(batch) = decode_prepare_slice(prepare.as_slice()) else {
        return;
    };
    let Some(selection) = select_batch_slice(&batch, query, *matched_messages) else {
        return;
    };
    // The batch's 256B header sits right after the prepare header in a resident
    // entry (see `decode_prepare_slice`), so the batch base is `PREPARE_HEADER_SIZE`.
    push_selected_batch_fragments(
        fragments,
        last_matching_offset,
        matched_messages,
        prepare,
        PREPARE_HEADER_SIZE,
        &batch,
        selection,
    );
}

/// Poll an owned, point-in-time snapshot of the resident journal tail.
/// `entries` are op-ascending `Frozen` clones captured while the partition
/// borrow was held; this runs off the borrow on owned data, so no concurrent
/// commit/eviction can interleave. Mirrors [`PartitionJournal::get_sync`] but
/// over owned entries: a single forward walk where `select_batch_slice` filters
/// by `query`, which is equivalent to the live `candidate_start_op` seek (a
/// batch entirely before the query bound contributes no records).
///
/// Used both for retention-recovery (disk walked clean, serve the journal with
/// the original query) and, after a contiguity check by the caller, for the
/// disk-tier straddle continuation. Returns `None` when nothing matched.
//
// Plain `pub` (not `pub(crate)`): the `journal` module is private, so this is
// not externally reachable, and `pub(crate)` here trips `redundant_pub_crate`.
// Matches `select_batch_slice` above.
pub fn select_resident(
    entries: &[Frozen<4096>],
    query: MessageLookup,
) -> Option<PollQueryResult<4096>> {
    let count = query.count();
    if count == 0 {
        return None;
    }

    let mut fragments = PollFragments::new();
    let mut last_matching_offset = None;
    let mut matched_messages = 0u32;

    for prepare in entries {
        if matched_messages >= count {
            break;
        }
        try_push_resident_entry(
            prepare,
            query,
            &mut fragments,
            &mut last_matching_offset,
            &mut matched_messages,
        );
    }

    (!fragments.is_empty()).then_some((fragments, last_matching_offset))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, HEADER_SIZE};
    use journal::Journal;
    use server_common::Message;

    fn build_prepare(op: u64, size: usize) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(size).transmute_header(|_, h: &mut PrepareHeader| {
            h.command = Command2::Prepare;
            h.op = op;
            h.size = u32::try_from(size).expect("size fits in u32");
        })
    }

    #[compio::test]
    async fn entry_round_trips_bytes_for_retransmit() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();

        let payload_size = HEADER_SIZE + 64;
        let prepare = build_prepare(3, payload_size);
        let expected_bytes = prepare.as_slice().to_vec();
        let frozen = prepare.into_frozen();

        journal.append(frozen).await.expect("append");

        let header = journal.header_by_op(3).expect("header for op 3");
        let entry = journal
            .entry(&header)
            .await
            .expect("entry for op 3 must exist");

        assert_eq!(
            entry.as_slice(),
            expected_bytes.as_slice(),
            "retransmit path must read back the exact bytes that were appended; \
             cloning the returned Frozen is the sole payload copy"
        );

        let cloned = entry.clone();
        assert_eq!(
            cloned.as_slice(),
            entry.as_slice(),
            "cloning a journal entry must yield identical bytes (refcount bump, not deep copy)"
        );
    }

    #[compio::test]
    async fn committed_prefix_reads_then_evict_retains_uncommitted_tail() {
        // A backup journals ops ahead of the commit frontier. Reading the
        // committed prefix (op <= commit_max) must return only those without
        // evicting; evicting it must keep the uncommitted tail resident +
        // readable, with its headers intact, so a later commit of that tail
        // still finds it (no commit_min wedge).
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in 1..=4 {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }

        let committed = journal.committed_prefix(2);
        assert_eq!(
            committed.len(),
            2,
            "ops 1 and 2 are the committed prefix and must be returned"
        );
        // Reading does not evict: the prefix stays resident until persisted.
        assert!(
            journal.header_by_op(1).is_some(),
            "read must not evict op 1"
        );

        let retained = journal.evict_prefix(committed.len()).await;
        assert_eq!(
            retained.len(),
            2,
            "ops 3 and 4 stay resident after eviction"
        );

        // Committed ops are evicted from the index; uncommitted ops remain.
        assert!(journal.header_by_op(1).is_none(), "op 1 must be evicted");
        assert!(journal.header_by_op(2).is_none(), "op 2 must be evicted");
        let header3 = journal.header_by_op(3).expect("op 3 must be retained");
        let header4 = journal.header_by_op(4).expect("op 4 must be retained");

        // Retained entries are still byte-readable after the storage rebuild.
        for header in [header3, header4] {
            let entry = journal
                .entry(&header)
                .await
                .expect("retained entry must read back");
            let stored = bytemuck::checked::try_from_bytes::<PrepareHeader>(
                &entry[..std::mem::size_of::<PrepareHeader>()],
            )
            .expect("retained entry must hold a valid prepare header");
            assert_eq!(stored.op, header.op);
        }

        // Advancing the frontier flushes the rest with no gap.
        let committed = journal.committed_prefix(4);
        let rest = journal.evict_prefix(committed.len()).await;
        assert!(rest.is_empty(), "ops 3 and 4 flush on the next evict");
        assert!(journal.is_empty(), "journal is empty once all ops flushed");
    }

    #[compio::test]
    async fn committed_prefix_stops_at_gap() {
        // Ops {1,2,4} resident, commit_max = 4. The contiguous committed prefix
        // is {1,2}; op 4 must stay retained because op 3 is missing - flushing
        // it would put op-4 bytes on the segment ahead of the op-3 hole and
        // skew the durable offset past a gap advance_commit_min cannot cross.
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1u64, 2, 4] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }

        let committed = journal.committed_prefix(4);
        let ops: Vec<u64> = committed
            .iter()
            .map(|entry| {
                bytemuck::checked::try_from_bytes::<PrepareHeader>(
                    &entry[..std::mem::size_of::<PrepareHeader>()],
                )
                .expect("entry holds a valid prepare header")
                .op
            })
            .collect();
        assert_eq!(ops, vec![1, 2], "prefix stops before the op 3 gap");

        let retained = journal.evict_prefix(committed.len()).await;
        assert_eq!(retained.len(), 1, "op 4 stays retained past the gap");
        assert!(journal.header_by_op(4).is_some(), "op 4 still resident");
    }

    #[compio::test]
    async fn committed_headers_from_stops_at_gap() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1u64, 2, 4] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }

        // Contiguous run from op 1 stops before the missing op 3 even though
        // op 4 is resident and within commit_max.
        let run = journal.committed_headers_from(1, 4);
        let ops: Vec<u64> = run.iter().map(|header| header.op).collect();
        assert_eq!(
            ops,
            vec![1, 2],
            "must stop at the op 3 gap, not skip to op 4"
        );

        assert!(
            journal.committed_headers_from(5, 4).is_empty(),
            "from_op past commit_max yields nothing"
        );
    }
}
