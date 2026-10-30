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

use ahash::AHashMap;
use compio::buf::{IntoInner, IoBuf};
use compio::io::AsyncReadAtExt;
use iggy_binary_protocol::{Operation, PrepareHeader};
use journal::partition_journal::SegmentReference;
use journal::{Journal, Storage};
use server_common::{
    iobuf::{Frozen, IOV_MAX, Owned},
    send_messages::{
        self, BatchRef, COMMAND_HEADER_SIZE, decode_prepare_slice, decode_prepare_slice_trusted,
    },
};
use std::io;
use std::{
    cell::{Cell, RefCell, UnsafeCell},
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    ops::RangeInclusive,
    rc::{Rc, Weak},
};
use tracing::warn;

use crate::log::SegmentedLog;
use crate::repair_memory::RepairMemoryReservation;
use crate::{Fragment, PollFragments, PollQueryResult};

const ZERO_LEN: usize = 0;
const PREPARE_HEADER_SIZE: usize = std::mem::size_of::<PrepareHeader>();
type JournalBuffer = Frozen<4096>;

/// Decoded `SendMessages` header fields: what a resident batch adds to the
/// journal's offset and timestamp indexes and message totals, and what a flush
/// folds into segment accounting. Raw header values only: the journal stays
/// agnostic of partition-layer accounting types (`JournalInfo` lives in the
/// log layer).
#[derive(Clone, Copy)]
pub struct RetainedBatchMeta {
    pub base_offset: u64,
    pub base_timestamp: u64,
    pub total_size: u64,
    pub message_count: u32,
}

impl RetainedBatchMeta {
    /// `None` for an op that is not `SendMessages` or that carries no
    /// messages: such an entry has no offset to index and no segment bytes.
    ///
    /// Trusted (no batch-hash): every entry here was just stamped by
    /// `stamp_prepare_for_persistence` (its checksum recomputed over this exact
    /// blob) or was validated by such an append before, so re-hashing the
    /// ~1 MiB blob only to read the header is waste.
    fn of(header: &PrepareHeader, entry: &JournalBuffer) -> Option<Self> {
        if header.operation != Operation::SendMessages {
            return None;
        }
        let batch = decode_prepare_slice_trusted(entry.as_slice()).ok()?;
        let message_count = batch.message_count();
        (message_count != 0).then(|| Self {
            base_offset: batch.header.base_offset,
            base_timestamp: batch.header.base_timestamp,
            total_size: batch.header.total_size() as u64,
            message_count,
        })
    }
}

/// What one pass over the journal headers found for a repair window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairedWindowShape {
    /// Every op in the window is resident. An EMPTY window is complete: nothing
    /// left can arrive and change the floor verdict.
    pub complete: bool,
}

/// Where a committed prepare's body sits in the log file of its segment. A
/// segment stores the body without the prepare header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyPosition {
    /// Start offset of the segment, which names its log file.
    pub segment_start: u64,
    pub position: u64,
}

impl From<SegmentReference> for BodyPosition {
    fn from(reference: SegmentReference) -> Self {
        Self {
            segment_start: reference.start_offset,
            position: reference.position,
        }
    }
}

/// A committed prepare the repair ring keeps without its body. The byte cap
/// released the body, which still sits in its segment, so repair reads it
/// back instead of answering `RangeEvicted`.
///
/// Built only from a whole `SendMessages` prepare, so `header.size` covers the
/// prepare header and a batch.
#[derive(Debug, Clone, Copy)]
pub struct EvictedHeader {
    header: PrepareHeader,
    body: BodyPosition,
    /// Checksum of the released batch. Bytes read back must carry it, so
    /// another batch at the same position cannot pass for this op.
    batch_checksum: u64,
}

/// What one released entry costs the evicted ring: 288 bytes, so
/// [`EVICTED_HEADERS_MAX`] of them take 4.5 MiB per partition.
const EVICTED_HEADER_BYTES: u64 = size_of::<EvictedHeader>() as u64;
const _: () = assert!(EVICTED_HEADER_BYTES == 288);

impl EvictedHeader {
    #[must_use]
    pub const fn header(&self) -> &PrepareHeader {
        &self.header
    }

    /// Whether the body of `next` lies right after this one in the same
    /// segment file.
    const fn followed_by(&self, next: &Self) -> bool {
        let body_len = (self.header.size as usize).saturating_sub(PREPARE_HEADER_SIZE);
        self.body.segment_start == next.body.segment_start
            && self.body.position + body_len as u64 == next.body.position
    }

    /// `prepare`, read back for this entry, if its body is the released one.
    fn verified(&self, prepare: Owned<4096>) -> io::Result<JournalBuffer> {
        let batch = decode_prepare_slice(prepare.as_slice()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the body read back fails its checks: {error}"),
            )
        })?;
        if batch.header.batch_checksum != self.batch_checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "another batch sits at the evicted body position",
            ));
        }
        Ok(prepare.into())
    }

    /// Start offset of the segment whose log file holds the body.
    #[must_use]
    pub const fn segment_start(&self) -> u64 {
        self.body.segment_start
    }
}

/// Reads the bodies of `released`, in op order, back from the log files of
/// their segments in `log`, and pushes each prepare to `bodies`. Bodies that
/// lie back to back in one file come back in one read.
///
/// # Errors
/// Returns an error if a segment left `log`, if a body cannot be read, or if
/// the bytes there are not the released body: a torn or corrupt batch, or
/// another batch at the same position. Such bytes are never returned. The
/// bodies before the one that failed, `released[bodies.len()]`, stay in
/// `bodies`.
pub async fn read_released_bodies<J: Debug + Journal>(
    log: &SegmentedLog<J>,
    released: &[EvictedHeader],
    bodies: &mut Vec<JournalBuffer>,
) -> io::Result<()> {
    let mut file: Option<(u64, compio::fs::File)> = None;
    let runs = released
        .chunk_by(EvictedHeader::followed_by)
        .flat_map(|run| run.chunks(IOV_MAX));
    for run in runs {
        let Some(first) = run.first() else {
            continue;
        };
        let segment = first.body.segment_start;
        let file = match &mut file {
            Some((start, file)) if *start == segment => file,
            slot => {
                let path = log.messages_path(segment).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "the segment left the log")
                })?;
                &slot
                    .insert((segment, compio::fs::File::open(path).await?))
                    .1
            }
        };
        // A vectored file read in compio fills only the initialized bytes of
        // each buffer, so the bodies start zeroed.
        let buffers: Vec<_> = run
            .iter()
            .map(|evicted| {
                let size = evicted.header.size as usize;
                let mut prepare = Owned::<4096>::zeroed(size);
                prepare.as_mut_slice()[..PREPARE_HEADER_SIZE]
                    .copy_from_slice(bytemuck::bytes_of(&evicted.header));
                prepare.slice(PREPARE_HEADER_SIZE..size)
            })
            .collect();
        let compio::BufResult(read, buffers) = file
            .read_vectored_exact_at(buffers, first.body.position)
            .await;
        for (evicted, body) in run.iter().zip(buffers) {
            match evicted.verified(body.into_inner()) {
                Ok(prepare) => bodies.push(prepare),
                // Only the released body itself passes the checks, and a
                // failed read leaves zeros where it stopped, so after a failed
                // read the first body that fails is where the read stopped.
                Err(error) => return Err(read.err().unwrap_or(error)),
            }
        }
    }
    Ok(())
}

/// A committed prepare the repair ring holds in memory, with the place of its
/// body in a segment when the WAL wrote it there.
struct RingEntry {
    prepare: JournalBuffer,
    body: Option<BodyPosition>,
}

/// A snapshot's repair suffix. Advancing or dropping the last lease releases
/// its extra retention immediately. The ordinary ring keeps its own caps.
pub struct RepairTailLease {
    state: Rc<RepairTailState>,
    retained: Weak<RefCell<RepairTail>>,
}

struct RepairTailState {
    from_op: Cell<u64>,
    valid: Cell<bool>,
}

impl RepairTailLease {
    pub(crate) fn is_valid(&self) -> bool {
        self.state.valid.get()
    }

    pub(crate) fn advance(&self, from_op: u64) {
        self.state
            .from_op
            .set(self.state.from_op.get().max(from_op));
        if let Some(retained) = self.retained.upgrade() {
            retained.borrow_mut().prune();
        }
    }
}

impl Drop for RepairTailLease {
    fn drop(&mut self) {
        self.state.valid.set(false);
        if let Some(retained) = self.retained.upgrade() {
            retained.borrow_mut().prune();
        }
    }
}

#[derive(Default)]
struct RepairTail {
    leases: Vec<Weak<RepairTailState>>,
    bodies: BTreeMap<u64, JournalBuffer>,
    headers: BTreeMap<u64, EvictedHeader>,
    memory: Option<RepairMemoryReservation>,
}

impl RepairTail {
    fn prune(&mut self) -> Option<u64> {
        let mut from_op = None;
        self.leases.retain(|lease| {
            let Some(lease) = lease.upgrade().filter(|lease| lease.valid.get()) else {
                return false;
            };
            let floor = lease.from_op.get();
            from_op = Some(from_op.map_or(floor, |old: u64| old.min(floor)));
            true
        });
        let Some(from_op) = from_op else {
            self.bodies.clear();
            self.headers.clear();
            self.memory = None;
            return None;
        };
        while let Some(entry) = self
            .bodies
            .first_entry()
            .filter(|entry| *entry.key() < from_op)
        {
            let bytes = Self::body_memory_bytes(&entry.remove());
            if let Some(memory) = &mut self.memory {
                memory.release(bytes);
            }
        }
        while let Some(entry) = self
            .headers
            .first_entry()
            .filter(|entry| *entry.key() < from_op)
        {
            entry.remove();
            if let Some(memory) = &mut self.memory {
                memory.release(std::mem::size_of::<(u64, EvictedHeader)>());
            }
        }
        Some(from_op)
    }

    fn invalidate(&mut self) {
        for lease in &self.leases {
            if let Some(lease) = lease.upgrade() {
                lease.valid.set(false);
            }
        }
        self.leases.clear();
        self.bodies.clear();
        self.headers.clear();
        self.memory = None;
    }

    fn reserve(&mut self, bytes: usize) -> bool {
        if self
            .memory
            .as_mut()
            .is_some_and(|memory| memory.try_add(bytes))
        {
            return true;
        }
        self.invalidate();
        false
    }

    fn keep_header(&mut self, entry: &EvictedHeader, from_op: Option<u64>) {
        let op = entry.header.op;
        if from_op.is_none_or(|from_op| op < from_op) || self.headers.contains_key(&op) {
            return;
        }
        if let Some(body) = self.bodies.remove(&op)
            && let Some(memory) = &mut self.memory
        {
            memory.release(Self::body_memory_bytes(&body));
        }
        if self.reserve(std::mem::size_of::<(u64, EvictedHeader)>()) {
            self.headers.insert(op, *entry);
        }
    }

    fn keep_body(&mut self, op: u64, entry: RingEntry, from_op: Option<u64>) {
        if from_op.is_none_or(|from_op| op < from_op)
            || self.bodies.contains_key(&op)
            || self.headers.contains_key(&op)
        {
            return;
        }
        if let Some(header) = entry.evicted_header() {
            self.keep_header(&header, from_op);
        } else if self.reserve(Self::body_memory_bytes(&entry.prepare)) {
            self.bodies.insert(op, entry.prepare);
        }
    }

    fn body_memory_bytes(body: &JournalBuffer) -> usize {
        JournalBuffer::allocation_size(body.backing_capacity())
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<(u64, JournalBuffer)>()))
            .unwrap_or(usize::MAX)
    }
}

impl RingEntry {
    /// What the ring keeps of this entry once its body leaves memory. `None`
    /// when no segment holds the body.
    fn evicted_header(&self) -> Option<EvictedHeader> {
        let body = self.body?;
        let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(
            self.prepare.as_slice().get(..PREPARE_HEADER_SIZE)?,
        )
        .ok()?;
        if header.operation != Operation::SendMessages {
            return None;
        }
        let batch = decode_prepare_slice_trusted(self.prepare.as_slice()).ok()?;
        Some(EvictedHeader {
            header,
            body,
            batch_checksum: batch.header.batch_checksum,
        })
    }
}

/// Drops the oldest entry of either tier of the evicted ring, so the ops it
/// keeps stay one run. Returns the body bytes it frees.
fn drop_oldest_evicted(
    ring: &mut BTreeMap<u64, RingEntry>,
    headers: &mut VecDeque<EvictedHeader>,
    retained: &mut RepairTail,
    from_op: Option<u64>,
) -> u64 {
    let oldest_held = ring.first_key_value().map(|(op, _)| *op);
    match headers.front() {
        Some(evicted) if oldest_held.is_none_or(|held| evicted.header.op < held) => {
            if let Some(entry) = headers.pop_front() {
                retained.keep_header(&entry, from_op);
            }
            0
        }
        _ => ring.pop_first().map_or(0, |(op, dropped)| {
            let bytes = dropped.prepare.len() as u64;
            retained.keep_body(op, dropped, from_op);
            bytes
        }),
    }
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
/// This storage backs the `Journal` trait with a plain `VecDeque<JournalBuffer>`
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
    entries: UnsafeCell<VecDeque<JournalBuffer>>,
    /// Offset of `entries[0]`. An offset counts the entries written since the
    /// last drain, so popping the front leaves every other offset valid.
    front_offset: Cell<usize>,
}

impl Storage for PartitionJournalMemStorage {
    type Buffer = JournalBuffer;

    async fn write_at(&self, _offset: usize, buf: Self::Buffer) -> io::Result<usize> {
        Ok(self.write_at_sync(buf))
    }

    async fn read_at(&self, offset: usize, _buffer: Self::Buffer) -> io::Result<Self::Buffer> {
        Ok(self
            .read_at_sync(offset)
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
    /// Maps `(base_timestamp, op)` -> op (for queryable entries).
    ///
    /// Keeping `op` in the key preserves duplicate timestamps while still
    /// letting us seek to the closest batch for timestamp-based polling.
    timestamp_to_op: UnsafeCell<BTreeMap<(u64, u64), u64>>,
    headers: UnsafeCell<VecDeque<PrepareHeader>>,
    /// Position of `headers[0]`, counted from the last clear, so the positions
    /// in `first_header_by_op` stay valid when the front leaves.
    front_position: Cell<usize>,
    /// Commit validation uses the first resident occurrence, including duplicates.
    first_header_by_op: RefCell<AHashMap<u64, usize>>,
    /// Messages in the resident `SendMessages` batches.
    resident_messages: Cell<u64>,
    /// Segment bytes of the resident `SendMessages` batches.
    resident_message_size: Cell<u64>,
    inner: UnsafeCell<JournalInner<S>>,
    /// Ring of recently evicted committed entries, keyed by op, retained so
    /// this replica can serve journal repair for rejoin windows after the
    /// entries left the resident journal at flush. Bodies past the byte cap
    /// move to `evicted_headers` when a segment holds them. Bounded by
    /// [`EVICTED_RING_CAPACITY`]; requests older than the ring answer
    /// `RangeEvicted` honestly.
    evicted_ring: UnsafeCell<BTreeMap<u64, RingEntry>>,
    /// Running byte total of the prepares held by `evicted_ring`.
    evicted_ring_bytes: Cell<u64>,
    /// The ring's entries whose bodies the byte cap released to their
    /// segments, in op order. At most [`EVICTED_HEADERS_MAX`].
    evicted_headers: UnsafeCell<VecDeque<EvictedHeader>>,
    repair_tail: Rc<RefCell<RepairTail>>,
    /// Highest op the byte cap released or passed over, so it looks at each
    /// entry once. An entry that lands below it later keeps its body until
    /// the oldest entries go.
    released_through: Cell<u64>,
    /// Entry-count ceiling for `evicted_ring`. Defaults to
    /// [`EVICTED_RING_CAPACITY`], and the server overrides it from config at
    /// partition build.
    evicted_ring_capacity: Cell<usize>,
    /// Byte ceiling for `evicted_ring`. Defaults to
    /// [`EVICTED_RING_BYTES_MAX`], and the server overrides it from config at
    /// partition build.
    evicted_ring_bytes_max: Cell<u64>,
    /// Single-replica groups have nobody to repair; retaining evicted
    /// entries for them is pure memory waste.
    repair_retention: Cell<bool>,
    /// Resident entries that are not `SendMessages` (consumer offset stores
    /// and deletes). They carry no segment bytes, so the message-count and
    /// byte flush thresholds never see them, yet the flush is the only
    /// eviction: without a bound on this count a consume-only workload, one
    /// replicated auto-commit per poll, grows the journal for as long as it
    /// runs. Rebuilt by every drain-and-re-append.
    resident_control_ops: Cell<usize>,
    /// Highest op an eviction removed. With the commit point it bounds where
    /// the next flush prefix may start.
    evicted_through: Cell<u64>,
    /// A resident op follows a higher one, so the next op to flush can sit
    /// behind the front. Flushes wait for `sort_by_op`.
    out_of_order: Cell<bool>,
    /// `(floor, through)`: every op in `floor + 1..=through` is resident. Repair
    /// ticks resume the walk from here instead of rescanning the held prefix, and
    /// any removal resets it.
    held_run: Cell<(u64, u64)>,
}

/// How many evicted entries each partition retains for repair with their
/// bodies in memory. The entries whose bodies went back to their segments
/// have their own cap, `EVICTED_HEADERS_MAX`.
pub const EVICTED_RING_CAPACITY: usize = 65536;

/// Byte ceiling for the bodies the evicted ring holds in memory.
///
/// An entry that holds its body pins a full batch, so an entry cap alone can
/// consume too much memory across many partitions. Past this cap the oldest
/// bodies that a segment still holds leave memory, and the oldest entries go
/// when none can leave. The headers of released bodies do not count here.
pub const EVICTED_RING_BYTES_MAX: u64 = 64 * 1024 * 1024;

/// How many entries whose bodies went back to their segments each partition
/// keeps for repair. Each one takes 288 bytes, so the cap holds 4.5 MiB per
/// partition, under 8% of the body window that [`EVICTED_RING_BYTES_MAX`] sets.
///
/// The cap sets how far a backup can fall behind and still catch up by
/// repair: 128 repair rounds at the default chunk of 128 ops. Past it, the
/// backup gets `RangeEvicted` for the older ops and catches up by state
/// transfer, which reuses the sealed segments that the backup holds.
/// A power of two, so the deque that holds the headers, which makes room
/// before each push, never allocates past it.
pub const EVICTED_HEADERS_MAX: usize = 16384;
const _: () = assert!(EVICTED_HEADERS_MAX.is_power_of_two());

impl<S> Default for PartitionJournal<S>
where
    S: Storage<Buffer = JournalBuffer> + Default,
{
    fn default() -> Self {
        Self::with_storage(S::default())
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
    /// storage never hits the reactor (it copies from an in-memory `VecDeque`),
    /// so the read can run under a partition borrow without crossing an
    /// `.await` - the property that keeps poll-read sound.
    fn read_at_sync(&self, offset: usize) -> Option<JournalBuffer> {
        let entries = unsafe { &*self.entries.get() };
        entries
            .get(offset.checked_sub(self.front_offset.get())?)
            .cloned()
    }

    fn entries(&self) -> Vec<JournalBuffer> {
        let entries = unsafe { &*self.entries.get() };
        entries.iter().cloned().collect()
    }

    fn drain(&self) -> Vec<JournalBuffer> {
        let entries = unsafe { &mut *self.entries.get() };
        self.front_offset.set(0);
        std::mem::take(entries).into()
    }

    /// Removes the front entry and keeps the offsets of the others.
    fn pop_front(&self) -> Option<JournalBuffer> {
        let entries = unsafe { &mut *self.entries.get() };
        let entry = entries.pop_front()?;
        self.front_offset.set(self.front_offset.get() + 1);
        Some(entry)
    }

    fn is_empty(&self) -> bool {
        let entries = unsafe { &*self.entries.get() };
        entries.is_empty()
    }

    fn current_offset(&self) -> usize {
        let entries = unsafe { &*self.entries.get() };
        self.front_offset.get() + entries.len()
    }

    fn write_at_sync(&self, buf: JournalBuffer) -> usize {
        let len = buf.len();
        let entries = unsafe { &mut *self.entries.get() };
        entries.push_back(buf);
        len
    }
}

impl PartitionJournal<PartitionJournalMemStorage> {
    /// Drop EVERYTHING this journal holds: resident entries, the
    /// op/offset/timestamp indexes, and the evicted repair ring.
    ///
    /// State-transfer install only. The installed segments supersede every
    /// journaled op at or below the new commit floor, and the stale suffix
    /// ABOVE it (prepared-but-uncommitted ops from a superseded view) would
    /// collide with the new view's prepares at the same op numbers. The
    /// journal is memory-only, so a full clear IS the partition plane's
    /// suffix truncation; the receiver re-fetches the live tail through
    /// normal journal repair afterwards. Ring caps and the retention flag
    /// survive: they are configuration, not content.
    pub fn clear_all(&self) {
        {
            let inner = unsafe { &*self.inner.get() };
            let _ = inner.storage.drain();
        }
        self.clear_resident_indexes();
        self.clear_evicted();
        self.evicted_through.set(0);
    }

    /// Disable repair retention (single-replica groups: nobody to repair).
    pub fn set_repair_retention(&self, enabled: bool) {
        self.repair_retention.set(enabled);
        if !enabled {
            self.clear_evicted();
        }
    }

    fn clear_evicted(&self) {
        self.repair_tail.borrow_mut().invalidate();
        unsafe { &mut *self.evicted_ring.get() }.clear();
        unsafe { &mut *self.evicted_headers.get() }.clear();
        self.evicted_ring_bytes.set(0);
        self.released_through.set(0);
    }

    pub(crate) fn retain_repair_from(
        &self,
        from_op: u64,
        memory: RepairMemoryReservation,
    ) -> RepairTailLease {
        let state = Rc::new(RepairTailState {
            from_op: Cell::new(from_op),
            valid: Cell::new(true),
        });
        let mut retained = self.repair_tail.borrow_mut();
        retained.prune();
        if retained.memory.is_none() {
            retained.memory = Some(memory);
        }
        retained.leases.push(Rc::downgrade(&state));
        RepairTailLease {
            state,
            retained: Rc::downgrade(&self.repair_tail),
        }
    }

    /// Override the evicted-ring ceilings from configuration. Called once at
    /// partition build, before any eviction, so the caps govern the first
    /// flush onward. Leaves `repair_retention` untouched: the single-replica
    /// disable path stands on its own.
    pub fn set_ring_caps(&self, capacity: usize, bytes_max: u64) {
        self.evicted_ring_capacity.set(capacity);
        self.evicted_ring_bytes_max.set(bytes_max);
    }

    /// Entries the repair ring retains and the bytes they pin, for the shard
    /// sweep: the prepare of each entry held in memory, and an
    /// [`EvictedHeader`] for each entry whose body went back to its segment.
    ///
    /// Excludes unused allocation capacity, alignment and index overhead.
    /// The newest entry is retained even when it exceeds the byte budget.
    pub fn evicted_ring_occupancy(&self) -> (usize, u64) {
        let ring = unsafe { &*self.evicted_ring.get() };
        let headers = unsafe { &*self.evicted_headers.get() };
        let retained = self.repair_tail.borrow();
        (
            ring.len() + headers.len() + retained.bodies.len() + retained.headers.len(),
            self.evicted_ring_bytes.get()
                + headers.len() as u64 * EVICTED_HEADER_BYTES
                + retained
                    .memory
                    .as_ref()
                    .map_or(0, |memory| memory.bytes() as u64),
        )
    }

    /// Committed entries already flushed out of the resident journal cannot be
    /// discarded by view reconciliation, even while the apply walk lags behind.
    #[must_use]
    pub const fn evicted_through(&self) -> u64 {
        self.evicted_through.get()
    }

    /// Resident (un-evicted) entry count; diagnostics only.
    pub fn resident_count(&self) -> usize {
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        op_to_storage_offset.len()
    }

    /// Resident entries that are not `SendMessages`; the partition's flush
    /// gate bounds this the way the message thresholds bound batches.
    pub const fn resident_control_ops(&self) -> usize {
        self.resident_control_ops.get()
    }

    /// Messages in the resident `SendMessages` batches, which the partition's
    /// message-count flush threshold reads.
    pub const fn resident_messages(&self) -> u64 {
        self.resident_messages.get()
    }

    /// Segment bytes of the resident `SendMessages` batches, which the
    /// partition's size flush threshold reads.
    pub const fn resident_message_size(&self) -> u64 {
        self.resident_message_size.get()
    }

    /// Restore the materialized commit point for elections and body repair.
    pub fn restore_checkpoint_prepare(&self, op: u64, prepare: JournalBuffer) {
        let ring = unsafe { &mut *self.evicted_ring.get() };
        debug_assert!(ring.is_empty() && unsafe { &*self.evicted_headers.get() }.is_empty());
        self.evicted_ring_bytes.set(prepare.len() as u64);
        ring.insert(
            op,
            RingEntry {
                prepare,
                body: None,
            },
        );
    }

    /// Entry bytes for `op`, from the resident journal or the evicted ring.
    /// `None` when the op predates the ring (bulk-sync territory) or was
    /// never journaled here.
    pub fn repair_entry(&self, op: u64) -> Option<JournalBuffer> {
        {
            let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
            if let Some(&storage_offset) = op_to_storage_offset.get(&op) {
                let inner = unsafe { &*self.inner.get() };
                return inner.storage.read_at_sync(storage_offset);
            }
        }
        let ring = unsafe { &*self.evicted_ring.get() };
        ring.get(&op)
            .map(|entry| entry.prepare.clone())
            .or_else(|| self.repair_tail.borrow().bodies.get(&op).cloned())
    }

    /// The lowest resident `SendMessages` op in `ops` with its entry bytes, in
    /// one pass over the headers. Resident only: the evicted ring holds what
    /// already committed, which is not what a repair window is judged by.
    #[must_use]
    pub fn lowest_resident_send_in(
        &self,
        ops: RangeInclusive<u64>,
    ) -> Option<(u64, JournalBuffer)> {
        let op = {
            let headers = unsafe { &*self.headers.get() };
            headers
                .iter()
                .filter(|header| {
                    header.operation == Operation::SendMessages && ops.contains(&header.op)
                })
                .map(|header| header.op)
                .min()?
        };
        let entry = {
            let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
            let inner = unsafe { &*self.inner.get() };
            op_to_storage_offset
                .get(&op)
                .and_then(|&storage_offset| inner.storage.read_at_sync(storage_offset))
        };
        if entry.is_none() {
            warn!(
                op,
                "resident SendMessages header has no stored entry; the repair window has no anchor"
            );
        }
        entry.map(|entry| (op, entry))
    }

    /// The header at `op`, over exactly the range [`Self::repair_entry`] serves.
    ///
    /// NOT [`Self::header_by_op`], which reads the resident headers alone. The
    /// committed prefix is evicted from those the moment its bytes reach a
    /// segment, up to and including `commit_max`, so a `DoViewChange` built off
    /// the resident headers reports its own commit point blank. The merge scans
    /// the commit point and cannot discard it, so a quorum of such senders is
    /// undecidable and the view never starts (`dvc_merge::merge_dvc_quorum`).
    /// The entry is still servable from the evicted ring, which is what makes
    /// the blank wrong rather than merely pessimistic.
    ///
    /// Retention drops the lowest op, including when repair backfills arrive
    /// out of order, so the commit point is the last thing it forgets.
    pub fn repair_header(&self, op: u64) -> Option<PrepareHeader> {
        if let Some(header) = self.header_by_op(op) {
            return Some(header);
        }
        let ring = unsafe { &*self.evicted_ring.get() };
        let retained = self.repair_tail.borrow();
        let prepare = ring
            .get(&op)
            .map(|entry| &entry.prepare)
            .or_else(|| retained.bodies.get(&op))?;
        let header_bytes = prepare.as_slice().get(..PREPARE_HEADER_SIZE)?;
        bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
            .ok()
            .copied()
    }

    /// Every repairable header with an op in `ops`, scanning resident headers
    /// once and seeking the retained repair range by op.
    ///
    /// Avoid repeating the resident header scan for each op in a view-change
    /// suffix. Result size is bounded by what the journal holds, not by the
    /// width of `ops`. Resident entries take precedence over retained repairs.
    #[must_use]
    pub fn repair_headers_in(&self, ops: RangeInclusive<u64>) -> BTreeMap<u64, PrepareHeader> {
        let mut found = BTreeMap::new();
        if ops.is_empty() {
            return found;
        }
        {
            let headers = unsafe { &*self.headers.get() };
            for header in headers.iter().filter(|header| ops.contains(&header.op)) {
                found.insert(header.op, *header);
            }
        }
        let ring = unsafe { &*self.evicted_ring.get() };
        for (op, entry) in ring.range(ops.clone()) {
            if found.contains_key(op) {
                continue;
            }
            let Some(header_bytes) = entry.prepare.as_slice().get(..PREPARE_HEADER_SIZE) else {
                continue;
            };
            if let Ok(header) = bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes) {
                found.insert(*op, *header);
            }
        }
        for (op, entry) in self.repair_tail.borrow().bodies.range(ops) {
            if found.contains_key(op) {
                continue;
            }
            let Some(bytes) = entry.as_slice().get(..PREPARE_HEADER_SIZE) else {
                continue;
            };
            if let Ok(header) = bytemuck::checked::try_from_bytes::<PrepareHeader>(bytes) {
                found.insert(*op, *header);
            }
        }
        found
    }

    /// Oldest op this journal can still serve for repair, including resident
    /// backfills older than the retained repair range and bodies read back
    /// from segments.
    pub fn repair_retained_from(&self) -> Option<u64> {
        let headers = unsafe { &*self.evicted_headers.get() };
        self.oldest_in_memory()
            .into_iter()
            .chain(headers.front().map(|evicted| evicted.header.op))
            .chain(
                self.repair_tail
                    .borrow()
                    .headers
                    .first_key_value()
                    .map(|(op, _)| *op),
            )
            .min()
    }

    /// Oldest op from which this journal holds every body up to its head in
    /// memory, so repair serves that tail without a disk read.
    pub fn memory_retained_from(&self) -> Option<u64> {
        let headers = unsafe { &*self.evicted_headers.get() };
        let oldest = self.oldest_in_memory()?;
        let newest_released = headers
            .back()
            .map(|entry| entry.header.op)
            .into_iter()
            .chain(
                self.repair_tail
                    .borrow()
                    .headers
                    .last_key_value()
                    .map(|(op, _)| *op),
            )
            .max();
        Some(newest_released.map_or(oldest, |op| oldest.max(op.saturating_add(1))))
    }

    fn oldest_in_memory(&self) -> Option<u64> {
        let ring = unsafe { &*self.evicted_ring.get() };
        let resident = unsafe { &*self.op_to_storage_offset.get() };
        ring.first_key_value()
            .map(|(op, _)| *op)
            .into_iter()
            .chain(resident.first_key_value().map(|(op, _)| *op))
            .chain(
                self.repair_tail
                    .borrow()
                    .bodies
                    .first_key_value()
                    .map(|(op, _)| *op),
            )
            .min()
    }

    /// The header the evicted ring keeps for `op` after the byte cap released
    /// its body to a segment. [`Self::repair_entry`] and
    /// [`Self::repair_header`] do not serve such an op: its body must be read
    /// back with [`read_released_bodies`].
    #[must_use]
    pub fn evicted_header(&self, op: u64) -> Option<EvictedHeader> {
        let headers = unsafe { &*self.evicted_headers.get() };
        headers
            .binary_search_by_key(&op, |evicted| evicted.header.op)
            .ok()
            .and_then(|index| headers.get(index).copied())
            .or_else(|| self.repair_tail.borrow().headers.get(&op).copied())
    }

    /// Forgets the released entries whose bodies lie in segments up to the
    /// one at `start_offset`, once retention deletes it, so repair answers
    /// `RangeEvicted` for them.
    pub fn forget_segment(&self, start_offset: u64) {
        if self
            .repair_tail
            .borrow()
            .headers
            .values()
            .any(|entry| entry.body.segment_start <= start_offset)
        {
            self.repair_tail.borrow_mut().invalidate();
        }
        let headers = unsafe { &*self.evicted_headers.get() };
        let through = headers
            .iter()
            .take_while(|evicted| evicted.body.segment_start <= start_offset)
            .last()
            .map(|evicted| evicted.header.op);
        if let Some(through) = through {
            self.forget_released_through(through);
        }
    }

    /// Forgets every evicted entry at or below the released entry at `op`,
    /// so repair answers `RangeEvicted` for them. A released entry is never
    /// the newest one, so the commit point stays.
    pub fn forget_released_through(&self, op: u64) {
        let retained_affected = {
            let retained = self.repair_tail.borrow();
            retained
                .headers
                .first_key_value()
                .is_some_and(|(first, _)| *first <= op)
                || retained
                    .bodies
                    .first_key_value()
                    .is_some_and(|(first, _)| *first <= op)
        };
        if retained_affected {
            self.repair_tail.borrow_mut().invalidate();
        }
        let ring = unsafe { &mut *self.evicted_ring.get() };
        let headers = unsafe { &mut *self.evicted_headers.get() };
        let mut bytes = self.evicted_ring_bytes.get();
        while let Some(entry) = ring.first_entry().filter(|entry| *entry.key() <= op) {
            bytes -= entry.remove().prepare.len() as u64;
        }
        while headers
            .front()
            .is_some_and(|evicted| evicted.header.op <= op)
        {
            headers.pop_front();
        }
        self.evicted_ring_bytes.set(bytes);
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
            return (PollFragments::new(), None, 0);
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
                let Some(bytes) = inner.storage.read_at_sync(storage_offset) else {
                    break;
                };
                bytes
            };

            try_push_resident_entry(
                &bytes,
                query,
                &mut fragments,
                &mut last_matching_offset,
                &mut matched_messages,
            );
        }

        (fragments, last_matching_offset, matched_messages)
    }

    /// Drain all accumulated batches, matching the legacy `PartitionJournal` API.
    pub fn commit(&self) -> Vec<JournalBuffer> {
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };
        self.clear_resident_indexes();
        entries
    }

    /// Pin the contiguous resident prefix through `commit_max`, stopping at gaps.
    /// Entries remain resident until their physical writes have been accepted.
    #[cfg(test)]
    pub(crate) fn committed_prefix(&self, commit_max: u64) -> Vec<JournalBuffer> {
        // No local commit point, so only evictions bound the start.
        let count = self.committed_prefix_len(0, commit_max);
        let inner = unsafe { &*self.inner.get() };
        let entries = unsafe { &*inner.storage.entries.get() };
        entries.iter().take(count).cloned().collect()
    }

    /// Inspect resident allocations without pinning extra payload references.
    /// The closure cannot retain the borrowed entry deque across owner mutations.
    pub(crate) fn with_entries<R>(&self, inspect: impl FnOnce(&VecDeque<JournalBuffer>) -> R) -> R {
        let inner = unsafe { &*self.inner.get() };
        let entries = unsafe { &*inner.storage.entries.get() };
        inspect(entries)
    }

    /// Front entries a flush through `through` can write in op order. Every op
    /// up to `commit_min` and up to the highest evicted op is flushed or
    /// resident, so the first entry may sit at most one op above them. A higher
    /// first op sits above a hole. An out-of-order journal can hide the next op
    /// behind the front, so it yields nothing until `sort_by_op` runs.
    pub(crate) fn committed_prefix_len(&self, commit_min: u64, through: u64) -> usize {
        if self.out_of_order.get() {
            return 0;
        }
        let headers = unsafe { &*self.headers.get() };
        let first_max = commit_min.max(self.evicted_through.get()).saturating_add(1);
        let mut previous: Option<u64> = None;
        headers
            .iter()
            .take_while(|header| {
                let connected = previous.map_or(header.op <= first_max, |op| {
                    op.checked_add(1) == Some(header.op)
                });
                if header.op > through || !connected {
                    return false;
                }
                previous = Some(header.op);
                true
            })
            .count()
    }

    /// Restore op order after an out-of-order append. Stable, so the first copy
    /// of a duplicated op stays first. Every resident entry can move, so a
    /// position taken before the call can name another entry after it. Returns
    /// whether the journal was rebuilt.
    pub(crate) fn sort_by_op(&self) -> bool {
        if !self.out_of_order.get() {
            return false;
        }
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };
        // Positional against `headers` until the clear below.
        let mut ordered: Vec<(u64, JournalBuffer)> = {
            let headers = unsafe { &*self.headers.get() };
            headers
                .iter()
                .map(|header| header.op)
                .zip(entries)
                .collect()
        };
        ordered.sort_by_key(|(op, _)| *op);
        self.clear_resident_indexes();
        for (_, entry) in ordered {
            self.append_sync(entry);
        }
        true
    }

    /// Remove the persisted prefix: the `count` front entries.
    pub fn evict_prefix(&self, count: usize) {
        self.evict_prefix_located(count, |_| None);
    }

    /// [`Self::evict_prefix`] for a journal whose bodies also sit in segments.
    /// `locate` names where the body of an evicted op lies, so the evicted
    /// ring can release that body from memory and still serve the op.
    ///
    /// Costs O(`count`), however many entries stay resident: a lagging backup
    /// holds hundreds of thousands and evicts after every flush. Two rare
    /// shapes rebuild the retained tail instead, because a popped entry would
    /// leave a stale index behind: a journal out of op order, and a prefix op
    /// that has another copy in the retained tail.
    pub fn evict_prefix_located(&self, count: usize, locate: impl Fn(u64) -> Option<BodyPosition>) {
        let evicted = if self.out_of_order.get() || self.retains_copy_of_prefix(count) {
            self.rebuild_without_prefix(count)
        } else {
            self.pop_prefix(count)
        };
        if let Some(highest) = evicted.iter().map(|(op, _)| *op).max() {
            self.evicted_through
                .set(self.evicted_through.get().max(highest));
        }
        if !self.repair_retention.get() {
            return;
        }
        let ring = unsafe { &mut *self.evicted_ring.get() };
        let mut ring_bytes = self.evicted_ring_bytes.get();
        for (op, prepare) in evicted {
            ring_bytes += prepare.len() as u64;
            let entry = RingEntry {
                prepare,
                body: locate(op),
            };
            if let Some(previous) = ring.insert(op, entry) {
                ring_bytes -= previous.prepare.len() as u64;
            }
        }
        self.evicted_ring_bytes.set(ring_bytes);
        self.trim_evicted_ring();
    }

    /// Whether an op in the `count` front entries has a copy behind them. The
    /// op index names the last copy, so one lookup per op answers it.
    fn retains_copy_of_prefix(&self, count: usize) -> bool {
        let headers = unsafe { &*self.headers.get() };
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        let inner = unsafe { &*self.inner.get() };
        let retained_from = inner.storage.current_offset() - headers.len().saturating_sub(count);
        headers.iter().take(count).any(|header| {
            op_to_storage_offset
                .get(&header.op)
                .is_some_and(|&storage_offset| storage_offset >= retained_from)
        })
    }

    /// Pops the `count` front entries and removes them from every index. The
    /// caller makes sure that no evicted op has a retained copy, so each index
    /// ends as a rebuild of the retained tail would leave it.
    fn pop_prefix(&self, count: usize) -> Vec<(u64, JournalBuffer)> {
        let headers = unsafe { &mut *self.headers.get() };
        let inner = unsafe { &*self.inner.get() };
        let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
        let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
        let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
        let mut first_header_by_op = self.first_header_by_op.borrow_mut();
        let mut evicted = Vec::with_capacity(count.min(headers.len()));
        while evicted.len() < count {
            // The length-lock pairs every header with one stored entry.
            let (Some(header), Some(entry)) = (headers.pop_front(), inner.storage.pop_front())
            else {
                break;
            };
            self.front_position.set(self.front_position.get() + 1);
            let op = header.op;
            op_to_storage_offset.remove(&op);
            first_header_by_op.remove(&op);
            if header.operation != Operation::SendMessages {
                self.resident_control_ops
                    .set(self.resident_control_ops.get() - 1);
            }
            if let Some(meta) = RetainedBatchMeta::of(&header, &entry) {
                // A later batch at the same offset keeps the key.
                if offset_to_op.get(&meta.base_offset) == Some(&op) {
                    offset_to_op.remove(&meta.base_offset);
                }
                timestamp_to_op.remove(&(meta.base_timestamp, op));
                self.resident_messages
                    .set(self.resident_messages.get() - u64::from(meta.message_count));
                self.resident_message_size
                    .set(self.resident_message_size.get() - meta.total_size);
            }
            evicted.push((op, entry));
        }
        // Every op above the highest evicted one is still resident, so the
        // cached run keeps that part instead of a full walk on the next probe.
        if let Some(highest) = evicted.iter().map(|(op, _)| *op).max() {
            let (run_floor, run_through) = self.held_run.get();
            let floor = run_floor.max(highest);
            self.held_run.set(if floor < run_through {
                (floor, run_through)
            } else {
                (0, 0)
            });
        }
        evicted
    }

    /// Evicts the `count` front entries by draining the journal and appending
    /// the retained tail again.
    fn rebuild_without_prefix(&self, count: usize) -> Vec<(u64, JournalBuffer)> {
        let entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };
        // Positional against `headers` until the clear below.
        let ops: Vec<u64> = {
            let headers = unsafe { &*self.headers.get() };
            headers.iter().map(|header| header.op).collect()
        };
        self.clear_resident_indexes();
        let mut entries = ops.into_iter().zip(entries);
        let evicted = entries.by_ref().take(count).collect();
        for (_, entry) in entries {
            self.append_sync(entry);
        }
        evicted
    }

    /// Keeps the evicted ring within its caps. Past the byte cap the oldest
    /// bodies that a segment holds leave memory first, and the oldest entries
    /// go when none can leave. The byte cap never takes the newest entry, so
    /// the commit point stays servable for a view change. A released body
    /// keeps its header, and the oldest entries make room for it first once
    /// [`EVICTED_HEADERS_MAX`] headers are kept.
    fn trim_evicted_ring(&self) {
        let mut retained = self.repair_tail.borrow_mut();
        let retained_from = retained.prune();
        let ring = unsafe { &mut *self.evicted_ring.get() };
        let headers = unsafe { &mut *self.evicted_headers.get() };
        let mut bytes = self.evicted_ring_bytes.get();
        let bytes_max = self.evicted_ring_bytes_max.get();
        let newest = ring.last_key_value().map(|(op, _)| *op);
        while bytes > bytes_max {
            let Some((&op, entry)) = ring
                .range(self.released_through.get().saturating_add(1)..)
                .next()
                .filter(|(op, _)| Some(**op) != newest)
            else {
                break;
            };
            self.released_through.set(op);
            if let Some(evicted) = entry.evicted_header() {
                bytes -= entry.prepare.len() as u64;
                ring.remove(&op);
                while headers.len() >= EVICTED_HEADERS_MAX {
                    bytes -= drop_oldest_evicted(ring, headers, &mut retained, retained_from);
                }
                headers.push_back(evicted);
            }
        }
        while ring.len() > self.evicted_ring_capacity.get() || (bytes > bytes_max && ring.len() > 1)
        {
            bytes -= drop_oldest_evicted(ring, headers, &mut retained, retained_from);
        }
        self.evicted_ring_bytes.set(bytes);
    }

    /// INVARIANT (length-lock): the header is pushed before `storage.write_at`,
    /// so `headers[i]` and the entry at storage index `i` stay positionally
    /// paired - materialization reads entries by that position. `MemStorage::write_at`
    /// is infallible, so the push never runs ahead of a failed write. A future
    /// fallible `Storage` MUST roll the header push back on a write error (or
    /// write before pushing the header) or the zip desyncs.
    fn append_sync(&self, entry: JournalBuffer) {
        let header_bytes = &entry[..PREPARE_HEADER_SIZE];
        let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(header_bytes)
            .expect("partition journal append expects a valid prepare header");
        let op = header.op;
        let meta = RetainedBatchMeta::of(&header, &entry);

        {
            let headers = unsafe { &mut *self.headers.get() };
            if headers.back().is_some_and(|last| last.op > op) {
                self.out_of_order.set(true);
            }
            if !headers.is_empty() {
                let position = self.front_position.get() + headers.len();
                self.first_header_by_op
                    .borrow_mut()
                    .entry(op)
                    .or_insert(position);
            }
            headers.push_back(header);
        };

        let storage_offset = {
            let inner = unsafe { &*self.inner.get() };
            let storage_offset = inner.storage.current_offset();
            inner.storage.write_at_sync(entry);
            storage_offset
        };

        {
            let op_to_storage_offset = unsafe { &mut *self.op_to_storage_offset.get() };
            op_to_storage_offset.insert(op, storage_offset);
        }
        if header.operation != Operation::SendMessages {
            self.resident_control_ops
                .set(self.resident_control_ops.get() + 1);
        }

        if let Some(meta) = meta {
            let offset_to_op = unsafe { &mut *self.offset_to_op.get() };
            offset_to_op.insert(meta.base_offset, op);

            // Keyed on `base_timestamp`, the broker append time stamped into
            // replies: the seek hint must live on the same clock as
            // `select_batch_slice`'s filter or timestamp polls seek to the
            // wrong resident entry.
            let timestamp_to_op = unsafe { &mut *self.timestamp_to_op.get() };
            timestamp_to_op.insert((meta.base_timestamp, op), op);

            self.resident_messages
                .set(self.resident_messages.get() + u64::from(meta.message_count));
            self.resident_message_size
                .set(self.resident_message_size.get() + meta.total_size);
        }
    }

    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.storage.is_empty()
    }

    /// Owned, offset-ascending clones of the resident `SendMessages` entries a
    /// poll may serve, one `Frozen` refcount bump each. Snapshots the resident
    /// tail at poll-plan time so a disk-tier straddle can be spliced off the
    /// partition borrow on owned data ([`crate::iggy_partition`]).
    ///
    /// Walks the offset index rather than the storage vector: the snapshot walk
    /// only ever matches message batches, and control ops (one per auto-commit
    /// poll) outnumber them by orders of magnitude between flushes, so cloning
    /// every entry made each disk-tier poll pay for every poll since the last
    /// flush.
    pub fn resident_message_entries(&self) -> Vec<JournalBuffer> {
        let offset_to_op = unsafe { &*self.offset_to_op.get() };
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        let inner = unsafe { &*self.inner.get() };
        let mut entries = Vec::with_capacity(offset_to_op.len());
        for op in offset_to_op.values() {
            if let Some(&storage_offset) = op_to_storage_offset.get(op)
                && let Some(entry) = inner.storage.read_at_sync(storage_offset)
            {
                entries.push(entry);
            }
        }
        entries
    }

    /// Owned, append-ordered clones of every resident entry, control ops
    /// included; one `Frozen` refcount bump each. Linear in the resident
    /// journal, so not for the poll path, which takes
    /// [`Self::resident_message_entries`].
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
    pub fn with_storage(storage: S) -> Self {
        Self {
            op_to_storage_offset: UnsafeCell::new(BTreeMap::new()),
            offset_to_op: UnsafeCell::new(BTreeMap::new()),
            timestamp_to_op: UnsafeCell::new(BTreeMap::new()),
            headers: UnsafeCell::new(VecDeque::new()),
            front_position: Cell::new(0),
            inner: UnsafeCell::new(JournalInner { storage }),
            first_header_by_op: RefCell::new(AHashMap::new()),
            resident_messages: Cell::new(0),
            resident_message_size: Cell::new(0),
            evicted_ring: UnsafeCell::new(BTreeMap::new()),
            evicted_ring_bytes: Cell::new(0),
            evicted_headers: UnsafeCell::new(VecDeque::new()),
            repair_tail: Rc::default(),
            released_through: Cell::new(0),
            evicted_ring_capacity: Cell::new(EVICTED_RING_CAPACITY),
            evicted_ring_bytes_max: Cell::new(EVICTED_RING_BYTES_MAX),
            repair_retention: Cell::new(true),
            resident_control_ops: Cell::new(0),
            evicted_through: Cell::new(0),
            out_of_order: Cell::new(false),
            held_run: Cell::new((0, 0)),
        }
    }

    /// Forget every resident header and index. The caller drained the entries.
    fn clear_resident_indexes(&self) {
        unsafe { &mut *self.headers.get() }.clear();
        self.front_position.set(0);
        self.first_header_by_op.borrow_mut().clear();
        unsafe { &mut *self.op_to_storage_offset.get() }.clear();
        unsafe { &mut *self.offset_to_op.get() }.clear();
        self.held_run.set((0, 0));
        unsafe { &mut *self.timestamp_to_op.get() }.clear();
        self.resident_control_ops.set(0);
        self.resident_messages.set(0);
        self.resident_message_size.set(0);
        self.out_of_order.set(false);
    }

    pub fn header_by_op(&self, op: u64) -> Option<PrepareHeader> {
        let headers = unsafe { &*self.headers.get() };
        if headers.front().is_some_and(|header| header.op == op) {
            return headers.front().copied();
        }
        let position = *self.first_header_by_op.borrow().get(&op)?;
        let header = position
            .checked_sub(self.front_position.get())
            .and_then(|index| headers.get(index));
        debug_assert!(header.is_some_and(|header| header.op == op));
        header.copied()
    }

    pub(crate) fn headers_for_commit(
        &self,
        from_op: u64,
        count: usize,
    ) -> Vec<Option<PrepareHeader>> {
        (0..count)
            .map(|index| {
                u64::try_from(index)
                    .ok()
                    .and_then(|index| from_op.checked_add(index))
                    .and_then(|op| self.header_by_op(op))
            })
            .collect()
    }

    pub(crate) fn commit_headers_match(
        &self,
        from_op: u64,
        expected: &[Option<PrepareHeader>],
    ) -> bool {
        expected.iter().enumerate().all(|(index, expected)| {
            u64::try_from(index)
                .ok()
                .and_then(|index| from_op.checked_add(index))
                .and_then(|op| self.header_by_op(op))
                == *expected
        })
    }

    /// Test residency without scanning the header vector.
    ///
    /// Synchronous insertion and eviction keep the storage map and headers
    /// consistent throughout every externally observable owner state.
    pub fn holds_op(&self, op: u64) -> bool {
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        op_to_storage_offset.contains_key(&op)
    }

    /// Highest resident op below `op`.
    pub fn held_below(&self, op: u64) -> Option<u64> {
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        op_to_storage_offset
            .range(..op)
            .next_back()
            .map(|(held, _)| *held)
    }

    /// Highest op `h >= floor` such that every op in `floor + 1..=h` is resident.
    pub fn held_through(&self, floor: u64) -> u64 {
        self.held_through_until(floor, u64::MAX)
    }

    /// [`Self::held_through`] capped at `top`, walking only the resident ops at
    /// or below it.
    pub fn held_through_until(&self, floor: u64, top: u64) -> u64 {
        if floor >= top {
            return floor;
        }
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        let (run_floor, run_through) = self.held_run.get();
        let mut through = if run_floor <= floor && run_through > floor {
            run_through.min(top)
        } else {
            floor
        };
        if through < top {
            for op in op_to_storage_offset
                .range(through + 1..=top)
                .map(|(op, _)| *op)
            {
                if op != through + 1 {
                    break;
                }
                through = op;
            }
        }
        if through > floor && (run_floor > floor || run_through < through) {
            self.held_run.set((floor, through));
        }
        through
    }

    /// Whether any op in `ops` is resident, in one pass over the headers: the
    /// range can be far wider than what is resident.
    pub fn holds_op_in(&self, ops: RangeInclusive<u64>) -> bool {
        let headers = unsafe { &*self.headers.get() };
        headers.iter().any(|header| ops.contains(&header.op))
    }

    /// Presence shape of the repair window `(floor, to_op]` in ONE pass over
    /// the header vec.
    ///
    /// Work and scratch space are bounded by resident headers, even when the
    /// requested op window is much larger than the local journal.
    ///
    /// The evicted ring is deliberately NOT consulted, matching the op-by-op
    /// form: consulting it would change the floor-refusal verdict.
    pub fn repaired_window_shape(&self, floor: u64, to_op: u64) -> RepairedWindowShape {
        let headers = unsafe { &*self.headers.get() };
        let expected = to_op.saturating_sub(floor);
        // More in-window ops than resident headers can never be covered, and
        // `expected` is unbounded here (`to_op` rides the local `commit_max`),
        // so this is both the early answer and what keeps the bitset below from
        // being sized off an arbitrary number.
        if expected > headers.len() as u64 {
            return RepairedWindowShape { complete: false };
        }
        // Dense window, so a flat presence vector beats a `HashSet`: no hashing
        // per op and one contiguous allocation. One BYTE per op rather than one
        // bit -- `expected` is bounded by `headers.len()`, so the 8x over a real
        // bitset buys simpler indexing at a size the caller already holds in
        // headers.
        #[allow(clippy::cast_possible_truncation)]
        let expected_len = expected as usize;
        let mut present = vec![false; expected_len];
        let mut covered = 0usize;
        for header in headers
            .iter()
            .filter(|header| header.op > floor && header.op <= to_op)
        {
            #[allow(clippy::cast_possible_truncation)]
            let slot = (header.op - floor - 1) as usize;
            if !present[slot] {
                present[slot] = true;
                covered += 1;
            }
        }
        RepairedWindowShape {
            // In-window ops only, deduplicated, so a count match IS coverage.
            complete: covered == expected_len,
        }
    }

    /// Highest `commit` any resident header stamped, in ONE pass.
    ///
    /// A lower bound on the group's commit point, which is what a rebuilt replica
    /// can recover from a log alone: a prepare records the primary's commit point
    /// at send time, so the true point may be one higher.
    ///
    /// Scans resident headers without probing every op up to the log head.
    pub fn max_commit_watermark(&self) -> u64 {
        let headers = unsafe { &*self.headers.get() };
        headers
            .iter()
            .map(|header| header.commit)
            .max()
            .unwrap_or(0)
    }

    /// Headers for the contiguous op run `from_op ..= commit_max`, in op order,
    /// stopping at the first missing op and at `limit` headers. A replication
    /// gap must not be skipped: the caller advances `commit_min` strictly by
    /// one, so a hole would break that contract.
    ///
    /// `limit` bounds what one caller commits in a single pass. The run is
    /// re-derived from the caller's own `commit_min` every time, so a truncated
    /// answer is resumed, not lost.
    ///
    /// Costs one index lookup per returned header, however many headers are
    /// resident: a lagging backup holds hundreds of thousands, and the commit
    /// walk calls this on every tick.
    pub fn committed_headers_from(
        &self,
        from_op: u64,
        commit_max: u64,
        limit: usize,
    ) -> Vec<PrepareHeader> {
        // Walk by OP, not by append position: after a rejoin the journal
        // interleaves live tail ops (which arrive while repair is still
        // streaming) with repaired window ops, so append order is no longer
        // op-ascending and a positional sequential scan would break at the
        // first interleave boundary forever.
        if from_op > commit_max {
            return Vec::new();
        }
        // `saturating_add` because `commit_max` is `u64::MAX` on a saturated
        // frontier, where `+ 1` would panic in debug and wrap to an empty span
        // (a walk that never resumes) in release.
        let span = (commit_max - from_op).saturating_add(1).min(limit as u64);
        // Stops at the first hole: a replication gap must not be skipped, or
        // `advance_commit_min`'s sequential contract breaks. `header_by_op`
        // returns the first resident occurrence of an op.
        (0..span)
            .map_while(|index| {
                from_op
                    .checked_add(index)
                    .and_then(|op| self.header_by_op(op))
            })
            .collect()
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

impl Journal for PartitionJournal<PartitionJournalMemStorage> {
    type Header = PrepareHeader;
    type Entry = JournalBuffer;
    #[rustfmt::skip]
    type HeaderRef<'a> = &'a Self::Header;

    /// No snapshot bookkeeping: the partition plane has no checkpoint of its
    /// own yet, so nothing supersedes journaled entries. Answered explicitly
    /// (the trait has no default) so partition-plane state transfer has to
    /// decide this deliberately rather than inherit it.
    fn snapshot_op(&self) -> u64 {
        0
    }

    fn set_snapshot_op(&self, _op: u64) {}

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
        self.append_sync(entry);
        Ok(())
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        self.bytes_by_op(header.op).await
    }

    /// Read off the op index, not the tail header: repair backfills append below
    /// higher ops and stay there until `sort_by_op` runs.
    fn last_op(&self) -> Option<u64> {
        let op_to_storage_offset = unsafe { &*self.op_to_storage_offset.get() };
        op_to_storage_offset.last_key_value().map(|(op, _)| *op)
    }

    /// Removed entries must stay refillable and cannot enter the committed repair ring.
    async fn remove_range(&self, ops: std::ops::RangeInclusive<u64>) -> io::Result<usize> {
        if *ops.start() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal removal requires 1-based operations",
            ));
        }
        let all_entries = {
            let inner = unsafe { &*self.inner.get() };
            inner.storage.drain()
        };
        // Positional against `headers` until the clear below (see the length-lock
        // invariant on `append_sync`), so the ops are captured first.
        let entry_ops: Vec<u64> = {
            let headers = unsafe { &*self.headers.get() };
            headers.iter().map(|header| header.op).collect()
        };
        self.clear_resident_indexes();

        let mut removed = 0usize;
        for (op, entry) in entry_ops.into_iter().zip(all_entries) {
            if ops.contains(&op) {
                removed += 1;
                continue;
            }
            // Replays bytes this journal already accepted once, so it cannot fail.
            self.append_sync(entry);
        }
        Ok(removed)
    }
}

pub fn select_batch_slice(
    batch: &BatchRef<'_>,
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
                // Match on the broker append time: replies stamp every message
                // with the flat batch `base_timestamp` (the per-message delta
                // applies to `origin_timestamp` only), so filtering on the
                // producer clock would skip the message stamped exactly at the
                // queried timestamp.
                batch.header.base_timestamp >= timestamp
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
/// `[256B BatchHeader][blob]` batch starting at byte `batch_base`
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
    batch: &BatchRef<'_>,
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
        fragments.push(Fragment::whole(send_messages::frozen_batch_header(
            &rewritten,
        )));
        fragments.push(Fragment::slice(
            source.clone(),
            batch_base + COMMAND_HEADER_SIZE + selection.start,
            batch_base + COMMAND_HEADER_SIZE + selection.end,
        ));
    }

    *last_matching_offset = Some(selection.last_matching_offset);
    *matched_messages += selection.matched_messages;
}

/// A fragment sliced from a storage buffer keeps the WHOLE allocation alive
/// until the reply frame is written out, and a reply can sit in a
/// per-connection mailbox for a while. Copy the matched bytes out when they
/// cover less than this fraction of the source, so a sparse match (a
/// `count=1` poll off a cold partition, a short poll into a large resident
/// batch) cannot pin a ~1 MiB chunk or a whole prepare per queued reply; a
/// dense match keeps the zero-copy path.
///
/// On the disk tier, the chunk allocations a poll reply keeps alive are
/// bounded by `SPARSE_CHUNK_PIN_DIVISOR` times the record bytes it serves
/// from disk, plus one page per compacted chunk. This is a ratio, not an
/// absolute cap: a grown chunk can still retain tens of MiB, and it does not
/// cover the reply's absolute size. The resident tier applies the same ratio
/// per prepare entry but only copies up to [`RESIDENT_SPARSE_COPY_MAX_BYTES`];
/// a sparse selection past that stays a slice and pins its prepare.
pub const SPARSE_CHUNK_PIN_DIVISOR: usize = 4;

/// Resident copies run inline on the shard pump (the disk walk is detached),
/// and a poll selects at most two partial batches (its first and last), so
/// this caps the pump's per-poll memcpy at about twice this many bytes.
const RESIDENT_SPARSE_COPY_MAX_BYTES: usize = 64 * 1024;

/// Rewrite the fragments pushed from index `pushed_from` on that slice
/// `source` to slices of one compact copy when their combined length is a
/// sparse fraction of `source` and at most `copy_max_bytes`. Fragments that
/// own their bytes (rewritten batch headers) are left alone. See
/// [`SPARSE_CHUNK_PIN_DIVISOR`].
pub fn unpin_sparse_source(
    fragments: &mut PollFragments<4096>,
    pushed_from: usize,
    source: &Frozen<4096>,
    copy_max_bytes: usize,
) {
    let pushed = &mut fragments[pushed_from..];
    let borrowed: usize = pushed
        .iter()
        .filter(|fragment| fragment.borrows_from(source))
        .map(Fragment::len)
        .sum();
    if borrowed == 0
        || borrowed >= source.len() / SPARSE_CHUNK_PIN_DIVISOR
        || borrowed > copy_max_bytes
    {
        return;
    }

    let mut compact = Owned::<4096>::with_capacity(borrowed);
    for fragment in pushed.iter().filter(|f| f.borrows_from(source)) {
        compact.extend_from_slice(fragment.as_slice());
    }
    let compact = Frozen::from(compact);
    let mut cursor = 0;
    for fragment in pushed.iter_mut() {
        if !fragment.borrows_from(source) {
            continue;
        }
        let len = fragment.len();
        *fragment = Fragment::slice(compact.clone(), cursor, cursor + len);
        cursor += len;
    }
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
    // Resident entries were locally stamped in `append_messages` or validated
    // at repair ingress, so a validating re-decode would only re-hash our own
    // write. See the invariant note at the committed-prefix flush walk.
    let Ok(batch) = decode_prepare_slice_trusted(prepare.as_slice()) else {
        return;
    };
    let Some(selection) = select_batch_slice(&batch, query, *matched_messages) else {
        return;
    };
    // The batch's 256B header sits right after the prepare header in a resident
    // entry (see `decode_prepare_slice`), so the batch base is `PREPARE_HEADER_SIZE`.
    let pushed_from = fragments.len();
    push_selected_batch_fragments(
        fragments,
        last_matching_offset,
        matched_messages,
        prepare,
        PREPARE_HEADER_SIZE,
        &batch,
        selection,
    );
    // `evict_prefix` drains the storage on the routine commit flush, so a
    // queued reply that still slices this prepare becomes its sole owner.
    // Accounted per entry here; the disk walk accounts per chunk, where many
    // batches share one allocation.
    unpin_sparse_source(
        fragments,
        pushed_from,
        prepare,
        RESIDENT_SPARSE_COPY_MAX_BYTES,
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

    (!fragments.is_empty()).then_some((fragments, last_matching_offset, matched_messages))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepairMemoryBudget;
    use bytes::Bytes;
    use iggy_binary_protocol::{Command, HEADER_SIZE};
    use journal::Journal;
    use server_common::Message;
    use server_common::send_messages::{
        BatchHeader, IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
        decode_batch_slice,
    };
    use server_common::sharding::IggyNamespace;
    use std::sync::Arc;

    #[compio::test]
    async fn given_a_small_snapshot_tail_with_a_large_backing_buffer_when_retained_should_charge_and_release_the_allocation()
     {
        const BACKING_CAPACITY: usize = 64 * 1024;
        let mut backing = Owned::<4096>::with_capacity(BACKING_CAPACITY);
        backing.extend_from_slice(build_prepare(1, HEADER_SIZE).as_slice());
        let prepare = Frozen::from(backing);
        let charge = Frozen::<4096>::allocation_size(prepare.backing_capacity()).unwrap()
            + size_of::<(u64, JournalBuffer)>();
        let budget = Arc::new(RepairMemoryBudget::new(charge));
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(1, u64::MAX);
        let lease = journal.retain_repair_from(1, budget.reservation());
        journal.append(prepare).await.unwrap();
        journal.evict_prefix(1);
        journal
            .append(build_prepare(2, HEADER_SIZE).into_frozen())
            .await
            .unwrap();
        journal.evict_prefix(1);

        assert!(lease.is_valid());
        assert!(journal.repair_entry(1).is_some());
        assert!(
            !budget.reservation().try_add(1),
            "the retained frame must charge its complete backing allocation"
        );
        lease.advance(2);
        assert!(journal.repair_entry(1).is_none());
        assert!(
            budget.reservation().try_add(charge),
            "advancing the lease must refund the complete allocation charge"
        );
    }

    #[compio::test]
    async fn given_snapshot_leases_when_the_ring_evicts_should_retain_until_every_receiver_advances()
     {
        const OPS: u64 = 8;
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(1, u64::MAX);
        let budget = Arc::new(RepairMemoryBudget::new(64 * 1024));
        let first = journal.retain_repair_from(2, budget.reservation());
        let second = journal.retain_repair_from(4, budget.reservation());
        for op in 1..=OPS {
            journal
                .append(build_prepare(op, HEADER_SIZE).into_frozen())
                .await
                .unwrap();
            journal.evict_prefix(1);
        }
        assert!(journal.repair_entry(1).is_none());
        for op in 2..=OPS {
            assert_eq!(
                journal.repair_entry(op).unwrap().as_slice(),
                build_prepare(op, HEADER_SIZE).as_slice()
            );
        }
        assert_eq!(journal.repair_retained_from(), Some(2));
        assert_eq!(journal.repair_headers_in(2..=OPS).len(), 7);
        first.advance(6);
        assert_eq!(journal.repair_retained_from(), Some(4));
        drop(second);
        assert_eq!(journal.repair_retained_from(), Some(6));
        drop(first);
        assert_eq!(journal.repair_retained_from(), Some(OPS));
        assert!(
            budget.reservation().try_add(64 * 1024),
            "all extra retention must release its budget"
        );
    }

    #[compio::test]
    async fn given_disk_backed_snapshot_tail_when_headers_leave_the_ring_should_keep_locations_without_bodies()
     {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(1, 1);
        let budget = Arc::new(RepairMemoryBudget::new(64 * 1024));
        let lease = journal.retain_repair_from(1, budget.reservation());
        let ops = u64::try_from(EVICTED_HEADERS_MAX + 2).unwrap();
        for op in 1..=ops {
            journal
                .append(build_message_prepare(op, op - 1, 1, 1))
                .await
                .unwrap();
        }
        journal.evict_prefix_located(usize::try_from(ops).unwrap(), |op| {
            Some(BodyPosition {
                segment_start: 0,
                position: op,
            })
        });
        assert!(lease.is_valid());
        assert!(journal.repair_entry(1).is_none());
        assert_eq!(journal.evicted_header(1).unwrap().body.position, 1);
        assert_eq!(journal.repair_retained_from(), Some(1));
        assert_eq!(journal.memory_retained_from(), Some(ops));
        journal.forget_segment(0);
        assert!(
            !lease.is_valid(),
            "GC must invalidate the promise before its disk bodies disappear"
        );
        assert!(journal.evicted_header(1).is_none());
        assert!(budget.reservation().try_add(64 * 1024));
    }

    #[compio::test]
    async fn given_snapshot_tail_and_unproven_walk_when_the_shared_cap_is_full_should_invalidate_without_blocking_writes()
     {
        let charge = build_prepare(1, HEADER_SIZE)
            .retained_allocation_size()
            .unwrap()
            + size_of::<(u64, JournalBuffer)>();
        let budget = Arc::new(RepairMemoryBudget::new(2 * charge));
        let mut walk = budget.reservation();
        assert!(walk.try_add(charge));
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(1, u64::MAX);
        let lease = journal.retain_repair_from(1, budget.reservation());
        for op in 1..=2 {
            journal
                .append(build_prepare(op, HEADER_SIZE).into_frozen())
                .await
                .unwrap();
            journal.evict_prefix(1);
        }
        assert!(lease.is_valid());
        journal
            .append(build_prepare(3, HEADER_SIZE).into_frozen())
            .await
            .unwrap();
        journal.evict_prefix(1);
        assert!(!lease.is_valid());
        assert_eq!(journal.repair_retained_from(), Some(3));
        assert_eq!(journal.evicted_ring_occupancy(), (1, HEADER_SIZE as u64));
        assert!(budget.reservation().try_add(charge));
        drop(walk);
        let replacement = journal.retain_repair_from(3, budget.reservation());
        journal.clear_all();
        assert!(!replacement.is_valid());
        assert_eq!(journal.evicted_ring_occupancy(), (0, 0));
        assert!(budget.reservation().try_add(2 * charge));
    }

    fn build_prepare(op: u64, size: usize) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(size).transmute_header(|_, h: &mut PrepareHeader| {
            h.command = Command::Prepare;
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
    async fn truncate_from_drops_the_suffix_and_keeps_the_prefix_readable() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in 1..=5 {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }

        let removed = journal.truncate_from(4).await.expect("truncate");
        assert_eq!(removed, 2, "ops 4 and 5 must go");
        assert_eq!(journal.last_op(), Some(3));
        for op in 1..=3u64 {
            let header = journal
                .header_by_op(op)
                .expect("a retained op must survive");
            assert!(
                journal.entry(&header).await.is_some(),
                "a retained entry must still read back after the rewrite"
            );
        }
        for op in 4..=5u64 {
            assert!(journal.header_by_op(op).is_none(), "op {op} must be gone");
        }

        // The point of dropping them: the primary's retransmission refills the range.
        journal
            .append(build_prepare(4, HEADER_SIZE + 16).into_frozen())
            .await
            .expect("a truncated op must be appendable again");
        assert_eq!(journal.last_op(), Some(4));
    }

    #[compio::test]
    async fn given_reordered_duplicates_when_truncating_should_index_retained_headers() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for (op, checksum) in [(3, 30), (1, 10), (4, 40), (2, 20), (1, 11), (2, 21)] {
            let entry = build_prepare(op, HEADER_SIZE).transmute_header(
                |mut old, header: &mut PrepareHeader| {
                    old.checksum = checksum;
                    *header = old;
                },
            );
            journal.append(entry.into_frozen()).await.unwrap();
        }

        assert_eq!(journal.truncate_from(3).await.unwrap(), 2);
        for (op, checksum) in [(1, 10), (2, 20)] {
            let header = journal.header_by_op(op).expect("retained header");
            assert_eq!(header.op, op);
            assert_eq!(header.checksum, checksum, "the first duplicate must win");
            assert!(journal.entry(&header).await.is_some());
        }
        for op in [3, 4] {
            assert!(journal.header_by_op(op).is_none(), "op {op} was removed");
        }
    }

    #[compio::test]
    async fn given_truncated_journal_when_repairing_out_of_order_should_keep_frontier_checksum() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1, 2, 3] {
            journal
                .append(build_prepare(op, HEADER_SIZE).into_frozen())
                .await
                .unwrap();
        }
        journal.truncate_from(2).await.unwrap();
        for op in [3, 2] {
            let entry = build_prepare(op, HEADER_SIZE).transmute_header(
                |mut old, header: &mut PrepareHeader| {
                    old.checksum = u128::from(op);
                    *header = old;
                },
            );
            journal.append(entry.into_frozen()).await.unwrap();
        }

        assert_eq!(
            consensus::repaired_frontier_update(3, |op| journal.header_by_op(op)),
            Some((3, 3)),
            "repair must pair the frontier op with its own checksum"
        );
    }

    #[compio::test]
    async fn given_backfill_below_the_tail_when_reading_last_op_should_report_the_highest_op() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1, 3, 2] {
            journal
                .append(build_prepare(op, HEADER_SIZE).into_frozen())
                .await
                .unwrap();
        }

        assert_eq!(journal.last_op(), Some(3));
    }

    #[compio::test]
    async fn given_a_run_past_the_top_when_capping_held_through_should_stop_at_the_top() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1, 2, 3, 4, 6] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }

        assert_eq!(journal.held_through(0), 4);
        assert_eq!(journal.held_through_until(0, 2), 2);
        assert_eq!(journal.held_through_until(0, 6), 4);
        assert_eq!(journal.held_through_until(3, 3), 3);
        assert_eq!(journal.held_through_until(5, 3), 5);
    }

    #[compio::test]
    async fn repaired_window_shape_rejects_unbounded_sparse_window_before_allocation() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal
            .append(build_prepare(7, HEADER_SIZE + 16).into_frozen())
            .await
            .expect("append");

        let shape = journal.repaired_window_shape(0, u64::MAX);

        assert!(!shape.complete);
    }

    #[compio::test]
    async fn repair_retention_keeps_the_newest_ops_after_out_of_order_backfill() {
        const CAPACITY: usize = 4;
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(CAPACITY, u64::MAX);
        for op in [4, 2, 8, 6, 3, 1, 7, 5] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .unwrap();
        }
        journal.evict_prefix(8);
        assert_eq!(journal.evicted_ring_occupancy().0, CAPACITY);
        assert_eq!(journal.repair_retained_from(), Some(5));
        for op in 1..=4 {
            assert!(journal.repair_entry(op).is_none());
            assert!(journal.repair_header(op).is_none());
        }
        for op in 5..=8 {
            assert_eq!(journal.repair_header(op).unwrap().op, op);
            assert_eq!(
                journal.repair_entry(op).unwrap().as_slice(),
                build_prepare(op, HEADER_SIZE + 16).as_slice()
            );
        }
        assert_eq!(
            journal
                .repair_headers_in(6..=7)
                .into_keys()
                .collect::<Vec<_>>(),
            vec![6, 7]
        );
        let empty_start = 7;
        assert!(journal.repair_headers_in(empty_start..=6).is_empty());
        for op in [3, 1] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .unwrap();
        }
        assert_eq!(journal.repair_retained_from(), Some(1));
        journal.evict_prefix(8);
        assert_eq!(journal.repair_retained_from(), Some(5));
        assert_eq!(journal.evicted_ring_occupancy().0, CAPACITY);
        journal.clear_all();
        assert_eq!(journal.evicted_ring_occupancy(), (0, 0));
        assert!(journal.repair_entry(8).is_none());
    }

    /// A body that no segment holds stays in memory while a newer body leaves
    /// it, so the window that memory holds up to the head starts above the
    /// newest released op.
    #[compio::test]
    async fn given_an_unlocated_body_below_a_released_one_when_asked_for_the_memory_window_should_start_above_the_release()
     {
        const PAYLOAD_LEN: usize = 64;
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        let body_len = build_message_prepare(1, 0, 1, PAYLOAD_LEN).len() as u64;
        journal.set_ring_caps(EVICTED_RING_CAPACITY, 2 * body_len);
        for op in 1..=3 {
            journal
                .append(build_message_prepare(op, op - 1, 1, PAYLOAD_LEN))
                .await
                .unwrap();
        }
        journal.evict_prefix_located(3, |op| {
            (op == 2).then_some(BodyPosition {
                segment_start: 0,
                position: 0,
            })
        });
        assert!(journal.repair_entry(1).is_some());
        assert!(journal.evicted_header(2).is_some());
        assert_eq!(journal.repair_retained_from(), Some(1));
        assert_eq!(journal.memory_retained_from(), Some(3));
    }

    /// Released headers take no room from the bodies the byte cap keeps in
    /// memory, and a newest body past the cap takes no header away.
    #[compio::test]
    async fn given_released_headers_when_bodies_pass_the_byte_cap_should_keep_both_windows() {
        const OPS: u64 = 20;
        const BODIES_KEPT: u64 = 3;
        const PAYLOAD_LEN: usize = 64;
        let located = |op| {
            Some(BodyPosition {
                segment_start: 0,
                position: op,
            })
        };
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        let body_len = build_message_prepare(1, 0, 1, PAYLOAD_LEN).len() as u64;
        journal.set_ring_caps(EVICTED_RING_CAPACITY, BODIES_KEPT * body_len);
        for op in 1..=OPS {
            journal
                .append(build_message_prepare(op, op - 1, 1, PAYLOAD_LEN))
                .await
                .unwrap();
        }
        journal.evict_prefix_located(usize::try_from(OPS).unwrap(), located);
        for op in 1..=OPS - BODIES_KEPT {
            assert!(
                journal.evicted_header(op).is_some(),
                "op {op} lost its header"
            );
        }
        for op in OPS - BODIES_KEPT + 1..=OPS {
            assert!(journal.repair_entry(op).is_some(), "op {op} lost its body");
        }

        let large = build_message_prepare(OPS + 1, OPS, 1, PAYLOAD_LEN * 64);
        assert!(large.len() as u64 > BODIES_KEPT * body_len);
        journal.append(large).await.unwrap();
        journal.evict_prefix_located(1, located);
        assert_eq!(journal.repair_retained_from(), Some(1));
        for op in 1..=OPS {
            assert!(
                journal.evicted_header(op).is_some(),
                "op {op} lost its header"
            );
        }
        assert!(journal.repair_entry(OPS + 1).is_some());
    }

    /// Each release makes room in the header window first, so a full window
    /// never grows its allocation past its cap.
    #[compio::test]
    async fn given_more_releases_than_the_header_cap_when_trimming_should_keep_the_deque_within_it()
    {
        const BATCH: u64 = 1024;
        let headers_max = EVICTED_HEADERS_MAX;
        let ops = u64::try_from(headers_max).unwrap() + 2;
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        journal.set_ring_caps(EVICTED_RING_CAPACITY, 1);
        let template = build_message_prepare(1, 0, 1, 1);
        for first in (1..=ops).step_by(usize::try_from(BATCH).unwrap()) {
            let last = (first + BATCH - 1).min(ops);
            for op in first..=last {
                let mut prepare = Message::<PrepareHeader>::new(template.len());
                prepare.as_mut_slice().copy_from_slice(template.as_slice());
                let prepare = prepare.transmute_header(|header, stamped: &mut PrepareHeader| {
                    *stamped = header;
                    stamped.op = op;
                });
                journal.append(prepare.into_frozen()).await.unwrap();
            }
            journal.evict_prefix_located(usize::try_from(last - first + 1).unwrap(), |op| {
                Some(BodyPosition {
                    segment_start: 0,
                    position: op,
                })
            });
        }
        let headers = unsafe { &*journal.evicted_headers.get() };
        assert!(
            headers.capacity() <= headers_max,
            "the header deque grew to {} slots",
            headers.capacity()
        );
        assert_eq!(headers.len(), headers_max);
        assert_eq!(
            journal.repair_retained_from(),
            Some(ops - u64::try_from(headers_max).unwrap())
        );
    }

    #[compio::test]
    async fn repair_headers_in_serves_the_commit_point_from_the_evicted_ring() {
        // Blank AT the commit point is the one slot a merge can neither adopt nor
        // discard, so a quorum that all flushed there deadlocks. A flushed replica has
        // no resident header there, so the ring must answer.
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in 1..=4 {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }
        // `commit_messages` evicts the committed prefix inclusively, so the commit
        // point's own resident header goes with it.
        journal.evict_prefix(2);
        assert!(
            journal.header_by_op(2).is_none(),
            "the resident header at the commit point is gone after the flush"
        );

        let window = journal.repair_headers_in(2..=4);
        assert!(
            window.contains_key(&2),
            "the commit point must still be describable, or the view change deadlocks"
        );
        for op in 3..=4u64 {
            assert!(window.contains_key(&op), "op {op} is resident and in range");
        }
        assert!(
            !window.contains_key(&1),
            "ops outside the window must not be reported"
        );
    }

    #[compio::test]
    async fn commit_header_snapshot_handles_repair_order_gaps_and_replacements() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [3, 1, 4] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .unwrap();
        }
        let selected = journal.headers_for_commit(1, 4);
        assert_eq!(
            selected
                .iter()
                .map(|header| header.map(|header| header.op))
                .collect::<Vec<_>>(),
            vec![Some(1), None, Some(3), Some(4)]
        );
        assert!(journal.commit_headers_match(1, &selected));
        journal
            .append(build_prepare(2, HEADER_SIZE + 16).into_frozen())
            .await
            .unwrap();
        assert!(
            !journal.commit_headers_match(1, &selected),
            "repair filled a previously missing header"
        );
        let selected = journal.headers_for_commit(1, 4);
        assert!(journal.commit_headers_match(1, &selected));
        let duplicate = build_prepare(1, HEADER_SIZE + 16).transmute_header(
            |mut old, header: &mut PrepareHeader| {
                old.checksum ^= 1;
                *header = old;
            },
        );
        journal.append(duplicate.into_frozen()).await.unwrap();
        assert_eq!(
            journal.headers_for_commit(1, 4),
            selected,
            "the snapshot must agree with header_by_op's first resident match"
        );
        assert!(journal.commit_headers_match(1, &selected));
        journal.truncate_from(3).await.unwrap();
        assert!(
            !journal.commit_headers_match(1, &selected),
            "a truncated selection must not publish"
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

        journal.evict_prefix(committed.len());
        assert_eq!(
            journal.resident_entries().len(),
            2,
            "ops 3 and 4 stay resident after eviction"
        );

        // Committed ops are evicted from the index; uncommitted ops remain.
        assert!(journal.header_by_op(1).is_none(), "op 1 must be evicted");
        assert!(journal.header_by_op(2).is_none(), "op 2 must be evicted");
        let header3 = journal.header_by_op(3).expect("op 3 must be retained");
        let header4 = journal.header_by_op(4).expect("op 4 must be retained");

        // Retained entries are still byte-readable after the eviction.
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
        journal.evict_prefix(committed.len());
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

        journal.evict_prefix(committed.len());
        assert_eq!(
            journal.resident_entries().len(),
            1,
            "op 4 stays retained past the gap"
        );
        assert!(journal.header_by_op(4).is_some(), "op 4 still resident");
    }

    #[compio::test]
    async fn given_backfill_below_the_front_when_reading_the_prefix_should_wait_for_op_order() {
        // Once ops 1 and 2 leave, op 4 sits above the op 3 hole and must not
        // start a prefix. Op 3 then lands behind op 4, where only a sort can
        // bring it to the front.
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1u64, 2, 4] {
            journal
                .append(build_prepare(op, HEADER_SIZE + 16).into_frozen())
                .await
                .expect("append");
        }
        journal.evict_prefix(journal.committed_prefix_len(0, 4));
        assert_eq!(journal.committed_prefix_len(0, 4), 0, "op 3 is missing");

        journal
            .append(build_prepare(3, HEADER_SIZE + 16).into_frozen())
            .await
            .expect("append");
        // Applying op 3 puts the bound at op 4, so only the order check stops
        // op 4 from going first.
        assert_eq!(
            journal.committed_prefix_len(3, 4),
            0,
            "op 3 is resident but behind op 4"
        );
        assert!(journal.sort_by_op());
        assert!(!journal.sort_by_op(), "a sorted journal stays put");
        assert_eq!(journal.committed_prefix_len(3, 4), 2);

        // An install supersedes evicted ops, so only its commit point bounds
        // the start afterwards.
        journal.evict_prefix(2);
        journal.clear_all();
        journal
            .append(build_prepare(4, HEADER_SIZE + 16).into_frozen())
            .await
            .expect("append");
        assert_eq!(journal.committed_prefix_len(2, 4), 0, "op 3 is missing");
        assert_eq!(journal.committed_prefix_len(3, 4), 1);
    }

    #[compio::test]
    async fn given_duplicated_op_when_sorting_should_keep_the_first_copy_first() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for (op, checksum) in [(2, 20), (1, 10), (2, 21)] {
            let entry = build_prepare(op, HEADER_SIZE).transmute_header(
                |mut old, header: &mut PrepareHeader| {
                    old.checksum = checksum;
                    *header = old;
                },
            );
            journal.append(entry.into_frozen()).await.unwrap();
        }
        assert!(journal.sort_by_op());
        assert_eq!(journal.header_by_op(2).unwrap().checksum, 20);
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
        let run = journal.committed_headers_from(1, 4, usize::MAX);
        let ops: Vec<u64> = run.iter().map(|header| header.op).collect();
        assert_eq!(
            ops,
            vec![1, 2],
            "must stop at the op 3 gap, not skip to op 4"
        );

        assert!(
            journal.committed_headers_from(5, 4, usize::MAX).is_empty(),
            "from_op past commit_max yields nothing"
        );

        // The limit truncates the run rather than skipping ahead in it, so the
        // caller resumes at the op it stopped on.
        let bounded = journal.committed_headers_from(1, 4, 1);
        let ops: Vec<u64> = bounded.iter().map(|header| header.op).collect();
        assert_eq!(ops, vec![1], "the limit must cut the run at its front");
        assert!(
            journal.committed_headers_from(1, 4, 0).is_empty(),
            "a zero limit commits nothing"
        );
    }

    #[compio::test]
    async fn given_reordered_duplicates_when_walking_should_follow_op_order_and_first_copy() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for (op, checksum) in [(3, 30), (1, 10), (4, 40), (2, 20), (1, 11), (2, 21)] {
            let entry = build_prepare(op, HEADER_SIZE).transmute_header(
                |mut old, header: &mut PrepareHeader| {
                    old.checksum = checksum;
                    *header = old;
                },
            );
            journal.append(entry.into_frozen()).await.unwrap();
        }

        let run: Vec<_> = journal
            .committed_headers_from(1, u64::MAX, usize::MAX)
            .iter()
            .map(|header| (header.op, header.checksum))
            .collect();
        assert_eq!(run, vec![(1, 10), (2, 20), (3, 30), (4, 40)]);
    }

    /// Three-message batch with the broker append time (`base_timestamp`)
    /// deliberately AFTER every producer stamp (`origin_timestamp` + deltas),
    /// the layout every real batch has (the broker stamps later than the
    /// producer). Timestamp polls filter on the broker time because that is
    /// the timestamp replies surface per message.
    fn build_timestamped_batch(base_timestamp: u64, origin_timestamp: u64) -> Vec<u8> {
        let mut messages = IggyMessages::with_capacity(3);
        for index in 0..3u64 {
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    origin_timestamp: origin_timestamp + index,
                    payload_length: 8,
                    ..Default::default()
                },
                payload: Bytes::from_static(b"abcdefgh"),
                user_headers: None,
            });
        }
        let mut owned = SendMessagesOwned::from_messages(IggyNamespace::new(1, 1, 0), &messages)
            .expect("build send_messages batch");
        owned.header.base_timestamp = base_timestamp;
        stamped_batch_record(owned)
    }

    /// Stamp `owned`'s checksum and lay it out as the `[256B batch header][blob]`
    /// record a batch occupies in storage.
    fn stamped_batch_record(mut owned: SendMessagesOwned) -> Vec<u8> {
        owned.header.batch_checksum = owned.header.checksum_for_blob(&owned.blob);
        let mut record = vec![0u8; COMMAND_HEADER_SIZE + owned.blob.len()];
        owned.header.encode_into(&mut record[..COMMAND_HEADER_SIZE]);
        record[COMMAND_HEADER_SIZE..].copy_from_slice(&owned.blob);
        record
    }

    /// A resident `SendMessages` prepare entry holding one batch of
    /// `message_count` records with distinct `payload_len`-byte payloads, in
    /// the `[PrepareHeader][256B batch header][blob]` layout
    /// `try_push_resident_entry` decodes.
    fn build_resident_prepare(message_count: usize, payload_len: usize) -> Frozen<4096> {
        build_message_prepare(1, 0, message_count, payload_len)
    }

    /// [`build_resident_prepare`] at a chosen op and stamped base offset, so a
    /// journal can hold several batches in offset order.
    fn build_message_prepare(
        op: u64,
        base_offset: u64,
        message_count: usize,
        payload_len: usize,
    ) -> Frozen<4096> {
        let mut messages = IggyMessages::with_capacity(message_count);
        for index in 0..message_count {
            let fill = u8::try_from(index % usize::from(u8::MAX)).expect("bounded by u8::MAX");
            messages.push(IggyMessage {
                header: IggyMessageHeader {
                    payload_length: u32::try_from(payload_len).expect("payload_len fits u32"),
                    ..Default::default()
                },
                payload: Bytes::from(vec![fill; payload_len]),
                user_headers: None,
            });
        }
        let mut owned = SendMessagesOwned::from_messages(IggyNamespace::new(1, 1, 0), &messages)
            .expect("build send_messages batch");
        owned.header.base_offset = base_offset;
        let record = stamped_batch_record(owned);

        let mut prepare = build_prepare(op, PREPARE_HEADER_SIZE + record.len()).transmute_header(
            |header: PrepareHeader, send_messages: &mut PrepareHeader| {
                *send_messages = header;
                send_messages.operation = Operation::SendMessages;
            },
        );
        prepare.as_mut_slice()[PREPARE_HEADER_SIZE..].copy_from_slice(&record);
        prepare.into_frozen()
    }

    /// A header-only consumer offset prepare: the shape every auto-commit
    /// journals, carrying no batch.
    fn build_control_prepare(op: u64) -> Frozen<4096> {
        build_prepare(op, HEADER_SIZE + 16)
            .transmute_header(|header: PrepareHeader, control: &mut PrepareHeader| {
                *control = header;
                control.operation = Operation::StoreConsumerOffset;
            })
            .into_frozen()
    }

    fn entry_ops(entries: &[Frozen<4096>]) -> Vec<u64> {
        entries
            .iter()
            .map(|entry| {
                bytemuck::checked::try_from_bytes::<PrepareHeader>(&entry[..PREPARE_HEADER_SIZE])
                    .expect("entry holds a valid prepare header")
                    .op
            })
            .collect()
    }

    #[compio::test]
    async fn resident_message_entries_skip_control_ops() {
        // Batches at ops 1, 3, 5 (three offsets each) interleaved with the
        // offset ops a polling group journals between them.
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in 1..=6u64 {
            let entry = if op % 2 == 1 {
                build_message_prepare(op, (op - 1) / 2 * 3, 3, 8)
            } else {
                build_control_prepare(op)
            };
            journal.append(entry).await.expect("append");
        }
        assert_eq!(
            entry_ops(&journal.resident_message_entries()),
            vec![1, 3, 5]
        );
        assert_eq!(journal.resident_control_ops(), 3);

        // Eviction drops the evicted batches from the poll view.
        journal.evict_prefix(2);
        assert_eq!(entry_ops(&journal.resident_message_entries()), vec![3, 5]);
        assert_eq!(journal.resident_control_ops(), 2);
    }

    #[compio::test]
    async fn resident_control_ops_follow_every_rebuild() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in 1..=4u64 {
            journal
                .append(build_control_prepare(op))
                .await
                .expect("append");
        }
        journal
            .append(build_message_prepare(5, 0, 3, 8))
            .await
            .expect("append");
        assert_eq!(journal.resident_control_ops(), 4);

        journal.evict_prefix(2);
        assert_eq!(journal.resident_control_ops(), 2, "ops 3 and 4 stay");

        journal.truncate_from(4).await.expect("truncate");
        assert_eq!(journal.resident_control_ops(), 1, "only op 3 survives");

        journal.commit();
        assert_eq!(journal.resident_control_ops(), 0);

        journal
            .append(build_control_prepare(6))
            .await
            .expect("append");
        journal.clear_all();
        assert_eq!(journal.resident_control_ops(), 0);
    }

    /// What readers see of the resident journal. The storage offsets of an
    /// eviction and of a rebuild differ, so entries are read back through them
    /// instead of compared. Probing `held_through` also seeds its cached run.
    #[derive(Debug, PartialEq)]
    struct ResidentView {
        entry_ops: Vec<u64>,
        headers: Vec<Option<PrepareHeader>>,
        reads: Vec<(u64, Option<u64>)>,
        offsets: BTreeMap<u64, u64>,
        timestamps: BTreeMap<(u64, u64), u64>,
        held_through: Vec<u64>,
        control_ops: usize,
        totals: (u64, u64),
        out_of_order: bool,
    }

    fn resident_view(
        journal: &PartitionJournal<PartitionJournalMemStorage>,
        top_op: u64,
    ) -> ResidentView {
        let inner = unsafe { &*journal.inner.get() };
        let entries = journal.resident_entries();
        let folded = entries.iter().fold((0, 0), |(messages, size), entry| {
            let header =
                bytemuck::checked::try_from_bytes::<PrepareHeader>(&entry[..PREPARE_HEADER_SIZE])
                    .expect("entry holds a valid prepare header");
            RetainedBatchMeta::of(header, entry).map_or((messages, size), |meta| {
                (
                    messages + u64::from(meta.message_count),
                    size + meta.total_size,
                )
            })
        });
        let totals = (journal.resident_messages(), journal.resident_message_size());
        assert_eq!(totals, folded, "the totals must count the resident batches");
        ResidentView {
            entry_ops: entry_ops(&entries),
            headers: (0..=top_op).map(|op| journal.header_by_op(op)).collect(),
            reads: unsafe { &*journal.op_to_storage_offset.get() }
                .iter()
                .map(|(&op, &storage_offset)| {
                    let entry = inner.storage.read_at_sync(storage_offset);
                    (op, entry.map(|entry| entry_ops(&[entry])[0]))
                })
                .collect(),
            offsets: unsafe { &*journal.offset_to_op.get() }.clone(),
            timestamps: unsafe { &*journal.timestamp_to_op.get() }.clone(),
            held_through: (0..=top_op)
                .map(|floor| journal.held_through(floor))
                .collect(),
            control_ops: journal.resident_control_ops(),
            totals,
            out_of_order: journal.out_of_order.get(),
        }
    }

    async fn journal_with(
        entries: &[Frozen<4096>],
    ) -> PartitionJournal<PartitionJournalMemStorage> {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for entry in entries {
            journal.append(entry.clone()).await.expect("append");
        }
        journal
    }

    #[compio::test]
    async fn given_journal_shapes_when_evicting_a_prefix_should_leave_what_a_rebuild_leaves() {
        // `(op, base offset)` per entry, `None` for an offset op. The shapes:
        // batches between offset ops, two batches at one base offset, and a
        // duplicated op that a prefix either takes whole or splits.
        let shapes: [&[(u64, Option<u64>)]; 3] = [
            &[
                (1, Some(0)),
                (2, None),
                (3, Some(3)),
                (4, None),
                (5, Some(6)),
                (6, Some(9)),
                (7, None),
            ],
            &[(1, Some(0)), (2, Some(3)), (3, Some(3)), (4, Some(6))],
            &[(1, Some(0)), (2, None), (2, None), (3, Some(3)), (4, None)],
        ];
        for shape in shapes {
            let entries: Vec<_> = shape
                .iter()
                .zip(0u128..)
                .map(|(&(op, base_offset), checksum)| {
                    base_offset.map_or_else(
                        || {
                            build_prepare(op, HEADER_SIZE + 16)
                                .transmute_header(|mut old, header: &mut PrepareHeader| {
                                    old.checksum = checksum;
                                    *header = old;
                                })
                                .into_frozen()
                        },
                        |base_offset| build_message_prepare(op, base_offset, 3, 8),
                    )
                })
                .collect();
            let next_op = shape.iter().map(|(op, _)| op).max().unwrap() + 1;
            let next = build_message_prepare(next_op, 100, 3, 8);
            for count in 0..=entries.len() + 1 {
                let popped = journal_with(&entries).await;
                let rebuilt = journal_with(&entries).await;
                resident_view(&popped, next_op);
                resident_view(&rebuilt, next_op);

                popped.evict_prefix(count);
                rebuilt.rebuild_without_prefix(count);
                assert_eq!(
                    resident_view(&popped, next_op),
                    resident_view(&rebuilt, next_op),
                    "shape {shape:?}, evicting {count}"
                );

                // Positions taken after the eviction must stay right too.
                for journal in [&popped, &rebuilt] {
                    journal.append(next.clone()).await.expect("append");
                }
                popped.evict_prefix(1);
                rebuilt.rebuild_without_prefix(1);
                assert_eq!(
                    resident_view(&popped, next_op),
                    resident_view(&rebuilt, next_op),
                    "shape {shape:?}, evicting {count} and then 1"
                );
            }
        }
    }

    #[compio::test]
    async fn given_a_backfill_inside_the_prefix_when_evicting_should_flush_the_tail_in_order() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for op in [1u64, 2, 5, 3, 4] {
            journal
                .append(build_control_prepare(op))
                .await
                .expect("append");
        }
        assert_eq!(journal.committed_prefix_len(0, u64::MAX), 0);

        journal.evict_prefix(3);

        assert_eq!(entry_ops(&journal.resident_entries()), vec![3, 4]);
        assert_eq!(
            journal.committed_prefix_len(0, u64::MAX),
            2,
            "the out-of-order pair left with the prefix"
        );
    }

    #[compio::test]
    async fn given_every_rebuild_when_counting_resident_messages_should_count_the_batches() {
        let journal = PartitionJournal::<PartitionJournalMemStorage>::default();
        for (op, base_offset) in [(1u64, 0u64), (3, 6), (4, 9), (2, 3)] {
            journal
                .append(build_message_prepare(op, base_offset, 3, 8))
                .await
                .expect("append");
        }
        let batch_size = journal.resident_message_size() / 4;
        assert_eq!(journal.resident_messages(), 12);

        assert!(journal.sort_by_op());
        assert_eq!(journal.resident_messages(), 12, "sorting keeps every batch");

        journal.evict_prefix(1);
        assert_eq!(journal.resident_messages(), 9);
        assert_eq!(journal.resident_message_size(), 3 * batch_size);

        journal.truncate_from(4).await.expect("truncate");
        assert_eq!(journal.resident_messages(), 6, "ops 2 and 3 stay");

        journal.commit();
        assert_eq!(journal.resident_messages(), 0);

        journal
            .append(build_message_prepare(5, 12, 3, 8))
            .await
            .expect("append");
        journal.clear_all();
        assert_eq!(
            (journal.resident_messages(), journal.resident_message_size()),
            (0, 0)
        );
    }

    fn offset_lookup(offset: u64, count: u32) -> MessageLookup {
        MessageLookup::Offset {
            offset,
            count,
            ceiling: u64::MAX,
        }
    }

    #[test]
    fn timestamp_poll_at_exact_broker_timestamp_includes_the_batch() {
        // A client polls with a timestamp read from a previous reply, which is
        // the batch `base_timestamp`. Filtering on the producer origin clock
        // (always a little earlier) made `origin >= base` false and silently
        // skipped the message stamped exactly at the queried time.
        let record = build_timestamped_batch(1_000, 900);
        let batch = decode_batch_slice(&record).expect("batch decodes");

        let at_exact = select_batch_slice(
            &batch,
            MessageLookup::Timestamp {
                timestamp: 1_000,
                count: 10,
                ceiling: u64::MAX,
            },
            0,
        )
        .expect("selection at the exact broker timestamp");
        assert_eq!(
            at_exact.matched_messages, 3,
            "poll at the reported timestamp must include the whole batch"
        );

        assert!(
            select_batch_slice(
                &batch,
                MessageLookup::Timestamp {
                    timestamp: 1_001,
                    count: 10,
                    ceiling: u64::MAX,
                },
                0,
            )
            .is_none(),
            "poll past the broker timestamp must match nothing"
        );
    }

    /// A short poll into a large resident batch ships a rewritten header plus
    /// a body slice. Left as a slice of the prepare, that body would keep the
    /// whole entry alive after `evict_prefix` drained it from the journal.
    #[test]
    fn resident_partial_selection_copies_out_of_a_large_prepare() {
        let prepare = build_resident_prepare(1_000, 300);
        let query = offset_lookup(500, 1);
        let batch = decode_prepare_slice_trusted(prepare.as_slice()).expect("prepare decodes");
        let selection = select_batch_slice(&batch, query, 0).expect("record 500 is selected");
        let expected_body = &batch.blob()[selection.start..selection.end];
        assert!(
            expected_body.len() < prepare.len() / SPARSE_CHUNK_PIN_DIVISOR,
            "fixture must select a sparse fraction of the prepare"
        );

        let (fragments, last_matching_offset, matched) =
            select_resident(std::slice::from_ref(&prepare), query).expect("one record matches");
        assert_eq!(matched, 1);
        assert_eq!(last_matching_offset, Some(500));
        assert_eq!(fragments.len(), 2, "rewritten header plus body slice");
        let (header, body) = (&fragments[0], &fragments[1]);
        assert!(
            !body.borrows_from(&prepare),
            "sparse body must be copied out of the prepare"
        );
        assert_eq!(body.as_slice(), expected_body);
        assert_eq!(header.len(), COMMAND_HEADER_SIZE);
        assert!(
            !header.borrows_from(&body.clone().into_frozen()),
            "the owned header must stay out of the compact copy"
        );
        let rewritten = BatchHeader::decode(header.as_slice()).expect("rewritten header decodes");
        assert_eq!(rewritten.message_count, 1);
    }

    #[test]
    fn resident_whole_batch_selection_keeps_the_zero_copy_slice() {
        let prepare = build_resident_prepare(1_000, 300);

        let (fragments, last_matching_offset, matched) =
            select_resident(std::slice::from_ref(&prepare), offset_lookup(0, 1_000))
                .expect("whole batch matches");
        assert_eq!(matched, 1_000);
        assert_eq!(last_matching_offset, Some(999));
        assert_eq!(fragments.len(), 1, "a whole batch ships its original bytes");
        assert!(
            fragments[0].borrows_from(&prepare),
            "dense selection keeps the zero-copy path"
        );
        assert_eq!(fragments[0].len(), prepare.len() - PREPARE_HEADER_SIZE);
    }

    /// The resident copy runs inline on the shard pump, so past the byte cap
    /// a sparse selection stays a zero-copy slice even though it pins the
    /// prepare.
    #[test]
    fn resident_sparse_copy_stops_at_the_byte_cap() {
        let prepare = build_resident_prepare(2_000, 300);
        let query = offset_lookup(0, 200);
        let batch = decode_prepare_slice_trusted(prepare.as_slice()).expect("prepare decodes");
        let selection = select_batch_slice(&batch, query, 0).expect("200 records are selected");
        let selected = selection.end - selection.start;
        assert!(
            selected > RESIDENT_SPARSE_COPY_MAX_BYTES
                && selected < prepare.len() / SPARSE_CHUNK_PIN_DIVISOR,
            "fixture must select a sparse fraction that is over the copy cap"
        );

        let (fragments, _, _) =
            select_resident(std::slice::from_ref(&prepare), query).expect("records match");
        assert_eq!(fragments.len(), 2, "rewritten header plus body slice");
        assert!(
            fragments[1].borrows_from(&prepare),
            "over the cap the body must stay a slice of the prepare"
        );
    }

    /// A sparse match must not pin the whole disk chunk: the matched bytes
    /// are copied out byte-for-byte and the fragments stop borrowing the
    /// chunk allocation. A dense match keeps the zero-copy slices, and
    /// fragments that already own their bytes (rewritten batch headers) are
    /// never touched.
    #[test]
    fn unpin_sparse_source_bounds_chunk_retention() {
        let chunk_len = 1 << 20;
        let mut backing = Owned::<4096>::zeroed(chunk_len);
        for (position, byte) in backing.as_mut_slice().iter_mut().enumerate() {
            *byte = u8::try_from(position % 251).unwrap();
        }
        let chunk = Frozen::from(backing);

        let mut fragments = PollFragments::<4096>::new();
        fragments.push(Fragment::whole(Owned::<4096>::zeroed(256).into()));
        fragments.push(Fragment::slice(chunk.clone(), 512, 512 + 600));
        fragments.push(Fragment::slice(chunk.clone(), 4096, 4096 + 300));
        let first = fragments[1].as_slice().to_vec();
        let second = fragments[2].as_slice().to_vec();
        unpin_sparse_source(&mut fragments, 0, &chunk, usize::MAX);
        assert!(
            !fragments[1].borrows_from(&chunk) && !fragments[2].borrows_from(&chunk),
            "sparse slices must be copied out of the chunk"
        );
        assert_eq!(fragments[1].as_slice(), &first[..]);
        assert_eq!(fragments[2].as_slice(), &second[..]);
        assert!(
            fragments[1].borrows_from(&fragments[2].clone().into_frozen()),
            "copies pack into one compact allocation"
        );
        assert_eq!(fragments[0].len(), 256);

        let mut fragments = PollFragments::<4096>::new();
        fragments.push(Fragment::slice(
            chunk.clone(),
            0,
            chunk_len / SPARSE_CHUNK_PIN_DIVISOR,
        ));
        unpin_sparse_source(&mut fragments, 0, &chunk, usize::MAX);
        assert!(
            fragments[0].borrows_from(&chunk),
            "dense slice keeps the zero-copy path"
        );
    }
    #[test]
    fn selected_message_count_does_not_include_offset_holes() {
        let entries = vec![
            build_message_prepare(1, 0, 1, 8),
            build_message_prepare(2, 100, 1, 8),
        ];
        let (fragments, last, count) = select_resident(
            &entries,
            MessageLookup::Offset {
                offset: 0,
                count: 3,
                ceiling: 100,
            },
        )
        .unwrap();
        assert!(!fragments.is_empty());
        assert_eq!(last, Some(100));
        assert_eq!(count, 2);
    }
}
