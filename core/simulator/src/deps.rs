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

use crate::executor::{TimerHandle, yield_once};
use clock::Clock;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{IggyTimestamp, variadic};
use journal::superblock::{SuperblockContents, SuperblockStore};
use journal::{Journal, JournalHandle, Storage};
use metadata::MuxStateMachine;
use metadata::stm::stream::Streams;
use metadata::stm::user::Users;
use server_common::{Message, iobuf::Owned};
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;

/// Fixed synthetic epoch for [`SimClock`]: 2026-01-01T00:00:00Z in micros.
///
/// Virtual time starts at zero; without a base every primary-stamped
/// `created_at` would decode as 1970 and expiry math would sit on the
/// epoch edge. A constant base keeps timestamps realistic while staying
/// a pure function of the seed.
pub const SIM_EPOCH_MICROS: u64 = 1_767_225_600_000_000;

/// Deterministic [`Clock`] over the executor's virtual timeline, injected
/// into every consensus group so prepare timestamps replay with the seed.
#[derive(Debug)]
pub struct SimClock {
    timer: TimerHandle,
}

impl SimClock {
    #[must_use]
    pub const fn new(timer: TimerHandle) -> Self {
        Self { timer }
    }
}

impl Clock for SimClock {
    type Realtime = IggyTimestamp;

    fn realtime(&self) -> IggyTimestamp {
        IggyTimestamp::from(SIM_EPOCH_MICROS + self.timer.now().as_nanos() / 1_000)
    }
}

/// In-memory storage backend for testing/simulation
#[derive(Debug, Default)]
pub struct MemStorage {
    data: RefCell<Vec<u8>>,
}

#[allow(clippy::future_not_send)]
impl Storage for MemStorage {
    type Buffer = Vec<u8>;

    async fn write_at(&self, offset: usize, buf: Self::Buffer) -> std::io::Result<usize> {
        let len = buf.len();
        let mut data = self.data.borrow_mut();
        let end = offset + len;
        if end > data.len() {
            data.resize(end, 0);
        }
        data[offset..end].copy_from_slice(&buf);
        Ok(len)
    }

    async fn read_at(
        &self,
        offset: usize,
        mut buffer: Self::Buffer,
    ) -> std::io::Result<Self::Buffer> {
        let data = self.data.borrow();
        let end = offset + buffer.len();
        if offset < data.len() && end <= data.len() {
            buffer[..].copy_from_slice(&data[offset..end]);
        }
        Ok(buffer)
    }
}

// TODO: Replace with actual Journal, the only thing that we will need to change is the `Storage` impl for an in-memory one.
/// Generic in-memory journal implementation for testing/simulation
pub struct SimJournal<S: Storage> {
    storage: S,
    headers: UnsafeCell<HashMap<u64, PrepareHeader>>,
    offsets: UnsafeCell<HashMap<u64, usize>>,
    write_offset: Cell<usize>,
    /// Highest op appended, `None` when empty. Tracked so a restart reads the
    /// retained head in O(1) without scanning `headers` (see
    /// [`SimJournal::last_op`]).
    last_op: Cell<Option<u64>>,
    /// Debug-only single-accessor tripwire. `entry` / `append` hold a
    /// [`JournalAccessGuard`] across their whole body, including the storage
    /// `.await`, so if a suspending storage tier ever let a second task touch
    /// the journal while one is parked mid-read the assert fires before the
    /// aliasing `UnsafeCell` access could become UB.
    #[cfg(debug_assertions)]
    accessing: Cell<bool>,
}

impl<S: Storage + Default> Default for SimJournal<S> {
    fn default() -> Self {
        Self {
            storage: S::default(),
            headers: UnsafeCell::new(HashMap::new()),
            offsets: UnsafeCell::new(HashMap::new()),
            write_offset: Cell::new(0),
            last_op: Cell::new(None),
            #[cfg(debug_assertions)]
            accessing: Cell::new(false),
        }
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl<S: Storage> std::fmt::Debug for SimJournal<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimJournal")
            .field("storage", &"<Storage>")
            .field("headers", &"<UnsafeCell>")
            .field("offsets", &"<UnsafeCell>")
            .field("write_offset", &self.write_offset.get())
            .finish()
    }
}

/// Debug-only RAII guard enforcing [`SimJournal`]'s single-accessor
/// invariant. Trips if a journal op runs while another is already in flight
/// (the precondition for the `UnsafeCell` borrows in `entry` / `append` to
/// alias). Decrements on drop so an early `?` return or an unwind clears it.
#[cfg(debug_assertions)]
struct JournalAccessGuard<'a>(&'a Cell<bool>);

#[cfg(debug_assertions)]
impl<'a> JournalAccessGuard<'a> {
    fn new(flag: &'a Cell<bool>) -> Self {
        assert!(
            !flag.replace(true),
            "SimJournal accessed re-entrantly: a borrow spans an .await while \
             another task touches the journal. Sound only while MemStorage \
             never suspends and journal access stays pump-serialized; a \
             suspending storage tier would make the UnsafeCell access aliasing UB."
        );
        Self(flag)
    }
}

#[cfg(debug_assertions)]
impl Drop for JournalAccessGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

#[allow(clippy::future_not_send)]
impl<S: Storage<Buffer = Vec<u8>>> Journal for SimJournal<S> {
    type Header = PrepareHeader;
    type Entry = Message<PrepareHeader>;
    type HeaderRef<'a>
        = &'a PrepareHeader
    where
        Self: 'a;

    fn last_op(&self) -> Option<u64> {
        self.last_op.get()
    }

    /// Drop the suffix, so a simulated backup whose entries disagree with a started
    /// view reconciles the way a real one does. Mirrors
    /// `PrepareJournal::truncate_from`, whose watermark stays put; here it never moves.
    async fn truncate_from(&self, from_op: u64) -> std::io::Result<usize> {
        if from_op == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "truncate_from: ops are 1-based, so 0 would discard the whole journal",
            ));
        }
        #[cfg(debug_assertions)]
        let _guard = JournalAccessGuard::new(&self.accessing);
        let headers = unsafe { &mut *self.headers.get() };
        let offsets = unsafe { &mut *self.offsets.get() };
        let doomed: Vec<u64> = headers
            .keys()
            .copied()
            .filter(|op| *op >= from_op)
            .collect();
        for op in &doomed {
            headers.remove(op);
            offsets.remove(op);
        }
        self.last_op.set(headers.keys().copied().max());
        Ok(doomed.len())
    }

    /// The simulated journal retains everything for the run, so nothing is
    /// ever superseded by a snapshot. Answered explicitly (the trait has no
    /// default) so a simulated state transfer has to opt into a watermark
    /// rather than silently inherit one that never moves.
    fn snapshot_op(&self) -> u64 {
        0
    }

    fn set_snapshot_op(&self, _op: u64) {}

    // TODO(hubcio): validate that the caller's checksum matches the stored
    // header - currently this looks up by op only, ignoring the checksum.
    // A real journal implementation must reject mismatches.
    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        // Single-accessor invariant: the `UnsafeCell` shared borrows below are
        // sound only because journal access is single-task (the pump serializes
        // it; the off-pump poll path reads an owned PollPlan, never this cell)
        // and `MemStorage::read_at` never actually suspends, so no borrow spans
        // a real await. Do NOT extend a `headers` / `offsets` borrow past the
        // `.await` (e.g. to validate the checksum against the stored header per
        // the TODO above): once storage can suspend, a concurrent `append`
        // taking `&mut` makes that aliasing UB. The guard makes both
        // preconditions loud rather than silently miscompiling.
        #[cfg(debug_assertions)]
        let _access = JournalAccessGuard::new(&self.accessing);
        let headers = unsafe { &*self.headers.get() };
        let offsets = unsafe { &*self.offsets.get() };

        let header = headers.get(&header.op)?;
        let offset = *offsets.get(&header.op)?;

        let buffer = self
            .storage
            .read_at(offset, vec![0; header.size as usize])
            .await
            .ok()?;
        let message = Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("prepare buffer must contain a valid prepare message");
        Some(message)
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        if header.op == 0 {
            return None;
        }
        unsafe { &*self.headers.get() }.get(&(header.op - 1))
    }

    async fn append(&self, entry: Self::Entry) -> std::io::Result<()> {
        // Held across the write `.await`; see `entry` for the single-accessor
        // invariant this tripwire guards.
        #[cfg(debug_assertions)]
        let _access = JournalAccessGuard::new(&self.accessing);
        let header = *entry.header();
        let message_bytes = entry.into_frozen();
        let offset = self.write_offset.get();

        let bytes_written = self
            .storage
            .write_at(offset, message_bytes.as_slice().to_vec())
            .await?;
        unsafe { &mut *self.headers.get() }.insert(header.op, header);
        unsafe { &mut *self.offsets.get() }.insert(header.op, offset);
        self.write_offset.set(offset + bytes_written);
        let head = self.last_op.get().map_or(header.op, |op| op.max(header.op));
        self.last_op.set(Some(head));
        Ok(())
    }

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = unsafe { &*self.headers.get() };
        headers.get(&(idx as u64))
    }
}

impl JournalHandle for SimJournal<MemStorage> {
    type Target = Self;

    fn handle(&self) -> &Self::Target {
        self
    }
}

impl SimJournal<MemStorage> {
    /// Highest op appended, `None` when empty. Survives a restart because the
    /// harness retains the whole journal behind an `Rc`.
    #[must_use]
    pub const fn last_op(&self) -> Option<u64> {
        self.last_op.get()
    }

    /// Forget one op, leaving a hole exactly where a lost prepare would.
    ///
    /// Tests only. The alternative is choreographing `Prepare`, `Commit` and
    /// `RepairPrepare` drops on a directed link until a replica falls behind, which
    /// is fragile to tune; the scenarios are about what a replica does with a hole,
    /// not how it got one.
    ///
    /// `last_op` is deliberately left alone: a hole below the head must not look like
    /// a shorter log, since that is the state a view change has to survive.
    pub fn forget_op(&self, op: u64) -> bool {
        #[cfg(debug_assertions)]
        let _guard = JournalAccessGuard::new(&self.accessing);
        let headers = unsafe { &mut *self.headers.get() };
        let offsets = unsafe { &mut *self.offsets.get() };
        offsets.remove(&op);
        headers.remove(&op).is_some()
    }

    /// The committed watermark to restore after a restart, mirroring
    /// `metadata::recover`. On a solo cluster every appended op commits the instant
    /// it is durable, so the head IS the commit point; otherwise the highest
    /// `commit` any journaled prepare stamped, a lower bound, since a prepare
    /// records the primary's commit point at send time, so the true point may be one
    /// op higher and re-commits on rejoin.
    #[must_use]
    pub fn recovery_commit_watermark(&self, solo: bool) -> u64 {
        if solo {
            return self.last_op.get().unwrap_or(0);
        }
        let headers = unsafe { &*self.headers.get() };
        headers
            .values()
            .map(|header| header.commit)
            .max()
            .unwrap_or(0)
    }

    /// The head prepare's header, `None` when empty. Restores the last-prepare
    /// checksum (hash-chain continuity) and the prepare-timestamp floor at restart,
    /// mirroring `restore_metadata_consensus`.
    #[must_use]
    pub fn last_header(&self) -> Option<PrepareHeader> {
        let head = self.last_op.get()?;
        let headers = unsafe { &*self.headers.get() };
        headers.get(&head).copied()
    }

    /// Read the entry at `op` synchronously, for off-executor WAL replay at restart.
    /// Mirrors [`Journal::entry`] without the never-suspending `MemStorage` await, so
    /// it needs no [`JournalAccessGuard`]: a synchronous read offers no suspension
    /// point for another task to interleave on.
    ///
    /// # Panics
    /// If the bytes at `op` do not decode as a prepare message, meaning the retained
    /// WAL is corrupt, which is a harness bug since the sim has no torn writes.
    #[must_use]
    pub fn entry_sync(&self, op: u64) -> Option<Message<PrepareHeader>> {
        let headers = unsafe { &*self.headers.get() };
        let offsets = unsafe { &*self.offsets.get() };
        let header = headers.get(&op)?;
        let offset = *offsets.get(&op)?;
        let data = self.storage.data.borrow();
        let end = offset.checked_add(header.size as usize)?;
        let buffer = data.get(offset..end)?.to_vec();
        let message = Message::try_from(Owned::<4096>::copy_from_slice(&buffer))
            .expect("prepare buffer must contain a valid prepare message");
        Some(message)
    }
}

#[derive(Debug, Default)]
pub struct SimSnapshot {}

/// In-memory superblock for the simulator.
///
/// Held by the harness behind an `Rc` so its bytes survive a replica being dropped
/// and rebuilt across a restart. RAM has no torn writes, so it holds only the latest
/// payload, with none of the framing or checksum `PingPongSuperblock` adds.
#[derive(Debug, Default)]
pub struct SimSuperblock {
    latest: RefCell<Option<Vec<u8>>>,
    /// Fault injection: when set, every [`SuperblockStore::write`] errors and
    /// persists nothing, so `persist_superblock_if_needed` returns `false` and the
    /// shard withholds the view-scoped send. Proves the split-brain gate, that a
    /// replica never sends in a view it has not durably recorded.
    fail_writes: Cell<bool>,
    /// Fault injection: when set, every [`SuperblockStore::write`] suspends once
    /// before completing. A real superblock persist is an fsync-wide suspension
    /// point; the default in-memory write completes on first poll, so nothing can
    /// interleave with a persist and every schedule-sensitive bug behind one is
    /// invisible to the simulator. The yield restores the window.
    yield_writes: Cell<bool>,
}

impl SimSuperblock {
    /// Latest persisted payload, read synchronously. The harness reads this off the
    /// executor at restart, so no `block_on` is needed.
    #[must_use]
    pub fn read_latest_sync(&self) -> Option<Vec<u8>> {
        self.latest.borrow().clone()
    }

    /// Make every subsequent write fail, a persistent write fault, so the durability
    /// gate withholds view-scoped sends. See [`Self::fail_writes`].
    pub fn set_fail_writes(&self) {
        self.fail_writes.set(true);
    }

    /// Make every subsequent write suspend once before completing, so tasks that
    /// are ready at persist time interleave with it. See [`Self::yield_writes`].
    pub fn set_yield_writes(&self) {
        self.yield_writes.set(true);
    }
}

#[allow(clippy::future_not_send)]
impl SuperblockStore for SimSuperblock {
    async fn write(&self, payload: &[u8]) -> std::io::Result<()> {
        if self.fail_writes.get() {
            return Err(std::io::Error::other("sim superblock write fault"));
        }
        if self.yield_writes.get() {
            yield_once().await;
        }
        *self.latest.borrow_mut() = Some(payload.to_vec());
        Ok(())
    }

    async fn read_latest(&self) -> std::io::Result<SuperblockContents> {
        // RAM has no torn writes and the sim never injects an unreadable record, so
        // the payload is either present or was never written.
        Ok(self
            .latest
            .borrow()
            .as_ref()
            .map_or(SuperblockContents::Empty, |payload| {
                SuperblockContents::Present(payload.clone())
            }))
    }
}

/// Type alias for simulator state machine
pub type SimMuxStateMachine = MuxStateMachine<variadic!(Users, Streams)>;
