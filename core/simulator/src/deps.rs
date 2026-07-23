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

use crate::executor::TimerHandle;
use clock::Clock;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::{IggyTimestamp, variadic};
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
impl<S: Storage<Buffer = Vec<u8>>> Journal<S> for SimJournal<S> {
    type Header = PrepareHeader;
    type Entry = Message<PrepareHeader>;
    type HeaderRef<'a>
        = &'a PrepareHeader
    where
        Self: 'a;

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
        Ok(())
    }

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = unsafe { &*self.headers.get() };
        headers.get(&(idx as u64))
    }
}

impl JournalHandle for SimJournal<MemStorage> {
    type Storage = MemStorage;
    type Target = Self;

    fn handle(&self) -> &Self::Target {
        self
    }
}

#[derive(Debug, Default)]
pub struct SimSnapshot {}

/// Type alias for simulator state machine
pub type SimMuxStateMachine = MuxStateMachine<variadic!(Users, Streams)>;
